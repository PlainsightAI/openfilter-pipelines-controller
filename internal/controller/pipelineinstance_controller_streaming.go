/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
)

// reconcileStreaming handles the streaming (Deployment-based) reconciliation path
func (r *PipelineInstanceReconciler) reconcileStreaming(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance, pipeline *pipelinesv1alpha1.Pipeline, pipelineSource *pipelinesv1alpha1.PipelineSource) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Initialize streaming status if not already set
	if pipelineInstance.Status.Streaming == nil {
		pipelineInstance.Status.Streaming = &pipelinesv1alpha1.StreamingStatus{}
	}

	// Set start time if not already set
	if pipelineInstance.Status.StartTime == nil {
		now := metav1.Now()
		pipelineInstance.Status.StartTime = &now
		if err := r.Status().Update(ctx, pipelineInstance); err != nil {
			log.Error(err, "Failed to set start time")
			return ctrl.Result{}, err
		}
	}

	// Handle deletion (finalizer cleanup)
	const finalizerName = "filter.plainsight.ai/streaming-cleanup"
	if !pipelineInstance.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(pipelineInstance, finalizerName) {
			// Delete the Deployment
			if err := r.deleteStreamingDeployment(ctx, pipelineInstance); err != nil {
				log.Error(err, "Failed to delete streaming deployment")
				return ctrl.Result{}, err
			}

			// Delete the Services (pipeline can be nil during deletion)
			if err := r.deleteFilterServices(ctx, pipelineInstance); err != nil {
				log.Error(err, "Failed to delete filter services")
				return ctrl.Result{}, err
			}

			// Remove finalizer
			controllerutil.RemoveFinalizer(pipelineInstance, finalizerName)
			if err := r.Update(ctx, pipelineInstance); err != nil {
				log.Error(err, "Failed to remove finalizer")
				return ctrl.Result{}, err
			}
			log.Info("Successfully removed finalizer and cleaned up resources")
		}
		return ctrl.Result{}, nil
	}

	// If we reach here during deletion without finalizer, Pipeline must exist for normal reconciliation
	if pipeline == nil {
		return ctrl.Result{}, fmt.Errorf("pipeline is required for non-deletion reconciliation")
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(pipelineInstance, finalizerName) {
		controllerutil.AddFinalizer(pipelineInstance, finalizerName)
		if err := r.Update(ctx, pipelineInstance); err != nil {
			log.Error(err, "Failed to add finalizer")
			return ctrl.Result{}, err
		}
	}

	// Step 1: Ensure Deployment exists
	if err := r.ensureStreamingDeployment(ctx, pipelineInstance, pipeline, pipelineSource); err != nil {
		log.Error(err, "Failed to ensure streaming deployment")
		r.setCondition(pipelineInstance, ConditionTypeDegraded, metav1.ConditionTrue, "DeploymentCreationFailed", err.Error())
		if err := r.Status().Update(ctx, pipelineInstance); err != nil {
			log.Error(err, "Failed to update status")
		}
		return ctrl.Result{}, err
	}

	// Step 1.5: Ensure Services exist for filters with exposed ports
	if err := r.ensureFilterServices(ctx, pipelineInstance, pipeline); err != nil {
		log.Error(err, "Failed to ensure filter services")
		r.setCondition(pipelineInstance, ConditionTypeDegraded, metav1.ConditionTrue, "ServiceCreationFailed", err.Error())
		if err := r.Status().Update(ctx, pipelineInstance); err != nil {
			log.Error(err, "Failed to update status")
		}
		return ctrl.Result{}, err
	}

	// Step 2: Update streaming status from Deployment
	if err := r.updateStreamingStatus(ctx, pipelineInstance); err != nil {
		log.Error(err, "Failed to update streaming status")
		// Don't fail reconciliation, just log and continue
	}

	// Step 3: Check for idle timeout
	if pipelineSource != nil && pipelineSource.Spec.RTSP != nil && pipelineSource.Spec.RTSP.IdleTimeout != nil {
		if shouldComplete, reason := r.checkIdleTimeout(pipelineInstance, pipelineSource); shouldComplete {
			log.Info("Streaming run idle timeout reached, marking as complete", "reason", reason)
			r.setCondition(pipelineInstance, ConditionTypeSucceeded, metav1.ConditionTrue, "IdleTimeout", reason)
			r.setCondition(pipelineInstance, ConditionTypeProgressing, metav1.ConditionFalse, "IdleTimeout", reason)

			now := metav1.Now()
			pipelineInstance.Status.CompletionTime = &now

			// Delete the Deployment
			if err := r.deleteStreamingDeployment(ctx, pipelineInstance); err != nil {
				log.Error(err, "Failed to delete deployment after idle timeout")
			}

			// Delete the Services
			if err := r.deleteFilterServices(ctx, pipelineInstance); err != nil {
				log.Error(err, "Failed to delete services after idle timeout")
			}

			if err := r.Status().Update(ctx, pipelineInstance); err != nil {
				log.Error(err, "Failed to update status after idle timeout")
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
	}

	// Step 4: Update conditions based on deployment status
	if pipelineInstance.Status.Streaming.ReadyReplicas > 0 {
		r.setCondition(pipelineInstance, ConditionTypeProgressing, metav1.ConditionTrue, "Running", "Stream is processing")
		// Note: We don't set Available=True for streaming runs as they run indefinitely
	} else {
		r.setCondition(pipelineInstance, ConditionTypeProgressing, metav1.ConditionTrue, "Starting", "Waiting for stream to become ready")
	}

	if err := r.Status().Update(ctx, pipelineInstance); err != nil {
		log.Error(err, "Failed to update status")
		return ctrl.Result{}, err
	}

	// Requeue for periodic status updates
	return ctrl.Result{RequeueAfter: StatusUpdateInterval}, nil
}

// ensureStreamingDeployment creates or updates the Deployment for streaming mode
func (r *PipelineInstanceReconciler) ensureStreamingDeployment(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance, pipeline *pipelinesv1alpha1.Pipeline, pipelineSource *pipelinesv1alpha1.PipelineSource) error {
	log := logf.FromContext(ctx)

	deploymentName := pipelineInstance.Name + "-deploy"
	deployment := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: deploymentName, Namespace: pipelineInstance.Namespace}, deployment)

	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to get deployment: %w", err)
	}

	// Build the desired Deployment spec
	desiredDeployment := r.buildStreamingDeployment(pipelineInstance, pipeline, pipelineSource, deploymentName)

	if apierrors.IsNotFound(err) {
		// Create the Deployment
		log.Info("Creating streaming deployment", "deployment", deploymentName)
		if err := controllerutil.SetControllerReference(pipelineInstance, desiredDeployment, r.Scheme); err != nil {
			return fmt.Errorf("failed to set controller reference: %w", err)
		}
		if err := r.Create(ctx, desiredDeployment); err != nil {
			return fmt.Errorf("failed to create deployment: %w", err)
		}
		pipelineInstance.Status.Streaming.DeploymentName = deploymentName
		return nil
	}

	// Update existing Deployment using Patch to avoid conflicts
	// We only patch the spec to avoid race conditions with status updates
	patchBase := client.MergeFrom(deployment.DeepCopy())
	deployment.Spec = desiredDeployment.Spec
	if err := r.Patch(ctx, deployment, patchBase); err != nil {
		return fmt.Errorf("failed to update deployment: %w", err)
	}
	pipelineInstance.Status.Streaming.DeploymentName = deploymentName

	return nil
}

// buildStreamingDeployment constructs a Deployment for streaming mode
func (r *PipelineInstanceReconciler) buildStreamingDeployment(pipelineInstance *pipelinesv1alpha1.PipelineInstance, pipeline *pipelinesv1alpha1.Pipeline, pipelineSource *pipelinesv1alpha1.PipelineSource, deploymentName string) *appsv1.Deployment {
	replicas := int32(1)
	maxUnavailable := intstr.FromInt32(0)
	maxSurge := intstr.FromInt32(1)

	// Build filter containers
	containers := make([]corev1.Container, 0, len(pipeline.Spec.Filters))
	for _, filter := range pipeline.Spec.Filters {
		container := corev1.Container{
			Name:            filter.Name,
			Image:           filter.Image,
			ImagePullPolicy: filter.ImagePullPolicy,
		}

		if len(filter.Command) > 0 {
			container.Command = filter.Command
		}
		if len(filter.Args) > 0 {
			container.Args = filter.Args
		}

		// Inject RTSP environment variables first so they can be referenced in filter config
		var envVars []corev1.EnvVar
		if pipelineSource != nil && pipelineSource.Spec.RTSP != nil {
			// If credentials are provided, inject internal env vars for username/password
			// and build URL with embedded credentials
			if pipelineSource.Spec.RTSP.CredentialsSecret != nil {
				secretName := pipelineSource.Spec.RTSP.CredentialsSecret.Name
				// Internal env vars for credential substitution
				envVars = append(envVars,
					corev1.EnvVar{
						Name: "_RTSP_USERNAME",
						ValueFrom: &corev1.EnvVarSource{
							SecretKeyRef: &corev1.SecretKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
								Key:                  "username",
							},
						},
					},
					corev1.EnvVar{
						Name: "_RTSP_PASSWORD",
						ValueFrom: &corev1.EnvVarSource{
							SecretKeyRef: &corev1.SecretKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
								Key:                  "password",
							},
						},
					},
				)
				// Build URL with credential placeholders
				rtspURL := buildRTSPURLWithCredentials(pipelineSource.Spec.RTSP)
				envVars = append(envVars, corev1.EnvVar{
					Name:  "RTSP_URL",
					Value: rtspURL,
				})
			} else {
				// No credentials, build simple URL
				rtspURL := buildRTSPURL(pipelineSource.Spec.RTSP)
				envVars = append(envVars, corev1.EnvVar{
					Name:  "RTSP_URL",
					Value: rtspURL,
				})
			}
		}

		// Add filter config as env vars with FILTER_ prefix
		// These are added after RTSP_URL so they can reference it using $(RTSP_URL)
		for _, cfg := range filter.Config {
			envVars = append(envVars, corev1.EnvVar{
				Name:  "FILTER_" + strings.ToUpper(cfg.Name),
				Value: cfg.Value,
			})
		}

		// Add user-defined env vars
		envVars = append(envVars, filter.Env...)

		container.Env = envVars

		if filter.Resources != nil {
			container.Resources = *filter.Resources
		}

		containers = append(containers, container)
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentName,
			Namespace: pipelineInstance.Namespace,
			Labels: map[string]string{
				"app":              "pipeline-stream",
				"pipelineinstance": pipelineInstance.Name,
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RollingUpdateDeploymentStrategyType,
				RollingUpdate: &appsv1.RollingUpdateDeployment{
					MaxUnavailable: &maxUnavailable,
					MaxSurge:       &maxSurge,
				},
			},
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app":              "pipeline-stream",
					"pipelineinstance": pipelineInstance.Name,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":              "pipeline-stream",
						"pipelineinstance": pipelineInstance.Name,
					},
				},
				Spec: corev1.PodSpec{
					// No dedicated ServiceAccount required for streaming mode; default SA is sufficient
					RestartPolicy: corev1.RestartPolicyAlways,
					Containers:    containers,
				},
			},
		},
	}

	return deployment
}

// updateStreamingStatus updates the streaming status from the Deployment and Pods
func (r *PipelineInstanceReconciler) updateStreamingStatus(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance) error {
	if pipelineInstance.Status.Streaming == nil || pipelineInstance.Status.Streaming.DeploymentName == "" {
		return nil
	}

	deployment := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: pipelineInstance.Status.Streaming.DeploymentName, Namespace: pipelineInstance.Namespace}, deployment)
	if err != nil {
		return fmt.Errorf("failed to get deployment: %w", err)
	}

	// Update replica counts
	pipelineInstance.Status.Streaming.ReadyReplicas = deployment.Status.ReadyReplicas
	pipelineInstance.Status.Streaming.UpdatedReplicas = deployment.Status.UpdatedReplicas
	pipelineInstance.Status.Streaming.AvailableReplicas = deployment.Status.AvailableReplicas

	// Track if deployment just became ready
	if deployment.Status.ReadyReplicas > 0 && pipelineInstance.Status.Streaming.LastReadyTime == nil {
		now := metav1.Now()
		pipelineInstance.Status.Streaming.LastReadyTime = &now
	}

	// Count container restarts from pods
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.InNamespace(pipelineInstance.Namespace), client.MatchingLabels{"pipelineinstance": pipelineInstance.Name}); err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	totalRestarts := int32(0)
	for _, pod := range podList.Items {
		for _, containerStatus := range pod.Status.ContainerStatuses {
			totalRestarts += containerStatus.RestartCount
		}
	}
	pipelineInstance.Status.Streaming.ContainerRestarts = totalRestarts

	return nil
}

// checkIdleTimeout checks if the streaming run should complete due to idle timeout
func (r *PipelineInstanceReconciler) checkIdleTimeout(pipelineInstance *pipelinesv1alpha1.PipelineInstance, pipelineSource *pipelinesv1alpha1.PipelineSource) (bool, string) {
	if pipelineSource == nil || pipelineSource.Spec.RTSP == nil || pipelineSource.Spec.RTSP.IdleTimeout == nil {
		return false, ""
	}

	// Ensure streaming status exists
	if pipelineInstance.Status.Streaming == nil {
		return false, ""
	}

	idleTimeout := pipelineSource.Spec.RTSP.IdleTimeout.Duration

	// Check if all replicas are unready
	if pipelineInstance.Status.Streaming.ReadyReplicas > 0 {
		// Stream is ready, reset idle tracking
		return false, ""
	}

	// If LastReadyTime is set, check how long it's been unready
	if pipelineInstance.Status.Streaming.LastReadyTime != nil {
		unreadyDuration := time.Since(pipelineInstance.Status.Streaming.LastReadyTime.Time)
		if unreadyDuration >= idleTimeout {
			return true, fmt.Sprintf("Stream has been unready for %v (idle timeout: %v)", unreadyDuration, idleTimeout)
		}
	} else if pipelineInstance.Status.StartTime != nil {
		// Never became ready, check time since start
		unreadyDuration := time.Since(pipelineInstance.Status.StartTime.Time)
		if unreadyDuration >= idleTimeout {
			return true, fmt.Sprintf("Stream never became ready after %v (idle timeout: %v)", unreadyDuration, idleTimeout)
		}
	}

	return false, ""
}

// deleteStreamingDeployment deletes the Deployment for a streaming PipelineInstance
func (r *PipelineInstanceReconciler) deleteStreamingDeployment(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance) error {
	if pipelineInstance.Status.Streaming == nil || pipelineInstance.Status.Streaming.DeploymentName == "" {
		return nil
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pipelineInstance.Status.Streaming.DeploymentName,
			Namespace: pipelineInstance.Namespace,
		},
	}

	err := r.Delete(ctx, deployment)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete deployment: %w", err)
	}

	return nil
}

// buildRTSPURL constructs an RTSP URL from RTSPSource components without credentials
// Format: rtsp://host:port/path
func buildRTSPURL(rtspSource *pipelinesv1alpha1.RTSPSource) string {
	host := rtspSource.Host

	// Default port is 554 if not specified
	port := rtspSource.Port
	if port == 0 {
		port = 554
	}

	path := rtspSource.Path
	// Ensure path starts with / if it's not empty
	if path != "" && !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	return fmt.Sprintf("rtsp://%s:%d%s", host, port, path)
}

// buildRTSPURLWithCredentials constructs an RTSP URL with embedded credentials
// Format: rtsp://$_RTSP_USERNAME:$_RTSP_PASSWORD@host:port/path
// The credential env vars will be substituted at runtime by the container
func buildRTSPURLWithCredentials(rtspSource *pipelinesv1alpha1.RTSPSource) string {
	host := rtspSource.Host

	// Default port is 554 if not specified
	port := rtspSource.Port
	if port == 0 {
		port = 554
	}

	path := rtspSource.Path
	// Ensure path starts with / if it's not empty
	if path != "" && !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	// Use environment variable references that will be expanded at runtime
	return fmt.Sprintf("rtsp://$_RTSP_USERNAME:$_RTSP_PASSWORD@%s:%d%s", host, port, path)
}

// ensureFilterServices creates or updates Kubernetes Services for filters with exposed ports
func (r *PipelineInstanceReconciler) ensureFilterServices(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance, pipeline *pipelinesv1alpha1.Pipeline) error {
	log := logf.FromContext(ctx)

	if len(pipeline.Spec.Services) == 0 {
		return nil
	}

	// Group services by filter name to assign indices
	servicesByFilter := make(map[string][]pipelinesv1alpha1.ServicePort)
	for _, svc := range pipeline.Spec.Services {
		servicesByFilter[svc.Name] = append(servicesByFilter[svc.Name], svc)
	}

	// Create or update each service
	for filterName, services := range servicesByFilter {
		for idx, svcPort := range services {
			serviceName := fmt.Sprintf("%s-%s-%d", pipelineInstance.Name, filterName, idx)

			service := &corev1.Service{}
			err := r.Get(ctx, types.NamespacedName{Name: serviceName, Namespace: pipelineInstance.Namespace}, service)

			targetPort := svcPort.Port
			if svcPort.TargetPort != nil {
				targetPort = *svcPort.TargetPort
			}

			protocol := corev1.ProtocolTCP
			if svcPort.Protocol != "" {
				protocol = svcPort.Protocol
			}

			desiredService := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      serviceName,
					Namespace: pipelineInstance.Namespace,
					Labels: map[string]string{
						"app":              "pipeline-stream",
						"pipelineinstance": pipelineInstance.Name,
						"filter":           filterName,
					},
				},
				Spec: corev1.ServiceSpec{
					Selector: map[string]string{
						"app":              "pipeline-stream",
						"pipelineinstance": pipelineInstance.Name,
					},
					Ports: []corev1.ServicePort{
						{
							Name:       filterName,
							Port:       svcPort.Port,
							TargetPort: intstr.FromInt32(targetPort),
							Protocol:   protocol,
						},
					},
					Type: corev1.ServiceTypeClusterIP,
				},
			}

			if apierrors.IsNotFound(err) {
				// Create the Service
				log.Info("Creating filter service", "service", serviceName, "filter", filterName, "port", svcPort.Port)
				if err := controllerutil.SetControllerReference(pipelineInstance, desiredService, r.Scheme); err != nil {
					return fmt.Errorf("failed to set controller reference on service %s: %w", serviceName, err)
				}
				if err := r.Create(ctx, desiredService); err != nil {
					return fmt.Errorf("failed to create service %s: %w", serviceName, err)
				}
			} else if err != nil {
				return fmt.Errorf("failed to get service %s: %w", serviceName, err)
			} else {
				// Update existing Service using Patch to avoid conflicts
				patchBase := client.MergeFrom(service.DeepCopy())
				service.Spec = desiredService.Spec
				service.Labels = desiredService.Labels
				if err := r.Patch(ctx, service, patchBase); err != nil {
					return fmt.Errorf("failed to update service %s: %w", serviceName, err)
				}
			}
		}
	}

	return nil
}

// deleteFilterServices deletes all Services created for this PipelineInstance's filters
// Uses label selectors to find services, so it works even if the Pipeline is deleted
func (r *PipelineInstanceReconciler) deleteFilterServices(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance) error {
	log := logf.FromContext(ctx)

	// List all services with the pipelineinstance label
	serviceList := &corev1.ServiceList{}
	if err := r.List(ctx, serviceList,
		client.InNamespace(pipelineInstance.Namespace),
		client.MatchingLabels{"pipelineinstance": pipelineInstance.Name}); err != nil {
		return fmt.Errorf("failed to list services: %w", err)
	}

	// Delete each service found
	for _, service := range serviceList.Items {
		log.Info("Deleting filter service", "service", service.Name)
		if err := r.Delete(ctx, &service); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete service %s: %w", service.Name, err)
		}
	}

	return nil
}
