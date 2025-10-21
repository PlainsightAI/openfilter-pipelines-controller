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
func (r *PipelineRunReconciler) reconcileStreaming(ctx context.Context, pipelineRun *pipelinesv1alpha1.PipelineRun, pipeline *pipelinesv1alpha1.Pipeline) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Initialize streaming status if not already set
	if pipelineRun.Status.Streaming == nil {
		pipelineRun.Status.Streaming = &pipelinesv1alpha1.StreamingStatus{}
	}

	// Set start time if not already set
	if pipelineRun.Status.StartTime == nil {
		now := metav1.Now()
		pipelineRun.Status.StartTime = &now
		if err := r.Status().Update(ctx, pipelineRun); err != nil {
			log.Error(err, "Failed to set start time")
			return ctrl.Result{}, err
		}
	}

	// Handle deletion (finalizer cleanup)
	const finalizerName = "filter.plainsight.ai/streaming-cleanup"
	if !pipelineRun.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(pipelineRun, finalizerName) {
			// Delete the Deployment
			if err := r.deleteStreamingDeployment(ctx, pipelineRun); err != nil {
				log.Error(err, "Failed to delete streaming deployment")
				return ctrl.Result{}, err
			}

			// Remove finalizer
			controllerutil.RemoveFinalizer(pipelineRun, finalizerName)
			if err := r.Update(ctx, pipelineRun); err != nil {
				log.Error(err, "Failed to remove finalizer")
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(pipelineRun, finalizerName) {
		controllerutil.AddFinalizer(pipelineRun, finalizerName)
		if err := r.Update(ctx, pipelineRun); err != nil {
			log.Error(err, "Failed to add finalizer")
			return ctrl.Result{}, err
		}
	}

	// Step 1: Ensure Deployment exists
	if err := r.ensureStreamingDeployment(ctx, pipelineRun, pipeline); err != nil {
		log.Error(err, "Failed to ensure streaming deployment")
		r.setCondition(pipelineRun, ConditionTypeDegraded, metav1.ConditionTrue, "DeploymentCreationFailed", err.Error())
		if err := r.Status().Update(ctx, pipelineRun); err != nil {
			log.Error(err, "Failed to update status")
		}
		return ctrl.Result{}, err
	}

	// Step 2: Update streaming status from Deployment
	if err := r.updateStreamingStatus(ctx, pipelineRun); err != nil {
		log.Error(err, "Failed to update streaming status")
		// Don't fail reconciliation, just log and continue
	}

	// Step 3: Check for idle timeout
	if pipeline.Spec.Source.RTSP != nil && pipeline.Spec.Source.RTSP.IdleTimeout != nil {
		if shouldComplete, reason := r.checkIdleTimeout(ctx, pipelineRun, pipeline); shouldComplete {
			log.Info("Streaming run idle timeout reached, marking as complete", "reason", reason)
			r.setCondition(pipelineRun, ConditionTypeSucceeded, metav1.ConditionTrue, "IdleTimeout", reason)
			r.setCondition(pipelineRun, ConditionTypeProgressing, metav1.ConditionFalse, "IdleTimeout", reason)

			now := metav1.Now()
			pipelineRun.Status.CompletionTime = &now

			// Delete the Deployment
			if err := r.deleteStreamingDeployment(ctx, pipelineRun); err != nil {
				log.Error(err, "Failed to delete deployment after idle timeout")
			}

			if err := r.Status().Update(ctx, pipelineRun); err != nil {
				log.Error(err, "Failed to update status after idle timeout")
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
	}

	// Step 4: Update conditions based on deployment status
	if pipelineRun.Status.Streaming.ReadyReplicas > 0 {
		r.setCondition(pipelineRun, ConditionTypeProgressing, metav1.ConditionTrue, "Running", "Stream is processing")
		// Note: We don't set Available=True for streaming runs as they run indefinitely
	} else {
		r.setCondition(pipelineRun, ConditionTypeProgressing, metav1.ConditionTrue, "Starting", "Waiting for stream to become ready")
	}

	if err := r.Status().Update(ctx, pipelineRun); err != nil {
		log.Error(err, "Failed to update status")
		return ctrl.Result{}, err
	}

	// Requeue for periodic status updates
	return ctrl.Result{RequeueAfter: StatusUpdateInterval}, nil
}

// ensureStreamingDeployment creates or updates the Deployment for streaming mode
func (r *PipelineRunReconciler) ensureStreamingDeployment(ctx context.Context, pipelineRun *pipelinesv1alpha1.PipelineRun, pipeline *pipelinesv1alpha1.Pipeline) error {
	log := logf.FromContext(ctx)

	deploymentName := pipelineRun.Name + "-deploy"
	deployment := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: deploymentName, Namespace: pipelineRun.Namespace}, deployment)

	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to get deployment: %w", err)
	}

	// Build the desired Deployment spec
	desiredDeployment := r.buildStreamingDeployment(pipelineRun, pipeline, deploymentName)

	if apierrors.IsNotFound(err) {
		// Create the Deployment
		log.Info("Creating streaming deployment", "deployment", deploymentName)
		if err := controllerutil.SetControllerReference(pipelineRun, desiredDeployment, r.Scheme); err != nil {
			return fmt.Errorf("failed to set controller reference: %w", err)
		}
		if err := r.Create(ctx, desiredDeployment); err != nil {
			return fmt.Errorf("failed to create deployment: %w", err)
		}
		pipelineRun.Status.Streaming.DeploymentName = deploymentName
		return nil
	}

	// Update existing Deployment if needed
	deployment.Spec = desiredDeployment.Spec
	if err := r.Update(ctx, deployment); err != nil {
		return fmt.Errorf("failed to update deployment: %w", err)
	}
	pipelineRun.Status.Streaming.DeploymentName = deploymentName

	return nil
}

// buildStreamingDeployment constructs a Deployment for streaming mode
func (r *PipelineRunReconciler) buildStreamingDeployment(pipelineRun *pipelinesv1alpha1.PipelineRun, pipeline *pipelinesv1alpha1.Pipeline, deploymentName string) *appsv1.Deployment {
	replicas := int32(1)
	maxUnavailable := intstr.FromInt32(0)
	maxSurge := intstr.FromInt32(1)

	// Build filter containers
	var containers []corev1.Container
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

		// Add filter config as env vars with FILTER_ prefix
		var envVars []corev1.EnvVar
		for _, cfg := range filter.Config {
			envVars = append(envVars, corev1.EnvVar{
				Name:  "FILTER_" + strings.ToUpper(cfg.Name),
				Value: cfg.Value,
			})
		}

		// Add user-defined env vars
		envVars = append(envVars, filter.Env...)

		// Inject RTSP environment variables
		if pipeline.Spec.Source.RTSP != nil {
			// If credentials are provided, inject internal env vars for username/password
			// and build URL with embedded credentials
			if pipeline.Spec.Source.RTSP.CredentialsSecret != nil {
				secretName := pipeline.Spec.Source.RTSP.CredentialsSecret.Name
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
				rtspURL := buildRTSPURLWithCredentials(pipeline.Spec.Source.RTSP)
				envVars = append(envVars, corev1.EnvVar{
					Name:  "RTSP_URL",
					Value: rtspURL,
				})
			} else {
				// No credentials, build simple URL
				rtspURL := buildRTSPURL(pipeline.Spec.Source.RTSP)
				envVars = append(envVars, corev1.EnvVar{
					Name:  "RTSP_URL",
					Value: rtspURL,
				})
			}
		}

		container.Env = envVars

		if filter.Resources != nil {
			container.Resources = *filter.Resources
		}

		containers = append(containers, container)
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deploymentName,
			Namespace: pipelineRun.Namespace,
			Labels: map[string]string{
				"app":         "pipeline-stream",
				"pipelinerun": pipelineRun.Name,
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
					"app":         "pipeline-stream",
					"pipelinerun": pipelineRun.Name,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":         "pipeline-stream",
						"pipelinerun": pipelineRun.Name,
					},
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: "pipeline-exec",
					RestartPolicy:      corev1.RestartPolicyAlways,
					Containers:         containers,
				},
			},
		},
	}

	return deployment
}

// updateStreamingStatus updates the streaming status from the Deployment and Pods
func (r *PipelineRunReconciler) updateStreamingStatus(ctx context.Context, pipelineRun *pipelinesv1alpha1.PipelineRun) error {
	if pipelineRun.Status.Streaming == nil || pipelineRun.Status.Streaming.DeploymentName == "" {
		return nil
	}

	deployment := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: pipelineRun.Status.Streaming.DeploymentName, Namespace: pipelineRun.Namespace}, deployment)
	if err != nil {
		return fmt.Errorf("failed to get deployment: %w", err)
	}

	// Update replica counts
	pipelineRun.Status.Streaming.ReadyReplicas = deployment.Status.ReadyReplicas
	pipelineRun.Status.Streaming.UpdatedReplicas = deployment.Status.UpdatedReplicas
	pipelineRun.Status.Streaming.AvailableReplicas = deployment.Status.AvailableReplicas

	// Track if deployment just became ready
	if deployment.Status.ReadyReplicas > 0 && pipelineRun.Status.Streaming.LastReadyTime == nil {
		now := metav1.Now()
		pipelineRun.Status.Streaming.LastReadyTime = &now
	}

	// Count container restarts from pods
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.InNamespace(pipelineRun.Namespace), client.MatchingLabels{"pipelinerun": pipelineRun.Name}); err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	totalRestarts := int32(0)
	for _, pod := range podList.Items {
		for _, containerStatus := range pod.Status.ContainerStatuses {
			totalRestarts += containerStatus.RestartCount
		}
	}
	pipelineRun.Status.Streaming.ContainerRestarts = totalRestarts

	return nil
}

// checkIdleTimeout checks if the streaming run should complete due to idle timeout
func (r *PipelineRunReconciler) checkIdleTimeout(ctx context.Context, pipelineRun *pipelinesv1alpha1.PipelineRun, pipeline *pipelinesv1alpha1.Pipeline) (bool, string) {
	if pipeline.Spec.Source.RTSP == nil || pipeline.Spec.Source.RTSP.IdleTimeout == nil {
		return false, ""
	}

	// Ensure streaming status exists
	if pipelineRun.Status.Streaming == nil {
		return false, ""
	}

	idleTimeout := pipeline.Spec.Source.RTSP.IdleTimeout.Duration

	// Check if all replicas are unready
	if pipelineRun.Status.Streaming.ReadyReplicas > 0 {
		// Stream is ready, reset idle tracking
		return false, ""
	}

	// If LastReadyTime is set, check how long it's been unready
	if pipelineRun.Status.Streaming.LastReadyTime != nil {
		unreadyDuration := time.Since(pipelineRun.Status.Streaming.LastReadyTime.Time)
		if unreadyDuration >= idleTimeout {
			return true, fmt.Sprintf("Stream has been unready for %v (idle timeout: %v)", unreadyDuration, idleTimeout)
		}
	} else if pipelineRun.Status.StartTime != nil {
		// Never became ready, check time since start
		unreadyDuration := time.Since(pipelineRun.Status.StartTime.Time)
		if unreadyDuration >= idleTimeout {
			return true, fmt.Sprintf("Stream never became ready after %v (idle timeout: %v)", unreadyDuration, idleTimeout)
		}
	}

	return false, ""
}

// deleteStreamingDeployment deletes the Deployment for a streaming PipelineRun
func (r *PipelineRunReconciler) deleteStreamingDeployment(ctx context.Context, pipelineRun *pipelinesv1alpha1.PipelineRun) error {
	if pipelineRun.Status.Streaming == nil || pipelineRun.Status.Streaming.DeploymentName == "" {
		return nil
	}

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pipelineRun.Status.Streaming.DeploymentName,
			Namespace: pipelineRun.Namespace,
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
