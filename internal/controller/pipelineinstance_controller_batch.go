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
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
)

// reconcileBatch handles the batch (Job-based) reconciliation path
func (r *PipelineInstanceReconciler) reconcileBatch(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance, pipeline *pipelinesv1alpha1.Pipeline, pipelineSource *pipelinesv1alpha1.PipelineSource) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Ensure counts object exists before initialization
	if pipelineInstance.Status.Counts == nil {
		pipelineInstance.Status.Counts = &pipelinesv1alpha1.FileCounts{}
	}

	initialized, err := r.initializePipelineInstance(ctx, pipelineInstance, pipelineSource)
	if err != nil {
		log.Error(err, "Failed to initialize PipelineInstance")
		return ctrl.Result{}, err
	}
	if !initialized {
		// Initialization failed or determined no work is required (e.g., empty input).
		// In these cases, reconciliation is complete.
		return ctrl.Result{}, nil
	}

	// Step 1: Ensure Job exists
	if err := r.ensureJob(ctx, pipelineInstance, pipeline, pipelineSource); err != nil {
		log.Error(err, "Failed to ensure Job")
		r.setCondition(pipelineInstance, ConditionTypeDegraded, metav1.ConditionTrue, "JobCreationFailed", err.Error())
		if err := r.Status().Update(ctx, pipelineInstance); err != nil {
			log.Error(err, "Failed to update status")
		}
		return ctrl.Result{}, err
	}

	// Step 2: Handle completed pods (ACK/retry/DLQ)
	if err := r.handleCompletedPods(ctx, pipelineInstance); err != nil {
		log.Error(err, "Failed to handle completed pods")
		// Don't fail reconciliation, just log and continue
	}

	// Step 3: Run reclaimer for stale pending messages
	if err := r.runReclaimer(ctx, pipelineInstance); err != nil {
		log.Error(err, "Failed to run reclaimer")
		// Don't fail reconciliation, just log and continue
	}

	// Step 4: Update status from Valkey metrics
	r.updateStatus(ctx, pipelineInstance)

	// Detect job failure and mark the run as degraded
	if failedCond, err := r.checkFailure(ctx, pipelineInstance); err != nil {
		log.Error(err, "Failed to check job failure state")
	} else if failedCond != nil {
		failureReason := failedCond.Reason
		if failureReason == "" {
			failureReason = "JobFailed"
		}

		failureMessage := failedCond.Message
		if failureMessage == "" {
			failureMessage = fmt.Sprintf("Job %s failed", pipelineInstance.Status.JobName)
		} else {
			failureMessage = fmt.Sprintf("Job %s failed: %s", pipelineInstance.Status.JobName, failureMessage)
		}

		r.flushOutstandingWork(ctx, pipelineInstance, failureReason, failureMessage)

		log.Info("PipelineInstance marked as Degraded due to job failure", "job", pipelineInstance.Status.JobName, "reason", failureReason)
		r.setCondition(pipelineInstance, ConditionTypeDegraded, metav1.ConditionTrue, failureReason, failureMessage)
		r.setCondition(pipelineInstance, ConditionTypeProgressing, metav1.ConditionFalse, failureReason, failureMessage)
		r.setCondition(pipelineInstance, ConditionTypeSucceeded, metav1.ConditionFalse, failureReason, failureMessage)

		if pipelineInstance.Status.CompletionTime == nil {
			now := metav1.Now()
			pipelineInstance.Status.CompletionTime = &now
		}

		if err := r.Status().Update(ctx, pipelineInstance); err != nil {
			log.Error(err, "Failed to update status after marking run degraded")
			return ctrl.Result{}, err
		}

		// No further processing is required once the run has failed
		return ctrl.Result{}, nil
	}

	// Step 5: Check for completion
	isComplete, err := r.checkCompletion(ctx, pipelineInstance)
	if err != nil {
		log.Error(err, "Failed to check completion")
		return ctrl.Result{RequeueAfter: StatusUpdateInterval}, nil
	}

	if isComplete {
		log.Info("PipelineInstance completed successfully")
		r.setCondition(pipelineInstance, ConditionTypeSucceeded, metav1.ConditionTrue, "Completed", "All files processed")
		r.setCondition(pipelineInstance, ConditionTypeProgressing, metav1.ConditionFalse, "Completed", "Processing finished")

		if pipelineInstance.Status.CompletionTime == nil {
			now := metav1.Now()
			pipelineInstance.Status.CompletionTime = &now
		}

		if err := r.Status().Update(ctx, pipelineInstance); err != nil {
			log.Error(err, "Failed to update status")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Set Progressing condition
	r.setCondition(pipelineInstance, ConditionTypeProgressing, metav1.ConditionTrue, "Processing", "Pipeline is processing files")

	if err := r.Status().Update(ctx, pipelineInstance); err != nil {
		log.Error(err, "Failed to update status")
		return ctrl.Result{}, err
	}

	// Requeue for periodic status updates
	return ctrl.Result{RequeueAfter: StatusUpdateInterval}, nil
}

// initializePipelineInstance performs one-time setup for new PipelineInstance resources.
// It creates the Valkey stream and consumer group, enumerates source files, enqueues
// work items, and seeds status counters. The logic is safe to call multiple times;
// after successful initialization it becomes a no-op. On error it returns a
// non-nil error so the reconciliation loop can retry.
func (r *PipelineInstanceReconciler) initializePipelineInstance(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance, pipelineSource *pipelinesv1alpha1.PipelineSource) (bool, error) {
	log := logf.FromContext(ctx)

	// If StartTime is already set, initialization has completed previously.
	if pipelineInstance.Status.StartTime != nil {
		if pipelineInstance.Status.Counts != nil && pipelineInstance.Status.Counts.TotalFiles == 0 {
			// Nothing to process. Skip further work.
			return false, nil
		}
		return true, nil
	}

	if r.ValkeyClient == nil {
		err := fmt.Errorf("valkey client is not configured")
		r.setCondition(pipelineInstance, ConditionTypeDegraded, metav1.ConditionTrue, "QueueUnavailable", err.Error())
		r.setCondition(pipelineInstance, ConditionTypeProgressing, metav1.ConditionFalse, "QueueUnavailable", "PipelineInstance cannot start without queue connectivity")
		if statusErr := r.Status().Update(ctx, pipelineInstance); statusErr != nil {
			return false, statusErr
		}
		return false, err
	}

	// Ensure the stream and consumer group exist. This call is idempotent and safe on retries.
	if err := r.ValkeyClient.CreateStreamAndGroup(ctx, pipelineInstance.GetQueueStream(), pipelineInstance.GetQueueGroup()); err != nil {
		r.setCondition(pipelineInstance, ConditionTypeDegraded, metav1.ConditionTrue, "QueueInitializationFailed", err.Error())
		r.setCondition(pipelineInstance, ConditionTypeProgressing, metav1.ConditionFalse, "QueueInitializationFailed", "PipelineInstance cannot initialize its queue")
		if statusErr := r.Status().Update(ctx, pipelineInstance); statusErr != nil {
			return false, statusErr
		}
		return false, fmt.Errorf("failed to create stream and group: %w", err)
	}

	currentLength, err := r.ValkeyClient.GetStreamLength(ctx, pipelineInstance.GetQueueStream())
	if err != nil {
		r.setCondition(pipelineInstance, ConditionTypeDegraded, metav1.ConditionTrue, "QueueInspectionFailed", err.Error())
		r.setCondition(pipelineInstance, ConditionTypeProgressing, metav1.ConditionFalse, "QueueInspectionFailed", "PipelineInstance cannot inspect queue state")
		if statusErr := r.Status().Update(ctx, pipelineInstance); statusErr != nil {
			return false, statusErr
		}
		return false, fmt.Errorf("failed to get stream length: %w", err)
	}

	instanceID := pipelineInstance.GetInstanceID()

	if currentLength == 0 {
		accessKey, secretKey, credErr := r.getCredentials(ctx, pipelineInstance, pipelineSource)
		if credErr != nil {
			r.setCondition(pipelineInstance, ConditionTypeDegraded, metav1.ConditionTrue, "CredentialsError", credErr.Error())
			r.setCondition(pipelineInstance, ConditionTypeProgressing, metav1.ConditionFalse, "CredentialsError", "PipelineInstance cannot access object storage credentials")
			if statusErr := r.Status().Update(ctx, pipelineInstance); statusErr != nil {
				return false, statusErr
			}
			return false, fmt.Errorf("failed to get S3 credentials: %w", credErr)
		}

		files, listErr := r.listBucketFiles(ctx, pipelineSource, accessKey, secretKey)
		if listErr != nil {
			r.setCondition(pipelineInstance, ConditionTypeDegraded, metav1.ConditionTrue, "ListingFailed", listErr.Error())
			r.setCondition(pipelineInstance, ConditionTypeProgressing, metav1.ConditionFalse, "ListingFailed", "PipelineInstance cannot enumerate input files")
			if statusErr := r.Status().Update(ctx, pipelineInstance); statusErr != nil {
				return false, statusErr
			}
			return false, fmt.Errorf("failed to list bucket files: %w", listErr)
		}

		if len(files) == 0 {
			now := metav1.Now()
			pipelineInstance.Status.StartTime = &now
			pipelineInstance.Status.CompletionTime = &now
			pipelineInstance.Status.Counts.TotalFiles = 0
			pipelineInstance.Status.Counts.Queued = 0
			pipelineInstance.Status.Counts.Running = 0
			pipelineInstance.Status.Counts.Succeeded = 0
			pipelineInstance.Status.Counts.Failed = 0

			r.setCondition(pipelineInstance, ConditionTypeDegraded, metav1.ConditionTrue, "NoFilesFound", "No files found in input bucket")
			r.setCondition(pipelineInstance, ConditionTypeProgressing, metav1.ConditionFalse, "NoFilesFound", "PipelineInstance cannot start without input files")
			r.setCondition(pipelineInstance, ConditionTypeSucceeded, metav1.ConditionFalse, "NoFilesFound", "PipelineInstance did not process any files")

			if statusErr := r.Status().Update(ctx, pipelineInstance); statusErr != nil {
				return false, statusErr
			}

			log.Info("PipelineInstance has no files to process; marking as degraded", "pipelineInstance", pipelineInstance.Name)
			return false, nil
		}

		successful := int64(0)
		for _, file := range files {
			if _, enqueueErr := r.ValkeyClient.EnqueueFileWithAttempts(ctx, pipelineInstance.GetQueueStream(), instanceID, file, 0); enqueueErr != nil {
				log.Error(enqueueErr, "Failed to enqueue file", "file", file)
				continue
			}
			successful++
		}

		// Refresh the stream length to reflect any enqueued work items.
		currentLength, err = r.ValkeyClient.GetStreamLength(ctx, pipelineInstance.GetQueueStream())
		if err != nil {
			r.setCondition(pipelineInstance, ConditionTypeDegraded, metav1.ConditionTrue, "QueueInspectionFailed", err.Error())
			r.setCondition(pipelineInstance, ConditionTypeProgressing, metav1.ConditionFalse, "QueueInspectionFailed", "PipelineInstance cannot inspect queue state")
			if statusErr := r.Status().Update(ctx, pipelineInstance); statusErr != nil {
				return false, statusErr
			}
			return false, fmt.Errorf("failed to get stream length after enqueue: %w", err)
		}

		if currentLength == 0 || successful == 0 {
			err := fmt.Errorf("no files were successfully enqueued for processing")
			r.setCondition(pipelineInstance, ConditionTypeDegraded, metav1.ConditionTrue, "EnqueueFailed", err.Error())
			r.setCondition(pipelineInstance, ConditionTypeProgressing, metav1.ConditionFalse, "EnqueueFailed", "PipelineInstance could not enqueue files for processing")
			if statusErr := r.Status().Update(ctx, pipelineInstance); statusErr != nil {
				return false, statusErr
			}
			return false, err
		}
	}

	now := metav1.Now()
	pipelineInstance.Status.StartTime = &now
	pipelineInstance.Status.Counts.TotalFiles = currentLength
	pipelineInstance.Status.Counts.Queued = currentLength
	pipelineInstance.Status.Counts.Running = 0
	pipelineInstance.Status.Counts.Succeeded = 0
	pipelineInstance.Status.Counts.Failed = 0

	r.setCondition(pipelineInstance, ConditionTypeDegraded, metav1.ConditionFalse, "Initialized", "PipelineInstance initialized successfully")
	r.setCondition(pipelineInstance, ConditionTypeProgressing, metav1.ConditionTrue, "Initialized", "PipelineInstance initialized and files queued")
	r.setCondition(pipelineInstance, ConditionTypeSucceeded, metav1.ConditionFalse, "Initialized", "PipelineInstance is processing input files")

	if statusErr := r.Status().Update(ctx, pipelineInstance); statusErr != nil {
		return false, statusErr
	}

	log.Info("PipelineInstance initialized", "pipelineInstance", pipelineInstance.Name, "totalFiles", pipelineInstance.Status.Counts.TotalFiles)
	return true, nil
}

// ensureJob creates or updates the Job for the PipelineInstance
func (r *PipelineInstanceReconciler) ensureJob(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance, pipeline *pipelinesv1alpha1.Pipeline, pipelineSource *pipelinesv1alpha1.PipelineSource) error {
	log := logf.FromContext(ctx)

	// Check if Job already exists
	if pipelineInstance.Status.JobName != "" {
		job := &batchv1.Job{}
		err := r.Get(ctx, types.NamespacedName{
			Name:      pipelineInstance.Status.JobName,
			Namespace: pipelineInstance.Namespace,
		}, job)

		if err == nil {
			// Job exists
			return nil
		}

		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to get job: %w", err)
		}
		// Job was deleted, create a new one
	}

	// Ensure per-namespace Valkey credentials exist in the target namespace
	if err := r.ensureNamespaceValkeyCredentials(ctx, pipelineInstance.Namespace); err != nil {
		return fmt.Errorf("failed to ensure namespace Valkey credentials: %w", err)
	}

	// Generate Job name from PipelineInstance name
	jobName := fmt.Sprintf("%s-job", pipelineInstance.Name)

	// Build the Job spec
	job := r.buildJob(ctx, pipelineInstance, pipeline, pipelineSource, jobName)

	// Set PipelineInstance as owner of the Job
	if err := controllerutil.SetControllerReference(pipelineInstance, job, r.Scheme); err != nil {
		return fmt.Errorf("failed to set owner reference: %w", err)
	}

	// Create the Job
	if err := r.Create(ctx, job); err != nil {
		return fmt.Errorf("failed to create job: %w", err)
	}

	log.Info("Created Job for PipelineInstance", "job", jobName)

	// Update status with Job name
	pipelineInstance.Status.JobName = jobName
	return nil
}

// buildJob constructs the Job specification for the PipelineInstance
func (r *PipelineInstanceReconciler) buildJob(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance, pipeline *pipelinesv1alpha1.Pipeline, pipelineSource *pipelinesv1alpha1.PipelineSource, jobName string) *batchv1.Job {
	log := logf.FromContext(ctx)

	// Get instance ID from the PipelineInstance UID
	instanceID := pipelineInstance.GetInstanceID()

	// Get execution config with defaults
	parallelism := int32(1)
	if pipelineInstance.Spec.Execution != nil && pipelineInstance.Spec.Execution.Parallelism != nil {
		parallelism = *pipelineInstance.Spec.Execution.Parallelism
	}

	// Get total files count, default to 0 if not set
	totalFiles := int64(0)
	if pipelineInstance.Status.Counts != nil {
		totalFiles = pipelineInstance.Status.Counts.TotalFiles
	}

	log.V(1).Info("Building job", "totalFiles", totalFiles, "parallelism", parallelism)

	// Get S3 credentials from PipelineSource
	var s3SecretName string
	if pipelineSource.Spec.Bucket != nil && pipelineSource.Spec.Bucket.CredentialsSecret != nil {
		s3SecretName = pipelineSource.Spec.Bucket.CredentialsSecret.Name
	}

	// Build claimer init container env vars
	claimerEnv := []corev1.EnvVar{
		{Name: "STREAM", Value: pipelineInstance.GetQueueStream()},
		{Name: "GROUP", Value: pipelineInstance.GetQueueGroup()},
		{Name: "VALKEY_URL", Value: r.ValkeyAddr},
		{Name: "CONSUMER_NAME", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
		}},
		{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
		}},
		{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
		}},
	}

	// Add S3 config if bucket source is configured
	if pipelineSource.Spec.Bucket != nil {
		claimerEnv = append(claimerEnv, []corev1.EnvVar{
			{Name: "S3_BUCKET", Value: pipelineSource.Spec.Bucket.Name},
			{Name: "S3_ENDPOINT", Value: pipelineSource.Spec.Bucket.Endpoint},
			{Name: "S3_REGION", Value: pipelineSource.Spec.Bucket.Region},
			{Name: "S3_USE_PATH_STYLE", Value: fmt.Sprintf("%t", pipelineSource.Spec.Bucket.UsePathStyle)},
			{Name: "S3_INSECURE_SKIP_TLS_VERIFY", Value: fmt.Sprintf("%t", pipelineSource.Spec.Bucket.InsecureSkipTLSVerify)},
		}...)
	}

	// Add S3 credentials if secret is specified
	if s3SecretName != "" {
		claimerEnv = append(claimerEnv, []corev1.EnvVar{
			{Name: "S3_ACCESS_KEY_ID", ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: s3SecretName},
					Key:                  "accessKeyId",
				},
			}},
			{Name: "S3_SECRET_ACCESS_KEY", ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: s3SecretName},
					Key:                  "secretAccessKey",
				},
			}},
		}...)
	}

	// Add Valkey credentials from per-namespace secret (managed by ensureNamespaceValkeyCredentials)
	nsSecretName := r.ValkeyNSSecretName
	if nsSecretName == "" {
		nsSecretName = DefaultValkeyNSSecretName
	}
	claimerEnv = append(claimerEnv,
		corev1.EnvVar{
			Name: "VALKEY_USERNAME",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: nsSecretName},
					Key:                  "valkey-username",
				},
			},
		},
		corev1.EnvVar{
			Name: "VALKEY_PASSWORD",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: nsSecretName},
					Key:                  "valkey-password",
				},
			},
		},
	)

	// Provide video input path to claimer for storing downloaded files.
	videoInputPath := pipeline.Spec.VideoInputPath
	if videoInputPath == "" {
		videoInputPath = DefaultVideoInputPath
	}
	claimerEnv = append(claimerEnv, corev1.EnvVar{
		Name:  "VIDEO_INPUT_PATH",
		Value: videoInputPath,
	})

	// Build filter containers from Pipeline spec
	filterContainers := make([]corev1.Container, 0, len(pipeline.Spec.Filters))

	// Add user-defined filters
	for _, filter := range pipeline.Spec.Filters {
		// Build config environment variables from filter.Config
		configEnvVars := make([]corev1.EnvVar, 0, len(filter.Config))
		for _, cfg := range filter.Config {
			// Convert name to uppercase and prefix with FILTER_
			envName := "FILTER_" + strings.ToUpper(cfg.Name)
			configEnvVars = append(configEnvVars, corev1.EnvVar{
				Name:  envName,
				Value: cfg.Value,
			})
		}

		// Start with config env vars, then add filter-specific env vars
		// Filter-specific env vars can override config if they have the same name
		containerEnv := make([]corev1.EnvVar, 0, len(configEnvVars)+len(filter.Env))
		containerEnv = append(containerEnv, configEnvVars...)

		// Inject GPU env vars before user env vars so users can override if needed.
		// Skipped when the respective path field is empty (e.g. on EKS).
		if filter.Resources != nil && containerResourcesRequireGPU(*filter.Resources) {
			if r.GPULibraryPath != "" {
				containerEnv = append(containerEnv,
					corev1.EnvVar{Name: ldLibraryPathEnvName, Value: r.GPULibraryPath},
				)
				log.V(1).Info("GPU resources detected, injecting LD_LIBRARY_PATH", "filter", filter.Name, "value", r.GPULibraryPath)
			}
			if r.GPUBinPath != "" {
				containerEnv = append(containerEnv,
					corev1.EnvVar{Name: appendPathEnvName, Value: r.GPUBinPath},
				)
				log.V(1).Info("GPU resources detected, injecting OPENFILTER_APPEND_PATH", "filter", filter.Name, "value", r.GPUBinPath)
			}
		}

		containerEnv = append(containerEnv, filter.Env...)

		container := corev1.Container{
			Name:            filter.Name,
			Image:           filter.Image,
			Command:         filter.Command,
			Args:            filter.Args,
			Env:             containerEnv,
			ImagePullPolicy: filter.ImagePullPolicy,
			VolumeMounts: []corev1.VolumeMount{
				{Name: "workspace", MountPath: "/ws"},
			},
		}

		if filter.Resources != nil {
			container.Resources = *filter.Resources
		}

		filterContainers = append(filterContainers, container)
	}

	// Apply configured GPU node selector labels when any container requests nvidia.com/gpu resources.
	// Copy the map to prevent downstream mutation from corrupting the reconciler's shared state.
	var nodeSelector map[string]string
	if len(r.GPUNodeSelectorLabels) > 0 && requiresGPU(filterContainers) {
		nodeSelector = make(map[string]string, len(r.GPUNodeSelectorLabels))
		for k, v := range r.GPUNodeSelectorLabels {
			nodeSelector[k] = v
		}
		log.V(1).Info("GPU resources detected, applying GPU node selector", "pipelineInstance", pipelineInstance.Name, "nodeSelector", nodeSelector)
	}

	// Build Job spec
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: pipelineInstance.Namespace,
			Labels:    buildPodLabels(instanceID, pipelineInstance),
		},
		Spec: batchv1.JobSpec{
			CompletionMode:          ptr.To(batchv1.NonIndexedCompletion),
			Completions:             ptr.To(int32(totalFiles)),
			Parallelism:             ptr.To(parallelism),
			BackoffLimit:            ptr.To(int32(2)),
			TTLSecondsAfterFinished: ptr.To(int32(86400)), // 24 hours
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: buildPodLabels(instanceID, pipelineInstance),
				},
				Spec: corev1.PodSpec{
					// No special ServiceAccount required; default SA is sufficient
					RestartPolicy:    corev1.RestartPolicyNever,
					NodeSelector:     nodeSelector,
					ImagePullSecrets: pipeline.Spec.ImagePullSecrets,
					Volumes: []corev1.Volume{
						{
							Name: "workspace",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
					InitContainers: []corev1.Container{
						{
							Name:  "claimer",
							Image: r.ClaimerImage,
							Env:   claimerEnv,
							VolumeMounts: []corev1.VolumeMount{
								{Name: "workspace", MountPath: "/ws"},
							},
						},
					},
					Containers: filterContainers,
				},
			},
		},
	}

	log.Info("Built Job spec", "job", jobName, "completions", totalFiles, "parallelism", parallelism)
	return job
}

// handleCompletedPods processes pods that have completed (succeeded or failed)
func (r *PipelineInstanceReconciler) handleCompletedPods(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance) error {
	log := logf.FromContext(ctx)

	// List all pods for this PipelineInstance
	podList := &corev1.PodList{}
	instanceID := pipelineInstance.GetInstanceID()
	if err := r.List(ctx, podList, client.InNamespace(pipelineInstance.Namespace), client.MatchingLabels{
		"filter.plainsight.ai/instance": instanceID,
	}); err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	maxAttempts := int32(3)
	if pipelineInstance.Spec.Execution != nil && pipelineInstance.Spec.Execution.MaxAttempts != nil {
		maxAttempts = *pipelineInstance.Spec.Execution.MaxAttempts
	}

	for _, pod := range podList.Items {
		// Only process completed pods or pods with crashed containers
		startFailureReason, hasStartFailure := detectPodStartFailure(&pod)
		crashReason, hasCrash := detectContainerCrash(&pod)
		if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed && !hasStartFailure && !hasCrash {
			log.V(1).Info("Skipping pod that is not complete", "pod", pod.Name, "phase", pod.Status.Phase)
			continue
		}

		// Discover the message claimed by this pod's init container using the consumer name = pod.Name.
		// There should be at most one pending entry for this consumer.
		pendingIDs, err := r.ValkeyClient.GetPendingForConsumer(ctx, pipelineInstance.GetQueueStream(), pipelineInstance.GetQueueGroup(), pod.Name, 1)
		if err != nil {
			log.Error(err, "Failed to get pending entries for consumer", "pod", pod.Name)
			continue
		}

		if len(pendingIDs) == 0 {
			if pod.Status.Phase == corev1.PodSucceeded {
				// The pod completed successfully but its pending entry is gone. This can happen
				// in a narrow window where the pod transitions to Succeeded after runReclaimer
				// already built its pod-state snapshot but before processing this message.
				// In that case the reclaimer may have re-enqueued the file; otherwise it cleaned
				// up via the succeeded-pod path with no duplicate run.
				log.Info("Succeeded pod has no pending entry; message was likely reclaimed while pod was still running", "pod", pod.Name)
			} else {
				log.V(1).Info("No pending entries for consumer", "pod", pod.Name)
			}
			continue
		}

		messageID := pendingIDs[0]
		// Fetch message fields
		msgs, err := r.ValkeyClient.ReadRange(ctx, pipelineInstance.GetQueueStream(), messageID, messageID, 1)
		if err != nil {
			log.Error(err, "Failed to read message for pending entry", "messageID", messageID)
			continue
		}
		if len(msgs) == 0 {
			// Message was deleted from the stream but the pending entry remains; ACK to clean up.
			log.Info("Message no longer exists in stream; ACKing orphan pending entry", "messageID", messageID)
			if ackErr := r.ValkeyClient.AckMessage(ctx, pipelineInstance.GetQueueStream(), pipelineInstance.GetQueueGroup(), messageID); ackErr != nil {
				log.Error(ackErr, "Failed to ACK orphan pending entry", "messageID", messageID)
			}
			continue
		}

		filepath := msgs[0].Values["file"]
		attempts := parseAttempts(msgs[0].Values["attempts"], 0)

		if pod.Status.Phase == corev1.PodSucceeded {
			// ACK the message
			if err := r.ValkeyClient.AckMessage(ctx, pipelineInstance.GetQueueStream(), pipelineInstance.GetQueueGroup(), messageID); err != nil {
				log.Error(err, "Failed to ACK message", "messageID", messageID)
				continue
			}
			log.Info("ACKed successful message", "pod", pod.Name, "file", filepath)

		} else if pod.Status.Phase == corev1.PodFailed || hasStartFailure || hasCrash {
			r.handleFailedPodMessage(ctx, pipelineInstance, &pod, messageID, filepath, attempts, maxAttempts, podFailureInfo{
				startFailureReason: startFailureReason,
				crashReason:        crashReason,
				hasStartFailure:    hasStartFailure,
				hasCrash:           hasCrash,
			})
		}

		// No need to mark pod as processed; we rely on queue state to avoid reprocessing.
	}

	return nil
}

// podFailureInfo groups crash and start-failure detection results for a pod.
type podFailureInfo struct {
	startFailureReason string
	crashReason        string
	hasStartFailure    bool
	hasCrash           bool
}

// handleFailedPodMessage handles a pod that has failed, crashed, or had a start failure.
// It re-enqueues the file or moves it to the DLQ, ACKs the original message, and optionally
// deletes the pod so the Job controller can schedule a replacement.
func (r *PipelineInstanceReconciler) handleFailedPodMessage(
	ctx context.Context,
	pipelineInstance *pipelinesv1alpha1.PipelineInstance,
	pod *corev1.Pod,
	messageID, filepath string,
	attempts int,
	maxAttempts int32,
	info podFailureInfo,
) {
	log := logf.FromContext(ctx)
	instanceID := pipelineInstance.GetInstanceID()
	dlqKey := pipelineInstance.GetQueueDLQ()

	reason := resolveFailureReason(pod, info.startFailureReason, info.crashReason)
	attempts++

	requeueOK := false
	if attempts < int(maxAttempts) {
		// Re-enqueue with incremented attempts
		if _, err := r.ValkeyClient.EnqueueFileWithAttempts(ctx, pipelineInstance.GetQueueStream(), instanceID, filepath, attempts); err != nil {
			log.Error(err, "Failed to re-enqueue file", "file", filepath)
		} else {
			log.Info("Re-enqueued failed file", "file", filepath, "attempts", attempts)
			requeueOK = true
		}
	} else {
		// Add to DLQ
		if err := r.ValkeyClient.AddToDLQ(ctx, dlqKey, instanceID, filepath, attempts, reason); err != nil {
			log.Error(err, "Failed to add to DLQ", "file", filepath)
		} else {
			log.Info("Added file to DLQ", "file", filepath, "reason", reason)
			requeueOK = true
		}
	}

	// Only ACK the original message if the re-enqueue or DLQ write succeeded.
	// Skipping ACK on failure keeps the message pending so it can be retried on the next reconcile.
	if !requeueOK {
		log.Error(nil, "Skipping ACK — re-enqueue/DLQ write failed; message will be retried on next reconcile", "file", filepath, "messageID", messageID)
		return
	}

	// ACK the original message
	if err := r.ValkeyClient.AckMessage(ctx, pipelineInstance.GetQueueStream(), pipelineInstance.GetQueueGroup(), messageID); err != nil {
		log.Error(err, "Failed to ACK failed message", "messageID", messageID)
		return
	}

	if info.hasStartFailure || info.hasCrash {
		// Delete the pod to allow the Job controller to schedule a replacement.
		// This relies on RestartPolicy: Never — with that policy Kubernetes will not
		// restart the pod automatically, so deletion is required to trigger a new pod.
		// If the policy were OnFailure, the kubelet would handle restarts and deleting
		// the pod would be counterproductive.
		if err := r.Delete(ctx, pod); err != nil {
			log.Error(err, "Failed to delete pod after failure", "pod", pod.Name)
		} else {
			log.Info("Deleted pod after failure", "pod", pod.Name, "reason", reason)
		}
	}
}

// resolveFailureReason returns a human-readable failure reason for a pod.
// It prefers explicit start-failure and crash reasons, then falls back to inspecting
// terminated container exit codes, and finally returns "Unknown".
func resolveFailureReason(pod *corev1.Pod, startFailureReason, crashReason string) string {
	if startFailureReason != "" {
		return startFailureReason
	}
	if crashReason != "" {
		return crashReason
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 {
			return fmt.Sprintf("Container %s exited with code %d", cs.Name, cs.State.Terminated.ExitCode)
		}
	}
	return "Unknown"
}

// ldLibraryPathEnvName is injected into GPU containers so they can find NVIDIA libraries.
const ldLibraryPathEnvName = "LD_LIBRARY_PATH"

// appendPathEnvName is read by the OpenFilter runtime to append to the container's PATH.
const appendPathEnvName = "OPENFILTER_APPEND_PATH"

// containerResourcesRequireGPU returns true if the given resource requirements request a positive
// quantity of nvidia.com/gpu. Zero and negative quantities return false.
func containerResourcesRequireGPU(resources corev1.ResourceRequirements) bool {
	if q, ok := resources.Limits["nvidia.com/gpu"]; ok && q.Sign() > 0 {
		return true
	}
	if q, ok := resources.Requests["nvidia.com/gpu"]; ok && q.Sign() > 0 {
		return true
	}
	return false
}

// requiresGPU returns true if any container in the slice requests a positive quantity of nvidia.com/gpu.
func requiresGPU(containers []corev1.Container) bool {
	for _, c := range containers {
		if containerResourcesRequireGPU(c.Resources) {
			return true
		}
	}
	return false
}

// detectPodStartFailure returns a descriptive reason when any container in the pod is unable to start.
func detectPodStartFailure(pod *corev1.Pod) (string, bool) {
	reason := detectContainerStartFailure(pod.Status.ContainerStatuses)
	if reason != "" {
		return reason, true
	}
	reason = detectContainerStartFailure(pod.Status.InitContainerStatuses)
	if reason != "" {
		return reason, true
	}
	return "", false
}

func detectContainerStartFailure(statuses []corev1.ContainerStatus) string {
	for _, status := range statuses {
		waiting := status.State.Waiting
		if waiting == nil {
			continue
		}

		switch waiting.Reason {
		case "ImagePullBackOff", "ErrImagePull", "InvalidImageName", "RegistryUnavailable":
			message := waiting.Message
			if message == "" {
				message = waiting.Reason
			}
			return fmt.Sprintf("Container %s image pull failed: %s", status.Name, message)
		case "CreateContainerConfigError", "CreateContainerError":
			message := waiting.Message
			if message == "" {
				message = waiting.Reason
			}
			return fmt.Sprintf("Container %s configuration error: %s", status.Name, message)
		case "CrashLoopBackOff":
			if status.LastTerminationState.Terminated != nil {
				terminated := status.LastTerminationState.Terminated
				return fmt.Sprintf("Container %s crashlooped with exit code %d: %s", status.Name, terminated.ExitCode, terminated.Reason)
			}
			message := waiting.Message
			if message == "" {
				message = waiting.Reason
			}
			return fmt.Sprintf("Container %s crashlooped: %s", status.Name, message)
		}
	}
	return ""
}

// detectContainerCrash checks whether any non-init container in a Running pod has terminated
// with a non-zero exit code. This catches the case where one container (e.g. huggingface-vision)
// crashes but the pod phase stays "Running" because another container (e.g. video-in) is still alive.
func detectContainerCrash(pod *corev1.Pod) (string, bool) {
	if pod.Status.Phase != corev1.PodRunning {
		return "", false
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 {
			return fmt.Sprintf("Container %s crashed with exit code %d while pod still running",
				cs.Name, cs.State.Terminated.ExitCode), true
		}
	}
	return "", false
}

// runReclaimer recovers stale pending messages from consumers whose pods are no longer running.
// It uses XPENDING to discover idle entries and their owning consumers, then checks whether
// each consumer's pod is still alive before reclaiming. This prevents stealing messages from
// pods that are still actively processing long-running work.
func (r *PipelineInstanceReconciler) runReclaimer(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance) error {
	log := logf.FromContext(ctx)

	pendingTimeout := 15 * time.Minute
	if pipelineInstance.Spec.Execution != nil && pipelineInstance.Spec.Execution.PendingTimeout != nil {
		pendingTimeout = pipelineInstance.Spec.Execution.PendingTimeout.Duration
	}

	minIdleTime := pendingTimeout.Milliseconds()
	consumerName := "controller-reclaimer"
	streamKey := pipelineInstance.GetQueueStream()
	groupName := pipelineInstance.GetQueueGroup()

	// Get pending entries that have been idle longer than the timeout, along with their consumer names.
	pendingEntries, err := r.ValkeyClient.GetPendingEntryDetails(ctx, streamKey, groupName, minIdleTime, 100)
	if err != nil {
		return fmt.Errorf("failed to get pending entry details: %w", err)
	}

	if len(pendingEntries) == 0 {
		return nil
	}

	// Build sets of running and succeeded pod names for this instance.
	// Running/Pending pods are skipped entirely (message still being processed).
	// Succeeded pods get their messages ACKed without re-enqueueing (work already done).
	// If the pod list fails, abort entirely — proceeding without pod-awareness would
	// reproduce the exact race condition this function is designed to prevent.
	runningPods := make(map[string]bool)
	succeededPods := make(map[string]bool)
	podList := &corev1.PodList{}
	instanceID := pipelineInstance.GetInstanceID()
	if err := r.List(ctx, podList, client.InNamespace(pipelineInstance.Namespace), client.MatchingLabels{
		"filter.plainsight.ai/instance": instanceID,
	}); err != nil {
		return fmt.Errorf("failed to list pods for reclaimer: %w", err)
	}
	for _, pod := range podList.Items {
		switch pod.Status.Phase {
		case corev1.PodRunning, corev1.PodPending:
			// A Running pod with a crashed container is effectively dead — don't
			// protect its message from reclamation.
			if _, crashed := detectContainerCrash(&pod); !crashed {
				runningPods[pod.Name] = true
			}
		case corev1.PodSucceeded:
			succeededPods[pod.Name] = true
		}
	}

	// Filter entries: only reclaim from consumers whose pods are not running.
	// Track which consumer owns each message so we can handle succeeded pods specially.
	reclaimIDs := make([]string, 0, len(pendingEntries))
	reclaimConsumers := make(map[string]string, len(pendingEntries)) // messageID -> consumer
	for _, entry := range pendingEntries {
		if runningPods[entry.Consumer] {
			log.V(1).Info("Skipping reclaim for message owned by running pod", "messageID", entry.ID, "consumer", entry.Consumer, "idleMs", entry.IdleMs)
			continue
		}
		reclaimIDs = append(reclaimIDs, entry.ID)
		reclaimConsumers[entry.ID] = entry.Consumer
	}

	if len(reclaimIDs) == 0 {
		return nil
	}

	// Use XCLAIM to transfer only the filtered messages to the reclaimer consumer.
	messages, err := r.ValkeyClient.ClaimMessages(ctx, streamKey, groupName, consumerName, minIdleTime, reclaimIDs...)
	if err != nil {
		return fmt.Errorf("failed to claim messages: %w", err)
	}

	log.Info("Reclaimed stale messages", "claimed", len(messages), "attempted", len(reclaimIDs))

	maxAttempts := int32(3)
	if pipelineInstance.Spec.Execution != nil && pipelineInstance.Spec.Execution.MaxAttempts != nil {
		maxAttempts = *pipelineInstance.Spec.Execution.MaxAttempts
	}

	dlqKey := pipelineInstance.GetQueueDLQ()

	for _, msg := range messages {
		filepath := msg.Values["file"]
		entryConsumer := reclaimConsumers[msg.ID]

		// If the consumer's pod succeeded, the work is already done.
		// Just ACK+XDEL without re-enqueueing to avoid creating orphan duplicates.
		if succeededPods[entryConsumer] {
			if err := r.ValkeyClient.AckMessage(ctx, streamKey, groupName, msg.ID); err != nil {
				log.Error(err, "Failed to ACK message for succeeded pod", "messageID", msg.ID)
				continue
			}
			if err := r.ValkeyClient.DeleteMessages(ctx, streamKey, msg.ID); err != nil {
				log.Error(err, "Failed to delete message for succeeded pod from stream", "messageID", msg.ID)
			}
			log.Info("Cleaned up stale message for succeeded pod", "consumer", entryConsumer, "file", filepath)
			continue
		}

		// Use 0 as fallback for unparseable attempts so reclaimed messages get a fresh retry
		// budget rather than being sent to DLQ immediately.
		attempts := parseAttempts(msg.Values["attempts"], 0)
		attempts++

		var processingErr error
		if attempts < int(maxAttempts) {
			if _, err := r.ValkeyClient.EnqueueFileWithAttempts(ctx, streamKey, instanceID, filepath, attempts); err != nil {
				log.Error(err, "Failed to re-enqueue reclaimed file; will retry on next cycle", "file", filepath)
				processingErr = err
			}
		} else {
			reason := "Max attempts exceeded (reclaimed stale message)"
			if err := r.ValkeyClient.AddToDLQ(ctx, dlqKey, instanceID, filepath, attempts, reason); err != nil {
				log.Error(err, "Failed to add reclaimed file to DLQ; will retry on next cycle", "file", filepath)
				processingErr = err
			}
		}

		if processingErr != nil {
			continue // Do not ACK — message stays pending for next reconciliation cycle
		}

		if err := r.ValkeyClient.AckMessage(ctx, streamKey, groupName, msg.ID); err != nil {
			log.Error(err, "Failed to ACK reclaimed message", "messageID", msg.ID)
			continue
		}
		if err := r.ValkeyClient.DeleteMessages(ctx, streamKey, msg.ID); err != nil {
			log.Error(err, "Failed to delete reclaimed message from stream", "messageID", msg.ID)
		}
	}

	return nil
}

// updateStatus updates the PipelineInstance status from Valkey metrics
func (r *PipelineInstanceReconciler) updateStatus(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance) {
	log := logf.FromContext(ctx)

	if pipelineInstance.Status.Counts == nil {
		pipelineInstance.Status.Counts = &pipelinesv1alpha1.FileCounts{}
	}

	// Get queued count (consumer group lag - messages not yet read)
	queued, err := r.ValkeyClient.GetConsumerGroupLag(ctx, pipelineInstance.GetQueueStream(), pipelineInstance.GetQueueGroup())
	if err != nil {
		log.Error(err, "Failed to get consumer group lag")
		queued = 0
	}

	// Get running count (pending messages - read but not acknowledged)
	running, err := r.ValkeyClient.GetPendingCount(ctx, pipelineInstance.GetQueueStream(), pipelineInstance.GetQueueGroup())
	if err != nil {
		log.Error(err, "Failed to get pending count")
		running = 0
	}

	// Get failed count (DLQ length)
	dlqKey := pipelineInstance.GetQueueDLQ()
	failed, err := r.ValkeyClient.GetStreamLength(ctx, dlqKey)
	if err != nil {
		log.Error(err, "Failed to get DLQ length")
		failed = 0
	}

	// Calculate succeeded count
	totalFiles := pipelineInstance.Status.Counts.TotalFiles
	succeeded := totalFiles - queued - running - failed
	if succeeded < 0 {
		succeeded = 0
	}

	// Update counts
	pipelineInstance.Status.Counts.Queued = queued
	pipelineInstance.Status.Counts.Running = running
	pipelineInstance.Status.Counts.Succeeded = succeeded
	pipelineInstance.Status.Counts.Failed = failed

	log.V(1).Info("Updated status counts", "queued", queued, "running", running, "succeeded", succeeded, "failed", failed)
}

// checkCompletion determines if the PipelineInstance has completed
func (r *PipelineInstanceReconciler) checkCompletion(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance) (bool, error) {
	if pipelineInstance.Status.Counts == nil {
		return false, nil
	}

	queued := pipelineInstance.Status.Counts.Queued
	running := pipelineInstance.Status.Counts.Running

	if queued == 0 && running == 0 {
		if pipelineInstance.Status.JobName != "" {
			job := &batchv1.Job{}
			err := r.Get(ctx, types.NamespacedName{
				Name:      pipelineInstance.Status.JobName,
				Namespace: pipelineInstance.Namespace,
			}, job)

			if err != nil {
				return false, fmt.Errorf("failed to get job: %w", err)
			}

			for _, condition := range job.Status.Conditions {
				if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
					return true, nil
				}
			}
		}
	}

	return false, nil
}

// checkFailure inspects the backing Job for failure conditions.
func (r *PipelineInstanceReconciler) checkFailure(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance) (*batchv1.JobCondition, error) {
	if pipelineInstance.Status.JobName == "" {
		return nil, nil
	}

	job := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{
		Name:      pipelineInstance.Status.JobName,
		Namespace: pipelineInstance.Namespace,
	}, job)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Job was deleted; treat as no failure condition yet.
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	for i := range job.Status.Conditions {
		cond := &job.Status.Conditions[i]
		if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue {
			return cond, nil
		}
	}

	return nil, nil
}

// flushOutstandingWork moves any remaining stream entries to the DLQ when a run can no longer progress.
func (r *PipelineInstanceReconciler) flushOutstandingWork(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance, reason, message string) {
	log := logf.FromContext(ctx)

	streamKey := pipelineInstance.GetQueueStream()
	groupName := pipelineInstance.GetQueueGroup()
	instanceID := pipelineInstance.GetInstanceID()
	dlqKey := pipelineInstance.GetQueueDLQ()

	maxAttempts := int32(3)
	if pipelineInstance.Spec.Execution != nil && pipelineInstance.Spec.Execution.MaxAttempts != nil {
		maxAttempts = *pipelineInstance.Spec.Execution.MaxAttempts
	}

	fileAttempts := make(map[string]int)
	existingAttempts := make(map[string]int)
	existingIDs := make(map[string][]string)
	if existing, err := r.ValkeyClient.ReadRange(ctx, dlqKey, "-", "+", 0); err != nil {
		log.Error(err, "Failed to read existing DLQ entries during flush")
	} else {
		for _, msg := range existing {
			file := msg.Values["file"]
			attempts := parseAttempts(msg.Values["attempts"], int(maxAttempts))
			if attempts > existingAttempts[file] {
				existingAttempts[file] = attempts
			}
			existingIDs[file] = append(existingIDs[file], msg.ID)
		}
	}

	// First, reclaim pending messages so we can safely ack and delete them.
	for {
		messages, err := r.ValkeyClient.AutoClaim(ctx, streamKey, groupName, "controller-flush", 0, 100)
		if err != nil {
			log.Error(err, "Failed to auto-claim pending messages during flush")
			break
		}
		if len(messages) == 0 {
			break
		}

		for _, msg := range messages {
			file := msg.Values["file"]
			attempts := parseAttempts(msg.Values["attempts"], int(maxAttempts))
			if file == "" {
				file = "<unknown>"
			}
			if attempts > fileAttempts[file] {
				fileAttempts[file] = attempts
			}

			if err := r.ValkeyClient.AckMessage(ctx, streamKey, groupName, msg.ID); err != nil {
				log.Error(err, "Failed to ack message during flush", "messageID", msg.ID)
			}
			if err := r.ValkeyClient.DeleteMessages(ctx, streamKey, msg.ID); err != nil {
				log.Error(err, "Failed to delete message during flush", "messageID", msg.ID)
			}
		}
	}

	// Then handle any messages that were never delivered to a consumer.
	for {
		messages, err := r.ValkeyClient.ReadRange(ctx, streamKey, "-", "+", 100)
		if err != nil {
			log.Error(err, "Failed to read remaining stream entries during flush")
			return
		}
		if len(messages) == 0 {
			break
		}

		ids := make([]string, 0, len(messages))
		for _, msg := range messages {
			file := msg.Values["file"]
			attempts := parseAttempts(msg.Values["attempts"], int(maxAttempts))
			if file == "" {
				file = "<unknown>"
			}
			if attempts > fileAttempts[file] {
				fileAttempts[file] = attempts
			}
			ids = append(ids, msg.ID)
		}

		if err := r.ValkeyClient.DeleteMessages(ctx, streamKey, ids...); err != nil {
			log.Error(err, "Failed to delete stream messages during flush", "ids", ids)
			break
		}
	}

	finalAttempts := make(map[string]int)
	for file, attempts := range existingAttempts {
		finalAttempts[file] = attempts
	}
	for file, attempts := range fileAttempts {
		if attempts > finalAttempts[file] {
			finalAttempts[file] = attempts
		}
	}

	for file, attempts := range finalAttempts {
		if attempts <= 0 || attempts < int(maxAttempts) {
			attempts = int(maxAttempts)
		}

		if ids := existingIDs[file]; len(ids) > 0 {
			if err := r.ValkeyClient.DeleteMessages(ctx, dlqKey, ids...); err != nil {
				log.Error(err, "Failed to delete existing DLQ entries during flush", "file", file)
				continue
			}
		}

		if err := r.ValkeyClient.AddToDLQ(ctx, dlqKey, instanceID, file, attempts, message); err != nil {
			log.Error(err, "Failed to add message to DLQ during flush", "file", file)
			continue
		}
		log.Info("Moved message to DLQ during flush", "file", file, "attempts", attempts, "reason", reason)
	}
}

func parseAttempts(value string, fallback int) int {
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	if parsed < 0 {
		return fallback
	}
	return parsed
}

// propagatedLabels is the whitelist of plainsight.ai/* labels that are copied
// from PipelineInstance CR metadata to pod templates. Using a whitelist (not
// prefix match) prevents unbounded cardinality in Loki from arbitrary labels.
var propagatedLabels = []string{
	"plainsight.ai/pipeline-instance-id",
	"plainsight.ai/organization-id",
	"plainsight.ai/project-id",
}

// buildPodLabels creates pod labels by merging the standard filter labels with
// whitelisted plainsight.ai/* labels from the PipelineInstance CR.
func buildPodLabels(instanceID string, pi *pipelinesv1alpha1.PipelineInstance) map[string]string {
	labels := map[string]string{
		"filter.plainsight.ai/instance":         instanceID,
		"filter.plainsight.ai/pipelineinstance": pi.Name,
	}
	for _, key := range propagatedLabels {
		if val, ok := pi.Labels[key]; ok {
			labels[key] = val
		}
	}
	return labels
}
