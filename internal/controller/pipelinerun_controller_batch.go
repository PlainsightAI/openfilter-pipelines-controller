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
func (r *PipelineRunReconciler) reconcileBatch(ctx context.Context, pipelineRun *pipelinesv1alpha1.PipelineRun, pipeline *pipelinesv1alpha1.Pipeline) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Ensure counts object exists before initialization
	if pipelineRun.Status.Counts == nil {
		pipelineRun.Status.Counts = &pipelinesv1alpha1.FileCounts{}
	}

	initialized, err := r.initializePipelineRun(ctx, pipelineRun)
	if err != nil {
		log.Error(err, "Failed to initialize PipelineRun")
		return ctrl.Result{}, err
	}
	if !initialized {
		// Initialization failed or determined no work is required (e.g., empty input).
		// In these cases, reconciliation is complete.
		return ctrl.Result{}, nil
	}

	// Step 1: Ensure Job exists
	if err := r.ensureJob(ctx, pipelineRun, pipeline); err != nil {
		log.Error(err, "Failed to ensure Job")
		r.setCondition(pipelineRun, ConditionTypeDegraded, metav1.ConditionTrue, "JobCreationFailed", err.Error())
		if err := r.Status().Update(ctx, pipelineRun); err != nil {
			log.Error(err, "Failed to update status")
		}
		return ctrl.Result{}, err
	}

	// Step 2: Handle completed pods (ACK/retry/DLQ)
	if err := r.handleCompletedPods(ctx, pipelineRun); err != nil {
		log.Error(err, "Failed to handle completed pods")
		// Don't fail reconciliation, just log and continue
	}

	// Step 3: Run reclaimer for stale pending messages
	if err := r.runReclaimer(ctx, pipelineRun); err != nil {
		log.Error(err, "Failed to run reclaimer")
		// Don't fail reconciliation, just log and continue
	}

	// Step 4: Update status from Valkey metrics
	r.updateStatus(ctx, pipelineRun)

	// Detect job failure and mark the run as degraded
	if failedCond, err := r.checkFailure(ctx, pipelineRun); err != nil {
		log.Error(err, "Failed to check job failure state")
	} else if failedCond != nil {
		failureReason := failedCond.Reason
		if failureReason == "" {
			failureReason = "JobFailed"
		}

		failureMessage := failedCond.Message
		if failureMessage == "" {
			failureMessage = fmt.Sprintf("Job %s failed", pipelineRun.Status.JobName)
		} else {
			failureMessage = fmt.Sprintf("Job %s failed: %s", pipelineRun.Status.JobName, failureMessage)
		}

		r.flushOutstandingWork(ctx, pipelineRun, failureReason, failureMessage)

		log.Info("PipelineRun marked as Degraded due to job failure", "job", pipelineRun.Status.JobName, "reason", failureReason)
		r.setCondition(pipelineRun, ConditionTypeDegraded, metav1.ConditionTrue, failureReason, failureMessage)
		r.setCondition(pipelineRun, ConditionTypeProgressing, metav1.ConditionFalse, failureReason, failureMessage)
		r.setCondition(pipelineRun, ConditionTypeSucceeded, metav1.ConditionFalse, failureReason, failureMessage)

		if pipelineRun.Status.CompletionTime == nil {
			now := metav1.Now()
			pipelineRun.Status.CompletionTime = &now
		}

		if err := r.Status().Update(ctx, pipelineRun); err != nil {
			log.Error(err, "Failed to update status after marking run degraded")
			return ctrl.Result{}, err
		}

		// No further processing is required once the run has failed
		return ctrl.Result{}, nil
	}

	// Step 5: Check for completion
	isComplete, err := r.checkCompletion(ctx, pipelineRun)
	if err != nil {
		log.Error(err, "Failed to check completion")
		return ctrl.Result{RequeueAfter: StatusUpdateInterval}, nil
	}

	if isComplete {
		log.Info("PipelineRun completed successfully")
		r.setCondition(pipelineRun, ConditionTypeSucceeded, metav1.ConditionTrue, "Completed", "All files processed")
		r.setCondition(pipelineRun, ConditionTypeProgressing, metav1.ConditionFalse, "Completed", "Processing finished")

		if pipelineRun.Status.CompletionTime == nil {
			now := metav1.Now()
			pipelineRun.Status.CompletionTime = &now
		}

		if err := r.Status().Update(ctx, pipelineRun); err != nil {
			log.Error(err, "Failed to update status")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// Set Progressing condition
	r.setCondition(pipelineRun, ConditionTypeProgressing, metav1.ConditionTrue, "Processing", "Pipeline is processing files")

	if err := r.Status().Update(ctx, pipelineRun); err != nil {
		log.Error(err, "Failed to update status")
		return ctrl.Result{}, err
	}

	// Requeue for periodic status updates
	return ctrl.Result{RequeueAfter: StatusUpdateInterval}, nil
}

// initializePipelineRun performs one-time setup for new PipelineRun resources.
// It creates the Valkey stream and consumer group, enumerates source files, enqueues
// work items, and seeds status counters. The logic is safe to call multiple times;
// after successful initialization it becomes a no-op. On error it returns a
// non-nil error so the reconciliation loop can retry.
func (r *PipelineRunReconciler) initializePipelineRun(ctx context.Context, pipelineRun *pipelinesv1alpha1.PipelineRun) (bool, error) {
	log := logf.FromContext(ctx)

	// If StartTime is already set, initialization has completed previously.
	if pipelineRun.Status.StartTime != nil {
		if pipelineRun.Status.Counts != nil && pipelineRun.Status.Counts.TotalFiles == 0 {
			// Nothing to process. Skip further work.
			return false, nil
		}
		return true, nil
	}

	if r.ValkeyClient == nil {
		err := fmt.Errorf("valkey client is not configured")
		r.setCondition(pipelineRun, ConditionTypeDegraded, metav1.ConditionTrue, "QueueUnavailable", err.Error())
		r.setCondition(pipelineRun, ConditionTypeProgressing, metav1.ConditionFalse, "QueueUnavailable", "PipelineRun cannot start without queue connectivity")
		if statusErr := r.Status().Update(ctx, pipelineRun); statusErr != nil {
			return false, statusErr
		}
		return false, err
	}

	// Ensure the stream and consumer group exist. This call is idempotent and safe on retries.
	if err := r.ValkeyClient.CreateStreamAndGroup(ctx, pipelineRun.GetQueueStream(), pipelineRun.GetQueueGroup()); err != nil {
		r.setCondition(pipelineRun, ConditionTypeDegraded, metav1.ConditionTrue, "QueueInitializationFailed", err.Error())
		r.setCondition(pipelineRun, ConditionTypeProgressing, metav1.ConditionFalse, "QueueInitializationFailed", "PipelineRun cannot initialize its queue")
		if statusErr := r.Status().Update(ctx, pipelineRun); statusErr != nil {
			return false, statusErr
		}
		return false, fmt.Errorf("failed to create stream and group: %w", err)
	}

	currentLength, err := r.ValkeyClient.GetStreamLength(ctx, pipelineRun.GetQueueStream())
	if err != nil {
		r.setCondition(pipelineRun, ConditionTypeDegraded, metav1.ConditionTrue, "QueueInspectionFailed", err.Error())
		r.setCondition(pipelineRun, ConditionTypeProgressing, metav1.ConditionFalse, "QueueInspectionFailed", "PipelineRun cannot inspect queue state")
		if statusErr := r.Status().Update(ctx, pipelineRun); statusErr != nil {
			return false, statusErr
		}
		return false, fmt.Errorf("failed to get stream length: %w", err)
	}

	runID := pipelineRun.GetRunID()

	if currentLength == 0 {
		accessKey, secretKey, credErr := r.getCredentials(ctx, pipelineRun)
		if credErr != nil {
			r.setCondition(pipelineRun, ConditionTypeDegraded, metav1.ConditionTrue, "CredentialsError", credErr.Error())
			r.setCondition(pipelineRun, ConditionTypeProgressing, metav1.ConditionFalse, "CredentialsError", "PipelineRun cannot access object storage credentials")
			if statusErr := r.Status().Update(ctx, pipelineRun); statusErr != nil {
				return false, statusErr
			}
			return false, fmt.Errorf("failed to get S3 credentials: %w", credErr)
		}

		files, listErr := r.listBucketFiles(ctx, pipelineRun, accessKey, secretKey)
		if listErr != nil {
			r.setCondition(pipelineRun, ConditionTypeDegraded, metav1.ConditionTrue, "ListingFailed", listErr.Error())
			r.setCondition(pipelineRun, ConditionTypeProgressing, metav1.ConditionFalse, "ListingFailed", "PipelineRun cannot enumerate input files")
			if statusErr := r.Status().Update(ctx, pipelineRun); statusErr != nil {
				return false, statusErr
			}
			return false, fmt.Errorf("failed to list bucket files: %w", listErr)
		}

		if len(files) == 0 {
			now := metav1.Now()
			pipelineRun.Status.StartTime = &now
			pipelineRun.Status.CompletionTime = &now
			pipelineRun.Status.Counts.TotalFiles = 0
			pipelineRun.Status.Counts.Queued = 0
			pipelineRun.Status.Counts.Running = 0
			pipelineRun.Status.Counts.Succeeded = 0
			pipelineRun.Status.Counts.Failed = 0

			r.setCondition(pipelineRun, ConditionTypeDegraded, metav1.ConditionTrue, "NoFilesFound", "No files found in input bucket")
			r.setCondition(pipelineRun, ConditionTypeProgressing, metav1.ConditionFalse, "NoFilesFound", "PipelineRun cannot start without input files")
			r.setCondition(pipelineRun, ConditionTypeSucceeded, metav1.ConditionFalse, "NoFilesFound", "PipelineRun did not process any files")

			if statusErr := r.Status().Update(ctx, pipelineRun); statusErr != nil {
				return false, statusErr
			}

			log.Info("PipelineRun has no files to process; marking as degraded", "pipelineRun", pipelineRun.Name)
			return false, nil
		}

		successful := int64(0)
		for _, file := range files {
			if _, enqueueErr := r.ValkeyClient.EnqueueFileWithAttempts(ctx, pipelineRun.GetQueueStream(), runID, file, 0); enqueueErr != nil {
				log.Error(enqueueErr, "Failed to enqueue file", "file", file)
				continue
			}
			successful++
		}

		// Refresh the stream length to reflect any enqueued work items.
		currentLength, err = r.ValkeyClient.GetStreamLength(ctx, pipelineRun.GetQueueStream())
		if err != nil {
			r.setCondition(pipelineRun, ConditionTypeDegraded, metav1.ConditionTrue, "QueueInspectionFailed", err.Error())
			r.setCondition(pipelineRun, ConditionTypeProgressing, metav1.ConditionFalse, "QueueInspectionFailed", "PipelineRun cannot inspect queue state")
			if statusErr := r.Status().Update(ctx, pipelineRun); statusErr != nil {
				return false, statusErr
			}
			return false, fmt.Errorf("failed to get stream length after enqueue: %w", err)
		}

		if currentLength == 0 || successful == 0 {
			err := fmt.Errorf("no files were successfully enqueued for processing")
			r.setCondition(pipelineRun, ConditionTypeDegraded, metav1.ConditionTrue, "EnqueueFailed", err.Error())
			r.setCondition(pipelineRun, ConditionTypeProgressing, metav1.ConditionFalse, "EnqueueFailed", "PipelineRun could not enqueue files for processing")
			if statusErr := r.Status().Update(ctx, pipelineRun); statusErr != nil {
				return false, statusErr
			}
			return false, err
		}
	}

	now := metav1.Now()
	pipelineRun.Status.StartTime = &now
	pipelineRun.Status.Counts.TotalFiles = currentLength
	pipelineRun.Status.Counts.Queued = currentLength
	pipelineRun.Status.Counts.Running = 0
	pipelineRun.Status.Counts.Succeeded = 0
	pipelineRun.Status.Counts.Failed = 0

	r.setCondition(pipelineRun, ConditionTypeDegraded, metav1.ConditionFalse, "Initialized", "PipelineRun initialized successfully")
	r.setCondition(pipelineRun, ConditionTypeProgressing, metav1.ConditionTrue, "Initialized", "PipelineRun initialized and files queued")
	r.setCondition(pipelineRun, ConditionTypeSucceeded, metav1.ConditionFalse, "Initialized", "PipelineRun is processing input files")

	if statusErr := r.Status().Update(ctx, pipelineRun); statusErr != nil {
		return false, statusErr
	}

	log.Info("PipelineRun initialized", "pipelineRun", pipelineRun.Name, "totalFiles", pipelineRun.Status.Counts.TotalFiles)
	return true, nil
}

// ensureJob creates or updates the Job for the PipelineRun
func (r *PipelineRunReconciler) ensureJob(ctx context.Context, pipelineRun *pipelinesv1alpha1.PipelineRun, pipeline *pipelinesv1alpha1.Pipeline) error {
	log := logf.FromContext(ctx)

	// Check if Job already exists
	if pipelineRun.Status.JobName != "" {
		job := &batchv1.Job{}
		err := r.Get(ctx, types.NamespacedName{
			Name:      pipelineRun.Status.JobName,
			Namespace: pipelineRun.Namespace,
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

	// Generate Job name from PipelineRun name
	jobName := fmt.Sprintf("%s-job", pipelineRun.Name)

	// Build the Job spec
	job := r.buildJob(ctx, pipelineRun, pipeline, jobName)

	// Set PipelineRun as owner of the Job
	if err := controllerutil.SetControllerReference(pipelineRun, job, r.Scheme); err != nil {
		return fmt.Errorf("failed to set owner reference: %w", err)
	}

	// Create the Job
	if err := r.Create(ctx, job); err != nil {
		return fmt.Errorf("failed to create job: %w", err)
	}

	log.Info("Created Job for PipelineRun", "job", jobName)

	// Update status with Job name
	pipelineRun.Status.JobName = jobName
	return nil
}

// buildJob constructs the Job specification for the PipelineRun
func (r *PipelineRunReconciler) buildJob(ctx context.Context, pipelineRun *pipelinesv1alpha1.PipelineRun, pipeline *pipelinesv1alpha1.Pipeline, jobName string) *batchv1.Job {
	log := logf.FromContext(ctx)

	// Get run ID from the PipelineRun UID
	runID := pipelineRun.GetRunID()

	// Get execution config with defaults
	parallelism := int32(10)
	if pipelineRun.Spec.Execution != nil && pipelineRun.Spec.Execution.Parallelism != nil {
		parallelism = *pipelineRun.Spec.Execution.Parallelism
	}

	// Get total files count, default to 0 if not set
	totalFiles := int64(0)
	if pipelineRun.Status.Counts != nil {
		totalFiles = pipelineRun.Status.Counts.TotalFiles
	}

	log.V(1).Info("Building job", "totalFiles", totalFiles, "parallelism", parallelism)

	// Get S3 credentials from PipelineRun
	var s3SecretName string
	if pipelineRun.Spec.Source.Bucket != nil && pipelineRun.Spec.Source.Bucket.CredentialsSecret != nil {
		s3SecretName = pipelineRun.Spec.Source.Bucket.CredentialsSecret.Name
	}

	// Build claimer init container env vars
	claimerEnv := []corev1.EnvVar{
		{Name: "STREAM", Value: pipelineRun.GetQueueStream()},
		{Name: "GROUP", Value: pipelineRun.GetQueueGroup()},
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
	if pipelineRun.Spec.Source.Bucket != nil {
		claimerEnv = append(claimerEnv, []corev1.EnvVar{
			{Name: "S3_BUCKET", Value: pipelineRun.Spec.Source.Bucket.Name},
			{Name: "S3_ENDPOINT", Value: pipelineRun.Spec.Source.Bucket.Endpoint},
			{Name: "S3_REGION", Value: pipelineRun.Spec.Source.Bucket.Region},
			{Name: "S3_USE_PATH_STYLE", Value: fmt.Sprintf("%t", pipelineRun.Spec.Source.Bucket.UsePathStyle)},
			{Name: "S3_INSECURE_SKIP_TLS_VERIFY", Value: fmt.Sprintf("%t", pipelineRun.Spec.Source.Bucket.InsecureSkipTLSVerify)},
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

	// Add Valkey password if set
	if r.ValkeyPassword != "" {
		claimerEnv = append(claimerEnv, corev1.EnvVar{
			Name:  "VALKEY_PASSWORD",
			Value: r.ValkeyPassword,
		})
	}

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

	// Build Job spec
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: pipelineRun.Namespace,
			Labels: map[string]string{
				"filter.plainsight.ai/run":         runID,
				"filter.plainsight.ai/pipelinerun": pipelineRun.Name,
			},
		},
		Spec: batchv1.JobSpec{
			CompletionMode:          ptr.To(batchv1.NonIndexedCompletion),
			Completions:             ptr.To(int32(totalFiles)),
			Parallelism:             ptr.To(parallelism),
			BackoffLimit:            ptr.To(int32(2)),
			TTLSecondsAfterFinished: ptr.To(int32(86400)), // 24 hours
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"filter.plainsight.ai/run":         runID,
						"filter.plainsight.ai/pipelinerun": pipelineRun.Name,
					},
				},
				Spec: corev1.PodSpec{
					// No special ServiceAccount required; default SA is sufficient
					RestartPolicy: corev1.RestartPolicyNever,
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
func (r *PipelineRunReconciler) handleCompletedPods(ctx context.Context, pipelineRun *pipelinesv1alpha1.PipelineRun) error {
	log := logf.FromContext(ctx)

	// List all pods for this PipelineRun
	podList := &corev1.PodList{}
	runID := pipelineRun.GetRunID()
	if err := r.List(ctx, podList, client.InNamespace(pipelineRun.Namespace), client.MatchingLabels{
		"filter.plainsight.ai/run": runID,
	}); err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	maxAttempts := int32(3)
	if pipelineRun.Spec.Execution != nil && pipelineRun.Spec.Execution.MaxAttempts != nil {
		maxAttempts = *pipelineRun.Spec.Execution.MaxAttempts
	}

	for _, pod := range podList.Items {
		// Only process completed pods
		startFailureReason, hasStartFailure := detectPodStartFailure(&pod)
		if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed && !hasStartFailure {
			log.V(1).Info("Skipping pod that is not complete", "pod", pod.Name, "phase", pod.Status.Phase)
			continue
		}

		// Discover the message claimed by this pod's init container using the consumer name = pod.Name.
		// There should be at most one pending entry for this consumer.
		pendingIDs, err := r.ValkeyClient.GetPendingForConsumer(ctx, pipelineRun.GetQueueStream(), pipelineRun.GetQueueGroup(), pod.Name, 1)
		if err != nil {
			log.Error(err, "Failed to get pending entries for consumer", "pod", pod.Name)
			continue
		}

		if len(pendingIDs) == 0 {
			// Nothing pending for this consumer; already processed or never claimed.
			log.V(1).Info("No pending entries for consumer", "pod", pod.Name)
			continue
		}

		messageID := pendingIDs[0]
		// Fetch message fields
		msgs, err := r.ValkeyClient.ReadRange(ctx, pipelineRun.GetQueueStream(), messageID, messageID, 1)
		if err != nil || len(msgs) == 0 {
			log.Error(err, "Failed to read message for pending entry; attempting to ACK", "messageID", messageID)
			// Best-effort ACK to prevent leaks
			if ackErr := r.ValkeyClient.AckMessage(ctx, pipelineRun.GetQueueStream(), pipelineRun.GetQueueGroup(), messageID); ackErr != nil {
				log.Error(ackErr, "Failed to ACK orphan pending entry", "messageID", messageID)
			}
			continue
		}

		filepath := msgs[0].Values["file"]
		attempts := parseAttempts(msgs[0].Values["attempts"], int(maxAttempts))

		// Get DLQ key
		dlqKey := fmt.Sprintf("pr:%s:dlq", runID)

		if pod.Status.Phase == corev1.PodSucceeded {
			// ACK the message
			if err := r.ValkeyClient.AckMessage(ctx, pipelineRun.GetQueueStream(), pipelineRun.GetQueueGroup(), messageID); err != nil {
				log.Error(err, "Failed to ACK message", "messageID", messageID)
				continue
			}
			log.Info("ACKed successful message", "pod", pod.Name, "file", filepath)

		} else if pod.Status.Phase == corev1.PodFailed || hasStartFailure {
			// Determine reason for failure
			reason := startFailureReason
			if reason == "" {
				reason = "Unknown"
				for _, containerStatus := range pod.Status.ContainerStatuses {
					if containerStatus.State.Terminated != nil && containerStatus.State.Terminated.ExitCode != 0 {
						reason = fmt.Sprintf("Container %s exited with code %d", containerStatus.Name, containerStatus.State.Terminated.ExitCode)
						break
					}
				}
			}

			attempts++
			if attempts < int(maxAttempts) {
				// Re-enqueue with incremented attempts
				if _, err := r.ValkeyClient.EnqueueFileWithAttempts(ctx, pipelineRun.GetQueueStream(), runID, filepath, attempts); err != nil {
					log.Error(err, "Failed to re-enqueue file", "file", filepath)
				} else {
					log.Info("Re-enqueued failed file", "file", filepath, "attempts", attempts)
				}
			} else {
				// Add to DLQ
				if err := r.ValkeyClient.AddToDLQ(ctx, dlqKey, runID, filepath, attempts, reason); err != nil {
					log.Error(err, "Failed to add to DLQ", "file", filepath)
				} else {
					log.Info("Added file to DLQ", "file", filepath, "reason", reason)
				}
			}

			// ACK the original message
			if err := r.ValkeyClient.AckMessage(ctx, pipelineRun.GetQueueStream(), pipelineRun.GetQueueGroup(), messageID); err != nil {
				log.Error(err, "Failed to ACK failed message", "messageID", messageID)
				continue
			}

			if hasStartFailure {
				// Delete the pod to allow the Job controller to create a replacement
				if err := r.Delete(ctx, &pod); err != nil {
					log.Error(err, "Failed to delete pod after start failure", "pod", pod.Name)
				} else {
					log.Info("Deleted pod after start failure", "pod", pod.Name)
				}
				// Skip marking processed because the pod is gone
				continue
			}
		}

		// No need to mark pod as processed; we rely on queue state to avoid reprocessing.
	}

	return nil
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
			message := waiting.Message
			if status.LastTerminationState.Terminated != nil {
				terminated := status.LastTerminationState.Terminated
				message = fmt.Sprintf("Container %s crashlooped with exit code %d: %s", status.Name, terminated.ExitCode, terminated.Reason)
			} else if message == "" {
				message = waiting.Reason
			}
			return message
		}
	}
	return ""
}

// runReclaimer runs XAUTOCLAIM to recover stale pending messages
func (r *PipelineRunReconciler) runReclaimer(ctx context.Context, pipelineRun *pipelinesv1alpha1.PipelineRun) error {
	log := logf.FromContext(ctx)

	pendingTimeout := 15 * time.Minute
	if pipelineRun.Spec.Execution != nil && pipelineRun.Spec.Execution.PendingTimeout != nil {
		pendingTimeout = pipelineRun.Spec.Execution.PendingTimeout.Duration
	}

	minIdleTime := pendingTimeout.Milliseconds()
	consumerName := "controller-reclaimer"

	// Run XAUTOCLAIM
	messages, err := r.ValkeyClient.AutoClaim(ctx, pipelineRun.GetQueueStream(), pipelineRun.GetQueueGroup(), consumerName, minIdleTime, 100)
	if err != nil {
		return fmt.Errorf("failed to auto-claim messages: %w", err)
	}

	if len(messages) > 0 {
		log.Info("Reclaimed stale messages", "count", len(messages))

		runID := pipelineRun.GetRunID()
		maxAttempts := int32(3)
		if pipelineRun.Spec.Execution != nil && pipelineRun.Spec.Execution.MaxAttempts != nil {
			maxAttempts = *pipelineRun.Spec.Execution.MaxAttempts
		}

		dlqKey := fmt.Sprintf("pr:%s:dlq", runID)

		// Process each reclaimed message
		for _, msg := range messages {
			filepath := msg.Values["file"]
			attemptsStr := msg.Values["attempts"]
			attempts, _ := strconv.Atoi(attemptsStr)
			attempts++

			if attempts < int(maxAttempts) {
				// Re-enqueue with incremented attempts
				if _, err := r.ValkeyClient.EnqueueFileWithAttempts(ctx, pipelineRun.GetQueueStream(), runID, filepath, attempts); err != nil {
					log.Error(err, "Failed to re-enqueue reclaimed file", "file", filepath)
				}
			} else {
				// Send to DLQ
				reason := "Max attempts exceeded (reclaimed stale message)"
				if err := r.ValkeyClient.AddToDLQ(ctx, dlqKey, runID, filepath, attempts, reason); err != nil {
					log.Error(err, "Failed to add reclaimed file to DLQ", "file", filepath)
				}
			}

			// ACK the reclaimed message
			if err := r.ValkeyClient.AckMessage(ctx, pipelineRun.GetQueueStream(), pipelineRun.GetQueueGroup(), msg.ID); err != nil {
				log.Error(err, "Failed to ACK reclaimed message", "messageID", msg.ID)
			}
		}
	}

	return nil
}

// updateStatus updates the PipelineRun status from Valkey metrics
func (r *PipelineRunReconciler) updateStatus(ctx context.Context, pipelineRun *pipelinesv1alpha1.PipelineRun) {
	log := logf.FromContext(ctx)

	if pipelineRun.Status.Counts == nil {
		pipelineRun.Status.Counts = &pipelinesv1alpha1.FileCounts{}
	}

	// Get queued count (consumer group lag - messages not yet read)
	queued, err := r.ValkeyClient.GetConsumerGroupLag(ctx, pipelineRun.GetQueueStream(), pipelineRun.GetQueueGroup())
	if err != nil {
		log.Error(err, "Failed to get consumer group lag")
		queued = 0
	}

	// Get running count (pending messages - read but not acknowledged)
	running, err := r.ValkeyClient.GetPendingCount(ctx, pipelineRun.GetQueueStream(), pipelineRun.GetQueueGroup())
	if err != nil {
		log.Error(err, "Failed to get pending count")
		running = 0
	}

	// Get failed count (DLQ length)
	runID := pipelineRun.GetRunID()
	dlqKey := fmt.Sprintf("pr:%s:dlq", runID)
	failed, err := r.ValkeyClient.GetStreamLength(ctx, dlqKey)
	if err != nil {
		failed = 0
	}

	// Calculate succeeded count
	totalFiles := pipelineRun.Status.Counts.TotalFiles
	succeeded := totalFiles - queued - running - failed
	if succeeded < 0 {
		succeeded = 0
	}

	// Update counts
	pipelineRun.Status.Counts.Queued = queued
	pipelineRun.Status.Counts.Running = running
	pipelineRun.Status.Counts.Succeeded = succeeded
	pipelineRun.Status.Counts.Failed = failed

	log.V(1).Info("Updated status counts", "queued", queued, "running", running, "succeeded", succeeded, "failed", failed)
}

// checkCompletion determines if the PipelineRun has completed
func (r *PipelineRunReconciler) checkCompletion(ctx context.Context, pipelineRun *pipelinesv1alpha1.PipelineRun) (bool, error) {
	if pipelineRun.Status.Counts == nil {
		return false, nil
	}

	queued := pipelineRun.Status.Counts.Queued
	running := pipelineRun.Status.Counts.Running

	if queued == 0 && running == 0 {
		if pipelineRun.Status.JobName != "" {
			job := &batchv1.Job{}
			err := r.Get(ctx, types.NamespacedName{
				Name:      pipelineRun.Status.JobName,
				Namespace: pipelineRun.Namespace,
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
func (r *PipelineRunReconciler) checkFailure(ctx context.Context, pipelineRun *pipelinesv1alpha1.PipelineRun) (*batchv1.JobCondition, error) {
	if pipelineRun.Status.JobName == "" {
		return nil, nil
	}

	job := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{
		Name:      pipelineRun.Status.JobName,
		Namespace: pipelineRun.Namespace,
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
func (r *PipelineRunReconciler) flushOutstandingWork(ctx context.Context, pipelineRun *pipelinesv1alpha1.PipelineRun, reason, message string) {
	log := logf.FromContext(ctx)

	streamKey := pipelineRun.GetQueueStream()
	groupName := pipelineRun.GetQueueGroup()
	runID := pipelineRun.GetRunID()
	dlqKey := fmt.Sprintf("pr:%s:dlq", runID)

	maxAttempts := int32(3)
	if pipelineRun.Spec.Execution != nil && pipelineRun.Spec.Execution.MaxAttempts != nil {
		maxAttempts = *pipelineRun.Spec.Execution.MaxAttempts
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

		if err := r.ValkeyClient.AddToDLQ(ctx, dlqKey, runID, file, attempts, message); err != nil {
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
	if parsed <= 0 {
		return fallback
	}
	return parsed
}
