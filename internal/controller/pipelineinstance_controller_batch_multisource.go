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
	"path/filepath"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
	"github.com/PlainsightAI/openfilter-pipelines-controller/internal/tracing"
)

// reconcileBatchMultiSource handles the multi-source batch path (PLAT-1071).
//
// Multi-source batch runs as a single Pod (parallelism=1, completions=1)
// with one init claimer per binding — each downloads its specific S3
// object to `/ws/<filterName>.<ext>` — followed by the filter chain. The
// Valkey work-queue (designed for parallel-Pods-per-bucket fan-out) is
// skipped entirely: the bindings already name exactly which files to
// process. Completion is detected via the Job's own status (Succeeded /
// Failed) the same way Kubernetes would for any one-shot Job.
//
// Constraints enforced here (V1):
//   - Every binding's PipelineSource must have a Bucket source set
//     (RTSP makes no sense for batch).
//   - Every binding must resolve a deterministic object key for its
//     direct-mode claimer: `Bucket.Prefix` used as the full key (the
//     platform's media model carries single-file keys in prefix — see
//     bindingObjectKey).
//
// Either constraint failing surfaces as a Degraded condition with an
// operator-actionable message; the reconciler returns nil so the kube
// API server doesn't pile up retry events on a known-bad CR.
func (r *PipelineInstanceReconciler) reconcileBatchMultiSource(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance, pipeline *pipelinesv1alpha1.Pipeline, sourceBindings []ResolvedSourceBinding) (ctrl.Result, error) {
	// Stamp the canonical pipeline-instance pivot so the multi-source
	// batch reconcile shares the same Cloud Trace grouping as
	// reconcileBatch / reconcileStreaming. Satisfies the spancov gate
	// and gives operators one query (`pipeline_instance.id = "<uid>"`)
	// that returns every reconcile pass regardless of path.
	tracing.Stamp(ctx, tracing.PipelineInstanceID(pipelineInstance))
	log := logf.FromContext(ctx)

	// Validate every binding has the shape multi-source batch needs.
	for _, b := range sourceBindings {
		if b.Source == nil || b.Source.Spec.Bucket == nil {
			msg := fmt.Sprintf("multi-source batch requires every binding's PipelineSource to be a Bucket source (filter %q is not)", b.FilterName)
			return r.degradeBatchValidation(ctx, pipelineInstance, ReasonMultiSourceBatchInvalidSource, msg)
		}
		if bindingObjectKey(b) == "" {
			msg := fmt.Sprintf("multi-source batch requires every binding's PipelineSource Bucket.Prefix to name a full object key; %q has an empty prefix", b.FilterName)
			return r.degradeBatchValidation(ctx, pipelineInstance, ReasonMultiSourceBatchMissingObject, msg)
		}
	}

	// Validation passed — clear any stale Degraded condition a previous
	// failing pass left behind. The validation failures above return without
	// a requeue, so recovery rides on the PipelineSource watch mapping in
	// SetupWithManager re-triggering this reconcile once the source is fixed;
	// without this clear the instance would stay Degraded forever even after
	// the fix.
	if err := r.clearDegradedReason(ctx, pipelineInstance,
		ReasonMultiSourceBatchInvalidSource, ReasonMultiSourceBatchMissingObject); err != nil {
		log.Error(err, "Failed to clear stale multi-source batch validation Degraded condition")
		return ctrl.Result{}, err
	}

	// Stamp StartTime once for trace correlation and to anchor the
	// Job-name derivation below to a stable value across reconciles.
	if pipelineInstance.Status.StartTime == nil {
		now := metav1.Now()
		pipelineInstance.Status.StartTime = &now
		if err := r.Status().Update(ctx, pipelineInstance); err != nil {
			log.Error(err, "Failed to set StartTime")
			return ctrl.Result{}, err
		}
	}

	jobName := pipelineInstance.Name + "-job"

	// Idempotent Job ensure: if the Job already exists we just observe
	// its status; if not, build and create.
	job := &batchv1.Job{}
	getErr := r.Get(ctx, types.NamespacedName{Name: jobName, Namespace: pipelineInstance.Namespace}, job)
	switch {
	case apierrors.IsNotFound(getErr):
		desired := r.buildMultiSourceBatchJob(ctx, pipelineInstance, pipeline, sourceBindings, jobName)
		if err := controllerutil.SetControllerReference(pipelineInstance, desired, r.Scheme); err != nil {
			return ctrl.Result{}, fmt.Errorf("set owner ref on job: %w", err)
		}
		if err := r.Create(ctx, desired); err != nil {
			r.setCondition(pipelineInstance, ConditionTypeDegraded, metav1.ConditionTrue, "JobCreationFailed", err.Error())
			if statusErr := r.Status().Update(ctx, pipelineInstance); statusErr != nil {
				log.Error(statusErr, "Failed to update status after job create failure")
			}
			return ctrl.Result{}, fmt.Errorf("create job: %w", err)
		}
		pipelineInstance.Status.JobName = jobName
		log.Info("Created multi-source batch Job", "job", jobName, "bindings", len(sourceBindings))
	case getErr != nil:
		return ctrl.Result{}, fmt.Errorf("get job: %w", getErr)
	}

	// Observe Job status. We translate the Job's terminal state into
	// our standard Succeeded / Degraded conditions; while running we
	// stay in Progressing.
	if pipelineInstance.Status.JobName == "" {
		pipelineInstance.Status.JobName = jobName
	}
	for _, c := range job.Status.Conditions {
		if c.Status != corev1.ConditionTrue {
			continue
		}
		switch c.Type {
		case batchv1.JobComplete, batchv1.JobSuccessCriteriaMet:
			r.setCondition(pipelineInstance, ConditionTypeSucceeded, metav1.ConditionTrue, "Completed", "Job completed successfully")
			r.setCondition(pipelineInstance, ConditionTypeProgressing, metav1.ConditionFalse, "Completed", "Job completed successfully")
			if pipelineInstance.Status.CompletionTime == nil {
				now := metav1.Now()
				pipelineInstance.Status.CompletionTime = &now
			}
			if err := r.Status().Update(ctx, pipelineInstance); err != nil {
				log.Error(err, "Failed to update status after Job success")
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		case batchv1.JobFailed, batchv1.JobFailureTarget:
			reason := c.Reason
			if reason == "" {
				reason = "JobFailed"
			}
			r.setCondition(pipelineInstance, ConditionTypeDegraded, metav1.ConditionTrue, reason, c.Message)
			r.setCondition(pipelineInstance, ConditionTypeProgressing, metav1.ConditionFalse, reason, c.Message)
			r.setCondition(pipelineInstance, ConditionTypeSucceeded, metav1.ConditionFalse, reason, c.Message)
			if pipelineInstance.Status.CompletionTime == nil {
				now := metav1.Now()
				pipelineInstance.Status.CompletionTime = &now
			}
			if err := r.Status().Update(ctx, pipelineInstance); err != nil {
				log.Error(err, "Failed to update status after Job failure")
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
	}

	// Still progressing. This branch re-runs every StatusUpdateInterval —
	// skip the status write when the condition is already in this exact
	// state (meta.SetStatusCondition reports whether anything changed), so
	// the steady-state loop doesn't issue a no-op API write every 30s.
	changed := meta.SetStatusCondition(&pipelineInstance.Status.Conditions, metav1.Condition{
		Type:               ConditionTypeProgressing,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: pipelineInstance.Generation,
		LastTransitionTime: metav1.Now(),
		Reason:             "Processing",
		Message:            "Multi-source batch Job is running",
	})
	if changed {
		if err := r.Status().Update(ctx, pipelineInstance); err != nil {
			log.Error(err, "Failed to update status")
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: StatusUpdateInterval}, nil
}

// buildMultiSourceBatchJob constructs the Job for the multi-source batch
// path. One init claimer per binding (direct mode), then the user's
// filter containers with a per-binding VIDEO_INPUT_PATH env var injected
// into the matching VideoIn container only.
func (r *PipelineInstanceReconciler) buildMultiSourceBatchJob(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance, pipeline *pipelinesv1alpha1.Pipeline, sourceBindings []ResolvedSourceBinding, jobName string) *batchv1.Job {
	log := logf.FromContext(ctx)
	instanceID := pipelineInstance.GetInstanceID()

	// Build the per-binding download path map up front; reused by both
	// the init containers (where to write) and the filter containers
	// (where their VideoIn reads).
	downloadPath := make(map[string]string, len(sourceBindings))
	for _, b := range sourceBindings {
		downloadPath[b.FilterName] = perFilterInputPath(b.FilterName, bindingObjectKey(b))
	}

	// One init claimer per binding (direct mode).
	initContainers := make([]corev1.Container, 0, len(sourceBindings))
	for i, b := range sourceBindings {
		initContainers = append(initContainers, corev1.Container{
			Name:  claimerContainerName(b.FilterName),
			Image: r.ClaimerImage,
			Env:   r.buildDirectClaimerEnv(b, downloadPath[b.FilterName]),
			VolumeMounts: []corev1.VolumeMount{
				{Name: "workspace", MountPath: "/ws"},
			},
		})
		log.V(1).Info("Added direct-mode claimer", "index", i, "filter", b.FilterName, "object", bindingObjectKey(b), "destination", downloadPath[b.FilterName])
	}

	// Build filter containers with per-VideoIn VIDEO_INPUT_PATH env.
	filterContainers := r.buildBatchFilterContainersForMultiSource(pipeline, pipelineInstance, downloadPath)

	// GPU sharing reuse (same shape as single-source batch).
	if requiresGPU(filterContainers) {
		applyGPUContainerSharing(filterContainers, instanceGPUCount(pipelineInstance.Spec))
	}

	pipelineRequiresGPU := requiresGPU(filterContainers)
	nodeSelector := mergeNodeSelector(r.GPUNodeSelectorLabels, pipelineInstance.Spec.NodeSelector, pipelineRequiresGPU)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: pipelineInstance.Namespace,
			Labels:    buildPodLabels(instanceID, pipelineInstance),
		},
		Spec: batchv1.JobSpec{
			CompletionMode:          ptr.To(batchv1.NonIndexedCompletion),
			Completions:             ptr.To(int32(1)),
			Parallelism:             ptr.To(int32(1)),
			BackoffLimit:            ptr.To(int32(2)),
			TTLSecondsAfterFinished: ptr.To(int32(86400)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: buildPodLabels(instanceID, pipelineInstance),
				},
				Spec: corev1.PodSpec{
					RestartPolicy:    corev1.RestartPolicyNever,
					NodeSelector:     nodeSelector,
					RuntimeClassName: gpuRuntimeClassName(r.GPURuntimeClassName, pipelineRequiresGPU),
					ImagePullSecrets: pipeline.Spec.ImagePullSecrets,
					Volumes: []corev1.Volume{
						{
							Name: "workspace",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
					InitContainers: initContainers,
					Containers:     filterContainers,
				},
			},
		},
	}

	applyInstanceScheduling(&job.Spec.Template.Spec, pipelineInstance.Spec)

	// Mount filter image volumes (PLAT-1095); no-op when no filter declares any.
	applyImageVolumes(&job.Spec.Template.Spec, pipeline.Spec.Filters)
	return job
}

// buildDirectClaimerEnv assembles the env for a direct-mode claimer init
// container. The presence of S3_OBJECT_KEY tells the claimer to skip
// Valkey and download exactly that key (see cmd/claimer/main.go).
func (r *PipelineInstanceReconciler) buildDirectClaimerEnv(b ResolvedSourceBinding, destPath string) []corev1.EnvVar {
	bucket := b.Source.Spec.Bucket
	env := []corev1.EnvVar{
		{Name: "S3_BUCKET", Value: bucket.Name},
		{Name: "S3_OBJECT_KEY", Value: bindingObjectKey(b)},
		{Name: "VIDEO_INPUT_PATH", Value: destPath},
		{Name: "S3_ENDPOINT", Value: bucket.Endpoint},
		{Name: "S3_REGION", Value: bucket.Region},
		{Name: "S3_USE_PATH_STYLE", Value: fmt.Sprintf("%t", bucket.UsePathStyle)},
		{Name: "S3_INSECURE_SKIP_TLS_VERIFY", Value: fmt.Sprintf("%t", bucket.InsecureSkipTLSVerify)},
	}
	if bucket.CredentialsSecret != nil {
		env = append(env,
			corev1.EnvVar{
				Name: "S3_ACCESS_KEY_ID",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: bucket.CredentialsSecret.Name},
						Key:                  "accessKeyId",
					},
				},
			},
			corev1.EnvVar{
				Name: "S3_SECRET_ACCESS_KEY",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: bucket.CredentialsSecret.Name},
						Key:                  "secretAccessKey",
					},
				},
			},
		)
	}
	return env
}

// buildBatchFilterContainersForMultiSource walks pipeline.Spec.Filters
// and builds the container slice. For each filter whose name matches a
// binding's FilterName, injects VIDEO_INPUT_PATH = the destination the
// matching claimer wrote to. Non-matching filters are built unchanged
// — they're downstream consumers and their `sources` config wires them
// to siblings via tcp://localhost, exactly like single-source today.
func (r *PipelineInstanceReconciler) buildBatchFilterContainersForMultiSource(pipeline *pipelinesv1alpha1.Pipeline, pipelineInstance *pipelinesv1alpha1.PipelineInstance, downloadPath map[string]string) []corev1.Container {
	containers := make([]corev1.Container, 0, len(pipeline.Spec.Filters))
	for _, filter := range pipeline.Spec.Filters {
		configEnv := make([]corev1.EnvVar, 0, len(filter.Config))
		for _, cfg := range filter.Config {
			configEnv = append(configEnv, corev1.EnvVar{
				Name:  "FILTER_" + strings.ToUpper(cfg.Name),
				Value: cfg.Value,
			})
		}

		// Per-binding env: the VideoIn container whose name matches a
		// binding picks up VIDEO_INPUT_PATH = its claimer's download
		// destination, so authors write `sources: file://$(VIDEO_INPUT_PATH)`.
		//
		// VIDEO_INPUT_PATH MUST precede the FILTER_* config env in the
		// list: Kubernetes dependent-env expansion only resolves $(VAR)
		// references to variables defined EARLIER — a forward reference
		// stays a literal string and the VideoIn fails to open its input.
		// (Mirrors the streaming builder's RTSP-env-first ordering.)
		env := make([]corev1.EnvVar, 0, len(configEnv)+1)
		if path, ok := downloadPath[filter.Name]; ok {
			env = append(env,
				corev1.EnvVar{Name: "VIDEO_INPUT_PATH", Value: path},
			)
		}
		env = append(env, configEnv...)

		// GPU + tracing env (mirrors single-source batch).
		if filter.Resources != nil && containerResourcesRequireGPU(*filter.Resources) {
			if r.GPULibraryPath != "" {
				env = append(env, corev1.EnvVar{Name: ldLibraryPathEnvName, Value: r.GPULibraryPath})
			}
			if r.GPUBinPath != "" {
				env = append(env, corev1.EnvVar{Name: appendPathEnvName, Value: r.GPUBinPath})
			}
		}
		env = append(env, r.tracingEnvVars(pipelineInstance)...)
		env = append(env, filter.Env...)

		c := corev1.Container{
			Name:            filter.Name,
			Image:           filter.Image,
			Command:         filter.Command,
			Args:            filter.Args,
			Env:             env,
			ImagePullPolicy: filter.ImagePullPolicy,
			VolumeMounts: []corev1.VolumeMount{
				{Name: "workspace", MountPath: "/ws"},
			},
		}
		if filter.Resources != nil {
			c.Resources = *filter.Resources
		}
		containers = append(containers, c)
	}
	return containers
}

// bindingObjectKey resolves the exact S3 object a binding's claimer must
// download: Bucket.Prefix used as the full key. The platform's media model
// has no separate object concept — single-file medias ship their full
// object key in prefix (the agent parses the media URL path into it), and
// the legacy queue path has always relied on prefix-as-key listing for
// single-file sources. Direct mode uses the same value as the exact GET
// key; a folder-style prefix fails at download with a per-claimer error
// naming the key.
func bindingObjectKey(b ResolvedSourceBinding) string {
	if b.Source == nil || b.Source.Spec.Bucket == nil {
		return ""
	}
	return b.Source.Spec.Bucket.Prefix
}

// claimerContainerName derives the init-claimer container name for a
// binding's filter. Kubernetes container names are DNS-1123 labels capped at
// 63 characters, and the CRD allows filterName itself to be 63 characters —
// "claimer-" (8 chars) plus a 63-char filterName would be 71. So the
// filterName is truncated to 55 characters (63 - len("claimer-")) before
// prefixing. Truncation is a deterministic prefix cut so repeated reconciles
// render byte-identical Job specs; trailing hyphens left by the cut are
// trimmed because DNS-1123 labels must end alphanumeric. Collisions between
// two 63-char filterNames sharing a 55-char prefix are theoretically possible
// but rejected by the apiserver at Job create (duplicate container name) —
// an operator-visible failure rather than a silent one.
func claimerContainerName(filterName string) string {
	const maxFilterNameLen = 63 - len("claimer-") // 55
	if len(filterName) > maxFilterNameLen {
		filterName = strings.TrimRight(filterName[:maxFilterNameLen], "-")
	}
	return "claimer-" + filterName
}

// perFilterInputPath computes the workspace path the claimer writes its
// downloaded object to. The extension is preserved from the bucket key
// when present (so openfilter's VideoIn can sniff the format from the
// file path); otherwise we default to ".mp4".
func perFilterInputPath(filterName, objectKey string) string {
	ext := filepath.Ext(objectKey)
	if ext == "" {
		ext = ".mp4"
	}
	return fmt.Sprintf("/ws/%s%s", filterName, ext)
}

// isDirectModeSingleBinding reports whether a single-binding batch instance
// should run in direct-download mode: the binding must name a filter
// container (plural `sources` form — the legacy broadcast sentinel has no
// container to scope the per-filter input path to) and its bucket must
// declare `singleObject: true` (prefix is a full object key, nothing to
// scan). Multi-binding instances always run direct; bindings without the
// flag keep the queue-based prefix-scan flow.
func isDirectModeSingleBinding(bindings []ResolvedSourceBinding) bool {
	if len(bindings) != 1 {
		return false
	}
	b := bindings[0]
	return b.FilterName != "" &&
		b.Source != nil &&
		b.Source.Spec.Bucket != nil &&
		b.Source.Spec.Bucket.SingleObject
}

// degradeBatchValidation marks a multi-source batch validation failure:
// Degraded=True with the given reason, Progressing=False (a
// validation-blocked instance is not making progress; Succeeded is left
// untouched — it records a past terminal outcome). The status write error
// is propagated so controller-runtime retries — these validation paths
// intentionally don't requeue, so a swallowed write failure would leave the
// server-side status stale forever.
func (r *PipelineInstanceReconciler) degradeBatchValidation(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance, reason, msg string) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	r.setCondition(pipelineInstance, ConditionTypeDegraded, metav1.ConditionTrue, reason, msg)
	r.setCondition(pipelineInstance, ConditionTypeProgressing, metav1.ConditionFalse, reason, msg)
	if statusErr := r.Status().Update(ctx, pipelineInstance); statusErr != nil {
		if apierrors.IsConflict(statusErr) {
			return ctrl.Result{Requeue: true}, nil
		}
		log.Error(statusErr, "Failed to update status after multi-source batch validation")
		return ctrl.Result{}, statusErr
	}
	return ctrl.Result{}, nil
}
