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
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net/http"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
	"github.com/PlainsightAI/openfilter-pipelines-controller/internal/queue"
	"github.com/PlainsightAI/openfilter-pipelines-controller/internal/tracing"
)

const (
	// Annotation keys for pod queue metadata
	AnnotationMessageID = "queue.valkey.mid"
	AnnotationFile      = "queue.file"
	AnnotationAttempts  = "queue.attempts"

	// Annotation values
	AnnotationValueTrue = "true"

	// Condition types
	ConditionTypeProgressing = "Progressing"
	ConditionTypeSucceeded   = "Succeeded"
	ConditionTypeDegraded    = "Degraded"

	// Degraded condition reasons
	ReasonPipelineNotFound       = "PipelineNotFound"
	ReasonPipelineSourceNotFound = "PipelineSourceNotFound"

	// Reconciliation intervals
	StatusUpdateInterval = 30 * time.Second

	// DefaultPipelineRefGracePeriod is the window after a PipelineInstance is
	// created during which a NotFound on its referenced Pipeline CR is treated
	// as a transient informer-cache miss (requeue) rather than a hard Degraded
	// condition. The deployment-agent creates Pipeline and PipelineInstance
	// CRs back-to-back; the controller's cache for the new Pipeline can lag the
	// new PipelineInstance's reconcile by hundreds of ms — enough to write a
	// false-positive Degraded that the agent's status poller then treats as
	// terminal. See pipelineInstancesForPipeline (watch-driven requeue) for the
	// complementary mechanism that wakes a waiting PipelineInstance the moment
	// its Pipeline appears in the cache.
	DefaultPipelineRefGracePeriod = 30 * time.Second

	// pipelineRefMissingRequeueAfter is how long to wait before re-checking
	// after a transient Pipeline NotFound when we're still inside the grace
	// period. Short enough to feel snappy; the watch-driven requeue below
	// usually wins anyway.
	pipelineRefMissingRequeueAfter = 1 * time.Second

	// DefaultVideoInputPath is where the claimer stores downloaded artifacts when not overridden.
	DefaultVideoInputPath = "/ws/input.mp4"

	// DefaultValkeyNSSecretName is the default name for per-namespace Valkey credentials secrets.
	DefaultValkeyNSSecretName = "valkey-ns-credentials"

	// FinalizerValkeyCredentials ensures Valkey ACL users are cleaned up on deletion.
	FinalizerValkeyCredentials = "filter.plainsight.ai/valkey-credentials"

	// FinalizerStreamingCleanup ensures streaming resources (Deployment, Services) are cleaned up on deletion.
	FinalizerStreamingCleanup = "filter.plainsight.ai/streaming-cleanup"

	// TraceparentAnnotation is the PipelineInstance annotation key carrying the
	// W3C `traceparent` header that the upstream span context wrote during the
	// API → controller → filter handoff. The controller copies it into the
	// TRACEPARENT env var on each filter container so openfilter's OTel SDK
	// continues the trace. Owned cross-repo with plainsight-deployment-agent
	// (PLAT-851), which writes the same key.
	TraceparentAnnotation = "traces.opentelemetry.io/traceparent"

	// TracestateAnnotation is the PipelineInstance annotation key carrying the
	// W3C `tracestate` header. Propagated as TRACESTATE env var alongside
	// TRACEPARENT so vendor-specific trace context survives the controller hop.
	TracestateAnnotation = "traces.opentelemetry.io/tracestate"

	// BaggageAnnotation is the PipelineInstance annotation key carrying the
	// W3C `baggage` header. Identity attributes (organization.id, project.id,
	// user.id, …) ride here so they survive the API → agent → controller hop
	// even when no traceparent is present (OSS deployments that opt into
	// identity propagation but not distributed tracing). Owned cross-repo with
	// plainsight-deployment-agent, which writes the same key.
	BaggageAnnotation = "traces.opentelemetry.io/baggage"
)

// ValkeyClientInterface defines the interface for Valkey operations
type ValkeyClientInterface interface {
	CreateStreamAndGroup(ctx context.Context, streamKey, groupName string) error
	GetStreamLength(ctx context.Context, streamKey string) (int64, error)
	GetConsumerGroupLag(ctx context.Context, streamKey, groupName string) (int64, error)
	GetPendingCount(ctx context.Context, streamKey, groupName string) (int64, error)
	GetPendingForConsumer(ctx context.Context, streamKey, groupName, consumer string, count int64) ([]string, error)
	GetPendingEntryDetails(ctx context.Context, streamKey, groupName string, minIdleTime int64, count int64) ([]queue.PendingEntry, error)
	AckMessage(ctx context.Context, streamKey, groupName, messageID string) error
	EnqueueFileWithAttempts(ctx context.Context, streamKey, runID, filepath string, attempts int) (string, error)
	AddToDLQ(ctx context.Context, dlqKey, runID, filepath string, attempts int, reason string) error
	AutoClaim(ctx context.Context, streamKey, groupName, consumerName string, minIdleTime int64, count int64) ([]queue.XMessage, error)
	ClaimMessages(ctx context.Context, streamKey, groupName, consumerName string, minIdleTime int64, messageIDs ...string) ([]queue.XMessage, error)
	ReadRange(ctx context.Context, streamKey, start, end string, count int64) ([]queue.XMessage, error)
	DeleteMessages(ctx context.Context, streamKey string, messageIDs ...string) error
	EnsureACLUser(ctx context.Context, username, password, namespace string) error
	DeleteACLUser(ctx context.Context, username string) error
}

// PipelineInstanceReconciler reconciles a PipelineInstance object
type PipelineInstanceReconciler struct {
	client.Client
	Scheme                *runtime.Scheme
	ValkeyClient          ValkeyClientInterface
	ValkeyAddr            string
	ValkeyNSSecretName    string            // Name of the per-namespace Valkey credentials secret (default: valkey-ns-credentials)
	ClaimerImage          string            // Image for the claimer init container
	GPUNodeSelectorLabels map[string]string // Node selector labels applied to pods that request nvidia.com/gpu resources; nil disables the feature
	GPULibraryPath        string            // Value injected as LD_LIBRARY_PATH for GPU containers; empty string disables injection
	GPUBinPath            string            // Value injected as OPENFILTER_APPEND_PATH for GPU containers; empty string disables injection

	// TelemetryExporterType and TelemetryExporterOTLPEndpoint are injected into
	// filter containers as TELEMETRY_EXPORTER_TYPE and TELEMETRY_EXPORTER_OTLP_ENDPOINT
	// so openfilter's OTel client ships spans and metrics to the configured collector.
	// Both empty disables injection and openfilter falls back to its silent exporter.
	TelemetryExporterType         string
	TelemetryExporterOTLPEndpoint string

	// PipelineRefGracePeriod overrides DefaultPipelineRefGracePeriod. Zero means
	// "use the default". Set to a small non-zero value (e.g. 1*time.Nanosecond)
	// in tests that need to assert the post-grace Degraded path without waiting
	// for wall-clock time to elapse.
	PipelineRefGracePeriod time.Duration
}

// pipelineRefGracePeriod returns the configured grace period or the default
// when unset. Kept private so callers don't accidentally read the zero value
// directly.
func (r *PipelineInstanceReconciler) pipelineRefGracePeriod() time.Duration {
	if r.PipelineRefGracePeriod > 0 {
		return r.PipelineRefGracePeriod
	}
	return DefaultPipelineRefGracePeriod
}

// +kubebuilder:rbac:groups=filter.plainsight.ai,resources=pipelineinstances,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=filter.plainsight.ai,resources=pipelineinstances/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=filter.plainsight.ai,resources=pipelineinstances/finalizers,verbs=update
// +kubebuilder:rbac:groups=filter.plainsight.ai,resources=pipelines,verbs=get;list;watch
// +kubebuilder:rbac:groups=filter.plainsight.ai,resources=pipelinesources,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments/status,verbs=get
// +kubebuilder:rbac:groups=apps,resources=replicasets,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=replicasets/status,verbs=get
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/status,verbs=get
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;delete

// ensureNamespaceValkeyCredentials ensures a per-namespace Valkey ACL user and corresponding
// secret exist in the target namespace. The ACL user is restricted to keys
// matching ns:<namespace>:* so it can only access its own namespace's data.
func (r *PipelineInstanceReconciler) ensureNamespaceValkeyCredentials(ctx context.Context, namespace string) error {
	log := logf.FromContext(ctx)

	secretName := r.ValkeyNSSecretName
	if secretName == "" {
		secretName = DefaultValkeyNSSecretName
	}

	username := queue.ValkeyUsernameForNamespace(namespace)

	// Check if the secret already exists
	existing := &corev1.Secret{}
	err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: namespace}, existing)
	if err == nil {
		// Secret exists — validate contents and ensure ACL user is up to date
		if existing.Data == nil {
			return fmt.Errorf("namespace Valkey secret %s/%s has no data", namespace, secretName)
		}
		storedUsername, ok := existing.Data["valkey-username"]
		if !ok {
			return fmt.Errorf("namespace Valkey secret %s/%s is missing key %q", namespace, secretName, "valkey-username")
		}
		if string(storedUsername) != username {
			return fmt.Errorf("namespace Valkey secret %s/%s has unexpected username %q, expected %q", namespace, secretName, string(storedUsername), username)
		}
		storedPassword, ok := existing.Data["valkey-password"]
		if !ok || len(storedPassword) == 0 {
			return fmt.Errorf("namespace Valkey secret %s/%s has missing or empty password", namespace, secretName)
		}
		if err := r.ValkeyClient.EnsureACLUser(ctx, username, string(storedPassword), namespace); err != nil {
			return fmt.Errorf("failed to ensure ACL user %s: %w", username, err)
		}
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to check namespace Valkey secret in %s: %w", namespace, err)
	}

	// Generate a random password
	passwordBytes := make([]byte, 32)
	if _, err := rand.Read(passwordBytes); err != nil {
		return fmt.Errorf("failed to generate random password: %w", err)
	}
	password := base64.RawURLEncoding.EncodeToString(passwordBytes)

	// Create the secret first — the secret is the source of truth for the password.
	// If two reconciles race, the loser gets AlreadyExists and re-reads the winner's
	// password to ensure the ACL user matches.
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "openfilter-pipelines-controller",
				"app.kubernetes.io/component":  "valkey-credentials",
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"valkey-username": []byte(username),
			"valkey-password": []byte(password),
		},
	}
	if err := r.Create(ctx, secret); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create namespace Valkey secret in %s: %w", namespace, err)
		}
		// Race: another reconcile created it first — re-read and validate
		if err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: namespace}, secret); err != nil {
			return fmt.Errorf("failed to re-read namespace Valkey secret after race in %s: %w", namespace, err)
		}
		if secret.Data == nil {
			return fmt.Errorf("namespace Valkey secret %s/%s has no data after race re-read", namespace, secretName)
		}
		storedUsername, ok := secret.Data["valkey-username"]
		if !ok || string(storedUsername) != username {
			return fmt.Errorf("namespace Valkey secret %s/%s has unexpected username %q after race re-read, expected %q", namespace, secretName, string(storedUsername), username)
		}
		storedPassword, ok := secret.Data["valkey-password"]
		if !ok || len(storedPassword) == 0 {
			return fmt.Errorf("namespace Valkey secret %s/%s has missing or empty password after race re-read", namespace, secretName)
		}
		password = string(storedPassword)
	}

	// Set the ACL user with the password from the secret (source of truth)
	if err := r.ValkeyClient.EnsureACLUser(ctx, username, password, namespace); err != nil {
		return fmt.Errorf("failed to create ACL user %s: %w", username, err)
	}

	log.Info("Created namespace Valkey credentials", "namespace", namespace, "username", username)
	return nil
}

// cleanupNamespaceValkeyCredentials removes the per-namespace Valkey ACL user and secret
// when the last PipelineInstance in a namespace is being deleted.
// Returns an error if cleanup fails so the finalizer is retained for retry.
func (r *PipelineInstanceReconciler) cleanupNamespaceValkeyCredentials(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance) error {
	log := logf.FromContext(ctx)
	namespace := pipelineInstance.Namespace

	// List other PipelineInstances in the same namespace
	list := &pipelinesv1alpha1.PipelineInstanceList{}
	if err := r.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return fmt.Errorf("failed to list PipelineInstances for ACL cleanup: %w", err)
	}

	// Count instances that are NOT being deleted (excluding the current one)
	active := 0
	for i := range list.Items {
		if list.Items[i].UID != pipelineInstance.UID && list.Items[i].DeletionTimestamp.IsZero() {
			active++
		}
	}
	if active > 0 {
		return nil // other active instances exist, keep the credentials
	}

	// Verify the namespace secret exists and is managed by us before cleaning up
	secretName := r.ValkeyNSSecretName
	if secretName == "" {
		secretName = DefaultValkeyNSSecretName
	}
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: secretName, Namespace: namespace}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return nil // secret already gone, nothing to clean up
		}
		return fmt.Errorf("failed to get namespace Valkey secret %s: %w", secretName, err)
	}
	if secret.Labels["app.kubernetes.io/managed-by"] != "openfilter-pipelines-controller" {
		log.Info("Skipping cleanup — namespace Valkey secret not managed by this controller", "secret", secretName)
		return nil
	}

	// Delete the ACL user from Valkey (only after confirming we own the secret)
	username := queue.ValkeyUsernameForNamespace(namespace)
	if err := r.ValkeyClient.DeleteACLUser(ctx, username); err != nil {
		return fmt.Errorf("failed to delete Valkey ACL user %s: %w", username, err)
	}
	log.Info("Deleted Valkey ACL user", "username", username)

	// Delete the namespace secret
	if err := r.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete namespace Valkey secret %s: %w", secretName, err)
	}
	log.Info("Deleted namespace Valkey secret", "namespace", namespace)
	return nil
}

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// The reconciler branches on Pipeline mode (Batch or Stream) and delegates to
// mode-specific reconciliation functions defined in:
// - pipelineinstance_controller_batch.go: Batch mode reconciliation
// - pipelineinstance_controller_streaming.go: Streaming mode reconciliation
//
// The reconcile body runs under a span rooted at the upstream traceparent the
// agent stamped onto the CR, so the controller hop sits between the agent's
// span and the filter pods' spans in the trace waterfall (PLAT-1000). When
// tracing is disabled (no OTel endpoint configured), the global tracer is a
// noop and span operations have negligible cost.
func (r *PipelineInstanceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, err error) {
	log := logf.FromContext(ctx)

	// Fetch the PipelineInstance
	pipelineInstance := &pipelinesv1alpha1.PipelineInstance{}
	if err := r.Get(ctx, req.NamespacedName, pipelineInstance); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("PipelineInstance resource not found, ignoring")
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get PipelineInstance")
		return ctrl.Result{}, err
	}

	// Extract the upstream W3C trace context from CR annotations BEFORE
	// starting the span so the controller's span is a child of the agent's
	// span. If no traceparent is present (OSS or pre-instrumented callers),
	// the controller's span becomes a new root — still useful for operator
	// debugging. Baggage is lifted from the CR independently of traceparent
	// (extractTraceContext) so identity members survive even on OSS hops.
	ctx = extractTraceContext(ctx, pipelineInstance)
	ctx, endSpan := tracing.StartSpan(ctx, spanReconcile,
		tracing.PipelineInstanceNamespace(pipelineInstance),
		tracing.PipelineInstanceName(pipelineInstance),
		tracing.PipelineInstanceUID(pipelineInstance),
		tracing.PipelineInstanceID(pipelineInstance),
	)
	// Lift baggage members onto the reconcile span so cross-service identity
	// (organization.id, project.id, user.id) is visible in the trace UI as
	// attributes — baggage members alone don't appear in span data, only in
	// downstream propagation. Done immediately after the span opens so the
	// values are visible across the full span lifetime.
	tracing.LiftBaggageToSpan(ctx, baggageSpanAllowlist)
	defer func() {
		tracing.Stamp(ctx, tracing.ReconcileOutcomeAttr(reconcileOutcome(result, err)))
		if err != nil {
			tracing.SpanError(ctx, err, err.Error())
		}
		endSpan()
	}()

	// Handle deletion: process finalizers in order (valkey credentials first, then mode-specific)
	if !pipelineInstance.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(pipelineInstance, FinalizerValkeyCredentials) {
			if err := r.cleanupNamespaceValkeyCredentials(ctx, pipelineInstance); err != nil {
				log.Error(err, "Failed to clean up Valkey credentials, will retry")
				return ctrl.Result{}, err
			}
			controllerutil.RemoveFinalizer(pipelineInstance, FinalizerValkeyCredentials)
			if err := r.Update(ctx, pipelineInstance); err != nil {
				log.Error(err, "Failed to remove Valkey credentials finalizer")
				return ctrl.Result{}, err
			}
			// Requeue so the next reconcile processes mode-specific finalizers
			// with a fresh resourceVersion (avoids "object has been modified" conflicts).
			return ctrl.Result{Requeue: true}, nil
		}
		// Valkey finalizer already handled — delegate to mode-specific cleanup.
		// reconcileStreaming handles the streaming-cleanup finalizer;
		// batch mode relies on owner references (no finalizer needed).
		return r.reconcileStreaming(ctx, pipelineInstance, nil, nil)
	}

	// Add Valkey credentials finalizer if not present (requeue to reconcile against the updated object)
	if !controllerutil.ContainsFinalizer(pipelineInstance, FinalizerValkeyCredentials) {
		controllerutil.AddFinalizer(pipelineInstance, FinalizerValkeyCredentials)
		if err := r.Update(ctx, pipelineInstance); err != nil {
			log.Error(err, "Failed to add Valkey credentials finalizer")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Get the Pipeline resource
	// Note: deletion is fully handled above (before reaching this point),
	// so DeletionTimestamp will always be zero here.
	//
	// r.Get reads through the manager's informer cache. The deployment-agent
	// creates Pipeline and PipelineInstance CRs back-to-back, so the first
	// reconcile here can fire before the Pipeline-CR cache has caught up.
	// Treat a NotFound during the grace window as transient: requeue without
	// flipping Degraded. A complementary Watches mapping (see SetupWithManager)
	// re-enqueues this PipelineInstance the moment its Pipeline appears in
	// the cache, so RequeueAfter is the floor, not the typical wake latency.
	//
	// The grace window is measured against pipelineInstance.CreationTimestamp,
	// not the first getPipeline call. The finalizer-add path above can return
	// and requeue once or twice before we reach this lookup, and under a
	// controller restart even more wall-clock can pass — so a fraction of the
	// 30s default is already spent before the first lookup. At 30s this gap
	// is well within the budget and the watch-driven wake usually fires
	// long before the floor; if the default ever shrinks, revisit this.
	pipeline, err := r.getPipeline(ctx, pipelineInstance)
	if err != nil {
		if apierrors.IsNotFound(err) && time.Since(pipelineInstance.CreationTimestamp.Time) < r.pipelineRefGracePeriod() {
			log.V(1).Info("Pipeline not yet visible in cache, requeueing",
				"pipelineRef", pipelineInstance.Spec.PipelineRef.Name,
				"age", time.Since(pipelineInstance.CreationTimestamp.Time))
			return ctrl.Result{RequeueAfter: pipelineRefMissingRequeueAfter}, nil
		}
		log.Error(err, "Failed to get Pipeline")
		r.setCondition(pipelineInstance, ConditionTypeDegraded, metav1.ConditionTrue, ReasonPipelineNotFound, err.Error())
		if err := r.Status().Update(ctx, pipelineInstance); err != nil {
			log.Error(err, "Failed to update status")
		}
		return ctrl.Result{}, err
	}

	// Pipeline is now observable — clear any prior Degraded/PipelineNotFound
	// condition so external watchers (the deployment-agent's status poller)
	// see the recovery. Without this, a transient NotFound that flipped
	// Degraded before the grace-period guard was in place would otherwise
	// linger and be read as a terminal failure.
	//
	// If the Status().Update conflicts (concurrent writer bumped the
	// resourceVersion between our Get and our Update), bail out and let
	// controller-runtime requeue with fresh state — continuing would just
	// have the rest of this reconcile's Status().Update calls hit the same
	// conflict against an in-memory object whose Conditions slice has
	// already been mutated. Non-conflict errors propagate the same way the
	// other Status().Update sites in this file do.
	if err := r.clearDegradedReason(ctx, pipelineInstance, ReasonPipelineNotFound); err != nil {
		if apierrors.IsConflict(err) {
			log.V(1).Info("Status update conflict while clearing stale Degraded; requeueing",
				"reason", ReasonPipelineNotFound)
			return ctrl.Result{Requeue: true}, nil
		}
		log.Error(err, "Failed to clear stale Degraded condition")
		return ctrl.Result{}, err
	}

	// Resolve all source bindings (legacy single SourceRef → one ResolvedSourceBinding
	// with empty FilterName for broadcast; new Sources → one binding per entry,
	// each scoped to a specific filter container).
	resolvedSources, err := r.resolveSourceBindings(ctx, pipelineInstance)
	if err != nil {
		log.Error(err, "Failed to resolve source bindings")
		r.setCondition(pipelineInstance, ConditionTypeDegraded, metav1.ConditionTrue, ReasonPipelineSourceNotFound, err.Error())
		if err := r.Status().Update(ctx, pipelineInstance); err != nil {
			log.Error(err, "Failed to update status")
		}
		return ctrl.Result{}, err
	}

	// Branch by pipeline mode
	mode := pipeline.Spec.Mode
	if mode == "" {
		mode = pipelinesv1alpha1.PipelineModeBatch // default
	}

	// Stamp domain attributes on the parent reconcile span (PLAT-1028).
	// Done lazily here — not at span Start — because pipeline.UID and
	// pipeline.mode are only known once the Pipeline CR has been fetched
	// and the mode default has been applied; stamping earlier would
	// either miss the default or require double-fetching.
	tracing.Stamp(ctx,
		tracing.PipelineUID(pipeline),
		tracing.PipelineName(pipeline),
		tracing.PipelineMode(mode),
	)

	if mode == pipelinesv1alpha1.PipelineModeStream {
		return r.reconcileStreaming(ctx, pipelineInstance, pipeline, resolvedSources)
	}

	// Default: Batch mode
	return r.reconcileBatch(ctx, pipelineInstance, pipeline, resolvedSources)
}

// Helper functions shared between batch and streaming modes are defined below.
// Mode-specific reconciliation logic is in:
// - pipelineinstance_controller_batch.go
// - pipelineinstance_controller_streaming.go

// ResolvedSourceBinding pairs a target filter name with the PipelineSource that
// feeds it. FilterName=="" is the legacy "broadcast to every filter container"
// sentinel produced when the PipelineInstance uses the deprecated singular
// `sourceRef` field. Non-empty FilterName targets the container whose
// `pipeline.spec.filters[].name` matches exactly.
type ResolvedSourceBinding struct {
	FilterName string
	Source     *pipelinesv1alpha1.PipelineSource
}

// resolveSourceBindings normalizes `Spec.SourceRef` (legacy, broadcast) and
// `Spec.Sources` (multi-source) into a uniform list of (filterName, source)
// pairs. The first error from any Get is returned with the binding context so
// the caller surfaces an operator-actionable message.
func (r *PipelineInstanceReconciler) resolveSourceBindings(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance) ([]ResolvedSourceBinding, error) {
	effective := pipelineInstance.Spec.EffectiveSources()
	if len(effective) == 0 {
		// Validation should catch this at admission, but a defensive error
		// here avoids a downstream nil-deref in the reconciler.
		return nil, fmt.Errorf("PipelineInstance has neither sourceRef nor sources set")
	}

	bindings := make([]ResolvedSourceBinding, 0, len(effective))
	for _, ref := range effective {
		namespace := pipelineInstance.Namespace
		if ref.SourceRef.Namespace != nil {
			namespace = *ref.SourceRef.Namespace
		}
		src := &pipelinesv1alpha1.PipelineSource{}
		if err := r.Get(ctx, types.NamespacedName{
			Name:      ref.SourceRef.Name,
			Namespace: namespace,
		}, src); err != nil {
			if ref.FilterName == "" {
				return nil, fmt.Errorf("failed to get pipeline source %q: %w", ref.SourceRef.Name, err)
			}
			return nil, fmt.Errorf("failed to get pipeline source %q (bound to filter %q): %w", ref.SourceRef.Name, ref.FilterName, err)
		}
		bindings = append(bindings, ResolvedSourceBinding{
			FilterName: ref.FilterName,
			Source:     src,
		})
	}
	return bindings, nil
}

// getCredentials retrieves S3 credentials for the pipelineSource bucket secret, if configured.
func (r *PipelineInstanceReconciler) getCredentials(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance, pipelineSource *pipelinesv1alpha1.PipelineSource) (string, string, error) {
	if pipelineSource.Spec.Bucket == nil {
		return "", "", nil
	}

	secretRef := pipelineSource.Spec.Bucket.CredentialsSecret
	if secretRef == nil {
		return "", "", nil
	}

	namespace := secretRef.Namespace
	if namespace == "" {
		namespace = pipelineInstance.Namespace
	}

	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: secretRef.Name, Namespace: namespace}, secret); err != nil {
		return "", "", fmt.Errorf("failed to get secret %s/%s: %w", namespace, secretRef.Name, err)
	}

	accessKey := string(secret.Data["accessKeyId"])
	secretKey := string(secret.Data["secretAccessKey"])

	if accessKey == "" || secretKey == "" {
		return "", "", fmt.Errorf("secret %s/%s missing required keys 'accessKeyId' or 'secretAccessKey'", namespace, secretRef.Name)
	}

	return accessKey, secretKey, nil
}

// listBucketFiles lists objects available to process for the pipelineSource configuration.
func (r *PipelineInstanceReconciler) listBucketFiles(ctx context.Context, pipelineSource *pipelinesv1alpha1.PipelineSource, accessKey, secretKey string) ([]string, error) {
	if pipelineSource.Spec.Bucket == nil {
		return nil, fmt.Errorf("pipelineSource has no bucket source configured")
	}

	bucket := pipelineSource.Spec.Bucket

	endpoint := bucket.Endpoint
	useSSL := true
	if endpoint != "" {
		if len(endpoint) > 7 && endpoint[:7] == "http://" {
			useSSL = false
			endpoint = endpoint[7:]
		} else if len(endpoint) > 8 && endpoint[:8] == "https://" {
			endpoint = endpoint[8:]
		}
	}

	var creds *credentials.Credentials
	if accessKey != "" && secretKey != "" {
		creds = credentials.NewStaticV4(accessKey, secretKey, "")
	} else {
		creds = credentials.NewStaticV4("", "", "")
	}

	var customTransport http.RoundTripper
	if bucket.InsecureSkipTLSVerify {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		customTransport = transport
	}

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:     creds,
		Secure:    useSSL,
		Region:    bucket.Region,
		Transport: customTransport,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}

	files := make([]string, 0, 100) // Pre-allocate with reasonable initial capacity
	objectCh := minioClient.ListObjects(ctx, bucket.Name, minio.ListObjectsOptions{
		Prefix:    bucket.Prefix,
		Recursive: true,
	})

	for object := range objectCh {
		if object.Err != nil {
			return nil, fmt.Errorf("error listing objects: %w", object.Err)
		}
		files = append(files, object.Key)
	}

	return files, nil
}

// getPipeline retrieves the Pipeline resource referenced by the PipelineInstance
func (r *PipelineInstanceReconciler) getPipeline(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance) (*pipelinesv1alpha1.Pipeline, error) {
	namespace := pipelineInstance.Namespace
	if pipelineInstance.Spec.PipelineRef.Namespace != nil {
		namespace = *pipelineInstance.Spec.PipelineRef.Namespace
	}

	pipeline := &pipelinesv1alpha1.Pipeline{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      pipelineInstance.Spec.PipelineRef.Name,
		Namespace: namespace,
	}, pipeline); err != nil {
		return nil, fmt.Errorf("failed to get pipeline: %w", err)
	}

	return pipeline, nil
}

// baggageSpanAllowlist bounds which W3C baggage members tracing.LiftBaggageToSpan
// copies onto the reconcile span. Baggage is a free-form propagation channel
// — any upstream service can inject arbitrary keys, and Cloud Trace retains
// span attributes for the full trace retention window — so an unbounded
// stamp would let a future agent regression accidentally exfiltrate PII
// (emails, session tokens, …) into trace storage.
//
// The set mirrors the identity tuple plainsight-api and
// plainsight-deployment-agent already stamp onto upstream spans:
// organization.id, project.id, user.id. New entries here must be coordinated
// cross-repo with both producers — adding a key the agent isn't yet
// producing is harmless (just a no-op), but stamping a key the agent
// produces without a matching entry here silently drops it from the trace UI.
//
// Baggage propagation itself is unfiltered — only the span-attribute lift is
// bounded, so downstream consumers reading baggage from ctx still see
// whatever the agent put in.
var baggageSpanAllowlist = map[string]struct{}{
	"organization.id": {},
	"project.id":      {},
	"user.id":         {},
}

// reconcileOutcome categorises a Reconcile return tuple for the
// reconcile.outcome span attribute. Error trumps requeue (a returned err is a
// failure even when paired with Requeue) so Cloud Trace's outcome facet doesn't
// hide retried failures behind the steady "requeue" bucket.
//
// Both `Requeue` and `RequeueAfter` count as a requeue. `Requeue` is
// deprecated in favour of `RequeueAfter`, but controller-runtime still
// honours it at runtime and the finalizer-bookkeeping paths in this
// controller still set it to trigger an immediate retry with a fresh
// resourceVersion. Consulting both fields keeps the trace facet honest
// regardless of which form a call site uses.
func reconcileOutcome(result ctrl.Result, err error) tracing.ReconcileOutcome {
	switch {
	case err != nil:
		return tracing.ReconcileOutcomeError
	// SA1019: reading the deprecated field is intentional — the
	// finalizer-bookkeeping call sites in this controller still set
	// Requeue=true, and skipping the read would silently misreport
	// those returns as `complete` in the trace facet.
	case result.Requeue || result.RequeueAfter > 0: //nolint:staticcheck // see comment above
		return tracing.ReconcileOutcomeRequeue
	default:
		return tracing.ReconcileOutcomeComplete
	}
}

// extractTraceContext lifts the W3C traceparent/tracestate/baggage that
// plainsight-deployment-agent (PLAT-851) wrote onto the CR into the supplied
// context using the globally registered text-map propagator.
//
// The annotation keys are normalized to lowercase carrier keys ("traceparent",
// "tracestate", "baggage") because the W3C TraceContext + Baggage propagators'
// Fields() are defined in lowercase and propagation.MapCarrier is
// case-sensitive.
//
// Baggage is lifted independently of traceparent: identity baggage members
// (organization.id, project.id, user.id, …) are meaningful even on OSS
// deployments that opt into identity propagation but not distributed tracing.
// Tracestate, by contrast, is meaningless without traceparent and is dropped
// when the parent context is absent (matching the invariant enforced by
// tracingEnvVars for filter env injection).
//
// When no annotation is present (OSS deployments, eager callers) the returned
// context is unchanged and the subsequent span becomes a root span — no error,
// no log, since this is an expected state.
func extractTraceContext(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance) context.Context {
	if len(pipelineInstance.Annotations) == 0 {
		return ctx
	}
	carrier := map[string]string{}
	if tp, ok := pipelineInstance.Annotations[TraceparentAnnotation]; ok && tp != "" {
		carrier["traceparent"] = tp
		if ts, ok := pipelineInstance.Annotations[TracestateAnnotation]; ok && ts != "" {
			carrier["tracestate"] = ts
		}
	}
	if bg, ok := pipelineInstance.Annotations[BaggageAnnotation]; ok && bg != "" {
		carrier["baggage"] = bg
	}
	return tracing.ContextFromCarrier(ctx, carrier)
}

// endPhase ends a child phase span (PLAT-1028: claim/build/apply granularity
// inside Reconcile) deterministically. When err is non-nil the error is
// recorded on the span and its status set to Error so per-phase failure
// attribution survives in Cloud Trace; otherwise the span ends Unset.
//
// spanCtx is the phase span's context (from tracing.StartSpan) and end is its
// returned closure. Pulled out of every call site instead of inlined as a
// `defer` so the span ends *before* the surrounding error-translation
// `return fmt.Errorf(...)` runs — keeping span duration tight to the actual
// phase work and preserving sibling (rather than nested) topology between
// adjacent phases.
func endPhase(spanCtx context.Context, end func(), err error) {
	if err != nil {
		tracing.SpanError(spanCtx, err, err.Error())
	}
	end()
}

// tracingEnvVars returns the env vars the controller injects into every
// filter container to propagate distributed-tracing context and configure
// openfilter's OTel exporter. The slice is appended to the container's env
// list BEFORE the user-supplied filter.Env, so any user-set entry with the
// same Name appears later and wins under kubelet's effective-env semantics.
//
// Cross-repo invariants kept here:
//   - TRACEPARENT / TRACESTATE annotation keys are owned jointly with
//     plainsight-deployment-agent (PLAT-851). See {Traceparent,Tracestate}Annotation.
//   - Env var names TRACEPARENT, TRACESTATE, TELEMETRY_EXPORTER_ENABLED,
//     TELEMETRY_EXPORTER_TYPE, and TELEMETRY_EXPORTER_OTLP_ENDPOINT are read
//     verbatim by openfilter (see openfilter/observability/{tracing,client}.py
//     and openfilter/filter_runtime/filter.py); do not rename without
//     coordinating with that repo.
//   - PIPELINE_INSTANCE_UID is reserved for the future PLAT-848 consumer and
//     is NOT yet read by any in-tree filter image — openfilter currently keys
//     telemetry off PIPELINE_ID (filter_runtime/filter.py, populating
//     OpenTelemetryClient(instance_id=self.pipeline_id)). It is pre-shipped
//     here so the contract surface is set when the consumer lands; remove or
//     rename it only after confirming nobody downstream has started reading
//     it.
//   - TELEMETRY_EXPORTER_ENABLED gates ALL telemetry init (tracer + meter)
//     in openfilter and defaults to false. It MUST travel with
//     TELEMETRY_EXPORTER_TYPE / TELEMETRY_EXPORTER_OTLP_ENDPOINT — without
//     it, configuring an exporter is a no-op and no spans/metrics ship.
//   - PIPELINE_ID is intentionally NOT injected here. plainsight-deployment-agent
//     owns it on Plainsight clusters and writes the canonical bare instance
//     UUID. On OSS clusters it stays unset; openfilter's `pipeline.id` span
//     attribute will be absent (safe), and OSS users can set it via Filter.Env
//     if they want it.
func (r *PipelineInstanceReconciler) tracingEnvVars(pipelineInstance *pipelinesv1alpha1.PipelineInstance) []corev1.EnvVar {
	envVars := make([]corev1.EnvVar, 0, 6)

	envVars = append(envVars, corev1.EnvVar{Name: "PIPELINE_INSTANCE_UID", Value: string(pipelineInstance.UID)})

	if tp, ok := pipelineInstance.Annotations[TraceparentAnnotation]; ok && tp != "" {
		envVars = append(envVars, corev1.EnvVar{Name: "TRACEPARENT", Value: tp})
		// tracestate is propagated only when traceparent is also present (the
		// W3C propagator requires the parent context to interpret it). Skipping
		// empty/missing tracestate is intentional — the upstream chain may
		// legitimately have no vendor-specific state to forward.
		if ts, ok := pipelineInstance.Annotations[TracestateAnnotation]; ok && ts != "" {
			envVars = append(envVars, corev1.EnvVar{Name: "TRACESTATE", Value: ts})
		}
	}

	if r.TelemetryExporterType != "" {
		// TELEMETRY_EXPORTER_ENABLED gates openfilter's tracer + meter init
		// (defaults to false); pair it with the exporter config so the three
		// env vars always travel together. validateTelemetryFlags rejects the
		// half-configured state at boot, but gating ENDPOINT under the same
		// TYPE check is defense-in-depth: a unit test constructing the
		// reconciler directly would otherwise bypass the boot validation and
		// emit a partial set of env vars.
		envVars = append(envVars, corev1.EnvVar{Name: "TELEMETRY_EXPORTER_ENABLED", Value: "true"})
		envVars = append(envVars, corev1.EnvVar{Name: "TELEMETRY_EXPORTER_TYPE", Value: r.TelemetryExporterType})
		envVars = append(envVars, corev1.EnvVar{Name: "TELEMETRY_EXPORTER_OTLP_ENDPOINT", Value: r.TelemetryExporterOTLPEndpoint})
	}

	return envVars
}

// setCondition sets or updates a condition in the PipelineInstance status
func (r *PipelineInstanceReconciler) setCondition(pipelineInstance *pipelinesv1alpha1.PipelineInstance, conditionType string, status metav1.ConditionStatus, reason, message string) {
	condition := metav1.Condition{
		Type:               conditionType,
		Status:             status,
		ObservedGeneration: pipelineInstance.Generation,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	}

	meta.SetStatusCondition(&pipelineInstance.Status.Conditions, condition)
}

// clearDegradedReason removes a Degraded condition whose Reason matches the
// given value and persists the status update. It's a no-op when no matching
// condition exists. Used to clear a stale Degraded set during a transient
// dependency miss once the dependency reappears, so external watchers (e.g.
// plainsight-deployment-agent) see the recovery instead of treating the
// previous condition as terminal.
func (r *PipelineInstanceReconciler) clearDegradedReason(ctx context.Context, pipelineInstance *pipelinesv1alpha1.PipelineInstance, reason string) error {
	cond := meta.FindStatusCondition(pipelineInstance.Status.Conditions, ConditionTypeDegraded)
	if cond == nil || cond.Reason != reason {
		return nil
	}
	meta.RemoveStatusCondition(&pipelineInstance.Status.Conditions, ConditionTypeDegraded)
	return r.Status().Update(ctx, pipelineInstance)
}

// SetupWithManager sets up the controller with the Manager.
func (r *PipelineInstanceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pipelinesv1alpha1.PipelineInstance{}).
		Owns(&batchv1.Job{}).
		Owns(&appsv1.Deployment{}).
		// Wake any PipelineInstance whose Pipeline reference resolves the moment
		// the Pipeline CR appears in the cache. Without this, a PipelineInstance
		// created back-to-back with its Pipeline would have to wait for the
		// in-Reconcile RequeueAfter floor; with it, the watch event fires the
		// reconcile immediately.
		Watches(
			&pipelinesv1alpha1.Pipeline{},
			handler.EnqueueRequestsFromMapFunc(r.pipelineInstancesForPipeline),
		).
		Named("pipelineinstance").
		Complete(r)
}

// pipelineInstancesForPipeline returns reconcile requests for every
// PipelineInstance in the Pipeline's namespace that references it by name.
// Wired in SetupWithManager so a Pipeline-CR create/update event wakes any
// PipelineInstance waiting on its Pipeline reference to resolve.
//
// Scope: same-namespace only. The list is bounded to pipeline.Namespace so a
// PipelineInstance living in namespace A that references a Pipeline in
// namespace B is never returned here when B's Pipeline event fires. Cross-
// namespace pipelineRefs are supported by the schema but not produced by
// plainsight-deployment-agent (it always co-locates the two CRs), so the
// optimization isn't worth the cost of a cluster-wide list. Cross-ns refs
// still recover correctly via the 1s requeue floor inside the grace window
// — just without the watch-driven instant wake. If cross-ns ever becomes
// common, switch to a field-indexed cross-namespace list keyed by
// pipelineRef.{namespace,name}.
func (r *PipelineInstanceReconciler) pipelineInstancesForPipeline(ctx context.Context, obj client.Object) []reconcile.Request {
	pipeline, ok := obj.(*pipelinesv1alpha1.Pipeline)
	if !ok {
		return nil
	}
	var list pipelinesv1alpha1.PipelineInstanceList
	if err := r.List(ctx, &list, client.InNamespace(pipeline.Namespace)); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list PipelineInstances for Pipeline watch",
			"pipeline", pipeline.Name, "namespace", pipeline.Namespace)
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for i := range list.Items {
		pi := &list.Items[i]
		if pi.Spec.PipelineRef.Name != pipeline.Name {
			continue
		}
		// PipelineRef.Namespace may override the PipelineInstance's own
		// namespace; honor that when filtering so a same-namespace match
		// doesn't fire when the ref actually points elsewhere.
		refNS := pi.Namespace
		if pi.Spec.PipelineRef.Namespace != nil {
			refNS = *pi.Spec.PipelineRef.Namespace
		}
		if refNS != pipeline.Namespace {
			continue
		}
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: pi.Namespace, Name: pi.Name},
		})
	}
	return requests
}
