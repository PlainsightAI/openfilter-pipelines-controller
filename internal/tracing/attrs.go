/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package tracing

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
)

// ReconcileOutcome categorises how a Reconcile call returned, stamped on the
// root reconcile span via AttrReconcileOutcome. The closed set keeps the trace
// UI's "outcome" facet predictable across services.
type ReconcileOutcome string

const (
	// ReconcileOutcomeComplete means the reconcile returned (Result{}, nil)
	// with no requeue request — the loop is at rest until the next watch
	// event.
	ReconcileOutcomeComplete ReconcileOutcome = "complete"

	// ReconcileOutcomeRequeue means the reconcile asked controller-runtime
	// to re-enter (Requeue or RequeueAfter) without surfacing an error.
	ReconcileOutcomeRequeue ReconcileOutcome = "requeue"

	// ReconcileOutcomeError means the reconcile returned a non-nil error,
	// which controller-runtime translates into a backoff requeue.
	ReconcileOutcomeError ReconcileOutcome = "error"
)

// ApplyResult categorises how an apply phase mutated (or didn't mutate) the
// kube-apiserver, stamped via AttrApplyResult. Mirrors controller-runtime's
// OperationResult vocabulary so traces line up with reconcile logs that quote
// it directly.
type ApplyResult string

const (
	// ApplyResultCreated means the apply call materialised a new object.
	ApplyResultCreated ApplyResult = "created"

	// ApplyResultUpdated means the apply call patched/updated an existing
	// object. Apply paths in this controller deliberately stamp `updated`
	// even for no-op MergeFrom patches (the kube-apiserver returns 200
	// regardless), so there is intentionally no `unchanged` variant.
	ApplyResultUpdated ApplyResult = "updated"
)

// Canonical attribute keys for the cross-domain Cloud Trace queries called
// out in PLAT-1000 / PLAT-1028. Centralised so a typo in a key string
// becomes a lint failure here instead of a silent miss in the trace UI.
// The matching typed builders (PipelineInstanceUID, PipelineUID, …) below
// are the intended entry points; reach for the bare constants only when
// something exotic (reflection-driven enrichment, a builder that needs a
// pre-validated string) needs the raw key.
//
// The structlint `attrkey` analyzer (`tools/structlint/`) wired into
// `make lint-struct` rejects raw `attribute.String(<one of these keys>, …)`
// calls anywhere outside this package — every domain stamp goes through the
// builders below or the matching constant is added here first. Add a new
// entry to BOTH the constant list and the `attrkeyCanonical` map in
// `tools/structlint/attrkey.go` when introducing a new canonical attribute.
//
// Naming convention: snake_case after the dot (`pipeline_instance.uid`, not
// `pipelineinstance.uid`) matches plainsight-api's `pkg/tracing/attrs.go`
// so a Cloud Trace query `pipeline_instance.uid="<uuid>"` returns spans
// from API + agent + controller without per-service casing skew.
const (
	// AttrPipelineInstanceUID is the K8s ObjectMeta.UID of the
	// PipelineInstance CR being reconciled. Distinct from `pipeline_instance.id`
	// which plainsight-api uses for the canonical business UUID — when those
	// values diverge in practice (agent-created CRs vs. test-created CRs),
	// querying by `.uid` returns the K8s identity and querying by `.id` returns
	// the business identity.
	AttrPipelineInstanceUID = "pipeline_instance.uid"

	// AttrPipelineInstanceID is the canonical business UUID used by
	// plainsight-api, plainsight-deployment-agent, and openfilter for trace
	// joining. It holds the same value as `pipeline_instance.name` in production
	// environments.
	AttrPipelineInstanceID = "pipeline_instance.id"

	// AttrPipelineInstanceName is the K8s metadata.name of the
	// PipelineInstance CR. Useful for kubectl cross-referencing from a
	// trace.
	AttrPipelineInstanceName = "pipeline_instance.name"

	// AttrPipelineInstanceNamespace is the K8s metadata.namespace of the
	// PipelineInstance CR.
	AttrPipelineInstanceNamespace = "pipeline_instance.namespace"

	// AttrPipelineUID is the K8s ObjectMeta.UID of the parent Pipeline CR.
	// NOT the same as openfilter's `pipeline.id` span attribute, which is
	// the canonical bare PipelineInstance UUID written by
	// plainsight-deployment-agent into the PIPELINE_ID env var. The two
	// keys disambiguate by suffix (`pipeline.uid` is the K8s identity of
	// the Pipeline template; `pipeline.id` is the canonical run identity
	// owned by the agent).
	AttrPipelineUID = "pipeline.uid"

	// AttrPipelineMode is the resolved reconcile mode ("batch" or
	// "stream"), stamped after defaulting so the trace UI can filter by
	// effective mode rather than the raw spec field.
	AttrPipelineMode = "pipeline.mode"

	// AttrPipelineName is the K8s metadata.name of the parent Pipeline CR.
	// Useful for kubectl cross-referencing from a trace; complements
	// `pipeline.uid` (the K8s identity) for human-readable lookup.
	AttrPipelineName = "pipeline.name"

	// AttrReconcileOutcome is the closed-set summary of how a Reconcile
	// call returned. See ReconcileOutcome for the value vocabulary.
	AttrReconcileOutcome = "reconcile.outcome"

	// AttrClaimAcquired is true when the reconcile actually performed
	// per-instance initialization (queue seed + status snapshot) on this
	// pass; false when the PipelineInstance was already initialized on a
	// prior reconcile. Lets the trace UI distinguish first-claim spans
	// from steady-state no-ops without scraping logs.
	AttrClaimAcquired = "claim.acquired"

	// AttrBuildContainerCount is the number of filter containers in the
	// rendered pod spec.
	AttrBuildContainerCount = "build.container.count"

	// AttrBuildGPU is true when at least one rendered filter container
	// requests a positive nvidia.com/gpu quantity.
	AttrBuildGPU = "build.gpu"

	// AttrBuildReplicas is the streaming-mode replica count on the
	// rendered Deployment.
	AttrBuildReplicas = "build.replicas"

	// AttrBuildParallelism is the batch-mode parallelism on the rendered
	// Job. Distinct attribute from build.replicas so the trace UI can
	// facet by mode without overloading a single key.
	AttrBuildParallelism = "build.parallelism"

	// AttrApplyResult is the closed-set summary of how the apply phase
	// affected the kube-apiserver. See ApplyResult for the value
	// vocabulary.
	AttrApplyResult = "apply.result"
)

// PipelineInstanceUID builds the canonical pipeline_instance.uid attribute
// from a PipelineInstance CR. Pulls the K8s UID rather than the configured
// instance ID so the value matches `kubectl get pipelineinstance -o
// jsonpath='{.metadata.uid}'` output for cross-referencing from a trace.
func PipelineInstanceUID(pi *pipelinesv1alpha1.PipelineInstance) attribute.KeyValue {
	return attribute.String(AttrPipelineInstanceUID, string(pi.UID))
}

// PipelineInstanceID builds the canonical pipeline_instance.id attribute.
func PipelineInstanceID(pi *pipelinesv1alpha1.PipelineInstance) attribute.KeyValue {
	return attribute.String(AttrPipelineInstanceID, pi.Name)
}

// PipelineInstanceName builds the canonical pipeline_instance.name attribute.
func PipelineInstanceName(pi *pipelinesv1alpha1.PipelineInstance) attribute.KeyValue {
	return attribute.String(AttrPipelineInstanceName, pi.Name)
}

// PipelineInstanceNamespace builds the canonical pipeline_instance.namespace attribute.
func PipelineInstanceNamespace(pi *pipelinesv1alpha1.PipelineInstance) attribute.KeyValue {
	return attribute.String(AttrPipelineInstanceNamespace, pi.Namespace)
}

// PipelineUID builds the canonical pipeline.uid attribute from a Pipeline CR.
// See AttrPipelineUID for why this is intentionally distinct from
// openfilter's `pipeline.id` span attribute.
func PipelineUID(p *pipelinesv1alpha1.Pipeline) attribute.KeyValue {
	return attribute.String(AttrPipelineUID, string(p.UID))
}

// PipelineMode builds the canonical pipeline.mode attribute. Takes the
// PipelineMode rather than a raw string so a typo at the call site is a
// compile error (the v1alpha1 type has a closed set of values).
func PipelineMode(mode pipelinesv1alpha1.PipelineMode) attribute.KeyValue {
	return attribute.String(AttrPipelineMode, string(mode))
}

// PipelineName builds the canonical pipeline.name attribute from a Pipeline CR.
func PipelineName(p *pipelinesv1alpha1.Pipeline) attribute.KeyValue {
	return attribute.String(AttrPipelineName, p.Name)
}

// ReconcileOutcomeAttr builds the canonical reconcile.outcome attribute.
// Takes the typed enum (rather than a raw string) so a typo at the call site
// is a compile error.
func ReconcileOutcomeAttr(outcome ReconcileOutcome) attribute.KeyValue {
	return attribute.String(AttrReconcileOutcome, string(outcome))
}

// ClaimAcquired builds the canonical claim.acquired attribute.
func ClaimAcquired(acquired bool) attribute.KeyValue {
	return attribute.Bool(AttrClaimAcquired, acquired)
}

// BuildContainerCount builds the canonical build.container.count attribute.
func BuildContainerCount(n int) attribute.KeyValue {
	return attribute.Int(AttrBuildContainerCount, n)
}

// BuildGPU builds the canonical build.gpu attribute.
func BuildGPU(gpu bool) attribute.KeyValue {
	return attribute.Bool(AttrBuildGPU, gpu)
}

// BuildReplicas builds the canonical build.replicas attribute (streaming mode).
func BuildReplicas(n int32) attribute.KeyValue {
	return attribute.Int(AttrBuildReplicas, int(n))
}

// BuildParallelism builds the canonical build.parallelism attribute (batch mode).
func BuildParallelism(n int32) attribute.KeyValue {
	return attribute.Int(AttrBuildParallelism, int(n))
}

// ApplyResultAttr builds the canonical apply.result attribute. Takes the
// typed enum so a typo at the call site is a compile error.
func ApplyResultAttr(result ApplyResult) attribute.KeyValue {
	return attribute.String(AttrApplyResult, string(result))
}

// Stamp sets the given attributes on the active span carried by ctx. It's a
// thin wrapper over `trace.SpanFromContext(ctx).SetAttributes(...)` whose
// only job is to keep the OTel imports out of domain code: a reconcile
// helper that wants to attach `pipeline.uid` should reach for
// `tracing.Stamp(ctx, tracing.PipelineUID(p))` and never have to know about
// `go.opentelemetry.io/otel/{attribute,trace}` directly.
//
// Safe in tracing-disabled builds: SpanFromContext returns a no-op span
// when no tracer provider is wired, and SetAttributes on that span is a
// no-op.
func Stamp(ctx context.Context, kv ...attribute.KeyValue) {
	if len(kv) == 0 {
		return
	}
	trace.SpanFromContext(ctx).SetAttributes(kv...)
}
