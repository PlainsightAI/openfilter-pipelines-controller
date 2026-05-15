/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
	"github.com/PlainsightAI/openfilter-pipelines-controller/internal/tracing"
)

// These tests pin the build/apply sibling topology against the actual
// reconcile paths (reconcileBatch + reconcileStreaming create and update
// branches). Without these, a refactor that nests apply inside build,
// drops the apply Start altogether, or restructures buildJob /
// ensureStreamingDeployment so the spans take a new parent would still
// pass every other unit test in this package — the helper-level coverage
// in pipelineinstance_controller_span_test.go exercises extractTraceContext
// and endPhaseSpan but never reaches the call sites that emit the per-phase
// spans inside the reconcile paths.
//
// Span topology contract (PLAT-1028, restated for future readers):
//   - One PipelineInstanceReconciler.build span per Job/Deployment write.
//   - One PipelineInstanceReconciler.apply span per Job/Deployment write.
//   - Both are SIBLINGS rooted at the surrounding Reconcile span — neither
//     is the parent of the other. Nesting apply inside build (or vice
//     versa) obscures the phase boundary in the Cloud Trace waterfall.
//
// We construct the Reconcile span manually here so the tests can exercise
// reconcileBatch / reconcileStreaming directly without dragging the full
// Reconcile() prelude (CR Get, traceparent extract, finalizer dance) into
// the assertion surface. The reconcile-path code under test does not
// inspect the parent span; only that its emitted spans share the supplied
// ctx as their parent.

const (
	reconcileSpanName = "PipelineInstanceReconciler.Reconcile"
	buildSpanName     = "PipelineInstanceReconciler.build"
	applySpanName     = "PipelineInstanceReconciler.apply"
)

// reconcileSpanScheme returns a scheme registered with the core Kubernetes
// types and the pipelines API group so the fake client can roundtrip the
// objects the reconcile paths Get/Create/Patch.
func reconcileSpanScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := scheme.AddToScheme(s); err != nil {
		t.Fatalf("register core scheme: %v", err)
	}
	if err := pipelinesv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("register pipelines scheme: %v", err)
	}
	return s
}

// findUniqueSpan returns the single recorded span with the given name and
// fails the test if the count is anything other than one. A duplicate
// emission (e.g. an accidental retry that produces two build spans in a
// single Reconcile) is just as much a regression as a missing span, so
// matching the first hit silently would hide that failure mode.
func findUniqueSpan(t *testing.T, spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	var matches []sdktrace.ReadOnlySpan
	for _, s := range spans {
		if s.Name() == name {
			matches = append(matches, s)
		}
	}
	if len(matches) != 1 {
		recorded := make([]string, 0, len(spans))
		for _, s := range spans {
			recorded = append(recorded, s.Name())
		}
		t.Fatalf("expected exactly one span named %q, got %d (recorded: %v)", name, len(matches), recorded)
	}
	return matches[0]
}

// assertBuildAndApplyAreSiblingsOfReconcile pins the three contract
// properties at once: build is a child of Reconcile, apply is a child of
// Reconcile, and neither is the parent of the other. Factored out so each
// branch (batch / streaming-create / streaming-update) reads as data
// rather than 30 lines of parent-id arithmetic.
func assertBuildAndApplyAreSiblingsOfReconcile(t *testing.T, spans []sdktrace.ReadOnlySpan) {
	t.Helper()
	reconcileSpan := findUniqueSpan(t, spans, reconcileSpanName)
	buildSpan := findUniqueSpan(t, spans, buildSpanName)
	applySpan := findUniqueSpan(t, spans, applySpanName)

	reconcileID := reconcileSpan.SpanContext().SpanID()
	buildID := buildSpan.SpanContext().SpanID()
	applyID := applySpan.SpanContext().SpanID()

	if got := buildSpan.Parent().SpanID(); got != reconcileID {
		t.Errorf("build span parent = %s, want Reconcile span %s", got, reconcileID)
	}
	if got := applySpan.Parent().SpanID(); got != reconcileID {
		t.Errorf("apply span parent = %s, want Reconcile span %s", got, reconcileID)
	}
	if applySpan.Parent().SpanID() == buildID {
		t.Error("apply span is nested inside build (expected siblings rooted at Reconcile)")
	}
	if buildSpan.Parent().SpanID() == applyID {
		t.Error("build span is nested inside apply (expected siblings rooted at Reconcile)")
	}
}

// withReconcileSpan starts a span named after the production Reconcile
// span and returns the resulting context plus an end-function. Tests use
// this to simulate the parent the real Reconcile() establishes without
// hauling in the rest of that method's preconditions.
func withReconcileSpan(ctx context.Context) (context.Context, func()) {
	ctx, span := tracing.Tracer().Start(ctx, reconcileSpanName, trace.WithSpanKind(trace.SpanKindInternal))
	return ctx, func() { span.End() }
}

// TestReconcileBatch_BuildAndApplyAreSiblingsOfReconcile drives reconcileBatch
// through the Job-create path (the only batch path that actually emits
// build/apply — once a Job exists, ensureJob short-circuits and produces
// no spans). The PI is pre-seeded with Status.StartTime and a non-zero
// totalFiles so initializePipelineInstance returns the "already
// initialized" branch and the test focuses on the build/apply emission
// site without needing a full Valkey seeding flow.
func TestReconcileBatch_BuildAndApplyAreSiblingsOfReconcile(t *testing.T) {
	recorder := installRecordingTracerProvider(t)

	sch := reconcileSpanScheme(t)

	pipeline := &pipelinesv1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pipeline", Namespace: "default"},
		Spec: pipelinesv1alpha1.PipelineSpec{
			Mode: pipelinesv1alpha1.PipelineModeBatch,
			Filters: []pipelinesv1alpha1.Filter{
				{Name: "f", Image: "filter:latest"},
			},
		},
	}
	source := &pipelinesv1alpha1.PipelineSource{
		ObjectMeta: metav1.ObjectMeta{Name: "test-source", Namespace: "default"},
	}
	startTime := metav1.NewTime(time.Now())
	pi := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "batch-pi",
			Namespace: "default",
			UID:       types.UID("batch-pi-uid"),
		},
		Spec: pipelinesv1alpha1.PipelineInstanceSpec{
			PipelineRef: pipelinesv1alpha1.PipelineReference{Name: pipeline.Name},
			SourceRef:   pipelinesv1alpha1.SourceReference{Name: source.Name},
		},
		Status: pipelinesv1alpha1.PipelineInstanceStatus{
			StartTime: &startTime,
			Counts: &pipelinesv1alpha1.FileCounts{
				TotalFiles: 1,
			},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(sch).
		WithStatusSubresource(&pipelinesv1alpha1.PipelineInstance{}).
		WithObjects(pi, pipeline, source).
		Build()

	r := &PipelineInstanceReconciler{
		Client:       c,
		Scheme:       sch,
		ValkeyClient: &MockValkeyClient{},
		ClaimerImage: "claimer:latest",
		ValkeyAddr:   "valkey:6379",
	}

	ctx, endReconcile := withReconcileSpan(context.Background())
	if _, err := r.reconcileBatch(ctx, pi, pipeline, source); err != nil {
		t.Fatalf("reconcileBatch returned error: %v", err)
	}
	endReconcile()

	assertBuildAndApplyAreSiblingsOfReconcile(t, recorder.Ended())
}

// makeStreamingFixtures returns the Pipeline / PipelineSource / PipelineInstance
// triple used by both streaming branches. Pre-seeds the finalizer and
// StartTime so reconcileStreaming reaches ensureStreamingDeployment in a
// single call (no early-return-with-Requeue cycle to drive).
func makeStreamingFixtures(name string) (*pipelinesv1alpha1.Pipeline, *pipelinesv1alpha1.PipelineSource, *pipelinesv1alpha1.PipelineInstance) {
	pipeline := &pipelinesv1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "stream-pipeline-" + name, Namespace: "default"},
		Spec: pipelinesv1alpha1.PipelineSpec{
			Mode: pipelinesv1alpha1.PipelineModeStream,
			Filters: []pipelinesv1alpha1.Filter{
				{Name: "f", Image: "filter:latest"},
			},
		},
	}
	source := &pipelinesv1alpha1.PipelineSource{
		ObjectMeta: metav1.ObjectMeta{Name: "stream-source-" + name, Namespace: "default"},
		Spec: pipelinesv1alpha1.PipelineSourceSpec{
			RTSP: &pipelinesv1alpha1.RTSPSource{Host: "rtsp", Port: 8554, Path: "/live"},
		},
	}
	startTime := metav1.NewTime(time.Now())
	pi := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "stream-pi-" + name,
			Namespace:  "default",
			UID:        types.UID("stream-pi-uid-" + name),
			Finalizers: []string{FinalizerStreamingCleanup},
		},
		Spec: pipelinesv1alpha1.PipelineInstanceSpec{
			PipelineRef: pipelinesv1alpha1.PipelineReference{Name: pipeline.Name},
			SourceRef:   pipelinesv1alpha1.SourceReference{Name: source.Name},
		},
		Status: pipelinesv1alpha1.PipelineInstanceStatus{
			StartTime: &startTime,
			Streaming: &pipelinesv1alpha1.StreamingStatus{},
		},
	}
	return pipeline, source, pi
}

// TestReconcileStreaming_Create_BuildAndApplyAreSiblingsOfReconcile drives
// reconcileStreaming through the Deployment-create branch
// (ensureStreamingDeployment's apierrors.IsNotFound path, where
// SetControllerReference + r.Create fire under the build/apply spans).
func TestReconcileStreaming_Create_BuildAndApplyAreSiblingsOfReconcile(t *testing.T) {
	recorder := installRecordingTracerProvider(t)

	sch := reconcileSpanScheme(t)
	pipeline, source, pi := makeStreamingFixtures("create")

	c := fake.NewClientBuilder().
		WithScheme(sch).
		WithStatusSubresource(&pipelinesv1alpha1.PipelineInstance{}).
		WithObjects(pi, pipeline, source).
		Build()

	r := &PipelineInstanceReconciler{
		Client:       c,
		Scheme:       sch,
		ValkeyClient: &MockValkeyClient{},
	}

	ctx, endReconcile := withReconcileSpan(context.Background())
	if _, err := r.reconcileStreaming(ctx, pi, pipeline, source); err != nil {
		t.Fatalf("reconcileStreaming (create) returned error: %v", err)
	}
	endReconcile()

	assertBuildAndApplyAreSiblingsOfReconcile(t, recorder.Ended())
}

// TestReconcileStreaming_Update_BuildAndApplyAreSiblingsOfReconcile drives
// reconcileStreaming through the Deployment-update branch by pre-seeding a
// live Deployment that matches what ensureStreamingDeployment expects to
// find. The update branch closes the build span before patching (the
// owner ref already exists on the live resource), but the apply span
// still has to be a sibling of build, not parent or child. A refactor
// that reordered Start/End so apply lifted build's context (or vice
// versa) would only fail here, not in the create-branch test.
func TestReconcileStreaming_Update_BuildAndApplyAreSiblingsOfReconcile(t *testing.T) {
	recorder := installRecordingTracerProvider(t)

	sch := reconcileSpanScheme(t)
	pipeline, source, pi := makeStreamingFixtures("update")

	// Pre-seed the live Deployment so ensureStreamingDeployment's Get
	// succeeds and it takes the Patch branch. The Spec body doesn't
	// matter for the span-topology assertion; only the existence under
	// the deterministic name PipelineInstance "<name>-deploy" does.
	existing := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pi.Name + "-deploy",
			Namespace: pi.Namespace,
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(sch).
		WithStatusSubresource(&pipelinesv1alpha1.PipelineInstance{}).
		WithObjects(pi, pipeline, source, existing).
		Build()

	r := &PipelineInstanceReconciler{
		Client:       c,
		Scheme:       sch,
		ValkeyClient: &MockValkeyClient{},
	}

	ctx, endReconcile := withReconcileSpan(context.Background())
	if _, err := r.reconcileStreaming(ctx, pi, pipeline, source); err != nil {
		t.Fatalf("reconcileStreaming (update) returned error: %v", err)
	}
	endReconcile()

	assertBuildAndApplyAreSiblingsOfReconcile(t, recorder.Ended())
}
