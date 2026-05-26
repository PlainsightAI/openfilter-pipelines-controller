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
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
	"github.com/PlainsightAI/openfilter-pipelines-controller/internal/tracing"
)

var errFakeBoom = errors.New("boom")

// schemeForPhaseTest returns a Scheme with the PipelineInstance + standard
// kube types registered so the fake client can owner-ref-stamp the
// Deployment back at a real PipelineInstance.
func schemeForPhaseTest(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := scheme.AddToScheme(s); err != nil {
		t.Fatalf("scheme.AddToScheme: %v", err)
	}
	if err := pipelinesv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("pipelinesv1alpha1.AddToScheme: %v", err)
	}
	return s
}

// findSpanByName returns the first recorded span with the given name, failing
// the test when none match.
func findSpanByName(t *testing.T, spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	for _, s := range spans {
		if s.Name() == name {
			return s
		}
	}
	t.Fatalf("no span recorded with name %q", name)
	return nil
}

// attrsByKey flattens span attributes into a key->value map for cheap lookup.
func attrsByKey(s sdktrace.ReadOnlySpan) map[string]attribute.Value {
	out := map[string]attribute.Value{}
	for _, a := range s.Attributes() {
		out[string(a.Key)] = a.Value
	}
	return out
}

// TestEnsureStreamingDeployment_BuildAndApplySpansCarryEnrichment exercises
// the create path of ensureStreamingDeployment under a recording tracer +
// in-memory kube client. Asserts the build span carries
// build.{container.count,gpu,replicas} and the apply span carries
// apply.result=created — the contract Cloud Trace reads to attribute time
// across reconcile phases (PLAT-1028 follow-on).
func TestEnsureStreamingDeployment_BuildAndApplySpansCarryEnrichment(t *testing.T) {
	recorder := installRecordingTracerProvider(t)

	s := schemeForPhaseTest(t)
	pi := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name: "stream-pi", Namespace: "default", UID: types.UID("stream-uid"),
		},
	}
	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{Name: "gpu-filter", Image: "f:latest", Resources: &corev1.ResourceRequirements{
					Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")},
				}},
				{Name: "cpu-filter", Image: "f:latest"},
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(pi).Build()
	r := &PipelineInstanceReconciler{Client: cli, Scheme: s}

	if err := r.ensureStreamingDeployment(context.Background(), pi, pipeline, nil); err != nil {
		t.Fatalf("ensureStreamingDeployment returned error: %v", err)
	}

	// Sanity: the Deployment landed in the fake.
	got := &appsv1.Deployment{}
	if err := cli.Get(context.Background(), types.NamespacedName{
		Name: "stream-pi-deploy", Namespace: "default",
	}, got); err != nil {
		t.Fatalf("expected Deployment to be created: %v", err)
	}

	spans := recorder.Ended()
	build := findSpanByName(t, spans, "PipelineInstanceReconciler.build")
	bAttrs := attrsByKey(build)

	if v, ok := bAttrs[tracing.AttrBuildContainerCount]; !ok || v.AsInt64() != 2 {
		t.Errorf("build.container.count: got %v ok=%v, want 2", v.Emit(), ok)
	}
	if v, ok := bAttrs[tracing.AttrBuildGPU]; !ok || !v.AsBool() {
		t.Errorf("build.gpu: got %v ok=%v, want true", v.Emit(), ok)
	}
	if v, ok := bAttrs[tracing.AttrBuildReplicas]; !ok || v.AsInt64() != 1 {
		t.Errorf("build.replicas: got %v ok=%v, want 1", v.Emit(), ok)
	}

	apply := findSpanByName(t, spans, "PipelineInstanceReconciler.apply")
	aAttrs := attrsByKey(apply)
	if v, ok := aAttrs[tracing.AttrApplyResult]; !ok || v.AsString() != string(tracing.ApplyResultCreated) {
		t.Errorf("apply.result: got %v ok=%v, want %q", v.Emit(), ok, tracing.ApplyResultCreated)
	}
}

// TestEnsureStreamingDeployment_UpdatePathSetsApplyResultUpdated exercises
// the update branch — a second ensureStreamingDeployment call against a
// pre-existing Deployment must produce apply.result=updated rather than
// created.
func TestEnsureStreamingDeployment_UpdatePathSetsApplyResultUpdated(t *testing.T) {
	s := schemeForPhaseTest(t)
	pi := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name: "stream-pi", Namespace: "default", UID: types.UID("stream-uid"),
		},
	}
	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{Name: "f", Image: "f:latest"},
			},
		},
	}

	cli := fake.NewClientBuilder().WithScheme(s).WithObjects(pi).Build()
	r := &PipelineInstanceReconciler{Client: cli, Scheme: s}

	// Create-path call first to seed the Deployment in the fake.
	if err := r.ensureStreamingDeployment(context.Background(), pi, pipeline, nil); err != nil {
		t.Fatalf("seed call returned error: %v", err)
	}

	// Now install the recorder so only the update-path spans are observed.
	recorder := installRecordingTracerProvider(t)
	if err := r.ensureStreamingDeployment(context.Background(), pi, pipeline, nil); err != nil {
		t.Fatalf("update call returned error: %v", err)
	}

	apply := findSpanByName(t, recorder.Ended(), "PipelineInstanceReconciler.apply")
	aAttrs := attrsByKey(apply)
	if v, ok := aAttrs[tracing.AttrApplyResult]; !ok || v.AsString() != string(tracing.ApplyResultUpdated) {
		t.Errorf("apply.result on update path: got %v ok=%v, want %q", v.Emit(), ok, tracing.ApplyResultUpdated)
	}
}

// TestReconcileOutcome_Categorisation guards the reconcile.outcome derivation
// across the return shapes Reconcile produces. A regression here would
// silently mis-categorise spans in Cloud Trace's outcome facet.
func TestReconcileOutcome_Categorisation(t *testing.T) {
	cases := []struct {
		name   string
		result ctrl.Result
		err    error
		want   tracing.ReconcileOutcome
	}{
		{"clean complete", ctrl.Result{}, nil, tracing.ReconcileOutcomeComplete},
		{"requeue after", ctrl.Result{RequeueAfter: time.Second}, nil, tracing.ReconcileOutcomeRequeue},
		{"bare requeue", ctrl.Result{Requeue: true}, nil, tracing.ReconcileOutcomeRequeue},
		{"error trumps requeue after", ctrl.Result{RequeueAfter: time.Second}, errFakeBoom, tracing.ReconcileOutcomeError},
		{"error trumps bare requeue", ctrl.Result{Requeue: true}, errFakeBoom, tracing.ReconcileOutcomeError},
		{"bare error", ctrl.Result{}, errFakeBoom, tracing.ReconcileOutcomeError},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := reconcileOutcome(c.result, c.err)
			if got != c.want {
				t.Errorf("got %q, want %q", got, c.want)
			}
		})
	}
}
