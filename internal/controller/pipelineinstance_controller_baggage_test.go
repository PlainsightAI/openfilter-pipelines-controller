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

	"go.opentelemetry.io/otel/baggage"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
	"github.com/PlainsightAI/openfilter-pipelines-controller/internal/tracing"
)

// TestExtractTraceContext_NoBaggageAnnotationLeavesContextUnchanged covers
// the OSS / no-identity-propagation path. Without the baggage annotation,
// baggage.FromContext on the returned context must remain empty.
func TestExtractTraceContext_NoBaggageAnnotationLeavesContextUnchanged(t *testing.T) {
	pi := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				TraceparentAnnotation: upstreamTraceparent,
			},
		},
	}
	// Install the composite propagator so Extract has Baggage support.
	installRecordingTracerProvider(t)

	ctx := extractTraceContext(context.Background(), pi)
	if got := baggage.FromContext(ctx).Len(); got != 0 {
		t.Errorf("expected zero baggage members, got %d", got)
	}
}

// TestExtractTraceContext_EmptyBaggageAnnotationLeavesContextUnchanged guards
// the defensive default where the agent writes the annotation key with an
// empty string (parity with the empty-traceparent edge).
func TestExtractTraceContext_EmptyBaggageAnnotationLeavesContextUnchanged(t *testing.T) {
	pi := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				BaggageAnnotation: "",
			},
		},
	}
	installRecordingTracerProvider(t)

	ctx := extractTraceContext(context.Background(), pi)
	if got := baggage.FromContext(ctx).Len(); got != 0 {
		t.Errorf("expected zero baggage members, got %d", got)
	}
}

// TestExtractTraceContext_SingleBaggageMember is the minimal positive case.
func TestExtractTraceContext_SingleBaggageMember(t *testing.T) {
	pi := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				TraceparentAnnotation: upstreamTraceparent,
				BaggageAnnotation:     "user.id=abc",
			},
		},
	}
	installRecordingTracerProvider(t)

	ctx := extractTraceContext(context.Background(), pi)
	got := baggage.FromContext(ctx).Member("user.id").Value()
	if got != "abc" {
		t.Errorf("expected user.id=abc baggage member, got %q", got)
	}
}

// TestExtractTraceContext_MultipleBaggageMembers exercises the comma-separated
// member encoding plainsight-api / plainsight-deployment-agent emit.
func TestExtractTraceContext_MultipleBaggageMembers(t *testing.T) {
	pi := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				TraceparentAnnotation: upstreamTraceparent,
				BaggageAnnotation:     "user.id=abc,organization.id=xyz",
			},
		},
	}
	installRecordingTracerProvider(t)

	ctx := extractTraceContext(context.Background(), pi)
	bag := baggage.FromContext(ctx)
	if got := bag.Member("user.id").Value(); got != "abc" {
		t.Errorf("user.id: got %q, want %q", got, "abc")
	}
	if got := bag.Member("organization.id").Value(); got != "xyz" {
		t.Errorf("organization.id: got %q, want %q", got, "xyz")
	}
}

// TestExtractTraceContext_BaggageWithoutTraceparent is the load-bearing new
// invariant: baggage is meaningful even without a parent trace context (OSS
// deployments that opt into identity propagation but not distributed tracing).
// A previous version of extractTraceContext gated baggage on traceparent
// presence and silently dropped identity members in that mode — this test
// guards against regressions to that behaviour.
func TestExtractTraceContext_BaggageWithoutTraceparent(t *testing.T) {
	pi := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				BaggageAnnotation: "organization.id=xyz",
			},
		},
	}
	installRecordingTracerProvider(t)

	ctx := extractTraceContext(context.Background(), pi)
	if got := baggage.FromContext(ctx).Member("organization.id").Value(); got != "xyz" {
		t.Errorf("baggage without traceparent must still be lifted: got %q, want %q", got, "xyz")
	}
}

// TestReconcileSpan_LiftsBaggageMembersAsAttributes verifies the integration
// shape: the Reconcile root span MUST carry baggage members as span
// attributes so cross-service identity (organization.id, user.id) is visible
// in Cloud Trace's attribute panel without scraping baggage propagation
// internals.
//
// Implementation parity with Reconcile: build a context the same way
// extractTraceContext would, start a span with the production tracer, and
// run the same lifting loop. This exercises the lifting code shape rather
// than driving an envtest reconcile end-to-end.
func TestReconcileSpan_LiftsBaggageMembersAsAttributes(t *testing.T) {
	recorder := installRecordingTracerProvider(t)

	pi := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				TraceparentAnnotation: upstreamTraceparent,
				BaggageAnnotation:     "user.id=abc,organization.id=xyz",
			},
		},
	}
	ctx := extractTraceContext(context.Background(), pi)
	ctx, span := tracing.Tracer().Start(ctx, "PipelineInstanceReconciler.Reconcile")
	stampBaggageOnSpan(ctx, span)
	span.End()

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected one ended span, got %d", len(spans))
	}
	got := map[string]string{}
	for _, attr := range spans[0].Attributes() {
		got[string(attr.Key)] = attr.Value.AsString()
	}
	for k, want := range map[string]string{
		"user.id":         "abc",
		"organization.id": "xyz",
	} {
		if got[k] != want {
			t.Errorf("baggage attribute %q: got %q, want %q", k, got[k], want)
		}
	}
}
