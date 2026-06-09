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
	"strings"
	"testing"

	"go.opentelemetry.io/otel/baggage"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
	"github.com/PlainsightAI/openfilter-pipelines-controller/internal/tracing"
)

// baggageOf returns a context that carries the supplied baggage members.
// Helper for the tracing.LiftBaggageToSpan tests below — bypasses
// extractTraceContext so the assertion is scoped to the stamp logic, not
// the lift.
func baggageOf(t *testing.T, kvs ...string) context.Context {
	t.Helper()
	members := make([]baggage.Member, 0, len(kvs))
	for _, kv := range kvs {
		// Format is "key=value". Split on the first '=' so values
		// containing '=' (allowed by the spec) survive.
		i := strings.IndexByte(kv, '=')
		if i < 0 {
			t.Fatalf("baggageOf: malformed entry %q (want key=value)", kv)
		}
		m, err := baggage.NewMember(kv[:i], kv[i+1:])
		if err != nil {
			t.Fatalf("baggageOf: NewMember(%q, %q): %v", kv[:i], kv[i+1:], err)
		}
		members = append(members, m)
	}
	bag, err := baggage.New(members...)
	if err != nil {
		t.Fatalf("baggageOf: baggage.New: %v", err)
	}
	return baggage.ContextWithBaggage(context.Background(), bag)
}

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

// TestStampBaggageOnSpan_AllowlistedMembersAreStamped is the positive
// half of the bounded-stamping contract: identity members named in
// baggageSpanAllowlist (organization.id, project.id, user.id) MUST land
// on the span as string attributes verbatim.
func TestStampBaggageOnSpan_AllowlistedMembersAreStamped(t *testing.T) {
	recorder := installRecordingTracerProvider(t)

	ctx := baggageOf(t,
		"organization.id=org-123",
		"project.id=proj-456",
		"user.id=user-789",
	)
	ctx, span := tracing.Tracer().Start(ctx, "test.stamp.allowlist")
	tracing.LiftBaggageToSpan(ctx, baggageSpanAllowlist)
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
		"organization.id": "org-123",
		"project.id":      "proj-456",
		"user.id":         "user-789",
	} {
		if got[k] != want {
			t.Errorf("attr %q: got %q, want %q", k, got[k], want)
		}
	}
}

// TestStampBaggageOnSpan_DropsNonAllowlistedMembers is the load-bearing
// negative half of the bounded-stamping contract: any baggage key NOT in
// baggageSpanAllowlist must be dropped silently. Guards against an
// upstream regression (an agent that accidentally puts PII into baggage,
// a future producer introducing diagnostic keys at scale) landing those
// values in Cloud Trace attribute storage. Pairs with the allowlist
// rationale comment in pipelineinstance_controller.go.
func TestStampBaggageOnSpan_DropsNonAllowlistedMembers(t *testing.T) {
	recorder := installRecordingTracerProvider(t)

	ctx := baggageOf(t,
		"user.id=user-789",        // allowlisted, must land
		"email=alice%40corp.test", // not allowlisted, must be dropped
		"session.token=opaque",    // not allowlisted, must be dropped
	)
	ctx, span := tracing.Tracer().Start(ctx, "test.stamp.dropped")
	tracing.LiftBaggageToSpan(ctx, baggageSpanAllowlist)
	span.End()

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected one ended span, got %d", len(spans))
	}
	got := map[string]string{}
	for _, attr := range spans[0].Attributes() {
		got[string(attr.Key)] = attr.Value.AsString()
	}
	if got["user.id"] != "user-789" {
		t.Errorf("allowlisted user.id missing: got %q", got["user.id"])
	}
	for _, dropped := range []string{"email", "session.token"} {
		if _, ok := got[dropped]; ok {
			t.Errorf("non-allowlisted baggage key %q leaked onto span as %q", dropped, got[dropped])
		}
	}
}

// TestStampBaggageOnSpan_AllNonAllowlistedIsNoOp is the empty-attribute
// guard: when every baggage member is non-allowlisted, the function must
// not call span.SetAttributes at all (so the span carries no spurious
// empty-attribute event). Catches a regression where the `if len(attrs)
// == 0` early-return is removed.
func TestStampBaggageOnSpan_AllNonAllowlistedIsNoOp(t *testing.T) {
	recorder := installRecordingTracerProvider(t)

	ctx := baggageOf(t, "email=alice%40corp.test", "session.token=opaque")
	ctx, span := tracing.Tracer().Start(ctx, "test.stamp.empty")
	tracing.LiftBaggageToSpan(ctx, baggageSpanAllowlist)
	span.End()

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected one ended span, got %d", len(spans))
	}
	if attrs := spans[0].Attributes(); len(attrs) != 0 {
		t.Errorf("expected no attributes on the span, got %d: %v", len(attrs), attrs)
	}
}
