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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
	"github.com/PlainsightAI/openfilter-pipelines-controller/internal/tracing"
)

// installRecordingTracerProvider installs a span-recording TracerProvider
// AND the composite W3C TraceContext + Baggage propagator the controller
// uses in production. Both the previous global provider and propagator are
// restored on cleanup so cross-test ordering can't leak state.
func installRecordingTracerProvider(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})

	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	return recorder
}

const (
	// W3C TraceContext spec example values. The trace-id portion of the
	// traceparent is the value the extracted context's TraceID must match.
	upstreamTraceparent     = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	upstreamTraceID         = "0af7651916cd43dd8448eb211c80319c"
	upstreamRemoteSpanIDHex = "b7ad6b7169203331"
	upstreamTracestate      = "rojo=00f067aa0ba902b7,congo=t61rcWkgMzE"
)

// TestExtractTraceContext_ParentsToUpstreamTraceparent is the contract test
// for the agent → controller hop: the controller's span MUST inherit the
// upstream traceparent's trace ID so Cloud Trace stitches the hops into
// one trace. A typo in the annotation key, a propagator install that drops
// TraceContext, or carrier-key casing skew (the W3C spec uses lowercase)
// all surface as a fresh trace ID.
func TestExtractTraceContext_ParentsToUpstreamTraceparent(t *testing.T) {
	recorder := installRecordingTracerProvider(t)

	pi := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				TraceparentAnnotation: upstreamTraceparent,
				TracestateAnnotation:  upstreamTracestate,
			},
		},
	}

	ctx := extractTraceContext(context.Background(), pi)
	_, span := tracing.Tracer().Start(ctx, "test.controller.span")
	span.End()

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected one ended span, got %d", len(spans))
	}
	if got := spans[0].SpanContext().TraceID().String(); got != upstreamTraceID {
		t.Errorf("controller span did not inherit upstream trace id: got %q, want %q", got, upstreamTraceID)
	}
	parent := spans[0].Parent()
	if !parent.IsValid() {
		t.Fatal("expected a valid parent span context lifted from the traceparent annotation")
	}
	if got := parent.SpanID().String(); got != upstreamRemoteSpanIDHex {
		t.Errorf("parent span id mismatch: got %q, want %q", got, upstreamRemoteSpanIDHex)
	}
	if !parent.IsRemote() {
		t.Error("expected the lifted parent to be marked remote")
	}
}

// TestExtractTraceContext_NoAnnotationsLeavesContextUnchanged covers the
// OSS / pre-instrumented-caller path. The contract is silent fall-through:
// no error, no log, the resulting span just becomes a new trace root.
func TestExtractTraceContext_NoAnnotationsLeavesContextUnchanged(t *testing.T) {
	recorder := installRecordingTracerProvider(t)

	pi := &pipelinesv1alpha1.PipelineInstance{}
	ctx := extractTraceContext(context.Background(), pi)
	_, span := tracing.Tracer().Start(ctx, "test.controller.span")
	span.End()

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected one ended span, got %d", len(spans))
	}
	if spans[0].Parent().IsValid() {
		t.Errorf("expected new trace root, got parent context %v", spans[0].Parent())
	}
}

// TestExtractTraceContext_BareTracestateIsDropped enforces the W3C-spec
// invariant: tracestate alone (no traceparent) is meaningless and must be
// silently discarded. Mirrors the same drop tracingEnvVars enforces for
// filter env injection.
func TestExtractTraceContext_BareTracestateIsDropped(t *testing.T) {
	recorder := installRecordingTracerProvider(t)

	pi := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				TracestateAnnotation: upstreamTracestate,
			},
		},
	}
	ctx := extractTraceContext(context.Background(), pi)
	_, span := tracing.Tracer().Start(ctx, "test.controller.span")
	span.End()

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected one ended span, got %d", len(spans))
	}
	if spans[0].Parent().IsValid() {
		t.Error("bare tracestate must not establish a parent context")
	}
}

// TestExtractTraceContext_EmptyTraceparentValueIsTreatedAsAbsent guards the
// edge case where the agent writes the annotation key with an empty string
// (e.g. defensive default). An empty traceparent is invalid per W3C and
// must not produce a malformed parent.
func TestExtractTraceContext_EmptyTraceparentValueIsTreatedAsAbsent(t *testing.T) {
	recorder := installRecordingTracerProvider(t)

	pi := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				TraceparentAnnotation: "",
				TracestateAnnotation:  upstreamTracestate,
			},
		},
	}
	ctx := extractTraceContext(context.Background(), pi)
	_, span := tracing.Tracer().Start(ctx, "test.controller.span")
	span.End()

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected one ended span, got %d", len(spans))
	}
	if spans[0].Parent().IsValid() {
		t.Error("empty traceparent must not establish a parent context")
	}
}

// TestEndPhaseSpan_RecordsErrorAndStatus exercises the error-attribution
// path the per-phase spans (claim/build/apply) rely on. A non-nil err must
// produce a recorded error event AND set the span status to Error so a
// trace UI can highlight the failing phase without parsing the human
// message.
func TestEndPhaseSpan_RecordsErrorAndStatus(t *testing.T) {
	recorder := installRecordingTracerProvider(t)

	_, span := tracing.Tracer().Start(context.Background(), "test.phase.error")
	endPhaseSpan(span, errors.New("boom"))

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected one ended span, got %d", len(spans))
	}
	if spans[0].Status().Code != codes.Error {
		t.Errorf("expected span status Error, got %v", spans[0].Status().Code)
	}
	if msg := spans[0].Status().Description; msg != "boom" {
		t.Errorf("status description mismatch: got %q, want %q", msg, "boom")
	}
	events := spans[0].Events()
	if len(events) == 0 {
		t.Fatal("expected at least one event from RecordError")
	}
	if events[0].Name != "exception" {
		t.Errorf("expected RecordError to emit an 'exception' event, got %q", events[0].Name)
	}
}

// TestEndPhaseSpan_SuccessLeavesStatusUnset is the negative companion: a
// nil err must leave the span at the OTel default (Unset). Otherwise every
// successful reconcile would look like an explicit Ok in the trace UI,
// which obscures genuine Ok-by-policy spans.
func TestEndPhaseSpan_SuccessLeavesStatusUnset(t *testing.T) {
	recorder := installRecordingTracerProvider(t)

	_, span := tracing.Tracer().Start(context.Background(), "test.phase.ok")
	endPhaseSpan(span, nil)

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected one ended span, got %d", len(spans))
	}
	if spans[0].Status().Code != codes.Unset {
		t.Errorf("expected span status Unset on success, got %v", spans[0].Status().Code)
	}
	if events := spans[0].Events(); len(events) != 0 {
		t.Errorf("expected no events on the success path, got %d", len(events))
	}
}
