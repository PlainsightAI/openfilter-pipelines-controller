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
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	k8stypes "k8s.io/apimachinery/pkg/types"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
)

// withRecordingTracerProvider installs a span-recording TracerProvider for
// the duration of the test and returns the recorder. The original global
// provider is restored on cleanup so tests don't leak state across the
// package.
func withRecordingTracerProvider(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	prevTP := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(prevTP) })

	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	otel.SetTracerProvider(tp)
	return recorder
}

// TestStamp_AppliesAttributesToActiveSpan exercises the full helper surface
// in one shot: the canonical-key constants, the typed builders, and Stamp
// itself. Anything that drifts (a typo'd key, a builder that wires the
// wrong constant, Stamp dropping arguments) shows up as a missing
// attribute.
func TestStamp_AppliesAttributesToActiveSpan(t *testing.T) {
	recorder := withRecordingTracerProvider(t)

	pi := &pipelinesv1alpha1.PipelineInstance{}
	pi.UID = k8stypes.UID("pi-uid-1234")
	pi.Name = "pi-name"
	pi.Namespace = "pi-ns"

	pipeline := &pipelinesv1alpha1.Pipeline{}
	pipeline.UID = k8stypes.UID("pipeline-uid-5678")

	mode := pipelinesv1alpha1.PipelineModeStream

	ctx, span := otel.Tracer("test").Start(context.Background(), "test.stamp")
	Stamp(ctx,
		PipelineInstanceUID(pi),
		PipelineInstanceName(pi),
		PipelineInstanceNamespace(pi),
		PipelineUID(pipeline),
		PipelineMode(mode),
	)
	span.End()

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected exactly one ended span, got %d", len(spans))
	}

	got := map[string]string{}
	for _, attr := range spans[0].Attributes() {
		got[string(attr.Key)] = attr.Value.AsString()
	}

	cases := []struct {
		key  string
		want string
	}{
		{AttrPipelineInstanceUID, "pi-uid-1234"},
		{AttrPipelineInstanceName, "pi-name"},
		{AttrPipelineInstanceNamespace, "pi-ns"},
		{AttrPipelineUID, "pipeline-uid-5678"},
		{AttrPipelineMode, string(mode)},
	}
	for _, c := range cases {
		if got[c.key] != c.want {
			t.Errorf("attr %q: got %q, want %q", c.key, got[c.key], c.want)
		}
	}
}

// TestStamp_NoArgsIsNoop guards the early-return: a domain that wants to
// stamp "all the standard attrs for this entity" can splat into Stamp
// without special-casing the empty slice.
func TestStamp_NoArgsIsNoop(t *testing.T) {
	recorder := withRecordingTracerProvider(t)

	ctx, span := otel.Tracer("test").Start(context.Background(), "test.stamp_empty")
	Stamp(ctx) // must not panic, must not add anything
	span.End()

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected exactly one ended span, got %d", len(spans))
	}
	if attrs := spans[0].Attributes(); len(attrs) != 0 {
		t.Errorf("empty Stamp call must not add attributes, got %d", len(attrs))
	}
}

// TestStamp_NoActiveSpanIsNoop covers the noop-tracer path used in
// production when OTEL_EXPORTER_OTLP_ENDPOINT is unset. Calling Stamp
// without an active span (a bare context) must not panic — domain code is
// allowed to call it unconditionally.
func TestStamp_NoActiveSpanIsNoop(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Stamp on a bare context panicked: %v", r)
		}
	}()
	pi := &pipelinesv1alpha1.PipelineInstance{}
	pi.UID = k8stypes.UID("nospan")
	Stamp(context.Background(), PipelineInstanceUID(pi))
}
