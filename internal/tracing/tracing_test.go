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
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// TestInitTracerProvider_EmptyEndpointIsNoop covers the OSS / unit-test path
// where no collector is configured. Init must not dial, must not install a
// non-noop TracerProvider, and must return a non-nil shutdown so callers
// can defer unconditionally.
func TestInitTracerProvider_EmptyEndpointIsNoop(t *testing.T) {
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})

	shutdown, err := InitTracerProvider(context.Background(), "", "")
	if err != nil {
		t.Fatalf("InitTracerProvider with empty endpoint returned error: %v", err)
	}
	if shutdown == nil {
		t.Fatal("expected non-nil shutdown even on empty-endpoint path")
	}

	// Global tracer must remain whatever it was (the test runtime's noop or
	// a previously configured provider). The contract is "don't touch it",
	// not "set it to a specific value".
	if got := otel.GetTracerProvider(); got != prevTP {
		t.Errorf("empty endpoint must leave the global TracerProvider untouched, got %T", got)
	}

	if err := shutdown(context.Background()); err != nil {
		t.Errorf("noop shutdown returned error: %v", err)
	}
}

// TestInitTracerProvider_StripsScheme verifies the typo-friendly affordance:
// operators can paste a `http://host:4317` or `https://host:4317` endpoint
// from a wiki and Init still dials bare host:port. The scheme is purely
// cosmetic; the connection is always plaintext.
//
// Asserted by exercising the code path — a malformed endpoint that survives
// the strip would surface as a dial error or a panic. Anything cleanly
// returning a non-nil shutdown means the strip + dial accepted the input.
func TestInitTracerProvider_StripsScheme(t *testing.T) {
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})

	cases := []string{
		"http://otel-collector.monitoring.svc.cluster.local:4317",
		"https://otel-collector.monitoring.svc.cluster.local:4317/",
		"otel-collector.monitoring.svc.cluster.local:4317",
	}
	for _, endpoint := range cases {
		t.Run(endpoint, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			shutdown, err := InitTracerProvider(ctx, endpoint, "")
			if err != nil {
				t.Fatalf("InitTracerProvider(%q) returned error: %v", endpoint, err)
			}
			if shutdown == nil {
				t.Fatal("expected non-nil shutdown")
			}
			// Bound shutdown so a non-resolvable test endpoint can't hang
			// the test on its export-retry budget.
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer shutdownCancel()
			_ = shutdown(shutdownCtx)
		})
	}
}

// TestInitTracerProvider_InstallsW3CPropagators exercises the propagator
// install branch: after a real-endpoint init, the global text map
// propagator must understand both `traceparent` and `baggage` headers (the
// two carriers the controller's extractTraceContext + cross-service baggage
// rollouts depend on).
func TestInitTracerProvider_InstallsW3CPropagators(t *testing.T) {
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	shutdown, err := InitTracerProvider(ctx, "127.0.0.1:0", "")
	if err != nil {
		t.Fatalf("InitTracerProvider returned error: %v", err)
	}
	t.Cleanup(func() {
		c, cn := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cn()
		_ = shutdown(c)
	})

	// Round-trip a traceparent through the propagator. If only the
	// TraceContext propagator were installed (no Baggage), this still
	// passes; the explicit Baggage check below catches the regression
	// where someone replaces the composite with TraceContext-only.
	carrier := propagation.MapCarrier{
		"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		"baggage":     "user.id=alice,org.id=plainsight",
	}
	extracted := otel.GetTextMapPropagator().Extract(context.Background(), carrier)

	sc := trace.SpanContextFromContext(extracted)
	if !sc.IsValid() {
		t.Fatal("propagator did not extract a valid span context from traceparent — TraceContext propagator missing")
	}

	// Baggage propagator check: re-inject and look for the baggage header
	// in the carrier output. If Baggage isn't installed, Inject will only
	// emit traceparent.
	out := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(extracted, out)
	if _, ok := out["baggage"]; !ok {
		// Baggage entries from Extract carry through Inject only when the
		// Baggage propagator is in the composite. The traceparent we
		// extracted carries no baggage by itself — but the round-trip
		// above also extracted the baggage carrier. If Baggage were
		// missing, Inject would silently drop it.
		t.Errorf("Baggage propagator not installed: round-trip dropped the baggage header (got carrier %v)", out)
	}
}

// TestInitTracerProvider_GatesEmptyServiceVersion guards the wart R2 caught:
// passing serviceVersion="" through to semconv.ServiceVersion would attach
// `service.version=""` to every span (a present-but-empty attribute, not an
// absent one). The gate must drop the attribute entirely when the value is
// empty, and pass it through when the value is set.
func TestInitTracerProvider_GatesEmptyServiceVersion(t *testing.T) {
	cases := []struct {
		name           string
		serviceVersion string
		wantPresent    bool
	}{
		{"empty version is dropped", "", false},
		{"non-empty version passes through", "v1.2.3", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			prevTP := otel.GetTracerProvider()
			prevProp := otel.GetTextMapPropagator()
			t.Cleanup(func() {
				otel.SetTracerProvider(prevTP)
				otel.SetTextMapPropagator(prevProp)
			})

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			shutdown, err := InitTracerProvider(ctx, "127.0.0.1:0", c.serviceVersion)
			if err != nil {
				t.Fatalf("InitTracerProvider error: %v", err)
			}
			t.Cleanup(func() {
				cc, cn := context.WithTimeout(context.Background(), 200*time.Millisecond)
				defer cn()
				_ = shutdown(cc)
			})

			// TracerProvider.Resource() is unexported; fish the resource
			// out by registering an in-memory span recorder, emitting a
			// span, and reading ReadOnlySpan.Resource() — that's the same
			// resource the production exporter would attach.
			tp, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider)
			if !ok {
				t.Fatalf("global TracerProvider is %T, want *sdktrace.TracerProvider", otel.GetTracerProvider())
			}
			recorder := tracetest.NewSpanRecorder()
			tp.RegisterSpanProcessor(recorder)
			_, span := tp.Tracer("gate-test").Start(context.Background(), "gate.probe")
			span.End()
			recorded := recorder.Ended()
			if len(recorded) != 1 {
				t.Fatalf("expected one recorded span, got %d", len(recorded))
			}
			var got string
			var present bool
			for _, attr := range recorded[0].Resource().Attributes() {
				if string(attr.Key) == "service.version" {
					present = true
					got = attr.Value.AsString()
				}
			}
			if present != c.wantPresent {
				t.Errorf("service.version present=%v, want %v (got value %q)", present, c.wantPresent, got)
			}
			if c.wantPresent && got != c.serviceVersion {
				t.Errorf("service.version value: got %q, want %q", got, c.serviceVersion)
			}
		})
	}
}

// TestTracer_NamedFromConstant guards against drift in the instrumentation
// scope name — Cloud Trace queries that filter by `otel.scope.name` depend
// on it staying the canonical value.
func TestTracer_NamedFromConstant(t *testing.T) {
	if Tracer() == nil {
		t.Fatal("Tracer() returned nil")
	}
	// TracerName is the documented public surface; a rename is a breaking
	// change for any downstream Cloud Trace dashboard pinned to it.
	if TracerName != "github.com/PlainsightAI/openfilter-pipelines-controller" {
		t.Errorf("TracerName drifted: %q — Cloud Trace queries pinned to it will silently miss", TracerName)
	}
}
