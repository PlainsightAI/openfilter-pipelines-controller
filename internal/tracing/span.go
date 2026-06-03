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

package tracing

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// StartSpan opens a child span on the controller's tracer, stamps the given
// canonical attributes on it at creation time, and returns the ctx carrying
// the new span plus a single `end` closure the caller must call (or defer).
//
// This is the canonical child-span idiom for controller code. The
// closure-return shape lets call sites stay out of
// `go.opentelemetry.io/otel/trace` entirely — the structlint `spanidiom`
// analyzer rejects that import outside this package, which is what keeps the
// instrumentation surface uniform across the controller. If a caller ever
// needs richer span access (RecordError, SetStatus), use SpanError below
// rather than reaching for the raw OTel SDK at the call site — that's how the
// helper set stays in lockstep with the analyzer's accepted-call list in
// tools/structlint/spancov.go.
//
// spanName follows the existing convention: the operation within the
// controller subsystem ("PipelineInstanceReconciler.claim",
// "PipelineInstanceReconciler.Reconcile"). Declare it as a named constant
// (per-package tracing.go) — the `spannames` analyzer rejects string literals
// here so a typo can't silently produce an unqueryable span.
//
// Safe in tracing-disabled builds: when InitTracerProvider got an empty
// endpoint the global tracer is a noop, so Start returns a noop span and
// end() is a no-op.
func StartSpan(ctx context.Context, spanName string, attrs ...attribute.KeyValue) (context.Context, func()) {
	ctx, span := Tracer().Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attrs...),
	)
	return ctx, func() { span.End() }
}

// StartConsumerSpan opens a span tagged with SpanKindConsumer — the right kind
// for queue/stream consumers and worker tick paths, where the span represents
// handling a message rather than serving a request (Cloud Trace renders
// consumer-kind spans distinctly and lets queueing dashboards split work from
// request latency). Otherwise identical to StartSpan; same closure-return
// shape, same noop-safe behaviour. Use this rather than reaching for
// trace.WithSpanKind at the call site — keeping consumer-kind centralised here
// is how spanidiom stays universal.
func StartConsumerSpan(ctx context.Context, spanName string, attrs ...attribute.KeyValue) (context.Context, func()) {
	ctx, span := Tracer().Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attrs...),
	)
	return ctx, func() { span.End() }
}

// SpanError marks the active span on ctx as errored: records the underlying
// err on the span (so it surfaces in Cloud Trace's error tab) and sets the
// span status to Error with the supplied human-readable message. No-op when
// err is nil or ctx carries no recording span. Mirrors the SetStatus +
// RecordError pair callers used to reach for through
// go.opentelemetry.io/otel/trace + go.opentelemetry.io/otel/codes directly —
// wrapping it here is what lets spanidiom keep banning those imports in
// controller code.
//
// `msg` is the short failure label; the wrapped err carries the detail.
func SpanError(ctx context.Context, err error, msg string) {
	if err == nil {
		return
	}
	span := trace.SpanFromContext(ctx)
	span.RecordError(err)
	span.SetStatus(codes.Error, msg)
}

// ContextFromCarrier extracts the W3C trace context (traceparent/tracestate)
// and baggage carried in a plain string carrier — built by the caller from CR
// annotations — into ctx, using the globally registered text-map propagator.
// Returns ctx unchanged when the carrier is empty.
//
// Wrapping otel.GetTextMapPropagator + propagation.MapCarrier here keeps the
// root otel package and the propagation package out of controller code (the
// spanidiom analyzer's purpose). The caller stays responsible for the
// annotation→carrier mapping, which needs no OTel types.
func ContextFromCarrier(ctx context.Context, carrier map[string]string) context.Context {
	if len(carrier) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(carrier))
}

// LiftBaggageToSpan copies allowlisted W3C baggage members from ctx onto the
// active span as string attributes. Baggage members alone don't appear in span
// data (only in downstream propagation), so this lift is what makes
// cross-service identity (organization.id, project.id, user.id) visible in the
// trace UI. Non-allowlisted members are dropped — the allowlist is the
// caller's PII guard (see the controller's baggageSpanAllowlist). No-op when
// ctx carries no baggage, no allowlisted members, or no recording span.
//
// Wrapping baggage.FromContext + the attribute build here keeps the baggage
// and trace packages out of controller code (the spanidiom analyzer's
// purpose); the caller supplies only the allowlist, which needs no OTel types.
func LiftBaggageToSpan(ctx context.Context, allow map[string]struct{}) {
	members := baggage.FromContext(ctx).Members()
	if len(members) == 0 {
		return
	}
	attrs := make([]attribute.KeyValue, 0, len(members))
	for _, m := range members {
		if _, ok := allow[m.Key()]; !ok {
			continue
		}
		attrs = append(attrs, attribute.String(m.Key(), m.Value()))
	}
	if len(attrs) == 0 {
		return
	}
	trace.SpanFromContext(ctx).SetAttributes(attrs...)
}
