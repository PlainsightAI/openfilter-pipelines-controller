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

// Package tracing wires the controller's own OTel TracerProvider so reconcile
// spans become children of the upstream API/agent span and parents of the
// downstream filter spans, closing the controller hop in the
// API -> agent -> controller -> filter trace waterfall (PLAT-1000).
//
// Connection to the collector is plaintext gRPC. The deployment topology in
// scope here is in-cluster (controller -> otel-collector Service); TLS adds
// no value over a kube-internal hop and would require certificate plumbing
// that the otel-collector OTLP receiver is not configured for.
package tracing

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// TracerName is the instrumentation scope name used for spans emitted by this
// controller. Kept as a package-level constant so tests and the reconciler
// share a single source of truth.
const TracerName = "github.com/PlainsightAI/openfilter-pipelines-controller"

// ShutdownFunc flushes pending spans and tears down the exporter. Always
// non-nil so callers can defer it unconditionally.
type ShutdownFunc func(context.Context) error

// noopShutdown is returned when tracing is disabled so callers can defer
// without nil-checking.
func noopShutdown(context.Context) error { return nil }

// InitTracerProvider initializes the global OTel TracerProvider with an OTLP
// gRPC exporter pointed at endpoint. The connection is plaintext (no TLS).
//
// When endpoint is empty the function is a no-op: the global tracer stays a
// noop, no exporter is dialed, and no resources are leaked. In this branch
// the W3C TraceContext + Baggage propagators are NOT installed, since with a
// noop tracer there is no span context for the propagators to act on (an
// extracted traceparent would attach to a dead context).
//
// When endpoint is non-empty, the W3C TraceContext + Baggage propagators are
// installed globally so the reconciler can extract the upstream traceparent
// that plainsight-deployment-agent (PLAT-851) writes onto the PipelineInstance
// CR.
//
// The returned ShutdownFunc must be called on graceful shutdown to flush the
// span batcher; otherwise in-flight spans are dropped.
func InitTracerProvider(ctx context.Context, endpoint, serviceVersion string) (ShutdownFunc, error) {
	if endpoint == "" {
		return noopShutdown, nil
	}

	// Strip an optional scheme so operators can paste the full
	// OTEL_EXPORTER_OTLP_ENDPOINT URL without breaking the gRPC dialer, which
	// expects bare host:port. We force insecure regardless of scheme; the
	// scheme is purely a typo-friendly affordance.
	endpoint = strings.TrimPrefix(endpoint, "http://")
	endpoint = strings.TrimPrefix(endpoint, "https://")
	endpoint = strings.TrimSuffix(endpoint, "/")

	res, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(
			semconv.ServiceName("openfilter-pipelines-controller"),
			semconv.ServiceVersion(serviceVersion),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("build OTel resource: %w", err)
	}

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("create OTLP gRPC exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}

// Tracer returns the controller's instrumentation tracer. Safe to call before
// InitTracerProvider runs; in that case it returns the global noop tracer.
func Tracer() trace.Tracer {
	return otel.Tracer(TracerName)
}
