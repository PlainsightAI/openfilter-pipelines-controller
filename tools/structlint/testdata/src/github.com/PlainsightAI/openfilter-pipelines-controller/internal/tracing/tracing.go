// Package tracing is the canonical home for the centralized attribute keys
// — the analyzer must NOT flag attribute.String calls here, even with
// otherwise-banned keys. This fixture ensures the package-path exemption
// is correct.
//
// The spancov / spannames / spanroot fixtures (see testdata/src/spancov*,
// spannames*, spanroot*) also depend on this stub for Stamp / StartSpan /
// StartConsumerSpan; the shapes mirror internal/tracing closely enough that
// the AST matchers find the same call patterns they would in real code.
package tracing

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
)

func PipelineInstanceUID(v string) attribute.KeyValue {
	return attribute.String("pipeline_instance.uid", v)
}

func PipelineInstanceID(v string) attribute.KeyValue {
	return attribute.String("pipeline_instance.id", v)
}

func PipelineUID(v string) attribute.KeyValue {
	return attribute.String("pipeline.uid", v)
}

func ClaimAcquired(b bool) attribute.KeyValue {
	return attribute.String("claim.acquired", "x")
}

func Stamp(ctx context.Context, kvs ...attribute.KeyValue) {}

func StartSpan(ctx context.Context, spanName string, attrs ...attribute.KeyValue) (context.Context, func()) {
	return ctx, func() {}
}

func StartConsumerSpan(ctx context.Context, spanName string, attrs ...attribute.KeyValue) (context.Context, func()) {
	return ctx, func() {}
}
