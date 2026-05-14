// Package tracing is the canonical home for the centralized attribute keys
// — the analyzer must NOT flag attribute.String calls here, even with
// otherwise-banned keys. This fixture ensures the package-path exemption
// is correct.
package tracing

import "go.opentelemetry.io/otel/attribute"

func PipelineInstanceUID(v string) attribute.KeyValue {
	return attribute.String("pipeline_instance.uid", v)
}

func PipelineUID(v string) attribute.KeyValue {
	return attribute.String("pipeline.uid", v)
}
