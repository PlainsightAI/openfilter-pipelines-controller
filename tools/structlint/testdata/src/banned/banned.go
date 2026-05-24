package banned

import "go.opentelemetry.io/otel/attribute"

// Top-level uses to exercise each canonical key. Each `want` regex matches
// the analyzer's "is a canonical OTel attribute" message — the key name and
// the suggested helper appear inline so the diagnostic is specific.
var (
	_ = attribute.String("pipeline_instance.uid", "x")       // want `pipeline_instance\.uid.*tracing\.PipelineInstanceUID`
	_ = attribute.String("pipeline_instance.id", "x")        // want `pipeline_instance\.id.*tracing\.PipelineInstanceID`
	_ = attribute.String("pipeline_instance.name", "x")      // want `pipeline_instance\.name.*tracing\.PipelineInstanceName`
	_ = attribute.String("pipeline_instance.namespace", "x") // want `pipeline_instance\.namespace.*tracing\.PipelineInstanceNamespace`
	_ = attribute.String("pipeline.uid", "x")                // want `pipeline\.uid.*tracing\.PipelineUID`
	_ = attribute.String("pipeline.mode", "x")               // want `pipeline\.mode.*tracing\.PipelineMode`

	// Non-canonical keys are NOT flagged — only the centralized set is
	// guarded. Adding a new attribute is unrestricted; promoting it to
	// canonical (cross-domain pivot) requires registering it in
	// internal/tracing/attrs.go AND in the analyzer's attrkeyCanonical map.
	_ = attribute.String("custom.unrelated_key", "x")
	_ = attribute.Int("custom.unrelated_count", 1)
)
