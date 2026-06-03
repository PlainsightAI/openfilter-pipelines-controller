// Package spanidiomunscoped sits at an unrelated path with no special
// standing. The spanidiom rule is codebase-wide, so the banned imports must be
// flagged here just as in spanidiomscoped — pinning that enforcement is
// universal, not gated on an opt-in allowlist.
package spanidiomunscoped

import (
	_ "go.opentelemetry.io/otel"       // want `spanidiom: direct import of "go.opentelemetry.io/otel" is banned outside internal/tracing.*internal/tracing`
	_ "go.opentelemetry.io/otel/trace" // want `spanidiom: direct import of "go.opentelemetry.io/otel/trace" is banned outside internal/tracing.*tracing\.Stamp.*tracing\.StartSpan`

	// The attribute package remains unregulated by spanidiom.
	_ "go.opentelemetry.io/otel/attribute"
)
