// Package spanidiomscoped is a fixture for the spanidiom analyzer. The rule is
// codebase-wide, so banned imports here must be flagged. Each banned import is
// annotated with a `want` regex that pins both the rejected path and the
// analyzer's "use internal/tracing" fix hint.
package spanidiomscoped

import (
	_ "go.opentelemetry.io/otel"       // want `spanidiom: direct import of "go.opentelemetry.io/otel" is banned outside internal/tracing.*internal/tracing`
	_ "go.opentelemetry.io/otel/trace" // want `spanidiom: direct import of "go.opentelemetry.io/otel/trace" is banned outside internal/tracing.*tracing\.Stamp.*tracing\.StartSpan`

	// The attribute package is intentionally NOT banned by spanidiom — the
	// canonical-key gate lives in the attrkey analyzer instead. Importing it
	// here must produce no spanidiom diagnostic.
	_ "go.opentelemetry.io/otel/attribute"
)
