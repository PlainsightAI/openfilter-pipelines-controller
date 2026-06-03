package main

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

// TestSpanidiom drives the codebase-wide spanidiom analyzer against two
// fixtures at unrelated paths — both must flag the banned otel imports (and
// leave the attribute import alone). Two different paths pin that enforcement
// is universal, not gated on any allowlist.
//
// The internal/tracing home exemption isn't exercised as its own fixture —
// the real internal/tracing (which legitimately imports both banned paths) is
// the actual regression target, and `go run ./tools/structlint ./...` against
// the live tree fails immediately if the suffix check breaks.
func TestSpanidiom(t *testing.T) {
	testdata := analysistest.TestData()
	analysistest.Run(t, testdata, spanidiomAnalyzer,
		"spanidiomscoped",
		"spanidiomunscoped",
	)
}
