package main

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

// TestSpancov drives the spancov analyzer against a fixture exercising each
// branch: a stamped Reconcile, a child-spanned reconcile* phase, an
// untraced reconcile* phase (flagged), a directive-exempted phase, a plain
// helper on the reconciler (not flagged), and a same-named method on a
// non-*Reconciler receiver (not flagged). Each `// want "…"` pins the expected
// diagnostic at the func decl's name token; absent comments mean no diagnostic.
func TestSpancov(t *testing.T) {
	testdata := analysistest.TestData()
	analysistest.Run(t, testdata, spancovAnalyzer, "github.com/PlainsightAI/openfilter-pipelines-controller/spancov")
}
