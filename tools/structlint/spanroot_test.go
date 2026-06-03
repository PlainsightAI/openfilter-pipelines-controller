package main

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

// TestSpanroot drives the spanroot analyzer against a fixture covering each
// branch: a flagged goroutine closure (bare Stamp), a clean one (StartSpan
// first), the `// spanroot:allow:` escape hatch, and an ordinary
// reconcile-path Stamp outside any goroutine that must NOT be flagged.
func TestSpanroot(t *testing.T) {
	testdata := analysistest.TestData()
	analysistest.Run(t, testdata, spanrootAnalyzer, "github.com/PlainsightAI/openfilter-pipelines-controller/spanroot")
}
