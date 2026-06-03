package main

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

// TestSpannames drives the spannames analyzer against a fixture where each
// tracing.StartSpan / StartConsumerSpan call uses either a constant or a
// literal spanName. The literal cases must be flagged, the constant cases must
// not.
func TestSpannames(t *testing.T) {
	testdata := analysistest.TestData()
	analysistest.Run(t, testdata, spannamesAnalyzer, "github.com/PlainsightAI/openfilter-pipelines-controller/spannames")
}
