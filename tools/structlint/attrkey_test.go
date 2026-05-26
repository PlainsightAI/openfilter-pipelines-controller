package main

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

// TestAttrkey drives the attrkey analyzer against synthetic packages under
// testdata/src/. Each `// want "…"` comment in those files declares the
// expected diagnostic on the same line — a missing diagnostic, an extra
// one, or a changed message all fail the test.
func TestAttrkey(t *testing.T) {
	testdata := analysistest.TestData()
	analysistest.Run(t, testdata, attrkeyAnalyzer,
		// banned: a domain package that should be flagged.
		"banned",
		// allowed: internal/tracing — canonical home, must not be flagged
		// even when calling attribute.String with a banned key.
		"github.com/PlainsightAI/openfilter-pipelines-controller/internal/tracing",
	)
}
