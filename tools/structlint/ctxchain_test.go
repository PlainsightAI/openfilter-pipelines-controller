package main

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
)

// TestCtxchain drives the ctxchain analyzer against a single fixture that
// exercises every branch of the rule:
//
//   - a ctx parameter in scope -> Background()/TODO() flagged
//   - chain-preserving derivations (WithTimeout/WithoutCancel/WithValue) clean
//   - no ctx in scope (entry point) -> root creation allowed
//   - closure capturing an outer ctx -> flagged (the goroutine-detach bug)
//   - closure with its own ctx param -> flagged on that basis
//   - `// ctxchain:allow: <reason>` -> exempted
//
// The analyzer is codebase-wide, so a single package path suffices; the
// `want` annotations live inline in the fixture.
func TestCtxchain(t *testing.T) {
	testdata := analysistest.TestData()
	analysistest.Run(t, testdata, ctxchainAnalyzer, "ctxchain")
}
