package main

import (
	"go/ast"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// spannamesAnalyzer rejects raw string literals passed as the spanName
// argument to tracing.StartSpan / tracing.StartConsumerSpan. The span name is
// a Cloud Trace pivot — a Grafana / Cloud Trace query for
// `name = "PipelineInstanceReconciler.claim"` only returns hits if the emitter
// spelled the string exactly that way. With literals scattered across files, a
// typo'd "PipelineInstanceReconciler.claim" (note the swap) silently produces
// an unqueryable span; with a named constant, the typo is a compile error and
// the constant becomes grep-discoverable from the query site.
//
// The rule is "the spanName positional arg must be an identifier reference" —
// it may resolve to a package-local const (the established home is a
// per-package tracing.go, e.g. internal/controller/tracing.go) or an imported
// constant. Inline string literals are the regression target; everything else
// (concatenation, function-returned strings) is rare enough that a
// compile-time error or human review will catch it.
//
// Test files are exempt: ad-hoc spans in tests are short-lived and not queried
// from dashboards, so the const-vs-literal cost is unjustified.
var spannamesAnalyzer = &analysis.Analyzer{
	Name: "spannames",
	Doc: "rejects raw string literals as the spanName positional argument to " +
		"tracing.StartSpan / tracing.StartConsumerSpan. The span name is a " +
		"Cloud Trace pivot; declare it as a named constant (e.g. in a " +
		"per-package tracing.go) so a typo is a compile error rather than a " +
		"silent miss in dashboards.",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      spannamesRun,
}

func spannamesRun(pass *analysis.Pass) (interface{}, error) {
	// internal/tracing itself defines StartSpan and exercises it in tests; the
	// analyzer must not gate the helper's own home.
	if strings.HasSuffix(pass.Pkg.Path(), spanidiomHome) {
		return nil, nil
	}

	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	insp.Preorder([]ast.Node{(*ast.CallExpr)(nil)}, func(n ast.Node) {
		call := n.(*ast.CallExpr)

		if strings.HasSuffix(pass.Fset.Position(call.Pos()).Filename, "_test.go") {
			return
		}

		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return
		}
		// Resolve the called function via types info — the import may be
		// aliased; what matters is that we're calling
		// (…/internal/tracing).StartSpan or .StartConsumerSpan.
		obj := pass.TypesInfo.ObjectOf(sel.Sel)
		fn, ok := obj.(*types.Func)
		if !ok {
			return
		}
		if fn.Name() != "StartSpan" && fn.Name() != "StartConsumerSpan" {
			return
		}
		if fn.Pkg() == nil || !strings.HasSuffix(fn.Pkg().Path(), spanidiomHome) {
			return
		}

		// StartSpan(ctx, spanName, attrs...) — args[1] is the span name and
		// must be an identifier reference; a literal gets reported. A short
		// arg list (defensively shaped malformed source) is left to the type
		// checker.
		if len(call.Args) < 2 {
			return
		}
		if _, isLit := call.Args[1].(*ast.BasicLit); isLit {
			pass.Reportf(call.Args[1].Pos(),
				"spannames: spanName argument to tracing.%s must be a named "+
					"constant, not a string literal — Cloud Trace queries pivot "+
					"on this string; a typo torpedoes every dashboard that "+
					"filters on it. Declare a const (per-package tracing.go is "+
					"the established home — see internal/controller/tracing.go) "+
					"and reference it here.",
				fn.Name())
		}
	})
	return nil, nil
}
