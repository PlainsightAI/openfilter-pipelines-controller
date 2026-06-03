package main

import (
	"go/ast"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// spancov ensures every reconcile entry point and reconcile* phase method on a
// *Reconciler receiver either stamps a canonical attribute on the active span
// (tracing.Stamp) or opens its own child span (tracing.StartSpan /
// tracing.StartConsumerSpan). This is the controller-shaped analogue of
// plainsight-api's handler/usecase spancov: the reconcile loop is this repo's
// request surface, so it is where trace coverage must be guaranteed.
//
// The matched surface is intentionally precise:
//
//   - the receiver type name ends in "Reconciler" (PipelineInstanceReconciler,
//     PipelineReconciler, …), and
//   - the method is the controller-runtime entry point "Reconcile" OR a
//     reconcile* phase method (reconcileBatch, reconcileStreaming, …).
//
// Plain helpers on the same receiver (getPipeline, buildJob, setCondition) are
// out of scope — they're implementation detail reached from within an
// already-spanned reconcile, not the trace boundary. Granularity at the method
// level means a newly added reconcile phase that forgets to span can't ride
// along on a passing diff.
//
// Codebase-wide and absolute — there is no grandfathering baseline. A method
// that genuinely cannot emit a span (pure delegation to another spanned
// method, etc.) opts out with a `// spancov:none: <reason>` doc directive —
// the reason is mandatory so future readers know whether the exemption is
// still load-bearing.
var spancovAnalyzer = &analysis.Analyzer{
	Name: "spancov",
	Doc: "ensures every Reconcile entry point and reconcile* phase method on a " +
		"*Reconciler receiver calls tracing.Stamp, tracing.StartSpan, or " +
		"tracing.StartConsumerSpan. Codebase-wide and absolute; opt a method " +
		"out with `// spancov:none: <reason>` (reason mandatory).",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      spancovRun,
}

func spancovRun(pass *analysis.Pass) (interface{}, error) {
	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
	insp.Preorder([]ast.Node{(*ast.FuncDecl)(nil)}, func(n ast.Node) {
		fn := n.(*ast.FuncDecl)
		if fn.Body == nil || fn.Recv == nil {
			return
		}
		// Test files contain stub reconcilers and table-driven harnesses whose
		// purpose is to drive tests — those don't need production tracing
		// calls. Mirrors the per-file exemption the other analyzers use.
		if strings.HasSuffix(pass.Fset.Position(fn.Pos()).Filename, "_test.go") {
			return
		}
		if spancovHasNoneDirective(fn.Doc) {
			return
		}

		recv := spancovReceiverTypeName(fn.Recv)
		if !strings.HasSuffix(recv, "Reconciler") {
			return
		}
		name := fn.Name.Name
		if name != "Reconcile" && !strings.HasPrefix(name, "reconcile") {
			return
		}

		if spancovBodyTracesSpan(fn.Body) {
			return
		}

		pass.Reportf(fn.Name.Pos(),
			"spancov: reconcile method %q on %s does not call tracing.Stamp(ctx, "+
				"...), tracing.StartSpan(ctx, ...), or tracing.StartConsumerSpan("+
				"ctx, ...). Stamp a canonical attribute onto the active reconcile "+
				"span (see internal/tracing/attrs.go) or open a child span via "+
				"tracing.StartSpan for a distinct phase. If this method genuinely "+
				"cannot emit a span (pure delegation to another spanned method, "+
				"etc.) annotate the doc comment with `// spancov:none: <reason>`.",
			name, recv)
	})

	return nil, nil
}

// spancovReceiverTypeName returns the receiver's base type name, unwrapping a
// pointer receiver (*PipelineInstanceReconciler -> PipelineInstanceReconciler).
// Returns "" for shapes it doesn't recognise (generic receivers, etc.), which
// the caller treats as out-of-scope.
func spancovReceiverTypeName(recv *ast.FieldList) string {
	if recv == nil || len(recv.List) == 0 {
		return ""
	}
	switch t := recv.List[0].Type.(type) {
	case *ast.StarExpr:
		if id, ok := t.X.(*ast.Ident); ok {
			return id.Name
		}
	case *ast.Ident:
		return t.Name
	}
	return ""
}

// spancovHasNoneDirective recognises `// spancov:none: <reason>` on a func's
// doc comment. The reason after the colon must be non-empty — a bare
// `// spancov:none:` is rejected so the exemption can't rot into "I forget why
// this is exempt." Mirrors the rationale behind the //nolint:reason convention
// used by other static-check ecosystems.
func spancovHasNoneDirective(doc *ast.CommentGroup) bool {
	if doc == nil {
		return false
	}
	for _, c := range doc.List {
		line := strings.TrimSpace(strings.TrimPrefix(c.Text, "//"))
		const prefix = "spancov:none:"
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		if strings.TrimSpace(strings.TrimPrefix(line, prefix)) != "" {
			return true
		}
	}
	return false
}

// spancovBodyTracesSpan returns true when body contains a call to one of the
// three canonical tracing entry points that satisfy span coverage:
//
//   - tracing.Stamp(ctx, attrs...) — stamp canonical attributes on the active
//     (parent) reconcile span without opening a child.
//   - tracing.StartSpan(ctx, spanName, attrs...) — open a child span; the
//     returned closure ends it.
//   - tracing.StartConsumerSpan(ctx, spanName, attrs...) — same shape with
//     SpanKind=Consumer, for queue/worker callbacks.
//
// tracing.SpanError is intentionally NOT on this list — it records on an
// existing span (no Stamp, no Start). A method that only calls SpanError still
// needs a Stamp/StartSpan upstream of it; that upstream call is what satisfies
// coverage.
//
// Matching by identifier name rather than resolved import is a deliberate
// trade: it keeps the analyzer simple and matches the convention used
// everywhere in the repo (unaliased internal/tracing imported as `tracing`).
// If a package ever needs to alias the import, the spancov:none directive is
// the escape hatch.
func spancovBodyTracesSpan(body *ast.BlockStmt) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		id, ok := sel.X.(*ast.Ident)
		if !ok || id.Name != "tracing" {
			return true
		}
		switch sel.Sel.Name {
		case "Stamp", "StartSpan", "StartConsumerSpan":
			found = true
			return false
		}
		return true
	})
	return found
}
