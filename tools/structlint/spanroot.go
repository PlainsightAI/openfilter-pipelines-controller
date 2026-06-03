package main

import (
	"go/ast"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// spanroot closes the blind spot in spancov: spancov checks that a reconcile
// method *calls* tracing.Stamp or tracing.StartSpan, but not whether there is a
// live span for Stamp to attach to. tracing.Stamp only records attributes on a
// *recording* span already on the ctx. On the reconcile path that span exists
// (Reconcile opens it via StartSpan), so a bare Stamp downstream is fine. But
// code reached off that path — a `go func` goroutine — gets a ctx whose
// recording span has very likely already ended, so a bare Stamp there is a
// silent no-op: it compiles, it passes spancov, and it traces nothing in
// production.
//
// We can't prove "the ctx carries no recording span" interprocedurally for an
// arbitrary function, but we CAN prove it for the entry-point shape where the
// ctx demonstrably originates without a live span: a `go func(){…}()` goroutine
// closure (fire-and-forget; even if it captures a reconcile ctx, that span has
// very likely ended by the time the goroutine runs). Inside such a closure,
// tracing.Stamp is flagged unless the closure also opens its own span via
// tracing.StartSpan first (the fix: re-parent with tracing.ContextFromCarrier
// if you carry W3C headers, then StartSpan so the attributes have somewhere to
// land). The rootless-shape set is intentionally small and extensible — add
// other framework callback shapes (queue message handlers, cron tick funcs) as
// they appear; the point is "ctx without a live span," not any one transport.
//
// Escape hatch: `// spanroot:allow: <reason>` on the enclosing function's doc
// comment, reason mandatory.
var spanrootAnalyzer = &analysis.Analyzer{
	Name: "spanroot",
	Doc: "flags tracing.Stamp inside a rootless `go func` goroutine closure " +
		"that does not first open a span via tracing.StartSpan. Stamp only " +
		"records on a span already present on the ctx; in a detached goroutine " +
		"the reconcile span has very likely ended, so the Stamp is a silent " +
		"no-op. Open a child span with tracing.StartSpan first. Exempt with " +
		"`// spanroot:allow: <reason>`.",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      spanrootRun,
}

func spanrootRun(pass *analysis.Pass) (interface{}, error) {
	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	insp.Preorder([]ast.Node{(*ast.FuncDecl)(nil)}, func(n ast.Node) {
		fn := n.(*ast.FuncDecl)
		if fn.Body == nil {
			return
		}
		if strings.HasSuffix(pass.Fset.Position(fn.Pos()).Filename, "_test.go") {
			return
		}
		if spanrootHasAllowDirective(fn.Doc) {
			return
		}

		// Goroutine closures anywhere in the body.
		ast.Inspect(fn.Body, func(m ast.Node) bool {
			gostmt, ok := m.(*ast.GoStmt)
			if !ok {
				return true
			}
			if fl, ok := gostmt.Call.Fun.(*ast.FuncLit); ok {
				spanrootCheckClosure(pass, fl.Body)
			}
			return true
		})
	})

	return nil, nil
}

// spanrootCheckClosure reports a Stamp call in a rootless closure body unless
// the body opens its own span via StartSpan. Nested closures are not descended
// into — each closure is judged on whether it roots its own span.
func spanrootCheckClosure(pass *analysis.Pass, body *ast.BlockStmt) {
	if spanrootBodyCalls(body, "StartSpan") || spanrootBodyCalls(body, "StartConsumerSpan") {
		return
	}
	for _, call := range spanrootStampCalls(body) {
		pass.Reportf(call.Pos(),
			"spanroot: tracing.Stamp in a rootless `go func` goroutine closure "+
				"where the ctx carries no live recording span — this Stamp is a "+
				"silent no-op. Open a span first with tracing.StartSpan (after "+
				"tracing.ContextFromCarrier if the work carries W3C trace "+
				"headers). Exempt with `// spanroot:allow: <reason>` on the "+
				"enclosing function.")
	}
}

// spanrootBodyCalls reports whether body calls tracing.<name>, without
// descending into nested closures (each closure roots its own span).
func spanrootBodyCalls(body *ast.BlockStmt, name string) bool {
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}
		if _, ok := n.(*ast.FuncLit); ok {
			return false
		}
		if spanrootIsTracingCall(n, name) {
			found = true
			return false
		}
		return true
	})
	return found
}

// spanrootStampCalls returns the tracing.Stamp calls directly in body (not in
// nested closures).
func spanrootStampCalls(body *ast.BlockStmt) []*ast.CallExpr {
	var calls []*ast.CallExpr
	ast.Inspect(body, func(n ast.Node) bool {
		if _, ok := n.(*ast.FuncLit); ok {
			return false
		}
		if spanrootIsTracingCall(n, "Stamp") {
			calls = append(calls, n.(*ast.CallExpr))
		}
		return true
	})
	return calls
}

// spanrootIsTracingCall reports whether n is a call to tracing.<name>.
// Identifier-based to match the repo convention (internal/tracing imported as
// tracing), consistent with the spancov analyzer.
func spanrootIsTracingCall(n ast.Node, name string) bool {
	call, ok := n.(*ast.CallExpr)
	if !ok {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	id, ok := sel.X.(*ast.Ident)
	return ok && id.Name == "tracing" && sel.Sel.Name == name
}

// spanrootHasAllowDirective recognises `// spanroot:allow: <reason>` with a
// mandatory non-empty reason.
func spanrootHasAllowDirective(doc *ast.CommentGroup) bool {
	if doc == nil {
		return false
	}
	for _, c := range doc.List {
		line := strings.TrimSpace(strings.TrimPrefix(c.Text, "//"))
		const prefix = "spanroot:allow:"
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		if strings.TrimSpace(strings.TrimPrefix(line, prefix)) != "" {
			return true
		}
	}
	return false
}
