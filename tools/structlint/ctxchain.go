package main

import (
	"go/ast"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// ctxchain rejects context.Background() / context.TODO() calls inside any
// function that already has a context.Context in scope. The "why": minting a
// fresh root context when one was handed to you severs the trace/log chain —
// the OTel span context, request ID, and logger values riding on the incoming
// ctx are silently dropped, so downstream spans become orphans and structured
// logs lose their correlation. In this controller the incoming reconcile ctx
// carries the upstream traceparent (lifted off the CR) and the
// controller-runtime logger; rooting a fresh context inside a reconcile path
// or a detached goroutine drops both and the span goes dark in prod.
//
// The rule is deliberately narrow and false-positive-resistant: it flags only
// the two parentless roots, Background() and TODO(). The chain-preserving
// derivations — context.WithoutCancel(ctx), context.WithTimeout(ctx, …),
// context.WithValue(ctx, …), context.WithCancel(ctx) — all take a parent ctx
// and are therefore never flagged. The correct fix for a flagged site is
// almost always one of:
//
//   - use the in-scope ctx directly; or
//   - context.WithoutCancel(ctx) when you must detach from the caller's
//     cancellation (e.g. fire-and-forget background work) but still want the
//     trace/log values — this is what a detached goroutine should use.
//
// "In scope" means the enclosing function declaration has a context.Context
// parameter, OR the call sits inside a closure nested within such a function
// (closures capture the outer ctx — this is what catches the
// `go func() { … context.Background() … }()` detachment pattern). Entry
// points that legitimately create the root — main, init, a worker's top-level
// loop with no ctx parameter — are not flagged, because at the Background()
// call site there is no ctx in scope yet.
//
// Escape hatch: `// ctxchain:allow: <reason>` on the enclosing function's doc
// comment exempts that function. The reason is mandatory (a bare directive is
// ignored) so an exemption can't rot into "I forget why this roots a fresh
// context." Codebase-wide — there is no opt-in allowlist, because preserving
// the ctx chain is a universal invariant, not a per-domain migration.
var ctxchainAnalyzer = &analysis.Analyzer{
	Name: "ctxchain",
	Doc: "rejects context.Background()/context.TODO() inside any function that " +
		"already has a context.Context in scope (parameter, or captured by a " +
		"nested closure). Minting a fresh root context discards the incoming " +
		"trace span, request ID, and logger values — use the in-scope ctx, or " +
		"context.WithoutCancel(ctx) to detach cancellation while keeping those " +
		"values. Exempt a function with `// ctxchain:allow: <reason>`.",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      ctxchainRun,
}

func ctxchainRun(pass *analysis.Pass) (interface{}, error) {
	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	insp.Preorder([]ast.Node{(*ast.FuncDecl)(nil)}, func(n ast.Node) {
		fn := n.(*ast.FuncDecl)
		if fn.Body == nil {
			return
		}
		// Tests legitimately root fresh contexts (table-driven cases,
		// standalone fixtures with no request ctx to inherit). Mirrors the
		// per-file exemption the other analyzers use.
		if strings.HasSuffix(pass.Fset.Position(fn.Pos()).Filename, "_test.go") {
			return
		}
		if ctxchainHasAllowDirective(fn.Doc) {
			return
		}

		ctxchainWalk(pass, fn.Body, ctxchainParamsHaveContext(pass, fn.Type))
	})

	return nil, nil
}

// ctxchainWalk descends an AST subtree, reporting Background()/TODO() calls
// whenever a context.Context is in scope. ctxInScope is sticky on the way
// down: once an enclosing function (decl or lit) supplies a ctx parameter,
// every descendant — including nested closures that capture it — is covered.
func ctxchainWalk(pass *analysis.Pass, node ast.Node, ctxInScope bool) {
	ast.Inspect(node, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.FuncLit:
			// A closure captures the outer ctx; it may also introduce its own
			// ctx parameter. Recurse with scope = outer ∨ own, then stop the
			// parent Inspect from double-visiting this subtree.
			ctxchainWalk(pass, v.Body, ctxInScope || ctxchainParamsHaveContext(pass, v.Type))
			return false
		case *ast.CallExpr:
			if ctxInScope && ctxchainIsRootContextCall(pass, v) {
				pass.Reportf(v.Pos(),
					"ctxchain: %s called where a context.Context is already in "+
						"scope — this discards the incoming trace span, request ID, "+
						"and logger values. Use the in-scope ctx, or "+
						"context.WithoutCancel(ctx) to detach cancellation while "+
						"preserving those values. Exempt with `// ctxchain:allow: "+
						"<reason>` on the enclosing function if a fresh root is "+
						"genuinely required.",
					ctxchainCalleeName(pass, v))
			}
		}
		return true
	})
}

// ctxchainParamsHaveContext reports whether any parameter of the function
// signature is a context.Context.
func ctxchainParamsHaveContext(pass *analysis.Pass, ft *ast.FuncType) bool {
	if ft == nil || ft.Params == nil {
		return false
	}
	for _, field := range ft.Params.List {
		if ctxchainIsContextType(pass.TypesInfo.TypeOf(field.Type)) {
			return true
		}
	}
	return false
}

// ctxchainIsContextType reports whether t is exactly context.Context.
func ctxchainIsContextType(t types.Type) bool {
	named, ok := t.(*types.Named)
	if !ok {
		return false
	}
	obj := named.Obj()
	return obj != nil && obj.Name() == "Context" &&
		obj.Pkg() != nil && obj.Pkg().Path() == "context"
}

// ctxchainIsRootContextCall reports whether call is context.Background() or
// context.TODO(), resolved through the type info so import aliases and
// dot-imports are handled correctly.
func ctxchainIsRootContextCall(pass *analysis.Pass, call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	fn, ok := pass.TypesInfo.Uses[sel.Sel].(*types.Func)
	if !ok || fn.Pkg() == nil || fn.Pkg().Path() != "context" {
		return false
	}
	return fn.Name() == "Background" || fn.Name() == "TODO"
}

// ctxchainCalleeName renders the flagged callee as "context.Background" /
// "context.TODO" for the diagnostic, falling back to the selector text.
func ctxchainCalleeName(pass *analysis.Pass, call *ast.CallExpr) string {
	if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
		if fn, ok := pass.TypesInfo.Uses[sel.Sel].(*types.Func); ok && fn.Pkg() != nil {
			return fn.Pkg().Name() + "." + fn.Name() + "()"
		}
		return sel.Sel.Name + "()"
	}
	return "context.Background()"
}

// ctxchainHasAllowDirective recognises `// ctxchain:allow: <reason>` on a
// func's doc comment. The reason after the colon must be non-empty — a bare
// directive is rejected so the exemption can't rot. Mirrors spancov:none.
func ctxchainHasAllowDirective(doc *ast.CommentGroup) bool {
	if doc == nil {
		return false
	}
	for _, c := range doc.List {
		line := strings.TrimSpace(strings.TrimPrefix(c.Text, "//"))
		const prefix = "ctxchain:allow:"
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		if strings.TrimSpace(strings.TrimPrefix(line, prefix)) != "" {
			return true
		}
	}
	return false
}
