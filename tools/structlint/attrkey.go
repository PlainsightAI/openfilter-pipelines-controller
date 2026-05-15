package main

import (
	"go/ast"
	"go/token"
	"go/types"
	"strconv"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/ast/inspector"
)

// attrkeyCanonical maps each centralized attribute key to the helper a
// caller should use instead. Keep in lockstep with internal/tracing/attrs.go —
// the analyzer rejects raw uses of these keys; the builders are the only
// sanctioned way to construct them outside internal/tracing.
//
// Conventions mirrored from plainsight-api's pkg/tracing/attrs.go so
// cross-service Cloud Trace queries (`pipeline_instance.uid="<uuid>"`) pivot
// on identical key strings without per-service casing skew.
var attrkeyCanonical = map[string]string{
	"pipeline_instance.uid":       "tracing.PipelineInstanceUID(pi)",
	"pipeline_instance.name":      "tracing.PipelineInstanceName(pi)",
	"pipeline_instance.namespace": "tracing.PipelineInstanceNamespace(pi)",
	"pipeline.uid":                "tracing.PipelineUID(p)",
	"pipeline.mode":               "tracing.PipelineMode(mode)",
}

// otelAttributePkg is the import path of the OTel attribute package whose
// constructors we're guarding. Matches against the *resolved* import path,
// not the local identifier, so an `import attr "go.opentelemetry.io/otel/attribute"`
// alias is detected the same as the canonical bare name.
const otelAttributePkg = "go.opentelemetry.io/otel/attribute"

// attrkeyHome is the package allowed to construct attribute.String(...)
// directly with these keys — internal/tracing is where the constants live,
// so the builders themselves naturally call attribute.String. Matched as a
// suffix to be agnostic of the module path the analyzer runs against.
const attrkeyHome = "/internal/tracing"

var attrkeyAnalyzer = &analysis.Analyzer{
	Name: "attrkey",
	Doc: "rejects raw attribute.String(<canonical-key>, …) calls outside internal/tracing. " +
		"Use the typed builders in internal/tracing/attrs.go (tracing.PipelineInstanceUID, " +
		"tracing.PipelineUID, tracing.PipelineMode, …) instead.",
	Requires: []*analysis.Analyzer{inspect.Analyzer},
	Run:      attrkeyRun,
}

func attrkeyRun(pass *analysis.Pass) (interface{}, error) {
	// internal/tracing is the canonical home for these constants — the
	// builders themselves call attribute.String, so they're allowed.
	if strings.HasSuffix(pass.Pkg.Path(), attrkeyHome) {
		return nil, nil
	}

	insp := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)

	insp.Preorder([]ast.Node{(*ast.CallExpr)(nil)}, func(n ast.Node) {
		call := n.(*ast.CallExpr)

		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return
		}
		// Receiver must be a package identifier — rules out method calls
		// and nested selectors that don't bind to a top-level imported
		// package.
		ident, ok := sel.X.(*ast.Ident)
		if !ok {
			return
		}
		obj := pass.TypesInfo.ObjectOf(ident)
		pkgName, ok := obj.(*types.PkgName)
		if !ok {
			return
		}
		if pkgName.Imported().Path() != otelAttributePkg {
			return
		}
		// Only flag calls that pass a string *literal* as the first arg. A
		// caller that already routes through `attribute.String(tracing.AttrFoo, …)`
		// (i.e. references the constant) is fine — that's the documented
		// escape hatch for exotic stamping needs.
		if len(call.Args) < 1 {
			return
		}
		lit, ok := call.Args[0].(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return
		}
		key, err := strconv.Unquote(lit.Value)
		if err != nil {
			return
		}
		builder, banned := attrkeyCanonical[key]
		if !banned {
			return
		}
		pass.Reportf(call.Pos(),
			"attribute.%s(%q, …) is a canonical OTel attribute owned by internal/tracing — "+
				"call %s instead (or reference tracing.Attr* via the constant if you need "+
				"raw attribute.KeyValue construction)",
			sel.Sel.Name, key, builder,
		)
	})

	return nil, nil
}
