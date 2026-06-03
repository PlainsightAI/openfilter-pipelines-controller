package main

import (
	"go/ast"
	"strconv"
	"strings"

	"golang.org/x/tools/go/analysis"
)

// spanidiomBannedImports lists OTel SDK packages that controller code must not
// import directly. Each entry has a single legitimate home in this repo:
//
//   - "go.opentelemetry.io/otel" — the root package exposes
//     GetTracerProvider, SetTracerProvider, SetTextMapPropagator,
//     GetTextMapPropagator. All of those are wired exactly once, inside
//     internal/tracing (InitTracerProvider / Tracer / ContextFromCarrier).
//     A second importer would mean the package is building a parallel
//     instrumentation surface, which is exactly the divergence this analyzer
//     exists to prevent.
//
//   - "go.opentelemetry.io/otel/trace" — exposes trace.SpanFromContext,
//     trace.WithAttributes, the trace.Span type. Every legitimate use of
//     these is mediated by internal/tracing: Stamp wraps
//     SpanFromContext.SetAttributes, StartSpan wraps Tracer.Start +
//     WithAttributes, SpanError wraps RecordError + SetStatus. Outside
//     internal/tracing the import is the marker for someone reaching past the
//     homogenised surface.
//
// The attribute package (go.opentelemetry.io/otel/attribute) is intentionally
// NOT banned here — the canonical-key gate lives in the attrkey analyzer
// instead, and domain code legitimately names attribute.KeyValue when calling
// the typed builders. The baggage, codes, and propagation packages are kept
// out of controller code by the internal/tracing wrappers (LiftBaggageToSpan,
// SpanError, ContextFromCarrier) but are not import-banned, since their only
// uses already route through those wrappers.
var spanidiomBannedImports = map[string]string{
	"go.opentelemetry.io/otel": "use internal/tracing (InitTracerProvider / Tracer / " +
		"StartSpan / StartConsumerSpan / SpanError / ContextFromCarrier / " +
		"LiftBaggageToSpan) — the root otel package is wired exactly once in " +
		"internal/tracing",
	"go.opentelemetry.io/otel/trace": "use tracing.Stamp(ctx, ...) for parent-span " +
		"attributes, tracing.StartSpan(ctx, spanName, attrs...) for child spans, " +
		"tracing.StartConsumerSpan(...) for queue/worker callbacks, and " +
		"tracing.SpanError(ctx, err, msg) for the RecordError+SetStatus pair — the " +
		"closure-returning shape removes the need for trace.WithAttributes / " +
		"trace.Span at the call site",
}

// spanidiomHome is the package suffix allowed to import the banned packages —
// internal/tracing is the wrapper layer those packages live behind. Matched as
// a suffix so the analyzer is agnostic of the module path it runs under.
const spanidiomHome = "/internal/tracing"

// The rule is codebase-wide: every production file outside internal/tracing
// must route tracing through the homogenised surface. After the PLAT-1000
// refactor there are zero direct imports of the banned packages anywhere
// outside internal/tracing, so there is no migration debt to grandfather —
// the gate is simply "nobody reaches past internal/tracing."

var spanidiomAnalyzer = &analysis.Analyzer{
	Name: "spanidiom",
	Doc: "rejects direct imports of go.opentelemetry.io/otel and " +
		"go.opentelemetry.io/otel/trace outside internal/tracing. Controller " +
		"code must go through the homogenised surface in internal/tracing " +
		"(Stamp, StartSpan, StartConsumerSpan, SpanError, ContextFromCarrier, " +
		"LiftBaggageToSpan, the typed attribute builders) so the " +
		"instrumentation idiom stays uniform and the spancov analyzer's AST " +
		"matcher remains exhaustive. Paired with spancov: spancov says " +
		"\"every reconcile method MUST trace\", spanidiom says \"and they all " +
		"do it the SAME way.\"",
	Run: spanidiomRun,
}

func spanidiomRun(pass *analysis.Pass) (interface{}, error) {
	if strings.HasSuffix(pass.Pkg.Path(), spanidiomHome) {
		return nil, nil
	}

	for _, f := range pass.Files {
		// Tests legitimately drive the OTel SDK directly — registering an
		// in-memory span recorder, snapshot/restoring the global provider,
		// inspecting Span methods we don't expose through internal/tracing.
		// The homogenisation rule only applies to production paths.
		if strings.HasSuffix(pass.Fset.Position(f.Pos()).Filename, "_test.go") {
			continue
		}
		// File-level escape hatch: `// spanidiom:allow: <reason>` at the top of
		// the file exempts every banned import in it. Mandatory reason — same
		// shape as `spancov:none:` / `ctxchain:allow:` / `spanroot:allow:`.
		// Reach for the helpers in internal/tracing first; use the directive
		// only when no helper fits.
		if spanidiomHasAllowDirective(f) {
			continue
		}
		for _, imp := range f.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				continue
			}
			fixHint, banned := spanidiomBannedImports[path]
			if !banned {
				continue
			}
			pass.Reportf(imp.Path.Pos(),
				"spanidiom: direct import of %q is banned outside internal/tracing — %s",
				path, fixHint)
		}
	}
	return nil, nil
}

// spanidiomHasAllowDirective recognises a `// spanidiom:allow: <reason>` line
// anywhere in the file's leading comment groups (the file-level doc comment
// or any standalone comment before the package clause). Reason after the colon
// must be non-empty so the exemption can't rot.
func spanidiomHasAllowDirective(f *ast.File) bool {
	if f.Doc != nil && spanidiomDirectiveInGroup(f.Doc) {
		return true
	}
	for _, cg := range f.Comments {
		// Only the leading comments — once we pass the package clause's line,
		// stop. Per ast.File semantics f.Package is the position of "package",
		// and only CommentGroups before it count as file-level directives.
		if cg.Pos() >= f.Package {
			break
		}
		if spanidiomDirectiveInGroup(cg) {
			return true
		}
	}
	return false
}

func spanidiomDirectiveInGroup(cg *ast.CommentGroup) bool {
	for _, c := range cg.List {
		line := strings.TrimSpace(strings.TrimPrefix(c.Text, "//"))
		const prefix = "spanidiom:allow:"
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		if strings.TrimSpace(strings.TrimPrefix(line, prefix)) != "" {
			return true
		}
	}
	return false
}
