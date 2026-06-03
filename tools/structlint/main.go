// Command structlint enforces project-specific structural rules that the
// stock Go toolchain (go vet, golangci-lint with the default linters) does
// not. The rules exist because the bugs they prevent are silent runtime
// regressions: a Cloud Trace pivot that returns nothing because two services
// diverged on `pipeline.id` vs `pipeline_instance.uid`, a reconcile phase that
// forgot to open a span, a goroutine that severs the trace chain.
//
// Rules:
//
//   - attrkey   — rejects raw attribute.String(<canonical-key>, …) calls
//     outside internal/tracing/. Canonical keys are the
//     cross-domain Cloud Trace pivots; centralising their
//     construction in internal/tracing/attrs.go means a typo
//     is a lint error instead of a silent miss.
//
//   - ctxchain  — rejects context.Background()/context.TODO() inside any
//     function that already has a context.Context in scope (param,
//     or captured by a nested closure). Minting a fresh root
//     discards the incoming trace span, request ID, and logger
//     values — the silent instrumentation-erasure bug. Fix is the
//     in-scope ctx, or context.WithoutCancel(ctx) to detach
//     cancellation while keeping those values. Escape hatch is
//     `// ctxchain:allow: <reason>`. Codebase-wide.
//
//   - spancov   — flags Reconcile entry points and reconcile* phase methods on
//     *Reconciler receivers that fail to call tracing.Stamp /
//     tracing.StartSpan / tracing.StartConsumerSpan. The reconcile
//     loop is this repo's request surface, so it is where trace
//     coverage must be guaranteed. Codebase-wide and absolute — no
//     grandfathering baseline; the only escape is a
//     `// spancov:none: <reason>` doc directive (reason mandatory).
//
//   - spanidiom — homogenises the tracing surface by rejecting direct imports
//     of go.opentelemetry.io/otel and go.opentelemetry.io/otel/trace
//     outside internal/tracing. Pairs with spancov: spancov demands
//     tracing coverage; spanidiom demands the coverage be expressed
//     through the same internal/tracing helpers everywhere.
//
//   - spannames — rejects raw string literals as the spanName argument to
//     tracing.StartSpan / tracing.StartConsumerSpan. The span name
//     is a Cloud Trace pivot; a literal at the emit site means a
//     typo silently produces an unqueryable span. Named constants
//     (per-package tracing.go) make the typo a compile error.
//
//   - spanroot  — closes spancov's blind spot: spancov checks that a Stamp is
//     present, not that a recording span exists for it to attach to.
//     Inside a rootless `go func` goroutine the captured reconcile
//     span has very likely ended, so a bare tracing.Stamp is a
//     silent no-op. Flags those unless the closure opens its own
//     span via tracing.StartSpan first. Escape hatch
//     `// spanroot:allow: <reason>`.
//
// Run:
//
//	go run ./tools/structlint ./...
//
// Wired into `make lint-struct` and the `make lint` aggregate.
package main

import (
	"golang.org/x/tools/go/analysis/multichecker"
)

func main() {
	multichecker.Main(
		attrkeyAnalyzer,
		ctxchainAnalyzer,
		spancovAnalyzer,
		spanidiomAnalyzer,
		spannamesAnalyzer,
		spanrootAnalyzer,
	)
}
