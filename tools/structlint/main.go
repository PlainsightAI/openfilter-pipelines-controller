// Command structlint enforces project-specific structural rules that the
// stock Go toolchain (go vet, golangci-lint with the default linters) does
// not. The single rule today — `attrkey` — exists because the typo it
// prevents would otherwise be a silent runtime regression: a Cloud Trace
// pivot that returns nothing because two services accidentally diverged on
// `pipeline.id` vs `pipeline_instance.uid`.
//
// Rules:
//
//   - attrkey   — rejects raw attribute.String(<canonical-key>, …) calls
//     outside internal/tracing/. Canonical keys are the
//     cross-domain Cloud Trace pivots; centralising their
//     construction in internal/tracing/attrs.go means a typo
//     is a lint error instead of a silent miss.
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
	multichecker.Main(attrkeyAnalyzer)
}
