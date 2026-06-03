// Package otel is a minimal stub of the real go.opentelemetry.io/otel root
// package, just so the spanidiom fixture can `import _ "go.opentelemetry.io/otel"`
// and have the path resolve under analysistest. The analyzer matches on the
// import-path string, not on any types from this package — leaving the stub
// effectively empty is intentional.
package otel
