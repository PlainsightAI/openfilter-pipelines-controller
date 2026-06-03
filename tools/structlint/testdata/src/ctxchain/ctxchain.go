// Package ctxchain is a fixture for the ctxchain analyzer. The rule is
// codebase-wide (no scope allowlist), so the package path is irrelevant —
// what matters is whether a context.Context is in scope at each
// context.Background()/TODO() call site.
package ctxchain

import (
	"context"
	"time"
)

// hasCtxParam: a fresh root where a ctx is already in scope must be flagged.
func hasCtxParam(ctx context.Context) {
	_ = context.Background() // want `ctxchain: context\.Background\(\) called where a context\.Context is already in scope`
	_ = context.TODO()       // want `ctxchain: context\.TODO\(\) called where a context\.Context is already in scope`
	_ = ctx
}

// chainPreserving: derivations that take the parent ctx are never flagged.
func chainPreserving(ctx context.Context) {
	a, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	b := context.WithoutCancel(ctx)
	c := context.WithValue(ctx, struct{}{}, 1)
	_, _, _ = a, b, c
}

// entryPoint has no ctx in scope, so creating the root is correct and must
// NOT be flagged — this is the main()/worker-loop case.
func entryPoint() {
	ctx := context.Background()
	_ = ctx
}

// detachedGoroutine: a closure nested in a ctx-bearing function captures the
// outer ctx, so a fresh root inside the goroutine is the erasure bug.
func detachedGoroutine(ctx context.Context) {
	go func() {
		_ = context.Background() // want `ctxchain: context\.Background\(\) called where a context\.Context is already in scope`
	}()
	_ = ctx
}

// closureOwnParam: the closure introduces its own ctx parameter, so the root
// inside it is flagged on that basis even though the outer func has none.
func closureOwnParam() func(context.Context) {
	return func(ctx context.Context) {
		_ = context.Background() // want `ctxchain: context\.Background\(\) called where a context\.Context is already in scope`
		_ = ctx
	}
}

// exempted is annotated, so its fresh root is allowed.
//
// ctxchain:allow: standalone bootstrap that intentionally roots a new context
func exempted(ctx context.Context) {
	_ = context.Background()
	_ = ctx
}
