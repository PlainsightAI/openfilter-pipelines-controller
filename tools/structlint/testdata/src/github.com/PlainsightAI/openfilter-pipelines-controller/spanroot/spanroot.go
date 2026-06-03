// Package spanroot fixtures the spanroot analyzer: tracing.Stamp inside a
// rootless `go func` goroutine closure is a no-op and must be flagged unless
// the closure opens its own span via tracing.StartSpan first.
package spanroot

import (
	"context"

	"github.com/PlainsightAI/openfilter-pipelines-controller/internal/tracing"
)

// flagged: a goroutine closure that stamps a captured/rootless ctx.
func flagged(ctx context.Context, id string) {
	go func() {
		tracing.Stamp(ctx, tracing.PipelineInstanceID(id)) // want `spanroot: tracing\.Stamp in a rootless`
	}()
}

// clean: opens its own span first, so the (later) attributes have somewhere to
// land. No diagnostic.
func clean(ctx context.Context, id string) {
	go func() {
		ctx, end := tracing.StartSpan(ctx, "worker.tick", tracing.PipelineInstanceID(id))
		defer end()
		tracing.Stamp(ctx, tracing.ClaimAcquired(true))
	}()
}

// exempted by directive — no diagnostic despite the bare Stamp in a goroutine.
//
// spanroot:allow: fixture exercising the escape hatch
func exempted(ctx context.Context, id string) {
	go func() {
		tracing.Stamp(ctx, tracing.PipelineInstanceID(id))
	}()
}

// notAGoroutine: an ordinary reconcile-path Stamp (not inside a goroutine) —
// spanroot must NOT flag it; that is spancov's domain, and the reconcile span
// exists here.
func notAGoroutine(ctx context.Context, id string) {
	tracing.Stamp(ctx, tracing.PipelineInstanceID(id))
}
