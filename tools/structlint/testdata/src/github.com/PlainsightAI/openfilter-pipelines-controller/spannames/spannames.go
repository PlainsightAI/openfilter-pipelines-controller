// Package spannames exercises the spannames analyzer. tracing.StartSpan and
// tracing.StartConsumerSpan must receive an identifier reference for their
// spanName arg; a raw string literal is flagged.
package spannames

import (
	"context"

	"github.com/PlainsightAI/openfilter-pipelines-controller/internal/tracing"
)

const spanOk = "FooReconciler.DoThing"

func constName(ctx context.Context) {
	_, end := tracing.StartSpan(ctx, spanOk)
	defer end()
}

func literalName(ctx context.Context) {
	_, end := tracing.StartSpan(ctx, "FooReconciler.DoThing") // want `spannames: spanName argument to tracing.StartSpan must be a named constant`
	defer end()
}

func consumerConst(ctx context.Context) {
	_, end := tracing.StartConsumerSpan(ctx, spanOk)
	defer end()
}

func consumerLiteral(ctx context.Context) {
	_, end := tracing.StartConsumerSpan(ctx, "FooReconciler.DoThing") // want `spannames: spanName argument to tracing.StartConsumerSpan must be a named constant`
	defer end()
}
