// Package spancov fixtures the spancov rule against reconcile methods on a
// *Reconciler receiver. The analyzer is codebase-wide; what it keys on is the
// receiver type name (ends in "Reconciler") and the method name ("Reconcile"
// or a reconcile* prefix). Each method exercises a different branch.
package spancov

import (
	"context"

	"github.com/PlainsightAI/openfilter-pipelines-controller/internal/tracing"
)

type Req struct{ ID string }
type Result struct{}

// FooReconciler is the in-scope receiver: its name ends in "Reconciler".
type FooReconciler struct{}

// Stamped: the entry point stamps the active reconcile span. No diagnostic.
func (r *FooReconciler) Reconcile(ctx context.Context, req Req) (Result, error) {
	tracing.Stamp(ctx, tracing.PipelineInstanceID(req.ID))
	return Result{}, nil
}

// Child-spanned: a reconcile* phase opens its own child span. No diagnostic.
func (r *FooReconciler) reconcileBatch(ctx context.Context, req Req) (Result, error) {
	_, end := tracing.StartSpan(ctx, "FooReconciler.batch")
	defer end()
	return Result{}, nil
}

// Untraced: a reconcile* phase with no tracing call — the regression the
// backstop catches.
func (r *FooReconciler) reconcileStreaming(ctx context.Context, req Req) (Result, error) { // want `spancov: reconcile method "reconcileStreaming" on FooReconciler`
	return Result{}, nil
}

// Exempted: the spancov:none directive with a non-empty reason suppresses the
// diagnostic. No diagnostic expected.
//
// spancov:none: pure delegation — reconcileBatch opens the span.
func (r *FooReconciler) reconcilePassthrough(ctx context.Context, req Req) (Result, error) {
	return r.reconcileBatch(ctx, req)
}

// Plain helper on the reconciler: name is neither "Reconcile" nor reconcile*,
// so it is out of scope even untraced.
func (r *FooReconciler) getPipeline(ctx context.Context, req Req) error {
	return nil
}

// plainController has a method named Reconcile, but its receiver type does NOT
// end in "Reconciler", so the analyzer ignores it — the suffix gate is what
// scopes spancov to actual controller-runtime reconcilers.
type plainController struct{}

func (n *plainController) Reconcile(ctx context.Context, req Req) (Result, error) {
	return Result{}, nil
}
