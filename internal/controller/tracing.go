/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

// Span names for the reconcile waterfall (PLAT-1000 / PLAT-1028). Declared as
// named constants — not string literals at the emit site — because they are
// Cloud Trace pivots: a query for `name = "PipelineInstanceReconciler.claim"`
// only returns hits if the emitter spelled the string exactly that way. The
// `spannames` structlint analyzer rejects string literals passed to
// tracing.StartSpan precisely so a typo here is a compile error rather than a
// silent miss in dashboards. Keeping all four in one place also keeps the
// batch and streaming paths (which both open build/apply spans) spelling the
// names identically, so the two lifecycles line up in the trace UI.
const (
	// spanReconcile is the root span for a single Reconcile pass; child of the
	// upstream agent span when a traceparent is present on the CR, otherwise a
	// new root.
	spanReconcile = "PipelineInstanceReconciler.Reconcile"

	// spanClaim covers the batch claim phase — bucket enumeration + Valkey
	// work-stream seeding (S3 LIST latency dominates).
	spanClaim = "PipelineInstanceReconciler.claim"

	// spanBuild covers the pure-CPU render of the Job/Deployment spec plus
	// owner-reference stamping. Shared by batch and streaming.
	spanBuild = "PipelineInstanceReconciler.build"

	// spanApply covers the kube-apiserver round-trip (Create/Patch). Shared by
	// batch and streaming.
	spanApply = "PipelineInstanceReconciler.apply"
)
