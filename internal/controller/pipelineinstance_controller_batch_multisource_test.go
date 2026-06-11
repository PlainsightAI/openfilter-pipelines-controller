/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
)

// These tests close the coverage gap on reconcileBatchMultiSource — the
// top-level multi-source batch reconciler. Each test drives the function
// with a fake K8s client and verifies one of its six branches:
//
//   1. Bucket-source validation (binding whose source isn't a Bucket)
//   2. Object-key validation (binding whose Bucket.Object is empty)
//   3. Happy-path Job create + StartTime stamp
//   4. Job-complete observation → PipelineInstance Succeeded
//   5. Job-failed observation → PipelineInstance Degraded
//   6. Job-progressing observation → PipelineInstance Progressing
//
// The builders themselves (buildMultiSourceBatchJob etc.) are covered by
// pipelineinstance_multisource_test.go; these tests focus on the
// state-machine side.

// makeMultiSourcePI returns the PipelineInstance / Pipeline / 2-binding
// fixture the reconciler tests reuse. The bindings reference fake
// in-memory PipelineSource objects (also returned) so callers can mutate
// them per-test to drive the validation branches.
func makeMultiSourcePI(t *testing.T) (*pipelinesv1alpha1.PipelineInstance, *pipelinesv1alpha1.Pipeline, []ResolvedSourceBinding) {
	t.Helper()
	pi := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ms-batch-pi",
			Namespace: "default",
			UID:       types.UID("ms-batch-pi-uid"),
		},
		Spec: pipelinesv1alpha1.PipelineInstanceSpec{
			PipelineRef: pipelinesv1alpha1.PipelineReference{Name: "ms-batch-pipeline"},
			Sources: []pipelinesv1alpha1.NamedSourceRef{
				{FilterName: "front-cam", SourceRef: pipelinesv1alpha1.SourceReference{Name: "src-front"}},
				{FilterName: "back-cam", SourceRef: pipelinesv1alpha1.SourceReference{Name: "src-back"}},
			},
		},
	}
	pipeline := &pipelinesv1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "ms-batch-pipeline", Namespace: "default"},
		Spec: pipelinesv1alpha1.PipelineSpec{
			Mode: pipelinesv1alpha1.PipelineModeBatch,
			Filters: []pipelinesv1alpha1.Filter{
				{Name: "front-cam", Image: "videoin:latest"},
				{Name: "back-cam", Image: "videoin:latest"},
				{Name: "image-out", Image: "imageout:latest"},
			},
		},
	}
	bindings := []ResolvedSourceBinding{
		{FilterName: "front-cam", Source: &pipelinesv1alpha1.PipelineSource{
			Spec: pipelinesv1alpha1.PipelineSourceSpec{
				Bucket: &pipelinesv1alpha1.BucketSource{Name: "media", Prefix: "front.mp4"},
			},
		}},
		{FilterName: "back-cam", Source: &pipelinesv1alpha1.PipelineSource{
			Spec: pipelinesv1alpha1.PipelineSourceSpec{
				Bucket: &pipelinesv1alpha1.BucketSource{Name: "media", Prefix: "back.mp4"},
			},
		}},
	}
	return pi, pipeline, bindings
}

// newMSReconciler returns a reconciler wired to a fake client with the
// given seed objects, including status-subresource support so the
// reconciler's r.Status().Update calls succeed against the fake.
func newMSReconciler(t *testing.T, objects ...interface{}) *PipelineInstanceReconciler {
	t.Helper()
	sch := reconcileSpanScheme(t)

	// Convert to client.Object via type assertion guarded by the schema
	// registration above. Fake builder is happy with the interface.
	clientObjects := make([]any, 0, len(objects))
	clientObjects = append(clientObjects, objects...)

	builder := fake.NewClientBuilder().
		WithScheme(sch).
		WithStatusSubresource(&pipelinesv1alpha1.PipelineInstance{})
	for _, o := range clientObjects {
		switch obj := o.(type) {
		case *pipelinesv1alpha1.PipelineInstance:
			builder = builder.WithObjects(obj)
		case *pipelinesv1alpha1.Pipeline:
			builder = builder.WithObjects(obj)
		case *batchv1.Job:
			builder = builder.WithObjects(obj)
		default:
			t.Fatalf("unsupported seed object type %T", obj)
		}
	}
	return &PipelineInstanceReconciler{
		Client:       builder.Build(),
		Scheme:       sch,
		ClaimerImage: "claimer:test",
	}
}

// findCondition returns the condition with the given type, or zero value.
func findCondition(t *testing.T, conds []metav1.Condition, ctype string) metav1.Condition {
	t.Helper()
	for _, c := range conds {
		if c.Type == ctype {
			return c
		}
	}
	return metav1.Condition{}
}

// TestReconcileBatchMultiSource_RejectsNonBucketSource pins branch 1:
// any binding whose PipelineSource isn't a Bucket source surfaces a
// `MultiSourceBatchInvalidSource` Degraded condition and no Job is
// created.
func TestReconcileBatchMultiSource_RejectsNonBucketSource(t *testing.T) {
	pi, pipeline, bindings := makeMultiSourcePI(t)
	// Corrupt the back-cam binding to look like an RTSP source.
	bindings[1].Source = &pipelinesv1alpha1.PipelineSource{
		Spec: pipelinesv1alpha1.PipelineSourceSpec{
			RTSP: &pipelinesv1alpha1.RTSPSource{Host: "cam2"},
		},
	}
	r := newMSReconciler(t, pi)

	if _, err := r.reconcileBatchMultiSource(context.Background(), pi, pipeline, bindings); err != nil {
		t.Fatalf("expected nil error (validation failure surfaces via Condition), got %v", err)
	}

	updated := &pipelinesv1alpha1.PipelineInstance{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name, Namespace: pi.Namespace}, updated); err != nil {
		t.Fatalf("re-fetch PI: %v", err)
	}
	cond := findCondition(t, updated.Status.Conditions, ConditionTypeDegraded)
	if cond.Status != metav1.ConditionTrue || cond.Reason != "MultiSourceBatchInvalidSource" {
		t.Errorf("expected Degraded=True (MultiSourceBatchInvalidSource), got %+v", cond)
	}

	// No Job must have been created when validation rejects.
	job := &batchv1.Job{}
	err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name + "-job", Namespace: pi.Namespace}, job)
	if err == nil {
		t.Errorf("expected no Job to be created after validation failure")
	}
}

// TestReconcileBatchMultiSource_RejectsMissingObject pins branch 2: a
// binding with a Bucket source whose Prefix is empty (no object key to
// resolve) surfaces a `MultiSourceBatchMissingObject` Degraded condition.
func TestReconcileBatchMultiSource_RejectsMissingObject(t *testing.T) {
	pi, pipeline, bindings := makeMultiSourcePI(t)
	// Strip the object key on back-cam.
	bindings[1].Source.Spec.Bucket.Prefix = ""
	r := newMSReconciler(t, pi)

	if _, err := r.reconcileBatchMultiSource(context.Background(), pi, pipeline, bindings); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	updated := &pipelinesv1alpha1.PipelineInstance{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name, Namespace: pi.Namespace}, updated); err != nil {
		t.Fatalf("re-fetch PI: %v", err)
	}
	cond := findCondition(t, updated.Status.Conditions, ConditionTypeDegraded)
	if cond.Status != metav1.ConditionTrue || cond.Reason != "MultiSourceBatchMissingObject" {
		t.Errorf("expected Degraded=True (MultiSourceBatchMissingObject), got %+v", cond)
	}
}

// TestReconcileBatchMultiSource_CreatesJobAndStampsStartTime pins branch
// 3: with valid bindings, the reconciler creates the Job (named
// "<pi>-job"), stamps StartTime, sets Status.JobName, and leaves a
// Progressing=True condition.
func TestReconcileBatchMultiSource_CreatesJobAndStampsStartTime(t *testing.T) {
	pi, pipeline, bindings := makeMultiSourcePI(t)
	r := newMSReconciler(t, pi)

	if _, err := r.reconcileBatchMultiSource(context.Background(), pi, pipeline, bindings); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	job := &batchv1.Job{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name + "-job", Namespace: pi.Namespace}, job); err != nil {
		t.Fatalf("expected Job %s-job to exist: %v", pi.Name, err)
	}
	if len(job.Spec.Template.Spec.InitContainers) != 2 {
		t.Errorf("expected 2 init claimers, got %d", len(job.Spec.Template.Spec.InitContainers))
	}

	updated := &pipelinesv1alpha1.PipelineInstance{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name, Namespace: pi.Namespace}, updated); err != nil {
		t.Fatalf("re-fetch PI: %v", err)
	}
	if updated.Status.StartTime == nil {
		t.Errorf("expected StartTime to be stamped on first reconcile")
	}
	if updated.Status.JobName != pi.Name+"-job" {
		t.Errorf("expected Status.JobName = %q, got %q", pi.Name+"-job", updated.Status.JobName)
	}
	if cond := findCondition(t, updated.Status.Conditions, ConditionTypeProgressing); cond.Status != metav1.ConditionTrue {
		t.Errorf("expected Progressing=True after Job create, got %+v", cond)
	}
}

// TestReconcileBatchMultiSource_JobCompleteMarksSucceeded pins branch 4:
// when the observed Job has a JobComplete=True condition the reconciler
// surfaces Succeeded=True on the PipelineInstance and stamps
// CompletionTime.
func TestReconcileBatchMultiSource_JobCompleteMarksSucceeded(t *testing.T) {
	pi, pipeline, bindings := makeMultiSourcePI(t)
	// Pre-seed the PI with StartTime so reconciler skips the stamp.
	now := metav1.Now()
	pi.Status.StartTime = &now
	// Pre-seed the Job in Complete state.
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pi.Name + "-job",
			Namespace: pi.Namespace,
		},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{
				{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, Reason: "Completed", Message: "ok"},
			},
		},
	}
	r := newMSReconciler(t, pi, job)

	if _, err := r.reconcileBatchMultiSource(context.Background(), pi, pipeline, bindings); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	updated := &pipelinesv1alpha1.PipelineInstance{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name, Namespace: pi.Namespace}, updated); err != nil {
		t.Fatalf("re-fetch PI: %v", err)
	}
	if cond := findCondition(t, updated.Status.Conditions, ConditionTypeSucceeded); cond.Status != metav1.ConditionTrue {
		t.Errorf("expected Succeeded=True, got %+v", cond)
	}
	if cond := findCondition(t, updated.Status.Conditions, ConditionTypeProgressing); cond.Status != metav1.ConditionFalse {
		t.Errorf("expected Progressing=False once Succeeded, got %+v", cond)
	}
	if updated.Status.CompletionTime == nil {
		t.Errorf("expected CompletionTime to be stamped on success")
	}
}

// TestReconcileBatchMultiSource_JobFailedMarksDegraded pins branch 5:
// JobFailed=True translates to Degraded=True with the Job condition's
// Reason/Message propagated for operator visibility.
func TestReconcileBatchMultiSource_JobFailedMarksDegraded(t *testing.T) {
	pi, pipeline, bindings := makeMultiSourcePI(t)
	now := metav1.Now()
	pi.Status.StartTime = &now
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pi.Name + "-job",
			Namespace: pi.Namespace,
		},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{
				{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded", Message: "Job has reached the specified backoff limit"},
			},
		},
	}
	r := newMSReconciler(t, pi, job)

	if _, err := r.reconcileBatchMultiSource(context.Background(), pi, pipeline, bindings); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	updated := &pipelinesv1alpha1.PipelineInstance{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name, Namespace: pi.Namespace}, updated); err != nil {
		t.Fatalf("re-fetch PI: %v", err)
	}
	cond := findCondition(t, updated.Status.Conditions, ConditionTypeDegraded)
	if cond.Status != metav1.ConditionTrue {
		t.Fatalf("expected Degraded=True, got %+v", cond)
	}
	if cond.Reason != "BackoffLimitExceeded" {
		t.Errorf("expected Job condition Reason to propagate (got %q)", cond.Reason)
	}
	if cond.Message == "" || cond.Message != "Job has reached the specified backoff limit" {
		t.Errorf("expected Job condition Message to propagate (got %q)", cond.Message)
	}
	if updated.Status.CompletionTime == nil {
		t.Errorf("expected CompletionTime to be stamped on failure")
	}
}

// TestReconcileBatchMultiSource_JobProgressingStaysProgressing pins
// branch 6: a Job without any terminal condition leaves the PI in
// Progressing=True. This is the common steady-state pass.
func TestReconcileBatchMultiSource_JobProgressingStaysProgressing(t *testing.T) {
	pi, pipeline, bindings := makeMultiSourcePI(t)
	now := metav1.Now()
	pi.Status.StartTime = &now
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pi.Name + "-job",
			Namespace: pi.Namespace,
		},
		Status: batchv1.JobStatus{Active: 1},
	}
	r := newMSReconciler(t, pi, job)

	res, err := r.reconcileBatchMultiSource(context.Background(), pi, pipeline, bindings)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Errorf("expected RequeueAfter set for progressing reconcile")
	}

	updated := &pipelinesv1alpha1.PipelineInstance{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name, Namespace: pi.Namespace}, updated); err != nil {
		t.Fatalf("re-fetch PI: %v", err)
	}
	if cond := findCondition(t, updated.Status.Conditions, ConditionTypeProgressing); cond.Status != metav1.ConditionTrue {
		t.Errorf("expected Progressing=True for active Job, got %+v", cond)
	}
	if cond := findCondition(t, updated.Status.Conditions, ConditionTypeSucceeded); cond.Status == metav1.ConditionTrue {
		t.Errorf("Succeeded must not be True while Job is still active, got %+v", cond)
	}
}
