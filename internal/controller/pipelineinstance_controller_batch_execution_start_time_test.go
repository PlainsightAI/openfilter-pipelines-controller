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

import (
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
)

// These tests cover ExecutionStartTime stamping (PLAT-1570) on the
// single-source (queue-mode) batch reconcile path: the durable anchor for
// deployment-agent's execution-phase timeout, distinct from the existing
// StartTime (which anchors the batch-phase timeout and is stamped by
// initializePipelineInstance, at the earlier "Initialized" condition write,
// before the Job/queue processing loop begins). ExecutionStartTime is only
// ever stamped at the later "Processing" condition write in reconcileBatch
// (pipelineinstance_controller_batch.go), which — unlike the multi-source
// path — already performs an unconditional Status().Update every pass, so
// no `changed`-gate decoupling is needed here.

// newBatchReconciler wires a reconciler to a fake client seeded with objs,
// including status-subresource support (so r.Status().Update succeeds) and
// a zero-valued MockValkeyClient (GetConsumerGroupLag / GetPendingCount /
// GetStreamLength all return 0, which keeps updateStatus + checkCompletion
// on the "still progressing" branch for a Job with no terminal condition).
func newBatchReconciler(t *testing.T, objs ...client.Object) *PipelineInstanceReconciler {
	t.Helper()
	sch := reconcileSpanScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(sch).
		WithStatusSubresource(&pipelinesv1alpha1.PipelineInstance{}).
		WithObjects(objs...).
		Build()
	return &PipelineInstanceReconciler{
		Client:       c,
		Scheme:       sch,
		ValkeyClient: &MockValkeyClient{},
		ClaimerImage: "claimer:latest",
		ValkeyAddr:   "valkey:6379",
	}
}

// makeExecStartTimeFixtures returns a PipelineInstance/Pipeline/PipelineSource/Job
// quadruple pre-seeded so initializePipelineInstance short-circuits on its
// "already initialized" branch (StartTime set, TotalFiles > 0) and ensureJob
// no-ops (Status.JobName already points at an existing Job). This isolates
// the tests on the Processing-condition write path without needing a full
// Valkey seeding / S3 listing flow.
func makeExecStartTimeFixtures(t *testing.T, name string) (*pipelinesv1alpha1.PipelineInstance, *pipelinesv1alpha1.Pipeline, *pipelinesv1alpha1.PipelineSource, *batchv1.Job) {
	t.Helper()
	pipeline := &pipelinesv1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "pipeline-" + name, Namespace: "default"},
		Spec: pipelinesv1alpha1.PipelineSpec{
			Mode: pipelinesv1alpha1.PipelineModeBatch,
			Filters: []pipelinesv1alpha1.Filter{
				{Name: "f", Image: "filter:latest"},
			},
		},
	}
	source := &pipelinesv1alpha1.PipelineSource{
		ObjectMeta: metav1.ObjectMeta{Name: "source-" + name, Namespace: "default"},
	}
	startTime := metav1.NewTime(time.Now())
	jobName := "pi-" + name + "-job"
	pi := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pi-" + name,
			Namespace: "default",
			UID:       types.UID("pi-uid-" + name),
		},
		Spec: pipelinesv1alpha1.PipelineInstanceSpec{
			PipelineRef: pipelinesv1alpha1.PipelineReference{Name: pipeline.Name},
			SourceRef:   &pipelinesv1alpha1.SourceReference{Name: source.Name},
		},
		Status: pipelinesv1alpha1.PipelineInstanceStatus{
			StartTime: &startTime,
			Counts:    &pipelinesv1alpha1.FileCounts{TotalFiles: 1},
			JobName:   jobName,
		},
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: "default"},
		Status:     batchv1.JobStatus{Active: 1},
	}
	return pi, pipeline, source, job
}

// TestReconcileBatch_ExecutionStartTimeNilAfterInitializedBeforeProcessing pins
// that the earlier "Initialized" condition write (paired with the StartTime
// stamp, before ensureJob/the queue-processing loop runs) intentionally does
// NOT touch ExecutionStartTime. Only the later "Processing" write anchors it.
func TestReconcileBatch_ExecutionStartTimeNilAfterInitializedBeforeProcessing(t *testing.T) {
	pi := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "pi-init-only", Namespace: "default"},
		Status:     pipelinesv1alpha1.PipelineInstanceStatus{Counts: &pipelinesv1alpha1.FileCounts{}},
	}
	source := &pipelinesv1alpha1.PipelineSource{
		ObjectMeta: metav1.ObjectMeta{Name: "source-init-only", Namespace: "default"},
	}
	r := newBatchReconciler(t, pi)
	r.ValkeyClient = &MockValkeyClient{StreamLength: 1}

	initialized, err := r.initializePipelineInstance(context.Background(), pi, source)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !initialized {
		t.Fatalf("expected initializePipelineInstance to report initialized=true")
	}
	if pi.Status.StartTime == nil {
		t.Fatalf("expected StartTime to be stamped by initializePipelineInstance")
	}
	if cond := findCondition(t, pi.Status.Conditions, ConditionTypeProgressing); cond.Reason != "Initialized" {
		t.Fatalf("expected Progressing/Initialized after initializePipelineInstance, got %+v", cond)
	}
	if pi.Status.ExecutionStartTime != nil {
		t.Errorf("expected ExecutionStartTime to stay nil at the Initialized write; got %v", pi.Status.ExecutionStartTime)
	}
}

// TestReconcileBatch_FirstProcessingPassStampsExecutionStartTime pins the
// happy path: the first reconcile pass that reaches the "Processing" branch
// stamps a non-nil ExecutionStartTime, verified via a fresh re-Get (not just
// the in-memory object) so the assertion actually proves Status().Update
// persisted the field.
func TestReconcileBatch_FirstProcessingPassStampsExecutionStartTime(t *testing.T) {
	pi, pipeline, source, job := makeExecStartTimeFixtures(t, "first-pass")
	if pi.Status.ExecutionStartTime != nil {
		t.Fatalf("expected ExecutionStartTime nil before reconcile")
	}
	r := newBatchReconciler(t, pi, pipeline, source, job)

	if _, err := r.reconcileBatch(context.Background(), pi, pipeline, []ResolvedSourceBinding{{Source: source}}); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	updated := &pipelinesv1alpha1.PipelineInstance{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name, Namespace: pi.Namespace}, updated); err != nil {
		t.Fatalf("re-fetch PI: %v", err)
	}
	if updated.Status.ExecutionStartTime == nil {
		t.Fatalf("expected ExecutionStartTime to be stamped on first reconcile pass that reaches Processing")
	}
	if cond := findCondition(t, updated.Status.Conditions, ConditionTypeProgressing); cond.Reason != "Processing" {
		t.Errorf("expected Progressing/Processing, got %+v", cond)
	}
}

// TestReconcileBatch_ExecutionStartTimeUnchangedOnSecondPass pins the
// stamp-once invariant: a second reconcile pass with the Job still running
// leaves a pre-seeded ExecutionStartTime byte-identical, not just non-nil.
func TestReconcileBatch_ExecutionStartTimeUnchangedOnSecondPass(t *testing.T) {
	pi, pipeline, source, job := makeExecStartTimeFixtures(t, "second-pass")
	fixed := metav1.NewTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	pi.Status.ExecutionStartTime = &fixed
	r := newBatchReconciler(t, pi, pipeline, source, job)

	if _, err := r.reconcileBatch(context.Background(), pi, pipeline, []ResolvedSourceBinding{{Source: source}}); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	updated := &pipelinesv1alpha1.PipelineInstance{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name, Namespace: pi.Namespace}, updated); err != nil {
		t.Fatalf("re-fetch PI: %v", err)
	}
	if updated.Status.ExecutionStartTime == nil {
		t.Fatalf("expected ExecutionStartTime to remain set")
	}
	if !updated.Status.ExecutionStartTime.Time.Equal(fixed.Time) {
		t.Errorf("expected ExecutionStartTime unchanged, got %v want %v", updated.Status.ExecutionStartTime.Time, fixed.Time)
	}
}

// TestReconcileBatch_DegradedPathDoesNotChangeExecutionStartTime pins that a
// terminal Degraded transition reached after ExecutionStartTime was already
// set leaves it untouched — the field is a monotonic execution-start anchor,
// not a completion-adjacent timestamp.
func TestReconcileBatch_DegradedPathDoesNotChangeExecutionStartTime(t *testing.T) {
	pi, pipeline, source, job := makeExecStartTimeFixtures(t, "degraded")
	fixed := metav1.NewTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	pi.Status.ExecutionStartTime = &fixed
	job.Status = batchv1.JobStatus{
		Conditions: []batchv1.JobCondition{
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded", Message: "boom"},
		},
	}
	r := newBatchReconciler(t, pi, pipeline, source, job)

	if _, err := r.reconcileBatch(context.Background(), pi, pipeline, []ResolvedSourceBinding{{Source: source}}); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	updated := &pipelinesv1alpha1.PipelineInstance{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name, Namespace: pi.Namespace}, updated); err != nil {
		t.Fatalf("re-fetch PI: %v", err)
	}
	if cond := findCondition(t, updated.Status.Conditions, ConditionTypeDegraded); cond.Status != metav1.ConditionTrue {
		t.Fatalf("expected Degraded=True, got %+v", cond)
	}
	if updated.Status.ExecutionStartTime == nil || !updated.Status.ExecutionStartTime.Time.Equal(fixed.Time) {
		t.Errorf("expected ExecutionStartTime unchanged by Degraded path, got %v want %v", updated.Status.ExecutionStartTime, fixed.Time)
	}
}
