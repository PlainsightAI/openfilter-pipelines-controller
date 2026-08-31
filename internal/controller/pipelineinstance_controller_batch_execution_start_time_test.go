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
	"fmt"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

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
//
// PLAT-1597 tightened when that write actually stamps: a Job existing is not
// evidence that its pod has started, so ExecutionStartTime now only stamps
// once anyPodStarted (pipelineinstance_controller_batch.go) confirms a live
// Running/Succeeded pod for the instance — never on the Job-exists-only
// fallthrough, and never on the pod-list fail-open path. Tests below that
// expect ExecutionStartTime to stamp must seed a Running pod alongside the
// Job; tests seeding no pod (or only a Pending one) now correctly expect it
// to stay nil and the Progressing condition to read Reason="Starting".

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

// newBatchReconcilerWithFailingPodList wires a reconciler the same way as
// newBatchReconciler, except every List against *corev1.PodList is
// intercepted to return a simulated error — the only way to exercise
// anyPodStarted's fail-open path against the fake client, since it has no
// pod to report Pending/Running/Succeeded that would otherwise trigger a
// real failure. Every other pod-listing step in reconcileBatch
// (handleCompletedPods, runReclaimer) already logs and continues on a List
// error rather than aborting, so intercepting unconditionally still lets the
// reconcile pass reach the Progressing-condition write this test asserts on.
// A record.FakeRecorder is attached so the PodStatusListDegraded warning
// event can be asserted too — neither this helper nor newMSReconciler
// otherwise sets Recorder, so the `r.Recorder != nil` guard is only ever
// exercised on its nil side.
func newBatchReconcilerWithFailingPodList(t *testing.T, objs ...client.Object) (*PipelineInstanceReconciler, *record.FakeRecorder) {
	t.Helper()
	sch := reconcileSpanScheme(t)
	base := fake.NewClientBuilder().
		WithScheme(sch).
		WithStatusSubresource(&pipelinesv1alpha1.PipelineInstance{}).
		WithObjects(objs...).
		Build()
	wrapped := interceptor.NewClient(base, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.PodList); ok {
				return fmt.Errorf("simulated pod list failure")
			}
			return c.List(ctx, list, opts...)
		},
	})
	recorder := record.NewFakeRecorder(10)
	return &PipelineInstanceReconciler{
		Client:       wrapped,
		Scheme:       sch,
		ValkeyClient: &MockValkeyClient{},
		ClaimerImage: "claimer:latest",
		ValkeyAddr:   "valkey:6379",
		Recorder:     recorder,
	}, recorder
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
	// A Running pod is required evidence (PLAT-1597): the Job existing alone
	// is no longer sufficient for either the Processing reason or the
	// ExecutionStartTime stamp.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pi-first-pass-pod",
			Namespace: "default",
			Labels:    map[string]string{"filter.plainsight.ai/instance": string(pi.UID)},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	r := newBatchReconciler(t, pi, pipeline, source, job, pod)

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
	if cond := findCondition(t, updated.Status.Conditions, ConditionTypeProgressing); cond.Reason != ReasonProcessing {
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

// TestReconcileBatch_PendingPodReasonStarting is the PLAT-1597 regression
// test: a Job whose only pod is stuck Pending (no NodeName, no
// ContainerStatuses — the reported reproduction shape) must NOT be reported
// as Processing, and must NOT have ExecutionStartTime stamped.
func TestReconcileBatch_PendingPodReasonStarting(t *testing.T) {
	pi, pipeline, source, job := makeExecStartTimeFixtures(t, "pending-pod")
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pi-pending-pod-pod",
			Namespace: "default",
			Labels:    map[string]string{"filter.plainsight.ai/instance": string(pi.UID)},
		},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}
	r := newBatchReconciler(t, pi, pipeline, source, job, pod)

	if _, err := r.reconcileBatch(context.Background(), pi, pipeline, []ResolvedSourceBinding{{Source: source}}); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	updated := &pipelinesv1alpha1.PipelineInstance{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name, Namespace: pi.Namespace}, updated); err != nil {
		t.Fatalf("re-fetch PI: %v", err)
	}
	if cond := findCondition(t, updated.Status.Conditions, ConditionTypeProgressing); cond.Reason != ReasonStarting {
		t.Errorf("expected Progressing/Starting for a Pending pod, got %+v", cond)
	}
	if updated.Status.ExecutionStartTime != nil {
		t.Errorf("expected ExecutionStartTime to stay nil while no pod has started, got %v", updated.Status.ExecutionStartTime)
	}
}

// TestReconcileBatch_InitContainerRunningReasonProcessing is the regression
// test for the init-container gap flagged in review: a pod whose claimer
// init container is running (or has already terminated) is doing real work
// even though Kubernetes keeps the pod's overall Phase at Pending for the
// entire init-container window. Before the anyPodStarted fix this reported
// Starting — exposing an actively-downloading pod to the agent's
// provisioningTimeout even though it had genuinely started.
func TestReconcileBatch_InitContainerRunningReasonProcessing(t *testing.T) {
	pi, pipeline, source, job := makeExecStartTimeFixtures(t, "init-container-running")
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pi-init-container-running-pod",
			Namespace: "default",
			Labels:    map[string]string{"filter.plainsight.ai/instance": string(pi.UID)},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			InitContainerStatuses: []corev1.ContainerStatus{
				{
					Name:  "claimer",
					State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
				},
			},
		},
	}
	r := newBatchReconciler(t, pi, pipeline, source, job, pod)

	if _, err := r.reconcileBatch(context.Background(), pi, pipeline, []ResolvedSourceBinding{{Source: source}}); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	updated := &pipelinesv1alpha1.PipelineInstance{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name, Namespace: pi.Namespace}, updated); err != nil {
		t.Fatalf("re-fetch PI: %v", err)
	}
	if cond := findCondition(t, updated.Status.Conditions, ConditionTypeProgressing); cond.Reason != ReasonProcessing {
		t.Errorf("expected Progressing/Processing for a Pending pod with a running init container, got %+v", cond)
	}
	if updated.Status.ExecutionStartTime == nil {
		t.Errorf("expected ExecutionStartTime to be stamped once the claimer init container is running")
	}
}

// TestReconcileBatch_InitContainerTerminatedReasonProcessing covers the
// terminated (not just running) init-container state — a claimer that has
// already finished downloading but whose pod has not yet moved past Pending
// to start the filter containers is equally evidence of a started pod.
func TestReconcileBatch_InitContainerTerminatedReasonProcessing(t *testing.T) {
	pi, pipeline, source, job := makeExecStartTimeFixtures(t, "init-container-terminated")
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pi-init-container-terminated-pod",
			Namespace: "default",
			Labels:    map[string]string{"filter.plainsight.ai/instance": string(pi.UID)},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			InitContainerStatuses: []corev1.ContainerStatus{
				{
					Name:  "claimer",
					State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}},
				},
			},
		},
	}
	r := newBatchReconciler(t, pi, pipeline, source, job, pod)

	if _, err := r.reconcileBatch(context.Background(), pi, pipeline, []ResolvedSourceBinding{{Source: source}}); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	updated := &pipelinesv1alpha1.PipelineInstance{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name, Namespace: pi.Namespace}, updated); err != nil {
		t.Fatalf("re-fetch PI: %v", err)
	}
	if cond := findCondition(t, updated.Status.Conditions, ConditionTypeProgressing); cond.Reason != ReasonProcessing {
		t.Errorf("expected Progressing/Processing for a Pending pod with a terminated init container, got %+v", cond)
	}
	if updated.Status.ExecutionStartTime == nil {
		t.Errorf("expected ExecutionStartTime to be stamped once the claimer init container has terminated")
	}
}

// TestReconcileBatch_NoPodsYetReasonStarting covers the race right after
// ensureJob succeeds but before the Job controller has created any pod
// object yet — an empty pod list must resolve to Starting, not Processing.
func TestReconcileBatch_NoPodsYetReasonStarting(t *testing.T) {
	pi, pipeline, source, job := makeExecStartTimeFixtures(t, "no-pods-yet")
	r := newBatchReconciler(t, pi, pipeline, source, job)

	if _, err := r.reconcileBatch(context.Background(), pi, pipeline, []ResolvedSourceBinding{{Source: source}}); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	updated := &pipelinesv1alpha1.PipelineInstance{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name, Namespace: pi.Namespace}, updated); err != nil {
		t.Fatalf("re-fetch PI: %v", err)
	}
	if cond := findCondition(t, updated.Status.Conditions, ConditionTypeProgressing); cond.Reason != ReasonStarting {
		t.Errorf("expected Progressing/Starting with zero pods, got %+v", cond)
	}
	if updated.Status.ExecutionStartTime != nil {
		t.Errorf("expected ExecutionStartTime to stay nil with zero pods, got %v", updated.Status.ExecutionStartTime)
	}
}

// TestReconcileBatch_PodListErrorFailsOpenToProcessing covers the fail-open
// branch flagged in review as untested: when anyPodStarted's pod List call
// itself errors (e.g. a transient API-server or cache outage), reconcileBatch
// must not treat that as "not started" — it fails open to Reason=Processing
// (never Starting) while still withholding ExecutionStartTime, since a
// transient error is not confirmed live evidence a pod actually ran. It must
// also emit a PodStatusListDegraded warning Event so the ambiguity is
// operator-visible.
func TestReconcileBatch_PodListErrorFailsOpenToProcessing(t *testing.T) {
	pi, pipeline, source, job := makeExecStartTimeFixtures(t, "pod-list-error")
	r, recorder := newBatchReconcilerWithFailingPodList(t, pi, pipeline, source, job)

	if _, err := r.reconcileBatch(context.Background(), pi, pipeline, []ResolvedSourceBinding{{Source: source}}); err != nil {
		t.Fatalf("expected nil error (fail-open, not fail-closed), got %v", err)
	}

	updated := &pipelinesv1alpha1.PipelineInstance{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name, Namespace: pi.Namespace}, updated); err != nil {
		t.Fatalf("re-fetch PI: %v", err)
	}
	if cond := findCondition(t, updated.Status.Conditions, ConditionTypeProgressing); cond.Reason != ReasonProcessing {
		t.Errorf("expected Progressing/Processing (fail-open) when the pod list errors, got %+v", cond)
	}
	if updated.Status.ExecutionStartTime != nil {
		t.Errorf("expected ExecutionStartTime to stay nil on the fail-open path (no confirmed live evidence), got %v", updated.Status.ExecutionStartTime)
	}

	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, "PodStatusListDegraded") {
			t.Errorf("expected a PodStatusListDegraded event, got %q", event)
		}
	default:
		t.Errorf("expected a PodStatusListDegraded event to be recorded, got none")
	}
}

// TestReconcileBatch_CrashRetryDoesNotRegressReasonToStarting is the
// sticky/monotonicity regression test: once ExecutionStartTime has been
// stamped by a prior pass that saw a Running pod, a later pass where that
// pod has crashed and is mid-replacement (Failed + a fresh Pending retry,
// per Job BackoffLimit — no currently-live Running/Succeeded pod) must NOT
// regress Reason back to "Starting". The field and the Reason must stay in
// agreement; ExecutionStartTime remains the source of truth once set.
func TestReconcileBatch_CrashRetryDoesNotRegressReasonToStarting(t *testing.T) {
	pi, pipeline, source, job := makeExecStartTimeFixtures(t, "crash-retry")
	fixed := metav1.NewTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	pi.Status.ExecutionStartTime = &fixed
	failedPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pi-crash-retry-pod-1",
			Namespace: "default",
			Labels:    map[string]string{"filter.plainsight.ai/instance": string(pi.UID)},
		},
		Status: corev1.PodStatus{Phase: corev1.PodFailed},
	}
	retryPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pi-crash-retry-pod-2",
			Namespace: "default",
			Labels:    map[string]string{"filter.plainsight.ai/instance": string(pi.UID)},
		},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}
	r := newBatchReconciler(t, pi, pipeline, source, job, failedPod, retryPod)

	if _, err := r.reconcileBatch(context.Background(), pi, pipeline, []ResolvedSourceBinding{{Source: source}}); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	updated := &pipelinesv1alpha1.PipelineInstance{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name, Namespace: pi.Namespace}, updated); err != nil {
		t.Fatalf("re-fetch PI: %v", err)
	}
	if cond := findCondition(t, updated.Status.Conditions, ConditionTypeProgressing); cond.Reason != ReasonProcessing {
		t.Errorf("expected Progressing/Processing to stay sticky during crash-retry (no live Running/Succeeded pod), got %+v", cond)
	}
	if updated.Status.ExecutionStartTime == nil || !updated.Status.ExecutionStartTime.Time.Equal(fixed.Time) {
		t.Errorf("expected ExecutionStartTime unchanged during crash-retry, got %v want %v", updated.Status.ExecutionStartTime, fixed.Time)
	}
}
