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
	"strings"
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
//   2. Object-key validation (binding whose Bucket.Prefix — the full object key — is empty)
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
	if prog := findCondition(t, updated.Status.Conditions, ConditionTypeProgressing); prog.Status == metav1.ConditionTrue {
		t.Errorf("expected Progressing!=True alongside the validation Degraded, got %+v", prog)
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
	if prog := findCondition(t, updated.Status.Conditions, ConditionTypeProgressing); prog.Status == metav1.ConditionTrue {
		t.Errorf("expected Progressing!=True alongside the validation Degraded, got %+v", prog)
	}
}

// TestReconcileBatchMultiSource_ClearsValidationDegradedOnRecovery pins the
// recovery half of the validation contract: a reconcile that Degrades on a
// missing object key must NOT leave the instance Degraded forever — once the
// PipelineSource is fixed (the event the PipelineSource watch mapping in
// SetupWithManager re-triggers reconcile for), the next pass clears the
// Degraded condition and proceeds to create the Job.
func TestReconcileBatchMultiSource_ClearsValidationDegradedOnRecovery(t *testing.T) {
	pi, pipeline, bindings := makeMultiSourcePI(t)
	// Break the back-cam binding: empty prefix → no object key.
	bindings[1].Source.Spec.Bucket.Prefix = ""
	r := newMSReconciler(t, pi)

	// Pass 1: validation fails, Degraded=True is persisted.
	if _, err := r.reconcileBatchMultiSource(context.Background(), pi, pipeline, bindings); err != nil {
		t.Fatalf("first reconcile: expected nil error, got %v", err)
	}
	degraded := &pipelinesv1alpha1.PipelineInstance{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name, Namespace: pi.Namespace}, degraded); err != nil {
		t.Fatalf("re-fetch PI after first reconcile: %v", err)
	}
	cond := findCondition(t, degraded.Status.Conditions, ConditionTypeDegraded)
	if cond.Status != metav1.ConditionTrue || cond.Reason != ReasonMultiSourceBatchMissingObject {
		t.Fatalf("expected Degraded=True (%s) after first reconcile, got %+v", ReasonMultiSourceBatchMissingObject, cond)
	}

	// Fix the source, then re-reconcile against the freshly fetched PI —
	// the shape a watch-triggered reconcile would see.
	bindings[1].Source.Spec.Bucket.Prefix = "back.mp4"
	if _, err := r.reconcileBatchMultiSource(context.Background(), degraded, pipeline, bindings); err != nil {
		t.Fatalf("second reconcile: expected nil error, got %v", err)
	}

	updated := &pipelinesv1alpha1.PipelineInstance{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name, Namespace: pi.Namespace}, updated); err != nil {
		t.Fatalf("re-fetch PI after second reconcile: %v", err)
	}
	if cond := findCondition(t, updated.Status.Conditions, ConditionTypeDegraded); cond.Type != "" {
		t.Errorf("expected Degraded condition to be cleared after the source was fixed, got %+v", cond)
	}
	if cond := findCondition(t, updated.Status.Conditions, ConditionTypeProgressing); cond.Status != metav1.ConditionTrue {
		t.Errorf("expected Progressing=True after recovery, got %+v", cond)
	}
	job := &batchv1.Job{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name + "-job", Namespace: pi.Namespace}, job); err != nil {
		t.Errorf("expected Job to be created after recovery: %v", err)
	}
}

// TestClaimerContainerName pins the 63-char container-name budget: the CRD
// allows filterName up to 63 chars, and "claimer-" (8 chars) on top of that
// would exceed Kubernetes' DNS-1123 container-name limit. The helper must
// truncate the filterName to 55 chars (63 - 8), deterministically, and never
// leave a trailing hyphen at the cut point.
func TestClaimerContainerName(t *testing.T) {
	if got := claimerContainerName("front-cam"); got != "claimer-front-cam" {
		t.Errorf("short name must pass through untruncated, got %q", got)
	}

	long := strings.Repeat("a", 63) // max the CRD pattern allows
	got := claimerContainerName(long)
	if len(got) > 63 {
		t.Errorf("claimer name for 63-char filterName exceeds 63 chars: %q (len %d)", got, len(got))
	}
	if want := "claimer-" + strings.Repeat("a", 55); got != want {
		t.Errorf("claimer name = %q, want %q", got, want)
	}
	if again := claimerContainerName(long); again != got {
		t.Errorf("truncation must be deterministic: %q != %q", again, got)
	}

	// A hyphen landing exactly at the cut point must be trimmed — DNS-1123
	// labels can't end with '-'.
	hyphenAtCut := strings.Repeat("a", 54) + "-" + strings.Repeat("b", 8) // 63 chars; [:55] ends with '-'
	got = claimerContainerName(hyphenAtCut)
	if strings.HasSuffix(got, "-") {
		t.Errorf("claimer name must not end with a hyphen, got %q", got)
	}
	if want := "claimer-" + strings.Repeat("a", 54); got != want {
		t.Errorf("claimer name = %q, want %q", got, want)
	}
}

// TestPerFilterInputPath pins the claimer↔filter path contract: the claimer
// downloads to this exact path and the filter container's
// `file://$(VIDEO_INPUT_PATH)` must resolve to the same bytes — a drift here
// breaks batch runs with a file-not-found deep inside the pod.
func TestPerFilterInputPath(t *testing.T) {
	cases := []struct {
		name       string
		filterName string
		objectKey  string
		want       string
	}{
		{"extension preserved", "front-cam", "clips/front_lobby_001.mp4", "/ws/front-cam.mp4"},
		{"non-mp4 extension preserved", "back-cam", "clips/back.mkv", "/ws/back-cam.mkv"},
		{"no extension defaults to mp4", "front-cam", "clips/raw-dump", "/ws/front-cam.mp4"},
		{"empty key defaults to mp4", "front-cam", "", "/ws/front-cam.mp4"},
		{"double extension keeps last", "cam", "clips/video.tar.gz", "/ws/cam.gz"},
		{"dot in directory not an extension", "cam", "v1.2/clip", "/ws/cam.mp4"},
		{"trailing dot yields bare dot ext", "cam", "clips/video.", "/ws/cam."},
		{"uppercase extension preserved verbatim", "cam", "clips/VIDEO.MP4", "/ws/cam.MP4"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := perFilterInputPath(tc.filterName, tc.objectKey); got != tc.want {
				t.Errorf("perFilterInputPath(%q, %q) = %q, want %q", tc.filterName, tc.objectKey, got, tc.want)
			}
		})
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

// TestIsDirectModeSingleBinding pins the routing predicate for the
// single-binding direct path: only a NAMED binding (plural sources form)
// whose bucket declares singleObject takes direct mode; everything else
// keeps the queue flow.
func TestIsDirectModeSingleBinding(t *testing.T) {
	singleObjBucket := &pipelinesv1alpha1.PipelineSource{
		Spec: pipelinesv1alpha1.PipelineSourceSpec{
			Bucket: &pipelinesv1alpha1.BucketSource{Name: "media", Prefix: "clip.png", SingleObject: true},
		},
	}
	prefixBucket := &pipelinesv1alpha1.PipelineSource{
		Spec: pipelinesv1alpha1.PipelineSourceSpec{
			Bucket: &pipelinesv1alpha1.BucketSource{Name: "media", Prefix: "clips/"},
		},
	}
	rtsp := &pipelinesv1alpha1.PipelineSource{
		Spec: pipelinesv1alpha1.PipelineSourceSpec{
			RTSP: &pipelinesv1alpha1.RTSPSource{Host: "cam.example"},
		},
	}

	cases := []struct {
		name     string
		bindings []ResolvedSourceBinding
		want     bool
	}{
		{"named binding with singleObject bucket", []ResolvedSourceBinding{{FilterName: "image-in", Source: singleObjBucket}}, true},
		{"named binding without singleObject", []ResolvedSourceBinding{{FilterName: "image-in", Source: prefixBucket}}, false},
		{"broadcast sentinel (legacy sourceRef) never direct", []ResolvedSourceBinding{{FilterName: "", Source: singleObjBucket}}, false},
		{"rtsp source never direct", []ResolvedSourceBinding{{FilterName: "cam", Source: rtsp}}, false},
		{"nil source", []ResolvedSourceBinding{{FilterName: "cam", Source: nil}}, false},
		{"two bindings handled by the multi-source branch, not this predicate", []ResolvedSourceBinding{
			{FilterName: "a", Source: singleObjBucket}, {FilterName: "b", Source: singleObjBucket},
		}, false},
		{"no bindings", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isDirectModeSingleBinding(tc.bindings); got != tc.want {
				t.Errorf("isDirectModeSingleBinding() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestReconcileBatch_SingleBindingSingleObject_UsesDirectMode pins the fix
// for single-source batch image pipelines: a single named binding whose
// bucket declares singleObject routes through reconcileBatch into the
// direct-download path — one init claimer carrying S3_OBJECT_KEY (no
// Valkey env), VIDEO_INPUT_PATH preserving the object's extension
// (/ws/image-in.png, not the queue path's fixed input.mp4), and a
// parallelism=1/completions=1 Job. ValkeyClient is intentionally nil:
// touching the queue would panic.
func TestReconcileBatch_SingleBindingSingleObject_UsesDirectMode(t *testing.T) {
	pi := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "single-direct-pi",
			Namespace: "default",
			UID:       types.UID("single-direct-pi-uid"),
		},
		Spec: pipelinesv1alpha1.PipelineInstanceSpec{
			PipelineRef: pipelinesv1alpha1.PipelineReference{Name: "single-direct-pipeline"},
			Sources: []pipelinesv1alpha1.NamedSourceRef{
				{FilterName: "image-in", SourceRef: pipelinesv1alpha1.SourceReference{Name: "src-img"}},
			},
		},
	}
	pipeline := &pipelinesv1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "single-direct-pipeline", Namespace: "default"},
		Spec: pipelinesv1alpha1.PipelineSpec{
			Mode: pipelinesv1alpha1.PipelineModeBatch,
			Filters: []pipelinesv1alpha1.Filter{
				{Name: "image-in", Image: "imagein:latest"},
				{Name: "image-out", Image: "imageout:latest"},
			},
		},
	}
	bindings := []ResolvedSourceBinding{
		{FilterName: "image-in", Source: &pipelinesv1alpha1.PipelineSource{
			Spec: pipelinesv1alpha1.PipelineSourceSpec{
				Bucket: &pipelinesv1alpha1.BucketSource{Name: "media", Prefix: "images/triangle.png", SingleObject: true},
			},
		}},
	}
	r := newMSReconciler(t, pi)

	if _, err := r.reconcileBatch(context.Background(), pi, pipeline, bindings); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	job := &batchv1.Job{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name + "-job", Namespace: pi.Namespace}, job); err != nil {
		t.Fatalf("expected direct-mode Job to exist: %v", err)
	}
	if len(job.Spec.Template.Spec.InitContainers) != 1 {
		t.Fatalf("expected 1 init claimer, got %d", len(job.Spec.Template.Spec.InitContainers))
	}
	claimer := job.Spec.Template.Spec.InitContainers[0]
	if claimer.Name != "claimer-image-in" {
		t.Errorf("claimer name = %q, want claimer-image-in", claimer.Name)
	}
	var objectKey, inputPath string
	hasValkey := false
	for _, e := range claimer.Env {
		switch e.Name {
		case "S3_OBJECT_KEY":
			objectKey = e.Value
		case "VIDEO_INPUT_PATH":
			inputPath = e.Value
		case "VALKEY_URL", "STREAM", "GROUP":
			hasValkey = true
		}
	}
	if objectKey != "images/triangle.png" {
		t.Errorf("S3_OBJECT_KEY = %q, want images/triangle.png", objectKey)
	}
	if inputPath != "/ws/image-in.png" {
		t.Errorf("VIDEO_INPUT_PATH = %q, want /ws/image-in.png (extension preserved)", inputPath)
	}
	if hasValkey {
		t.Errorf("direct-mode claimer must not carry Valkey env, got %v", claimer.Env)
	}
}

// TestReconcileBatch_SingleBindingWithoutFlag_KeepsQueueMode pins backward
// compatibility: a single binding whose bucket does NOT declare
// singleObject must keep the queue flow — reconcileBatch must not route
// it into the direct path (asserted by the absence of the direct-mode
// Job; the queue path errors immediately on this fixture's nil
// ValkeyClient, which is fine — reaching the queue IS the assertion).
func TestReconcileBatch_SingleBindingWithoutFlag_KeepsQueueMode(t *testing.T) {
	pi := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "single-queue-pi",
			Namespace: "default",
			UID:       types.UID("single-queue-pi-uid"),
		},
		Spec: pipelinesv1alpha1.PipelineInstanceSpec{
			PipelineRef: pipelinesv1alpha1.PipelineReference{Name: "single-queue-pipeline"},
			Sources: []pipelinesv1alpha1.NamedSourceRef{
				{FilterName: "video-in", SourceRef: pipelinesv1alpha1.SourceReference{Name: "src-vid"}},
			},
		},
	}
	pipeline := &pipelinesv1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "single-queue-pipeline", Namespace: "default"},
		Spec: pipelinesv1alpha1.PipelineSpec{
			Mode:    pipelinesv1alpha1.PipelineModeBatch,
			Filters: []pipelinesv1alpha1.Filter{{Name: "video-in", Image: "videoin:latest"}},
		},
	}
	bindings := []ResolvedSourceBinding{
		{FilterName: "video-in", Source: &pipelinesv1alpha1.PipelineSource{
			Spec: pipelinesv1alpha1.PipelineSourceSpec{
				Bucket: &pipelinesv1alpha1.BucketSource{Name: "media", Prefix: "clips/"},
			},
		}},
	}
	r := newMSReconciler(t, pi)

	// The queue path will fail fast on the nil Valkey client — that is the
	// expected (and asserted) routing outcome; what must NOT happen is the
	// direct-mode Job appearing.
	_, _ = r.reconcileBatch(context.Background(), pi, pipeline, bindings)

	job := &batchv1.Job{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name + "-job", Namespace: pi.Namespace}, job); err == nil {
		if len(job.Spec.Template.Spec.InitContainers) == 1 {
			for _, e := range job.Spec.Template.Spec.InitContainers[0].Env {
				if e.Name == "S3_OBJECT_KEY" {
					t.Fatalf("single binding without singleObject must not run in direct mode")
				}
			}
		}
	}
}
