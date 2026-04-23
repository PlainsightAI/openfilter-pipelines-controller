/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
)

func TestMergeLabelsFromCR(t *testing.T) {
	base := map[string]string{
		"app":              "pipeline-stream",
		"pipelineinstance": "pi-test",
	}

	tests := []struct {
		name     string
		crLabels map[string]string
		expect   map[string]string
	}{
		{
			name: "CR has all 3 propagated labels",
			crLabels: map[string]string{
				"plainsight.ai/pipeline-instance-id": "550e8400",
				"plainsight.ai/organization-id":      "org-aaa",
				"plainsight.ai/project-id":           "proj-bbb",
			},
			expect: map[string]string{
				"app":                                "pipeline-stream",
				"pipelineinstance":                   "pi-test",
				"plainsight.ai/pipeline-instance-id": "550e8400",
				"plainsight.ai/organization-id":      "org-aaa",
				"plainsight.ai/project-id":           "proj-bbb",
			},
		},
		{
			name:     "CR has no propagated labels",
			crLabels: map[string]string{},
			expect: map[string]string{
				"app":              "pipeline-stream",
				"pipelineinstance": "pi-test",
			},
		},
		{
			name: "CR has extra plainsight.ai/* labels (must NOT propagate)",
			crLabels: map[string]string{
				"plainsight.ai/pipeline-instance-id": "550e8400",
				"plainsight.ai/request-id":           "req-xxx",
				"plainsight.ai/trace-id":             "trace-yyy",
				"plainsight.ai/build-sha":            "abc123",
			},
			expect: map[string]string{
				"app":                                "pipeline-stream",
				"pipelineinstance":                   "pi-test",
				"plainsight.ai/pipeline-instance-id": "550e8400",
			},
		},
		{
			name: "CR has only some propagated labels",
			crLabels: map[string]string{
				"plainsight.ai/organization-id": "org-aaa",
			},
			expect: map[string]string{
				"app":                           "pipeline-stream",
				"pipelineinstance":              "pi-test",
				"plainsight.ai/organization-id": "org-aaa",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pi := &pipelinesv1alpha1.PipelineInstance{
				ObjectMeta: metav1.ObjectMeta{Labels: tt.crLabels},
			}
			got := mergeLabelsFromCR(base, pi)
			if len(got) != len(tt.expect) {
				t.Errorf("len mismatch: got %d (%v), want %d (%v)", len(got), got, len(tt.expect), tt.expect)
			}
			for k, v := range tt.expect {
				if got[k] != v {
					t.Errorf("got[%q] = %q, want %q", k, got[k], v)
				}
			}
		})
	}
}

// TestMergeLabelsFromCR_FreshMap verifies that each call returns a fresh map
// (no shared reference across call sites).
func TestMergeLabelsFromCR_FreshMap(t *testing.T) {
	base := map[string]string{"app": "pipeline-stream"}
	pi := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
			"plainsight.ai/pipeline-instance-id": "550e8400",
		}},
	}

	m1 := mergeLabelsFromCR(base, pi)
	m2 := mergeLabelsFromCR(base, pi)

	m1["mutation-test"] = "test-value"
	if _, ok := m2["mutation-test"]; ok {
		t.Error("m2 was affected by m1 — maps share underlying reference")
	}
}

func TestBuildPodLabels(t *testing.T) {
	pi := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pi-test",
			Labels: map[string]string{
				"plainsight.ai/organization-id": "org-aaa",
				"plainsight.ai/request-id":      "should-not-propagate",
			},
		},
	}

	labels := buildPodLabels("instance-123", pi)

	if labels["filter.plainsight.ai/instance"] != "instance-123" {
		t.Errorf("missing filter.plainsight.ai/instance: got %v", labels)
	}
	if labels["filter.plainsight.ai/pipelineinstance"] != "pi-test" {
		t.Errorf("missing filter.plainsight.ai/pipelineinstance: got %v", labels)
	}
	if labels["plainsight.ai/organization-id"] != "org-aaa" {
		t.Errorf("whitelisted label not propagated: got %v", labels)
	}
	if _, ok := labels["plainsight.ai/request-id"]; ok {
		t.Errorf("non-whitelisted label must NOT propagate: got %v", labels)
	}
}

// TestBuildJob_PropagatesPlainsightLabels is an integration test proving the
// label whitelist flows end-to-end through buildJob into the resulting Job's
// pod template. This guards against someone later skipping buildPodLabels /
// mergeLabelsFromCR in the builder.
func TestBuildJob_PropagatesPlainsightLabels(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	pi.Labels = map[string]string{
		"plainsight.ai/pipeline-instance-id": "pi-550e8400",
		"plainsight.ai/organization-id":      "org-aaa",
		"plainsight.ai/project-id":           "proj-bbb",
		"plainsight.ai/request-id":           "should-NOT-propagate",
	}
	ps := makeMinimalPipelineSource()
	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{{Name: "f", Image: "f:latest"}},
		},
	}

	job := r.buildJob(t.Context(), pi, pipeline, ps, "test-job")

	podLabels := job.Spec.Template.Labels
	for k, want := range map[string]string{
		"plainsight.ai/pipeline-instance-id": "pi-550e8400",
		"plainsight.ai/organization-id":      "org-aaa",
		"plainsight.ai/project-id":           "proj-bbb",
	} {
		if podLabels[k] != want {
			t.Errorf("job pod template missing %s=%s; got %q (all labels: %v)", k, want, podLabels[k], podLabels)
		}
	}
	if _, ok := podLabels["plainsight.ai/request-id"]; ok {
		t.Errorf("non-whitelisted label leaked into pod template labels: %v", podLabels)
	}
}

// TestBuildStreamingDeployment_PropagatesPlainsightLabels is the streaming
// counterpart. Validates both the Deployment's ObjectMeta labels and the pod
// template labels, and confirms the map references are not shared.
func TestBuildStreamingDeployment_PropagatesPlainsightLabels(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	pi.Labels = map[string]string{
		"plainsight.ai/pipeline-instance-id": "pi-550e8400",
		"plainsight.ai/organization-id":      "org-aaa",
		"plainsight.ai/project-id":           "proj-bbb",
		"plainsight.ai/build-sha":            "should-NOT-propagate",
	}
	ps := makeMinimalPipelineSource()
	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{{Name: "f", Image: "f:latest"}},
		},
	}

	dep := r.buildStreamingDeployment(pi, pipeline, ps, "test-deploy")

	for _, tc := range []struct {
		name   string
		labels map[string]string
	}{
		{"deployment", dep.Labels},
		{"pod template", dep.Spec.Template.Labels},
	} {
		for k, want := range map[string]string{
			"plainsight.ai/pipeline-instance-id": "pi-550e8400",
			"plainsight.ai/organization-id":      "org-aaa",
			"plainsight.ai/project-id":           "proj-bbb",
		} {
			if tc.labels[k] != want {
				t.Errorf("%s: missing %s=%s; got %q (all: %v)", tc.name, k, want, tc.labels[k], tc.labels)
			}
		}
		if _, ok := tc.labels["plainsight.ai/build-sha"]; ok {
			t.Errorf("%s: non-whitelisted label leaked: %v", tc.name, tc.labels)
		}
	}

	// Defensive: mutating the pod template labels must not affect the deployment labels.
	// This catches the shared-map-reference bug class.
	dep.Spec.Template.Labels["mutation-test"] = "x"
	if _, ok := dep.Labels["mutation-test"]; ok {
		t.Errorf("deployment labels and pod template labels share the same map")
	}
}
