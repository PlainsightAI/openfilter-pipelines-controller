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

	m1["mutation-test"] = "mutated"
	if _, ok := m2["mutation-test"]; ok {
		t.Error("m2 was mutated by m1 — maps share underlying reference")
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
