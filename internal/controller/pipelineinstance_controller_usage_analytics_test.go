package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
)

// doNotTrackEnvVar is the env var openfilter's Scarf SDK checks before it opens
// the connection, so setting it prevents the request rather than discarding the
// response.
const doNotTrackEnvVar = "DO_NOT_TRACK"

// makeUsageAnalyticsPipeline returns a Pipeline with a single CPU filter, so the
// injection paths are exercised without GPU branching in the picture.
func makeUsageAnalyticsPipeline() *pipelinesv1alpha1.Pipeline {
	return &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{
					Name:  "analytics-filter",
					Image: "filter:latest",
				},
			},
		},
	}
}

// assertDoNotTrack checks DO_NOT_TRACK against what the flag should have
// produced. The absent case matters as much as the present one: injecting it
// unconditionally would disable analytics for OSS operators who never asked for
// that, which is the opposite of openfilter's opt-out contract.
func assertDoNotTrack(t *testing.T, env []corev1.EnvVar, want bool) {
	t.Helper()

	got, ok := findEnvVar(env, doNotTrackEnvVar)
	if want {
		if !ok {
			t.Fatalf("expected %s to be injected when DisableUsageAnalytics is true", doNotTrackEnvVar)
		}
		if got.Value != "1" {
			t.Errorf("expected %s=%q, got %q", doNotTrackEnvVar, "1", got.Value)
		}
		return
	}
	if ok {
		t.Errorf("expected %s to be absent by default, got %q", doNotTrackEnvVar, got.Value)
	}
}

// ─── batch (Job) ─────────────────────────────────────────────────────────────

func TestBuildJob_DoNotTrackAbsentByDefault(t *testing.T) {
	r := makeMinimalReconciler()

	job := r.buildJob(context.Background(), makeMinimalPipelineInstance(), makeUsageAnalyticsPipeline(),
		makeMinimalPipelineSource(), "test-job")

	assertDoNotTrack(t, job.Spec.Template.Spec.Containers[0].Env, false)
}

func TestBuildJob_DoNotTrackInjectedWhenDisabled(t *testing.T) {
	r := makeMinimalReconciler()
	r.DisableUsageAnalytics = true

	job := r.buildJob(context.Background(), makeMinimalPipelineInstance(), makeUsageAnalyticsPipeline(),
		makeMinimalPipelineSource(), "test-job")

	assertDoNotTrack(t, job.Spec.Template.Spec.Containers[0].Env, true)
}

// ─── streaming (Deployment) ──────────────────────────────────────────────────

func TestBuildStreamingDeployment_DoNotTrackAbsentByDefault(t *testing.T) {
	r := makeMinimalReconciler()

	deployment := r.buildStreamingDeployment(context.Background(), makeMinimalPipelineInstance(),
		makeUsageAnalyticsPipeline(), nil, "test-deployment")

	assertDoNotTrack(t, deployment.Spec.Template.Spec.Containers[0].Env, false)
}

func TestBuildStreamingDeployment_DoNotTrackInjectedWhenDisabled(t *testing.T) {
	r := makeMinimalReconciler()
	r.DisableUsageAnalytics = true

	deployment := r.buildStreamingDeployment(context.Background(), makeMinimalPipelineInstance(),
		makeUsageAnalyticsPipeline(), nil, "test-deployment")

	assertDoNotTrack(t, deployment.Spec.Template.Spec.Containers[0].Env, true)
}

// ─── multi-source batch (Job) ────────────────────────────────────────────────
//
// This third path is the reason the injection lives in tracingEnvVars rather
// than in each builder: it is the one the existing tracing tests do not cover,
// so a per-builder implementation could ship with this path silently missing.

func TestBuildMultiSourceBatchJob_DoNotTrackAbsentByDefault(t *testing.T) {
	r := makeMinimalReconciler()

	job := r.buildMultiSourceBatchJob(context.Background(), makeMinimalPipelineInstance(),
		makeUsageAnalyticsPipeline(), nil, "ms-job")

	assertDoNotTrack(t, job.Spec.Template.Spec.Containers[0].Env, false)
}

func TestBuildMultiSourceBatchJob_DoNotTrackInjectedWhenDisabled(t *testing.T) {
	r := makeMinimalReconciler()
	r.DisableUsageAnalytics = true

	job := r.buildMultiSourceBatchJob(context.Background(), makeMinimalPipelineInstance(),
		makeUsageAnalyticsPipeline(), nil, "ms-job")

	assertDoNotTrack(t, job.Spec.Template.Spec.Containers[0].Env, true)
}
