package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
)

// findContainerByName is a local helper used by the multi-source tests to
// look up a container by name. The pre-existing `findContainer` in
// pipelineinstance_scheduling_test.go has a different signature, so we
// keep this scoped to avoid collision.
func findContainerByName(t *testing.T, containers []corev1.Container, name string) corev1.Container {
	t.Helper()
	for _, c := range containers {
		if c.Name == name {
			return c
		}
	}
	names := make([]string, 0, len(containers))
	for _, c := range containers {
		names = append(names, c.Name)
	}
	t.Fatalf("container %q not found; have: %v", name, names)
	return corev1.Container{} // unreachable
}

// rtspURLEnv returns the value of the RTSP_URL env var on a container, or
// empty string if not set. Kept narrow on purpose — the multi-source tests
// only inspect RTSP_URL today.
func rtspURLEnv(envs []corev1.EnvVar) string {
	for _, e := range envs {
		if e.Name == "RTSP_URL" {
			return e.Value
		}
	}
	return ""
}

// hasEnv reports whether `envs` contains any entry with the given name.
func hasEnv(envs []corev1.EnvVar, name string) bool {
	for _, e := range envs {
		if e.Name == name {
			return true
		}
	}
	return false
}

// TestBuildStreamingDeployment_MultiSource_PerContainerRTSP exercises the
// PLAT-1071 multi-source path: a Pipeline with two VideoIn-style filters
// (front_cam, back_cam) plus one downstream filter (webvis). The
// PipelineInstance binds each VideoIn to a distinct PipelineSource via
// NamedSourceRef. The build step must:
//
//   - Inject RTSP_URL into the `front_cam` container, valued at the
//     `front` source's URL, and NOT the `back` source's URL.
//   - Inject RTSP_URL into the `back_cam` container, valued at the
//     `back` source's URL.
//   - Leave the `webvis` container with NO RTSP_URL env var — it's a
//     downstream filter that consumes from siblings via its own
//     `sources: tcp://localhost:...` filter config.
func TestBuildStreamingDeployment_MultiSource_PerContainerRTSP(t *testing.T) {
	r := &PipelineInstanceReconciler{}
	pi := makeMinimalStreamingPipelineInstance()
	pi.Spec.Sources = []pipelinesv1alpha1.NamedSourceRef{
		{FilterName: "front_cam", SourceRef: pipelinesv1alpha1.SourceReference{Name: "front-source"}},
		{FilterName: "back_cam", SourceRef: pipelinesv1alpha1.SourceReference{Name: "back-source"}},
	}

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{Name: "front_cam", Image: "videoin:latest"},
				{Name: "back_cam", Image: "videoin:latest"},
				{Name: "webvis", Image: "webvis:latest"},
			},
		},
	}

	frontSource := &pipelinesv1alpha1.PipelineSource{
		Spec: pipelinesv1alpha1.PipelineSourceSpec{
			RTSP: &pipelinesv1alpha1.RTSPSource{Host: "front.example", Port: 554, Path: "/stream"},
		},
	}
	backSource := &pipelinesv1alpha1.PipelineSource{
		Spec: pipelinesv1alpha1.PipelineSourceSpec{
			RTSP: &pipelinesv1alpha1.RTSPSource{Host: "back.example", Port: 554, Path: "/stream"},
		},
	}
	bindings := []ResolvedSourceBinding{
		{FilterName: "front_cam", Source: frontSource},
		{FilterName: "back_cam", Source: backSource},
	}

	deployment := r.buildStreamingDeployment(context.Background(), pi, pipeline, bindings, "ms-deployment")
	containers := deployment.Spec.Template.Spec.Containers

	frontContainer := findContainerByName(t, containers, "front_cam")
	backContainer := findContainerByName(t, containers, "back_cam")
	webvisContainer := findContainerByName(t, containers, "webvis")

	wantFront := buildRTSPURL(frontSource.Spec.RTSP)
	wantBack := buildRTSPURL(backSource.Spec.RTSP)

	if got := rtspURLEnv(frontContainer.Env); got != wantFront {
		t.Errorf("front_cam RTSP_URL = %q, want %q", got, wantFront)
	}
	if got := rtspURLEnv(backContainer.Env); got != wantBack {
		t.Errorf("back_cam RTSP_URL = %q, want %q", got, wantBack)
	}
	if hasEnv(webvisContainer.Env, "RTSP_URL") {
		t.Errorf("webvis must not receive RTSP_URL — it's a downstream filter; got env: %v", webvisContainer.Env)
	}

	// Defense-in-depth: the front binding must not leak into the back
	// container or vice versa (would surface as a swap bug in the
	// bindingsByFilter lookup).
	if got := rtspURLEnv(frontContainer.Env); got == wantBack {
		t.Errorf("front_cam ended up with back source URL — bindings swapped")
	}
	if got := rtspURLEnv(backContainer.Env); got == wantFront {
		t.Errorf("back_cam ended up with front source URL — bindings swapped")
	}
}

// TestBuildStreamingDeployment_LegacyBroadcast confirms the deprecated
// `Spec.SourceRef` path still broadcasts RTSP_URL to every container,
// matching pre-PLAT-1071 behavior. The resolver upstream produces a
// single ResolvedSourceBinding with FilterName=="" — the sentinel for
// broadcast.
func TestBuildStreamingDeployment_LegacyBroadcast(t *testing.T) {
	r := &PipelineInstanceReconciler{}
	pi := makeMinimalStreamingPipelineInstance()
	// Deprecated field intentionally exercised: this test guards the
	// legacy-CR compatibility path that the SA1019 deprecation comment
	// asks new callers to avoid.
	//nolint:staticcheck // SA1019: legacy SourceRef path is the system under test.
	pi.Spec.SourceRef = &pipelinesv1alpha1.SourceReference{Name: "legacy-source"}

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{Name: "video-in", Image: "videoin:latest"},
				{Name: "webvis", Image: "webvis:latest"},
			},
		},
	}

	source := &pipelinesv1alpha1.PipelineSource{
		Spec: pipelinesv1alpha1.PipelineSourceSpec{
			RTSP: &pipelinesv1alpha1.RTSPSource{Host: "legacy.example", Port: 554, Path: "/stream"},
		},
	}
	// FilterName="" — the legacy-broadcast sentinel the resolver produces
	// when only Spec.SourceRef is set.
	bindings := []ResolvedSourceBinding{{FilterName: "", Source: source}}

	deployment := r.buildStreamingDeployment(context.Background(), pi, pipeline, bindings, "legacy-deployment")
	containers := deployment.Spec.Template.Spec.Containers

	wantURL := buildRTSPURL(source.Spec.RTSP)
	for _, c := range containers {
		got := rtspURLEnv(c.Env)
		if got != wantURL {
			t.Errorf("container %q RTSP_URL = %q, want %q (legacy broadcast must hit every container)", c.Name, got, wantURL)
		}
	}
}

// TestBuildMultiSourceBatchJob_PerBindingInitClaimersAndEnv pins the
// multi-source batch path (PLAT-1071): one init claimer per binding in
// direct mode (S3_OBJECT_KEY set + queue env absent), and each VideoIn
// filter container gets `VIDEO_INPUT_PATH` pointing at its own bound
// download path. Downstream filters get no VIDEO_INPUT_PATH.
func TestBuildMultiSourceBatchJob_PerBindingInitClaimersAndEnv(t *testing.T) {
	r := &PipelineInstanceReconciler{ClaimerImage: "claimer:test"}
	pi := makeMinimalStreamingPipelineInstance() // shape-only; values reused
	pi.Spec.Sources = []pipelinesv1alpha1.NamedSourceRef{
		{FilterName: "front-cam", SourceRef: pipelinesv1alpha1.SourceReference{Name: "front-source"}},
		{FilterName: "back-cam", SourceRef: pipelinesv1alpha1.SourceReference{Name: "back-source"}},
	}

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Mode: pipelinesv1alpha1.PipelineModeBatch,
			Filters: []pipelinesv1alpha1.Filter{
				{Name: "front-cam", Image: "videoin:latest"},
				{Name: "back-cam", Image: "videoin:latest"},
				{Name: "image-out", Image: "imageout:latest"},
			},
		},
	}

	frontSource := &pipelinesv1alpha1.PipelineSource{
		Spec: pipelinesv1alpha1.PipelineSourceSpec{
			Bucket: &pipelinesv1alpha1.BucketSource{
				Name:   "media",
				Object: "front.mp4",
				CredentialsSecret: &pipelinesv1alpha1.SecretReference{
					Name: "s3-creds",
				},
			},
		},
	}
	backSource := &pipelinesv1alpha1.PipelineSource{
		Spec: pipelinesv1alpha1.PipelineSourceSpec{
			Bucket: &pipelinesv1alpha1.BucketSource{
				Name:   "media",
				Object: "back.mp4",
				CredentialsSecret: &pipelinesv1alpha1.SecretReference{
					Name: "s3-creds",
				},
			},
		},
	}
	bindings := []ResolvedSourceBinding{
		{FilterName: "front-cam", Source: frontSource},
		{FilterName: "back-cam", Source: backSource},
	}

	job := r.buildMultiSourceBatchJob(context.Background(), pi, pipeline, bindings, "ms-job")

	initContainers := job.Spec.Template.Spec.InitContainers
	if len(initContainers) != 2 {
		t.Fatalf("expected 2 init claimers (one per binding), got %d", len(initContainers))
	}

	// Each init claimer must be in direct mode and target the correct object/path.
	for _, c := range initContainers {
		objKey := envValue(c.Env, "S3_OBJECT_KEY")
		dest := envValue(c.Env, "VIDEO_INPUT_PATH")
		switch c.Name {
		case "claimer-front-cam":
			if objKey != "front.mp4" {
				t.Errorf("claimer-front-cam S3_OBJECT_KEY = %q, want %q", objKey, "front.mp4")
			}
			if dest != "/ws/front-cam.mp4" {
				t.Errorf("claimer-front-cam VIDEO_INPUT_PATH = %q, want %q", dest, "/ws/front-cam.mp4")
			}
		case "claimer-back-cam":
			if objKey != "back.mp4" {
				t.Errorf("claimer-back-cam S3_OBJECT_KEY = %q, want %q", objKey, "back.mp4")
			}
			if dest != "/ws/back-cam.mp4" {
				t.Errorf("claimer-back-cam VIDEO_INPUT_PATH = %q, want %q", dest, "/ws/back-cam.mp4")
			}
		default:
			t.Errorf("unexpected init container %q", c.Name)
		}
		// Direct mode = no Valkey env.
		if envValue(c.Env, "STREAM") != "" || envValue(c.Env, "GROUP") != "" {
			t.Errorf("claimer %q has Valkey env set, expected direct-mode only", c.Name)
		}
	}

	// VideoIn filter containers must each carry VIDEO_INPUT_PATH for their
	// own download path; the downstream image-out filter must not.
	containers := job.Spec.Template.Spec.Containers
	front := findContainerByName(t, containers, "front-cam")
	back := findContainerByName(t, containers, "back-cam")
	imageOut := findContainerByName(t, containers, "image-out")
	if got := envValue(front.Env, "VIDEO_INPUT_PATH"); got != "/ws/front-cam.mp4" {
		t.Errorf("front-cam VIDEO_INPUT_PATH = %q, want %q", got, "/ws/front-cam.mp4")
	}
	if got := envValue(back.Env, "VIDEO_INPUT_PATH"); got != "/ws/back-cam.mp4" {
		t.Errorf("back-cam VIDEO_INPUT_PATH = %q, want %q", got, "/ws/back-cam.mp4")
	}
	if hasEnv(imageOut.Env, "VIDEO_INPUT_PATH") {
		t.Errorf("image-out must not receive VIDEO_INPUT_PATH; it's a downstream consumer")
	}

	// Job shape: single Pod (parallelism=1, completions=1).
	if job.Spec.Parallelism == nil || *job.Spec.Parallelism != 1 {
		t.Errorf("expected parallelism=1, got %v", job.Spec.Parallelism)
	}
	if job.Spec.Completions == nil || *job.Spec.Completions != 1 {
		t.Errorf("expected completions=1, got %v", job.Spec.Completions)
	}
}

// envValue returns the inline value of the env var named `name`, or "".
func envValue(envs []corev1.EnvVar, name string) string {
	for _, e := range envs {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}

// TestEffectiveSources covers the spec-shape normalization: SourceRef
// becomes a one-entry broadcast list; Sources is returned verbatim; both
// unset returns nil (the reconciler treats this as an error).
func TestEffectiveSources(t *testing.T) {
	t.Run("legacy_source_ref_becomes_broadcast_sentinel", func(t *testing.T) {
		spec := pipelinesv1alpha1.PipelineInstanceSpec{
			//nolint:staticcheck // SA1019: deprecated SourceRef is the system under test.
			SourceRef: &pipelinesv1alpha1.SourceReference{Name: "src1"},
		}
		got := spec.EffectiveSources()
		if len(got) != 1 || got[0].FilterName != "" || got[0].SourceRef.Name != "src1" {
			t.Errorf("legacy SourceRef did not normalize to broadcast sentinel: %+v", got)
		}
	})
	t.Run("sources_returned_verbatim", func(t *testing.T) {
		spec := pipelinesv1alpha1.PipelineInstanceSpec{
			Sources: []pipelinesv1alpha1.NamedSourceRef{
				{FilterName: "a", SourceRef: pipelinesv1alpha1.SourceReference{Name: "src-a"}},
				{FilterName: "b", SourceRef: pipelinesv1alpha1.SourceReference{Name: "src-b"}},
			},
		}
		got := spec.EffectiveSources()
		if len(got) != 2 || got[0].FilterName != "a" || got[1].FilterName != "b" {
			t.Errorf("Sources not returned verbatim: %+v", got)
		}
	})
	t.Run("nothing_set_returns_nil", func(t *testing.T) {
		spec := pipelinesv1alpha1.PipelineInstanceSpec{}
		got := spec.EffectiveSources()
		if got != nil {
			t.Errorf("empty spec must yield nil, got: %+v", got)
		}
	})
}
