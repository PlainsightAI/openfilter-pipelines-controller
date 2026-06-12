package controller

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
)

// frontCredSecretName is the secret-name fixture used by the
// credentials-injection test below. Constant to satisfy goconst.
const frontCredSecretName = "front-rtsp-creds"

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
// (front-cam, back-cam) plus one downstream filter (webvis). The
// PipelineInstance binds each VideoIn to a distinct PipelineSource via
// NamedSourceRef. The build step must:
//
//   - Inject RTSP_URL into the `front-cam` container, valued at the
//     `front` source's URL, and NOT the `back` source's URL.
//   - Inject RTSP_URL into the `back-cam` container, valued at the
//     `back` source's URL.
//   - Leave the `webvis` container with NO RTSP_URL env var — it's a
//     downstream filter that consumes from siblings via its own
//     `sources: tcp://localhost:...` filter config.
func TestBuildStreamingDeployment_MultiSource_PerContainerRTSP(t *testing.T) {
	r := &PipelineInstanceReconciler{}
	pi := makeMinimalStreamingPipelineInstance()
	pi.Spec.Sources = []pipelinesv1alpha1.NamedSourceRef{
		{FilterName: "front-cam", SourceRef: pipelinesv1alpha1.SourceReference{Name: "front-source"}},
		{FilterName: "back-cam", SourceRef: pipelinesv1alpha1.SourceReference{Name: "back-source"}},
	}

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{Name: "front-cam", Image: "videoin:latest"},
				{Name: "back-cam", Image: "videoin:latest"},
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
		{FilterName: "front-cam", Source: frontSource},
		{FilterName: "back-cam", Source: backSource},
	}

	deployment := r.buildStreamingDeployment(context.Background(), pi, pipeline, bindings, "ms-deployment")
	containers := deployment.Spec.Template.Spec.Containers

	frontContainer := findContainerByName(t, containers, "front-cam")
	backContainer := findContainerByName(t, containers, "back-cam")
	webvisContainer := findContainerByName(t, containers, "webvis")

	wantFront := buildRTSPURL(frontSource.Spec.RTSP)
	wantBack := buildRTSPURL(backSource.Spec.RTSP)

	if got := rtspURLEnv(frontContainer.Env); got != wantFront {
		t.Errorf("front-cam RTSP_URL = %q, want %q", got, wantFront)
	}
	if got := rtspURLEnv(backContainer.Env); got != wantBack {
		t.Errorf("back-cam RTSP_URL = %q, want %q", got, wantBack)
	}
	if hasEnv(webvisContainer.Env, "RTSP_URL") {
		t.Errorf("webvis must not receive RTSP_URL — it's a downstream filter; got env: %v", webvisContainer.Env)
	}

	// Defense-in-depth: the front binding must not leak into the back
	// container or vice versa (would surface as a swap bug in the
	// bindingsByFilter lookup).
	if got := rtspURLEnv(frontContainer.Env); got == wantBack {
		t.Errorf("front-cam ended up with back source URL — bindings swapped")
	}
	if got := rtspURLEnv(backContainer.Env); got == wantFront {
		t.Errorf("back-cam ended up with front source URL — bindings swapped")
	}
}

// TestBuildStreamingDeployment_LegacyBroadcast confirms the singular
// `Spec.SourceRef` form still broadcasts RTSP_URL to every container,
// matching pre-PLAT-1071 behavior. The resolver upstream produces a
// single ResolvedSourceBinding with FilterName=="" — the sentinel for
// broadcast.
func TestBuildStreamingDeployment_LegacyBroadcast(t *testing.T) {
	r := &PipelineInstanceReconciler{}
	pi := makeMinimalStreamingPipelineInstance()
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
				Prefix: "front.mp4",
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
				Prefix: "back.mp4",
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
	// ORDERING contract: VIDEO_INPUT_PATH must precede every FILTER_* env
	// entry. Kubernetes dependent-env expansion only resolves $(VAR)
	// references to variables defined earlier in the list, so a
	// config-first ordering turns `sources: file://$(VIDEO_INPUT_PATH)`
	// into a literal unexpanded string inside the container.
	for _, c := range []corev1.Container{front, back} {
		assertEnvPrecedesFilterConfig(t, c, "VIDEO_INPUT_PATH")
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

// findSecretEnvRef returns the SecretKeySelector for the env var with the
// given name, or nil. Used by the credentials-injection test below to
// assert on ValueFrom-style env vars without dragging full struct
// comparisons into the call sites.
func findSecretEnvRef(envs []corev1.EnvVar, name string) *corev1.SecretKeySelector {
	for _, e := range envs {
		if e.Name == name && e.ValueFrom != nil {
			return e.ValueFrom.SecretKeyRef
		}
	}
	return nil
}

// TestBuildStreamingDeployment_MultiSource_WithCredentials covers the
// secretRef path: when a per-binding RTSP source has CredentialsSecret
// set the controller must inject _RTSP_USERNAME / _RTSP_PASSWORD env
// vars sourced from that secret into the matching VideoIn container
// alongside RTSP_URL. The pre-existing PerContainerRTSP test covers the
// no-credentials case; this fills the credentials code path the
// multi-source change touches.
func TestBuildStreamingDeployment_MultiSource_WithCredentials(t *testing.T) {
	r := &PipelineInstanceReconciler{}
	pi := makeMinimalStreamingPipelineInstance()
	pi.Spec.Sources = []pipelinesv1alpha1.NamedSourceRef{
		{FilterName: "front-cam", SourceRef: pipelinesv1alpha1.SourceReference{Name: "front-source"}},
		{FilterName: "back-cam", SourceRef: pipelinesv1alpha1.SourceReference{Name: "back-source"}},
	}

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{Name: "front-cam", Image: "videoin:latest"},
				{Name: "back-cam", Image: "videoin:latest"},
			},
		},
	}

	bindings := []ResolvedSourceBinding{
		{FilterName: "front-cam", Source: &pipelinesv1alpha1.PipelineSource{
			Spec: pipelinesv1alpha1.PipelineSourceSpec{
				RTSP: &pipelinesv1alpha1.RTSPSource{
					Host:              "front.example",
					Port:              554,
					Path:              "/stream",
					CredentialsSecret: &pipelinesv1alpha1.SecretReference{Name: frontCredSecretName},
				},
			},
		}},
		{FilterName: "back-cam", Source: &pipelinesv1alpha1.PipelineSource{
			Spec: pipelinesv1alpha1.PipelineSourceSpec{
				RTSP: &pipelinesv1alpha1.RTSPSource{
					Host: "back.example",
					Port: 554,
					Path: "/stream",
					// No credentials — verifies one-with, one-without works.
				},
			},
		}},
	}

	deployment := r.buildStreamingDeployment(context.Background(), pi, pipeline, bindings, "ms-deployment")
	containers := deployment.Spec.Template.Spec.Containers
	front := findContainerByName(t, containers, "front-cam")
	back := findContainerByName(t, containers, "back-cam")

	usernameRef := findSecretEnvRef(front.Env, "_RTSP_USERNAME")
	passwordRef := findSecretEnvRef(front.Env, "_RTSP_PASSWORD")
	if usernameRef == nil || usernameRef.Name != frontCredSecretName || usernameRef.Key != "username" {
		t.Errorf("front-cam _RTSP_USERNAME secretKeyRef = %+v, want %s/username", usernameRef, frontCredSecretName)
	}
	if passwordRef == nil || passwordRef.Name != frontCredSecretName || passwordRef.Key != "password" {
		t.Errorf("front-cam _RTSP_PASSWORD secretKeyRef = %+v, want %s/password", passwordRef, frontCredSecretName)
	}
	if got := rtspURLEnv(front.Env); got == "" {
		t.Errorf("front-cam RTSP_URL must still be injected alongside credentials, got empty")
	}

	if findSecretEnvRef(back.Env, "_RTSP_USERNAME") != nil {
		t.Errorf("back-cam must not have _RTSP_USERNAME (no credentials configured)")
	}
	if findSecretEnvRef(back.Env, "_RTSP_PASSWORD") != nil {
		t.Errorf("back-cam must not have _RTSP_PASSWORD (no credentials configured)")
	}
	for _, e := range back.Env {
		if e.ValueFrom != nil && e.ValueFrom.SecretKeyRef != nil && e.ValueFrom.SecretKeyRef.Name == frontCredSecretName {
			t.Errorf("front-cam's secret leaked into back-cam container env: %+v", e)
		}
	}
}

// TestResolveSourceBindings_SourceMissingSurfacesActionableError covers
// the error path of resolveSourceBindings: when the referenced
// PipelineSource doesn't exist in the cluster the function returns a
// wrapped error naming the binding's FilterName so operators can locate
// the misconfiguration in the CR.
func TestResolveSourceBindings_SourceMissingSurfacesActionableError(t *testing.T) {
	sch := reconcileSpanScheme(t)
	pi := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pi", Namespace: "default"},
		Spec: pipelinesv1alpha1.PipelineInstanceSpec{
			Sources: []pipelinesv1alpha1.NamedSourceRef{
				{FilterName: "front-cam", SourceRef: pipelinesv1alpha1.SourceReference{Name: "src-front"}},
				{FilterName: "back-cam", SourceRef: pipelinesv1alpha1.SourceReference{Name: "src-back-missing"}},
			},
		},
	}
	// Only front exists; back is missing on purpose.
	frontSrc := &pipelinesv1alpha1.PipelineSource{
		ObjectMeta: metav1.ObjectMeta{Name: "src-front", Namespace: "default"},
		Spec: pipelinesv1alpha1.PipelineSourceSpec{
			RTSP: &pipelinesv1alpha1.RTSPSource{Host: "front"},
		},
	}
	r := &PipelineInstanceReconciler{
		Client: fake.NewClientBuilder().WithScheme(sch).WithObjects(pi, frontSrc).Build(),
		Scheme: sch,
	}

	_, err := r.resolveSourceBindings(context.Background(), pi)
	if err == nil {
		t.Fatalf("expected error when a referenced source is missing, got nil")
	}
	if !strings.Contains(err.Error(), "src-back-missing") {
		t.Errorf("error must name the missing source, got: %v", err)
	}
	if !strings.Contains(err.Error(), "back-cam") {
		t.Errorf("error must name the filter binding, got: %v", err)
	}
}

// TestResolveSourceBindings_LegacySourceRefMissingSurfacesError covers
// the same error path for the legacy SourceRef shape (FilterName==""). In
// the legacy path the error wording omits the filter-name suffix since
// the binding isn't filter-scoped.
func TestResolveSourceBindings_LegacySourceRefMissingSurfacesError(t *testing.T) {
	sch := reconcileSpanScheme(t)
	pi := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pi", Namespace: "default"},
		Spec: pipelinesv1alpha1.PipelineInstanceSpec{
			SourceRef: &pipelinesv1alpha1.SourceReference{Name: "src-missing"},
		},
	}
	r := &PipelineInstanceReconciler{
		Client: fake.NewClientBuilder().WithScheme(sch).WithObjects(pi).Build(),
		Scheme: sch,
	}

	_, err := r.resolveSourceBindings(context.Background(), pi)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "src-missing") {
		t.Errorf("error must name the missing source, got: %v", err)
	}
	if strings.Contains(err.Error(), "bound to filter") {
		t.Errorf("legacy SourceRef error should not mention filter binding (FilterName is empty), got: %v", err)
	}
}

// assertEnvPrecedesFilterConfig fails the test unless env var `name` appears
// in c.Env BEFORE every FILTER_*-prefixed entry — the position Kubernetes
// dependent-env expansion requires for $(name) references inside filter
// config values to resolve.
func assertEnvPrecedesFilterConfig(t *testing.T, c corev1.Container, name string) {
	t.Helper()
	nameIdx := -1
	firstFilterIdx := -1
	for i, e := range c.Env {
		if e.Name == name && nameIdx == -1 {
			nameIdx = i
		}
		if strings.HasPrefix(e.Name, "FILTER_") && firstFilterIdx == -1 {
			firstFilterIdx = i
		}
	}
	if nameIdx == -1 {
		t.Fatalf("container %q: env %s not found", c.Name, name)
	}
	if firstFilterIdx != -1 && nameIdx > firstFilterIdx {
		t.Errorf("container %q: %s at index %d must precede first FILTER_* env at index %d (K8s $(VAR) expansion only resolves earlier-defined vars)", c.Name, name, nameIdx, firstFilterIdx)
	}
}

// TestPipelineInstancesForPipelineSource_MapsReferencingInstances pins the
// PipelineSource→PipelineInstance watch mapping wired in SetupWithManager: a
// PipelineSource event must enqueue every PipelineInstance in the source's
// namespace that references it — via spec.sources[].sourceRef.name OR the
// legacy spec.sourceRef.name — and nothing else. Without this mapping, an
// instance Degraded by multi-source batch validation (which returns without
// requeue) would never reconcile again after the source is fixed.
func TestPipelineInstancesForPipelineSource_MapsReferencingInstances(t *testing.T) {
	sch := reconcileSpanScheme(t)
	otherNS := "elsewhere"

	src := &pipelinesv1alpha1.PipelineSource{
		ObjectMeta: metav1.ObjectMeta{Name: "shared-src", Namespace: "default"},
	}

	piMulti := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "pi-multi", Namespace: "default"},
		Spec: pipelinesv1alpha1.PipelineInstanceSpec{
			PipelineRef: pipelinesv1alpha1.PipelineReference{Name: "p"},
			Sources: []pipelinesv1alpha1.NamedSourceRef{
				{FilterName: "front-cam", SourceRef: pipelinesv1alpha1.SourceReference{Name: "shared-src"}},
				{FilterName: "back-cam", SourceRef: pipelinesv1alpha1.SourceReference{Name: "some-other-src"}},
			},
		},
	}
	piLegacy := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "pi-legacy", Namespace: "default"},
		Spec: pipelinesv1alpha1.PipelineInstanceSpec{
			PipelineRef: pipelinesv1alpha1.PipelineReference{Name: "p"},
			SourceRef:   &pipelinesv1alpha1.SourceReference{Name: "shared-src"},
		},
	}
	piUnrelated := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "pi-unrelated", Namespace: "default"},
		Spec: pipelinesv1alpha1.PipelineInstanceSpec{
			PipelineRef: pipelinesv1alpha1.PipelineReference{Name: "p"},
			SourceRef:   &pipelinesv1alpha1.SourceReference{Name: "different-src"},
		},
	}
	// Same name but the ref explicitly points at another namespace — the
	// event for default/shared-src must NOT wake this instance.
	piCrossNSAway := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "pi-cross-ns-away", Namespace: "default"},
		Spec: pipelinesv1alpha1.PipelineInstanceSpec{
			PipelineRef: pipelinesv1alpha1.PipelineReference{Name: "p"},
			Sources: []pipelinesv1alpha1.NamedSourceRef{
				{FilterName: "front-cam", SourceRef: pipelinesv1alpha1.SourceReference{Name: "shared-src", Namespace: &otherNS}},
			},
		},
	}
	// The inverse: an instance in ANOTHER namespace whose ref explicitly
	// points at default/shared-src MUST wake on the event — the list is
	// cluster-wide and the match resolves the ref's namespace.
	defaultNS := "default"
	piCrossNSToward := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "pi-cross-ns-toward", Namespace: otherNS},
		Spec: pipelinesv1alpha1.PipelineInstanceSpec{
			PipelineRef: pipelinesv1alpha1.PipelineReference{Name: "p"},
			Sources: []pipelinesv1alpha1.NamedSourceRef{
				{FilterName: "front-cam", SourceRef: pipelinesv1alpha1.SourceReference{Name: "shared-src", Namespace: &defaultNS}},
			},
		},
	}
	// Same-name source in the other namespace must not be confused with
	// default/shared-src: an instance referencing it locally stays asleep.
	piOtherNSLocal := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{Name: "pi-other-ns-local", Namespace: otherNS},
		Spec: pipelinesv1alpha1.PipelineInstanceSpec{
			PipelineRef: pipelinesv1alpha1.PipelineReference{Name: "p"},
			SourceRef:   &pipelinesv1alpha1.SourceReference{Name: "shared-src"},
		},
	}

	r := &PipelineInstanceReconciler{
		Client: fake.NewClientBuilder().WithScheme(sch).
			WithObjects(src, piMulti, piLegacy, piUnrelated, piCrossNSAway, piCrossNSToward, piOtherNSLocal).Build(),
		Scheme: sch,
	}

	requests := r.pipelineInstancesForPipelineSource(context.Background(), src)
	got := make(map[string]bool, len(requests))
	for _, req := range requests {
		got[req.Name] = true
	}
	if len(requests) != 3 || !got["pi-multi"] || !got["pi-legacy"] || !got["pi-cross-ns-toward"] {
		t.Errorf("expected exactly [pi-multi pi-legacy pi-cross-ns-toward], got %v", requests)
	}
}

// makeUnmatchedBindingFixtures builds the Pipeline / PipelineSource /
// PipelineInstance triple the SourceBindingUnmatched Reconcile tests share.
// The instance pre-seeds the Valkey-credentials finalizer so the full
// Reconcile entry point flows straight through to source-binding validation
// instead of returning early to persist the finalizer.
func makeUnmatchedBindingFixtures(t *testing.T, mode pipelinesv1alpha1.PipelineMode, filterName string) (*pipelinesv1alpha1.Pipeline, *pipelinesv1alpha1.PipelineSource, *pipelinesv1alpha1.PipelineInstance) {
	t.Helper()
	pipeline := &pipelinesv1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "unmatched-pipeline", Namespace: "default"},
		Spec: pipelinesv1alpha1.PipelineSpec{
			Mode: mode,
			Filters: []pipelinesv1alpha1.Filter{
				{Name: "front-cam", Image: "videoin:latest"},
				{Name: "webvis", Image: "webvis:latest"},
			},
		},
	}
	src := &pipelinesv1alpha1.PipelineSource{
		ObjectMeta: metav1.ObjectMeta{Name: "unmatched-src", Namespace: "default"},
		Spec: pipelinesv1alpha1.PipelineSourceSpec{
			RTSP:   &pipelinesv1alpha1.RTSPSource{Host: "cam.example"},
			Bucket: nil,
		},
	}
	if mode == pipelinesv1alpha1.PipelineModeBatch {
		src.Spec.RTSP = nil
		src.Spec.Bucket = &pipelinesv1alpha1.BucketSource{Name: "media", Prefix: "clip.mp4"}
	}
	pi := &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "unmatched-pi",
			Namespace:  "default",
			UID:        types.UID("unmatched-pi-uid"),
			Finalizers: []string{FinalizerValkeyCredentials},
		},
		Spec: pipelinesv1alpha1.PipelineInstanceSpec{
			PipelineRef: pipelinesv1alpha1.PipelineReference{Name: pipeline.Name},
			Sources: []pipelinesv1alpha1.NamedSourceRef{
				{FilterName: filterName, SourceRef: pipelinesv1alpha1.SourceReference{Name: src.Name}},
			},
		},
	}
	return pipeline, src, pi
}

// TestReconcile_UnmatchedSourceBinding_DegradesStreaming pins the
// streaming-mode half of the unmatched-binding validation: a
// sources[].filterName that matches no pipeline.spec.filters[].name must
// surface Degraded=True (SourceBindingUnmatched) with a message naming both
// the unmatched and the available filter names, and no Deployment may be
// created (the binding would otherwise be a silent no-op — the RTSP env
// would land on no container).
func TestReconcile_UnmatchedSourceBinding_DegradesStreaming(t *testing.T) {
	sch := reconcileSpanScheme(t)
	pipeline, src, pi := makeUnmatchedBindingFixtures(t, pipelinesv1alpha1.PipelineModeStream, "ghost-cam")
	r := &PipelineInstanceReconciler{
		Client: fake.NewClientBuilder().WithScheme(sch).
			WithStatusSubresource(&pipelinesv1alpha1.PipelineInstance{}).
			WithObjects(pipeline, src, pi).Build(),
		Scheme: sch,
	}

	if _, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: pi.Name, Namespace: pi.Namespace},
	}); err != nil {
		t.Fatalf("expected nil error (validation failure surfaces via Condition), got %v", err)
	}

	updated := &pipelinesv1alpha1.PipelineInstance{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name, Namespace: pi.Namespace}, updated); err != nil {
		t.Fatalf("re-fetch PI: %v", err)
	}
	cond := findCondition(t, updated.Status.Conditions, ConditionTypeDegraded)
	if cond.Status != metav1.ConditionTrue || cond.Reason != ReasonSourceBindingUnmatched {
		t.Fatalf("expected Degraded=True (%s), got %+v", ReasonSourceBindingUnmatched, cond)
	}
	if prog := findCondition(t, updated.Status.Conditions, ConditionTypeProgressing); prog.Status == metav1.ConditionTrue {
		t.Errorf("expected Progressing!=True alongside the validation Degraded, got %+v", prog)
	}
	if !strings.Contains(cond.Message, "ghost-cam") {
		t.Errorf("message must name the unmatched binding, got %q", cond.Message)
	}
	if !strings.Contains(cond.Message, "front-cam") || !strings.Contains(cond.Message, "webvis") {
		t.Errorf("message must list the available filter names, got %q", cond.Message)
	}

	deploy := &appsv1.Deployment{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name + "-deploy", Namespace: pi.Namespace}, deploy); err == nil {
		t.Errorf("expected no Deployment to be created after validation failure")
	}
}

// TestReconcile_UnmatchedSourceBinding_DegradesBatch pins the batch-mode
// half: the same validation fires before reconcileBatch ever runs (shared
// path), so no Job is created and no Valkey access happens.
func TestReconcile_UnmatchedSourceBinding_DegradesBatch(t *testing.T) {
	sch := reconcileSpanScheme(t)
	pipeline, src, pi := makeUnmatchedBindingFixtures(t, pipelinesv1alpha1.PipelineModeBatch, "ghost-cam")
	r := &PipelineInstanceReconciler{
		Client: fake.NewClientBuilder().WithScheme(sch).
			WithStatusSubresource(&pipelinesv1alpha1.PipelineInstance{}).
			WithObjects(pipeline, src, pi).Build(),
		Scheme: sch,
		// ValkeyClient intentionally nil: validation must reject before any
		// batch-mode queue work is attempted.
	}

	if _, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: pi.Name, Namespace: pi.Namespace},
	}); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	updated := &pipelinesv1alpha1.PipelineInstance{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name, Namespace: pi.Namespace}, updated); err != nil {
		t.Fatalf("re-fetch PI: %v", err)
	}
	cond := findCondition(t, updated.Status.Conditions, ConditionTypeDegraded)
	if cond.Status != metav1.ConditionTrue || cond.Reason != ReasonSourceBindingUnmatched {
		t.Fatalf("expected Degraded=True (%s), got %+v", ReasonSourceBindingUnmatched, cond)
	}

	job := &batchv1.Job{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name + "-job", Namespace: pi.Namespace}, job); err == nil {
		t.Errorf("expected no Job to be created after validation failure")
	}
}

// TestReconcile_UnmatchedSourceBinding_ClearsOnFix pins the recovery half of
// the shared-path validation: an instance previously Degraded with
// SourceBindingUnmatched must have the condition cleared on the next
// reconcile once its bindings validate (e.g. after the Pipeline gained the
// missing filter or the CR's filterName was corrected — both events
// re-trigger reconcile via the Pipeline watch / spec generation bump).
func TestReconcile_UnmatchedSourceBinding_ClearsOnFix(t *testing.T) {
	sch := reconcileSpanScheme(t)
	// Binding targets front-cam, which the Pipeline HAS — validation passes.
	pipeline, src, pi := makeUnmatchedBindingFixtures(t, pipelinesv1alpha1.PipelineModeStream, "front-cam")
	// Pre-seed the stale Degraded condition a previous failing pass left.
	pi.Status.Conditions = []metav1.Condition{{
		Type:               ConditionTypeDegraded,
		Status:             metav1.ConditionTrue,
		Reason:             ReasonSourceBindingUnmatched,
		Message:            "stale",
		LastTransitionTime: metav1.Now(),
	}}
	r := &PipelineInstanceReconciler{
		Client: fake.NewClientBuilder().WithScheme(sch).
			WithStatusSubresource(&pipelinesv1alpha1.PipelineInstance{}).
			WithObjects(pipeline, src, pi).Build(),
		Scheme: sch,
	}

	if _, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: pi.Name, Namespace: pi.Namespace},
	}); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	updated := &pipelinesv1alpha1.PipelineInstance{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name, Namespace: pi.Namespace}, updated); err != nil {
		t.Fatalf("re-fetch PI: %v", err)
	}
	if cond := findCondition(t, updated.Status.Conditions, ConditionTypeDegraded); cond.Type != "" {
		t.Errorf("expected stale Degraded/%s condition to be cleared once bindings validate, got %+v", ReasonSourceBindingUnmatched, cond)
	}
}

// TestReconcile_PipelineSourceNotFound_ClearsOnceSourceExists pins the
// creation-race recovery the deployment agent depends on: the agent applies
// the PipelineInstance BEFORE its PipelineSources (intentional — cleanup
// treats a source as orphaned when no PI references it), so the first
// reconcile finds no sources and sets Degraded/PipelineSourceNotFound. Once
// the sources land, the PipelineSource watch re-reconciles and the stale
// condition MUST clear — the agent's provisioning monitor tears the whole
// instance down on any lingering Degraded (observed live in the cross-PR
// smoke test before this clear existed).
func TestReconcile_PipelineSourceNotFound_ClearsOnceSourceExists(t *testing.T) {
	sch := reconcileSpanScheme(t)
	pipeline, src, pi := makeUnmatchedBindingFixtures(t, pipelinesv1alpha1.PipelineModeStream, "front-cam")
	// Phase 1: PI exists, its PipelineSource does NOT (agent apply order).
	r := &PipelineInstanceReconciler{
		Client: fake.NewClientBuilder().WithScheme(sch).
			WithStatusSubresource(&pipelinesv1alpha1.PipelineInstance{}).
			WithObjects(pipeline, pi).Build(),
		Scheme: sch,
	}

	if _, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: pi.Name, Namespace: pi.Namespace},
	}); err == nil {
		t.Fatalf("expected error while the PipelineSource is missing, got nil")
	}

	updated := &pipelinesv1alpha1.PipelineInstance{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name, Namespace: pi.Namespace}, updated); err != nil {
		t.Fatalf("re-fetch PI: %v", err)
	}
	cond := findCondition(t, updated.Status.Conditions, ConditionTypeDegraded)
	if cond.Status != metav1.ConditionTrue || cond.Reason != ReasonPipelineSourceNotFound {
		t.Fatalf("expected Degraded=True (%s) while source is missing, got %+v", ReasonPipelineSourceNotFound, cond)
	}

	// Phase 2: the source lands (what the agent does milliseconds later).
	if err := r.Create(context.Background(), src); err != nil {
		t.Fatalf("create PipelineSource: %v", err)
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: pi.Name, Namespace: pi.Namespace},
	}); err != nil {
		t.Fatalf("expected nil error once the source exists, got %v", err)
	}

	updated = &pipelinesv1alpha1.PipelineInstance{}
	if err := r.Get(context.Background(), types.NamespacedName{Name: pi.Name, Namespace: pi.Namespace}, updated); err != nil {
		t.Fatalf("re-fetch PI: %v", err)
	}
	if cond := findCondition(t, updated.Status.Conditions, ConditionTypeDegraded); cond.Type != "" {
		t.Errorf("expected stale Degraded/%s to clear once the PipelineSource exists, got %+v", ReasonPipelineSourceNotFound, cond)
	}
}

// TestBindingObjectKey pins the object-key resolution the direct-mode
// claimers depend on: Bucket.Prefix is the full object key (the platform's
// media model carries single-file object keys in prefix — there is no
// separate object concept anywhere in the train); nil/empty resolves empty
// and the reconciler Degrades.
func TestBindingObjectKey(t *testing.T) {
	mk := func(prefix string) ResolvedSourceBinding {
		return ResolvedSourceBinding{
			FilterName: "front-cam",
			Source: &pipelinesv1alpha1.PipelineSource{
				Spec: pipelinesv1alpha1.PipelineSourceSpec{
					Bucket: &pipelinesv1alpha1.BucketSource{
						Name:   "b",
						Prefix: prefix,
					},
				},
			},
		}
	}
	if got := bindingObjectKey(mk("clips/front_001.mp4")); got != "clips/front_001.mp4" {
		t.Errorf("Prefix must serve as the full key, got %q", got)
	}
	if got := bindingObjectKey(mk("")); got != "" {
		t.Errorf("empty prefix must resolve empty, got %q", got)
	}
	if got := bindingObjectKey(ResolvedSourceBinding{FilterName: "x"}); got != "" {
		t.Errorf("nil source must resolve empty, got %q", got)
	}
}

// TestIsDirectoryPlaceholder pins the bucket-listing filter: zero-byte
// keys ending in "/" (GCS/S3 console "folders") are skipped; real files —
// including zero-byte regular files and oddly-named keys — are kept.
func TestIsDirectoryPlaceholder(t *testing.T) {
	cases := []struct {
		key  string
		size int64
		want bool
	}{
		{"videos/", 0, true},
		{"videos/nested/", 0, true},
		{"videos/train.mp4", 10821688, false},
		{"videos/empty.mp4", 0, false},
		{"videos/", 5, false}, // non-empty trailing-slash key: keep, let processing decide
		{"", 0, false},
	}
	for _, tc := range cases {
		if got := isDirectoryPlaceholder(tc.key, tc.size); got != tc.want {
			t.Errorf("isDirectoryPlaceholder(%q, %d) = %v, want %v", tc.key, tc.size, got, tc.want)
		}
	}
}

// TestIdleAnchorBinding pins the deterministic idle-timeout anchor: the
// lexicographically-smallest filterName wins regardless of slice order
// (sources is listType=map — element order is not semantically stable).
func TestIdleAnchorBinding(t *testing.T) {
	a := ResolvedSourceBinding{FilterName: "a-cam"}
	m := ResolvedSourceBinding{FilterName: "m-cam"}
	z := ResolvedSourceBinding{FilterName: "z-cam"}

	for name, order := range map[string][]ResolvedSourceBinding{
		"sorted":   {a, m, z},
		"reversed": {z, m, a},
		"shuffled": {m, z, a},
	} {
		t.Run(name, func(t *testing.T) {
			if got := idleAnchorBinding(order); got.FilterName != "a-cam" {
				t.Errorf("anchor = %q, want a-cam", got.FilterName)
			}
		})
	}

	// Legacy broadcast sentinel: single binding anchors itself.
	if got := idleAnchorBinding([]ResolvedSourceBinding{{FilterName: ""}}); got.FilterName != "" {
		t.Errorf("broadcast sentinel must anchor itself")
	}
}
