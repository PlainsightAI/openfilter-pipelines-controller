package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	k8sversion "k8s.io/apimachinery/pkg/version"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
)

const (
	testModelImage      = "registry.example.com/model@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	workspaceVolumeName = "workspace"
)

func imageVolumePipeline(filters ...pipelinesv1alpha1.Filter) *pipelinesv1alpha1.Pipeline {
	return &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{Filters: filters},
	}
}

func findVolume(volumes []corev1.Volume, name string) *corev1.Volume {
	for i := range volumes {
		if volumes[i].Name == name {
			return &volumes[i]
		}
	}
	return nil
}

func findMount(container *corev1.Container, mountPath string) *corev1.VolumeMount {
	for i := range container.VolumeMounts {
		if container.VolumeMounts[i].MountPath == mountPath {
			return &container.VolumeMounts[i]
		}
	}
	return nil
}

func podContainer(t *testing.T, podSpec corev1.PodSpec, name string) *corev1.Container {
	t.Helper()
	c := containerByName(&podSpec, name)
	if c == nil {
		t.Fatalf("container %q not found in pod spec", name)
	}
	return c
}

func TestBuildJob_NoImageVolumes_Unchanged(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()
	pipeline := imageVolumePipeline(pipelinesv1alpha1.Filter{Name: "plain", Image: "plain:v1"})
	pipeline.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "existing"}}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")

	podSpec := job.Spec.Template.Spec
	if len(podSpec.Volumes) != 1 || podSpec.Volumes[0].Name != workspaceVolumeName {
		t.Fatalf("expected only the workspace volume, got %v", podSpec.Volumes)
	}
	if len(podSpec.ImagePullSecrets) != 1 || podSpec.ImagePullSecrets[0].Name != "existing" {
		t.Fatalf("expected untouched imagePullSecrets, got %v", podSpec.ImagePullSecrets)
	}
	if got := len(podContainer(t, podSpec, "plain").VolumeMounts); got != 1 {
		t.Fatalf("expected only the workspace mount on the filter container, got %d mounts", got)
	}
}

func TestBuildJob_ImageVolumes_SharedAcrossFilters(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()
	vol := pipelinesv1alpha1.FilterImageVolume{Name: "model", Image: testModelImage, MountPath: "/opt/model"}
	pipeline := imageVolumePipeline(
		pipelinesv1alpha1.Filter{Name: "detector", Image: "detector:v1", ImageVolumes: []pipelinesv1alpha1.FilterImageVolume{vol}},
		pipelinesv1alpha1.Filter{Name: "classifier", Image: "classifier:v1", ImageVolumes: []pipelinesv1alpha1.FilterImageVolume{vol}},
	)

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")
	podSpec := job.Spec.Template.Spec

	if len(podSpec.Volumes) != 2 {
		t.Fatalf("expected workspace + 1 deduplicated image volume, got %v", podSpec.Volumes)
	}
	imgVol := findVolume(podSpec.Volumes, imageVolumeName(testModelImage, ""))
	if imgVol == nil || imgVol.Image == nil {
		t.Fatalf("expected an image-source volume, got %v", podSpec.Volumes)
	}
	if imgVol.Image.Reference != testModelImage {
		t.Errorf("expected volume reference %q, got %q", testModelImage, imgVol.Image.Reference)
	}
	for _, name := range []string{"detector", "classifier"} {
		mount := findMount(podContainer(t, podSpec, name), "/opt/model")
		if mount == nil {
			t.Fatalf("expected /opt/model mount on container %q", name)
		}
		if !mount.ReadOnly {
			t.Errorf("expected read-only mount on container %q", name)
		}
		if mount.Name != imgVol.Name {
			t.Errorf("expected mount on container %q to reference volume %q, got %q", name, imgVol.Name, mount.Name)
		}
	}

	// The claimer initContainer must stay untouched (workspace mount only).
	claimer := podSpec.InitContainers[0]
	if len(claimer.VolumeMounts) != 1 || claimer.VolumeMounts[0].Name != workspaceVolumeName {
		t.Errorf("expected claimer initContainer untouched, got mounts %v", claimer.VolumeMounts)
	}
}

func TestBuildJob_ImageVolumes_MountShapeAndDistinctKeys(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()
	pipeline := imageVolumePipeline(pipelinesv1alpha1.Filter{
		Name:  "detector",
		Image: "detector:v1",
		ImageVolumes: []pipelinesv1alpha1.FilterImageVolume{
			{Name: "model-a", Image: testModelImage, MountPath: "/opt/model-a", SubPath: "models/latest", PullPolicy: corev1.PullAlways},
			{Name: "model-b", Image: testModelImage, MountPath: "/opt/model-b", PullSecret: "reg-secret"},
		},
	})

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")
	podSpec := job.Spec.Template.Spec

	// Same image, different pullSecret: distinct dedup keys, two volumes.
	if len(podSpec.Volumes) != 3 {
		t.Fatalf("expected workspace + 2 image volumes, got %v", podSpec.Volumes)
	}
	volA := findVolume(podSpec.Volumes, imageVolumeName(testModelImage, ""))
	if volA == nil || volA.Image == nil || volA.Image.PullPolicy != corev1.PullAlways {
		t.Fatalf("expected volume A with pullPolicy Always, got %v", volA)
	}
	volB := findVolume(podSpec.Volumes, imageVolumeName(testModelImage, "reg-secret"))
	if volB == nil || volB.Image == nil {
		t.Fatalf("expected a second volume for the pullSecret variant, got %v", podSpec.Volumes)
	}

	container := podContainer(t, podSpec, "detector")
	mountA := findMount(container, "/opt/model-a")
	if mountA == nil || mountA.SubPath != "models/latest" {
		t.Fatalf("expected /opt/model-a mount with subPath, got %v", mountA)
	}
	if mountB := findMount(container, "/opt/model-b"); mountB == nil || mountB.Name != volB.Name {
		t.Fatalf("expected /opt/model-b mount referencing %q, got %v", volB.Name, mountB)
	}
}

func TestBuildJob_ImageVolumes_PullSecretMerge(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()
	pipeline := imageVolumePipeline(pipelinesv1alpha1.Filter{
		Name:  "detector",
		Image: "detector:v1",
		ImageVolumes: []pipelinesv1alpha1.FilterImageVolume{
			{Name: "model-a", Image: "registry.example.com/a:v1", MountPath: "/opt/a", PullSecret: "reg-secret"},
			{Name: "model-b", Image: "registry.example.com/b:v1", MountPath: "/opt/b", PullSecret: "existing"},
		},
	})
	pipeline.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "existing"}}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")
	got := job.Spec.Template.Spec.ImagePullSecrets

	if len(got) != 2 || got[0].Name != "existing" || got[1].Name != "reg-secret" {
		t.Fatalf("expected [existing reg-secret] (new secret appended, duplicate skipped), got %v", got)
	}
	// The Pipeline object's own slice must not have been mutated.
	if len(pipeline.Spec.ImagePullSecrets) != 1 {
		t.Fatalf("pipeline.Spec.ImagePullSecrets was mutated: %v", pipeline.Spec.ImagePullSecrets)
	}
}

func TestBuildStreamingDeployment_ImageVolumes(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	pipeline := imageVolumePipeline(pipelinesv1alpha1.Filter{
		Name:  "detector",
		Image: "detector:v1",
		ImageVolumes: []pipelinesv1alpha1.FilterImageVolume{
			{Name: "model", Image: testModelImage, MountPath: "/opt/model"},
		},
	})

	deployment := r.buildStreamingDeployment(context.Background(), pi, pipeline, nil, "test-deploy")
	podSpec := deployment.Spec.Template.Spec

	if len(podSpec.Volumes) != 1 {
		t.Fatalf("expected exactly the image volume on the streaming pod, got %v", podSpec.Volumes)
	}
	if podSpec.Volumes[0].Image == nil || podSpec.Volumes[0].Image.Reference != testModelImage {
		t.Fatalf("expected an image-source volume, got %v", podSpec.Volumes[0])
	}
	mount := findMount(podContainer(t, podSpec, "detector"), "/opt/model")
	if mount == nil || !mount.ReadOnly {
		t.Fatalf("expected read-only /opt/model mount on streaming container, got %v", mount)
	}
}

func TestBuildStreamingDeployment_NoImageVolumes_NoVolumes(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	pipeline := imageVolumePipeline(pipelinesv1alpha1.Filter{Name: "detector", Image: "detector:v1"})

	deployment := r.buildStreamingDeployment(context.Background(), pi, pipeline, nil, "test-deploy")

	if deployment.Spec.Template.Spec.Volumes != nil {
		t.Fatalf("expected no volumes on a streaming pod without imageVolumes, got %v", deployment.Spec.Template.Spec.Volumes)
	}
}

func TestBuildMultiSourceBatchJob_ImageVolumes(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	pipeline := imageVolumePipeline(pipelinesv1alpha1.Filter{
		Name:  "detector",
		Image: "detector:v1",
		ImageVolumes: []pipelinesv1alpha1.FilterImageVolume{
			{Name: "model", Image: testModelImage, MountPath: "/opt/model"},
		},
	})

	job := r.buildMultiSourceBatchJob(context.Background(), pi, pipeline, nil, "test-job")
	podSpec := job.Spec.Template.Spec

	if findVolume(podSpec.Volumes, imageVolumeName(testModelImage, "")) == nil {
		t.Fatalf("expected the image volume on the multi-source pod, got %v", podSpec.Volumes)
	}
	mount := findMount(podContainer(t, podSpec, "detector"), "/opt/model")
	if mount == nil || !mount.ReadOnly {
		t.Fatalf("expected read-only /opt/model mount on multi-source container, got %v", mount)
	}
}

func TestBuildMultiSourceBatchJob_NoImageVolumes_Unchanged(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	pipeline := imageVolumePipeline(pipelinesv1alpha1.Filter{Name: "plain", Image: "plain:v1"})

	job := r.buildMultiSourceBatchJob(context.Background(), pi, pipeline, nil, "test-job")

	podSpec := job.Spec.Template.Spec
	if len(podSpec.Volumes) != 1 || podSpec.Volumes[0].Name != workspaceVolumeName {
		t.Fatalf("expected only the workspace volume on the multi-source pod, got %v", podSpec.Volumes)
	}
	if got := len(podContainer(t, podSpec, "plain").VolumeMounts); got != 1 {
		t.Fatalf("expected only the workspace mount on the filter container, got %d mounts", got)
	}
}

func TestBuildJob_ImageVolumes_StableNamesAcrossReordering(t *testing.T) {
	r := makeMinimalReconciler()
	ps := makeMinimalPipelineSource()
	filterA := pipelinesv1alpha1.Filter{
		Name: "detector", Image: "detector:v1",
		ImageVolumes: []pipelinesv1alpha1.FilterImageVolume{
			{Name: "model-a", Image: "registry.example.com/a:v1", MountPath: "/opt/a"},
		},
	}
	filterB := pipelinesv1alpha1.Filter{
		Name: "classifier", Image: "classifier:v1",
		ImageVolumes: []pipelinesv1alpha1.FilterImageVolume{
			{Name: "model-b", Image: "registry.example.com/b:v1", MountPath: "/opt/b", PullSecret: "reg-secret"},
		},
	}

	volumeNames := func(filters ...pipelinesv1alpha1.Filter) map[string]bool {
		job := r.buildJob(context.Background(), makeMinimalPipelineInstance(), imageVolumePipeline(filters...), ps, "test-job")
		names := map[string]bool{}
		for _, v := range job.Spec.Template.Spec.Volumes {
			if v.Image != nil {
				names[v.Name] = true
			}
		}
		return names
	}

	forward := volumeNames(filterA, filterB)
	reversed := volumeNames(filterB, filterA)

	if len(forward) != 2 {
		t.Fatalf("expected 2 image volumes, got %v", forward)
	}
	for name := range forward {
		if !reversed[name] {
			t.Errorf("volume name %q not stable under filter reordering: forward=%v reversed=%v", name, forward, reversed)
		}
	}
}

func TestImageVolumeSupportReason(t *testing.T) {
	cases := []struct {
		name        string
		gitVersion  string
		wantBlocked bool
	}{
		{"exactly 1.33", "v1.33.0", false},
		{"newer minor", "v1.34.1", false},
		{"newer with vendor suffix", "v1.34.5-gke.100", false},
		{"next major", "v2.0.0", false},
		{"1.32 blocked", "v1.32.9", true},
		{"1.31 blocked even though gate could be on", "v1.31.0", true},
		{"much older", "v1.28.3+k3s1", true},
		{"unparseable fails open", "weird-build", false},
		{"empty fails open", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reason := ImageVolumeSupportReason(&k8sversion.Info{GitVersion: tc.gitVersion})
			if got := reason != ""; got != tc.wantBlocked {
				t.Fatalf("ImageVolumeSupportReason(%q) = %q, wantBlocked=%v", tc.gitVersion, reason, tc.wantBlocked)
			}
			if tc.wantBlocked && !strings.Contains(reason, tc.gitVersion) {
				t.Errorf("expected reason to name the detected version %q, got %q", tc.gitVersion, reason)
			}
		})
	}
	if ImageVolumeSupportReason(nil) != "" {
		t.Error("expected nil version info to fail open")
	}
}

func TestPipelineHasImageVolumes(t *testing.T) {
	without := imageVolumePipeline(pipelinesv1alpha1.Filter{Name: "plain", Image: "plain:v1"})
	if pipelineHasImageVolumes(without) {
		t.Error("expected false for a pipeline without imageVolumes")
	}
	with := imageVolumePipeline(
		pipelinesv1alpha1.Filter{Name: "plain", Image: "plain:v1"},
		pipelinesv1alpha1.Filter{Name: "model", Image: "model:v1", ImageVolumes: []pipelinesv1alpha1.FilterImageVolume{
			{Name: "m", Image: testModelImage, MountPath: "/opt/model"},
		}},
	)
	if !pipelineHasImageVolumes(with) {
		t.Error("expected true when any filter declares imageVolumes")
	}
}
