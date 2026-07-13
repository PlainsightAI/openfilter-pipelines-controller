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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
)

// gpuFilter is a tiny convenience constructor so the test bodies below stay
// focused on the assertions rather than fixture noise.
func gpuFilter(name string) pipelinesv1alpha1.Filter {
	return pipelinesv1alpha1.Filter{
		Name:  name,
		Image: name + ":latest",
		Resources: &corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				"nvidia.com/gpu": resource.MustParse("1"),
			},
		},
	}
}

func cpuFilter(name string) pipelinesv1alpha1.Filter {
	return pipelinesv1alpha1.Filter{
		Name:  name,
		Image: name + ":latest",
		Resources: &corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("500m"),
				corev1.ResourceMemory: resource.MustParse("256Mi"),
			},
		},
	}
}

// findContainer returns the container with the given name, or t.Fatalf-s if
// not found. Saves the per-test loop boilerplate.
func findContainer(t *testing.T, containers []corev1.Container, name string) corev1.Container {
	t.Helper()
	for _, c := range containers {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("container %q not found in %v", name, containerNames(containers))
	return corev1.Container{}
}

func containerNames(containers []corev1.Container) []string {
	out := make([]string, len(containers))
	for i, c := range containers {
		out[i] = c.Name
	}
	return out
}

// hasNvidiaVisibleDevicesAll reports whether the container env carries
// NVIDIA_VISIBLE_DEVICES=all (the value the reconciler injects on
// GPU-requesting containers).
func hasNvidiaVisibleDevicesAll(env []corev1.EnvVar) bool {
	for _, e := range env {
		if e.Name == nvidiaVisibleDevicesEnvName && e.Value == "all" {
			return true
		}
	}
	return false
}

// TestNodeSelector_MergePrecedence verifies that the per-instance NodeSelector
// merges with the controller-wide GPUNodeSelectorLabels and that instance keys
// win on conflict.
func TestNodeSelector_MergePrecedence(t *testing.T) {
	r := &PipelineInstanceReconciler{
		GPUNodeSelectorLabels: map[string]string{"a": "1"},
	}
	pi := makeMinimalStreamingPipelineInstance()
	pi.Spec.NodeSelector = map[string]string{"a": "2", "b": "3"}

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{gpuFilter("gpu1")},
		},
	}

	deployment := r.buildStreamingDeployment(context.Background(), pi, pipeline, nil, "test-deployment")
	got := deployment.Spec.Template.Spec.NodeSelector

	want := map[string]string{"a": "2", "b": "3"}
	if len(got) != len(want) {
		t.Fatalf("expected %d NodeSelector entries, got %d: %v", len(want), len(got), got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("NodeSelector[%q] = %q, want %q (full map: %v)", k, got[k], v, got)
		}
	}
}

// TestNodeSelector_NonGPUPipeline verifies that for a non-GPU pipeline the
// controller-wide GPU selector is intentionally NOT applied and only the
// per-instance NodeSelector lands on the pod.
func TestNodeSelector_NonGPUPipeline(t *testing.T) {
	r := &PipelineInstanceReconciler{
		GPUNodeSelectorLabels: map[string]string{"cloud.google.com/gke-gpu-driver-version": "latest"},
	}
	pi := makeMinimalStreamingPipelineInstance()
	pi.Spec.NodeSelector = map[string]string{"x": "y"}

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{cpuFilter("cpu-only")},
		},
	}

	deployment := r.buildStreamingDeployment(context.Background(), pi, pipeline, nil, "test-deployment")
	got := deployment.Spec.Template.Spec.NodeSelector

	if got["x"] != "y" {
		t.Errorf("NodeSelector[x] = %q, want %q (full map: %v)", got["x"], "y", got)
	}
	if _, ok := got["cloud.google.com/gke-gpu-driver-version"]; ok {
		t.Errorf("controller-wide GPU label leaked into non-GPU pipeline: %v", got)
	}
	if len(got) != 1 {
		t.Errorf("expected exactly 1 NodeSelector entry, got %d: %v", len(got), got)
	}
}

// TestTolerations_Passthrough verifies that PipelineInstance Tolerations are
// appended to the pod spec, preserving any controller-injected ones (today
// there are none, but the API contract is "append, never replace").
func TestTolerations_Passthrough(t *testing.T) {
	r := &PipelineInstanceReconciler{}
	pi := makeMinimalStreamingPipelineInstance()
	pi.Spec.Tolerations = []corev1.Toleration{
		{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "gpu", Effect: corev1.TaintEffectNoSchedule},
		{Key: "spot", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectPreferNoSchedule},
	}

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{cpuFilter("cpu-only")},
		},
	}

	deployment := r.buildStreamingDeployment(context.Background(), pi, pipeline, nil, "test-deployment")
	got := deployment.Spec.Template.Spec.Tolerations

	if len(got) != 2 {
		t.Fatalf("expected 2 tolerations, got %d: %v", len(got), got)
	}
	if got[0].Key != "dedicated" || got[0].Value != "gpu" {
		t.Errorf("toleration[0] = %+v, want key=dedicated value=gpu", got[0])
	}
	if got[1].Key != "spot" || got[1].Operator != corev1.TolerationOpExists {
		t.Errorf("toleration[1] = %+v, want key=spot operator=Exists", got[1])
	}
}

// TestAffinity_Passthrough verifies that PipelineInstance Affinity is passed
// through verbatim to the pod spec.
func TestAffinity_Passthrough(t *testing.T) {
	r := &PipelineInstanceReconciler{}
	pi := makeMinimalStreamingPipelineInstance()
	pi.Spec.Affinity = &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{
						MatchExpressions: []corev1.NodeSelectorRequirement{
							{
								Key:      "topology.kubernetes.io/zone",
								Operator: corev1.NodeSelectorOpIn,
								Values:   []string{"us-central1-a", "us-central1-b"},
							},
						},
					},
				},
			},
		},
	}

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{cpuFilter("cpu-only")},
		},
	}

	deployment := r.buildStreamingDeployment(context.Background(), pi, pipeline, nil, "test-deployment")
	got := deployment.Spec.Template.Spec.Affinity

	if got == nil {
		t.Fatal("expected Affinity to be set, got nil")
	}
	if got.NodeAffinity == nil || got.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		t.Fatalf("expected NodeAffinity.Required set, got %+v", got)
	}
	terms := got.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) != 1 || len(terms[0].MatchExpressions) != 1 {
		t.Fatalf("unexpected NodeSelectorTerms shape: %+v", terms)
	}
	expr := terms[0].MatchExpressions[0]
	if expr.Key != "topology.kubernetes.io/zone" || expr.Operator != corev1.NodeSelectorOpIn {
		t.Errorf("expr = %+v, want key=topology.kubernetes.io/zone operator=In", expr)
	}
}

// TestNVIDIAVisibleDevices_ScopedToGPUContainers verifies that
// NVIDIA_VISIBLE_DEVICES=all is injected ONLY into containers whose backing
// filter requests nvidia.com/gpu. CPU-only sidecars (recorder, webvis, etc.)
// must not see the env var, otherwise the NVIDIA runtime may try to expose
// devices to them.
func TestNVIDIAVisibleDevices_ScopedToGPUContainers(t *testing.T) {
	r := &PipelineInstanceReconciler{}
	pi := makeMinimalStreamingPipelineInstance()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				gpuFilter("gpu1"),
				cpuFilter("cpu1"),
				gpuFilter("gpu2"),
				cpuFilter("cpu2"),
			},
		},
	}

	deployment := r.buildStreamingDeployment(context.Background(), pi, pipeline, nil, "test-deployment")
	containers := deployment.Spec.Template.Spec.Containers

	for _, name := range []string{"gpu1", "gpu2"} {
		c := findContainer(t, containers, name)
		if !hasNvidiaVisibleDevicesAll(c.Env) {
			t.Errorf("GPU container %q missing %s=all (env=%v)", name, nvidiaVisibleDevicesEnvName, c.Env)
		}
	}
	for _, name := range []string{"cpu1", "cpu2"} {
		c := findContainer(t, containers, name)
		if hasNvidiaVisibleDevicesAll(c.Env) {
			t.Errorf("CPU container %q unexpectedly has %s=all (env=%v)", name, nvidiaVisibleDevicesEnvName, c.Env)
		}
	}
}

// TestGPULimit_SingleContainer verifies that with multiple GPU-requesting
// filters, the nvidia.com/gpu limit lands on exactly one container — the
// first GPU filter in declaration order. Other GPU containers share via env.
// Today GPUCount is hard-coded to 1; PLAT-1113 will add the per-instance
// count and this test will gain a sibling that uses count>1.
func TestGPULimit_SingleContainer(t *testing.T) {
	r := &PipelineInstanceReconciler{}
	pi := makeMinimalStreamingPipelineInstance()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				gpuFilter("lead-gpu"),
				gpuFilter("follower-gpu"),
			},
		},
	}

	deployment := r.buildStreamingDeployment(context.Background(), pi, pipeline, nil, "test-deployment")
	containers := deployment.Spec.Template.Spec.Containers

	lead := findContainer(t, containers, "lead-gpu")
	leadQ, ok := lead.Resources.Limits["nvidia.com/gpu"]
	if !ok {
		t.Fatalf("lead container missing nvidia.com/gpu limit: %+v", lead.Resources)
	}
	if leadQ.Value() != 1 {
		t.Errorf("lead nvidia.com/gpu = %d, want 1", leadQ.Value())
	}

	follower := findContainer(t, containers, "follower-gpu")
	if _, ok := follower.Resources.Limits["nvidia.com/gpu"]; ok {
		t.Errorf("follower container still has nvidia.com/gpu limit: %+v", follower.Resources.Limits)
	}
	if _, ok := follower.Resources.Requests["nvidia.com/gpu"]; ok {
		t.Errorf("follower container still has nvidia.com/gpu request: %+v", follower.Resources.Requests)
	}
	if !hasNvidiaVisibleDevicesAll(follower.Env) {
		t.Errorf("follower container missing %s=all (env=%v)", nvidiaVisibleDevicesEnvName, follower.Env)
	}
}

// TestMergeNodeSelector_Unit exercises the helper directly so we can cover the
// nil/empty cases without spinning up a full Deployment build.
func TestMergeNodeSelector_Unit(t *testing.T) {
	tests := []struct {
		name              string
		gpu               map[string]string
		instance          map[string]string
		requiresGPU       bool
		want              map[string]string
		expectNilSelector bool
	}{
		{
			name:              "nil + nil, no GPU",
			expectNilSelector: true,
		},
		{
			name:              "GPU labels but pipeline not GPU and no instance selector",
			gpu:               map[string]string{"k": "v"},
			requiresGPU:       false,
			expectNilSelector: true,
		},
		{
			name:        "GPU labels and pipeline GPU",
			gpu:         map[string]string{"k": "v"},
			requiresGPU: true,
			want:        map[string]string{"k": "v"},
		},
		{
			name:     "instance only, non-GPU pipeline",
			instance: map[string]string{"x": "y"},
			want:     map[string]string{"x": "y"},
		},
		{
			name:        "merge with instance override",
			gpu:         map[string]string{"a": "1", "shared": "from-gpu"},
			instance:    map[string]string{"shared": "from-instance", "b": "2"},
			requiresGPU: true,
			want:        map[string]string{"a": "1", "shared": "from-instance", "b": "2"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := mergeNodeSelector(tc.gpu, tc.instance, tc.requiresGPU)
			if tc.expectNilSelector {
				if got != nil {
					t.Fatalf("expected nil selector, got %v", got)
				}
				return
			}
			if len(got) != len(tc.want) {
				t.Fatalf("len=%d want %d (got=%v want=%v)", len(got), len(tc.want), got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Errorf("[%s] = %q, want %q", k, got[k], v)
				}
			}
		})
	}
}

// TestGPURuntimeClassName_Unit covers the helper: a RuntimeClass is only
// returned when a name is configured AND the pod requires a GPU.
func TestGPURuntimeClassName_Unit(t *testing.T) {
	strPtr := func(s string) *string { return &s }
	tests := []struct {
		name        string
		configured  string
		requiresGPU bool
		want        *string
	}{
		{name: "unset + non-GPU", configured: "", requiresGPU: false, want: nil},
		{name: "unset + GPU", configured: "", requiresGPU: true, want: nil},
		{name: "set + non-GPU", configured: "nvidia", requiresGPU: false, want: nil},
		{name: "set + GPU", configured: "nvidia", requiresGPU: true, want: strPtr("nvidia")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := gpuRuntimeClassName(tc.configured, tc.requiresGPU)
			switch {
			case tc.want == nil && got != nil:
				t.Fatalf("expected nil, got %q", *got)
			case tc.want != nil && got == nil:
				t.Fatalf("expected %q, got nil", *tc.want)
			case tc.want != nil && *got != *tc.want:
				t.Errorf("= %q, want %q", *got, *tc.want)
			}
		})
	}
}

// TestInjectNvidiaVisibleDevices_Idempotent confirms that calling the helper
// twice does not append duplicate entries — the reconciler invokes it once
// per filter today but defense-in-depth matters if a future call site
// re-runs the reshape.
func TestInjectNvidiaVisibleDevices_Idempotent(t *testing.T) {
	c := corev1.Container{
		Env: []corev1.EnvVar{{Name: nvidiaVisibleDevicesEnvName, Value: "all"}},
	}
	injectNvidiaVisibleDevices(&c)
	count := 0
	for _, e := range c.Env {
		if e.Name == nvidiaVisibleDevicesEnvName {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 %s entry, got %d (env=%v)", nvidiaVisibleDevicesEnvName, count, c.Env)
	}
}

// TestBatch_NodeSelector_MergePrecedence mirrors the streaming test against
// the batch (buildJob) path. The two share helpers but we keep an explicit
// batch-side test to lock the wiring in place.
func TestBatch_NodeSelector_MergePrecedence(t *testing.T) {
	r := makeMinimalReconciler()
	r.GPUNodeSelectorLabels = map[string]string{"a": "1"}
	pi := makeMinimalPipelineInstance()
	pi.Spec.NodeSelector = map[string]string{"a": "2", "b": "3"}
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{gpuFilter("gpu1")},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")
	got := job.Spec.Template.Spec.NodeSelector

	if got["a"] != "2" {
		t.Errorf("NodeSelector[a] = %q, want %q (instance must win on conflict)", got["a"], "2")
	}
	if got["b"] != "3" {
		t.Errorf("NodeSelector[b] = %q, want %q", got["b"], "3")
	}
}

// TestBatch_Tolerations_Passthrough mirrors the streaming toleration test
// against the batch path.
func TestBatch_Tolerations_Passthrough(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	pi.Spec.Tolerations = []corev1.Toleration{
		{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "gpu", Effect: corev1.TaintEffectNoSchedule},
	}
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{cpuFilter("cpu1")},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")
	got := job.Spec.Template.Spec.Tolerations
	if len(got) != 1 || got[0].Key != "dedicated" {
		t.Errorf("tolerations = %v, want one with key=dedicated", got)
	}
}

// TestBatch_NVIDIAVisibleDevices_ScopedToGPUContainers mirrors the streaming-
// path scoping test against the batch (buildJob) path. The two reconcilers
// share applyGPUContainerSharing, but a batch-side test locks the wiring so a
// future refactor that splits the paths cannot regress CPU sidecars into
// accidentally seeing NVIDIA_VISIBLE_DEVICES=all.
func TestBatch_NVIDIAVisibleDevices_ScopedToGPUContainers(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				gpuFilter("gpu1"),
				cpuFilter("cpu1"),
				gpuFilter("gpu2"),
				cpuFilter("cpu2"),
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")
	containers := job.Spec.Template.Spec.Containers

	for _, name := range []string{"gpu1", "gpu2"} {
		c := findContainer(t, containers, name)
		if !hasNvidiaVisibleDevicesAll(c.Env) {
			t.Errorf("GPU container %q missing %s=all (env=%v)", name, nvidiaVisibleDevicesEnvName, c.Env)
		}
	}
	for _, name := range []string{"cpu1", "cpu2"} {
		c := findContainer(t, containers, name)
		if hasNvidiaVisibleDevicesAll(c.Env) {
			t.Errorf("CPU container %q unexpectedly has %s=all (env=%v)", name, nvidiaVisibleDevicesEnvName, c.Env)
		}
		// Also assert the env var is absent entirely (not just != "all"), since
		// any value here would put the container in the NVIDIA runtime's path.
		for _, e := range c.Env {
			if e.Name == nvidiaVisibleDevicesEnvName {
				t.Errorf("CPU container %q has %s=%q, expected env to be absent", name, e.Name, e.Value)
			}
		}
	}
}

// TestApplyGPUContainerSharing_CountGreaterThanOne exercises the helper
// directly with gpuCount=2. The reconciler currently hard-codes the count to 1
// (instanceGPUCount returns defaultGPUCount until PLAT-1113 wires the API
// field), so this path is unreachable end-to-end today — but locking the
// contract on the helper means PLAT-1113 can flip the wiring with confidence.
//
// The contract under test:
//   - the FIRST GPU container in declaration order carries
//     `nvidia.com/gpu = <count>` on its Limits
//   - subsequent GPU containers have nvidia.com/gpu stripped from both Limits
//     and Requests (per-pod count is authoritative, not summed)
//   - every GPU container has NVIDIA_VISIBLE_DEVICES=all injected so it can
//     see the shared device(s)
//   - CPU-only containers are not touched
func TestApplyGPUContainerSharing_CountGreaterThanOne(t *testing.T) {
	containers := []corev1.Container{
		{
			Name: "lead-gpu",
			Resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					"nvidia.com/gpu": resource.MustParse("1"),
				},
			},
		},
		{
			Name: "cpu-sidecar",
			Resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("250m"),
				},
			},
		},
		{
			Name: "follower-gpu",
			Resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					"nvidia.com/gpu": resource.MustParse("1"),
				},
				Requests: corev1.ResourceList{
					"nvidia.com/gpu": resource.MustParse("1"),
				},
			},
		},
	}

	applyGPUContainerSharing(containers, 2)

	lead := findContainer(t, containers, "lead-gpu")
	leadQ, ok := lead.Resources.Limits["nvidia.com/gpu"]
	if !ok {
		t.Fatalf("lead-gpu missing nvidia.com/gpu limit: %+v", lead.Resources)
	}
	if leadQ.Value() != 2 {
		t.Errorf("lead-gpu nvidia.com/gpu = %d, want 2", leadQ.Value())
	}
	if !hasNvidiaVisibleDevicesAll(lead.Env) {
		t.Errorf("lead-gpu missing %s=all (env=%v)", nvidiaVisibleDevicesEnvName, lead.Env)
	}

	follower := findContainer(t, containers, "follower-gpu")
	if _, ok := follower.Resources.Limits["nvidia.com/gpu"]; ok {
		t.Errorf("follower-gpu must have nvidia.com/gpu stripped from Limits: %v", follower.Resources.Limits)
	}
	if _, ok := follower.Resources.Requests["nvidia.com/gpu"]; ok {
		t.Errorf("follower-gpu must have nvidia.com/gpu stripped from Requests: %v", follower.Resources.Requests)
	}
	if !hasNvidiaVisibleDevicesAll(follower.Env) {
		t.Errorf("follower-gpu missing %s=all (env=%v)", nvidiaVisibleDevicesEnvName, follower.Env)
	}

	cpu := findContainer(t, containers, "cpu-sidecar")
	if _, ok := cpu.Resources.Limits["nvidia.com/gpu"]; ok {
		t.Errorf("cpu-sidecar unexpectedly gained nvidia.com/gpu limit: %v", cpu.Resources.Limits)
	}
	for _, e := range cpu.Env {
		if e.Name == nvidiaVisibleDevicesEnvName {
			t.Errorf("cpu-sidecar unexpectedly has %s=%q, expected env to be absent", e.Name, e.Value)
		}
	}
}

// TestBatch_GPULimit_SingleContainer mirrors the single-container GPU limit
// rule against the batch path.
func TestBatch_GPULimit_SingleContainer(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				gpuFilter("lead"),
				gpuFilter("follower"),
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")
	containers := job.Spec.Template.Spec.Containers

	lead := findContainer(t, containers, "lead")
	if q, ok := lead.Resources.Limits["nvidia.com/gpu"]; !ok || q.Value() != 1 {
		t.Errorf("lead nvidia.com/gpu = %v, ok=%v; want 1, true", q, ok)
	}
	follower := findContainer(t, containers, "follower")
	if _, ok := follower.Resources.Limits["nvidia.com/gpu"]; ok {
		t.Errorf("follower must not carry nvidia.com/gpu limit: %v", follower.Resources.Limits)
	}
	if !hasNvidiaVisibleDevicesAll(follower.Env) {
		t.Errorf("follower must have %s=all", nvidiaVisibleDevicesEnvName)
	}
}
