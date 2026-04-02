package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
)

const expectedDriverVersion = "latest"

func makeMinimalReconciler() *PipelineInstanceReconciler {
	return &PipelineInstanceReconciler{
		ClaimerImage:   "claimer:latest",
		ValkeyAddr:     "valkey:6379",
		GPULibraryPath: DefaultGPULibraryPath,
	}
}

func makeMinimalPipelineInstance() *pipelinesv1alpha1.PipelineInstance {
	return &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-instance",
			Namespace: "default",
			UID:       types.UID("test-uid-1234"),
		},
	}
}

func makeMinimalPipelineSource() *pipelinesv1alpha1.PipelineSource {
	return &pipelinesv1alpha1.PipelineSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-source",
			Namespace: "default",
		},
	}
}

func TestBuildJob_GPUNodeSelector_WithGPULimits(t *testing.T) {
	r := makeMinimalReconciler()
	r.GPUNodeSelectorLabels = map[string]string{"cloud.google.com/gke-gpu-driver-version": expectedDriverVersion}
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{
					Name:  "gpu-filter",
					Image: "gpu-filter:latest",
					Resources: &corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							"nvidia.com/gpu": resource.MustParse("1"),
						},
					},
				},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")

	nodeSelector := job.Spec.Template.Spec.NodeSelector
	if nodeSelector == nil {
		t.Fatal("expected NodeSelector to be set for GPU workload, got nil")
	}
	got := nodeSelector["cloud.google.com/gke-gpu-driver-version"]
	if got != expectedDriverVersion {
		t.Errorf("expected NodeSelector[cloud.google.com/gke-gpu-driver-version]=latest, got %q", got)
	}
}

func TestBuildJob_GPUNodeSelector_WithGPURequests(t *testing.T) {
	r := makeMinimalReconciler()
	r.GPUNodeSelectorLabels = map[string]string{"cloud.google.com/gke-gpu-driver-version": expectedDriverVersion}
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{
					Name:  "gpu-filter",
					Image: "gpu-filter:latest",
					Resources: &corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							"nvidia.com/gpu": resource.MustParse("1"),
						},
					},
				},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")

	nodeSelector := job.Spec.Template.Spec.NodeSelector
	if nodeSelector == nil {
		t.Fatal("expected NodeSelector to be set for GPU workload, got nil")
	}
	got := nodeSelector["cloud.google.com/gke-gpu-driver-version"]
	if got != expectedDriverVersion {
		t.Errorf("expected NodeSelector[cloud.google.com/gke-gpu-driver-version]=latest, got %q", got)
	}
}

func TestBuildJob_GPUNodeSelector_WithoutGPU(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{
					Name:  "cpu-filter",
					Image: "cpu-filter:latest",
					Resources: &corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("500m"),
							corev1.ResourceMemory: resource.MustParse("256Mi"),
						},
					},
				},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")

	nodeSelector := job.Spec.Template.Spec.NodeSelector
	if nodeSelector != nil {
		if _, ok := nodeSelector["cloud.google.com/gke-gpu-driver-version"]; ok {
			t.Error("expected no GPU driver NodeSelector for non-GPU workload, but found one")
		}
	}
}

func TestBuildJob_GPUNodeSelector_NoResources(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{
					Name:  "basic-filter",
					Image: "basic-filter:latest",
				},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")

	nodeSelector := job.Spec.Template.Spec.NodeSelector
	if nodeSelector != nil {
		if _, ok := nodeSelector["cloud.google.com/gke-gpu-driver-version"]; ok {
			t.Error("expected no GPU driver NodeSelector for filter with no resources, but found one")
		}
	}
}

func TestBuildJob_GPUNodeSelector_MultipleLabels(t *testing.T) {
	r := makeMinimalReconciler()
	r.GPUNodeSelectorLabels = map[string]string{
		"cloud.google.com/gke-gpu-driver-version": expectedDriverVersion,
		"nvidia.com/present":                      "true",
	}
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{
					Name:  "gpu-filter",
					Image: "gpu-filter:latest",
					Resources: &corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							"nvidia.com/gpu": resource.MustParse("1"),
						},
					},
				},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")

	nodeSelector := job.Spec.Template.Spec.NodeSelector
	if nodeSelector == nil {
		t.Fatal("expected NodeSelector to be set for GPU workload, got nil")
	}
	if got := nodeSelector["cloud.google.com/gke-gpu-driver-version"]; got != expectedDriverVersion {
		t.Errorf("expected NodeSelector[cloud.google.com/gke-gpu-driver-version]=latest, got %q", got)
	}
	if got := nodeSelector["nvidia.com/present"]; got != "true" {
		t.Errorf("expected NodeSelector[nvidia.com/present]=true, got %q", got)
	}
}

func TestBuildJob_GPUNodeSelector_NilLabels(t *testing.T) {
	r := makeMinimalReconciler()
	// GPUNodeSelectorLabels is nil (zero value)
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{
					Name:  "gpu-filter",
					Image: "gpu-filter:latest",
					Resources: &corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							"nvidia.com/gpu": resource.MustParse("1"),
						},
					},
				},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")

	if nodeSelector := job.Spec.Template.Spec.NodeSelector; nodeSelector != nil {
		if len(nodeSelector) != 0 {
			t.Errorf("expected empty NodeSelector when GPUNodeSelectorLabels is nil, got %v", nodeSelector)
		}
	}
}

func TestBuildJob_GPUNodeSelector_EmptyLabels(t *testing.T) {
	r := makeMinimalReconciler()
	r.GPUNodeSelectorLabels = map[string]string{}
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{
					Name:  "gpu-filter",
					Image: "gpu-filter:latest",
					Resources: &corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							"nvidia.com/gpu": resource.MustParse("1"),
						},
					},
				},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")

	if nodeSelector := job.Spec.Template.Spec.NodeSelector; nodeSelector != nil {
		if len(nodeSelector) != 0 {
			t.Errorf("expected empty NodeSelector when GPUNodeSelectorLabels is empty, got %v", nodeSelector)
		}
	}
}

func TestBuildJob_GPUNodeSelector_DefensiveCopy(t *testing.T) {
	r := makeMinimalReconciler()
	r.GPUNodeSelectorLabels = map[string]string{"cloud.google.com/gke-gpu-driver-version": expectedDriverVersion}
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{
					Name:  "gpu-filter",
					Image: "gpu-filter:latest",
					Resources: &corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							"nvidia.com/gpu": resource.MustParse("1"),
						},
					},
				},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")

	// Mutate the returned pod's NodeSelector
	job.Spec.Template.Spec.NodeSelector["injected-key"] = "injected-value"
	job.Spec.Template.Spec.NodeSelector["cloud.google.com/gke-gpu-driver-version"] = "mutated"

	// The reconciler's shared map must be unaffected
	if got := r.GPUNodeSelectorLabels["cloud.google.com/gke-gpu-driver-version"]; got != expectedDriverVersion {
		t.Errorf("defensive copy broken: r.GPUNodeSelectorLabels[driver-version] = %q, want \"latest\"", got)
	}
	if _, ok := r.GPUNodeSelectorLabels["injected-key"]; ok {
		t.Error("defensive copy broken: injected-key appeared in r.GPUNodeSelectorLabels")
	}
}

func TestBuildJob_GPUNodeSelector_BothLimitsAndRequests(t *testing.T) {
	r := makeMinimalReconciler()
	r.GPUNodeSelectorLabels = map[string]string{"cloud.google.com/gke-gpu-driver-version": expectedDriverVersion}
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{
					Name:  "gpu-filter",
					Image: "gpu-filter:latest",
					Resources: &corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							"nvidia.com/gpu": resource.MustParse("1"),
						},
						Requests: corev1.ResourceList{
							"nvidia.com/gpu": resource.MustParse("1"),
						},
					},
				},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")

	nodeSelector := job.Spec.Template.Spec.NodeSelector
	if nodeSelector == nil {
		t.Fatal("expected NodeSelector to be set when GPU is in both limits and requests, got nil")
	}
	if got := nodeSelector["cloud.google.com/gke-gpu-driver-version"]; got != expectedDriverVersion {
		t.Errorf("expected NodeSelector[cloud.google.com/gke-gpu-driver-version]=latest, got %q", got)
	}
	if len(nodeSelector) != 1 {
		t.Errorf("expected exactly 1 NodeSelector entry, got %d: %v", len(nodeSelector), nodeSelector)
	}
}

func findEnvVar(envVars []corev1.EnvVar, name string) (corev1.EnvVar, bool) {
	for _, e := range envVars {
		if e.Name == name {
			return e, true
		}
	}
	return corev1.EnvVar{}, false
}

func TestBuildJob_GPUEnvInjection_WithGPULimits(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{
					Name:  "gpu-filter",
					Image: "gpu-filter:latest",
					Resources: &corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							"nvidia.com/gpu": resource.MustParse("1"),
						},
					},
				},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")

	containers := job.Spec.Template.Spec.Containers
	if len(containers) == 0 {
		t.Fatal("expected at least one container")
	}
	env := containers[0].Env

	ldLibPath, ok := findEnvVar(env, ldLibraryPathEnvName)
	if !ok {
		t.Fatal("expected LD_LIBRARY_PATH to be set for GPU container")
	}
	if ldLibPath.Value != DefaultGPULibraryPath {
		t.Errorf("expected LD_LIBRARY_PATH=%q, got %q", DefaultGPULibraryPath, ldLibPath.Value)
	}

	if _, ok := findEnvVar(env, "PATH"); ok {
		t.Error("PATH should not be injected (Kubernetes $(PATH) does not expand image PATH)")
	}
}

func TestBuildJob_GPUEnvInjection_WithGPURequests(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{
					Name:  "gpu-filter",
					Image: "gpu-filter:latest",
					Resources: &corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							"nvidia.com/gpu": resource.MustParse("1"),
						},
					},
				},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")

	env := job.Spec.Template.Spec.Containers[0].Env
	if _, ok := findEnvVar(env, ldLibraryPathEnvName); !ok {
		t.Error("expected LD_LIBRARY_PATH to be set for GPU container with Requests")
	}
}

func TestBuildJob_GPUEnvInjection_NotInjectedForCPUContainer(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{
					Name:  "cpu-filter",
					Image: "cpu-filter:latest",
					Resources: &corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("500m"),
							corev1.ResourceMemory: resource.MustParse("256Mi"),
						},
					},
				},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")

	env := job.Spec.Template.Spec.Containers[0].Env
	if _, ok := findEnvVar(env, ldLibraryPathEnvName); ok {
		t.Error("expected LD_LIBRARY_PATH NOT to be set for CPU-only container")
	}
	if _, ok := findEnvVar(env, "PATH"); ok {
		t.Error("expected PATH NOT to be set for CPU-only container")
	}
}

func TestBuildJob_GPUEnvInjection_ZeroQuantityNotInjected(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{
					Name:  "zero-gpu-filter",
					Image: "filter:latest",
					Resources: &corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							"nvidia.com/gpu": resource.MustParse("0"),
						},
					},
				},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")

	env := job.Spec.Template.Spec.Containers[0].Env
	if _, ok := findEnvVar(env, ldLibraryPathEnvName); ok {
		t.Error("expected LD_LIBRARY_PATH NOT to be injected for nvidia.com/gpu: 0")
	}
	if nodeSelector := job.Spec.Template.Spec.NodeSelector; nodeSelector != nil {
		if _, ok := nodeSelector["cloud.google.com/gke-gpu-driver-version"]; ok {
			t.Error("expected no GPU NodeSelector for nvidia.com/gpu: 0")
		}
	}
}

func TestBuildJob_GPUEnvInjection_NotInjectedForNoResources(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{
					Name:  "basic-filter",
					Image: "basic-filter:latest",
				},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")

	env := job.Spec.Template.Spec.Containers[0].Env
	if _, ok := findEnvVar(env, ldLibraryPathEnvName); ok {
		t.Error("expected LD_LIBRARY_PATH NOT to be set for container with no resources")
	}
}

func TestBuildJob_GPUEnvInjection_UserCanOverride(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	userPath := "/custom/lib:/usr/lib"
	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{
					Name:  "gpu-filter",
					Image: "gpu-filter:latest",
					Resources: &corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							"nvidia.com/gpu": resource.MustParse("1"),
						},
					},
					Env: []corev1.EnvVar{
						{Name: ldLibraryPathEnvName, Value: userPath},
					},
				},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")
	env := job.Spec.Template.Spec.Containers[0].Env

	// Both the injected default and the user override should be present;
	// the user's value appears last so container runtime uses it.
	var ldLibVals []string
	for _, e := range env {
		if e.Name == ldLibraryPathEnvName {
			ldLibVals = append(ldLibVals, e.Value)
		}
	}
	if len(ldLibVals) != 2 {
		t.Fatalf("expected 2 LD_LIBRARY_PATH entries (default + user override), got %d: %v", len(ldLibVals), ldLibVals)
	}
	if ldLibVals[0] != DefaultGPULibraryPath {
		t.Errorf("first LD_LIBRARY_PATH should be default %q, got %q", DefaultGPULibraryPath, ldLibVals[0])
	}
	if ldLibVals[1] != userPath {
		t.Errorf("second LD_LIBRARY_PATH should be user override %q, got %q", userPath, ldLibVals[1])
	}
}

func TestBuildJob_GPUEnvInjection_PerContainerNotPod(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{
					Name:  "gpu-filter",
					Image: "gpu-filter:latest",
					Resources: &corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							"nvidia.com/gpu": resource.MustParse("1"),
						},
					},
				},
				{
					Name:  "cpu-sidecar",
					Image: "cpu-sidecar:latest",
				},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")
	containers := job.Spec.Template.Spec.Containers

	if len(containers) < 2 {
		t.Fatalf("expected 2 containers, got %d", len(containers))
	}

	// GPU container should have the env vars
	var gpuContainer, cpuContainer corev1.Container
	for _, c := range containers {
		switch c.Name {
		case "gpu-filter":
			gpuContainer = c
		case "cpu-sidecar":
			cpuContainer = c
		}
	}

	if _, ok := findEnvVar(gpuContainer.Env, ldLibraryPathEnvName); !ok {
		t.Error("expected LD_LIBRARY_PATH on GPU container")
	}
	if _, ok := findEnvVar(cpuContainer.Env, ldLibraryPathEnvName); ok {
		t.Error("expected LD_LIBRARY_PATH NOT on CPU sidecar")
	}
}

func TestBuildJob_GPUEnvInjection_NilResources(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{Name: "nil-resources-filter", Image: "filter:latest", Resources: nil},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")
	env := job.Spec.Template.Spec.Containers[0].Env
	if _, ok := findEnvVar(env, ldLibraryPathEnvName); ok {
		t.Error("expected LD_LIBRARY_PATH NOT to be set for filter with nil Resources")
	}
}

func TestBuildJob_GPUEnvInjection_EmptyResourceRequirements(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{
					Name:      "empty-resources-filter",
					Image:     "filter:latest",
					Resources: &corev1.ResourceRequirements{},
				},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")
	env := job.Spec.Template.Spec.Containers[0].Env
	if _, ok := findEnvVar(env, ldLibraryPathEnvName); ok {
		t.Error("expected LD_LIBRARY_PATH NOT to be set for filter with empty ResourceRequirements")
	}
}

func TestBuildJob_GPUEnvInjection_BothLimitsAndRequestsInjectsOnce(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{
					Name:  "gpu-filter",
					Image: "gpu-filter:latest",
					Resources: &corev1.ResourceRequirements{
						Limits:   corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")},
						Requests: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")},
					},
				},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")
	env := job.Spec.Template.Spec.Containers[0].Env

	var count int
	for _, e := range env {
		if e.Name == ldLibraryPathEnvName {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 LD_LIBRARY_PATH entry when GPU is in both Limits and Requests, got %d", count)
	}
}

func TestBuildJob_GPUEnvInjection_ZeroLimitsNonZeroRequests(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{
					Name:  "gpu-filter",
					Image: "gpu-filter:latest",
					Resources: &corev1.ResourceRequirements{
						Limits:   corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("0")},
						Requests: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")},
					},
				},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")
	env := job.Spec.Template.Spec.Containers[0].Env
	if _, ok := findEnvVar(env, ldLibraryPathEnvName); !ok {
		t.Error("expected LD_LIBRARY_PATH to be injected when Requests has positive GPU (Limits is zero)")
	}
}

func TestBuildJob_GPUEnvInjection_NonZeroLimitsZeroRequests(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{
					Name:  "gpu-filter",
					Image: "gpu-filter:latest",
					Resources: &corev1.ResourceRequirements{
						Limits:   corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")},
						Requests: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("0")},
					},
				},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")
	env := job.Spec.Template.Spec.Containers[0].Env
	if _, ok := findEnvVar(env, ldLibraryPathEnvName); !ok {
		t.Error("expected LD_LIBRARY_PATH to be injected when Limits has positive GPU (Requests is zero)")
	}
}

func TestBuildJob_GPUEnvInjection_NegativeQuantityNotInjected(t *testing.T) {
	// resource.Quantity.Sign() returns -1 for negative values, which are invalid per
	// Kubernetes API validation but may appear in malformed specs. The controller
	// should treat negative as non-GPU (Sign() > 0 guard).
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{
					Name:  "bad-filter",
					Image: "filter:latest",
					Resources: &corev1.ResourceRequirements{
						Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("-1")},
					},
				},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")
	env := job.Spec.Template.Spec.Containers[0].Env
	if _, ok := findEnvVar(env, ldLibraryPathEnvName); ok {
		t.Error("expected LD_LIBRARY_PATH NOT to be injected for negative GPU quantity")
	}
}

func TestBuildJob_GPUEnvInjection_LargeGPUCount(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{
					Name:  "multi-gpu-filter",
					Image: "gpu-filter:latest",
					Resources: &corev1.ResourceRequirements{
						Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("8")},
					},
				},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")
	env := job.Spec.Template.Spec.Containers[0].Env
	ldLib, ok := findEnvVar(env, ldLibraryPathEnvName)
	if !ok {
		t.Fatal("expected LD_LIBRARY_PATH to be set for large GPU count")
	}
	if ldLib.Value != DefaultGPULibraryPath {
		t.Errorf("expected LD_LIBRARY_PATH=%q, got %q", DefaultGPULibraryPath, ldLib.Value)
	}
}

func TestBuildJob_GPUEnvInjection_UserOverridesWithEmptyString(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{
					Name:  "gpu-filter",
					Image: "gpu-filter:latest",
					Resources: &corev1.ResourceRequirements{
						Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("1")},
					},
					Env: []corev1.EnvVar{
						{Name: ldLibraryPathEnvName, Value: ""},
					},
				},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")
	env := job.Spec.Template.Spec.Containers[0].Env

	// Both entries present; user's empty-string override appears last.
	var ldLibVals []string
	for _, e := range env {
		if e.Name == ldLibraryPathEnvName {
			ldLibVals = append(ldLibVals, e.Value)
		}
	}
	if len(ldLibVals) != 2 {
		t.Fatalf("expected 2 LD_LIBRARY_PATH entries, got %d: %v", len(ldLibVals), ldLibVals)
	}
	if ldLibVals[0] != DefaultGPULibraryPath {
		t.Errorf("first LD_LIBRARY_PATH should be default %q, got %q", DefaultGPULibraryPath, ldLibVals[0])
	}
	if ldLibVals[1] != "" {
		t.Errorf("second LD_LIBRARY_PATH should be empty string (user override), got %q", ldLibVals[1])
	}
}

func TestBuildJob_ImagePullSecrets_None(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{Name: "f1", Image: "public/image:latest"},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")
	if len(job.Spec.Template.Spec.ImagePullSecrets) != 0 {
		t.Errorf("expected no ImagePullSecrets, got %v", job.Spec.Template.Spec.ImagePullSecrets)
	}
}

func TestBuildJob_ImagePullSecrets_Propagated(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			ImagePullSecrets: []corev1.LocalObjectReference{
				{Name: "registry-creds"},
				{Name: "other-creds"},
			},
			Filters: []pipelinesv1alpha1.Filter{
				{Name: "f1", Image: "private.registry/image:latest"},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")
	secrets := job.Spec.Template.Spec.ImagePullSecrets
	if len(secrets) != 2 {
		t.Fatalf("expected 2 ImagePullSecrets, got %d", len(secrets))
	}
	if secrets[0].Name != "registry-creds" {
		t.Errorf("expected first secret name 'registry-creds', got %q", secrets[0].Name)
	}
	if secrets[1].Name != "other-creds" {
		t.Errorf("expected second secret name 'other-creds', got %q", secrets[1].Name)
	}
}

func TestBuildJob_ValkeyNSCredentials(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{Name: "filter", Image: "filter:latest"},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")
	claimerEnv := job.Spec.Template.Spec.InitContainers[0].Env

	// Verify VALKEY_USERNAME references the per-namespace secret
	var foundUsername, foundPassword bool
	for _, env := range claimerEnv {
		if env.Name == "VALKEY_USERNAME" {
			foundUsername = true
			if env.ValueFrom == nil || env.ValueFrom.SecretKeyRef == nil {
				t.Fatal("VALKEY_USERNAME should use secretKeyRef")
			}
			if env.ValueFrom.SecretKeyRef.Name != DefaultValkeyNSSecretName {
				t.Errorf("VALKEY_USERNAME secret name = %q, want %q",
					env.ValueFrom.SecretKeyRef.Name, DefaultValkeyNSSecretName)
			}
			if env.ValueFrom.SecretKeyRef.Key != "valkey-username" {
				t.Errorf("VALKEY_USERNAME secret key = %q, want %q",
					env.ValueFrom.SecretKeyRef.Key, "valkey-username")
			}
		}
		if env.Name == "VALKEY_PASSWORD" {
			foundPassword = true
			if env.ValueFrom == nil || env.ValueFrom.SecretKeyRef == nil {
				t.Fatal("VALKEY_PASSWORD should use secretKeyRef")
			}
			if env.ValueFrom.SecretKeyRef.Name != DefaultValkeyNSSecretName {
				t.Errorf("VALKEY_PASSWORD secret name = %q, want %q",
					env.ValueFrom.SecretKeyRef.Name, DefaultValkeyNSSecretName)
			}
			if env.ValueFrom.SecretKeyRef.Key != "valkey-password" {
				t.Errorf("VALKEY_PASSWORD secret key = %q, want %q",
					env.ValueFrom.SecretKeyRef.Key, "valkey-password")
			}
		}
	}
	if !foundUsername {
		t.Error("expected VALKEY_USERNAME env var in claimer, not found")
	}
	if !foundPassword {
		t.Error("expected VALKEY_PASSWORD env var in claimer, not found")
	}
}

func TestBuildJob_ValkeyNSCredentials_CustomSecretName(t *testing.T) {
	r := makeMinimalReconciler()
	r.ValkeyNSSecretName = "custom-valkey-secret"
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{Name: "filter", Image: "filter:latest"},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")
	claimerEnv := job.Spec.Template.Spec.InitContainers[0].Env

	for _, env := range claimerEnv {
		if env.Name == "VALKEY_USERNAME" {
			if env.ValueFrom.SecretKeyRef.Name != "custom-valkey-secret" {
				t.Errorf("VALKEY_USERNAME secret name = %q, want %q",
					env.ValueFrom.SecretKeyRef.Name, "custom-valkey-secret")
			}
			return
		}
	}
	t.Error("expected VALKEY_USERNAME env var in claimer, not found")
}

func TestBuildJob_StreamKeyUsesNamespacePrefix(t *testing.T) {
	r := makeMinimalReconciler()
	pi := makeMinimalPipelineInstance()
	ps := makeMinimalPipelineSource()

	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{Name: "filter", Image: "filter:latest"},
			},
		},
	}

	job := r.buildJob(context.Background(), pi, pipeline, ps, "test-job")
	claimerEnv := job.Spec.Template.Spec.InitContainers[0].Env

	for _, env := range claimerEnv {
		if env.Name == "STREAM" {
			expected := pi.GetQueueStream()
			if env.Value != expected {
				t.Errorf("STREAM = %q, want %q", env.Value, expected)
			}
			if !strings.HasPrefix(env.Value, "ns:") {
				t.Errorf("STREAM should start with 'ns:' prefix, got %q", env.Value)
			}
			return
		}
	}
	t.Error("expected STREAM env var in claimer, not found")
}
