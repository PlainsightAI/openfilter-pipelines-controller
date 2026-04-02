package controller

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
)

func makeMinimalStreamingPipelineInstance() *pipelinesv1alpha1.PipelineInstance {
	return &pipelinesv1alpha1.PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stream-instance",
			Namespace: "default",
			UID:       types.UID("stream-uid-1234"),
		},
	}
}

func TestBuildStreamingDeployment_GPUNodeSelector_WithGPULimits(t *testing.T) {
	r := &PipelineInstanceReconciler{
		GPUNodeSelectorLabels: map[string]string{"cloud.google.com/gke-gpu-driver-version": expectedDriverVersion},
	}
	pi := makeMinimalStreamingPipelineInstance()
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

	deployment := r.buildStreamingDeployment(pi, pipeline, nil, "test-deployment")

	nodeSelector := deployment.Spec.Template.Spec.NodeSelector
	if nodeSelector == nil {
		t.Fatal("expected NodeSelector to be set for GPU workload, got nil")
	}
	got := nodeSelector["cloud.google.com/gke-gpu-driver-version"]
	if got != expectedDriverVersion {
		t.Errorf("expected NodeSelector[cloud.google.com/gke-gpu-driver-version]=latest, got %q", got)
	}
}

func TestBuildStreamingDeployment_GPUNodeSelector_WithGPURequests(t *testing.T) {
	r := &PipelineInstanceReconciler{
		GPUNodeSelectorLabels: map[string]string{"cloud.google.com/gke-gpu-driver-version": expectedDriverVersion},
	}
	pi := makeMinimalStreamingPipelineInstance()
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

	deployment := r.buildStreamingDeployment(pi, pipeline, nil, "test-deployment")

	nodeSelector := deployment.Spec.Template.Spec.NodeSelector
	if nodeSelector == nil {
		t.Fatal("expected NodeSelector to be set for GPU workload, got nil")
	}
	got := nodeSelector["cloud.google.com/gke-gpu-driver-version"]
	if got != expectedDriverVersion {
		t.Errorf("expected NodeSelector[cloud.google.com/gke-gpu-driver-version]=latest, got %q", got)
	}
}

func TestBuildStreamingDeployment_GPUNodeSelector_WithoutGPU(t *testing.T) {
	r := &PipelineInstanceReconciler{
		GPUNodeSelectorLabels: map[string]string{"cloud.google.com/gke-gpu-driver-version": expectedDriverVersion},
	}
	pi := makeMinimalStreamingPipelineInstance()
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

	deployment := r.buildStreamingDeployment(pi, pipeline, nil, "test-deployment")

	nodeSelector := deployment.Spec.Template.Spec.NodeSelector
	if nodeSelector != nil {
		if _, ok := nodeSelector["cloud.google.com/gke-gpu-driver-version"]; ok {
			t.Error("expected no GPU driver NodeSelector for non-GPU workload, but found one")
		}
	}
}

func TestBuildStreamingDeployment_GPUNodeSelector_NoResources(t *testing.T) {
	r := &PipelineInstanceReconciler{
		GPUNodeSelectorLabels: map[string]string{"cloud.google.com/gke-gpu-driver-version": expectedDriverVersion},
	}
	pi := makeMinimalStreamingPipelineInstance()
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

	deployment := r.buildStreamingDeployment(pi, pipeline, nil, "test-deployment")

	nodeSelector := deployment.Spec.Template.Spec.NodeSelector
	if nodeSelector != nil {
		if _, ok := nodeSelector["cloud.google.com/gke-gpu-driver-version"]; ok {
			t.Error("expected no GPU driver NodeSelector for filter with no resources, but found one")
		}
	}
}

func TestBuildStreamingDeployment_GPUNodeSelector_MultipleLabels(t *testing.T) {
	r := &PipelineInstanceReconciler{
		GPUNodeSelectorLabels: map[string]string{
			"cloud.google.com/gke-gpu-driver-version": expectedDriverVersion,
			"nvidia.com/present":                      "true",
		},
	}
	pi := makeMinimalStreamingPipelineInstance()
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

	deployment := r.buildStreamingDeployment(pi, pipeline, nil, "test-deployment")

	nodeSelector := deployment.Spec.Template.Spec.NodeSelector
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

func TestBuildStreamingDeployment_GPUNodeSelector_NilLabels(t *testing.T) {
	r := &PipelineInstanceReconciler{
		// GPUNodeSelectorLabels is nil (zero value)
	}
	pi := makeMinimalStreamingPipelineInstance()
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

	deployment := r.buildStreamingDeployment(pi, pipeline, nil, "test-deployment")

	if nodeSelector := deployment.Spec.Template.Spec.NodeSelector; nodeSelector != nil {
		if len(nodeSelector) != 0 {
			t.Errorf("expected empty NodeSelector when GPUNodeSelectorLabels is nil, got %v", nodeSelector)
		}
	}
}

func TestBuildStreamingDeployment_GPUNodeSelector_EmptyLabels(t *testing.T) {
	r := &PipelineInstanceReconciler{
		GPUNodeSelectorLabels: map[string]string{},
	}
	pi := makeMinimalStreamingPipelineInstance()
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

	deployment := r.buildStreamingDeployment(pi, pipeline, nil, "test-deployment")

	if nodeSelector := deployment.Spec.Template.Spec.NodeSelector; nodeSelector != nil {
		if len(nodeSelector) != 0 {
			t.Errorf("expected empty NodeSelector when GPUNodeSelectorLabels is empty, got %v", nodeSelector)
		}
	}
}

func TestBuildStreamingDeployment_GPUNodeSelector_DefensiveCopy(t *testing.T) {
	r := &PipelineInstanceReconciler{
		GPUNodeSelectorLabels: map[string]string{"cloud.google.com/gke-gpu-driver-version": expectedDriverVersion},
	}
	pi := makeMinimalStreamingPipelineInstance()
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

	deployment := r.buildStreamingDeployment(pi, pipeline, nil, "test-deployment")

	// Mutate the returned deployment's NodeSelector
	deployment.Spec.Template.Spec.NodeSelector["injected-key"] = "injected-value"
	deployment.Spec.Template.Spec.NodeSelector["cloud.google.com/gke-gpu-driver-version"] = "mutated"

	// The reconciler's shared map must be unaffected
	if got := r.GPUNodeSelectorLabels["cloud.google.com/gke-gpu-driver-version"]; got != expectedDriverVersion {
		t.Errorf("defensive copy broken: r.GPUNodeSelectorLabels[driver-version] = %q, want \"latest\"", got)
	}
	if _, ok := r.GPUNodeSelectorLabels["injected-key"]; ok {
		t.Error("defensive copy broken: injected-key appeared in r.GPUNodeSelectorLabels")
	}
}

func TestBuildStreamingDeployment_GPUNodeSelector_BothLimitsAndRequests(t *testing.T) {
	r := &PipelineInstanceReconciler{
		GPUNodeSelectorLabels: map[string]string{"cloud.google.com/gke-gpu-driver-version": expectedDriverVersion},
	}
	pi := makeMinimalStreamingPipelineInstance()
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

	deployment := r.buildStreamingDeployment(pi, pipeline, nil, "test-deployment")

	nodeSelector := deployment.Spec.Template.Spec.NodeSelector
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

func TestBuildStreamingDeployment_GPUEnvInjection_WithGPULimits(t *testing.T) {
	r := &PipelineInstanceReconciler{GPULibraryPath: DefaultGPULibraryPath, GPUBinPath: DefaultGPUBinPath}
	pi := makeMinimalStreamingPipelineInstance()
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

	deployment := r.buildStreamingDeployment(pi, pipeline, nil, "test-deployment")
	containers := deployment.Spec.Template.Spec.Containers
	if len(containers) == 0 {
		t.Fatal("expected at least one container")
	}
	env := containers[0].Env

	ldLibPath, ok := findEnvVar(env, appendLdLibraryPathEnvName)
	if !ok {
		t.Fatal("expected OPENFILTER_APPEND_LD_LIBRARY_PATH to be set for GPU container")
	}
	if ldLibPath.Value != DefaultGPULibraryPath {
		t.Errorf("expected OPENFILTER_APPEND_LD_LIBRARY_PATH=%q, got %q", DefaultGPULibraryPath, ldLibPath.Value)
	}

	pathVar, ok := findEnvVar(env, appendPathEnvName)
	if !ok {
		t.Fatal("expected OPENFILTER_APPEND_PATH to be set for GPU container")
	}
	if pathVar.Value != DefaultGPUBinPath {
		t.Errorf("expected OPENFILTER_APPEND_PATH=%q, got %q", DefaultGPUBinPath, pathVar.Value)
	}
}

func TestBuildStreamingDeployment_GPUEnvInjection_ZeroQuantityNotInjected(t *testing.T) {
	r := &PipelineInstanceReconciler{}
	pi := makeMinimalStreamingPipelineInstance()
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

	deployment := r.buildStreamingDeployment(pi, pipeline, nil, "test-deployment")
	env := deployment.Spec.Template.Spec.Containers[0].Env

	if _, ok := findEnvVar(env, appendLdLibraryPathEnvName); ok {
		t.Error("expected OPENFILTER_APPEND_LD_LIBRARY_PATH NOT to be injected for nvidia.com/gpu: 0")
	}
}

func TestBuildStreamingDeployment_GPUEnvInjection_NotInjectedForCPUContainer(t *testing.T) {
	r := &PipelineInstanceReconciler{}
	pi := makeMinimalStreamingPipelineInstance()
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

	deployment := r.buildStreamingDeployment(pi, pipeline, nil, "test-deployment")
	env := deployment.Spec.Template.Spec.Containers[0].Env

	if _, ok := findEnvVar(env, appendLdLibraryPathEnvName); ok {
		t.Error("expected OPENFILTER_APPEND_LD_LIBRARY_PATH NOT to be set for CPU-only container")
	}
}

func TestBuildStreamingDeployment_GPUEnvInjection_UserCanOverride(t *testing.T) {
	r := &PipelineInstanceReconciler{GPULibraryPath: DefaultGPULibraryPath}
	pi := makeMinimalStreamingPipelineInstance()

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
						{Name: appendLdLibraryPathEnvName, Value: userPath},
					},
				},
			},
		},
	}

	deployment := r.buildStreamingDeployment(pi, pipeline, nil, "test-deployment")
	env := deployment.Spec.Template.Spec.Containers[0].Env

	var ldLibVals []string
	for _, e := range env {
		if e.Name == appendLdLibraryPathEnvName {
			ldLibVals = append(ldLibVals, e.Value)
		}
	}
	if len(ldLibVals) != 2 {
		t.Fatalf("expected 2 OPENFILTER_APPEND_LD_LIBRARY_PATH entries (default + user override), got %d: %v", len(ldLibVals), ldLibVals)
	}
	if ldLibVals[0] != DefaultGPULibraryPath {
		t.Errorf("first OPENFILTER_APPEND_LD_LIBRARY_PATH should be default %q, got %q", DefaultGPULibraryPath, ldLibVals[0])
	}
	if ldLibVals[1] != userPath {
		t.Errorf("second OPENFILTER_APPEND_LD_LIBRARY_PATH should be user override %q, got %q", userPath, ldLibVals[1])
	}
}

func TestBuildStreamingDeployment_GPUEnvInjection_PerContainerNotPod(t *testing.T) {
	r := &PipelineInstanceReconciler{GPULibraryPath: DefaultGPULibraryPath}
	pi := makeMinimalStreamingPipelineInstance()
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

	deployment := r.buildStreamingDeployment(pi, pipeline, nil, "test-deployment")
	containers := deployment.Spec.Template.Spec.Containers

	if len(containers) < 2 {
		t.Fatalf("expected 2 containers, got %d", len(containers))
	}

	var gpuContainer, cpuContainer corev1.Container
	for _, c := range containers {
		switch c.Name {
		case "gpu-filter":
			gpuContainer = c
		case "cpu-sidecar":
			cpuContainer = c
		}
	}

	if _, ok := findEnvVar(gpuContainer.Env, appendLdLibraryPathEnvName); !ok {
		t.Error("expected OPENFILTER_APPEND_LD_LIBRARY_PATH on GPU container")
	}
	if _, ok := findEnvVar(cpuContainer.Env, appendLdLibraryPathEnvName); ok {
		t.Error("expected OPENFILTER_APPEND_LD_LIBRARY_PATH NOT on CPU sidecar")
	}
}

func TestBuildStreamingDeployment_GPUEnvInjection_NilResources(t *testing.T) {
	r := &PipelineInstanceReconciler{}
	pi := makeMinimalStreamingPipelineInstance()
	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{Name: "nil-resources-filter", Image: "filter:latest", Resources: nil},
			},
		},
	}

	deployment := r.buildStreamingDeployment(pi, pipeline, nil, "test-deployment")
	env := deployment.Spec.Template.Spec.Containers[0].Env
	if _, ok := findEnvVar(env, appendLdLibraryPathEnvName); ok {
		t.Error("expected OPENFILTER_APPEND_LD_LIBRARY_PATH NOT to be set for filter with nil Resources")
	}
}

func TestBuildStreamingDeployment_GPUEnvInjection_EmptyResourceRequirements(t *testing.T) {
	r := &PipelineInstanceReconciler{}
	pi := makeMinimalStreamingPipelineInstance()
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

	deployment := r.buildStreamingDeployment(pi, pipeline, nil, "test-deployment")
	env := deployment.Spec.Template.Spec.Containers[0].Env
	if _, ok := findEnvVar(env, appendLdLibraryPathEnvName); ok {
		t.Error("expected OPENFILTER_APPEND_LD_LIBRARY_PATH NOT to be set for filter with empty ResourceRequirements")
	}
}

func TestBuildStreamingDeployment_GPUEnvInjection_BothLimitsAndRequestsInjectsOnce(t *testing.T) {
	r := &PipelineInstanceReconciler{GPULibraryPath: DefaultGPULibraryPath}
	pi := makeMinimalStreamingPipelineInstance()
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

	deployment := r.buildStreamingDeployment(pi, pipeline, nil, "test-deployment")
	env := deployment.Spec.Template.Spec.Containers[0].Env

	var count int
	for _, e := range env {
		if e.Name == appendLdLibraryPathEnvName {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 OPENFILTER_APPEND_LD_LIBRARY_PATH entry when GPU is in both Limits and Requests, got %d", count)
	}
}

func TestBuildStreamingDeployment_GPUEnvInjection_ZeroLimitsNonZeroRequests(t *testing.T) {
	r := &PipelineInstanceReconciler{GPULibraryPath: DefaultGPULibraryPath}
	pi := makeMinimalStreamingPipelineInstance()
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

	deployment := r.buildStreamingDeployment(pi, pipeline, nil, "test-deployment")
	env := deployment.Spec.Template.Spec.Containers[0].Env
	if _, ok := findEnvVar(env, appendLdLibraryPathEnvName); !ok {
		t.Error("expected OPENFILTER_APPEND_LD_LIBRARY_PATH to be injected when Requests has positive GPU (Limits is zero)")
	}
}

func TestBuildStreamingDeployment_GPUEnvInjection_NegativeQuantityNotInjected(t *testing.T) {
	r := &PipelineInstanceReconciler{}
	pi := makeMinimalStreamingPipelineInstance()
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

	deployment := r.buildStreamingDeployment(pi, pipeline, nil, "test-deployment")
	env := deployment.Spec.Template.Spec.Containers[0].Env
	if _, ok := findEnvVar(env, appendLdLibraryPathEnvName); ok {
		t.Error("expected OPENFILTER_APPEND_LD_LIBRARY_PATH NOT to be injected for negative GPU quantity")
	}
}

func TestBuildStreamingDeployment_GPUEnvInjection_LargeGPUCount(t *testing.T) {
	r := &PipelineInstanceReconciler{GPULibraryPath: DefaultGPULibraryPath}
	pi := makeMinimalStreamingPipelineInstance()
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

	deployment := r.buildStreamingDeployment(pi, pipeline, nil, "test-deployment")
	env := deployment.Spec.Template.Spec.Containers[0].Env
	ldLib, ok := findEnvVar(env, appendLdLibraryPathEnvName)
	if !ok {
		t.Fatal("expected OPENFILTER_APPEND_LD_LIBRARY_PATH to be set for large GPU count")
	}
	if ldLib.Value != DefaultGPULibraryPath {
		t.Errorf("expected OPENFILTER_APPEND_LD_LIBRARY_PATH=%q, got %q", DefaultGPULibraryPath, ldLib.Value)
	}
}

func TestBuildStreamingDeployment_GPUEnvInjection_UserOverridesWithEmptyString(t *testing.T) {
	r := &PipelineInstanceReconciler{GPULibraryPath: DefaultGPULibraryPath}
	pi := makeMinimalStreamingPipelineInstance()
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
						{Name: appendLdLibraryPathEnvName, Value: ""},
					},
				},
			},
		},
	}

	deployment := r.buildStreamingDeployment(pi, pipeline, nil, "test-deployment")
	env := deployment.Spec.Template.Spec.Containers[0].Env

	var ldLibVals []string
	for _, e := range env {
		if e.Name == appendLdLibraryPathEnvName {
			ldLibVals = append(ldLibVals, e.Value)
		}
	}
	if len(ldLibVals) != 2 {
		t.Fatalf("expected 2 OPENFILTER_APPEND_LD_LIBRARY_PATH entries, got %d: %v", len(ldLibVals), ldLibVals)
	}
	if ldLibVals[0] != DefaultGPULibraryPath {
		t.Errorf("first entry should be default %q, got %q", DefaultGPULibraryPath, ldLibVals[0])
	}
	if ldLibVals[1] != "" {
		t.Errorf("second entry should be empty string (user override), got %q", ldLibVals[1])
	}
}

func TestBuildStreamingDeployment_PATHInjection_GPUContainerGetsPATH(t *testing.T) {
	r := &PipelineInstanceReconciler{GPULibraryPath: DefaultGPULibraryPath, GPUBinPath: DefaultGPUBinPath}
	pi := makeMinimalStreamingPipelineInstance()
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

	deployment := r.buildStreamingDeployment(pi, pipeline, nil, "test-deployment")
	env := deployment.Spec.Template.Spec.Containers[0].Env

	pathVar, ok := findEnvVar(env, appendPathEnvName)
	if !ok {
		t.Fatal("expected OPENFILTER_APPEND_PATH to be set for GPU container")
	}
	if pathVar.Value != DefaultGPUBinPath {
		t.Errorf("expected OPENFILTER_APPEND_PATH=%q, got %q", DefaultGPUBinPath, pathVar.Value)
	}
}

func TestBuildStreamingDeployment_PATHInjection_CPUContainerDoesNotGetPATH(t *testing.T) {
	r := &PipelineInstanceReconciler{GPUBinPath: DefaultGPUBinPath}
	pi := makeMinimalStreamingPipelineInstance()
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

	deployment := r.buildStreamingDeployment(pi, pipeline, nil, "test-deployment")
	env := deployment.Spec.Template.Spec.Containers[0].Env

	if _, ok := findEnvVar(env, appendPathEnvName); ok {
		t.Error("expected OPENFILTER_APPEND_PATH NOT to be set for CPU-only container")
	}
}

func TestBuildStreamingDeployment_PATHInjection_UserCanOverride(t *testing.T) {
	r := &PipelineInstanceReconciler{GPULibraryPath: DefaultGPULibraryPath, GPUBinPath: DefaultGPUBinPath}
	pi := makeMinimalStreamingPipelineInstance()

	userBinPath := "/custom/bin:/usr/bin:/bin"
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
						{Name: appendPathEnvName, Value: userBinPath},
					},
				},
			},
		},
	}

	deployment := r.buildStreamingDeployment(pi, pipeline, nil, "test-deployment")
	env := deployment.Spec.Template.Spec.Containers[0].Env

	var pathVals []string
	for _, e := range env {
		if e.Name == appendPathEnvName {
			pathVals = append(pathVals, e.Value)
		}
	}
	if len(pathVals) != 2 {
		t.Fatalf("expected 2 OPENFILTER_APPEND_PATH entries (default + user override), got %d: %v", len(pathVals), pathVals)
	}
	if pathVals[0] != DefaultGPUBinPath {
		t.Errorf("first OPENFILTER_APPEND_PATH should be default %q, got %q", DefaultGPUBinPath, pathVals[0])
	}
	if pathVals[1] != userBinPath {
		t.Errorf("second OPENFILTER_APPEND_PATH should be user override %q, got %q", userBinPath, pathVals[1])
	}
}

func TestBuildStreamingDeployment_PATHInjection_EmptyGPUBinPathSkipsInjection(t *testing.T) {
	r := &PipelineInstanceReconciler{GPULibraryPath: DefaultGPULibraryPath, GPUBinPath: ""}
	pi := makeMinimalStreamingPipelineInstance()
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

	deployment := r.buildStreamingDeployment(pi, pipeline, nil, "test-deployment")
	env := deployment.Spec.Template.Spec.Containers[0].Env

	if _, ok := findEnvVar(env, appendPathEnvName); ok {
		t.Error("expected OPENFILTER_APPEND_PATH NOT to be injected when GPUBinPath is empty")
	}
}

func TestBuildStreamingDeployment_ImagePullSecrets_None(t *testing.T) {
	r := &PipelineInstanceReconciler{}
	pi := makeMinimalStreamingPipelineInstance()
	pipeline := &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{
			Filters: []pipelinesv1alpha1.Filter{
				{Name: "f1", Image: "public/image:latest"},
			},
		},
	}

	deployment := r.buildStreamingDeployment(pi, pipeline, nil, "test-deployment")
	if len(deployment.Spec.Template.Spec.ImagePullSecrets) != 0 {
		t.Errorf("expected no ImagePullSecrets, got %v", deployment.Spec.Template.Spec.ImagePullSecrets)
	}
}

func TestBuildStreamingDeployment_ImagePullSecrets_Propagated(t *testing.T) {
	r := &PipelineInstanceReconciler{}
	pi := makeMinimalStreamingPipelineInstance()
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

	deployment := r.buildStreamingDeployment(pi, pipeline, nil, "test-deployment")
	secrets := deployment.Spec.Template.Spec.ImagePullSecrets
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

func TestBuildRTSPURLWithCredentials(t *testing.T) {
	tests := []struct {
		name     string
		rtsp     *pipelinesv1alpha1.RTSPSource
		expected string
	}{
		{
			name: "basic with default port",
			rtsp: &pipelinesv1alpha1.RTSPSource{
				Host: "camera.example.com",
				Path: "/stream1",
			},
			expected: "rtsp://$(_RTSP_USERNAME):$(_RTSP_PASSWORD)@camera.example.com:554/stream1",
		},
		{
			name: "custom port",
			rtsp: &pipelinesv1alpha1.RTSPSource{
				Host: "192.168.1.100",
				Port: 8554,
				Path: "/live",
			},
			expected: "rtsp://$(_RTSP_USERNAME):$(_RTSP_PASSWORD)@192.168.1.100:8554/live",
		},
		{
			name: "path without leading slash",
			rtsp: &pipelinesv1alpha1.RTSPSource{
				Host: "camera.local",
				Port: 554,
				Path: "stream",
			},
			expected: "rtsp://$(_RTSP_USERNAME):$(_RTSP_PASSWORD)@camera.local:554/stream",
		},
		{
			name: "empty path",
			rtsp: &pipelinesv1alpha1.RTSPSource{
				Host: "camera.local",
				Port: 554,
			},
			expected: "rtsp://$(_RTSP_USERNAME):$(_RTSP_PASSWORD)@camera.local:554",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildRTSPURLWithCredentials(tt.rtsp)
			if result != tt.expected {
				t.Errorf("buildRTSPURLWithCredentials() = %q, want %q", result, tt.expected)
			}
		})
	}
}
