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
		GPUNodeSelectorLabels: map[string]string{"cloud.google.com/gke-gpu-driver-version": "latest"},
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
	if got != "latest" {
		t.Errorf("expected NodeSelector[cloud.google.com/gke-gpu-driver-version]=latest, got %q", got)
	}
}

func TestBuildStreamingDeployment_GPUNodeSelector_WithGPURequests(t *testing.T) {
	r := &PipelineInstanceReconciler{
		GPUNodeSelectorLabels: map[string]string{"cloud.google.com/gke-gpu-driver-version": "latest"},
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
	if got != "latest" {
		t.Errorf("expected NodeSelector[cloud.google.com/gke-gpu-driver-version]=latest, got %q", got)
	}
}

func TestBuildStreamingDeployment_GPUNodeSelector_WithoutGPU(t *testing.T) {
	r := &PipelineInstanceReconciler{
		GPUNodeSelectorLabels: map[string]string{"cloud.google.com/gke-gpu-driver-version": "latest"},
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
		GPUNodeSelectorLabels: map[string]string{"cloud.google.com/gke-gpu-driver-version": "latest"},
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
