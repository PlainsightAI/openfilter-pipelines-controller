package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
)

func makeMinimalReconciler() *PipelineInstanceReconciler {
	return &PipelineInstanceReconciler{
		ClaimerImage: "claimer:latest",
		ValkeyAddr:   "valkey:6379",
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
