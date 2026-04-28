/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package controller

import (
	"context"
	"fmt"
	"sync/atomic"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
)

// streamingUpdateCounter generates unique resource names per spec so this
// suite can run alongside the rest of the Ginkgo specs without collision.
var streamingUpdateCounter int32

var _ = Describe("PipelineInstance streaming update path", func() {
	Context("When ensureStreamingDeployment is called for an existing Deployment", func() {
		It("should converge outer Deployment.ObjectMeta.Labels when CR labels change", func() {
			ctx := context.Background()
			ns := "default"
			n := atomic.AddInt32(&streamingUpdateCounter, 1)
			pipelineName := fmt.Sprintf("update-pipeline-%d", n)
			sourceName := fmt.Sprintf("update-source-%d", n)
			instanceName := fmt.Sprintf("update-instance-%d", n)

			pipeline := &pipelinesv1alpha1.Pipeline{
				ObjectMeta: metav1.ObjectMeta{Name: pipelineName, Namespace: ns},
				Spec: pipelinesv1alpha1.PipelineSpec{
					Mode: pipelinesv1alpha1.PipelineModeStream,
					Filters: []pipelinesv1alpha1.Filter{
						{Name: "f", Image: "busybox:latest"},
					},
				},
			}
			Expect(k8sClient.Create(ctx, pipeline)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, pipeline) })

			source := &pipelinesv1alpha1.PipelineSource{
				ObjectMeta: metav1.ObjectMeta{Name: sourceName, Namespace: ns},
				Spec: pipelinesv1alpha1.PipelineSourceSpec{
					RTSP: &pipelinesv1alpha1.RTSPSource{
						Host: "rtsp", Port: 8554, Path: "/live",
					},
				},
			}
			Expect(k8sClient.Create(ctx, source)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, source) })

			pi := &pipelinesv1alpha1.PipelineInstance{
				ObjectMeta: metav1.ObjectMeta{
					Name:      instanceName,
					Namespace: ns,
					Labels: map[string]string{
						"plainsight.ai/pipeline-instance-id": "pi-OLD",
						"plainsight.ai/organization-id":      "org-OLD",
						"plainsight.ai/project-id":           "proj-OLD",
					},
				},
				Spec: pipelinesv1alpha1.PipelineInstanceSpec{
					PipelineRef: pipelinesv1alpha1.PipelineReference{Name: pipelineName},
					SourceRef:   pipelinesv1alpha1.SourceReference{Name: sourceName},
				},
			}
			Expect(k8sClient.Create(ctx, pi)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, pi) })

			r := &PipelineInstanceReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			Expect(r.ensureStreamingDeployment(ctx, pi, pipeline, source)).To(Succeed())

			depName := pi.Name + "-deploy"
			depKey := types.NamespacedName{Name: depName, Namespace: ns}

			dep := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, depKey, dep)).To(Succeed())
			Expect(dep.Labels).To(HaveKeyWithValue("plainsight.ai/organization-id", "org-OLD"))
			Expect(dep.Spec.Template.Labels).To(HaveKeyWithValue("plainsight.ai/organization-id", "org-OLD"))
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, dep) })

			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: instanceName, Namespace: ns}, pi)).To(Succeed())
			pi.Labels["plainsight.ai/pipeline-instance-id"] = "pi-NEW"
			pi.Labels["plainsight.ai/organization-id"] = "org-NEW"
			pi.Labels["plainsight.ai/project-id"] = "proj-NEW"
			Expect(k8sClient.Update(ctx, pi)).To(Succeed())

			Expect(r.ensureStreamingDeployment(ctx, pi, pipeline, source)).To(Succeed())

			Expect(k8sClient.Get(ctx, depKey, dep)).To(Succeed())

			for k, want := range map[string]string{
				"plainsight.ai/pipeline-instance-id": "pi-NEW",
				"plainsight.ai/organization-id":      "org-NEW",
				"plainsight.ai/project-id":           "proj-NEW",
			} {
				Expect(dep.Labels).To(
					HaveKeyWithValue(k, want),
					"Deployment.ObjectMeta.Labels[%q] did not converge — fires if ensureStreamingDeployment drops `deployment.Labels = desiredDeployment.Labels` from its update branch",
					k,
				)
				Expect(dep.Spec.Template.Labels).To(
					HaveKeyWithValue(k, want),
					"Deployment.Spec.Template.Labels[%q] did not converge", k,
				)
			}
		})
	})
})
