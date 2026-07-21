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
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
)

// These specs pin the PLAT-1096 rejection contract: a PipelineInstance whose
// Pipeline declares filter imageVolumes on a cluster that cannot serve the
// image volume source is failed at reconcile time with an explicit
// Degraded/UnsupportedClusterVersion condition and a Warning Event, and no
// workload is created. The deployment-agent's provisioning monitor reads the
// condition; the Event is the cluster-side copy for kubectl/k9s operators.
var _ = Describe("PipelineInstance reconcile on a cluster without image volume support", func() {
	const (
		namespace         = "default"
		unsupportedReason = "the cluster does not support the image volume source: Kubernetes 1.33.0+ required (alpha in 1.31-1.32 behind the ImageVolume feature gate), detected server version v1.30.0"
	)

	ctx := context.Background()

	var (
		nameSeq          int
		pipeline         *pipelinesv1alpha1.Pipeline
		pipelineSource   *pipelinesv1alpha1.PipelineSource
		pipelineInstance *pipelinesv1alpha1.PipelineInstance
		recorder         *record.FakeRecorder
		reconciler       *PipelineInstanceReconciler
	)

	reconcileTwice := func() {
		// First pass adds the Valkey-credentials finalizer and requeues;
		// second pass reaches the image-volume support check.
		GinkgoHelper()
		req := reconcile.Request{NamespacedName: types.NamespacedName{
			Name: pipelineInstance.Name, Namespace: namespace,
		}}
		_, err := reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
		_, err = reconciler.Reconcile(ctx, req)
		Expect(err).NotTo(HaveOccurred())
	}

	refreshInstance := func() {
		GinkgoHelper()
		Expect(k8sClient.Get(ctx, types.NamespacedName{
			Name: pipelineInstance.Name, Namespace: namespace,
		}, pipelineInstance)).To(Succeed())
	}

	BeforeEach(func() {
		nameSeq++
		pipeline = &pipelinesv1alpha1.Pipeline{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("unsupported-cluster-pipeline-%d", nameSeq),
				Namespace: namespace,
			},
			Spec: pipelinesv1alpha1.PipelineSpec{
				Filters: []pipelinesv1alpha1.Filter{
					{
						Name:  "detector",
						Image: "detector:v1",
						ImageVolumes: []pipelinesv1alpha1.FilterImageVolume{
							{Name: "model", Image: testModelImage, MountPath: "/opt/model"},
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, pipeline)).To(Succeed())

		pipelineSource = &pipelinesv1alpha1.PipelineSource{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("unsupported-cluster-source-%d", nameSeq),
				Namespace: namespace,
			},
			Spec: pipelinesv1alpha1.PipelineSourceSpec{
				Bucket: &pipelinesv1alpha1.BucketSource{
					Name:     "test-bucket",
					Endpoint: "http://minio:9000",
					Region:   "us-east-1",
				},
			},
		}
		Expect(k8sClient.Create(ctx, pipelineSource)).To(Succeed())

		pipelineInstance = &pipelinesv1alpha1.PipelineInstance{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("unsupported-cluster-instance-%d", nameSeq),
				Namespace: namespace,
			},
			Spec: pipelinesv1alpha1.PipelineInstanceSpec{
				PipelineRef: pipelinesv1alpha1.PipelineReference{Name: pipeline.Name},
				SourceRef:   &pipelinesv1alpha1.SourceReference{Name: pipelineSource.Name},
			},
		}
		Expect(k8sClient.Create(ctx, pipelineInstance)).To(Succeed())

		recorder = record.NewFakeRecorder(16)
		reconciler = &PipelineInstanceReconciler{
			Client:                        k8sClient,
			Scheme:                        k8sClient.Scheme(),
			ValkeyClient:                  &MockValkeyClient{},
			ValkeyAddr:                    "valkey:6379",
			ClaimerImage:                  "claimer:latest",
			ImageVolumesUnsupportedReason: unsupportedReason,
			Recorder:                      recorder,
		}
	})

	AfterEach(func() {
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, pipelineInstance))).To(Succeed())
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, pipeline))).To(Succeed())
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, pipelineSource))).To(Succeed())
	})

	It("rejects with Degraded/UnsupportedClusterVersion, emits a Warning Event, and creates no Job", func() {
		reconcileTwice()
		refreshInstance()

		degraded := meta.FindStatusCondition(pipelineInstance.Status.Conditions, ConditionTypeDegraded)
		Expect(degraded).NotTo(BeNil())
		Expect(degraded.Status).To(Equal(metav1.ConditionTrue))
		Expect(degraded.Reason).To(Equal(ReasonUnsupportedClusterVersion))
		Expect(degraded.Message).To(ContainSubstring("v1.30.0"))

		progressing := meta.FindStatusCondition(pipelineInstance.Status.Conditions, ConditionTypeProgressing)
		Expect(progressing).NotTo(BeNil())
		Expect(progressing.Status).To(Equal(metav1.ConditionFalse))

		var event string
		Eventually(recorder.Events).Should(Receive(&event))
		Expect(event).To(ContainSubstring(ReasonUnsupportedClusterVersion))

		jobs := &batchv1.JobList{}
		Expect(k8sClient.List(ctx, jobs, client.InNamespace(namespace))).To(Succeed())
		for _, job := range jobs.Items {
			for _, owner := range job.OwnerReferences {
				Expect(owner.UID).NotTo(Equal(pipelineInstance.UID),
					"no Job may be created for a rejected instance")
			}
		}
	})

	It("does not reject a pipeline without imageVolumes", func() {
		plain := &pipelinesv1alpha1.Pipeline{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("unsupported-cluster-plain-%d", nameSeq),
				Namespace: namespace,
			},
			Spec: pipelinesv1alpha1.PipelineSpec{
				Filters: []pipelinesv1alpha1.Filter{{Name: "detector", Image: "detector:v1"}},
			},
		}
		Expect(k8sClient.Create(ctx, plain)).To(Succeed())
		DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, plain))).To(Succeed()) })

		pipelineInstance.Spec.PipelineRef.Name = plain.Name
		Expect(k8sClient.Update(ctx, pipelineInstance)).To(Succeed())

		req := reconcile.Request{NamespacedName: types.NamespacedName{
			Name: pipelineInstance.Name, Namespace: namespace,
		}}
		// Reconcile proceeds into the batch path, whose own outcome is out of
		// scope here — only the absence of the rejection condition matters.
		_, _ = reconciler.Reconcile(ctx, req)
		_, _ = reconciler.Reconcile(ctx, req)
		refreshInstance()

		degraded := meta.FindStatusCondition(pipelineInstance.Status.Conditions, ConditionTypeDegraded)
		if degraded != nil {
			Expect(degraded.Reason).NotTo(Equal(ReasonUnsupportedClusterVersion))
		}
	})

	It("clears the condition after the probe reports support again", func() {
		reconcileTwice()
		refreshInstance()
		Expect(meta.FindStatusCondition(pipelineInstance.Status.Conditions, ConditionTypeDegraded).Reason).
			To(Equal(ReasonUnsupportedClusterVersion))

		// A controller restart on an upgraded cluster re-probes and comes up
		// with an empty reason.
		reconciler.ImageVolumesUnsupportedReason = ""
		req := reconcile.Request{NamespacedName: types.NamespacedName{
			Name: pipelineInstance.Name, Namespace: namespace,
		}}
		_, _ = reconciler.Reconcile(ctx, req)
		refreshInstance()

		degraded := meta.FindStatusCondition(pipelineInstance.Status.Conditions, ConditionTypeDegraded)
		if degraded != nil {
			Expect(degraded.Reason).NotTo(Equal(ReasonUnsupportedClusterVersion))
		}
	})
})
