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
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
)

// These specs pin the Pipeline CRD's admission-time validation contract for
// filter imageVolumes (PLAT-1094) against a real API server: required
// name/image/mountPath, the absolute-path pattern on mountPath, the DNS-1123
// name pattern, and the listMapKey uniqueness of imageVolumes[].name.
// plainsight-api's export resolver (PLAT-1100) renders against these
// invariants holding at the cluster boundary, so a `make manifests`
// regeneration that drops or weakens any of them must fail here.
var _ = Describe("Pipeline CRD admission validation for imageVolumes", func() {
	var nameSeq int

	newPipeline := func(volumes ...pipelinesv1alpha1.FilterImageVolume) *pipelinesv1alpha1.Pipeline {
		nameSeq++
		return &pipelinesv1alpha1.Pipeline{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("imagevolume-admission-test-%d", nameSeq),
				Namespace: "default",
			},
			Spec: pipelinesv1alpha1.PipelineSpec{
				Filters: []pipelinesv1alpha1.Filter{
					{
						Name:         "my-filter",
						Image:        "registry.example.com/filter:v1",
						ImageVolumes: volumes,
					},
				},
			},
		}
	}

	volume := func(mutate func(*pipelinesv1alpha1.FilterImageVolume)) pipelinesv1alpha1.FilterImageVolume {
		v := pipelinesv1alpha1.FilterImageVolume{
			Name:      "model",
			Image:     "registry.example.com/model@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			MountPath: "/opt/model",
		}
		if mutate != nil {
			mutate(&v)
		}
		return v
	}

	mustReject := func(p *pipelinesv1alpha1.Pipeline, fragment string) {
		GinkgoHelper()
		err := k8sClient.Create(ctx, p)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(fragment))
	}

	mustAccept := func(p *pipelinesv1alpha1.Pipeline) {
		GinkgoHelper()
		Expect(k8sClient.Create(ctx, p)).To(Succeed())
		Expect(k8sClient.Delete(ctx, p)).To(Succeed())
	}

	Context("additive contract", func() {
		It("accepts a filter without imageVolumes", func() {
			mustAccept(newPipeline())
		})

		It("accepts a valid trained-model volume", func() {
			mustAccept(newPipeline(volume(nil)))
		})

		It("accepts a BYO volume with pullSecret, subPath, and pullPolicy", func() {
			mustAccept(newPipeline(volume(func(v *pipelinesv1alpha1.FilterImageVolume) {
				v.Image = "registry.example.com/byo-model:v3"
				v.PullSecret = "my-registry-secret"
				v.SubPath = "models/latest"
				v.PullPolicy = corev1.PullAlways
			})))
		})

		It("defaults pullPolicy to IfNotPresent", func() {
			p := newPipeline(volume(nil))
			Expect(k8sClient.Create(ctx, p)).To(Succeed())
			defer func() { Expect(k8sClient.Delete(ctx, p)).To(Succeed()) }()

			fetched := &pipelinesv1alpha1.Pipeline{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: p.Name, Namespace: p.Namespace}, fetched)).To(Succeed())
			Expect(fetched.Spec.Filters[0].ImageVolumes[0].PullPolicy).To(Equal(corev1.PullIfNotPresent))
		})
	})

	Context("required fields", func() {
		It("rejects an empty name", func() {
			mustReject(newPipeline(volume(func(v *pipelinesv1alpha1.FilterImageVolume) {
				v.Name = ""
			})), "imageVolumes[0].name")
		})

		It("rejects an empty image", func() {
			mustReject(newPipeline(volume(func(v *pipelinesv1alpha1.FilterImageVolume) {
				v.Image = ""
			})), "imageVolumes[0].image")
		})

		It("rejects an empty mountPath", func() {
			mustReject(newPipeline(volume(func(v *pipelinesv1alpha1.FilterImageVolume) {
				v.MountPath = ""
			})), "imageVolumes[0].mountPath")
		})
	})

	Context("mountPath absolute-path pattern", func() {
		It("rejects a relative path", func() {
			mustReject(newPipeline(volume(func(v *pipelinesv1alpha1.FilterImageVolume) {
				v.MountPath = "opt/model"
			})), "imageVolumes[0].mountPath")
		})
	})

	Context("name DNS-1123 pattern", func() {
		It("rejects underscores", func() {
			mustReject(newPipeline(volume(func(v *pipelinesv1alpha1.FilterImageVolume) {
				v.Name = "my_model"
			})), "imageVolumes[0].name")
		})

		It("rejects uppercase", func() {
			mustReject(newPipeline(volume(func(v *pipelinesv1alpha1.FilterImageVolume) {
				v.Name = "MyModel"
			})), "imageVolumes[0].name")
		})
	})

	Context("listMapKey uniqueness on name", func() {
		It("rejects two volumes with the same name", func() {
			mustReject(newPipeline(
				volume(nil),
				volume(func(v *pipelinesv1alpha1.FilterImageVolume) {
					v.MountPath = "/opt/other"
				}),
			), "Duplicate value")
		})

		It("accepts two volumes with distinct names sharing an image", func() {
			mustAccept(newPipeline(
				volume(nil),
				volume(func(v *pipelinesv1alpha1.FilterImageVolume) {
					v.Name = "model-copy"
					v.MountPath = "/opt/model-copy"
				}),
			))
		})
	})
})
