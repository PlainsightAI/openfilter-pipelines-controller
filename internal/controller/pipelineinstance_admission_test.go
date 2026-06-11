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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
)

// These specs pin the PipelineInstance CRD's admission-time validation
// contract (PLAT-1071) against a real API server: the CEL xor rule between
// `sourceRef` and `sources`, the listMapKey uniqueness of
// `sources[].filterName`, and the DNS-1123 filterName pattern. Both the
// deployment agent (plainsight-deployment-agent#65) and the portal
// (client-portal#429) build on these invariants holding at the cluster
// boundary, so a `make manifests` regeneration that drops or weakens any
// of them must fail here.
var _ = Describe("PipelineInstance CRD admission validation", func() {
	var nameSeq int

	newPI := func(mutate func(*pipelinesv1alpha1.PipelineInstanceSpec)) *pipelinesv1alpha1.PipelineInstance {
		nameSeq++
		pi := &pipelinesv1alpha1.PipelineInstance{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("admission-test-%d", nameSeq),
				Namespace: "default",
			},
			Spec: pipelinesv1alpha1.PipelineInstanceSpec{
				PipelineRef: pipelinesv1alpha1.PipelineReference{Name: "some-pipeline"},
			},
		}
		mutate(&pi.Spec)
		return pi
	}

	mustReject := func(pi *pipelinesv1alpha1.PipelineInstance, fragment string) {
		GinkgoHelper()
		err := k8sClient.Create(ctx, pi)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(fragment))
	}

	mustAccept := func(pi *pipelinesv1alpha1.PipelineInstance) {
		GinkgoHelper()
		Expect(k8sClient.Create(ctx, pi)).To(Succeed())
		Expect(k8sClient.Delete(ctx, pi)).To(Succeed())
	}

	namedSource := func(filterName, sourceName string) pipelinesv1alpha1.NamedSourceRef {
		return pipelinesv1alpha1.NamedSourceRef{
			FilterName: filterName,
			SourceRef:  pipelinesv1alpha1.SourceReference{Name: sourceName},
		}
	}

	Context("xor rule between sourceRef and sources", func() {
		It("accepts the singular sourceRef form", func() {
			mustAccept(newPI(func(s *pipelinesv1alpha1.PipelineInstanceSpec) {
				s.SourceRef = &pipelinesv1alpha1.SourceReference{Name: "single-source"}
			}))
		})

		It("accepts the plural sources form", func() {
			mustAccept(newPI(func(s *pipelinesv1alpha1.PipelineInstanceSpec) {
				s.Sources = []pipelinesv1alpha1.NamedSourceRef{
					namedSource("front-cam", "src-front"),
					namedSource("back-cam", "src-back"),
				}
			}))
		})

		It("rejects setting both sourceRef and sources", func() {
			mustReject(newPI(func(s *pipelinesv1alpha1.PipelineInstanceSpec) {
				s.SourceRef = &pipelinesv1alpha1.SourceReference{Name: "single-source"}
				s.Sources = []pipelinesv1alpha1.NamedSourceRef{namedSource("front-cam", "src-front")}
			}), "exactly one of")
		})

		It("rejects setting neither sourceRef nor sources", func() {
			mustReject(newPI(func(s *pipelinesv1alpha1.PipelineInstanceSpec) {}), "exactly one of")
		})

		It("rejects an empty sources list", func() {
			mustReject(newPI(func(s *pipelinesv1alpha1.PipelineInstanceSpec) {
				s.Sources = []pipelinesv1alpha1.NamedSourceRef{}
			}), "exactly one of")
		})
	})

	Context("sources listMapKey uniqueness on filterName", func() {
		It("rejects two entries binding the same filterName", func() {
			mustReject(newPI(func(s *pipelinesv1alpha1.PipelineInstanceSpec) {
				s.Sources = []pipelinesv1alpha1.NamedSourceRef{
					namedSource("front-cam", "src-a"),
					namedSource("front-cam", "src-b"),
				}
			}), "Duplicate value")
		})

		It("accepts distinct filterNames binding the same source", func() {
			// The same media bound to two VideoIns is allowed (useful for
			// N-camera topologies driven by one test source).
			mustAccept(newPI(func(s *pipelinesv1alpha1.PipelineInstanceSpec) {
				s.Sources = []pipelinesv1alpha1.NamedSourceRef{
					namedSource("front-cam", "shared-src"),
					namedSource("back-cam", "shared-src"),
				}
			}))
		})
	})

	Context("filterName DNS-1123 pattern", func() {
		reject := func(name string) {
			GinkgoHelper()
			mustReject(newPI(func(s *pipelinesv1alpha1.PipelineInstanceSpec) {
				s.Sources = []pipelinesv1alpha1.NamedSourceRef{namedSource(name, "src")}
			}), "filterName")
		}
		accept := func(name string) {
			GinkgoHelper()
			mustAccept(newPI(func(s *pipelinesv1alpha1.PipelineInstanceSpec) {
				s.Sources = []pipelinesv1alpha1.NamedSourceRef{namedSource(name, "src")}
			}))
		}

		It("rejects underscores", func() { reject("front_cam") })
		It("rejects uppercase", func() { reject("FrontCam") })
		It("rejects a leading hyphen", func() { reject("-front") })
		It("rejects a trailing hyphen", func() { reject("front-") })
		It("rejects the empty string", func() { reject("") })
		It("rejects 64 characters", func() { reject(strings.Repeat("a", 64)) })

		It("accepts a single character", func() { accept("a") })
		It("accepts 63 characters", func() { accept(strings.Repeat("a", 63)) })
		It("accepts interior hyphens and digits", func() { accept("cam-2-front") })
	})
})
