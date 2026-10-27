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

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
)

// builtFilterContainer returns the built container with the given name, failing the
// test when the deployment has none.
func builtFilterContainer(t *testing.T, pipeline *pipelinesv1alpha1.Pipeline, name string) *corev1.Container {
	t.Helper()
	r := &PipelineInstanceReconciler{}
	pi := makeMinimalStreamingPipelineInstance()
	deployment := r.buildStreamingDeployment(context.Background(), pi, pipeline, nil, "test-deployment")
	for i := range deployment.Spec.Template.Spec.Containers {
		if deployment.Spec.Template.Spec.Containers[i].Name == name {
			return &deployment.Spec.Template.Spec.Containers[i]
		}
	}
	t.Fatalf("container %q not found in built deployment", name)
	return nil
}

func pipelineWithServices(filters []string, services []pipelinesv1alpha1.ServicePort) *pipelinesv1alpha1.Pipeline {
	fs := make([]pipelinesv1alpha1.Filter, 0, len(filters))
	for _, name := range filters {
		fs = append(fs, pipelinesv1alpha1.Filter{Name: name, Image: name + ":latest"})
	}
	return &pipelinesv1alpha1.Pipeline{
		Spec: pipelinesv1alpha1.PipelineSpec{Filters: fs, Services: services},
	}
}

func TestReadinessProbe_ServiceExposingFilterGetsTCPProbe(t *testing.T) {
	pipeline := pipelineWithServices(
		[]string{"webvis"},
		[]pipelinesv1alpha1.ServicePort{{Name: "webvis", Port: 8000}},
	)

	c := builtFilterContainer(t, pipeline, "webvis")

	if c.ReadinessProbe == nil {
		t.Fatal("expected a readiness probe on a filter that exposes a Service, got nil")
	}
	if c.ReadinessProbe.TCPSocket == nil {
		t.Fatalf("expected a TCPSocket probe, got %+v", c.ReadinessProbe.ProbeHandler)
	}
	if got := c.ReadinessProbe.TCPSocket.Port.IntValue(); got != 8000 {
		t.Errorf("expected the probe on port 8000, got %d", got)
	}
	if c.ReadinessProbe.HTTPGet != nil {
		t.Error("probe must not be HTTP: filters are third-party and need not serve a readiness path")
	}
	if c.LivenessProbe != nil {
		t.Error("expected no liveness probe: a filter that stops listening should leave the endpoints, not be restarted")
	}
}

func TestReadinessProbe_UsesTargetPortNotServicePort(t *testing.T) {
	targetPort := int32(8090)
	pipeline := pipelineWithServices(
		[]string{"aggregator"},
		[]pipelinesv1alpha1.ServicePort{{Name: "aggregator", Port: 80, TargetPort: &targetPort}},
	)

	c := builtFilterContainer(t, pipeline, "aggregator")

	if c.ReadinessProbe == nil {
		t.Fatal("expected a readiness probe, got nil")
	}
	// Probing the Service port here would dial 80, which nothing in the
	// container listens on.
	if got := c.ReadinessProbe.TCPSocket.Port.IntValue(); got != 8090 {
		t.Errorf("expected the probe on the container port 8090, got %d", got)
	}
}

func TestReadinessProbe_FilterWithoutServiceHasNoProbe(t *testing.T) {
	pipeline := pipelineWithServices(
		[]string{"webvis", "detector"},
		[]pipelinesv1alpha1.ServicePort{{Name: "webvis", Port: 8000}},
	)

	if c := builtFilterContainer(t, pipeline, "detector"); c.ReadinessProbe != nil {
		t.Errorf("expected no probe on a filter that exposes nothing, got %+v", c.ReadinessProbe)
	}
	if c := builtFilterContainer(t, pipeline, "webvis"); c.ReadinessProbe == nil {
		t.Error("expected the exposing filter in the same pipeline to still get its probe")
	}
}

func TestReadinessProbe_MultipleServicesOnOneFilterUseTheFirst(t *testing.T) {
	pipeline := pipelineWithServices(
		[]string{"multi"},
		[]pipelinesv1alpha1.ServicePort{
			{Name: "multi", Port: 8000},
			{Name: "multi", Port: 9000},
		},
	)

	c := builtFilterContainer(t, pipeline, "multi")

	if c.ReadinessProbe == nil {
		t.Fatal("expected a readiness probe, got nil")
	}
	if got := c.ReadinessProbe.TCPSocket.Port.IntValue(); got != 8000 {
		t.Errorf("expected the first declared port 8000, got %d", got)
	}
}

func TestReadinessProbe_UDPServiceIsNotProbed(t *testing.T) {
	pipeline := pipelineWithServices(
		[]string{"udp-filter"},
		[]pipelinesv1alpha1.ServicePort{
			{Name: "udp-filter", Port: 5000, Protocol: corev1.ProtocolUDP},
		},
	)

	// Kubernetes has no UDP probe, and a TCP probe against a UDP port can
	// never succeed -- the pod would stay NotReady forever.
	if c := builtFilterContainer(t, pipeline, "udp-filter"); c.ReadinessProbe != nil {
		t.Errorf("expected no probe on a UDP-only service, got %+v", c.ReadinessProbe)
	}
}

func TestReadinessProbe_SkipsUDPAndProbesTheTCPPort(t *testing.T) {
	pipeline := pipelineWithServices(
		[]string{"mixed"},
		[]pipelinesv1alpha1.ServicePort{
			{Name: "mixed", Port: 5000, Protocol: corev1.ProtocolUDP},
			{Name: "mixed", Port: 8080, Protocol: corev1.ProtocolTCP},
		},
	)

	c := builtFilterContainer(t, pipeline, "mixed")

	if c.ReadinessProbe == nil {
		t.Fatal("expected the TCP service to still earn a probe, got nil")
	}
	// "first service wins" must not mean "first even if it is UDP".
	if got := c.ReadinessProbe.TCPSocket.Port.IntValue(); got != 8080 {
		t.Errorf("expected the probe on the TCP port 8080, got %d", got)
	}
}

func TestReadinessProbe_NoServicesLeavesEveryContainerAlone(t *testing.T) {
	pipeline := pipelineWithServices([]string{"a", "b"}, nil)

	for _, name := range []string{"a", "b"} {
		if c := builtFilterContainer(t, pipeline, name); c.ReadinessProbe != nil {
			t.Errorf("container %q: expected no probe when the pipeline declares no services", name)
		}
	}
}
