package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// TestStreamingPodFailure pins that a broken streaming pipeline pod surfaces as a
// Degraded message rather than a perpetual "Running"/"Starting" — the fix behind
// PLAT-1254 (a misconfigured filter that CrashLoopBackOffs, or a container that
// crashed while the pod stays Running).
func TestStreamingPodFailure(t *testing.T) {
	const crashLoopBackOff = "CrashLoopBackOff"
	sch := reconcileSpanScheme(t)
	pi := makeMinimalPipelineInstance() // Name "test-instance", namespace "default"

	// streaming pods are selected by the `pipelineinstance: <name>` label.
	pod := func(name string, phase corev1.PodPhase, statuses ...corev1.ContainerStatus) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: pi.Namespace,
				Labels:    map[string]string{"pipelineinstance": pi.Name},
			},
			Status: corev1.PodStatus{Phase: phase, ContainerStatuses: statuses},
		}
	}
	newR := func(objs ...client.Object) *PipelineInstanceReconciler {
		all := append([]client.Object{pi}, objs...)
		cli := fake.NewClientBuilder().WithScheme(sch).WithObjects(all...).Build()
		return &PipelineInstanceReconciler{Client: cli, Scheme: sch}
	}

	t.Run("surfaces a CrashLoopBackOff filter with its real error", func(t *testing.T) {
		crashing := corev1.ContainerStatus{
			Name:  "huggingface-vision",
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: crashLoopBackOff}},
			LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode: 1, Reason: "Error",
				Message: "ValueError: text_labels must be a nested list [['person']]",
			}},
		}
		msg, failed := newR(pod("test-instance-abc", corev1.PodRunning, crashing)).
			streamingPodFailure(context.Background(), pi)
		if !failed {
			t.Fatalf("expected a failure, got healthy")
		}
		if !strings.Contains(msg, "huggingface-vision") || !strings.Contains(msg, "crashlooped") {
			t.Errorf("expected the crashlooping filter in the message, got %q", msg)
		}
		if !strings.Contains(msg, "text_labels must be a nested list") {
			t.Errorf("expected the real error tail in the message, got %q", msg)
		}
	})

	t.Run("surfaces a container that crashed while the pod stays Running", func(t *testing.T) {
		crashed := corev1.ContainerStatus{
			Name: "video-out",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode: 137, Reason: "OOMKilled",
			}},
		}
		alive := corev1.ContainerStatus{Name: "video-in", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}
		msg, failed := newR(pod("test-instance-def", corev1.PodRunning, crashed, alive)).
			streamingPodFailure(context.Background(), pi)
		if !failed {
			t.Fatalf("expected a failure for a crashed container in a running pod, got healthy")
		}
		if !strings.Contains(msg, "video-out") {
			t.Errorf("expected the crashed container in the message, got %q", msg)
		}
	})

	t.Run("healthy pods report no failure", func(t *testing.T) {
		ready := corev1.ContainerStatus{Name: "video-in", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}
		if _, failed := newR(pod("test-instance-ok", corev1.PodRunning, ready)).
			streamingPodFailure(context.Background(), pi); failed {
			t.Errorf("expected healthy for a running ready pod")
		}
	})

	t.Run("ignores pods that do not belong to this instance", func(t *testing.T) {
		other := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "someone-else",
				Namespace: pi.Namespace,
				Labels:    map[string]string{"pipelineinstance": "other-instance"},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{
				Name:  "boom",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: crashLoopBackOff}},
			}}},
		}
		if _, failed := newR(other).streamingPodFailure(context.Background(), pi); failed {
			t.Errorf("a crashing pod from another instance must not count")
		}
	})
}
