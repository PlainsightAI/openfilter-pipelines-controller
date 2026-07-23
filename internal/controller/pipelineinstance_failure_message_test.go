package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// TestFailedContainerMessages pins that a failed batch pipeline surfaces the
// real container error (the claimer's download failure or a filter crash) rather
// than only the Job-level "backoff limit" — the fix behind PLAT-1353.
func TestFailedContainerMessages(t *testing.T) {
	sch := reconcileSpanScheme(t)
	pi := makeMinimalPipelineInstance() // UID "test-uid-1234", namespace "default"

	failedPod := func(name string, init, main []corev1.ContainerStatus) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: pi.Namespace,
				Labels:    map[string]string{"filter.plainsight.ai/instance": string(pi.UID)},
			},
			Status: corev1.PodStatus{InitContainerStatuses: init, ContainerStatuses: main},
		}
	}
	term := func(exit int32, msg, reason string) corev1.ContainerState {
		return corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: exit, Message: msg, Reason: reason}}
	}
	newR := func(pod *corev1.Pod) *PipelineInstanceReconciler {
		cli := fake.NewClientBuilder().WithScheme(sch).WithObjects(pi, pod).Build()
		return &PipelineInstanceReconciler{Client: cli, Scheme: sch}
	}

	t.Run("surfaces the claimer's real error from the init container", func(t *testing.T) {
		pod := failedPod("pi-job-a",
			[]corev1.ContainerStatus{{Name: "claimer-video-in", State: term(1, "Claimer failed: failed to download file: The specified bucket does not exist", "Error")}},
			[]corev1.ContainerStatus{{Name: "video-in", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}}}},
		)
		got := newR(pod).failedContainerMessages(context.Background(), pi)
		if !strings.Contains(got, "claimer-video-in:") || !strings.Contains(got, "The specified bucket does not exist") {
			t.Errorf("expected the claimer's real error, got %q", got)
		}
	})

	t.Run("falls back to the reason when the message is empty", func(t *testing.T) {
		pod := failedPod("pi-job-b", nil,
			[]corev1.ContainerStatus{{Name: "gpu-filter", State: term(137, "", "OOMKilled")}},
		)
		got := newR(pod).failedContainerMessages(context.Background(), pi)
		if !strings.Contains(got, "gpu-filter: OOMKilled") {
			t.Errorf("expected the OOMKilled reason, got %q", got)
		}
	})

	t.Run("ignores containers that terminated successfully", func(t *testing.T) {
		pod := failedPod("pi-job-c",
			[]corev1.ContainerStatus{{Name: "claimer", State: term(0, "", "Completed")}},
			nil,
		)
		if got := newR(pod).failedContainerMessages(context.Background(), pi); got != "" {
			t.Errorf("expected empty for a successful container, got %q", got)
		}
	})

	t.Run("bounds the summary so it stays under the condition.message CRD cap", func(t *testing.T) {
		// A wide pipeline: many containers, each failing with a distinct multi-KiB
		// FallbackToLogsOnError tail. Unbounded, the join would exceed the CRD's
		// maxLength: 32768 and wedge Status().Update. It must stay well under.
		statuses := make([]corev1.ContainerStatus, 0, 64)
		for i := range 64 {
			// Distinct per container (so de-dup can't collapse them) and multi-line
			// (so the whitespace-collapse path is exercised), ~4KiB each.
			msg := fmt.Sprintf("container-%d error:\n%s", i, strings.Repeat("x\t", 2048))
			statuses = append(statuses, corev1.ContainerStatus{
				Name:  fmt.Sprintf("filter-%d", i),
				State: term(1, msg, "Error"),
			})
		}
		got := newR(failedPod("pi-job-wide", nil, statuses)).failedContainerMessages(context.Background(), pi)

		if len([]rune(got)) >= 32768 {
			t.Errorf("summary must stay under the 32768 CRD cap, got %d runes", len([]rune(got)))
		}
		if !strings.Contains(got, "more)") {
			t.Errorf("expected a truncation marker when containers are dropped, got %q", got)
		}
		if strings.ContainsAny(got, "\n\t") {
			t.Errorf("internal newlines/tabs must be collapsed, got %q", got)
		}
	})
}
