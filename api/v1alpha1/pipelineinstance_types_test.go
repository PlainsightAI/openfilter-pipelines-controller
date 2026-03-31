package v1alpha1

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestGetQueueStream(t *testing.T) {
	pi := &PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-instance",
			Namespace: "team-alpha",
			UID:       types.UID("abc-def-ghi"),
		},
	}

	got := pi.GetQueueStream()
	expected := "ns:team-alpha:pi:abc-def-ghi:work"
	if got != expected {
		t.Errorf("GetQueueStream() = %q, want %q", got, expected)
	}
}

func TestGetQueueDLQ(t *testing.T) {
	pi := &PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-instance",
			Namespace: "team-alpha",
			UID:       types.UID("abc-def-ghi"),
		},
	}

	got := pi.GetQueueDLQ()
	expected := "ns:team-alpha:pi:abc-def-ghi:dlq"
	if got != expected {
		t.Errorf("GetQueueDLQ() = %q, want %q", got, expected)
	}
}

func TestGetQueueGroup(t *testing.T) {
	pi := &PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{
			UID: types.UID("abc-def-ghi"),
		},
	}

	got := pi.GetQueueGroup()
	expected := "cg:abc-def-ghi"
	if got != expected {
		t.Errorf("GetQueueGroup() = %q, want %q", got, expected)
	}
}

func TestGetQueueStream_NamespaceWithHyphens(t *testing.T) {
	pi := &PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "tenant-my-company-prod",
			UID:       types.UID("uid-1"),
		},
	}

	got := pi.GetQueueStream()
	expected := "ns:tenant-my-company-prod:pi:uid-1:work"
	if got != expected {
		t.Errorf("GetQueueStream() = %q, want %q", got, expected)
	}
}

func TestGetInstanceID(t *testing.T) {
	pi := &PipelineInstance{
		ObjectMeta: metav1.ObjectMeta{
			UID: types.UID("test-uid-123"),
		},
	}

	got := pi.GetInstanceID()
	if got != "test-uid-123" {
		t.Errorf("GetInstanceID() = %q, want %q", got, "test-uid-123")
	}
}
