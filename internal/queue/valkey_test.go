package queue

import "testing"

func TestValkeyUsernameForNamespace(t *testing.T) {
	tests := []struct {
		namespace string
		expected  string
	}{
		{"org-123", "ns-org-123"},
		{"default", "ns-default"},
		{"org-my-company-prod", "ns-org-my-company-prod"},
		{"", "ns-"},
	}

	for _, tt := range tests {
		t.Run(tt.namespace, func(t *testing.T) {
			got := ValkeyUsernameForNamespace(tt.namespace)
			if got != tt.expected {
				t.Errorf("ValkeyUsernameForNamespace(%q) = %q, want %q", tt.namespace, got, tt.expected)
			}
		})
	}
}
