package queue

import "testing"

func TestValkeyUsernameForNamespace(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		expected  string
	}{
		{"standard", "team-alpha", "ns-team-alpha"},
		{"default-ns", "default", "ns-default"},
		{"long-name", "tenant-my-company-prod", "ns-tenant-my-company-prod"},
		{"empty", "", "ns-"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ValkeyUsernameForNamespace(tt.namespace)
			if got != tt.expected {
				t.Errorf("ValkeyUsernameForNamespace(%q) = %q, want %q", tt.namespace, got, tt.expected)
			}
		})
	}
}
