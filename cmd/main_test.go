package main

import (
	"reflect"
	"strings"
	"testing"
)

func TestValidateTelemetryFlags(t *testing.T) {
	tests := []struct {
		name         string
		exporterType string
		endpoint     string
		wantErr      bool
	}{
		{
			name:         "both empty disables injection — OK",
			exporterType: "",
			endpoint:     "",
			wantErr:      false,
		},
		{
			name:         "both set enables injection — OK",
			exporterType: "otlp",
			endpoint:     "otel-collector.monitoring.svc.cluster.local:4317",
			wantErr:      false,
		},
		{
			name:         "only type set is rejected (half-configured)",
			exporterType: "otlp",
			endpoint:     "",
			wantErr:      true,
		},
		{
			name:         "only endpoint set is rejected (half-configured)",
			exporterType: "",
			endpoint:     "otel-collector.monitoring.svc.cluster.local:4317",
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTelemetryFlags(tt.exporterType, tt.endpoint)
			if tt.wantErr && err == nil {
				t.Errorf("validateTelemetryFlags(%q, %q) expected error, got nil", tt.exporterType, tt.endpoint)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("validateTelemetryFlags(%q, %q) unexpected error: %v", tt.exporterType, tt.endpoint, err)
			}
			if tt.wantErr && err != nil {
				// Ensure error mentions both flag names so operators can find the misconfig fast.
				msg := err.Error()
				if !strings.Contains(msg, "telemetry-exporter-type") || !strings.Contains(msg, "telemetry-exporter-otlp-endpoint") {
					t.Errorf("expected error message to mention both flag names, got: %q", msg)
				}
			}
		})
	}
}

func TestParseNodeSelectorLabels(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected map[string]string
	}{
		{
			name:     "empty string returns nil",
			input:    "",
			expected: nil,
		},
		{
			name:     "single pair",
			input:    "key=value",
			expected: map[string]string{"key": "value"},
		},
		{
			name:     "multiple pairs",
			input:    "cloud.google.com/gke-gpu-driver-version=latest,nvidia.com/gpu=true",
			expected: map[string]string{"cloud.google.com/gke-gpu-driver-version": "latest", "nvidia.com/gpu": "true"},
		},
		{
			name:     "whitespace around pair is trimmed",
			input:    "  key=value  ",
			expected: map[string]string{"key": "value"},
		},
		{
			name:     "whitespace around multiple pairs is trimmed",
			input:    " k1=v1 , k2=v2 ",
			expected: map[string]string{"k1": "v1", "k2": "v2"},
		},
		{
			name:     "pair missing = is skipped",
			input:    "invalidpair",
			expected: nil,
		},
		{
			name:     "pair with empty key is skipped",
			input:    "=value",
			expected: nil,
		},
		{
			name:     "all invalid pairs returns nil",
			input:    "noequalssign,=emptykey",
			expected: nil,
		},
		{
			name:     "mix of valid and invalid pairs keeps valid ones",
			input:    "valid=yes,noequalssign",
			expected: map[string]string{"valid": "yes"},
		},
		{
			name:     "value with = in it is preserved",
			input:    "key=val=ue",
			expected: map[string]string{"key": "val=ue"},
		},
		{
			name:     "empty value is valid",
			input:    "key=",
			expected: map[string]string{"key": ""},
		},
		{
			name:     "only whitespace returns nil",
			input:    "   ",
			expected: nil,
		},
		{
			name:     "trailing comma keeps valid pair",
			input:    "key=value,",
			expected: map[string]string{"key": "value"},
		},
		{
			name:     "leading comma keeps valid pair",
			input:    ",key=value",
			expected: map[string]string{"key": "value"},
		},
		{
			name:     "double comma keeps surrounding pairs",
			input:    "a=1,,b=2",
			expected: map[string]string{"a": "1", "b": "2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseNodeSelectorLabels(tt.input)
			if !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("parseNodeSelectorLabels(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}
