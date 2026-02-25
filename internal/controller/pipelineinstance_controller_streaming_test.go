package controller

import (
	"testing"

	pipelinesv1alpha1 "github.com/PlainsightAI/openfilter-pipelines-controller/api/v1alpha1"
)

func TestBuildRTSPURLWithCredentials(t *testing.T) {
	tests := []struct {
		name     string
		rtsp     *pipelinesv1alpha1.RTSPSource
		expected string
	}{
		{
			name: "basic with default port",
			rtsp: &pipelinesv1alpha1.RTSPSource{
				Host: "camera.example.com",
				Path: "/stream1",
			},
			expected: "rtsp://$(_RTSP_USERNAME):$(_RTSP_PASSWORD)@camera.example.com:554/stream1",
		},
		{
			name: "custom port",
			rtsp: &pipelinesv1alpha1.RTSPSource{
				Host: "192.168.1.100",
				Port: 8554,
				Path: "/live",
			},
			expected: "rtsp://$(_RTSP_USERNAME):$(_RTSP_PASSWORD)@192.168.1.100:8554/live",
		},
		{
			name: "path without leading slash",
			rtsp: &pipelinesv1alpha1.RTSPSource{
				Host: "camera.local",
				Port: 554,
				Path: "stream",
			},
			expected: "rtsp://$(_RTSP_USERNAME):$(_RTSP_PASSWORD)@camera.local:554/stream",
		},
		{
			name: "empty path",
			rtsp: &pipelinesv1alpha1.RTSPSource{
				Host: "camera.local",
				Port: 554,
			},
			expected: "rtsp://$(_RTSP_USERNAME):$(_RTSP_PASSWORD)@camera.local:554",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildRTSPURLWithCredentials(tt.rtsp)
			if result != tt.expected {
				t.Errorf("buildRTSPURLWithCredentials() = %q, want %q", result, tt.expected)
			}
		})
	}
}
