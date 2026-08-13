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

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestExponentialBackoff(t *testing.T) {
	backoff := newExponentialBackoff(8 * time.Second)

	if got := backoff.Next(); got != 1*time.Second {
		t.Fatalf("expected first delay 1s, got %s", got)
	}

	if got := backoff.Next(); got != 2*time.Second {
		t.Fatalf("expected second delay 2s, got %s", got)
	}

	if got := backoff.Next(); got != 4*time.Second {
		t.Fatalf("expected third delay 4s, got %s", got)
	}

	if got := backoff.Next(); got != 8*time.Second {
		t.Fatalf("expected capped delay 8s, got %s", got)
	}

	if got := backoff.Next(); got != 8*time.Second {
		t.Fatalf("expected capped delay 8s on subsequent calls, got %s", got)
	}

	backoff.Reset()
	if got := backoff.Next(); got != 1*time.Second {
		t.Fatalf("expected reset to min delay 1s, got %s", got)
	}
}

// TestLoadConfig_DirectMode covers the PLAT-1071 direct-download mode
// shape on loadConfig: when S3_OBJECT_KEY is set the Stream/Group env
// requirement is lifted (because the claimer skips Valkey), and the
// resulting Config carries the object key for run() to dispatch on.
func TestLoadConfig_DirectMode(t *testing.T) {
	// Required for every mode.
	t.Setenv("S3_BUCKET", "test-bucket")

	t.Run("direct_mode_no_stream_or_group_required", func(t *testing.T) {
		t.Setenv("S3_OBJECT_KEY", "media/front.mp4")
		// Deliberately do NOT set STREAM or GROUP.
		t.Setenv("STREAM", "")
		t.Setenv("GROUP", "")
		cfg, err := loadConfig()
		if err != nil {
			t.Fatalf("direct mode must not require STREAM/GROUP, got error: %v", err)
		}
		if cfg.S3ObjectKey != "media/front.mp4" {
			t.Errorf("S3ObjectKey = %q, want %q", cfg.S3ObjectKey, "media/front.mp4")
		}
	})

	t.Run("queue_mode_still_requires_stream", func(t *testing.T) {
		// Leaving S3_OBJECT_KEY empty puts the claimer back in queue mode,
		// where STREAM/GROUP remain mandatory — the existing contract.
		t.Setenv("S3_OBJECT_KEY", "")
		t.Setenv("STREAM", "")
		t.Setenv("GROUP", "g")
		if _, err := loadConfig(); err == nil {
			t.Error("queue mode must reject missing STREAM, got nil error")
		}
	})

	t.Run("queue_mode_still_requires_group", func(t *testing.T) {
		t.Setenv("S3_OBJECT_KEY", "")
		t.Setenv("STREAM", "s")
		t.Setenv("GROUP", "")
		if _, err := loadConfig(); err == nil {
			t.Error("queue mode must reject missing GROUP, got nil error")
		}
	})

	t.Run("s3_bucket_required_in_either_mode", func(t *testing.T) {
		t.Setenv("S3_BUCKET", "")
		t.Setenv("S3_OBJECT_KEY", "media/front.mp4")
		if _, err := loadConfig(); err == nil {
			t.Error("S3_BUCKET must be required even in direct mode, got nil error")
		}
	})

	t.Run("source_path_default_applies_in_direct_mode", func(t *testing.T) {
		t.Setenv("S3_BUCKET", "test-bucket")
		t.Setenv("S3_OBJECT_KEY", "media/front.mp4")
		t.Setenv("SOURCE_PATH", "")
		t.Setenv("VIDEO_INPUT_PATH", "")
		cfg, err := loadConfig()
		if err != nil {
			t.Fatalf("expected default path to satisfy validation, got: %v", err)
		}
		if cfg.SourcePath != defaultInputPath {
			t.Errorf("expected default %q, got %q", defaultInputPath, cfg.SourcePath)
		}
	})
}

// TestSourcePathResolution covers the SOURCE_PATH / VIDEO_INPUT_PATH (deprecated
// alias) precedence and the extension-less default introduced with PLAT-1499.
func TestSourcePathResolution(t *testing.T) {
	t.Setenv("S3_BUCKET", "test-bucket")
	t.Setenv("S3_OBJECT_KEY", "media/front.mp4") // direct mode so validation passes

	t.Run("SOURCE_PATH wins over VIDEO_INPUT_PATH", func(t *testing.T) {
		t.Setenv("SOURCE_PATH", "/ws/custom")
		t.Setenv("VIDEO_INPUT_PATH", "/ws/legacy")
		cfg, err := loadConfig()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.SourcePath != "/ws/custom" {
			t.Errorf("expected SOURCE_PATH to win, got %q", cfg.SourcePath)
		}
	})

	t.Run("VIDEO_INPUT_PATH used as deprecated alias", func(t *testing.T) {
		t.Setenv("SOURCE_PATH", "")
		t.Setenv("VIDEO_INPUT_PATH", "/ws/legacy.mp4")
		cfg, err := loadConfig()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.SourcePath != "/ws/legacy.mp4" {
			t.Errorf("expected VIDEO_INPUT_PATH alias, got %q", cfg.SourcePath)
		}
	})

	t.Run("default is extension-less /ws/input", func(t *testing.T) {
		t.Setenv("SOURCE_PATH", "")
		t.Setenv("VIDEO_INPUT_PATH", "")
		cfg, err := loadConfig()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.SourcePath != "/ws/input" {
			t.Errorf("expected /ws/input, got %q", cfg.SourcePath)
		}
	})
}

// TestWriteSourceURIFile checks the sidecar the claimer writes so an entry filter can
// report the object's real source URI as meta['src'] (PLAT-1498/1499).
func TestWriteSourceURIFile(t *testing.T) {
	cases := []struct{ name, key, want string }{
		{"plain key", "nested/path/original.png", "s3://my-bucket/nested/path/original.png"},
		{"leading slash trimmed (no double slash)", "/nested/path/original.png", "s3://my-bucket/nested/path/original.png"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sourcePath := filepath.Join(t.TempDir(), "input")
			writeSourceURIFile(sourcePath, "my-bucket", tc.key)
			got, err := os.ReadFile(sourcePath + sourceURIFileSuffix)
			if err != nil {
				t.Fatalf("expected sidecar file to be written: %v", err)
			}
			if string(got) != tc.want {
				t.Errorf("expected %q, got %q", tc.want, string(got))
			}
		})
	}
}

func TestLoadConfig_ValkeyPassword(t *testing.T) {
	t.Setenv("STREAM", "test-stream")
	t.Setenv("GROUP", "test-group")
	t.Setenv("S3_BUCKET", "test-bucket")

	t.Run("reads VALKEY_PASSWORD when set", func(t *testing.T) {
		t.Setenv("VALKEY_PASSWORD", "secret123")
		cfg, err := loadConfig()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.ValkeyPassword != "secret123" {
			t.Fatalf("expected ValkeyPassword='secret123', got '%s'", cfg.ValkeyPassword)
		}
	})

	t.Run("ValkeyPassword is empty when not set", func(t *testing.T) {
		cfg, err := loadConfig()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.ValkeyPassword != "" {
			t.Fatalf("expected empty ValkeyPassword, got '%s'", cfg.ValkeyPassword)
		}
	})
}

func TestLoadConfig_ValkeyUsername(t *testing.T) {
	t.Setenv("STREAM", "test-stream")
	t.Setenv("GROUP", "test-group")
	t.Setenv("S3_BUCKET", "test-bucket")

	t.Run("reads VALKEY_USERNAME when set", func(t *testing.T) {
		t.Setenv("VALKEY_USERNAME", "ns-team-alpha")
		cfg, err := loadConfig()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.ValkeyUsername != "ns-team-alpha" {
			t.Fatalf("expected ValkeyUsername='ns-team-alpha', got '%s'", cfg.ValkeyUsername)
		}
	})

	t.Run("ValkeyUsername is empty when not set", func(t *testing.T) {
		cfg, err := loadConfig()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.ValkeyUsername != "" {
			t.Fatalf("expected empty ValkeyUsername, got '%s'", cfg.ValkeyUsername)
		}
	})
}

func TestLoadConfig_S3Region(t *testing.T) {
	t.Setenv("STREAM", "test-stream")
	t.Setenv("GROUP", "test-group")
	t.Setenv("S3_BUCKET", "test-bucket")

	t.Run("reads S3_REGION when set", func(t *testing.T) {
		t.Setenv("S3_REGION", "eu-west-1")
		cfg, err := loadConfig()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.S3Region != "eu-west-1" {
			t.Fatalf("expected S3Region='eu-west-1', got '%s'", cfg.S3Region)
		}
	})

	t.Run("S3Region defaults to empty when not set", func(t *testing.T) {
		cfg, err := loadConfig()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.S3Region != "" {
			t.Fatalf("expected empty S3Region, got '%s'", cfg.S3Region)
		}
	})
}

func TestIsRetryableValkeyError(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name      string
		err       error
		retryable bool
	}{
		{
			name:      "net operation error",
			err:       &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")},
			retryable: true,
		},
		{
			name:      "connection refused string",
			err:       errors.New("dial tcp 127.0.0.1:6379: connect: connection refused"),
			retryable: true,
		},
		{
			name: "wrapped client creation error",
			err: fmt.Errorf("failed to create Valkey client: %w",
				&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}),
			retryable: true,
		},
		{
			name:      "EOF",
			err:       io.EOF,
			retryable: true,
		},
		{
			name:      "context canceled",
			err:       context.Canceled,
			retryable: false,
		},
		{
			name:      "NOAUTH error should be fatal",
			err:       errors.New("NOAUTH Authentication required."),
			retryable: false,
		},
		{
			name:      "NOGROUP error should be fatal",
			err:       errors.New("NOGROUP No such key"),
			retryable: false,
		},
		{
			name:      "generic error",
			err:       errors.New("boom"),
			retryable: false,
		},
	}

	for _, tc := range testCases {

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := isRetryableValkeyError(tc.err); got != tc.retryable {
				t.Fatalf("expected retryable=%t, got %t (error: %v)", tc.retryable, got, tc.err)
			}
		})
	}
}
