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
