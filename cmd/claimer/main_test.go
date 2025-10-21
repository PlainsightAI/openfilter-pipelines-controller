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
