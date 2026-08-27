package provider

import (
	"errors"
	"net/http"
	"strings"
	"testing"
)

func TestProviderError_IsRetryable(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		retryable  bool
		want       bool
	}{
		{name: "429 too many requests is retryable", statusCode: http.StatusTooManyRequests, retryable: true, want: true},
		{name: "400 bad request is not retryable", statusCode: http.StatusBadRequest, retryable: false, want: false},
		{name: "503 service unavailable is retryable", statusCode: http.StatusServiceUnavailable, retryable: true, want: true},
		{name: "200 ok is not retryable", statusCode: http.StatusOK, retryable: false, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := &ProviderError{
				Provider:   "test-provider",
				StatusCode: tt.statusCode,
				Retryable:  tt.retryable,
				Err:        errors.New("boom"),
			}
			if got := err.IsRetryable(); got != tt.want {
				t.Errorf("IsRetryable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestProviderError_ErrorsIs(t *testing.T) {
	tests := []struct {
		name    string
		wrapped error
	}{
		{name: "wraps ErrRateLimited", wrapped: ErrRateLimited},
		{name: "wraps ErrProviderUnavailable", wrapped: ErrProviderUnavailable},
		{name: "wraps ErrInvalidRequest", wrapped: ErrInvalidRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err error = &ProviderError{
				Provider:   "test-provider",
				StatusCode: 500,
				Err:        tt.wrapped,
			}
			if !errors.Is(err, tt.wrapped) {
				t.Errorf("errors.Is(err, %v) = false, want true", tt.wrapped)
			}
		})
	}
}

func TestProviderError_Error(t *testing.T) {
	t.Run("includes provider and status code", func(t *testing.T) {
		err := &ProviderError{Provider: "openai", StatusCode: 429, Err: ErrRateLimited}
		msg := err.Error()
		if !strings.Contains(msg, "openai") {
			t.Errorf("Error() = %q, want it to contain provider name %q", msg, "openai")
		}
		if !strings.Contains(msg, "429") {
			t.Errorf("Error() = %q, want it to contain status code %q", msg, "429")
		}
	})

	t.Run("omits status when zero", func(t *testing.T) {
		err := &ProviderError{Provider: "anthropic", StatusCode: 0, Err: ErrProviderUnavailable}
		msg := err.Error()
		if !strings.Contains(msg, "anthropic") {
			t.Errorf("Error() = %q, want it to contain provider name %q", msg, "anthropic")
		}
		if strings.Contains(msg, "status") {
			t.Errorf("Error() = %q, want no status mention when StatusCode is 0", msg)
		}
	})
}
