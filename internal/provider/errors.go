package provider

import (
	"errors"
	"fmt"
)

// Sentinel errors returned (typically wrapped inside a ProviderError)
// by Provider implementations. Callers should use errors.Is against
// these rather than comparing upstream status codes directly.
var (
	// ErrProviderUnavailable indicates the upstream provider could not
	// be reached or returned a server-side failure (e.g. connection
	// refused, timeout, 5xx response).
	ErrProviderUnavailable = errors.New("provider: upstream unavailable")

	// ErrRateLimited indicates the upstream provider rejected the
	// request due to rate limiting (e.g. HTTP 429).
	ErrRateLimited = errors.New("provider: rate limited")

	// ErrInvalidRequest indicates the request was rejected by the
	// upstream provider as malformed or otherwise invalid (e.g. HTTP
	// 400), and retrying it unmodified will not help.
	ErrInvalidRequest = errors.New("provider: invalid request")
)

// ProviderError wraps an error returned by an upstream provider with
// the HTTP status code it responded with (when applicable) and
// whether the failed request is safe to retry.
type ProviderError struct {
	// Provider is the name of the provider that produced the error,
	// matching Provider.Name().
	Provider string
	// StatusCode is the upstream HTTP status code, or 0 if the failure
	// occurred before an HTTP response was received (e.g. a timeout).
	StatusCode int
	// Retryable indicates whether the same request can reasonably be
	// retried (e.g. true for rate limits and transient 5xx errors,
	// false for invalid requests).
	Retryable bool
	// Err is the underlying error, typically one of the sentinel
	// errors declared in this package. It is exposed via Unwrap so
	// callers can use errors.Is/errors.As.
	Err error
}

// Error implements the error interface.
func (e *ProviderError) Error() string {
	if e.StatusCode != 0 {
		return fmt.Sprintf("provider %s: %s (status %d)", e.Provider, e.Err, e.StatusCode)
	}
	return fmt.Sprintf("provider %s: %s", e.Provider, e.Err)
}

// Unwrap returns the underlying error, allowing errors.Is and
// errors.As to see through a ProviderError to a sentinel error such
// as ErrRateLimited.
func (e *ProviderError) Unwrap() error {
	return e.Err
}

// IsRetryable reports whether the request that produced this error is
// safe to retry.
func (e *ProviderError) IsRetryable() bool {
	return e.Retryable
}
