// Package resilience contains the gateway's fault-tolerance
// utilities. Its first piece is retry with exponential backoff,
// used to shield upstream provider calls from transient failures
// (rate limits, 5xx responses, dropped connections) without waiting
// a fixed — and therefore either too long or too short — delay on
// each retry.
//
// The package is deliberately isolated from the rest of the codebase:
// it knows nothing about HTTP, routes, or providers, and only a
// single function (DefaultIsRetryable) references the provider error
// type. This keeps the retry mechanics unit-testable on their own and
// reusable for any call that can fail transiently.
package resilience

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/saeseduardo/ai-gateway/internal/provider"
)

// RetryConfig controls the retry loop. The zero value is not useful;
// construct it with explicit fields.
type RetryConfig struct {
	// MaxAttempts is the total number of times fn will be executed,
	// including the first attempt. Values below 1 are treated as 1.
	MaxAttempts int

	// BaseDelay is the backoff waited before the first retry. Each
	// subsequent retry doubles it, up to MaxDelay.
	BaseDelay time.Duration

	// MaxDelay caps how large a single backoff may grow. A value <= 0
	// leaves the delay uncapped.
	MaxDelay time.Duration

	// Jitter, when true, randomizes each backoff to a uniformly
	// random duration in [0, delay). This spreads reconnects across
	// many clients retrying at once, so they do not all hit the
	// provider at the same instant the moment it recovers (the
	// "thundering herd" problem). Keep it off when deterministic
	// backoff timing matters, e.g. in tests.
	Jitter bool
}

// DefaultIsRetryable reports whether err is safe to retry. It finds a
// *provider.ProviderError anywhere in the chain via errors.As and asks
// whether that error IsRetryable, so a ProviderError bubbling up from
// deep inside a call still decides the outcome.
//
// Any error that is not a provider error reports false. The retry
// utility deliberately refuses to guess about opaque failures:
// retrying an invalid request is pointless, and retrying a bug in the
// caller only delays the inevitable. Callers with a different policy
// (a custom error type, a status code convention, anything else) can
// pass their own isRetryable function to Do instead.
func DefaultIsRetryable(err error) bool {
	var perr *provider.ProviderError
	return errors.As(err, &perr) && perr.IsRetryable()
}

// Do runs fn up to cfg.MaxAttempts times, retrying only while a retry
// is still possible:
//
//   - fn is always executed at least once, even if cfg.MaxAttempts < 1;
//   - a nil return ends the loop with success;
//   - a non-retryable error (per isRetryable) stops the loop and is
//     returned unchanged, so callers can still use errors.Is /
//     errors.As against it;
//   - a retryable error is retried only if attempts remain, after
//     waiting the backoff produced by backoffDelay.
//
// The context is respected in both places it can matter:
//
//   - If ctx is cancelled while Do is blocked in a backoff wait, the
//     wait is abandoned immediately and ctx.Err() is returned with no
//     further executions of fn.
//   - If fn itself fails with an error after ctx has already been
//     cancelled, Do returns ctx.Err() without starting another wait.
//
// When every retryable attempt fails, the last error is returned
// wrapped with the number of attempts actually made, e.g. for
// MaxAttempts=3: "resilience: exhausted 3 attempts: ...". The wrap
// uses %w, so the underlying provider error (and its sentinel) can
// still be matched with errors.Is / errors.As.
func Do(ctx context.Context, cfg RetryConfig, isRetryable func(error) bool, fn func() error) error {
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		if !isRetryable(lastErr) {
			return lastErr
		}

		if attempt == cfg.MaxAttempts {
			break
		}

		delay := backoffDelay(cfg, attempt)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}

	return fmt.Errorf("resilience: exhausted %d attempts: %w", cfg.MaxAttempts, lastErr)
}

// backoffDelay returns the wait scheduled before retry number retry
// (1-based: retry 1 happens after the first failure) of a previously
// failed attempt. Without jitter it is BaseDelay * 2^(retry-1), capped
// at MaxDelay; with jitter enabled it is a uniform random duration in
// [0, delay).
//
// The doubling loop stops as soon as the cap is reached, which also
// keeps the value from overflowing on very large retry counts.
func backoffDelay(cfg RetryConfig, retry int) time.Duration {
	d := cfg.BaseDelay
	for i := 1; i < retry && (cfg.MaxDelay <= 0 || d < cfg.MaxDelay); i++ {
		d *= 2
	}
	if cfg.MaxDelay > 0 && d > cfg.MaxDelay {
		d = cfg.MaxDelay
	}
	if cfg.Jitter && d > 0 {
		d = time.Duration(rand.Int63n(int64(d) + 1))
	}
	return d
}