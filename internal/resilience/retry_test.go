package resilience

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/saeseduardo/ai-gateway/internal/provider"
)

var (
	errRetryable = &provider.ProviderError{
		Provider:   "fake",
		StatusCode: http.StatusServiceUnavailable,
		Retryable:  true,
		Err:        provider.ErrProviderUnavailable,
	}
	errNotRetryable = &provider.ProviderError{
		Provider:   "fake",
		StatusCode: http.StatusBadRequest,
		Retryable:  false,
		Err:        provider.ErrInvalidRequest,
	}
)

func TestDo_RetriesThenSucceeds(t *testing.T) {
	var calls atomic.Int32
	fn := func() error {
		if calls.Add(1) <= 2 {
			return errRetryable
		}
		return nil
	}

	cfg := RetryConfig{MaxAttempts: 5, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond}
	if err := Do(context.Background(), cfg, DefaultIsRetryable, fn); err != nil {
		t.Fatalf("Do returned error, want nil success: %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("fn executed %d times, want 3 (2 failures + 1 success)", got)
	}
}

func TestDo_NonRetryableStopsAfterOneAttempt(t *testing.T) {
	var calls atomic.Int32
	fn := func() error {
		calls.Add(1)
		return errNotRetryable
	}

	cfg := RetryConfig{MaxAttempts: 5, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond}
	err := Do(context.Background(), cfg, DefaultIsRetryable, fn)
	if err == nil {
		t.Fatal("Do returned nil, want the non-retryable error")
	}
	if !errors.Is(err, provider.ErrInvalidRequest) {
		t.Errorf("err = %v, want errors.Is(err, ErrInvalidRequest)", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("fn executed %d times, want 1 (non-retryable error must not be retried)", got)
	}
}

func TestDo_ExhaustsAttempts(t *testing.T) {
	var calls atomic.Int32
	fn := func() error {
		calls.Add(1)
		return errRetryable
	}

	cfg := RetryConfig{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond}
	err := Do(context.Background(), cfg, DefaultIsRetryable, fn)
	if err == nil {
		t.Fatal("Do returned nil, want exhaustion error")
	}
	if !errors.Is(err, provider.ErrProviderUnavailable) {
		t.Errorf("err = %v, want errors.Is(err, ErrProviderUnavailable) through the wrap", err)
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("fn executed %d times, want %d (MaxAttempts)", got, cfg.MaxAttempts)
	}
	if !strings.Contains(err.Error(), "exhausted 3 attempts") {
		t.Errorf("error message %q does not report the attempt count", err.Error())
	}
}

// TestDo_ContextCancelledDuringBackoff is the test that matters most
// for the cost-sensitive streaming use case: when the client goes
// away, the retry loop must give up immediately instead of sleeping
// out a long backoff and then firing another upstream request (and so
// another billed token generation) nobody is waiting for.
//
// The fake fn fails retryably on every call, and the backoff is a full
// minute — far longer than the test. The context is cancelled shortly
// after the loop has entered its first backoff wait, and the test then
// asserts Do returns context.Canceled within a small budget instead of
// after the minute the backoff would have taken.
func TestDo_ContextCancelledDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var calls atomic.Int32
	fn := func() error {
		calls.Add(1)
		return errRetryable
	}

	cfg := RetryConfig{MaxAttempts: 10, BaseDelay: time.Minute, MaxDelay: time.Minute}

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := Do(ctx, cfg, DefaultIsRetryable, fn)
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if elapsed > 2*time.Second {
		t.Errorf("Do took %s to return after cancellation, want prompt (backoff was 1m, it must not be waited out)", elapsed)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("fn executed %d times, want 1 (no further execution after cancellation)", got)
	}
}

// TestDo_ContextCancelledBeforeRetry verifies the defense-in-depth
// check: if fn fails and by the time it has returned the context is
// already cancelled, Do must not start a backoff at all — the caller
// is gone, so another attempt would be wasted work.
func TestDo_ContextCancelledBeforeRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var calls atomic.Int32
	fn := func() error {
		calls.Add(1)
		return errRetryable
	}

	cfg := RetryConfig{MaxAttempts: 5, BaseDelay: time.Second, MaxDelay: time.Second}
	err := Do(ctx, cfg, DefaultIsRetryable, fn)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("fn executed %d times, want 1", got)
	}
}

// TestDo_BackoffGrows runs three failing attempts under a short,
// deterministic (no jitter) backoff and verifies the second wait is
// longer than the first — 2*BaseDelay against BaseDelay — proving the
// exponential growth is actually applied between attempts, not just
// computed in a helper nobody calls.
func TestDo_BackoffGrows(t *testing.T) {
	var mu sync.Mutex
	var times []time.Time
	var calls atomic.Int32

	fn := func() error {
		mu.Lock()
		times = append(times, time.Now())
		mu.Unlock()
		calls.Add(1)
		return errRetryable
	}

	cfg := RetryConfig{MaxAttempts: 3, BaseDelay: 50 * time.Millisecond, MaxDelay: time.Second}
	if err := Do(context.Background(), cfg, DefaultIsRetryable, fn); err == nil {
		t.Fatal("Do returned nil, want exhaustion error")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(times) != 3 {
		t.Fatalf("recorded %d executions, want 3", len(times))
	}
	first := times[1].Sub(times[0])
	second := times[2].Sub(times[1])
	if second <= first {
		t.Errorf("second backoff (%s) is not longer than first (%s); the delay must grow exponentially", second, first)
	}
	if first < 40*time.Millisecond {
		t.Errorf("first backoff measured %s, want roughly BaseDelay (50ms) or more", first)
	}
}

func TestBackoffDelay_ExponentialAndCapped(t *testing.T) {
	cfg := RetryConfig{BaseDelay: 10 * time.Millisecond, MaxDelay: 35 * time.Millisecond}
	tests := []struct {
		retry int
		want  time.Duration
	}{
		{1, 10 * time.Millisecond},  // BaseDelay * 2^0
		{2, 20 * time.Millisecond},  // BaseDelay * 2^1
		{3, 35 * time.Millisecond},  // BaseDelay * 2^2 = 40, capped at MaxDelay
		{4, 35 * time.Millisecond},  // stays at the cap
		{10, 35 * time.Millisecond}, // never exceeds the cap
	}
	for _, tt := range tests {
		if got := backoffDelay(cfg, tt.retry); got != tt.want {
			t.Errorf("backoffDelay(retry=%d) = %s, want %s", tt.retry, got, tt.want)
		}
	}
}

func TestBackoffDelay_JitterStaysInRange(t *testing.T) {
	cfg := RetryConfig{BaseDelay: 100 * time.Millisecond, MaxDelay: 100 * time.Millisecond, Jitter: true}
	for i := 0; i < 200; i++ {
		d := backoffDelay(cfg, 1)
		if d < 0 || d > 100*time.Millisecond {
			t.Fatalf("jittered delay %s out of [0, 100ms)", d)
		}
	}
}

func TestDefaultIsRetryable(t *testing.T) {
	if !DefaultIsRetryable(errRetryable) {
		t.Error("retryable ProviderError reported not retryable")
	}
	if DefaultIsRetryable(errNotRetryable) {
		t.Error("non-retryable ProviderError reported retryable")
	}
	if DefaultIsRetryable(errors.New("boom")) {
		t.Error("plain error reported retryable")
	}
	if !DefaultIsRetryable(fmt.Errorf("wrapped: %w", errRetryable)) {
		t.Error("ProviderError wrapped in another error not detected via errors.As")
	}
}