package resilience

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newTestBreaker returns a breaker whose clock is a fixed point that
// tests can advance by mutating the returned pointer, so the
// OpenTimeout transitions are exercised without sleeping.
func newTestBreaker(cfg CircuitConfig) (*CircuitBreaker, *time.Time) {
	now := time.Now()
	cb := NewCircuitBreaker(cfg)
	cb.now = func() time.Time { return now }
	return cb, &now
}

func failFn() error { return errors.New("boom") }

// TestCircuitBreaker_OpensAfterThresholdFailures drives the breaker to
// StateOpen by hitting FailureThreshold consecutive failures, and then
// checks that an immediate call is rejected.
func TestCircuitBreaker_OpensAfterThresholdFailures(t *testing.T) {
	cb, _ := newTestBreaker(CircuitConfig{FailureThreshold: 3, OpenTimeout: time.Minute, SuccessThreshold: 1})

	for i := 0; i < 2; i++ {
		if err := cb.Execute(failFn); err == nil {
			t.Fatalf("call %d: Execute returned nil, want the fn error", i+1)
		}
		if cb.State() != StateClosed {
			t.Fatalf("after %d failures state = %s, want closed", i+1, cb.State())
		}
	}

	// The third consecutive failure crosses the threshold.
	if err := cb.Execute(failFn); err == nil {
		t.Fatal("third failure: Execute returned nil, want the fn error")
	}
	if cb.State() != StateOpen {
		t.Fatalf("state = %s, want open", cb.State())
	}

	// While open (timeout not elapsed) every call is rejected.
	if err := cb.Execute(failFn); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("open breaker: err = %v, want ErrCircuitOpen", err)
	}
}

// TestCircuitBreaker_OpenDoesNotInvokeFn proves the fail-fast side of
// the breaker: while StateOpen, fn is never executed — a caller gets
// ErrCircuitOpen without paying the cost of an upstream call.
func TestCircuitBreaker_OpenDoesNotInvokeFn(t *testing.T) {
	cb, _ := newTestBreaker(CircuitConfig{FailureThreshold: 1, OpenTimeout: time.Minute, SuccessThreshold: 1})

	var calls int
	counting := func() error {
		calls++
		return failFn()
	}

	// One failure trips the breaker with FailureThreshold=1. The fn
	// error is returned unchanged (and is not ErrCircuitOpen).
	if err := cb.Execute(counting); err == nil || errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("err = %v, want the fn error to pass through unchanged", err)
	}
	if cb.State() != StateOpen {
		t.Fatalf("state = %s, want open", cb.State())
	}

	before := calls
	for i := 0; i < 5; i++ {
		if err := cb.Execute(counting); !errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("call %d: err = %v, want ErrCircuitOpen", i+1, err)
		}
	}
	if calls != before {
		t.Errorf("fn executed %d times while open, want %d (must be rejected without execution)", calls, before)
	}
}

// TestCircuitBreaker_HalfOpenAfterTimeout checks the guarded
// transition: while OpenTimeout has not elapsed the breaker rejects,
// and once it elapses the next Execute is admitted, runs fn, and
// leaves the breaker probing in StateHalfOpen.
func TestCircuitBreaker_HalfOpenAfterTimeout(t *testing.T) {
	cb, now := newTestBreaker(CircuitConfig{FailureThreshold: 2, OpenTimeout: time.Second, SuccessThreshold: 2})

	cb.Execute(failFn)
	cb.Execute(failFn)
	if cb.State() != StateOpen {
		t.Fatalf("state = %s, want open", cb.State())
	}

	// Before the timeout: still rejected.
	if err := cb.Execute(failFn); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("before timeout: err = %v, want ErrCircuitOpen", err)
	}

	// Advance the clock past OpenTimeout and let one probe through.
	*now = now.Add(2 * time.Second)
	if err := cb.Execute(func() error { return nil }); err != nil {
		t.Fatalf("after timeout: err = %v, want nil (probe admitted)", err)
	}
	if cb.State() != StateHalfOpen {
		t.Fatalf("state = %s, want half_open (threshold 2 keeps it probing after 1 success)", cb.State())
	}
}

// TestCircuitBreaker_ClosesAfterSuccessThreshold proves the recovery
// path: SuccessThreshold consecutive successes in StateHalfOpen bring
// the breaker back to StateClosed, after which normal traffic is
// allowed and a success does not tear anything.
func TestCircuitBreaker_ClosesAfterSuccessThreshold(t *testing.T) {
	cb, now := newTestBreaker(CircuitConfig{FailureThreshold: 2, OpenTimeout: time.Second, SuccessThreshold: 2})

	ok := func() error { return nil }
	cb.Execute(failFn)
	cb.Execute(failFn)
	if cb.State() != StateOpen {
		t.Fatalf("state = %s, want open", cb.State())
	}

	*now = now.Add(2 * time.Second)

	if err := cb.Execute(ok); err != nil {
		t.Fatalf("first probe: err = %v, want nil", err)
	}
	if cb.State() != StateHalfOpen {
		t.Fatalf("state = %s, want half_open after 1 probe success", cb.State())
	}

	if err := cb.Execute(ok); err != nil {
		t.Fatalf("second probe: err = %v, want nil", err)
	}
	if cb.State() != StateClosed {
		t.Fatalf("state = %s, want closed after SuccessThreshold probe successes", cb.State())
	}

	// Fully recovered: traffic flows, and a success is still fine.
	if err := cb.Execute(ok); err != nil {
		t.Fatalf("post-recovery call: err = %v, want nil", err)
	}
	if cb.State() != StateClosed {
		t.Fatalf("state = %s, want closed", cb.State())
	}
}

// TestCircuitBreaker_HalfOpenFailureReopens covers the other probe
// outcome: a single failure in StateHalfOpen re-opens the breaker and
// restarts the timeout, so no more calls are admitted until it elapses
// again.
func TestCircuitBreaker_HalfOpenFailureReopens(t *testing.T) {
	cb, now := newTestBreaker(CircuitConfig{FailureThreshold: 2, OpenTimeout: time.Second, SuccessThreshold: 2})

	cb.Execute(failFn)
	cb.Execute(failFn)
	*now = now.Add(2 * time.Second)

	// First probe succeeds: half-open, still probing.
	if err := cb.Execute(func() error { return nil }); err != nil {
		t.Fatalf("probe: err = %v, want nil", err)
	}
	if cb.State() != StateHalfOpen {
		t.Fatalf("state = %s, want half_open", cb.State())
	}

	// Second probe fails: straight back to open, timeout restarted.
	if err := cb.Execute(failFn); err == nil {
		t.Fatal("failing probe: Execute returned nil, want the fn error")
	}
	if cb.State() != StateOpen {
		t.Fatalf("state = %s, want open after a half-open failure", cb.State())
	}

	// The reopened breaker rejects again immediately (clock not moved).
	if err := cb.Execute(failFn); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("reopened breaker: err = %v, want ErrCircuitOpen", err)
	}
}

// TestCircuitBreaker_RoundTripThroughStates drives the full lifecycle
// closed -> open -> half-open -> closed under one fixed clock, making
// sure the state machine is coherent end to end.
func TestCircuitBreaker_RoundTripThroughStates(t *testing.T) {
	cb, now := newTestBreaker(CircuitConfig{FailureThreshold: 3, OpenTimeout: time.Second, SuccessThreshold: 2})

	for i := 0; i < 3; i++ {
		cb.Execute(failFn)
	}
	if cb.State() != StateOpen {
		t.Fatalf("state = %s, want open", cb.State())
	}

	*now = now.Add(2 * time.Second)
	for i := 0; i < 2; i++ {
		if err := cb.Execute(func() error { return nil }); err != nil {
			t.Fatalf("probe %d: err = %v, want nil", i+1, err)
		}
	}
	if cb.State() != StateClosed {
		t.Fatalf("state = %s, want closed", cb.State())
	}
}

// TestCircuitBreaker_Concurrent hammers the breaker from many
// goroutines at once — a mix of succeeding and failing calls run
// against a real, non-fixed clock so the OpenTimeout transitions also
// happen under contention. Run with -race, this must be clean: every
// piece of shared state lives behind the mutex, and fn runs outside
// it.
//
// The assertions here are the ones guaranteed by the load profile
// (every call lands in exactly one bucket, both the success and the
// failing paths run). Whether the breaker is ever observed open is
// load-dependent and flaky to assert; TestCircuitBreaker_ConcurrentOpenRejects
// pins that behavior down deterministically instead.
func TestCircuitBreaker_Concurrent(t *testing.T) {
	cb := NewCircuitBreaker(CircuitConfig{FailureThreshold: 5, OpenTimeout: 5 * time.Millisecond, SuccessThreshold: 2})

	var wg sync.WaitGroup
	var successes, opens, failures atomic.Int64

	const goroutines = 32
	const iterations = 200

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				i := i
				err := cb.Execute(func() error {
					if i%3 == 0 {
						return failFn()
					}
					return nil
				})
				switch {
				case err == nil:
					successes.Add(1)
				case errors.Is(err, ErrCircuitOpen):
					opens.Add(1)
				default:
					failures.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	const total = goroutines * iterations
	done := successes.Load() + opens.Load() + failures.Load()
	if done != total {
		t.Errorf("accounted %d Execute returns, want %d (every call must land in one bucket)", done, total)
	}
	if successes.Load() == 0 {
		t.Error("no succeeded call observed; the breaker should be letting traffic through")
	}
	if failures.Load() == 0 {
		t.Error("no fn-error observed; the breaker should be counting failures")
	}
	t.Logf("observed %d successes, %d opens, %d fn failures; final state %s",
		successes.Load(), opens.Load(), failures.Load(), cb.State())
}

// TestCircuitBreaker_ConcurrentOpenRejects makes the open-rejection
// path deterministic under concurrency: it trips the breaker
// single-threaded, then has many goroutines at once call Execute while
// it is open. Every one of them must get ErrCircuitOpen and none may
// run fn — which also verifies the shared state read is safe under
// -race.
func TestCircuitBreaker_ConcurrentOpenRejects(t *testing.T) {
	cb := NewCircuitBreaker(CircuitConfig{FailureThreshold: 1, OpenTimeout: time.Hour, SuccessThreshold: 1})
	if err := cb.Execute(failFn); err == nil {
		t.Fatalf("tripping call: err = %v, want the fn error", err)
	}
	if cb.State() != StateOpen {
		t.Fatalf("state = %s, want open", cb.State())
	}

	var wg sync.WaitGroup
	var calls atomic.Int64
	const goroutines = 32
	const iterations = 200

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn := func() error {
				calls.Add(1)
				return nil
			}
			for i := 0; i < iterations; i++ {
				if err := cb.Execute(fn); !errors.Is(err, ErrCircuitOpen) {
					t.Errorf("open breaker: err = %v, want ErrCircuitOpen", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	if got := calls.Load(); got != 0 {
		t.Errorf("fn executed %d times while the breaker was open, want 0 (all calls must have been rejected)", got)
	}
	if cb.State() != StateOpen {
		t.Fatalf("state = %s, want open (an open breaker that is never probed stays open)", cb.State())
	}
}