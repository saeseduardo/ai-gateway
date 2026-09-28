package resilience

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// CircuitState is the state of a CircuitBreaker at a given moment.
// The breaker moves between the three states as failures and
// successes accumulate; see CircuitBreaker for the transition rules.
type CircuitState int

const (
	// StateClosed is the normal operating state: every request is
	// allowed through, and consecutive failures below the threshold
	// are simply counted. A success resets the failure counter.
	StateClosed CircuitState = iota

	// StateOpen is the tripped state: requests are rejected
	// immediately with ErrCircuitOpen without calling fn, giving the
	// upstream a chance to recover. It lasts OpenTimeout.
	StateOpen

	// StateHalfOpen is the probing state entered after OpenTimeout
	// elapses: a limited number of requests are allowed through to
	// test whether the upstream has recovered.
	StateHalfOpen
)

// String returns a stable, lowercase name for the state, suitable for
// logging and metrics labels.
func (s CircuitState) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half_open"
	default:
		return fmt.Sprintf("state(%d)", int(s))
	}
}

// ErrCircuitOpen is returned by CircuitBreaker.Execute while the
// breaker is in StateOpen and the OpenTimeout has not yet elapsed. It
// signals that fn was NOT executed, so callers can fail fast without
// paying the cost (latency, tokens, money) of an upstream call that
// would almost certainly fail again. Match it with errors.Is.
var ErrCircuitOpen = errors.New("resilience: circuit breaker open")

// CircuitConfig controls a CircuitBreaker. The zero value matches
// nothing useful; construct it with explicit fields (the constructor
// clamps any value below 1 to 1 and any negative OpenTimeout to 0).
type CircuitConfig struct {
	// FailureThreshold is the number of consecutive failures that
	// trips the breaker from StateClosed to StateOpen. A single
	// success in StateClosed resets the streak.
	FailureThreshold int

	// OpenTimeout is how long the breaker stays in StateOpen,
	// rejecting all requests, before the next request transitions it
	// to StateHalfOpen and is let through as a probe.
	OpenTimeout time.Duration

	// SuccessThreshold is the number of consecutive successes required
	// in StateHalfOpen to close the breaker again. A single failure in
	// StateHalfOpen re-opens it immediately.
	SuccessThreshold int
}

// CircuitBreaker is a concurrency-safe circuit breaker used to short-
// circuit calls to an upstream that is currently failing, so the
// gateway does not keep hammering it (and burning latency and cost)
// while it is down.
//
// The breaker behaves like the classic three-state machine:
//
//   - StateClosed: fn runs normally. Each failure increments a
//     consecutive-failure counter; when it reaches FailureThreshold
//     the breaker opens. Any success resets the counter.
//   - StateOpen: Execute returns ErrCircuitOpen immediately, without
//     calling fn. After OpenTimeout has elapsed, the next Execute
//     transitions to StateHalfOpen and lets that call through.
//   - StateHalfOpen: fn runs as a probe. SuccessThreshold consecutive
//     successes close the breaker; a single failure re-opens it (and
//     the timeout restarts).
//
// All internal state is guarded by a sync.Mutex. Two caveats follow
// from the decision to run fn outside the lock: a request admitted
// while the breaker was Closed or HalfOpen is always executed (its
// result is discarded if another goroutine tripped the breaker in the
// meantime), and because StateHalfOpen allows fn to run without
// reserving the probe, more than one goroutine may share a probe call
// burst. Both are safe, bounded, and keep Execute fast — fn never
// blocks other goroutines from checking the state.
//
// The zero value is not usable; always construct it with
// NewCircuitBreaker.
type CircuitBreaker struct {
	cfg CircuitConfig

	mu        sync.Mutex
	state     CircuitState
	failures  int
	successes int
	openedAt  time.Time

	// now is the clock source. It wraps time.Now in production and is
	// overridden by tests to move time forward deterministically, so
	// the OpenTimeout transitions can be exercised without sleeping.
	now func() time.Time
}

// NewCircuitBreaker returns a *CircuitBreaker governed by cfg and
// starting in StateClosed. Config values below their minimum are
// clamped rather than rejected (FailureThreshold and SuccessThreshold
// to 1, OpenTimeout to 0) so a half-configured breaker still behaves
// predictably instead of never allowing anything through.
func NewCircuitBreaker(cfg CircuitConfig) *CircuitBreaker {
	if cfg.FailureThreshold < 1 {
		cfg.FailureThreshold = 1
	}
	if cfg.SuccessThreshold < 1 {
		cfg.SuccessThreshold = 1
	}
	if cfg.OpenTimeout < 0 {
		cfg.OpenTimeout = 0
	}
	return &CircuitBreaker{
		cfg:   cfg,
		state: StateClosed,
		now:   time.Now,
	}
}

// State returns the current breaker state. It is safe to call from any
// goroutine at any time, including from inside a fn passed to Execute
// (Execute does not hold the lock while fn runs).
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

// Execute runs fn subject to the breaker state:
//
//   - In StateOpen, if OpenTimeout has not elapsed, fn is NOT executed
//     and ErrCircuitOpen is returned.
//   - In StateOpen, once OpenTimeout has elapsed, the breaker moves to
//     StateHalfOpen and the call is let through.
//   - In StateClosed and StateHalfOpen, fn always runs and its error
//     is returned unchanged (so callers can use errors.Is / errors.As
//     against it); the counters are then updated per the state machine
//     above.
//
// The concurrent-check updates described above always happen under the
// mutex, so the breaker is safe to share across many goroutines.
func (cb *CircuitBreaker) Execute(fn func() error) error {
	cb.mu.Lock()
	if cb.state == StateOpen {
		if cb.now().Sub(cb.openedAt) < cb.cfg.OpenTimeout {
			cb.mu.Unlock()
			return ErrCircuitOpen
		}
		// The upstream may have recovered: transition to half-open
		// and let this call through as the first probe.
		cb.state = StateHalfOpen
		cb.successes = 0
	}
	cb.mu.Unlock()

	err := fn()

	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		if err != nil {
			cb.failures++
			if cb.failures >= cb.cfg.FailureThreshold {
				cb.setStateOpen()
			}
		} else {
			cb.failures = 0
		}
	case StateHalfOpen:
		if err != nil {
			cb.setStateOpen()
		} else {
			cb.successes++
			if cb.successes >= cb.cfg.SuccessThreshold {
				cb.state = StateClosed
				cb.failures = 0
				cb.successes = 0
			}
		}
	case StateOpen:
		// This call was admitted before another goroutine tripped the
		// breaker; its outcome no longer matters, so leave the breaker
		// as-is rather than letting a stale success or failure contort
		// the fresh timeout.
	}
	return err
}

// setStateOpen trips the breaker: it records openedAt on the injected
// clock so the OpenTimeout counts from now, and clears both counters
// so the next Closed/HalfOpen cycle starts cleanly. Callers must hold
// cb.mu.
func (cb *CircuitBreaker) setStateOpen() {
	cb.state = StateOpen
	cb.openedAt = cb.now()
	cb.failures = 0
	cb.successes = 0
}