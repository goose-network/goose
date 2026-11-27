package router

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/core"
	"github.com/goose-network/goose/internal/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// failTerminalOnce wraps the persist seam so the FIRST terminal write fails
// with a transient error and every later call delegates to the real persist.
// It returns the terminal-call counter. Only terminal writes route through
// the seam's failure branch; progress snapshots pass straight through.
func failTerminalOnce(t *testing.T, r *Router, fail func(terminalCalls int64) error) *atomic.Int64 {
	t.Helper()
	real := r.persistFullProbeFn
	var calls atomic.Int64
	r.persistFullProbeFn = func(fp *fullProbeRun, force bool) error {
		if force && fp.snapshot().State != core.FullProbeRunning {
			n := calls.Add(1)
			if err := fail(n); err != nil {
				return err
			}
		}
		return real(fp, force)
	}
	return &calls
}

// TestFullProbeTerminalPersistRetryHeals pins the fix for the stranded-job
// deadlock: when the terminal snapshot's synchronous write fails transiently,
// the runner must retry until the durable record lands, then evict the live
// copy. Without the retry the durable snapshot stays "running" forever, both
// retention paths exempt the job's row bucket, and zombie buckets crowd out
// healthy finished jobs.
func TestFullProbeTerminalPersistRetryHeals(t *testing.T) {
	f := newReachFixture(t, false)
	srv := probeTarget(t)
	calls := failTerminalOnce(t, f.router, func(n int64) error {
		if n == 1 {
			return errors.New("transient disk hiccup")
		}
		return nil
	})

	j0, err := f.router.StartFullProbe(core.FullProbeSpec{
		Target: srv.URL, Timeout: 5 * time.Second, RPS: 1000, MaxDepth: 1,
	})
	require.NoError(t, err)
	// The live copy is terminal immediately; the DURABLE getter is what the
	// retry has to heal, so await it, not r.FullProbeJob.
	require.Eventually(t, func() bool {
		j, ok := f.ms.FullProbeJob(j0.ID)
		return ok && j.State == core.FullProbeDone
	}, 10*time.Second, 25*time.Millisecond, "terminal snapshot never landed durably after the transient failure")
	assert.GreaterOrEqual(t, calls.Load(), int64(2), "the terminal persist must have been retried")

	// Once the record lands the live copy is dropped, exactly like the
	// err==nil fast path.
	require.Eventually(t, func() bool {
		f.router.fullProbeMu.Lock()
		defer f.router.fullProbeMu.Unlock()
		_, ok := f.router.fullProbes[j0.ID]
		return !ok
	}, 5*time.Second, 10*time.Millisecond, "healed job never evicted from the live map")

	// GET serves the durable terminal state and the attempt rows survive.
	got, ok := f.router.FullProbeJob(j0.ID)
	require.True(t, ok)
	assert.Equal(t, core.FullProbeDone, got.State)
	require.Eventually(t, func() bool {
		rows, err := f.ms.FullProbeResults(j0.ID, 10, "", metrics.FullProbeResultFilter{})
		require.NoError(t, err)
		return len(rows) > 0
	}, 5*time.Second, 25*time.Millisecond, "attempt rows never became readable")
}

// TestFullProbeTerminalPersistRetryShutdownPrompt pins that a retrying
// terminal persist cannot hang Close: stopFullProbes closes r.shutdown before
// joining, so the retry loop exits within one backoff tick even while the
// store keeps rejecting the write.
func TestFullProbeTerminalPersistRetryShutdownPrompt(t *testing.T) {
	f := newReachFixture(t, false)
	srv := probeTarget(t)
	calls := failTerminalOnce(t, f.router, func(int64) error {
		return errors.New("store keeps failing")
	})

	j0, err := f.router.StartFullProbe(core.FullProbeSpec{
		Target: srv.URL, Timeout: 5 * time.Second, RPS: 1000, MaxDepth: 1,
	})
	require.NoError(t, err)
	// Wait until the runner exited into the retry loop (first terminal
	// failure observed), then close concurrently with the backoff wait.
	require.Eventually(t, func() bool { return calls.Load() >= 1 },
		10*time.Second, 10*time.Millisecond, "terminal persist never failed")
	closed := make(chan struct{})
	go func() {
		f.router.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close hung behind the terminal-persist retry loop")
	}
	// Give-up keeps the live copy for reconcileFullProbeZombies at next Open.
	f.router.fullProbeMu.Lock()
	_, live := f.router.fullProbes[j0.ID]
	f.router.fullProbeMu.Unlock()
	assert.True(t, live, "an unpersisted terminal job must keep its live copy on shutdown")
}

// TestFullProbeTerminalPersistGivesUpOnClosedStore pins the give-up path: a
// closed store returns metrics.ErrClosed, which stops the retry after one
// attempt instead of spinning until shutdown. The runner exits promptly and
// keeps the live copy (the durable record can never land in this process).
func TestFullProbeTerminalPersistGivesUpOnClosedStore(t *testing.T) {
	f := newReachFixture(t, false)
	srv := probeTarget(t)
	var closeErr error
	calls := failTerminalOnce(t, f.router, func(n int64) error {
		if n == 1 {
			// Close the store at the moment the job finishes: the delegated
			// terminal write returns ErrClosed. (No require here: the seam
			// runs on the runner goroutine, not the test goroutine.)
			closeErr = f.ms.Close()
		}
		return nil
	})

	j0, err := f.router.StartFullProbe(core.FullProbeSpec{
		Target: srv.URL, Timeout: 5 * time.Second, RPS: 1000, MaxDepth: 1,
	})
	require.NoError(t, err)
	awaitFullProbe(t, f.router, j0.ID)
	f.router.fullProbeMu.Lock()
	fp := f.router.fullProbes[j0.ID]
	f.router.fullProbeMu.Unlock()
	require.NotNil(t, fp, "give-up must keep the live copy")
	select {
	case <-fp.done:
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not exit promptly on a closed store")
	}
	assert.NoError(t, closeErr)
	// Initial failure + exactly one retry that observes ErrClosed; no spin.
	assert.Equal(t, int64(2), calls.Load())
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int64(2), calls.Load(), "retry loop kept spinning after ErrClosed")
}
