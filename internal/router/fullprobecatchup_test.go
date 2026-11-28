package router

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/core"
	"github.com/goose-network/goose/internal/metrics"
	"github.com/goose-network/goose/internal/provider"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests pin the settling-tail fix for the depth-1 coverage gap: a full
// probe snapshots the outbound fleet at start, but provider fleets keep
// growing for minutes (server lists download in bursts), so outbounds that
// register after the start were never probed at depth 1 (the 428-vs-266
// report). The tail re-snapshots the live fleet after the beam and probes
// never-offered outbounds at depth 1, folding them into Levels[0].
//
// Fleet growth is injected deterministically with cfg.SetOutbound + Rebuild
// (the same choke point a provider poll flows through); no fake provider
// manager is needed because the router only reads cfg.Providers() to gate the
// tail. The catchup tick/quiet seams are shrunk to millisecond scale.

// enableCatchup configures a provider (so the tail gate engages) and shrinks
// the tick/quiet seams. quiet is the window the fleet must be unchanged before
// the tail exits; tick is the re-snapshot cadence.
func enableCatchup(f *reachFixture, tick, quiet time.Duration) {
	f.cfg.SetProvider(&config.ProviderSpec{ID: "prov", Provider: "psiphon"})
	f.router.catchupTickDur = tick
	f.router.catchupQuietDur = quiet
}

// lateOutbound is a newcomer outbound aimed at the fixture's reachable relay
// server, so it succeeds at depth 1 when probed.
func lateOutbound(f *reachFixture, id string) *config.OutboundSpec {
	return &config.OutboundSpec{ID: id, Protocol: "socks5", Config: map[string]any{"address": f.rAddr}}
}

// injectGrowth adds outbounds and rebuilds, simulating a provider poll landing
// after the job's start snapshot. StartFullProbe snapshots synchronously before
// it returns, so calling this right after start guarantees the newcomers are
// post-snapshot (the beam never sees them; only the tail does).
func injectGrowth(f *reachFixture, specs ...*config.OutboundSpec) {
	for _, s := range specs {
		f.cfg.SetOutbound(s)
	}
	f.router.Rebuild()
}

// awaitRows polls the job's durable attempt rows until their count settles at
// the job's TotalAttempts (the terminal snapshot can land a beat before the
// final async row flush).
func awaitRows(t *testing.T, f *reachFixture, id string, want int) []core.HTTPProbe {
	t.Helper()
	var rows []core.HTTPProbe
	require.Eventually(t, func() bool {
		got, err := f.ms.FullProbeResults(id, 1000, "", metrics.FullProbeResultFilter{})
		require.NoError(t, err)
		if len(got) != want {
			return false
		}
		rows = got
		return true
	}, 10*time.Second, 10*time.Millisecond, "attempt rows never settled at %d", want)
	return rows
}

// gatedProbeTarget is probeTarget with a hold gate: while held, every probe
// request blocks, so the beam cannot finish before the test injects growth.
// The single-final-pass tests use it to synchronize injection before the
// runner's ONE final pass (otherwise a starved test goroutine could let the
// final pass snapshot an unchanged fleet and the coverage pins flake).
func gatedProbeTarget(t *testing.T) (*httptest.Server, func()) {
	t.Helper()
	var mu sync.Mutex
	hold := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for {
			mu.Lock()
			h := hold
			mu.Unlock()
			if !h {
				break
			}
			time.Sleep(time.Millisecond)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv, func() { mu.Lock(); hold = false; mu.Unlock() }
}

// TestFullProbeCatchupCoversLateOutbound is the core regression: a depth-1 job
// starts with relay+work, a newcomer registers right after start, and the
// settling tail probes it at depth 1 so level 1 covers the whole live fleet.
// Every outbound gets exactly one local→X row.
func TestFullProbeCatchupCoversLateOutbound(t *testing.T) {
	f := newReachFixture(t, true) // work is locally blocked
	srv := probeTarget(t)
	enableCatchup(f, 5*time.Millisecond, 150*time.Millisecond)

	j0, err := f.router.StartFullProbe(core.FullProbeSpec{
		Target: srv.URL, Timeout: 5 * time.Second, RPS: 1000, MaxDepth: 1,
		CatchupSeconds: 5,
	})
	require.NoError(t, err)
	injectGrowth(f, lateOutbound(f, "late"))

	j := awaitFullProbe(t, f.router, j0.ID)
	assert.Equal(t, core.FullProbeDone, j.State, "job error: %s", j.Error)
	require.Len(t, j.Levels, 1)
	l1 := j.Levels[0]
	assert.Equal(t, 1, l1.Depth)
	assert.Equal(t, 3, l1.Candidates, "level 1 must cover the whole live fleet")
	assert.Equal(t, 3, l1.Total)
	assert.Equal(t, 3, l1.Done)
	assert.Equal(t, 2, l1.Success, "relay + late succeed")
	assert.Equal(t, 1, l1.Failed, "work stays blocked")

	var ok, fail []string
	for _, p := range l1.SuccessPaths {
		ok = append(ok, hopsOf(p))
	}
	for _, fi := range l1.FailedItems {
		fail = append(fail, fi.ID)
	}
	assert.Contains(t, ok, "late")
	assert.Contains(t, ok, "relay")
	assert.Equal(t, []string{"work"}, fail)

	// Exactly one depth-1 row per outbound, success and failure alike.
	rows := awaitRows(t, f, j0.ID, j.TotalAttempts)
	seen := map[string]int{}
	for _, p := range rows {
		assert.Equal(t, 1, p.Depth, "tail arrivals are measured at depth 1 only")
		seen[p.Route]++
	}
	assert.Equal(t, map[string]int{"relay": 1, "work": 1, "late": 1}, seen)
}

// TestFullProbeCatchupDepthLabelIntegrity asserts the tail folds late arrivals
// into level 1 only: deeper levels keep their depth labels and hop counts, and
// a newcomer never leaks into a deep success path.
func TestFullProbeCatchupDepthLabelIntegrity(t *testing.T) {
	f := newReachFixture(t, true)
	srv := probeTarget(t)
	enableCatchup(f, 5*time.Millisecond, 150*time.Millisecond)

	j0, err := f.router.StartFullProbe(core.FullProbeSpec{
		Target: srv.URL, Timeout: 5 * time.Second, RPS: 1000, MaxDepth: 5,
		CatchupSeconds: 5,
	})
	require.NoError(t, err)
	injectGrowth(f, lateOutbound(f, "late"))

	j := awaitFullProbe(t, f.router, j0.ID)
	assert.Equal(t, core.FullProbeDone, j.State, "job error: %s", j.Error)
	require.Len(t, j.Levels, 2, "the rescue still runs exactly two levels")
	for i, l := range j.Levels {
		assert.Equal(t, i+1, l.Depth)
		for _, p := range l.SuccessPaths {
			assert.Len(t, p.Hops, l.Depth, "depth-%d path must have %d hops", l.Depth, l.Depth)
		}
	}
	// late appears only at depth 1, never in a deep success path.
	found := false
	for _, p := range j.Levels[0].SuccessPaths {
		if hopsOf(p) == "late" {
			found = true
		}
	}
	assert.True(t, found, "late must be a depth-1 success")
	for _, p := range j.Levels[1].SuccessPaths {
		assert.NotContains(t, p.Hops, "late", "a newcomer must not leak into a deep path")
	}
	// The rescue path is intact.
	assert.Equal(t, []string{"relay", "work"}, j.Levels[1].SuccessPaths[0].Hops)
}

// TestFullProbeCatchupMaxDepthOneZeroSuccess reproduces the user's exact live
// case: a depth-1 job where every outbound fails (0 successes). The beam breaks
// immediately, but the tail must still run and cover late arrivals — the
// newcomer-union design was inert here.
func TestFullProbeCatchupMaxDepthOneZeroSuccess(t *testing.T) {
	f := newReachFixture(t, true)
	enableCatchup(f, 5*time.Millisecond, 150*time.Millisecond)

	// A refused target makes every depth-1 attempt fail regardless of outbound.
	j0, err := f.router.StartFullProbe(core.FullProbeSpec{
		Target: "http://127.0.0.1:1", Timeout: 2 * time.Second, RPS: 1000, MaxDepth: 1,
		CatchupSeconds: 5,
	})
	require.NoError(t, err)
	injectGrowth(f, lateOutbound(f, "late"))

	j := awaitFullProbe(t, f.router, j0.ID)
	assert.Equal(t, core.FullProbeDone, j.State, "job error: %s", j.Error)
	require.Len(t, j.Levels, 1)
	l1 := j.Levels[0]
	assert.Equal(t, 0, l1.Success)
	assert.Equal(t, 3, l1.Candidates, "the tail runs even when the beam found no success")
	assert.Equal(t, 3, l1.Failed)
	var fail []string
	for _, fi := range l1.FailedItems {
		fail = append(fail, fi.ID)
	}
	assert.Contains(t, fail, "late")
}

// TestFullProbeCatchupSettledFastPath asserts a settled fleet gets no tail
// latency: without providers the tail is one final pass, and with providers but
// a fleet quiet since before the start the first pass exits immediately.
func TestFullProbeCatchupSettledFastPath(t *testing.T) {
	srv := probeTarget(t)

	t.Run("no providers", func(t *testing.T) {
		f := newReachFixture(t, false)
		// Default CatchupSeconds (600) must NOT make a static-fleet job wait.
		start := time.Now()
		j0, err := f.router.StartFullProbe(core.FullProbeSpec{
			Target: srv.URL, Timeout: 5 * time.Second, RPS: 1000, MaxDepth: 1,
		})
		require.NoError(t, err)
		j := awaitFullProbe(t, f.router, j0.ID)
		assert.Equal(t, core.FullProbeDone, j.State)
		assert.Less(t, time.Since(start), 5*time.Second, "static fleet must not wait out the tail")
		assert.Equal(t, 2, j.Levels[0].Candidates)
	})

	t.Run("providers but fleet already quiet", func(t *testing.T) {
		f := newReachFixture(t, false)
		enableCatchup(f, 5*time.Millisecond, 50*time.Millisecond)
		// Pretend the fleet settled an hour ago.
		f.router.mu.Lock()
		f.router.fleetChangedAt = time.Now().Add(-time.Hour)
		f.router.mu.Unlock()

		start := time.Now()
		j0, err := f.router.StartFullProbe(core.FullProbeSpec{
			Target: srv.URL, Timeout: 5 * time.Second, RPS: 1000, MaxDepth: 1,
			CatchupSeconds: 600,
		})
		require.NoError(t, err)
		j := awaitFullProbe(t, f.router, j0.ID)
		assert.Equal(t, core.FullProbeDone, j.State)
		assert.Less(t, time.Since(start), 5*time.Second, "a settled fleet must exit on the first pass")
		assert.Equal(t, 2, j.Levels[0].Candidates)
	})
}

// TestFullProbeCatchupQuietExit asserts the tail exits one quiet window after
// the last fleet change, not at the CatchupSeconds deadline.
func TestFullProbeCatchupQuietExit(t *testing.T) {
	f := newReachFixture(t, true)
	srv := probeTarget(t)
	enableCatchup(f, 5*time.Millisecond, 100*time.Millisecond)

	j0, err := f.router.StartFullProbe(core.FullProbeSpec{
		Target: srv.URL, Timeout: 5 * time.Second, RPS: 1000, MaxDepth: 1,
		CatchupSeconds: 30, // deadline far away; quiet must win
	})
	require.NoError(t, err)
	injectGrowth(f, lateOutbound(f, "late"))
	// Anchor on the router's own quiet clock (stamped inside Rebuild), not a
	// post-injection wall stamp: the tail honors now-fleetChangedAt >= quiet,
	// and fleetChangedAt <= injected, so this floor holds under any load.
	f.router.mu.RLock()
	changed := f.router.fleetChangedAt
	f.router.mu.RUnlock()

	j := awaitFullProbe(t, f.router, j0.ID)
	assert.Equal(t, core.FullProbeDone, j.State)
	elapsed := time.Since(changed)
	assert.Less(t, elapsed, 5*time.Second, "tail must exit on quiet, not wait for the 30s deadline")
	assert.GreaterOrEqual(t, elapsed, 100*time.Millisecond, "tail must honor the quiet window")
	assert.Equal(t, 3, j.Levels[0].Candidates)
}

// TestFullProbeCatchupDeadlineExit asserts the CatchupSeconds bound terminates
// the tail when the fleet never goes quiet (quiet window set unreachable).
func TestFullProbeCatchupDeadlineExit(t *testing.T) {
	f := newReachFixture(t, true)
	srv := probeTarget(t)
	enableCatchup(f, 5*time.Millisecond, time.Hour) // never quiet

	j0, err := f.router.StartFullProbe(core.FullProbeSpec{
		Target: srv.URL, Timeout: 5 * time.Second, RPS: 1000, MaxDepth: 1,
		CatchupSeconds: 1,
	})
	require.NoError(t, err)
	injectGrowth(f, lateOutbound(f, "late"))
	start := time.Now()

	j := awaitFullProbe(t, f.router, j0.ID)
	assert.Equal(t, core.FullProbeDone, j.State)
	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, 900*time.Millisecond, "tail must run to the deadline when never quiet")
	assert.Less(t, elapsed, 5*time.Second, "deadline must bound the tail")
	assert.Equal(t, 3, j.Levels[0].Candidates, "the newcomer is still covered before the deadline")
}

// TestFullProbeCatchupCancelDuringTail asserts a cancel during the settling
// tail lands canceled and the runner exits (Close joins it without hanging).
func TestFullProbeCatchupCancelDuringTail(t *testing.T) {
	f := newReachFixture(t, false)
	srv := probeTarget(t)
	enableCatchup(f, 5*time.Millisecond, time.Hour) // tail would run to the deadline

	j0, err := f.router.StartFullProbe(core.FullProbeSpec{
		Target: srv.URL, Timeout: 5 * time.Second, RPS: 1000, MaxDepth: 1,
		CatchupSeconds: 600, // deadline must not race the cancel under load
	})
	require.NoError(t, err)
	injectGrowth(f, lateOutbound(f, "late"))

	// Wait until a top-up pass has folded the newcomer (tail is active).
	require.Eventually(t, func() bool {
		j, ok := f.router.FullProbeJob(j0.ID)
		return ok && len(j.Levels) > 0 && j.Levels[0].Candidates == 3
	}, 10*time.Second, 5*time.Millisecond, "tail never picked up the newcomer")

	require.True(t, f.router.CancelFullProbe(j0.ID))
	j := awaitFullProbe(t, f.router, j0.ID)
	assert.Equal(t, core.FullProbeCanceled, j.State)
	assert.False(t, f.router.CancelFullProbe(j0.ID), "a canceled job is not cancelable again")
}

// TestFullProbeCatchupBudgetDry asserts a dry MaxTotalAttempts stops the tail
// PROMPTLY (not via the quiet window or the deadline) and the cap-cut newcomers
// stay honestly Skipped. quiet is unreachable and the deadline far away, so
// only the budget-dry early exit can explain completion.
func TestFullProbeCatchupBudgetDry(t *testing.T) {
	f := newReachFixture(t, true)
	srv := probeTarget(t)
	enableCatchup(f, 5*time.Millisecond, time.Hour) // never quiet

	j0, err := f.router.StartFullProbe(core.FullProbeSpec{
		Target: srv.URL, Timeout: 5 * time.Second, RPS: 1000, MaxDepth: 1,
		MaxTotalAttempts: 3, CatchupSeconds: 30,
	})
	require.NoError(t, err)
	injectGrowth(f, lateOutbound(f, "late"), lateOutbound(f, "late2"))
	start := time.Now()

	j := awaitFullProbe(t, f.router, j0.ID)
	assert.Equal(t, core.FullProbeDone, j.State)
	assert.Less(t, time.Since(start), 5*time.Second, "a dry budget must end the tail promptly, not wait out quiet/deadline")
	l1 := j.Levels[0]
	assert.Equal(t, 3, l1.Total, "budget caps total planned attempts at 3")
	assert.Equal(t, 3, l1.Done)
	assert.Equal(t, 4, l1.Candidates, "all four outbounds were offered")
	assert.Equal(t, 1, l1.Skipped, "the tail's budget-dry cut of the second newcomer is honestly Skipped (the beam issued relay+work within the 3-attempt budget, so it skipped none)")
	assert.Equal(t, 3, j.TotalAttempts)
}

// TestFullProbeCatchupBurstOverflowReoffered pins the per-pass vs total budget
// distinction: a burst larger than MaxAttemptsPerLevel must be re-offered on
// later ticks while the TOTAL budget has room — the per-pass allowance resets
// every tick, so marking the overflow tried would permanently skip it (the
// 428-vs-266 coverage gap in miniature).
func TestFullProbeCatchupBurstOverflowReoffered(t *testing.T) {
	f := newReachFixture(t, true)
	srv := probeTarget(t)
	enableCatchup(f, 5*time.Millisecond, 150*time.Millisecond)

	j0, err := f.router.StartFullProbe(core.FullProbeSpec{
		Target: srv.URL, Timeout: 5 * time.Second, RPS: 1000, MaxDepth: 1,
		MaxAttemptsPerLevel: 2, MaxTotalAttempts: 100, CatchupSeconds: 5,
	})
	require.NoError(t, err)
	// One burst of four newcomers: the beam already spent 2 of the per-pass
	// allowance's worth of total budget, but 98 total attempts remain.
	injectGrowth(f, lateOutbound(f, "a"), lateOutbound(f, "b"), lateOutbound(f, "c"), lateOutbound(f, "d"))

	j := awaitFullProbe(t, f.router, j0.ID)
	assert.Equal(t, core.FullProbeDone, j.State, "job error: %s", j.Error)
	l1 := j.Levels[0]
	assert.Equal(t, 6, l1.Candidates, "level 1 must cover the whole live fleet")
	assert.Equal(t, 6, l1.Total, "the per-pass overflow must be re-offered, not permanently skipped")
	assert.Equal(t, 6, l1.Done)
	assert.Equal(t, 0, l1.Skipped, "nothing is cap-cut for good while the total budget has room")
	assert.Equal(t, 6, j.TotalAttempts)

	rows := awaitRows(t, f, j0.ID, 6)
	seen := map[string]int{}
	for _, p := range rows {
		assert.Equal(t, 1, p.Depth)
		seen[p.Route]++
	}
	assert.Equal(t, map[string]int{"relay": 1, "work": 1, "a": 1, "b": 1, "c": 1, "d": 1}, seen,
		"every burst member gets exactly one depth-1 row")
}

// TestFullProbeCatchupFinalPassWithGrowth pins the single-final-pass branches'
// documented guarantee: even without a tail (no providers, or an opted-out
// CatchupSeconds), the one final pass still covers outbounds that registered
// during the beam.
func TestFullProbeCatchupFinalPassWithGrowth(t *testing.T) {
	srv, release := gatedProbeTarget(t)

	t.Run("no providers", func(t *testing.T) {
		f := newReachFixture(t, false)
		start := time.Now()
		j0, err := f.router.StartFullProbe(core.FullProbeSpec{
			Target: srv.URL, Timeout: 5 * time.Second, RPS: 1000, MaxDepth: 1,
		})
		require.NoError(t, err)
		injectGrowth(f, lateOutbound(f, "late"))
		release() // beam may finish now; the final pass sees the newcomer

		j := awaitFullProbe(t, f.router, j0.ID)
		assert.Equal(t, core.FullProbeDone, j.State)
		assert.Less(t, time.Since(start), 5*time.Second, "the final pass adds no tail latency")
		assert.Equal(t, 3, j.Levels[0].Candidates, "the final pass must catch growth during the beam")
		rows := awaitRows(t, f, j0.ID, 3)
		found := false
		for _, p := range rows {
			if p.Route == "late" && p.Depth == 1 {
				found = true
			}
		}
		assert.True(t, found, "a durable local→late row must exist")
	})

	t.Run("opted out tail", func(t *testing.T) {
		f := newReachFixture(t, false)
		enableCatchup(f, 5*time.Millisecond, time.Hour)
		start := time.Now()
		j0, err := f.router.StartFullProbe(core.FullProbeSpec{
			Target: srv.URL, Timeout: 5 * time.Second, RPS: 1000, MaxDepth: 1,
			CatchupSeconds: -1,
		})
		require.NoError(t, err)
		assert.Equal(t, -1, j0.Spec.CatchupSeconds, "the opt-out must survive normalization as a fixed point")
		injectGrowth(f, lateOutbound(f, "late"))
		release() // beam may finish now; the final pass sees the newcomer

		j := awaitFullProbe(t, f.router, j0.ID)
		assert.Equal(t, core.FullProbeDone, j.State)
		assert.Less(t, time.Since(start), 5*time.Second, "opt-out must not wait out quiet/deadline")
		assert.Equal(t, 3, j.Levels[0].Candidates, "the final pass still runs with the tail off")
		rows := awaitRows(t, f, j0.ID, 3)
		found := false
		for _, p := range rows {
			if p.Route == "late" && p.Depth == 1 {
				found = true
			}
		}
		assert.True(t, found, "a durable local→late row must exist")
	})
}

// TestFullProbeCatchupFinalPassCapCut pins the final flag's accounting on the
// single-final-pass branches (no providers / opted-out tail): a per-pass cap
// smaller than a late burst must mark the overflow tried and honestly Skipped
// (there is no next tick to re-offer it), so Candidates still covers the whole
// observed fleet. Dropping `final ||` from the cut condition leaves the
// overflow untried and unaccounted — Candidates undercounts and Skipped stays 0
// (the surviving mutant this test exists to kill).
func TestFullProbeCatchupFinalPassCapCut(t *testing.T) {
	srv, release := gatedProbeTarget(t)
	burst := []string{"a", "b", "c", "d"}

	for _, tc := range []struct {
		name      string
		providers bool
		catchup   int
	}{
		{"no providers", false, 0},
		{"opted out tail", true, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// work must stay reachable so every issued row succeeds; the
			// fixture bool is "blocked", not "providers" — providers come
			// from enableCatchup below.
			f := newReachFixture(t, false)
			if tc.providers {
				enableCatchup(f, 5*time.Millisecond, time.Hour)
			}
			start := time.Now()
			j0, err := f.router.StartFullProbe(core.FullProbeSpec{
				Target: srv.URL, Timeout: 5 * time.Second, RPS: 1000, MaxDepth: 1,
				MaxAttemptsPerLevel: 2, MaxTotalAttempts: 100, CatchupSeconds: tc.catchup,
			})
			require.NoError(t, err)
			// Beam issues relay+work (2 = the per-pass allowance, no cut); the
			// burst of four then exceeds the final pass's allowance of two.
			injectGrowth(f, lateOutbound(f, "a"), lateOutbound(f, "b"),
				lateOutbound(f, "c"), lateOutbound(f, "d"))
			release() // beam may finish now; the final pass sees the whole burst

			j := awaitFullProbe(t, f.router, j0.ID)
			assert.Equal(t, core.FullProbeDone, j.State, "job error: %s", j.Error)
			assert.Less(t, time.Since(start), 5*time.Second, "the final pass adds no tail latency")
			l1 := j.Levels[0]
			assert.Equal(t, 6, l1.Candidates, "Candidates must cover the whole observed fleet (2 beam + 4 burst)")
			assert.Equal(t, 4, l1.Total, "2 beam + 2 of the burst issued; the other 2 are cap-cut")
			assert.Equal(t, 4, l1.Done)
			assert.Equal(t, 2, l1.Skipped, "the final pass must honestly skip the overflow it cannot re-offer")
			assert.Equal(t, 4, j.TotalAttempts)
			assert.Equal(t, l1.Candidates, l1.Total+l1.Skipped, "every observed candidate is planned or skipped")

			rows := awaitRows(t, f, j0.ID, 4)
			seen := map[string]int{}
			for _, p := range rows {
				assert.Equal(t, 1, p.Depth)
				assert.True(t, p.Success, "row %s: %s", p.Route, p.Error)
				seen[p.Route]++
			}
			assert.Equal(t, 1, seen["relay"])
			assert.Equal(t, 1, seen["work"])
			burstSeen := 0
			for _, id := range burst {
				burstSeen += seen[id]
			}
			assert.Equal(t, 2, burstSeen, "exactly two burst members are probed; the other two are Skipped, not rowed")
			assert.Len(t, seen, 4, "relay + work + two burst members, one row each")
		})
	}
}

// TestFullProbeCatchupMergeCapAndOrder pins the top-up merge contract: the
// level's success record stays re-capped at MaxPathsPerLevel, fastest first,
// so a faster late arrival displaces a slower beam path (whose durable row
// survives in the results view) and a slower one never does.
func TestFullProbeCatchupMergeCapAndOrder(t *testing.T) {
	f := newReachFixture(t, false)
	srv := probeTarget(t)
	enableCatchup(f, 5*time.Millisecond, 150*time.Millisecond)

	j0, err := f.router.StartFullProbe(core.FullProbeSpec{
		Target: srv.URL, Timeout: 5 * time.Second, RPS: 1000, MaxDepth: 1,
		MaxPathsPerLevel: 1, CatchupSeconds: 5,
	})
	require.NoError(t, err)
	injectGrowth(f, lateOutbound(f, "late"))

	j := awaitFullProbe(t, f.router, j0.ID)
	assert.Equal(t, core.FullProbeDone, j.State, "job error: %s", j.Error)
	l1 := j.Levels[0]
	require.Len(t, l1.SuccessPaths, 1, "the merged record must stay re-capped at the beam width")
	assert.Equal(t, 3, l1.Candidates)
	assert.Equal(t, 3, l1.Success, "the displaced path still counts as a success")

	// All three outbounds reach the same echo server, so which one wins the
	// width-1 record is jitter-dependent — and path latencies (fullProbeOne's
	// span) are not comparable to row latencies (assembleProbe stamps later).
	// Ordering/displacement semantics are pinned deterministically with
	// synthetic latencies in TestMergeFullProbePathsUnit; here we pin the
	// integration mechanics: the re-cap, the honest success count, and the
	// displaced attempts' durable rows.
	rows := awaitRows(t, f, j0.ID, 3)
	byRoute := map[string]core.HTTPProbe{}
	for _, p := range rows {
		require.True(t, p.Success, "row %s: %s", p.Route, p.Error)
		byRoute[p.Route] = p
	}
	kept := hopsOf(l1.SuccessPaths[0])
	_, ok := byRoute[kept]
	assert.True(t, ok, "the kept path %s must have its own durable attempt row", kept)
}

// TestFullProbeCatchupQuietExitAccounting pins the quiet-exit branch's claim
// that nothing live is untried when the fleet settles: a cap-cut overflow is
// re-offered on later ticks (n>0 keeps the tail alive), so the quiet exit
// needs no skipRest — and must not silently drop the overflow either.
func TestFullProbeCatchupQuietExitAccounting(t *testing.T) {
	f := newReachFixture(t, true)
	srv := probeTarget(t)
	enableCatchup(f, 5*time.Millisecond, 100*time.Millisecond)

	j0, err := f.router.StartFullProbe(core.FullProbeSpec{
		Target: srv.URL, Timeout: 5 * time.Second, RPS: 1000, MaxDepth: 1,
		MaxAttemptsPerLevel: 2, MaxTotalAttempts: 100, CatchupSeconds: 30,
	})
	require.NoError(t, err)
	injectGrowth(f, lateOutbound(f, "a"), lateOutbound(f, "b"),
		lateOutbound(f, "c"), lateOutbound(f, "d"))

	j := awaitFullProbe(t, f.router, j0.ID)
	assert.Equal(t, core.FullProbeDone, j.State, "job error: %s", j.Error)
	l1 := j.Levels[0]
	assert.Equal(t, 6, l1.Candidates)
	assert.Equal(t, 6, l1.Total, "the quiet exit must not strand a re-offerable overflow")
	assert.Equal(t, 0, l1.Skipped)
	assert.Equal(t, 6, j.TotalAttempts)
}

// TestFullProbeCatchupDeadlineSkipRest asserts that when the bound expires
// mid-burst, the unprobed remainder is accounted for: Candidates covers the
// whole observed fleet and the cut is visible as Skipped, not silently absent.
// The tick is slow relative to the 1s bound so the tail cannot work through
// the burst before the deadline.
func TestFullProbeCatchupDeadlineSkipRest(t *testing.T) {
	f := newReachFixture(t, true)
	srv := probeTarget(t)
	enableCatchup(f, 300*time.Millisecond, time.Hour) // never quiet, slow ticks

	j0, err := f.router.StartFullProbe(core.FullProbeSpec{
		Target: srv.URL, Timeout: 5 * time.Second, RPS: 1000, MaxDepth: 1,
		MaxAttemptsPerLevel: 1, MaxTotalAttempts: 100, CatchupSeconds: 1,
	})
	require.NoError(t, err)
	// A burst far larger than the tail can issue at 1 per 300ms tick in 1s.
	injectGrowth(f, lateOutbound(f, "a"), lateOutbound(f, "b"), lateOutbound(f, "c"),
		lateOutbound(f, "d"), lateOutbound(f, "e"), lateOutbound(f, "f"))

	j := awaitFullProbe(t, f.router, j0.ID)
	assert.Equal(t, core.FullProbeDone, j.State, "job error: %s", j.Error)
	l1 := j.Levels[0]
	assert.Equal(t, 8, l1.Candidates, "Candidates must cover the whole observed fleet even when the bound cut the tail")
	assert.Less(t, l1.Total, 8, "the 1s bound must expire before the slow tail issues the whole burst")
	assert.GreaterOrEqual(t, l1.Skipped, 1, "the deadline cut must be visible as Skipped")
	assert.Equal(t, l1.Total, l1.Done)
	assert.Equal(t, l1.Total, j.TotalAttempts)
	// Every observed candidate is accounted for as planned or skipped.
	assert.Equal(t, l1.Candidates, l1.Total+l1.Skipped)
}

// TestMergeFullProbePathsUnit pins the merge contract deterministically with
// synthetic latencies: fastest-first ordering, re-cap at the beam width, a
// faster late arrival displacing a slower beam path, and a slower late arrival
// never displacing a faster one.
func TestMergeFullProbePathsUnit(t *testing.T) {
	p := func(id string, d time.Duration) core.FullProbePath {
		return core.FullProbePath{Hops: []string{id}, Latency: d}
	}
	slow, mid, fast := p("slow", 30*time.Millisecond), p("mid", 20*time.Millisecond), p("fast", 10*time.Millisecond)

	// Re-cap keeps the fastest two of three, ordered fastest first.
	got := mergeFullProbePaths([]core.FullProbePath{slow, mid}, []core.FullProbePath{fast}, 2)
	require.Len(t, got, 2)
	assert.Equal(t, "fast", got[0].Hops[0])
	assert.Equal(t, "mid", got[1].Hops[0])

	// A faster late arrival displaces the slower beam path under a width-1 cap.
	got = mergeFullProbePaths([]core.FullProbePath{slow}, []core.FullProbePath{fast}, 1)
	require.Len(t, got, 1)
	assert.Equal(t, "fast", got[0].Hops[0])

	// A slower late arrival never displaces the faster beam path.
	got = mergeFullProbePaths([]core.FullProbePath{fast}, []core.FullProbePath{slow}, 1)
	require.Len(t, got, 1)
	assert.Equal(t, "fast", got[0].Hops[0])

	// Ties break by hop-join, deterministically.
	a1, a2 := p("a", 10*time.Millisecond), p("b", 10*time.Millisecond)
	got = mergeFullProbePaths([]core.FullProbePath{a2}, []core.FullProbePath{a1}, 1)
	require.Len(t, got, 1)
	assert.Equal(t, "a", got[0].Hops[0])

	// Both empty preserves nil (no spurious empty slice).
	assert.Nil(t, mergeFullProbePaths(nil, nil, 4))
}

// TestMergeFullProbeFailedItemsUnit pins the failure merge: disjoint IDs stay
// sorted, and the defensive combine branch aggregates Count by ID.
func TestMergeFullProbeFailedItemsUnit(t *testing.T) {
	fi := func(id string, n int, reason string) core.FullProbeFailedItem {
		return core.FullProbeFailedItem{ID: id, Count: n, Reason: reason}
	}
	got := mergeFullProbeFailedItems([]core.FullProbeFailedItem{fi("z", 1, "refused")}, []core.FullProbeFailedItem{fi("a", 2, "timeout")})
	require.Len(t, got, 2)
	assert.Equal(t, "a", got[0].ID, "sorted by ID")
	assert.Equal(t, "z", got[1].ID)

	// Overlapping ID: counts add, the newer reason/error win, and a known
	// Address survives an empty newer one (the defensive combine branch).
	old := core.FullProbeFailedItem{ID: "x", Count: 2, Reason: "old", Address: "1.2.3.4:1080", Error: "old-err"}
	add := core.FullProbeFailedItem{ID: "x", Count: 3, Reason: "new", Error: "new-err"}
	got = mergeFullProbeFailedItems([]core.FullProbeFailedItem{old}, []core.FullProbeFailedItem{add})
	require.Len(t, got, 1)
	assert.Equal(t, 5, got[0].Count)
	assert.Equal(t, "new", got[0].Reason)
	assert.Equal(t, "new-err", got[0].Error)
	assert.Equal(t, "1.2.3.4:1080", got[0].Address, "an empty newer Address must not wipe a known one")

	assert.Nil(t, mergeFullProbeFailedItems(nil, nil))
}

// TestFullProbeCatchupProductionTuning pins the production tail constants the
// seam-overriding tests above cannot see: the quiet window must exceed the
// provider manager's poll interval, or an inter-poll lull is mistaken for a
// settled fleet and the tail exits early (the exact regression the 428-vs-266
// fix exists to prevent). The tick stays prompt relative to the window.
func TestFullProbeCatchupProductionTuning(t *testing.T) {
	assert.Greater(t, fullProbeCatchupQuiet, provider.PollInterval(),
		"quiet window must outlast the provider re-poll interval")
	assert.Greater(t, fullProbeCatchupTick, time.Duration(0))
	assert.Less(t, fullProbeCatchupTick, fullProbeCatchupQuiet,
		"the tail must re-snapshot several times inside one quiet window")
}

// TestFullProbeCatchupRemovedMidSweep asserts removing outbounds after the beam
// neither panics nor corrupts the record: byID stays add-only, deeper levels
// keep their results, and the tail still covers a newcomer.
func TestFullProbeCatchupRemovedMidSweep(t *testing.T) {
	f := newReachFixture(t, true)
	srv := probeTarget(t)
	enableCatchup(f, 5*time.Millisecond, 150*time.Millisecond)

	j0, err := f.router.StartFullProbe(core.FullProbeSpec{
		Target: srv.URL, Timeout: 5 * time.Second, RPS: 1000, MaxDepth: 5,
		CatchupSeconds: 5,
	})
	require.NoError(t, err)
	// Remove the beam's outbounds and add a newcomer in one rebuild.
	f.cfg.DeleteOutbound("relay")
	f.cfg.DeleteOutbound("work")
	injectGrowth(f, lateOutbound(f, "late"))

	j := awaitFullProbe(t, f.router, j0.ID)
	assert.Equal(t, core.FullProbeDone, j.State, "job error: %s", j.Error)
	require.Len(t, j.Levels, 2, "the rescue completed before the removal")
	assert.Equal(t, []string{"relay", "work"}, j.Levels[1].SuccessPaths[0].Hops)
	assert.Equal(t, 3, j.Levels[0].Candidates)
	found := false
	for _, p := range j.Levels[0].SuccessPaths {
		if hopsOf(p) == "late" {
			found = true
		}
	}
	assert.True(t, found, "the newcomer must be a depth-1 success")
}

// TestFullProbeCatchupTerminalPersist asserts the merged level-1 record survives
// the terminal-persist retry path: after a transient first-write failure the
// durable snapshot still carries the tail's newcomer.
func TestFullProbeCatchupTerminalPersist(t *testing.T) {
	f := newReachFixture(t, true)
	srv := probeTarget(t)
	enableCatchup(f, 5*time.Millisecond, 150*time.Millisecond)
	calls := failTerminalOnce(t, f.router, func(n int64) error {
		if n == 1 {
			return errors.New("transient disk hiccup")
		}
		return nil
	})

	j0, err := f.router.StartFullProbe(core.FullProbeSpec{
		Target: srv.URL, Timeout: 5 * time.Second, RPS: 1000, MaxDepth: 1,
		CatchupSeconds: 5,
	})
	require.NoError(t, err)
	injectGrowth(f, lateOutbound(f, "late"))

	require.Eventually(t, func() bool {
		j, ok := f.ms.FullProbeJob(j0.ID)
		return ok && j.State == core.FullProbeDone
	}, 10*time.Second, 25*time.Millisecond, "terminal snapshot never landed durably")
	assert.GreaterOrEqual(t, calls.Load(), int64(2), "terminal persist must have retried")

	got, ok := f.ms.FullProbeJob(j0.ID)
	require.True(t, ok)
	assert.Equal(t, 3, got.Levels[0].Candidates, "the durable terminal snapshot must carry the merged level 1")
	found := false
	for _, p := range got.Levels[0].SuccessPaths {
		if hopsOf(p) == "late" {
			found = true
		}
	}
	assert.True(t, found, "the newcomer must survive into the durable record")
}

// TestFleetChangedAtTracksIDSet pins the rebuild tracker the tail's quiet exit
// reads: value-only churn does not reset it, but any ID-set change does —
// including a remove+add at constant count.
func TestFleetChangedAtTracksIDSet(t *testing.T) {
	f := newReachFixture(t, false)
	r := f.router
	changedAt := func() time.Time {
		r.mu.RLock()
		defer r.mu.RUnlock()
		return r.fleetChangedAt
	}

	t0 := changedAt()
	time.Sleep(2 * time.Millisecond)

	// Same-ID config churn: the ID set is unchanged, so the clock must not reset.
	f.cfg.SetOutbound(&config.OutboundSpec{ID: "relay", Protocol: "socks5", Config: map[string]any{"address": f.rAddr, "extra": 1}})
	r.Rebuild()
	assert.Equal(t, t0, changedAt(), "value-only churn must not reset the fleet clock")

	// Adding an ID resets it.
	time.Sleep(2 * time.Millisecond)
	f.cfg.SetOutbound(lateOutbound(f, "late"))
	r.Rebuild()
	t1 := changedAt()
	assert.True(t, t1.After(t0), "an added ID must reset the fleet clock")

	// Remove+add at constant count: the set differs, so the clock resets again.
	time.Sleep(2 * time.Millisecond)
	f.cfg.DeleteOutbound("work")
	f.cfg.SetOutbound(lateOutbound(f, "late2"))
	r.Rebuild()
	assert.True(t, changedAt().After(t1), "a remove+add at constant count must reset the fleet clock")
}
