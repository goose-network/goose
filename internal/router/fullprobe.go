// Full active probing: the multi-level sweep that discovers which proxy
// paths actually reach a target (core/fullprobe.go documents the workflow).
//
// One job runs level by level. Level 1 probes local → every outbound; each
// deeper level extends every surviving successful prefix with every failed
// item from the level before — a failed outbound relayed through a working
// one may succeed, and that is exactly what the sweep is looking for. Every
// attempt is a real HTTP request through the pinned path, dialed with the
// same dialHops machinery dispatch uses (no reachability narrowing: the
// sweep is measuring whether the path works, not routing traffic). After the
// level loop a bounded settling tail probes outbounds that registered late
// (provider fleets grow over minutes) at depth 1, folding them into level 1.
//
// Throughput: a worker pool of spec.Concurrency goroutines pulls attempts
// through an x/time/rate limiter capped at spec.RPS (up to 10000/s).
// Persistence is batched (RecordHTTPProbeAsync / RecordFullProbeJob). Every
// issued attempt persists a probe row — success and failure alike — linked to
// the job (FullProbeID/Depth); the store routes those rows to the job's own
// result bucket, and mirrors successes into the shared probe history. Metric
// rows stay success-only (they feed the 20000-row requests bucket); failures
// also aggregate into the level's FailedItems.
package router

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goose-network/goose/internal/core"
	"github.com/goose-network/goose/internal/metrics"
	"golang.org/x/time/rate"
)

// fullProbeSeq disambiguates job IDs minted in the same nanosecond.
var fullProbeSeq atomic.Uint64

// Settling-tail tuning. After the beam sweep, a job on a provider-backed
// engine keeps probing newly registered outbounds at depth 1 until the fleet
// has been quiet for fullProbeCatchupQuiet — longer than the provider
// manager's 2-minute poll interval, so an inter-poll lull is not mistaken for
// settle — or the spec's CatchupSeconds bound expires. Tests shrink both via
// the Router's catchupTickDur/catchupQuietDur seams.
const (
	fullProbeCatchupQuiet = 130 * time.Second
	fullProbeCatchupTick  = 5 * time.Second
)

// fullProbeResult is one attempt's outcome folded back into the level: a
// successful path, or a failure keyed by the last-hop outbound that failed.
type fullProbeResult struct {
	path   core.FullProbePath
	failID string
	addr   string
	reason string
	errStr string
}

// fullProbeRun is one job's live state: the runner owns job under mu, the
// API reads clones. cancel stops issuance; done closes when the runner exits.
type fullProbeRun struct {
	mu     sync.Mutex
	job    core.FullProbeJob
	cancel context.CancelFunc
	done   chan struct{}
	// lastPersist throttles progress snapshots to ~1/s.
	lastPersist time.Time
}

// snapshot clones the live job under the run lock.
func (fp *fullProbeRun) snapshot() *core.FullProbeJob {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	return fp.job.Clone()
}

// StartFullProbe validates spec, registers a job and launches its sweep
// detached from any caller context. It returns the job's initial snapshot;
// poll FullProbeJob for progress. The only synchronous errors are a spec
// that cannot run (bad target) and an engine with nothing to probe or too
// many jobs already running.
func (r *Router) StartFullProbe(spec core.FullProbeSpec) (*core.FullProbeJob, error) {
	spec, err := core.NormalizeFullProbeSpec(spec)
	if err != nil {
		return nil, err
	}
	outs := r.fullProbeOutbounds()
	if len(outs) == 0 {
		return nil, fmt.Errorf("full probe: no outbounds built to probe")
	}

	r.fullProbeMu.Lock()
	if r.fullProbes == nil {
		r.fullProbes = map[string]*fullProbeRun{}
	}
	running := 0
	for _, fp := range r.fullProbes {
		select {
		case <-fp.done:
		default:
			running++
		}
	}
	if running >= core.FullProbeMaxRunningJobs {
		r.fullProbeMu.Unlock()
		return nil, fmt.Errorf("%w (%d running, limit %d)", core.ErrFullProbeCapacity, running, core.FullProbeMaxRunningJobs)
	}
	id := fmt.Sprintf("fp-%d-%d", time.Now().UnixNano(), fullProbeSeq.Add(1))
	// The ID doubles as a URL path segment and as the job's row-bucket name
	// in the metrics store, so it stays '/'-free; the 19-digit nanosecond
	// prefix makes lexicographic order chronological until the year 2262 —
	// the store evicts oldest jobs first by sorting these keys.
	ctx, cancel := context.WithCancel(context.Background())
	fp := &fullProbeRun{
		job: core.FullProbeJob{
			ID:        id,
			Spec:      spec,
			State:     core.FullProbeRunning,
			StartedAt: time.Now(),
		},
		cancel: cancel,
		done:   make(chan struct{}),
	}
	r.fullProbes[id] = fp
	r.fullProbeWG.Add(1)
	r.fullProbeMu.Unlock()

	go func() {
		defer r.fullProbeWG.Done()
		defer close(fp.done)
		err := r.runFullProbe(ctx, fp, outs)
		if err != nil {
			// The terminal snapshot did not land. Keeping the live copy is
			// not enough: the durable record stays "running" (or absent), so
			// both retention paths exempt the job's row bucket forever —
			// live eviction needs terminal proof, history prune exempts
			// running snapshots — and zombie buckets would crowd out healthy
			// finished jobs. Retry until the record lands (or the store
			// closes / shutdown begins); the retry stays inside this
			// goroutine so stopFullProbes still joins it.
			log.Printf("router: full probe %s: retrying terminal snapshot persist: %v", id, err)
			backoff := 500 * time.Millisecond
		retry:
			for {
				timer := time.NewTimer(backoff)
				select {
				case <-timer.C:
				case <-r.shutdown:
					// Shutdown began joining: leave err non-nil so the live
					// copy survives for reconcileFullProbeZombies at Open.
					timer.Stop()
					break retry
				}
				err = r.persistFullProbeFn(fp, true)
				if err == nil {
					break
				}
				if errors.Is(err, metrics.ErrClosed) {
					log.Printf("router: full probe %s: giving up terminal snapshot persist: %v", id, err)
					break
				}
				// Double AFTER the wait, clamped before doubling so the
				// duration can never overflow into a hot retry loop.
				if backoff < 10*time.Second {
					backoff *= 2
					if backoff > 10*time.Second {
						backoff = 10 * time.Second
					}
				}
			}
		}
		r.fullProbeMu.Lock()
		if err == nil {
			// The terminal snapshot is durable; drop the live copy so
			// finished jobs do not accumulate in memory. GET/list serve
			// the finished job from the durable history from here on.
			delete(r.fullProbes, id)
		}
		// On give-up (closed store or shutdown) keep the live copy: evicting
		// would expose the last "running" progress snapshot forever. done
		// still closes (the runner exited), so it no longer counts as
		// running; reconcileFullProbeZombies terminalizes it at next Open.
		r.fullProbeMu.Unlock()
	}()
	return fp.snapshot(), nil
}

// FullProbeJob returns one job's current state: the live copy while it runs,
// else the durable snapshot (finished jobs are evicted from the live map).
func (r *Router) FullProbeJob(id string) (*core.FullProbeJob, bool) {
	r.fullProbeMu.Lock()
	fp := r.fullProbes[id]
	r.fullProbeMu.Unlock()
	if fp != nil {
		return fp.snapshot(), true
	}
	return r.metrics.FullProbeJob(id)
}

// FullProbeJobs merges live jobs with the durable history, newest first,
// capped at n. A live job shadows its persisted snapshot of the same ID.
func (r *Router) FullProbeJobs(n int) []*core.FullProbeJob {
	if n < 1 {
		n = 1
	}
	r.fullProbeMu.Lock()
	live := make([]*core.FullProbeJob, 0, len(r.fullProbes))
	seen := make(map[string]bool, len(r.fullProbes))
	for id, fp := range r.fullProbes {
		live = append(live, fp.snapshot())
		seen[id] = true
	}
	r.fullProbeMu.Unlock()
	out := live
	if persisted, err := r.metrics.RecentFullProbeJobs(n); err == nil {
		for _, j := range persisted {
			if !seen[j.ID] {
				out = append(out, j)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.After(out[j].StartedAt) })
	if len(out) > n {
		out = out[:n]
	}
	return out
}

// CancelFullProbe stops a running job's issuance; in-flight attempts finish
// under their own timeouts and the job lands in state "canceled". It reports
// whether a running job was found. A job whose runner has exited (done
// closed) or whose snapshot is already terminal is not cancelable — checked
// by state, not just liveness, so the window between the durable terminal
// write and the live-map eviction cannot report a stale "running".
func (r *Router) CancelFullProbe(id string) bool {
	r.fullProbeMu.Lock()
	fp := r.fullProbes[id]
	r.fullProbeMu.Unlock()
	if fp == nil {
		return false
	}
	// Check and cancel under the run lock: the runner stores its terminal
	// state under the same lock and re-checks ctx.Err() there, so a true
	// return guarantees the job lands canceled, and a job that already
	// stored a terminal state returns false. fp.cancel only signals the
	// context — it never takes the run lock, so holding it here is safe.
	fp.mu.Lock()
	defer fp.mu.Unlock()
	select {
	case <-fp.done:
		return false
	default:
	}
	if fp.job.State != core.FullProbeRunning {
		return false
	}
	fp.cancel()
	return true
}

// stopFullProbes cancels every running job and joins the runners. Used by
// Close, where no router lock is held so joining is safe. shutdown closes
// BEFORE the join so a runner retrying its terminal persist exits within one
// backoff tick instead of holding Close open; the Once keeps a second Close
// from double-closing the channel.
func (r *Router) stopFullProbes() {
	r.shutdownOnce.Do(func() { close(r.shutdown) })
	r.fullProbeMu.Lock()
	for _, fp := range r.fullProbes {
		fp.cancel()
	}
	r.fullProbeMu.Unlock()
	r.fullProbeWG.Wait()
}

// fullProbeOutbounds snapshots every built outbound, sorted by ID. Pin-only
// outbounds are included: the sweep is an explicit measurement of every
// configured path, not auto-selection. "direct" is included for the depth-1
// baseline but never extended or retried (it has no server to relay through).
func (r *Router) fullProbeOutbounds() []core.Outbound {
	r.mu.RLock()
	ids := make([]string, 0, len(r.outbounds))
	for id := range r.outbounds {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	outs := make([]core.Outbound, 0, len(ids))
	for _, id := range ids {
		outs = append(outs, r.outbounds[id])
	}
	r.mu.RUnlock()
	return outs
}

// fullProbeAttempt is one planned (prefix, candidate) pair: the pinned path
// local → prefix hops → candidate, dialed toward the job's target.
type fullProbeAttempt struct {
	prefix []string // outbound IDs of the surviving prefix (empty at depth 1)
	cand   string   // the outbound appended as this attempt's last hop
}

// runFullProbe drives one job's level loop. It owns fp.job's mutations under
// fp.mu and persists progress snapshots at ~1/s plus every level boundary.
// It returns the terminal persist's error: nil means the finished job is
// durable and its live copy can be evicted.
func (r *Router) runFullProbe(ctx context.Context, fp *fullProbeRun, outs []core.Outbound) error {
	spec := fp.job.Spec
	byID := make(map[string]core.Outbound, len(outs))
	for _, o := range outs {
		byID[o.ID()] = o
	}
	// tried is the depth-1 candidate universe: every outbound offered at
	// level 1, start snapshot plus anything the settling tail takes in. It
	// keeps the tail from probing an ID twice, and byID stays add-only so
	// fullProbeOne's lookupOutbound→byID fallback never sees a nil hop even
	// when an outbound is removed mid-job.
	tried := make(map[string]bool, len(outs))
	for _, o := range outs {
		tried[o.ID()] = true
	}

	// prefixes are the successful paths the next level extends; candidates
	// the failed items it retries. Depth 1 starts from the empty local prefix
	// and every outbound.
	prefixes := [][]string{nil}
	var candidates []string
	for _, o := range outs {
		candidates = append(candidates, o.ID())
	}

	state := core.FullProbeDone
	for depth := 1; depth <= spec.MaxDepth; depth++ {
		if ctx.Err() != nil {
			state = core.FullProbeCanceled
			break
		}
		// Plan the level's fan-out: every prefix × every candidate, minus
		// cycles (a candidate already in the prefix) and unresolvable hops.
		// The caps bound the plan, not the dial loop, so Skipped is an honest
		// count of pairs the sweep chose not to try.
		attempts := make([]fullProbeAttempt, 0)
		skipped := 0
		fp.mu.Lock()
		total := fp.job.TotalAttempts
		fp.mu.Unlock()
		budget := spec.MaxAttemptsPerLevel
		if remain := spec.MaxTotalAttempts - total; remain < budget {
			budget = remain
		}
		for _, prefix := range prefixes {
			for _, cand := range candidates {
				if budget <= 0 {
					skipped++
					continue
				}
				if !fullProbeResolvable(byID, prefix, cand) {
					skipped++
					continue
				}
				attempts = append(attempts, fullProbeAttempt{prefix: prefix, cand: cand})
				budget--
			}
		}
		if len(attempts) == 0 {
			break
		}

		level := core.FullProbeLevel{
			Depth:      depth,
			Prefixes:   len(prefixes),
			Candidates: len(candidates),
			Total:      len(attempts),
			Skipped:    skipped,
			StartedAt:  time.Now(),
		}
		fp.mu.Lock()
		fp.job.Levels = append(fp.job.Levels, level)
		li := len(fp.job.Levels) - 1
		fp.mu.Unlock()
		r.persistFullProbe(fp, true)

		successes, failures := r.runFullProbeLevel(ctx, fp, spec, li, attempts, byID, fp.job.ID)

		fp.mu.Lock()
		l := &fp.job.Levels[li]
		l.SuccessPaths = successes
		l.FailedItems = failures
		l.FinishedAt = time.Now()
		fp.mu.Unlock()
		r.persistFullProbe(fp, true)

		if ctx.Err() != nil {
			state = core.FullProbeCanceled
			break
		}
		fp.mu.Lock()
		total = fp.job.TotalAttempts
		fp.mu.Unlock()
		if len(successes) == 0 || len(failures) == 0 || total >= spec.MaxTotalAttempts {
			break
		}
		// Next level: extend the surviving paths with the failed items.
		// "direct" never relays (no server of its own), so it drops out of
		// both sets past the baseline.
		nextPrefixes := make([][]string, 0, len(successes))
		for _, p := range successes {
			if len(p.Hops) == 1 && byID[p.Hops[0]].Protocol() == "direct" {
				continue
			}
			nextPrefixes = append(nextPrefixes, p.Hops)
		}
		nextCandidates := make([]string, 0, len(failures))
		for _, f := range failures {
			if o, ok := byID[f.ID]; ok && o.Protocol() != "direct" {
				nextCandidates = append(nextCandidates, f.ID)
			}
		}
		if len(nextPrefixes) == 0 || len(nextCandidates) == 0 {
			break
		}
		prefixes, candidates = nextPrefixes, nextCandidates
	}

	// The beam measured the fleet as it was at start. Provider fleets keep
	// growing for minutes after boot — server lists download in bursts — so
	// ending here would report only the outbounds that existed when the job
	// started (the 428-vs-266 depth-1 gap). The settling tail probes late
	// arrivals at depth 1 and folds them into level 1; bounded, cancelable,
	// and skipped for static fleets. A cancel during the tail still lands
	// canceled via the terminal store's ctx re-check below.
	if ctx.Err() == nil {
		r.fullProbeTopUp(ctx, fp, spec, byID, tried)
	}

	// Re-check cancellation under the same lock that stores the terminal
	// state: a cancel arriving after the loop's last ctx check but before
	// this store must still land as canceled — CancelFullProbe returned 204
	// under fp.mu, so the state it observed running has to become canceled.
	fp.mu.Lock()
	if ctx.Err() != nil {
		state = core.FullProbeCanceled
	}
	fp.job.State = state
	fp.job.FinishedAt = time.Now()
	fp.mu.Unlock()
	// Through the seam so tests can fail the first terminal write; in
	// production this is exactly r.persistFullProbe(fp, true).
	return r.persistFullProbeFn(fp, true)
}

// fullProbeTopUp chases outbounds registered after the sweep started. With
// providers configured the fleet can grow for minutes (server-list downloads
// land in bursts), so the tail keeps taking depth-1 top-up passes until the
// fleet has been quiet for the quiet window or spec.CatchupSeconds expires.
// Static fleets (no providers) and an opted-out tail (CatchupSeconds <= 0)
// get exactly one final pass — enough to catch anything that arrived during
// the beam itself, with no added latency when nothing did. An already-settled
// fleet exits on the first pass: nothing new, quiet window elapsed.
//
// The tail never extends the beam: late arrivals are measured local→X only
// (depth 1) and folded into Levels[0]. Rescuing a late arrival behind a
// working relay is what re-running a deep sweep after settle is for.
func (r *Router) fullProbeTopUp(ctx context.Context, fp *fullProbeRun, spec core.FullProbeSpec, byID map[string]core.Outbound, tried map[string]bool) {
	if len(r.cfg.Providers()) == 0 || spec.CatchupSeconds <= 0 {
		r.fullProbeTopUpPass(ctx, fp, spec, byID, tried, true)
		return
	}
	tick, quiet := r.catchupTickDur, r.catchupQuietDur
	if tick <= 0 {
		tick = fullProbeCatchupTick
	}
	if quiet <= 0 {
		quiet = fullProbeCatchupQuiet
	}
	deadline := time.Now().Add(time.Duration(spec.CatchupSeconds) * time.Second)
	for {
		n := r.fullProbeTopUpPass(ctx, fp, spec, byID, tried, false)
		if ctx.Err() != nil {
			return
		}
		fp.mu.Lock()
		total := fp.job.TotalAttempts
		fp.mu.Unlock()
		if total >= spec.MaxTotalAttempts {
			return // attempt budget dry
		}
		r.mu.RLock()
		changed := r.fleetChangedAt
		r.mu.RUnlock()
		now := time.Now()
		if n == 0 && now.Sub(changed) >= quiet {
			// Fleet settled: nothing live is untried. The quiet clock only
			// starts once the fleet stops changing, and any cap overflow is
			// re-offered on the next tick (n>0) long before the quiet window
			// elapses — so there is nothing left to account for here.
			return // fleet settled
		}
		if !now.Before(deadline) {
			// Bound expired. Account for whatever the tail never got to so
			// the record stays honest: coverage is partial and the counters
			// say so (Candidates = whole observed fleet, Skipped = unprobed).
			r.fullProbeSkipRest(fp, byID, tried)
			return
		}
		timer := time.NewTimer(tick)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return
		}
	}
}

// fullProbeSkipRest accounts for live outbounds the tail never issued (the
// bound expired mid-burst): marks them tried and Skipped without probing, so
// Candidates covers the whole observed fleet and the cut is visible.
func (r *Router) fullProbeSkipRest(fp *fullProbeRun, byID map[string]core.Outbound, tried map[string]bool) {
	skipped := 0
	for _, o := range r.fullProbeOutbounds() {
		if !tried[o.ID()] {
			byID[o.ID()] = o
			tried[o.ID()] = true
			skipped++
		}
	}
	if skipped == 0 {
		return
	}
	fp.mu.Lock()
	if len(fp.job.Levels) > 0 {
		fp.job.Levels[0].Skipped += skipped
		fp.job.Levels[0].Candidates = len(tried)
	}
	fp.mu.Unlock()
	r.persistFullProbe(fp, true)
}

// fullProbeTopUpPass probes every live outbound never offered at depth 1 and
// folds the results into level 1 (the beam's depth-1 record): planned/skipped
// counters, fastest-first success paths re-capped at the beam width, and
// failures aggregated by ID. It returns how many new outbounds it took in
// (0 = the fleet has nothing new). byID and tried are add-only and owned by
// the runner goroutine; the level's workers join before the merge, so the
// maps are never mutated concurrently with a dial.
//
// final marks the last pass (static fleet or opted-out tail): there is no
// next tick, so every cap-cut is final and honestly Skipped. On a non-final
// pass the per-pass allowance (MaxAttemptsPerLevel) resets next tick, so a
// burst larger than one pass is re-offered later while the TOTAL budget has
// room — only a dry total budget makes the cut permanent.
func (r *Router) fullProbeTopUpPass(ctx context.Context, fp *fullProbeRun, spec core.FullProbeSpec, byID map[string]core.Outbound, tried map[string]bool, final bool) int {
	if ctx.Err() != nil {
		return 0
	}
	live := r.fullProbeOutbounds()
	var fresh []core.Outbound
	for _, o := range live {
		if !tried[o.ID()] {
			fresh = append(fresh, o)
		}
	}
	if len(fresh) == 0 {
		return 0
	}
	fp.mu.Lock()
	if len(fp.job.Levels) == 0 {
		// Defensive: an un-canceled beam always records level 1 before it
		// can exit, so there is nothing to fold into only in a state the
		// ctx check above already rules out.
		fp.mu.Unlock()
		return 0
	}
	total := fp.job.TotalAttempts
	fp.mu.Unlock()
	remain := spec.MaxTotalAttempts - total
	budget := spec.MaxAttemptsPerLevel
	if remain < budget {
		budget = remain
	}
	attempts := make([]fullProbeAttempt, 0, len(fresh))
	skipped := 0
	for _, o := range fresh {
		if budget <= 0 {
			if final || remain <= 0 {
				// No next pass, or the total budget is dry: the cut is
				// final — mark the rest tried and honestly Skipped.
				byID[o.ID()] = o
				tried[o.ID()] = true
				skipped++
			}
			// Otherwise the per-pass allowance resets next tick: leave the
			// overflow untried so a later pass re-offers it.
			continue
		}
		byID[o.ID()] = o
		tried[o.ID()] = true
		attempts = append(attempts, fullProbeAttempt{cand: o.ID()})
		budget--
		remain--
	}
	fp.mu.Lock()
	l := &fp.job.Levels[0]
	l.Total += len(attempts)
	l.Skipped += skipped
	l.Candidates = len(tried)
	fp.mu.Unlock()
	r.persistFullProbe(fp, true)
	if len(attempts) == 0 {
		return len(fresh)
	}

	// li=0: the level's workers fold Done/Success/Failed/TotalAttempts (and
	// any cancel-cut Skipped) into Levels[0] exactly as they did for the
	// beam's depth-1 pass.
	successes, failures := r.runFullProbeLevel(ctx, fp, spec, 0, attempts, byID, fp.job.ID)
	fp.mu.Lock()
	l = &fp.job.Levels[0]
	l.SuccessPaths = mergeFullProbePaths(l.SuccessPaths, successes, spec.MaxPathsPerLevel)
	l.FailedItems = mergeFullProbeFailedItems(l.FailedItems, failures)
	l.FinishedAt = time.Now()
	fp.mu.Unlock()
	r.persistFullProbe(fp, true)
	return len(fresh)
}

// fullProbePathFaster orders paths fastest first, ties by hop-join — the
// beam's ordering, shared by the level cap and the top-up merge.
func fullProbePathFaster(a, b core.FullProbePath) bool {
	if a.Latency != b.Latency {
		return a.Latency < b.Latency
	}
	return strings.Join(a.Hops, ">") < strings.Join(b.Hops, ">")
}

// mergeFullProbePaths merges a top-up pass's successes into the level's
// existing record, fastest first, re-capped at the beam width. A late path
// can displace an earlier one from the capped record; the displaced path's
// attempt row stays durable in the results view, and deeper levels already
// ran with the prefixes they had.
func mergeFullProbePaths(existing, add []core.FullProbePath, max int) []core.FullProbePath {
	if len(existing)+len(add) == 0 {
		return existing
	}
	out := make([]core.FullProbePath, 0, len(existing)+len(add))
	out = append(out, existing...)
	out = append(out, add...)
	sort.Slice(out, func(i, j int) bool { return fullProbePathFaster(out[i], out[j]) })
	if len(out) > max {
		out = out[:max]
	}
	return out
}

// mergeFullProbeFailedItems merges a top-up pass's failures into the level's
// aggregate, sorted by ID. Fresh IDs never overlap the beam's (tried keeps
// depth-1 issuance unique); the combine branch is defensive.
func mergeFullProbeFailedItems(existing, add []core.FullProbeFailedItem) []core.FullProbeFailedItem {
	if len(existing)+len(add) == 0 {
		return existing
	}
	byID := make(map[string]*core.FullProbeFailedItem, len(existing)+len(add))
	for _, f := range existing {
		cp := f
		byID[f.ID] = &cp
	}
	for _, f := range add {
		if e, ok := byID[f.ID]; ok {
			e.Count += f.Count
			e.Reason, e.Error = f.Reason, f.Error
			if f.Address != "" {
				e.Address = f.Address
			}
			continue
		}
		cp := f
		byID[f.ID] = &cp
	}
	out := make([]core.FullProbeFailedItem, 0, len(byID))
	for _, f := range byID {
		out = append(out, *f)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// fullProbeResolvable reports whether every hop of prefix+cand still exists
// and the candidate is not already in the prefix (a cycle would re-dial a
// server the path already rode).
func fullProbeResolvable(byID map[string]core.Outbound, prefix []string, cand string) bool {
	if _, ok := byID[cand]; !ok {
		return false
	}
	for _, id := range prefix {
		if id == cand {
			return false
		}
		if _, ok := byID[id]; !ok {
			return false
		}
	}
	return true
}

// runFullProbeLevel issues one level's attempts through the worker pool and
// rate limiter, returning the level's successful paths (fastest first, beam
// capped) and its failures aggregated by last-hop outbound.
func (r *Router) runFullProbeLevel(ctx context.Context, fp *fullProbeRun, spec core.FullProbeSpec, li int, attempts []fullProbeAttempt, byID map[string]core.Outbound, jobID string) ([]core.FullProbePath, []core.FullProbeFailedItem) {
	limiter := rate.NewLimiter(rate.Limit(spec.RPS), fullProbeBurst(spec.RPS))
	var next atomic.Int64
	var issued atomic.Int64

	results := make(chan fullProbeResult, spec.Concurrency)

	var wg sync.WaitGroup
	for w := 0; w < spec.Concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= len(attempts) {
					return
				}
				if ctx.Err() != nil {
					return
				}
				if err := limiter.Wait(ctx); err != nil {
					return
				}
				issued.Add(1)
				a := attempts[i]
				res := r.fullProbeOne(ctx, spec, a, byID, jobID)
				// Fold the outcome into the live counters BEFORE handing the
				// result on: the level loop may observe the last result and
				// finish the level, and Done must already account for it.
				fp.mu.Lock()
				l := &fp.job.Levels[li]
				l.Done++
				if res.failID == "" {
					l.Success++
				} else {
					l.Failed++
				}
				fp.job.TotalAttempts++
				fp.mu.Unlock()
				r.persistFullProbe(fp, false)
				select {
				case results <- res:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	var successes []core.FullProbePath
	failures := map[string]*core.FullProbeFailedItem{}
	for res := range results {
		if res.failID == "" {
			successes = append(successes, res.path)
			continue
		}
		f := failures[res.failID]
		if f == nil {
			f = &core.FullProbeFailedItem{ID: res.failID, Address: res.addr}
			failures[res.failID] = f
		}
		f.Count++
		f.Reason, f.Error = res.reason, res.errStr
	}

	// Attempts the cancel cut before issue are Skipped, not failed: a request
	// that never left must not stand in history as a dead route.
	if cut := len(attempts) - int(issued.Load()); cut > 0 {
		fp.mu.Lock()
		fp.job.Levels[li].Skipped += cut
		fp.mu.Unlock()
	}

	// Beam: keep the fastest paths as next-level prefixes, and as what
	// inbound creation builds from.
	sort.Slice(successes, func(i, j int) bool { return fullProbePathFaster(successes[i], successes[j]) })
	if len(successes) > spec.MaxPathsPerLevel {
		successes = successes[:spec.MaxPathsPerLevel]
	}
	items := make([]core.FullProbeFailedItem, 0, len(failures))
	for _, f := range failures {
		items = append(items, *f)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	return successes, items
}

// fullProbeBurst sizes the limiter's burst: a tenth of the rate (at least 1),
// so a 10000 rps job absorbs a 1000-request scheduling hiccup without
// throttling below its configured rate, while a 1 rps job stays at 1.
func fullProbeBurst(rps int) int {
	b := rps / 10
	if b < 1 {
		b = 1
	}
	return b
}

// fullProbeOne performs one attempt: dial the pinned path with dialHops and
// issue the HTTP request through it. Both outcomes persist a probe row
// (async, batched) linked to the job — the store keeps it in the job's result
// bucket, mirroring successes into the shared probe history — while only a
// success adds a metric row. Failures also aggregate into the level's
// FailedItems (counts and a sample error). The worker folds the result into
// the live counters; this function only produces the outcome.
func (r *Router) fullProbeOne(ctx context.Context, spec core.FullProbeSpec, a fullProbeAttempt, byID map[string]core.Outbound, jobID string) fullProbeResult {
	var res fullProbeResult
	hops := make([]core.Outbound, 0, len(a.prefix)+1)
	ids := make([]string, 0, len(a.prefix)+1)
	for _, id := range a.prefix {
		o, ok := r.lookupOutbound(id)
		if !ok {
			o = byID[id]
		}
		hops = append(hops, o)
		ids = append(ids, id)
	}
	cand, ok := r.lookupOutbound(a.cand)
	if !ok {
		cand = byID[a.cand]
	}
	hops = append(hops, cand)
	ids = append(ids, a.cand)

	pctx, cancel := context.WithTimeout(ctx, spec.Timeout)
	defer cancel()
	start := time.Now()
	httpSpec := core.HTTPProbeSpec{
		Targets: []string{spec.Target},
		Method:  spec.Method,
		Headers: spec.Headers,
		Body:    spec.Body,
		Timeout: spec.Timeout,
	}
	out := r.performProbeAttempt(pctx, httpSpec, spec.Target, spec.Timeout, 0, func(ctx context.Context, network, addr string) (net.Conn, error) {
		core.PathRecorderFromContext(ctx).NoteLocalDial()
		meta := core.Metadata{Network: core.Network(network), StartedAt: start}
		if host, port, err := net.SplitHostPort(addr); err == nil {
			meta.TargetHost, meta.TargetPort = host, port
		}
		meta.Path = core.PathRecorderFromContext(ctx)
		conn, _, err := r.dialHops(ctx, hops, meta)
		return conn, err
	})
	latency := time.Since(start)
	depth := len(ids)

	if out.err == nil {
		// Success: a response arrived through the whole pinned path. Persist
		// the evidence (probe row + metric row, both batched) and keep the
		// path for the next level and for inbound creation.
		route := strings.Join(ids, ">")
		p := assembleProbe(httpSpec, spec.Target, route, start, out)
		p.FullProbeID, p.Depth = jobID, depth
		_ = r.metrics.RecordHTTPProbeAsync(p)
		meta := core.Metadata{Network: core.NetworkTCP, StartedAt: start}
		if u, uerr := url.Parse(spec.Target); uerr == nil && u.Host != "" {
			meta.TargetHost, meta.TargetPort = probeTargetHostPort(spec.Target)
		}
		chain := append([]string{core.HopProbe}, ids...)
		r.recordRow(meta, chain, true, nil, latency, "", p.Path)
		res.path = core.FullProbePath{Hops: ids, Latency: latency, Status: out.status}
	} else {
		// Failure: persist the attempt's evidence too (Error/Phase/Reason,
		// timings and the partial path), linked to the job like the success
		// row — assembleProbe measures the same span from start, so both
		// outcomes carry a consistent Latency. No metric row: a failure may
		// be the target's, not the route's, and thousands of dead candidates
		// would flood the requests bucket. A cancel that cut the attempt
		// before issue never reaches here (Skipped); one that cut it
		// in-flight genuinely ran and lands with reason "canceled".
		route := strings.Join(ids, ">")
		p := assembleProbe(httpSpec, spec.Target, route, start, out)
		p.FullProbeID, p.Depth = jobID, depth
		_ = r.metrics.RecordHTTPProbeAsync(p)
		_, reason := classifyProbeFailure(out.stage, out.err, out.timings)
		res.failID, res.addr = a.cand, cand.Address()
		res.reason, res.errStr = reason, out.err.Error()
	}
	return res
}

// persistFullProbe persists a job snapshot: always when force (level
// boundaries, completion), otherwise at most ~1/s so a 10000 rps sweep does
// not enqueue one snapshot per attempt. Progress snapshots ride the lossy
// batched pipeline; the terminal snapshot is written synchronously by the
// store. It returns the store's error (nil when throttled); the runner
// retries a failed terminal persist with backoff until it lands (the retry
// loop logs, so a per-attempt log here would repeat every tick).
func (r *Router) persistFullProbe(fp *fullProbeRun, force bool) error {
	fp.mu.Lock()
	now := time.Now()
	if !force && now.Sub(fp.lastPersist) < time.Second {
		fp.mu.Unlock()
		return nil
	}
	fp.lastPersist = now
	snap := fp.job.Clone()
	fp.mu.Unlock()
	return r.metrics.RecordFullProbeJob(snap)
}
