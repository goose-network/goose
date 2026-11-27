// Full active probing: the multi-level sweep that discovers which proxy
// paths actually reach a target (core/fullprobe.go documents the workflow).
//
// One job runs level by level. Level 1 probes local → every outbound; each
// deeper level extends every surviving successful prefix with every failed
// item from the level before — a failed outbound relayed through a working
// one may succeed, and that is exactly what the sweep is looking for. Every
// attempt is a real HTTP request through the pinned path, dialed with the
// same dialHops machinery dispatch uses (no reachability narrowing: the
// sweep is measuring whether the path works, not routing traffic).
//
// Throughput: a worker pool of spec.Concurrency goroutines pulls attempts
// through an x/time/rate limiter capped at spec.RPS (up to 10000/s).
// Persistence is batched (RecordHTTPProbeAsync / RecordFullProbeJob), success
// rows only — failures aggregate into the level's FailedItems, so a sweep of
// mostly-dead outbounds does not write one durable row per failure.
package router

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goose-network/goose/internal/core"
	"golang.org/x/time/rate"
)

// fullProbeSeq disambiguates job IDs minted in the same nanosecond.
var fullProbeSeq atomic.Uint64

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
		return nil, fmt.Errorf("full probe: %d jobs already running (limit %d)", running, core.FullProbeMaxRunningJobs)
	}
	id := fmt.Sprintf("fp-%d-%d", time.Now().UnixNano(), fullProbeSeq.Add(1))
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
		r.runFullProbe(ctx, fp, outs)
	}()
	return fp.snapshot(), nil
}

// FullProbeJob returns one job's current state: the live copy while it runs
// (or after, until the engine restarts), else the durable snapshot.
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
// whether a running job was found.
func (r *Router) CancelFullProbe(id string) bool {
	r.fullProbeMu.Lock()
	fp := r.fullProbes[id]
	r.fullProbeMu.Unlock()
	if fp == nil {
		return false
	}
	select {
	case <-fp.done:
		return false
	default:
	}
	fp.cancel()
	return true
}

// stopFullProbes cancels every running job and joins the runners. Used by
// Close, where no router lock is held so joining is safe.
func (r *Router) stopFullProbes() {
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
func (r *Router) runFullProbe(ctx context.Context, fp *fullProbeRun, outs []core.Outbound) {
	spec := fp.job.Spec
	byID := make(map[string]core.Outbound, len(outs))
	for _, o := range outs {
		byID[o.ID()] = o
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

	fp.mu.Lock()
	fp.job.State = state
	fp.job.FinishedAt = time.Now()
	fp.mu.Unlock()
	r.persistFullProbe(fp, true)
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
	sort.Slice(successes, func(i, j int) bool {
		if successes[i].Latency != successes[j].Latency {
			return successes[i].Latency < successes[j].Latency
		}
		return strings.Join(successes[i].Hops, ">") < strings.Join(successes[j].Hops, ">")
	})
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
// issue the HTTP request through it. Success persists a probe row + metric
// row (async, batched); failure only aggregates — the level's FailedItems
// carry the counts and a sample error. The worker folds the result into the
// live counters; this function only produces the outcome.
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
		_, reason := classifyProbeFailure(out.stage, out.err, out.timings)
		res.failID, res.addr = a.cand, cand.Address()
		res.reason, res.errStr = reason, out.err.Error()
	}
	return res
}

// persistFullProbe enqueues a job snapshot for durable storage: always when
// force (level boundaries, completion), otherwise at most ~1/s so a 10000 rps
// sweep does not enqueue one snapshot per attempt.
func (r *Router) persistFullProbe(fp *fullProbeRun, force bool) {
	fp.mu.Lock()
	now := time.Now()
	if !force && now.Sub(fp.lastPersist) < time.Second {
		fp.mu.Unlock()
		return
	}
	fp.lastPersist = now
	snap := fp.job.Clone()
	fp.mu.Unlock()
	_ = r.metrics.RecordFullProbeJob(snap)
}
