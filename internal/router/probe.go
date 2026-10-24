// Reachability probing for auto-chained pools (pools with an access filter).
//
// The local network can often reach only some of a pool's proxy servers
// directly — an ISP may block the rest. The prober measures, for every
// member of every access-filtered pool:
//
//   - direct: can the engine dial the member's server itself?
//   - relay: when not, can it reach the server through each access-pool
//     candidate, and which one worked?
//
// Results drive dispatch-time chain building (see dialChain): members
// reachable directly are dialed single-hop; members reachable only through
// an access proxy get a relay hop prepended; members reachable neither way
// are dropped from the pool's candidates. Results are also exposed on the
// admin API (GET /api/chains/probes) for visibility.
//
// Probing dials exactly the transports dispatch will use: the direct probe
// is a stack dial to the member's address (what a first hop does), and the
// relay probe is an access outbound's DialContext toward the member's
// address (the CONNECT tunnel the relay hop provides).
package router

import (
	"context"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/goose-network/goose/internal/core"
)

// probeEntry is the latest probe verdicts for one outbound.
type probeEntry struct {
	poolID   string
	address  string
	direct   core.ProbeVerdict
	relay    core.ProbeVerdict
	relayVia string
}

// probeCtl controls the probe loop's lifecycle and parameters.
type probeCtl struct {
	cancel   context.CancelFunc
	done     chan struct{}
	signal   chan struct{} // refresh nudge (buffered 1, coalesced)
	interval time.Duration
	timeout  time.Duration
}

// probeConcurrency caps parallel probe dials per round so a large pool
// doesn't produce a burst of simultaneous connections.
const probeConcurrency = 8

// setProberLocked ensures the probe loop is running with the given
// parameters, restarting it when they changed. Caller holds r.mu (the write
// lock): restart cancels the old loop without joining, since a probe round
// may be mid-dial and must not block a rebuild.
func (r *Router) setProberLocked(interval, timeout time.Duration) {
	r.probeCtlMu.Lock()
	defer r.probeCtlMu.Unlock()
	if r.probeCtl != nil && r.probeCtl.interval == interval && r.probeCtl.timeout == timeout {
		select {
		case r.probeCtl.signal <- struct{}{}:
		default:
		}
		return
	}
	if r.probeCtl != nil {
		r.probeCtl.cancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	ctl := &probeCtl{
		cancel:   cancel,
		done:     make(chan struct{}),
		signal:   make(chan struct{}, 1),
		interval: interval,
		timeout:  timeout,
	}
	r.probeCtl = ctl
	go r.probeLoop(ctx, ctl)
}

// stopProber stops the probe loop and waits for it to exit. Used by Close,
// where no router lock is held so joining is safe.
func (r *Router) stopProber() {
	r.probeCtlMu.Lock()
	ctl := r.probeCtl
	r.probeCtl = nil
	r.probeCtlMu.Unlock()
	if ctl != nil {
		ctl.cancel()
		<-ctl.done
	}
}

// reconcileProberLocked starts/stops/re-parameters the probe loop to match
// the current pool configs: running iff at least one pool has an access
// filter, with the smallest interval and timeout across those pools. Caller
// holds r.mu.
func (r *Router) reconcileProberLocked() {
	var interval, timeout time.Duration
	want := false
	for _, bp := range r.pools {
		if bp.access == nil {
			continue
		}
		iv := time.Duration(bp.access.EffectiveProbeIntervalSec()) * time.Second
		tv := time.Duration(bp.access.EffectiveProbeTimeoutSec()) * time.Second
		if !want || iv < interval {
			interval = iv
		}
		if !want || tv < timeout {
			timeout = tv
		}
		want = true
	}
	if !want {
		r.probeCtlMu.Lock()
		ctl := r.probeCtl
		r.probeCtl = nil
		r.probeCtlMu.Unlock()
		if ctl != nil {
			ctl.cancel() // no join: see setProberLocked
		}
		return
	}
	r.setProberLocked(interval, timeout)
}

// probeLoop runs probe rounds on the interval and on refresh nudges (a
// config rebuild signals a round so newly added outbounds are probed
// promptly instead of waiting a full interval).
func (r *Router) probeLoop(ctx context.Context, ctl *probeCtl) {
	defer close(ctl.done)
	r.probeRound(ctx, ctl)
	tick := time.NewTicker(ctl.interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			r.probeRound(ctx, ctl)
		case <-ctl.signal:
			for { // coalesce pending signals
				select {
				case <-ctl.signal:
					continue
				default:
				}
				break
			}
			r.probeRound(ctx, ctl)
		}
	}
}

// memberJob is one outbound to probe: the outbound, the pool whose access
// filter asked for it, and that pool's access-proxy candidates.
type memberJob struct {
	poolID string
	out    core.Outbound
	access []core.Outbound
}

// probeRound probes every member of every access-filtered pool. Results
// replace the previous round's entries wholesale, so entries for outbounds
// that no longer exist (or pools that lost their access filter) disappear.
func (r *Router) probeRound(ctx context.Context, ctl *probeCtl) {
	r.mu.RLock()
	var jobs []memberJob
	for _, bp := range r.pools {
		if bp.access == nil || bp.id == bp.access.AccessPoolID {
			continue
		}
		var accessOuts []core.Outbound
		if ap, ok := r.pools[bp.access.AccessPoolID]; ok && ap != bp {
			accessOuts = ap.outbounds
		}
		for _, o := range bp.outbounds {
			if o.Address() == "" || o.ID() == "" {
				// No dial endpoint (direct / tunnel outbounds): nothing to
				// probe; treated as locally reachable at dispatch.
				continue
			}
			jobs = append(jobs, memberJob{poolID: bp.id, out: o, access: accessOuts})
		}
	}
	r.mu.RUnlock()
	if len(jobs) == 0 {
		r.probeMu.Lock()
		r.probes = map[string]*probeEntry{}
		r.probeMu.Unlock()
		return
	}

	type result struct {
		id string
		e  probeEntry
	}
	results := make([]result, len(jobs))
	var wg sync.WaitGroup
	sem := make(chan struct{}, probeConcurrency)
	for i, j := range jobs {
		wg.Add(1)
		go func(i int, j memberJob) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			id, e := r.probeOne(ctx, ctl.timeout, j)
			results[i] = result{id: id, e: e}
		}(i, j)
	}
	wg.Wait()

	next := make(map[string]*probeEntry, len(results))
	for _, res := range results {
		if res.id == "" {
			continue
		}
		e := res.e
		next[res.id] = &e
	}
	r.probeMu.Lock()
	r.probes = next
	r.probeMu.Unlock()
}

// probeOne probes one member: direct dial to its server and, when that
// fails, a dial through each access candidate until one succeeds.
func (r *Router) probeOne(ctx context.Context, timeout time.Duration, j memberJob) (string, probeEntry) {
	e := probeEntry{poolID: j.poolID, address: j.out.Address()}

	dctx, dcancel := context.WithTimeout(ctx, timeout)
	defer dcancel()
	start := time.Now()
	conn, err := r.stack.Dial(dctx, "tcp", j.out.Address())
	lat := time.Since(start)
	if err == nil {
		_ = conn.Close()
		e.direct = core.ProbeVerdict{OK: true, Latency: lat, At: start}
		return j.out.ID(), e
	}
	e.direct = core.ProbeVerdict{Latency: lat, Error: err.Error(), At: start}

	for _, a := range j.access {
		if a.ID() == "" || a.ID() == j.out.ID() {
			continue
		}
		rctx, rcancel := context.WithTimeout(ctx, timeout)
		start := time.Now()
		conn, err := a.DialContext(rctx, core.NetworkTCP, j.out.Address())
		lat := time.Since(start)
		rcancel()
		if err == nil {
			_ = conn.Close()
			e.relay = core.ProbeVerdict{OK: true, Latency: lat, At: start}
			e.relayVia = a.ID()
			return j.out.ID(), e
		}
		e.relay = core.ProbeVerdict{Latency: lat, Error: err.Error(), At: start}
	}
	return j.out.ID(), e
}

// reachableCandidates drops members whose latest probe failed both directly
// and through the access pool — they are known-unreachable from this
// network. Members with no probe result yet are kept (fail-open) so a fresh
// config still routes while the first probe round runs.
func (r *Router) reachableCandidates(pool []core.Outbound) []core.Outbound {
	r.probeMu.RLock()
	defer r.probeMu.RUnlock()
	out := pool[:0:0]
	for _, o := range pool {
		if e, ok := r.probes[o.ID()]; ok && !e.direct.OK && !e.relay.OK {
			continue
		}
		out = append(out, o)
	}
	return out
}

// relayFor returns the access outbound to prepend when the given outbound is
// dialed as a first hop: its probe found the server unreachable directly but
// reachable through relayVia. Returns nil when no relay applies.
func (r *Router) relayFor(o core.Outbound) core.Outbound {
	if o.Address() == "" {
		return nil // no server address: nothing to relay toward
	}
	r.probeMu.RLock()
	e, ok := r.probes[o.ID()]
	via := ""
	if ok && !e.direct.OK && e.relay.OK {
		via = e.relayVia
	}
	r.probeMu.RUnlock()
	if via == "" || via == o.ID() {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.outbounds[via]
}

// ChainProbeStatus returns the latest probe results for every probed
// outbound, sorted by outbound id. Implements the API's probe reporter.
func (r *Router) ChainProbeStatus() []core.ProbeStatus {
	r.probeMu.RLock()
	defer r.probeMu.RUnlock()
	out := make([]core.ProbeStatus, 0, len(r.probes))
	for id, e := range r.probes {
		out = append(out, core.ProbeStatus{
			OutboundID: id,
			Address:    e.address,
			PoolID:     e.poolID,
			Direct:     e.direct,
			Relay:      e.relay,
			RelayVia:   e.relayVia,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].OutboundID < out[j].OutboundID })
	return out
}

// CheckOutbounds returns every built outbound, sorted by id, for the exit-IP
// checker (which filters to those with a dial address). Implements exitip's
// outbound source.
func (r *Router) CheckOutbounds() []core.Outbound {
	r.mu.RLock()
	out := make([]core.Outbound, 0, len(r.outbounds))
	for _, o := range r.outbounds {
		out = append(out, o)
	}
	r.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID() < out[j].ID() })
	return out
}

// CheckDial dials target through out exactly the way dispatch dials out as a
// first hop — including prepending the auto-chain relay hop when out's server
// is only reachable through the access pool — so exit-IP checks measure the
// path real traffic takes rather than a bypassing tunnel.
func (r *Router) CheckDial(ctx context.Context, out core.Outbound, network core.Network, target string) (net.Conn, error) {
	if relay := r.relayFor(out); relay != nil {
		return r.dialRelayed(ctx, out, relay, core.Metadata{Network: network, StartedAt: time.Now()}, target)
	}
	return out.DialContext(ctx, network, target)
}
