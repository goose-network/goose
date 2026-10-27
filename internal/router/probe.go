// Reachability probing of outbound proxy servers.
//
// The local network can often reach only some of the configured proxy
// servers directly — an ISP may block the rest. Rather than hiding a relay
// ("access") pool in the config model, the prober measures reachability and
// records it where every other measurement lives: the outbound metrics. Each
// round dials every outbound's server from the engine stack and records a
// metric row whose Chain is [core.HopLocal, outboundID] and whose Target is
// the server address — the same table that holds single-hop request rows and
// chain rows, so one metrics surface describes every measured path.
//
// Routing then filters by those metrics: a pool's first-hop candidates are
// narrowed to outbounds whose latest local verdict is not a failure
// (reachableCandidates), so a blocked server drops out of selection until a
// later round — or a successful request — measures it reachable again. A
// server that needs a relay is reached through explicit config instead: a
// chain layer, or the outbound's upstream_proxy; those paths are measured
// per hop like any other request.
//
// Probing dials exactly the transport dispatch uses for a first hop: a stack
// dial to the server's address.
package router

import (
	"context"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/goose-network/goose/internal/core"
)

// probeCtl controls the probe loop's lifecycle and parameters.
type probeCtl struct {
	cancel   context.CancelFunc
	done     chan struct{}
	signal   chan struct{} // refresh nudge (buffered 1, coalesced)
	interval time.Duration
	timeout  time.Duration
}

// probeConcurrency caps parallel probe dials per round so a large outbound
// set doesn't produce a burst of simultaneous connections.
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

// reconcileProberLocked starts or stops the probe loop to match the engine's
// probe config: running iff Engine.Probes.Enabled, with its effective
// cadence. Caller holds r.mu.
func (r *Router) reconcileProberLocked() {
	p := r.cfg.Engine().Probes
	if !p.Enabled {
		r.probeCtlMu.Lock()
		ctl := r.probeCtl
		r.probeCtl = nil
		r.probeCtlMu.Unlock()
		if ctl != nil {
			ctl.cancel() // no join: see setProberLocked
		}
		return
	}
	r.setProberLocked(
		time.Duration(p.EffectiveIntervalSec())*time.Second,
		time.Duration(p.EffectiveTimeoutSec())*time.Second,
	)
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

// probeRound probes every outbound that has a dial address, recording each
// verdict as an outbound metric row. Outbounds without an address (direct /
// tunnel outbounds) have no server to reach and are treated as locally
// reachable at dispatch.
func (r *Router) probeRound(ctx context.Context, ctl *probeCtl) {
	r.mu.RLock()
	outs := make([]core.Outbound, 0, len(r.outbounds))
	for _, o := range r.outbounds {
		if o.Address() == "" || o.ID() == "" {
			continue
		}
		outs = append(outs, o)
	}
	r.mu.RUnlock()
	if len(outs) == 0 {
		return
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, probeConcurrency)
	for _, o := range outs {
		wg.Add(1)
		go func(o core.Outbound) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			r.probeOne(ctx, ctl.timeout, o)
		}(o)
	}
	wg.Wait()
}

// probeOne dials one outbound's server from the engine stack and records the
// outcome as a metric row: Chain [core.HopLocal, id], Target the server
// address. The metrics store derives the outbound's reachability verdict
// from exactly these rows (plus successful first hops), so the probe result
// is both durable and immediately filterable.
func (r *Router) probeOne(ctx context.Context, timeout time.Duration, o core.Outbound) {
	dctx, cancel := context.WithTimeout(ctx, timeout)
	start := time.Now()
	conn, err := r.stack.Dial(dctx, "tcp", o.Address())
	lat := time.Since(start)
	cancel()
	if err == nil {
		_ = conn.Close()
	}
	meta := core.Metadata{Network: core.NetworkTCP, StartedAt: start}
	if host, port, serr := net.SplitHostPort(o.Address()); serr == nil {
		meta.TargetHost, meta.TargetPort = host, port
	}
	r.record(meta, []string{core.HopLocal, o.ID()}, err == nil, err, lat)
}

// reachableCandidates drops first-hop candidates whose latest measured local
// verdict is a failure — the local network cannot reach those servers, so
// selecting them would only burn a dial timeout. Outbounds with no verdict
// yet are kept (fail-open) so a fresh config still routes while the first
// probe round runs.
func (r *Router) reachableCandidates(pool []core.Outbound) []core.Outbound {
	out := pool[:0:0]
	for _, o := range pool {
		if ok, _, measured := r.metrics.Reachability(o.ID()); measured && !ok {
			continue
		}
		out = append(out, o)
	}
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
// first hop, so exit-IP checks measure the path real traffic takes rather
// than a bypassing tunnel.
func (r *Router) CheckDial(ctx context.Context, out core.Outbound, network core.Network, target string) (net.Conn, error) {
	return out.DialContext(ctx, network, target)
}
