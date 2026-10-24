// Package router is the engine's central dispatcher. For each inbound
// connection it:
//  1. resolves the inbound's (or user's) chain from config;
//  2. executes the chain layer-by-layer: filter the pool, select an outbound,
//     dial through it, and (for multi-hop chains) dial the next layer's
//     outbound through the previous connection;
//  3. bridges the inbound connection with the final upstream connection;
//  4. records a request metric (success/failure, latency, chain).
//
// The router implements inbound.Handler. It reconciles its runtime objects
// (built outbounds, pools, chains, listeners) from the config store on every
// change.
package router

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/core"
	"github.com/goose-network/goose/internal/filter"
	"github.com/goose-network/goose/internal/metrics"
	"github.com/goose-network/goose/internal/plugin"
	"github.com/goose-network/goose/internal/selector"
	"github.com/goose-network/goose/internal/stack"
)

// Router routes inbound connections through the configured chains.
type Router struct {
	cfg     *config.Store
	metrics *metrics.Store
	stack   stack.Stack

	mu        sync.RWMutex
	outbounds map[string]core.Outbound // built, live outbounds by id
	pools     map[string]*builtPool
	chains    map[string]core.Chain
	// policy resolution: inboundID + user -> chainID
	policy policyResolver
	// fallback is the long-lived selector used when an inbound has no
	// configured chain (or the chain is missing). It is shared across all
	// such requests so its round-robin counter advances per request instead
	// of resetting to 0 (which would always pick the first outbound).
	fallback *fallbackSelector

	// probes is the latest reachability verdicts per outbound for
	// access-filtered pools (auto chains); guarded by probeMu. probeCtl runs
	// the probe loop. Both are wired in probe.go.
	probeMu    sync.RWMutex
	probes     map[string]*probeEntry
	probeCtl   *probeCtl
	probeCtlMu sync.Mutex

	copyBufPool sync.Pool
	metricSeq   uint64
}

type policyResolver struct {
	// per inbound: inboundID -> (user -> chainID), with a default for no-auth.
	inbounds map[string]*inboundPolicy
}

type inboundPolicy struct {
	defaultChain string
	users        map[string]string // user -> chainID
	// defaultRoute is the policy-level pool route (pool + per-inbound
	// filters) synthesized into a single-layer chain when no chain_id is
	// configured. Prebuilt at rebuild time so dispatch stays a map lookup.
	defaultRoute core.Chain
	// userRoutes is per-user pool routes, same shape as defaultRoute.
	userRoutes map[string]core.Chain
}

// builtPool is a pool with its filters + selector materialized.
type builtPool struct {
	id        string
	outbounds []core.Outbound
	filters   []core.Filter
	selector  core.Selector
	// access is non-nil for an auto-chained pool: the pool's access filter
	// config (access pool + probe cadence), used by the prober and by
	// dispatch-time chain building.
	access *config.AccessFilterSpec
}

// New creates a router. The stack is used as the underlying dialer for
// outbounds that need to reach their own server.
func New(cfg *config.Store, ms *metrics.Store, stk stack.Stack) *Router {
	r := &Router{
		cfg:         cfg,
		metrics:     ms,
		stack:       stk,
		outbounds:   map[string]core.Outbound{},
		pools:       map[string]*builtPool{},
		chains:      map[string]core.Chain{},
		fallback:    &fallbackSelector{},
		probes:      map[string]*probeEntry{},
		copyBufPool: sync.Pool{New: func() any { b := make([]byte, 32*1024); return &b }},
	}
	r.rebuild()
	return r
}

// Close stops the router's background probe loop. The router has no other
// owned resources; the engine closes it before the stack and metrics store.
func (r *Router) Close() {
	r.stopProber()
}

// Rebuild reconciles runtime objects from the current config. Called on
// startup and on every config change.
func (r *Router) Rebuild() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rebuild()
}

func (r *Router) rebuild() {
	// build outbounds
	r.outbounds = map[string]core.Outbound{}
	for _, spec := range r.cfg.Outbounds() {
		// Inject the engine stack dialer so outbound plugins reach their own
		// servers (and, for "direct", the target) through the configured
		// system/gvisor stack instead of a bare net.Dialer. We copy the
		// config map so we never mutate the caller's config.
		cfg := spec.Config
		if r.stack != nil {
			cfg = make(map[string]any, len(spec.Config)+1)
			for k, v := range spec.Config {
				cfg[k] = v
			}
			cfg["_dialer"] = r.stack
		}
		o, err := plugin.Build(spec.Protocol, cfg)
		if err != nil {
			continue // skip broken outbound, keep serving others
		}
		// wrap with stats + id
		r.outbounds[spec.ID] = &outboundWrapper{Outbound: o, id: spec.ID, protocol: spec.Protocol, ms: r.metrics}
	}
	// build pools
	r.pools = map[string]*builtPool{}
	for _, p := range r.cfg.Pools() {
		bp := &builtPool{id: p.ID}
		for _, id := range p.OutboundIDs {
			if o, ok := r.outbounds[id]; ok {
				bp.outbounds = append(bp.outbounds, o)
			}
		}
		for _, fs := range p.Filters {
			if f, err := filter.New(fs.Type, fs.Params); err == nil {
				bp.filters = append(bp.filters, f)
			}
		}
		if s, err := selector.New(p.Selector.Type, p.Selector.Params); err == nil {
			bp.selector = s
		} else {
			bp.selector = &fallbackSelector{}
		}
		bp.access = p.Access
		r.pools[p.ID] = bp
	}
	// build chains. Each layer's selector is wrapped in a poolSelector that
	// carries the pool's candidate outbounds, so the router can recover them
	// at dispatch time without a pool lookup.
	r.chains = map[string]core.Chain{}
	for _, c := range r.cfg.Chains() {
		var ch core.Chain
		for _, pid := range c.Layers {
			bp, ok := r.pools[pid]
			if !ok {
				continue
			}
			sel := bp.selector
			if sel == nil {
				sel = &fallbackSelector{}
			}
			ch = append(ch, core.ChainLayer{
				Filters:  bp.filters,
				Selector: &poolSelector{Selector: sel, poolID: bp.id, pool: bp.outbounds},
			})
		}
		r.chains[c.ID] = ch
	}
	// build policy resolver
	r.policy = policyResolver{inbounds: map[string]*inboundPolicy{}}
	for _, in := range r.cfg.Inbounds() {
		ip := &inboundPolicy{
			defaultChain: in.Policy.ChainID,
			users:        map[string]string{},
			userRoutes:   map[string]core.Chain{},
		}
		ip.defaultRoute = r.buildRoute(in.Policy)
		for _, u := range in.Users {
			if u.Policy == nil {
				continue
			}
			if u.Policy.ChainID != "" {
				ip.users[u.Username] = u.Policy.ChainID
			} else if ch := r.buildRoute(*u.Policy); len(ch) > 0 {
				// A user policy with only pool+filters (no chain) gets its own
				// single-layer route; otherwise it falls back to the inbound's
				// default route.
				ip.userRoutes[u.Username] = ch
			}
		}
		r.policy.inbounds[in.ID] = ip
	}
	// Wire the reachability prober for access-filtered pools (auto chains):
	// start it when the first pool gains an access filter, re-parameterize
	// when cadences change, stop it when the last one goes away.
	r.reconcileProberLocked()
}

// buildRoute synthesizes the single-layer chain for a policy that names a
// pool instead of a chain: the pool's outbounds as candidates, the pool's
// own filters plus the policy's filters, and the pool's selector (or the
// fallback). Returns nil when the policy names no pool.
func (r *Router) buildRoute(p config.InboundPolicy) core.Chain {
	if p.PoolID == "" {
		return nil
	}
	bp, ok := r.pools[p.PoolID]
	if !ok {
		return nil
	}
	filters := make([]core.Filter, 0, len(bp.filters)+len(p.Filters))
	filters = append(filters, bp.filters...)
	for _, fs := range p.Filters {
		if f, err := filter.New(fs.Type, fs.Params); err == nil {
			filters = append(filters, f)
		}
	}
	sel := core.Selector(bp.selector)
	if sel == nil {
		sel = &fallbackSelector{}
	}
	return core.Chain{{
		Filters:  filters,
		Selector: &poolSelector{Selector: sel, poolID: bp.id, pool: bp.outbounds},
	}}
}

// resolveChain returns the chain for an inbound+user: a named chain when the
// policy references one, otherwise the policy's single-layer pool route, and
// finally the shared all-outbounds fallback if neither is configured.
func (r *Router) resolveChain(inboundID, user string) core.Chain {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if ip, ok := r.policy.inbounds[inboundID]; ok {
		chainID := ip.defaultChain
		if c, ok := ip.users[user]; ok && c != "" {
			chainID = c
		}
		if ch, ok := r.chains[chainID]; ok && len(ch) > 0 {
			return ch
		}
		if ch, ok := ip.userRoutes[user]; ok && len(ch) > 0 {
			return ch
		}
		if len(ip.defaultRoute) > 0 {
			return ip.defaultRoute
		}
	}
	// fallback: a single layer that picks any available outbound.
	return core.Chain{{Selector: r.fallback}}
}

// Dial implements inbound.Handler. It resolves the chain for the request
// and dials the target through it, returning the upstream connection and the
// ordered list of outbound IDs used. The listener writes its protocol
// success reply, then calls Bridge.
func (r *Router) Dial(ctx context.Context, meta core.Metadata) (net.Conn, []string, error) {
	if meta.StartedAt.IsZero() {
		meta.StartedAt = time.Now()
	}
	chain := r.resolveChain(meta.InboundID, meta.User)
	upstream, chainIDs, err := r.dialChain(ctx, chain, meta)
	if err != nil {
		r.record(meta, chainIDs, false, err, time.Since(meta.StartedAt))
		return nil, chainIDs, err
	}
	return upstream, chainIDs, nil
}

// Bridge copies bytes bidirectionally between the inbound and upstream
// connections. As soon as either direction completes (one side half-closes
// or EOFs), both connections are torn down and a metric is recorded.
// Tearing down on the first EOF — rather than waiting for both — is what
// lets metrics be recorded promptly for HTTP/1.1 keep-alive responses, where
// the client may otherwise hold the connection open long after the response
// has been delivered.
//
// The metric's success flag reflects whether the completed direction ended
// cleanly (io.EOF or nil) rather than with a read/write error, so a broken
// relay does not inflate the success-rate stats. An idle timeout tears down
// connections that go silent for longer than idleTimeout so the bridge
// goroutines cannot pin a connection forever.
func (r *Router) Bridge(inbound, upstream net.Conn, meta core.Metadata) {
	start := meta.StartedAt
	if start.IsZero() {
		start = time.Now()
	}
	defer upstream.Close()
	defer inbound.Close()

	const idleTimeout = 5 * time.Minute
	timer := time.AfterFunc(idleTimeout, func() {
		_ = inbound.Close()
		_ = upstream.Close()
	})
	defer timer.Stop()

	done := make(chan struct{})
	var once sync.Once
	var firstErr error
	var errMu sync.Mutex
	closeBoth := func(err error) {
		once.Do(func() {
			if err != nil {
				errMu.Lock()
				firstErr = err
				errMu.Unlock()
			}
			close(done)
		})
	}
	// relay copies src->dst, resetting the idle timer on each chunk. A clean
	// EOF (or nil) is a normal half-close; any other error is a relay failure.
	relay := func(dst, src net.Conn) {
		buf := r.copyBufPool.Get().(*[]byte)
		defer r.copyBufPool.Put(buf)
		for {
			nr, err := src.Read(*buf)
			if nr > 0 {
				timer.Reset(idleTimeout)
				nw, werr := dst.Write((*buf)[:nr])
				if werr != nil {
					closeBoth(werr)
					return
				}
				if nw != nr {
					closeBoth(io.ErrShortWrite)
					return
				}
			}
			if err != nil {
				if err != io.EOF {
					closeBoth(err)
				} else {
					closeBoth(nil)
				}
				return
			}
		}
	}
	go relay(upstream, inbound)
	go relay(inbound, upstream)
	<-done

	success := firstErr == nil
	r.record(meta, meta.Chain, success, firstErr, time.Since(start))
}

// dialChain executes the chain: for each layer, filter the pool, select an
// outbound, and dial. For multi-hop chains, layer N+1's proxy server is
// dialed through the tunnel established by layer N (the previous conn
// becomes the transport for the next hop), and only the final layer CONNECTs
// toward the request's actual target.
//
// The per-layer candidate pool and selector are snapshotted under the read
// lock, but the network dials themselves happen WITHOUT the lock: a slow
// outbound dial (up to 30s) must not block Rebuild (which takes the write
// lock) or starve every other in-flight dial.
func (r *Router) dialChain(ctx context.Context, chain core.Chain, meta core.Metadata) (net.Conn, []string, error) {
	// Snapshot each layer's candidates + selector under the lock.
	layers := make([]snapshottedLayer, len(chain))
	r.mu.RLock()
	for i, layer := range chain {
		pool := r.layerPool(layer)
		cands := make([]core.Outbound, len(pool))
		copy(cands, pool)
		layers[i] = snapshottedLayer{filters: layer.Filters, selector: layer.Selector, pool: cands}
	}
	r.mu.RUnlock()

	return r.dialLayers(ctx, layers, meta)
}

// dialLayers dials the snapshotted layers left-to-right. Real proxy
// chaining: hop i tunnels toward hop i+1's proxy-server address, and only
// the final hop tunnels toward the request's actual target — so every
// layer's outbound is selected BEFORE the first dial (hop i's dial target
// depends on hop i+1's selection). When the first layer's pool is
// access-filtered and the selected member's server is unreachable directly,
// an access-proxy hop is prepended automatically (probe-driven auto chain).
//
// Dials happen outside the router lock, per dialChain's comment.
func (r *Router) dialLayers(ctx context.Context, layers []snapshottedLayer, meta core.Metadata) (net.Conn, []string, error) {
	// Select every layer's outbound up front. This also means empty-pool /
	// filter / select errors surface with no connections open — nothing to
	// leak — and the error still carries the outbounds selected so far.
	type hop struct {
		out   core.Outbound
		relay core.Outbound // access proxy prepended for layer 0, if probed
	}
	hops := make([]hop, len(layers))
	chainIDs := make([]string, 0, len(layers))
	for i, sl := range layers {
		pool := sl.pool
		// An auto-chained pool's candidates are narrowed by the latest
		// reachability probes: members known-unreachable both directly and
		// through the access pool are dropped.
		if bp := r.poolOfLayer(sl); bp != nil && bp.access != nil {
			pool = r.reachableCandidates(pool)
			if len(pool) == 0 {
				return nil, chainIDs, fmt.Errorf("router: layer %d no reachable outbound (probe failed directly and via access pool)", i)
			}
		}
		if len(pool) == 0 {
			return nil, chainIDs, fmt.Errorf("router: layer %d empty pool", i)
		}
		filtered := applyFilters(sl.filters, pool, meta)
		if len(filtered) == 0 {
			return nil, chainIDs, fmt.Errorf("router: layer %d no outbound passed filters", i)
		}
		out, err := sl.selector.Pick(filtered, meta)
		if err != nil {
			return nil, chainIDs, fmt.Errorf("router: layer %d select: %w", i, err)
		}
		hops[i] = hop{out: out}
	}
	// The relay applies to the first layer only: later hops are reached
	// through the previous layer's tunnel, not the engine's local network.
	if len(hops) > 0 {
		if bp := r.poolOfLayer(layers[0]); bp != nil && bp.access != nil {
			hops[0].relay = r.relayFor(hops[0].out)
		}
	}

	var conn net.Conn
	for i, h := range hops {
		if h.relay != nil {
			chainIDs = append(chainIDs, h.relay.ID())
		}
		chainIDs = append(chainIDs, h.out.ID())

		// Hop i's dial target: hop i+1's proxy-server address, or the
		// request's target for the final hop.
		target := meta.Target()
		if i+1 < len(hops) {
			if next := hops[i+1].out.Address(); next != "" {
				target = next
			}
		}

		if h.relay != nil {
			// Auto chain: dial the access proxy toward this hop's server,
			// then handshake with this hop through that tunnel.
			dialed, derr := r.dialRelayed(ctx, h.out, h.relay, meta, target)
			if derr != nil {
				return nil, chainIDs, fmt.Errorf("router: layer 0 relay %s dial %s: %w", h.relay.ID(), h.out.ID(), derr)
			}
			conn = dialed
			continue
		}

		dialed, err, consumed := r.dialOutbound(ctx, h.out, conn, meta, target)
		if err != nil {
			if conn != nil {
				conn.Close()
			}
			return nil, chainIDs, fmt.Errorf("router: layer %d dial %s: %w", i, h.out.ID(), err)
		}
		// If the dial did not consume the previous conn (chaining fell back
		// to a direct dial), close it to avoid leaking one conn per hop.
		if conn != nil && !consumed {
			conn.Close()
		}
		conn = dialed
	}
	if conn == nil {
		return nil, chainIDs, errors.New("router: empty chain")
	}
	return conn, chainIDs, nil
}

// poolOfLayer recovers the built pool a snapshotted layer came from, by the
// poolID carried on the layer's poolSelector. Layers synthesized without a
// pool (the fallback chain) return nil. The returned *builtPool is an
// immutable snapshot: rebuild replaces the pools map wholesale and never
// mutates a builtPool in place.
func (r *Router) poolOfLayer(sl snapshottedLayer) *builtPool {
	ps, ok := sl.selector.(*poolSelector)
	if !ok || ps.poolID == "" {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.pools[ps.poolID]
}

// snapshottedLayer is a chain layer's immutable view captured under the read
// lock so dials can proceed without holding it.
type snapshottedLayer struct {
	filters  []core.Filter
	selector core.Selector
	pool     []core.Outbound
}

// layerPool returns the candidate outbounds for a layer. The pool is carried
// by the poolSelector that wraps the layer's real selector at build time.
func (r *Router) layerPool(layer core.ChainLayer) []core.Outbound {
	if ps, ok := layer.Selector.(*poolSelector); ok {
		return ps.pool
	}
	// fallback chain (no explicit pool): use all built outbounds.
	all := make([]core.Outbound, 0, len(r.outbounds))
	for _, o := range r.outbounds {
		all = append(all, o)
	}
	return all
}

// poolSelector wraps a real selector and carries the pool's candidate
// outbounds so the router can recover them at dispatch time. poolID records
// which built pool supplied the candidates (for access-filter narrowing).
type poolSelector struct {
	core.Selector
	poolID string
	pool   []core.Outbound
}

// dialOutbound dials an outbound toward target. If prev is non-nil, the
// outbound's server is reached through prev (chaining) and the outbound then
// CONNECTs toward target through that tunnel; otherwise the outbound dials
// target through its own server via the engine stack. The returned consumed
// flag is true when prev was handed off to the outbound (DialThrough took
// ownership); false when the dial fell back to a direct dial and prev is
// still the caller's responsibility to close.
func (r *Router) dialOutbound(ctx context.Context, o core.Outbound, prev net.Conn, meta core.Metadata, target string) (net.Conn, error, bool) {
	if prev == nil {
		// first hop: outbound dials target through its own server via stack.
		c, err := o.DialContext(ctx, meta.Network, target)
		return c, err, false
	}
	// chaining: dial the next outbound's server through prev, then have it
	// connect to the target. This requires the outbound to support a
	// "dial through given conn" mode. We expose that via the ChainDialer
	// interface; outbounds that don't implement it fall back to dialing
	// directly (single-hop semantics). The wrapper is transparent: assert
	// ChainDialer on the unwrapped plugin outbound, not the wrapper.
	inner := o
	if w, ok := o.(interface{ Unwrap() core.Outbound }); ok {
		inner = w.Unwrap()
	}
	if cd, ok := inner.(ChainDialer); ok {
		c, err := cd.DialThrough(ctx, meta.Network, target, prev)
		return c, err, true
	}
	// no chaining support: dial directly (chain collapses to this hop).
	// prev is NOT consumed; caller closes it.
	c, err := o.DialContext(ctx, meta.Network, target)
	return c, err, false
}

// dialRelayed dials out's server through relay (the access proxy) and then
// has out connect toward target through that tunnel — the auto-chain first
// hop. The relay conn is ours until handoff, so it is closed on every path
// where the outbound did not take ownership.
func (r *Router) dialRelayed(ctx context.Context, out, relay core.Outbound, meta core.Metadata, target string) (net.Conn, error) {
	rc, err := relay.DialContext(ctx, meta.Network, out.Address())
	if err != nil {
		return nil, fmt.Errorf("relay %s dial %s: %w", relay.ID(), out.ID(), err)
	}
	dialed, derr, consumed := r.dialOutbound(ctx, out, rc, meta, target)
	if derr != nil || !consumed {
		// DialThrough impls close via on error and on success; a double
		// close is harmless. !consumed means chaining fell back to a
		// direct dial, so the tunnel is simply unused.
		_ = rc.Close()
	}
	return dialed, derr
}

// ChainDialer is an optional capability an outbound plugin implements to
// support being dialed through a pre-existing connection (proxy chaining).
type ChainDialer interface {
	DialThrough(ctx context.Context, network core.Network, target string, via net.Conn) (net.Conn, error)
}

func applyFilters(filters []core.Filter, pool []core.Outbound, meta core.Metadata) []core.Outbound {
	out := pool[:0:0]
	for _, o := range pool {
		ok := true
		for _, f := range filters {
			if !f.Allow(o, meta) {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, o)
		}
	}
	return out
}

func (r *Router) record(meta core.Metadata, chain []string, success bool, err error, dur time.Duration) {
	m := core.RequestMetric{
		ID:         fmt.Sprintf("%d", atomic.AddUint64(&r.metricSeq, 1)),
		InboundID:  meta.InboundID,
		User:       meta.User,
		Network:    meta.Network,
		Target:     meta.Target(),
		Chain:      append([]string{}, chain...),
		Success:    success,
		Latency:    dur,
		StartedAt:  meta.StartedAt,
		FinishedAt: time.Now(),
	}
	if err != nil {
		m.Error = err.Error()
	}
	_ = r.metrics.Record(m)
}

// metricSeq generates monotonic metric IDs (see record()).
