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
	"slices"
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
	pub "github.com/goose-network/goose/pkg/plugin"
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
	// route is the inbound's default route fully resolved at rebuild time:
	// a named chain (with the policy's filters narrowing every layer), a
	// pinned outbound server, or a single-layer pool route. nil means no
	// route: dispatch falls back to the shared all-outbounds selector.
	route core.Chain
	// userRoutes is each user's fully resolved route, built by merging the
	// user's policy over the inbound's (route fields replace as a group;
	// the user's filters replace the inbound's). Present for every user
	// with a policy, possibly nil — nil defers to route at dispatch.
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

// StackDialer returns the engine's configured network-stack dialer — the
// layer-3/4 ability (the host kernel's system stack, or the userspace
// gvisor netstack). The provider manager injects it into provider configs
// under pkg/plugin.StackDialerKey so plugins can route their own
// connections through the engine's stack instead of opening bare sockets.
// It implements the provider package's stackSource.
func (r *Router) StackDialer() pub.StackDialer {
	return r.stack
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
	// build chains. Each layer references either a pool or a single outbound
	// server (when an ID matches both, the pool wins). Pool layers are
	// wrapped in a poolSelector that carries the pool's candidates, so the
	// router can recover them at dispatch time without a pool lookup;
	// outbound layers pin that one server for the hop.
	r.chains = map[string]core.Chain{}
	for _, c := range r.cfg.Chains() {
		var ch core.Chain
		for _, ref := range c.Layers {
			if bp, ok := r.pools[ref]; ok {
				sel := bp.selector
				if sel == nil {
					sel = &fallbackSelector{}
				}
				ch = append(ch, core.ChainLayer{
					Filters:  bp.filters,
					Selector: &poolSelector{Selector: sel, poolID: bp.id, pool: bp.outbounds},
				})
			} else if o, ok := r.outbounds[ref]; ok {
				ch = append(ch, core.ChainLayer{
					Selector: &poolSelector{Selector: &fixedSelector{out: o}, pool: []core.Outbound{o}},
				})
			}
			// unknown references are skipped; the chain keeps its order
		}
		r.chains[c.ID] = ch
	}
	// build policy resolver
	r.policy = policyResolver{inbounds: map[string]*inboundPolicy{}}
	for _, in := range r.cfg.Inbounds() {
		ip := &inboundPolicy{
			route:      r.resolveRoute(in.Policy),
			userRoutes: map[string]core.Chain{},
		}
		for _, u := range in.Users {
			if u.Policy == nil {
				continue
			}
			// A user policy merges over the inbound's: route fields replace
			// as a group (set any of them and you own all three), filters
			// replace the inbound's outright. Store nil when the merged
			// policy resolves to nothing — dispatch then defers to route.
			merged := effectivePolicy(in.Policy, u.Policy)
			ip.userRoutes[u.Username] = r.resolveRoute(merged)
		}
		r.policy.inbounds[in.ID] = ip
	}
	// Wire the reachability prober for access-filtered pools (auto chains):
	// start it when the first pool gains an access filter, re-parameterize
	// when cadences change, stop it when the last one goes away.
	r.reconcileProberLocked()
}

// resolveRoute materializes a policy into a fully dialable chain: a named
// chain from the chain store, a single layer pinning one outbound server, or
// a single-layer pool route — in that order of preference. Policy filters are
// applied in every case (narrowing each layer of a named chain, or joining
// the pool's own filters). Returns nil when the policy names nothing
// resolvable; the caller falls back to the next policy level.
func (r *Router) resolveRoute(p config.InboundPolicy) core.Chain {
	var filters []core.Filter
	for _, fs := range p.Filters {
		if f, err := filter.New(fs.Type, fs.Params); err == nil {
			filters = append(filters, f)
		}
	}
	if p.ChainID != "" {
		if ch, ok := r.chains[p.ChainID]; ok && len(ch) > 0 {
			return r.chainWithFilters(ch, filters)
		}
	}
	if p.OutboundID != "" {
		if o, ok := r.outbounds[p.OutboundID]; ok {
			return core.Chain{{Selector: &poolSelector{Selector: &fixedSelector{out: o}, pool: []core.Outbound{o}}}}
		}
	}
	if p.PoolID == "" {
		return nil
	}
	bp, ok := r.pools[p.PoolID]
	if !ok {
		return nil
	}
	layerFilters := make([]core.Filter, 0, len(bp.filters)+len(filters))
	layerFilters = append(layerFilters, bp.filters...)
	layerFilters = append(layerFilters, filters...)
	sel := core.Selector(bp.selector)
	if sel == nil {
		sel = &fallbackSelector{}
	}
	return core.Chain{{
		Filters:  layerFilters,
		Selector: &poolSelector{Selector: sel, poolID: bp.id, pool: bp.outbounds},
	}}
}

// chainWithFilters returns a copy of ch with the given filters appended to
// every layer's own filters, so an inbound/user policy narrows each hop of a
// named chain (a pool layer keeps its pool filters, an outbound layer gains
// the policy filters). The layers' selectors — and their embedded
// poolSelectors with live candidate slices — are shared, not copied.
func (r *Router) chainWithFilters(ch core.Chain, filters []core.Filter) core.Chain {
	if len(filters) == 0 {
		return ch
	}
	out := make(core.Chain, len(ch))
	for i, layer := range ch {
		fs := make([]core.Filter, 0, len(layer.Filters)+len(filters))
		fs = append(fs, layer.Filters...)
		fs = append(fs, filters...)
		out[i] = core.ChainLayer{Filters: fs, Selector: layer.Selector}
	}
	return out
}

// effectivePolicy merges a user policy over the inbound-level policy. Route
// fields (chain_id / outbound_id / pool_id) are replaced as a group: if the
// user policy sets any of them, the user's trio wins wholesale; otherwise the
// inbound's route is inherited. Filters are not a route field — a user policy
// with filters always replaces the inbound's filters (that is the point of
// per-user filters), even when the route itself is inherited.
func effectivePolicy(base config.InboundPolicy, over *config.InboundPolicy) config.InboundPolicy {
	if over == nil {
		return base
	}
	p := config.InboundPolicy{Filters: base.Filters}
	if over.ChainID != "" || over.OutboundID != "" || over.PoolID != "" {
		p.ChainID, p.OutboundID, p.PoolID = over.ChainID, over.OutboundID, over.PoolID
	} else {
		p.ChainID, p.OutboundID, p.PoolID = base.ChainID, base.OutboundID, base.PoolID
	}
	if len(over.Filters) > 0 {
		p.Filters = over.Filters
	}
	return p
}

// fixedSelector always picks its one pinned outbound; used for chain hops
// and policies that reference a specific server instead of a pool.
type fixedSelector struct{ out core.Outbound }

func (*fixedSelector) Name() string { return "fixed" }

func (s *fixedSelector) Pick(_ []core.Outbound, _ core.Metadata) (core.Outbound, error) {
	return s.out, nil
}

// resolveChain returns the chain for an inbound+user: the user's resolved
// route, else the inbound's, else the shared all-outbounds fallback. Routes
// are fully precomputed at rebuild time; this only picks between them.
func (r *Router) resolveChain(inboundID, user string) core.Chain {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if ip, ok := r.policy.inbounds[inboundID]; ok {
		if ch, ok := ip.userRoutes[user]; ok && len(ch) > 0 {
			return ch
		}
		if len(ip.route) > 0 {
			return ip.route
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
	// fallback chain (no explicit pool): use all built outbounds. Sorted by
	// id so the shared fallback selector's round-robin counter produces a
	// stable rotation — map iteration order is random per call, which would
	// scramble which outbound each consecutive pick lands on.
	all := make([]core.Outbound, 0, len(r.outbounds))
	ids := make([]string, 0, len(r.outbounds))
	for id := range r.outbounds {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		all = append(all, r.outbounds[id])
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
