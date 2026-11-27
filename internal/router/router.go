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
	"log"
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

	// probeCtl runs the reachability probe loop; reachability verdicts
	// themselves live in the metrics store (probe rows + successful first
	// hops), not in the router. Both are wired in probe.go.
	probeCtl   *probeCtl
	probeCtlMu sync.Mutex

	// httpProbeInFlight single-flights the scheduled HTTP round, which runs
	// detached from the probe loop so a slow round cannot delay the dial
	// sweep; httpProbeWG joins detached rounds on shutdown.
	httpProbeInFlight atomic.Bool
	httpProbeWG       sync.WaitGroup

	// fullProbes holds the live full active probing jobs by ID (fullprobe.go);
	// fullProbeWG joins their runners on shutdown.
	fullProbeMu sync.Mutex
	fullProbes  map[string]*fullProbeRun
	fullProbeWG sync.WaitGroup
	// persistFullProbeFn is the terminal-persist seam: New wires the real
	// persistFullProbe. Tests override it to inject transient store failures
	// without disk tricks; production never reassigns it.
	persistFullProbeFn func(fp *fullProbeRun, force bool) error
	// shutdown closes when stopFullProbes begins joining, so a runner
	// retrying its terminal persist bails instead of holding Close open for
	// another backoff tick. shutdownOnce keeps the close idempotent — Close
	// may run twice (explicit call plus defer).
	shutdown     chan struct{}
	shutdownOnce sync.Once

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
		shutdown:    make(chan struct{}),
		copyBufPool: sync.Pool{New: func() any { b := make([]byte, 32*1024); return &b }},
	}
	r.persistFullProbeFn = r.persistFullProbe
	r.rebuild()
	return r
}

// Close stops the router's background probe loop and cancels any running
// full active probing jobs, joining their runners. The router has no other
// owned resources; the engine closes it before the stack and metrics store.
func (r *Router) Close() {
	r.stopProber()
	r.stopFullProbes()
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
	// Snapshot epoch: each config collection is read exactly ONCE per
	// rebuild; collections consumed by several passes (pools, chains) are
	// snapshotted here and shared by every pass. config.Store hands out a
	// FRESH slice of immutable spec pointers per getter call, and
	// SetPool/SetChain/SetOutbound/SetInbound each bump the version with no
	// serialization against the engine watch goroutine that runs Rebuild.
	// Re-reading a getter between passes could therefore mix epochs: a
	// pool-mode full-probe apply writes SetChain xN -> SetPool -> SetInbound
	// back-to-back, and a SetPool landing between the pools pass and the
	// member-attach pass would surface a pool ID that r.pools never built —
	// a nil *builtPool dereference and an engine crash. One snapshot per
	// collection makes cross-pass membership coherent by construction.
	// (Outbounds and Inbounds feed a single pass each, so their one read
	// happens in that pass; cross-collection references are all guarded by
	// existence checks.)
	pools := r.cfg.Pools()
	chains := r.cfg.Chains()

	// build outbounds
	r.outbounds = map[string]core.Outbound{}
	for _, spec := range r.cfg.Outbounds() {
		// Inject the engine stack dialer so outbound plugins reach their own
		// servers (and, for "direct", the target) through the configured
		// system/gvisor stack instead of a bare net.Dialer. Inject the
		// upstream-route dialer the same way when the outbound's own server
		// connection is configured to ride another engine route
		// (spec.UpstreamProxy) — it re-resolves the route against live config
		// per dial, so this injection survives pool edits and rebuilds.
		// We copy the config map so we never mutate the caller's config.
		cfg := spec.Config
		if r.stack != nil || spec.UpstreamProxy != "" {
			cfg = make(map[string]any, len(spec.Config)+2)
			for k, v := range spec.Config {
				cfg[k] = v
			}
			if r.stack != nil {
				cfg[pub.StackDialerKey] = r.stack
			}
			if spec.UpstreamProxy != "" {
				cfg[pub.UpstreamDialerKey] = r.OutboundUpstreamDialer(spec.UpstreamProxy)
			}
		}
		o, err := plugin.Build(spec.Protocol, cfg)
		if err != nil {
			continue // skip broken outbound, keep serving others
		}
		// wrap with stats + id
		r.outbounds[spec.ID] = &outboundWrapper{Outbound: o, id: spec.ID, protocol: spec.Protocol, ms: r.metrics, pinOnly: spec.PinOnly()}
	}
	// build pools (from the epoch snapshot — see top of rebuild)
	r.pools = map[string]*builtPool{}
	for _, p := range pools {
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
		r.pools[p.ID] = bp
	}
	// build chains. Each layer references either a pool or a single outbound
	// server (when an ID matches both, the pool wins). Pool layers are
	// wrapped in a poolSelector that carries the pool's candidates, so the
	// router can recover them at dispatch time without a pool lookup;
	// outbound layers pin that one server for the hop.
	//
	// A path-pool (one with chain members) may not be a chain layer: a member
	// chain expands into hops at selection time, and a chain traversing a
	// path-pool would nest that expansion. The API refuses such writes; this
	// is the backstop for configs loaded from disk — skip the WHOLE chain
	// (never silently shorten it) and say so.
	pathPoolIDs := map[string]bool{}
	for _, p := range pools {
		if len(p.ChainIDs) > 0 {
			pathPoolIDs[p.ID] = true
		}
	}
	r.chains = map[string]core.Chain{}
	for _, c := range chains {
		forbidden := false
		for _, ref := range c.Layers {
			if pathPoolIDs[ref] {
				log.Printf("router: chain %s references path-pool %s (a pool with chain members); skipping the whole chain", c.ID, ref)
				forbidden = true
				break
			}
		}
		if forbidden {
			continue
		}
		var ch core.Chain
		for _, ref := range c.Layers {
			if bp, ok := r.pools[ref]; ok {
				sel := bp.selector
				if sel == nil {
					sel = &fallbackSelector{}
				}
				ch = append(ch, core.ChainLayer{
					Filters:  bp.filters,
					Selector: &poolSelector{Selector: sel, pool: bp.outbounds},
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
	// Attach chain members to pools. Runs AFTER chains and BEFORE policies
	// so resolveRoute captures the token-bearing candidate slices; both
	// inputs are the epoch snapshots taken at the top of rebuild.
	chainSpecs := make(map[string]*config.ChainSpec, len(chains))
	for _, c := range chains {
		chainSpecs[c.ID] = c
	}
	r.attachChainMembersLocked(pools, chainSpecs)
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
	// Wire the reachability prober to the engine's probe config: running
	// iff Engine.Probes.Enabled, re-parameterized when its cadence changes.
	r.reconcileProberLocked()
}

// attachChainMembersLocked is rebuild's member-attach pass. pools and
// chainSpecs are the SAME epoch snapshots the pools and chains passes
// consumed (see rebuild's snapshot-epoch comment): membership is coherent
// across passes by construction even while concurrent config writes bump
// the store version between getter calls. Caller holds r.mu (or is the
// pre-publication New constructor).
//
// Each member chain is resolved from its config spec against r.outbounds
// directly — NOT via the built r.chains, whose layers may have been
// shortened by unknown references: a member must expand to exactly its
// proved path or not be selectable at all. Every layer must be a pinned
// outbound that exists and is not pool-shadowed (routing resolves pools
// first); an ineligible member is skipped with a log, never partially
// attached.
func (r *Router) attachChainMembersLocked(pools []*config.Pool, chainSpecs map[string]*config.ChainSpec) {
	for _, p := range pools {
		if len(p.ChainIDs) == 0 {
			continue
		}
		// Snapshot coherence guarantees every pool in `pools` was built in
		// the pools pass. Guard anyway: a future regression that re-reads
		// config mid-rebuild (or feeds a mismatched snapshot pair) must
		// degrade to skipped members with a log — like the other member
		// violations — never a nil-deref that takes down the engine:
		// nothing recovers panics on this path.
		bp, ok := r.pools[p.ID]
		if !ok || bp == nil {
			log.Printf("router: pool %s has chain members but was not built; skipping its members", p.ID)
			continue
		}
		for _, chainID := range p.ChainIDs {
			spec, ok := chainSpecs[chainID]
			if !ok || len(spec.Layers) == 0 {
				log.Printf("router: pool %s chain member %s: chain missing or empty; skipping member", p.ID, chainID)
				continue
			}
			hops := make([]core.Outbound, 0, len(spec.Layers))
			eligible := true
			for _, ref := range spec.Layers {
				if _, shadowed := r.pools[ref]; shadowed {
					log.Printf("router: pool %s chain member %s: layer %s is a pool (members must be pinned outbounds); skipping member", p.ID, chainID, ref)
					eligible = false
					break
				}
				o, ok := r.outbounds[ref]
				if !ok {
					log.Printf("router: pool %s chain member %s: layer %s is not a built outbound; skipping member", p.ID, chainID, ref)
					eligible = false
					break
				}
				hops = append(hops, o)
			}
			if !eligible {
				continue
			}
			bp.outbounds = append(bp.outbounds, &chainMember{
				id:           chainID,
				hops:         hops,
				firstHopID:   hops[0].ID(),
				firstHopAddr: hops[0].Address(),
				exit:         hops[len(hops)-1],
				ms:           r.metrics,
			})
		}
	}
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
		Selector: &poolSelector{Selector: sel, pool: bp.outbounds},
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
	if meta.Path == nil {
		// A caller that does not observe (a test, a Handler wired elsewhere):
		// its failure row still gets a graph. Both inbound listeners create
		// the recorder themselves, so the success row Bridge records from the
		// caller's own copy of meta carries the same one.
		meta.Path = core.NewPathRecorder()
	}
	meta.Path.SetClassifier(classifyLeg)
	meta.Path.AddLocal()
	meta.Path.AddTarget(meta.TargetHost, meta.TargetPort)
	// The ctx carrier reaches the two seams that see no Metadata: CheckDial's
	// callers and routeDialer.DialUpstream, which builds a fresh Metadata.
	ctx = core.WithPathRecorder(ctx, meta.Path)

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
	hasMember := false
	r.mu.RLock()
	for i, layer := range chain {
		pool := r.layerPool(layer)
		cands := make([]core.Outbound, len(pool))
		copy(cands, pool)
		for _, o := range cands {
			if _, ok := o.(*chainMember); ok {
				hasMember = true
			}
		}
		layers[i] = snapshottedLayer{filters: layer.Filters, selector: layer.Selector, pool: cands}
	}
	r.mu.RUnlock()

	if hasMember {
		// A layer can pick a chain member: selection expands it into hops.
		return r.dialLayersExpanding(ctx, layers, meta)
	}
	return r.dialLayers(ctx, layers, meta)
}

// dialLayers dials the snapshotted layers left-to-right. Real proxy
// chaining: hop i tunnels toward hop i+1's proxy-server address, and only
// the final hop tunnels toward the request's actual target — so every
// layer's outbound is selected BEFORE the first dial (hop i's dial target
// depends on hop i+1's selection). The first hop is dialed from the engine's
// local network, so its candidates are narrowed by the measured
// reachability verdicts in the outbound metrics; later hops ride the
// previous tunnel, where local reachability says nothing.
//
// Dials happen outside the router lock, per dialChain's comment.
func (r *Router) dialLayers(ctx context.Context, layers []snapshottedLayer, meta core.Metadata) (net.Conn, []string, error) {
	// Select every layer's outbound up front. This also means empty-pool /
	// filter / select errors surface with no connections open — nothing to
	// leak — and the error still carries the outbounds selected so far.
	hops := make([]core.Outbound, len(layers))
	chainIDs := make([]string, 0, len(layers))
	for i, sl := range layers {
		pool := sl.pool
		if len(pool) == 0 {
			return nil, chainIDs, fmt.Errorf("router: layer %d empty pool", i)
		}
		if i == 0 {
			// First hop rides the local network: drop candidates whose
			// latest measured verdict says the server is unreachable from
			// here. Fail-open when nothing has been measured yet.
			pool = r.reachableCandidates(pool)
			if len(pool) == 0 {
				return nil, chainIDs, fmt.Errorf("router: layer 0 no reachable outbound (local reachability probes failed)")
			}
		}
		filtered := applyFilters(sl.filters, pool, meta)
		if len(filtered) == 0 {
			return nil, chainIDs, fmt.Errorf("router: layer %d no outbound passed filters", i)
		}
		out, err := sl.selector.Pick(filtered, meta)
		if err != nil {
			return nil, chainIDs, fmt.Errorf("router: layer %d select: %w", i, err)
		}
		hops[i] = out
	}

	return r.dialHops(ctx, hops, meta)
}

// dialLayersExpanding is dialLayers for chains whose layers can pick a chain
// member (a pool with ChainIDs). Selection works the same — every hop is
// chosen before the first dial — but picking a *chainMember splices its
// pre-pinned hops into the layer list as fixed layers and re-processes the
// position, so each expanded hop goes through the same select machinery and
// the entry hop keeps its reachability narrowing until the first real hop is
// selected. The splice budget bounds expansions: member hops are pinned
// outbounds by construction (rebuild verifies it), so a member expanding into
// another member means a cyclic reference smuggled past validation.
//
// Dials happen outside the router lock, per dialChain's comment.
func (r *Router) dialLayersExpanding(ctx context.Context, layers []snapshottedLayer, meta core.Metadata) (net.Conn, []string, error) {
	// Work on a private copy: expansion splices layers in place.
	layers = append([]snapshottedLayer(nil), layers...)
	var hops []core.Outbound
	chainIDs := make([]string, 0, len(layers))
	selectedAny := false
	splices := 0
	for i := 0; i < len(layers); i++ {
		sl := layers[i]
		pool := sl.pool
		if len(pool) == 0 {
			return nil, chainIDs, fmt.Errorf("router: layer %d empty pool", i)
		}
		if !selectedAny {
			// The first real hop rides the local network: drop candidates
			// whose latest measured verdict says the server is unreachable
			// from here (chain members key on their entry hop). Fail-open
			// when nothing has been measured yet.
			pool = r.reachableCandidates(pool)
			if len(pool) == 0 {
				return nil, chainIDs, fmt.Errorf("router: layer %d no reachable outbound (local reachability probes failed)", i)
			}
		}
		filtered := applyFilters(sl.filters, pool, meta)
		if len(filtered) == 0 {
			return nil, chainIDs, fmt.Errorf("router: layer %d no outbound passed filters", i)
		}
		out, err := sl.selector.Pick(filtered, meta)
		if err != nil {
			return nil, chainIDs, fmt.Errorf("router: layer %d select: %w", i, err)
		}
		if cm, ok := out.(*chainMember); ok {
			splices++
			if splices > 8 {
				return nil, chainIDs, fmt.Errorf("router: layer %d expanded more than 8 chain members (cyclic pool/chain reference?)", i)
			}
			expanded := make([]snapshottedLayer, 0, len(layers)-1+len(cm.hops))
			expanded = append(expanded, layers[:i]...)
			for _, h := range cm.hops {
				expanded = append(expanded, snapshottedLayer{
					selector: &poolSelector{Selector: &fixedSelector{out: h}, pool: []core.Outbound{h}},
					pool:     []core.Outbound{h},
				})
			}
			expanded = append(expanded, layers[i+1:]...)
			layers = expanded
			i-- // re-process this position: it now holds the member's first hop
			continue
		}
		hops = append(hops, out)
		chainIDs = append(chainIDs, out.ID())
		selectedAny = true
	}

	return r.dialHops(ctx, hops, meta)
}

// dialHops dials a pre-selected, ordered hop list left-to-right: hop i tunnels
// toward hop i+1's proxy-server address, and only the final hop CONNECTs toward
// meta's target. It is the dial half of dialLayers, split out so the full-probe
// runner can dial an explicitly pinned path (no selection, no reachability
// narrowing — it is measuring whether the path works, not routing traffic).
// The returned []string is the ordered hop IDs actually dialed.
//
// Dials happen outside the router lock, per dialChain's comment.
func (r *Router) dialHops(ctx context.Context, hops []core.Outbound, meta core.Metadata) (net.Conn, []string, error) {
	chainIDs := make([]string, 0, len(hops))
	var conn net.Conn
	for i, h := range hops {
		chainIDs = append(chainIDs, h.ID())

		// Name this hop's own node before anything it dials toward. The
		// recorder keeps insertion order and PathGraph.Nodes documents proxies
		// in dial order, so a node named ahead of the hop that reaches it
		// publishes the route out of order.
		hop := hopNodeID(meta.Path, h)

		// Hop i's dial target: hop i+1's proxy-server address, or the
		// request's target for the final hop.
		target := meta.Target()
		// The node the final hop tunnels toward. TargetNodeIDFor refuses a
		// target node naming a different destination, which is what keeps a
		// nested upstream chain (sharing the outer request's recorder while
		// dialing a different address) from pointing its last leg at the
		// outer request's target.
		to := meta.Path.TargetNodeIDFor(meta.TargetHost, meta.TargetPort)
		if to == "" {
			to = meta.Path.AddServer(target)
		}
		if i+1 < len(hops) {
			if next := hops[i+1].Address(); next != "" {
				target = next
			}
			// Name the successor now, so hop i's tunnel leg has a real node to
			// point at. It is named second, which is what makes the node list
			// read in dial order rather than in reverse.
			to = hopNodeID(meta.Path, hops[i+1])
			if to == "" {
				to = meta.Path.AddServer(target)
			}
			if to == "" {
				to = meta.Path.TargetNodeIDFor(meta.TargetHost, meta.TargetPort)
			}
		}
		legs := hopLegs{
			hop:    hop,
			to:     to,
			target: target,
		}

		dialed, err, consumed := r.dialOutbound(ctx, h, conn, meta, legs)
		if err != nil {
			if conn != nil {
				conn.Close()
			}
			return nil, chainIDs, fmt.Errorf("router: layer %d dial %s: %w", i, h.ID(), err)
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
	// fallback chain (no explicit pool): use all auto-selectable built outbounds
	// (pin-only ones are excluded — they resolve only for explicit references).
	// Sorted by id so the shared fallback selector's round-robin counter produces
	// a stable rotation — map iteration order is random per call, which would
	// scramble which outbound each consecutive pick lands on.
	all := make([]core.Outbound, 0, len(r.outbounds))
	ids := make([]string, 0, len(r.outbounds))
	for id := range r.outbounds {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		o := r.outbounds[id]
		if w, ok := o.(*outboundWrapper); ok && w.pinOnly {
			continue
		}
		all = append(all, o)
	}
	return all
}

// poolSelector wraps a real selector and carries the pool's candidate
// outbounds so the router can recover them at dispatch time.
type poolSelector struct {
	core.Selector
	pool []core.Outbound
}

// dialOutbound dials an outbound toward legs.target. If prev is non-nil, the
// outbound's server is reached through prev (chaining) and the outbound then
// CONNECTs toward the target through that tunnel; otherwise the outbound dials
// the target through its own server via the engine stack. The returned
// consumed flag is true when prev was handed off to the outbound (DialThrough
// took ownership); false when the dial fell back to a direct dial and prev is
// still the caller's responsibility to close.
//
// Each branch brackets the dial in legs' recorder, so the graph records the
// dial that actually happened — including the collapse to a direct dial, which
// is a fact about this hop that no configuration lookup can tell.
func (r *Router) dialOutbound(ctx context.Context, o core.Outbound, prev net.Conn, meta core.Metadata, legs hopLegs) (net.Conn, error, bool) {
	rec := meta.Path
	if prev == nil {
		// First hop: the outbound dials legs.target through its own server via
		// the engine stack, so one call covers the arrival at its server and
		// the handshake toward legs.to. A hop with no server of its own
		// (legs.hop == "", direct) leaves To empty, and the leg is then the
		// engine's own dial toward legs.to. When such a plugin reached
		// legs.to through an upstream route of its own, that route has already
		// recorded the arrival here — the recorder recognises this bracket as
		// the same physical leg, records no second edge and adopts no socket
		// for legs.to, so the destination keeps the address config named.
		c, err := rec.Observe(core.Leg{
			Kind: core.LegDial, To: legs.hop, Through: legs.to, Target: legs.target,
		}, nil, func() (net.Conn, error) {
			return o.DialContext(ctx, meta.Network, legs.target)
		})
		return c, err, false
	}
	cd, ok := chainDialer(o)
	if !ok {
		// No chaining support: the hop re-dials from the local network and the
		// previous conn is dropped. Collapsed says so on the edge, and From is
		// the local node rather than legs.prev — the leg really did leave the
		// engine host, and the tunnel the hop before it established is wasted.
		// An upstream route running inside this dial records its own legs from
		// local too, which makes this bracket nested; the recorder then credits
		// the member's socket to the member's node and leaves this hop's own
		// address as configured.
		// prev is NOT consumed; the caller closes it.
		c, err := rec.Observe(core.Leg{
			Kind: core.LegDial, To: legs.hop, Through: legs.to,
			Target: legs.target, Collapsed: true,
		}, nil, func() (net.Conn, error) {
			return o.DialContext(ctx, meta.Network, legs.target)
		})
		return c, err, false
	}
	if legs.hop == "" {
		// A hop with no server of its own (direct): DialThrough hands prev
		// back unchanged, so this call opens no socket and has no leg of its
		// own to bracket. The previous hop's tunnel edge covers the leg only
		// when the recorder is standing at legs.to — it is not when this hop
		// configures an address (the previous hop tunneled toward a server:
		// node standing for it) or when a node cap swallowed the vertex.
		// Nothing honest can be recorded for the gap: the conn returned here
		// does not go to legs.to. The graph says it may be incomplete instead
		// of claiming a leg nobody observed.
		if rec.LastNodeID() != legs.to {
			rec.NotePartial()
		}
		c, err := cd.DialThrough(ctx, meta.Network, legs.target, prev)
		return c, err, true
	}
	// prev already reaches this hop's server — the previous hop's tunnel leg
	// put it there — so what this call adds is this hop's own handshake toward
	// legs.to. Its socket is prev's, whose peer is the PREVIOUS hop's server:
	// a chain holds exactly one engine socket and that hop's own dial leg
	// already adopted it, so passing prev as via keeps the recorder from
	// crediting legs.to with an address the leg did not produce.
	c, err := rec.Observe(core.Leg{
		Kind: core.LegTunnel, From: legs.hop, To: legs.to, Target: legs.target,
	}, prev, func() (net.Conn, error) {
		return cd.DialThrough(ctx, meta.Network, legs.target, prev)
	})
	return c, err, true
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

// record writes the request metric row for one finished request. Failed rows
// carry the generically classified reason (typed errors only — dispatch has
// no stage or path trace); a path that classifies precisely, like the HTTP
// prober, passes its own reason via recordWithReason.
func (r *Router) record(meta core.Metadata, chain []string, success bool, err error, dur time.Duration) {
	r.recordWithReason(meta, chain, success, err, dur, classifyErr(err))
}

func (r *Router) recordWithReason(meta core.Metadata, chain []string, success bool, err error, dur time.Duration, reason string) {
	r.recordRow(meta, chain, success, err, dur, reason, meta.Path.Snapshot())
}

// recordRow builds and enqueues one metric row. path is the already-snapshotted
// graph (nil when nothing was observed), never the recorder itself: the
// snapshot is deep-copied and sealed, so the metrics writer's json.Marshal —
// which can run hundreds of milliseconds later, on another goroutine — reads
// state nothing can mutate any more. A producer that already snapshotted (the
// HTTP prober, which puts the same graph on its probe row) passes its own
// pointer rather than copying twice.
func (r *Router) recordRow(meta core.Metadata, chain []string, success bool, err error, dur time.Duration, reason string, path *core.PathGraph) {
	m := core.RequestMetric{
		ID:         fmt.Sprintf("%d", atomic.AddUint64(&r.metricSeq, 1)),
		InboundID:  meta.InboundID,
		User:       meta.User,
		Network:    meta.Network,
		Target:     meta.Target(),
		Chain:      append([]string{}, chain...),
		Success:    success,
		Reason:     reason,
		Latency:    dur,
		StartedAt:  meta.StartedAt,
		FinishedAt: time.Now(),
		Path:       path,
	}
	if err != nil {
		m.Error = err.Error()
	}
	_ = r.metrics.Record(m)
}

// metricSeq generates monotonic metric IDs (see record()).
