// upstream_proxy resolution: the router implements the engine side of the
// upstream-route references, routing a plugin's own network activity through
// engine-managed proxies instead of the engine host's local network. Two
// config surfaces name a route:
//
//   - config.ProviderSpec.UpstreamProxy — a provider's own URL fetches (the
//     subscription download, the server-list fetch, ...). The manager
//     injects the resolved dialer into the provider's config map.
//   - config.OutboundSpec.UpstreamProxy — an outbound's own connection to
//     its proxy server. The router injects the resolved dialer into the
//     outbound's config map at build time.
//
// The reference is resolved against live config at DIAL time — a running
// provider's next fetch, and a built outbound's next server dial, see pool
// edits, access-pool changes, and rebuilt outbounds without a restart. The
// routes it can name mirror the engine's other reference forms (chain
// layers, inbound policies):
//
//   - "" / "direct": dial the target directly through the engine
//     stack (the pre-feature behavior)
//   - "outbound:<id>": one specific proxy server; a bare id resolves as a
//     pool when a pool with that id exists, else as an outbound — the same
//     precedence chain layers use
//   - "pool:<id>": pick a member with the pool's own selector, with
//     failover across members; the pool's access filter applies, so members
//     the local network cannot reach directly are dialed through the access
//     pool's proxies (auto-chain semantics)
//   - "chain:<id>": traverse a named chain layer by layer — real multi-hop
//     semantics: hop i tunnels toward hop i+1's proxy-server address, and
//     only the last hop connects toward the target
//   - "access": (providers only) test-dial the target directly first, then
//     fall back to the access pool of the pools this provider feeds — the
//     same direct-then-relay reachability testing the access filter
//     performs for pool members
//
// Nested routes (an upstream whose own server dial rides another route, a
// chain whose hop outbounds carry upstreams) are bounded by maxUpstreamDepth
// so a cyclic reference fails with a diagnostic instead of recursing.
package router

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/core"
	pub "github.com/goose-network/goose/pkg/plugin"
)

const (
	// upstreamDialTimeout bounds a single member dial attempt, matching the
	// budget of a normal router hop dial.
	upstreamDialTimeout = 30 * time.Second
	// directFirstTimeout bounds the direct attempt of the "access" route
	// before falling back to the access pool: enough to tell
	// locally-reachable from blocked without burning the fetch budget.
	directFirstTimeout = 5 * time.Second
	// upstreamTotalBudget bounds one whole fetch dial when the caller's
	// context has no deadline, so a pool failover loop cannot run unbounded.
	upstreamTotalBudget = 60 * time.Second
	// maxUpstreamDepth bounds how many upstream routes may nest inside one
	// dial: an outbound's server dial can ride a route whose members carry
	// their own routes, and a chain hop likewise. A cyclic reference
	// (outbound X routed through a pool containing X) would otherwise
	// recurse until the stack dies; the depth counter turns it into a
	// diagnostic instead.
	maxUpstreamDepth = 8
)

// upstreamDepthKey is the context key carrying the current route-nesting
// depth. Each DialUpstream entry increments it; the value flows through the
// engine's dial path (CheckDial, dialChain, hop DialContexts) into any
// nested DialUpstream call.
type upstreamDepthKey struct{}

// UpstreamDialer implements the provider side of spec's upstream source: it
// returns the dialer routing spec's own fetches through
// spec.UpstreamProxy, or nil when the spec configures none (the provider
// then fetches directly — the pre-feature behavior).
func (r *Router) UpstreamDialer(spec *config.ProviderSpec) pub.UpstreamDialer {
	if spec == nil || spec.UpstreamProxy == "" {
		return nil
	}
	return &routeDialer{r: r, route: spec.UpstreamProxy, spec: spec}
}

// OutboundUpstreamDialer returns the dialer routing one outbound's own
// server connection through route (the outbound-level
// config.OutboundSpec.UpstreamProxy), or nil for an empty route. The dialer
// re-resolves the route against live config per dial, exactly like the
// provider-side UpstreamDialer; the router injects it into the outbound's
// config map under pub.UpstreamDialerKey at build time. The "access" form
// is provider-only (an outbound feeds no pools) and errors at dial time.
func (r *Router) OutboundUpstreamDialer(route string) pub.UpstreamDialer {
	if route == "" {
		return nil
	}
	return &routeDialer{r: r, route: route}
}

// routeDialer dials a plugin's own targets through the route it was built
// from, re-resolved against the router's live state per dial. spec is
// non-nil for provider routes (it feeds the "access" fallback); nil for
// outbound-level routes.
type routeDialer struct {
	r     *Router
	route string
	spec  *config.ProviderSpec
}

// DialUpstream dials address (host:port) through the configured upstream
// route. The returned connection is a tunnel to address: the upstream
// proxy's handshake has already been performed, and the target protocol
// (TLS, HTTP, ...) runs over it directly.
func (d *routeDialer) DialUpstream(ctx context.Context, network, address string) (net.Conn, error) {
	// Bound the whole resolution when the caller did not: failover across a
	// pool's members must not run unbounded on a deadline-less context.
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, upstreamTotalBudget)
		defer cancel()
	}
	// Cycle guard: count this resolution, so a route that (transitively)
	// routes back through itself errors out instead of recursing.
	depth := 0
	if n, ok := ctx.Value(upstreamDepthKey{}).(int); ok {
		depth = n
	}
	if depth >= maxUpstreamDepth {
		return nil, fmt.Errorf("router: upstream_proxy %q: route nesting deeper than %d levels (cyclic reference?)", d.route, maxUpstreamDepth)
	}
	ctx = context.WithValue(ctx, upstreamDepthKey{}, depth+1)

	meta := core.Metadata{
		Network:   core.Network(network),
		StartedAt: time.Now(),
	}
	if host, port, err := net.SplitHostPort(address); err == nil {
		meta.TargetHost, meta.TargetPort = host, port
	}
	return d.r.dialUpstream(ctx, d.route, meta, d.spec)
}

// dialUpstream resolves route and dials meta's target through it. Every
// route converges on the same first-hop machinery dispatch uses, so
// access-filter narrowing and auto-chain relays behave identically for
// fetches and for user traffic.
func (r *Router) dialUpstream(ctx context.Context, route string, meta core.Metadata, spec *config.ProviderSpec) (net.Conn, error) {
	switch {
	case route == "" || route == "direct":
		return r.stack.Dial(ctx, string(meta.Network), meta.Target())
	case route == "access":
		if spec == nil {
			return nil, fmt.Errorf("router: upstream_proxy access: valid only on a provider (feeds pools); an outbound has no pools to resolve an access route from")
		}
		return r.dialUpstreamAccess(ctx, meta, spec)
	}
	kind, id := splitRoute(route)
	switch kind {
	case "outbound":
		return r.dialUpstreamOutbound(ctx, id, meta)
	case "pool":
		return r.dialUpstreamPool(ctx, id, meta)
	case "chain":
		return r.dialUpstreamChain(ctx, id, meta)
	case "":
		// Bare id: a pool with that id wins, else an outbound — the same
		// precedence chain layers use.
		if _, ok := r.lookupPool(id); ok {
			return r.dialUpstreamPool(ctx, id, meta)
		}
		return r.dialUpstreamOutbound(ctx, id, meta)
	default:
		return nil, fmt.Errorf("router: upstream_proxy %q: unknown prefix %q (want outbound:, pool:, chain:, access, or direct)", route, kind)
	}
}

// dialUpstreamChain traverses a named chain: the router's own dialChain
// semantics, so each layer's filters, selector, and access narrowing apply,
// hop i tunnels toward hop i+1's proxy-server address, and the final hop
// connects toward the fetch target.
func (r *Router) dialUpstreamChain(ctx context.Context, id string, meta core.Metadata) (net.Conn, error) {
	r.mu.RLock()
	ch, ok := r.chains[id]
	r.mu.RUnlock()
	if !ok || len(ch) == 0 {
		return nil, fmt.Errorf("router: upstream_proxy chain %q: not configured", id)
	}
	// r.chains is replaced wholesale on rebuild and never mutated in place,
	// so the snapshot chain stays valid after the lock is released; dialChain
	// copies each layer's candidate pool under its own lock before dialing.
	conn, _, err := r.dialChain(ctx, ch, meta)
	if err != nil {
		return nil, fmt.Errorf("router: upstream_proxy chain %s: %w", id, err)
	}
	return conn, nil
}

// dialUpstreamOutbound pins one specific proxy server. The dial follows the
// full first-hop path — including an auto-chain relay hop when that server
// is only reachable through its pool's access proxies (CheckDial).
func (r *Router) dialUpstreamOutbound(ctx context.Context, id string, meta core.Metadata) (net.Conn, error) {
	out, ok := r.lookupOutbound(id)
	if !ok {
		return nil, fmt.Errorf("router: upstream_proxy outbound %q: not configured", id)
	}
	dialCtx, cancel := context.WithTimeout(ctx, upstreamDialTimeout)
	defer cancel()
	conn, err := r.CheckDial(dialCtx, out, meta.Network, meta.Target())
	if err != nil {
		return nil, fmt.Errorf("router: upstream_proxy outbound %s: %w", id, err)
	}
	return conn, nil
}

// dialUpstreamPool dials through the members of one pool.
func (r *Router) dialUpstreamPool(ctx context.Context, poolID string, meta core.Metadata) (net.Conn, error) {
	snap, ok := r.snapshotPool(poolID)
	if !ok {
		return nil, fmt.Errorf("router: upstream_proxy pool %q: not configured", poolID)
	}
	return r.dialPoolSnapshot(ctx, "pool "+poolID, snap, meta)
}

// dialUpstreamAccess implements the "access" route: try the fetch target
// directly from the local network first — the engine host often sits on a
// less restricted network than anything else — then fall back to the access
// pool of the first pool this provider feeds that has an access filter,
// mirroring the direct-vs-relay testing the access filter applies to pool
// members.
func (r *Router) dialUpstreamAccess(ctx context.Context, meta core.Metadata, spec *config.ProviderSpec) (net.Conn, error) {
	directCtx, cancel := context.WithTimeout(ctx, directFirstTimeout)
	conn, derr := r.stack.Dial(directCtx, string(meta.Network), meta.Target())
	cancel()
	if derr == nil {
		return conn, nil
	}

	accessID := r.accessPoolForSpec(spec)
	if accessID == "" {
		return nil, fmt.Errorf("router: upstream_proxy access: direct dial %s failed: %w; no access pool configured on any pool the provider feeds", meta.Target(), derr)
	}
	snap, ok := r.snapshotPool(accessID)
	if !ok {
		return nil, fmt.Errorf("router: upstream_proxy access: direct dial %s failed: %w; access pool %q not configured", meta.Target(), derr, accessID)
	}
	return r.dialPoolSnapshot(ctx, "access pool "+accessID, snap, meta)
}

// poolSnapshot is an immutable snapshot of a built pool, taken under the
// read lock so member dials can proceed without holding it (rebuild replaces
// pools wholesale, so a snapshot never mutates under a dialer).
type poolSnapshot struct {
	bp         *builtPool
	candidates []core.Outbound
	filters    []core.Filter
	selector   core.Selector
}

// snapshotPool captures a pool's members, filters, and selector.
func (r *Router) snapshotPool(poolID string) (poolSnapshot, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	bp, ok := r.pools[poolID]
	if !ok {
		return poolSnapshot{}, false
	}
	snap := poolSnapshot{
		bp:         bp,
		candidates: make([]core.Outbound, len(bp.outbounds)),
		filters:    bp.filters,
		selector:   bp.selector,
	}
	copy(snap.candidates, bp.outbounds)
	if snap.selector == nil {
		snap.selector = &fallbackSelector{}
	}
	return snap, true
}

// dialPoolSnapshot dials meta's target through a snapshotted pool: narrow
// the members by the latest reachability probes when the pool is
// access-filtered, apply the pool's own filters, then pick with the pool's
// selector and fail over across the surviving members.
func (r *Router) dialPoolSnapshot(ctx context.Context, what string, snap poolSnapshot, meta core.Metadata) (net.Conn, error) {
	cands := snap.candidates
	if snap.bp.access != nil {
		cands = r.reachableCandidates(cands)
	}
	if len(cands) == 0 {
		return nil, fmt.Errorf("router: upstream_proxy %s: no reachable member", what)
	}
	cands = applyFilters(snap.filters, cands, meta)
	if len(cands) == 0 {
		return nil, fmt.Errorf("router: upstream_proxy %s: no member passed filters", what)
	}
	var lastErr error
	for len(cands) > 0 {
		out, err := snap.selector.Pick(cands, meta)
		if err != nil {
			return nil, fmt.Errorf("router: upstream_proxy %s select: %w", what, err)
		}
		dialCtx, cancel := context.WithTimeout(ctx, upstreamDialTimeout)
		conn, derr := r.CheckDial(dialCtx, out, meta.Network, meta.Target())
		cancel()
		if derr == nil {
			return conn, nil
		}
		lastErr = derr
		// Failover: drop the member that just failed and re-pick, so the
		// selector's rotation lands on a different one.
		cands = withoutOutbound(cands, out)
	}
	return nil, fmt.Errorf("router: upstream_proxy %s: every member dial failed: %w", what, lastErr)
}

// withoutOutbound returns pool without its first occurrence of out. Members
// are compared by identity: a pool snapshot holds the same outbound wrapper
// pointers the router built.
func withoutOutbound(pool []core.Outbound, out core.Outbound) []core.Outbound {
	for i, o := range pool {
		if o == out {
			return append(pool[:i], pool[i+1:]...)
		}
	}
	return pool
}

// lookupOutbound snapshots one built outbound by id.
func (r *Router) lookupOutbound(id string) (core.Outbound, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out, ok := r.outbounds[id]
	return out, ok
}

// lookupPool returns the built pool by id, an immutable snapshot.
func (r *Router) lookupPool(id string) (*builtPool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	bp, ok := r.pools[id]
	return bp, ok
}

// accessPoolForSpec returns the access pool id of the first pool the spec
// feeds that has an access filter configured, or "" when none does.
func (r *Router) accessPoolForSpec(spec *config.ProviderSpec) string {
	if spec == nil {
		return ""
	}
	for _, poolID := range spec.EffectivePoolIDs() {
		bp, ok := r.lookupPool(poolID)
		if !ok {
			continue
		}
		if bp.access != nil && bp.access.AccessPoolID != "" {
			return bp.access.AccessPoolID
		}
	}
	return ""
}

// splitRoute splits "kind:id" into kind and id; a value with no ":" is a
// bare id (kind "").
func splitRoute(route string) (kind, id string) {
	for i := 0; i < len(route); i++ {
		if route[i] == ':' {
			return route[:i], route[i+1:]
		}
	}
	return "", route
}
