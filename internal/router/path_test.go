// Observed-network-path tests for the dispatch side: what Router.Dial,
// dialLayers/dialOutbound, the upstream-route machinery and classifyLeg put
// into a request's core.PathGraph.
//
// The graph is strictly additive, so every test here also pins the Chain the
// router returned (or the row recorded): the adjacency list must not change
// what dispatch did, only describe it.
package router

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/core"
	"github.com/goose-network/goose/internal/inbound"
	_ "github.com/goose-network/goose/plugins/direct" // the real "direct" protocol plugin
	_ "github.com/goose-network/goose/plugins/socks5" // the real SOCKS5 outbound plugin
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- helpers (all dpath-prefixed: another test file in this package owns a
// --- different namespace and bare names would collide) ---------------------

// dpathNodeIDs renders a graph's node IDs in snapshot order.
func dpathNodeIDs(g *core.PathGraph) []string {
	ids := make([]string, 0, len(g.Nodes))
	for _, n := range g.Nodes {
		ids = append(ids, n.ID)
	}
	return ids
}

// dpathEdgePairs renders a graph's edges as "from->to" in observation order.
func dpathEdgePairs(g *core.PathGraph) []string {
	out := make([]string, 0, len(g.Edges))
	for _, e := range g.Edges {
		out = append(out, e.From+"->"+e.To)
	}
	return out
}

// dpathNode returns the graph's node with id, failing the test when absent.
func dpathNode(t *testing.T, g *core.PathGraph, id string) core.PathNode {
	t.Helper()
	for _, n := range g.Nodes {
		if n.ID == id {
			return n
		}
	}
	require.FailNow(t, "missing path node "+id, "graph nodes: %v", dpathNodeIDs(g))
	return core.PathNode{}
}

// dpathEdge returns the graph's one edge from->to, failing when there is not
// exactly one.
func dpathEdge(t *testing.T, g *core.PathGraph, from, to string) core.PathEdge {
	t.Helper()
	var found []core.PathEdge
	for _, e := range g.Edges {
		if e.From == from && e.To == to {
			found = append(found, e)
		}
	}
	require.Lenf(t, found, 1, "want exactly one edge %s->%s, edges: %v", from, to, dpathEdgePairs(g))
	return found[0]
}

// dpathEdgesTo returns every edge arriving at id.
func dpathEdgesTo(g *core.PathGraph, id string) []core.PathEdge {
	var out []core.PathEdge
	for _, e := range g.Edges {
		if e.To == id {
			out = append(out, e)
		}
	}
	return out
}

// dpathEdgesFrom returns every edge leaving id.
func dpathEdgesFrom(g *core.PathGraph, id string) []core.PathEdge {
	var out []core.PathEdge
	for _, e := range g.Edges {
		if e.From == id {
			out = append(out, e)
		}
	}
	return out
}

// dpathRefused is a typed connection-refused dial error: classification is
// deterministic because the errno is in the chain, not in a message.
func dpathRefused() error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
}

// dpathNXDomain is a typed resolver failure for name, used where a test needs a
// DNS phase without depending on a resolver being present (or absent) in the
// sandbox: classification reads the *net.DNSError in the chain, exactly as it
// does for a real lookup.
func dpathNXDomain(name string) error {
	return &net.OpError{Op: "dial", Net: "tcp",
		Err: &net.DNSError{Err: "no such host", Name: name, IsNotFound: true}}
}

// dpathObservedMeta builds a dispatch Metadata the way an inbound listener
// does: with its own recorder on it, so the SAME pointer reaches Dial and (in
// the bridge test) Bridge, and the test can snapshot the graph afterwards.
func dpathObservedMeta(inboundID, host, port string) (core.Metadata, *core.PathRecorder) {
	meta := metaFor(inboundID, host, port)
	meta.Path = core.NewPathRecorder()
	return meta, meta.Path
}

// dpathRouteCtx seeds a recorder exactly the way Router.Dial seeds one and
// returns a context carrying it — the shape an upstream-route dial finds when
// a plugin's own server dial rides a route.
func dpathRouteCtx(host, port string) (context.Context, *core.PathRecorder) {
	rec := core.NewPathRecorder()
	rec.SetClassifier(classifyLeg)
	rec.AddLocal()
	rec.AddTarget(host, port)
	return core.WithPathRecorder(context.Background(), rec), rec
}

// dpathRow polls the router's metrics store until a row matching match lands
// and returns it. The store's writer flushes on a 500ms tick, so the row is
// durable shortly after Record returned; polling (not sleeping) is the
// synchronization.
func dpathRow(t *testing.T, r *Router, match func(core.RequestMetric) bool) core.RequestMetric {
	t.Helper()
	var found core.RequestMetric
	require.Eventually(t, func() bool {
		rows, err := r.metrics.Recent(50)
		require.NoError(t, err)
		for _, m := range rows {
			if match(m) {
				found = m
				return true
			}
		}
		return false
	}, 10*time.Second, 25*time.Millisecond, "no metric row matched")
	return found
}

// --- dispatch: single hop ---------------------------------------------------

// TestDispatchPathSingleHopProxy pins the smallest real graph: one proxy hop
// produces three nodes and two legs — the timed dial that arrived at the
// server, and the untimed tunnel leg that one bracketed call also covered
// (the server's handshake toward the target).
func TestDispatchPathSingleHopProxy(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{{ID: "ob1", Protocol: "testdirect",
			Config: map[string]any{"id": "ob1", "address": "203.0.113.41:1080"}}},
		[]*config.Pool{{ID: "p1", OutboundIDs: []string{"ob1"}, Selector: config.SelectorSpec{Type: "roundrobin"}}},
		[]*config.ChainSpec{{ID: "c1", Layers: []string{"p1"}}},
		[]*config.Inbound{inboundOnChain("in", "c1")},
	)

	meta, rec := dpathObservedMeta("in", "example.com", "443")
	conn, chain, err := r.Dial(context.Background(), meta)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()

	// The graph is strictly additive: the chain is exactly what it was before.
	assert.Equal(t, []string{"ob1"}, chain)

	g := rec.Snapshot()
	require.NotNil(t, g, "a dispatched request always observes a path")
	assert.False(t, g.Partial)
	assert.Zero(t, g.Dropped)

	// Snapshot puts local first and the target LAST, even though the target
	// node was created before any dial.
	assert.Equal(t, []string{"local", "proxy:ob1", "target"}, dpathNodeIDs(g))

	local := dpathNode(t, g, "local")
	assert.Equal(t, core.NodeLocal, local.Kind)
	assert.Empty(t, local.Address)

	proxy := dpathNode(t, g, "proxy:ob1")
	assert.Equal(t, core.NodeProxy, proxy.Kind)
	assert.Equal(t, "ob1", proxy.OutboundID)
	assert.Equal(t, "203.0.113.41:1080", proxy.Address)
	assert.Equal(t, core.AddrConfigured, proxy.AddressSource,
		"a fake dial produces no socket peer, so the configured address stands")
	assert.Zero(t, proxy.Attempt, "a hop that was not one of several attempts carries no attempt number")

	target := dpathNode(t, g, "target")
	assert.Equal(t, core.NodeTarget, target.Kind)
	assert.Equal(t, "example.com", target.Host)
	assert.Equal(t, "443", target.Port)
	assert.Equal(t, "example.com:443", target.Address)
	assert.Equal(t, core.AddrConfigured, target.AddressSource)

	// Two legs: the timed arrival, then the tunnel leg the same call covered.
	require.Len(t, g.Edges, 2)
	assert.Equal(t, []string{"local->proxy:ob1", "proxy:ob1->target"}, dpathEdgePairs(g))

	dial := g.Edges[0]
	assert.Equal(t, core.LegDial, dial.Kind)
	assert.Equal(t, "example.com:443", dial.Target)
	assert.True(t, dial.Observed)
	assert.True(t, dial.Success)
	assert.Empty(t, dial.Error)
	assert.Greater(t, dial.Duration, time.Duration(0), "the hop dial was bracketed")
	assert.False(t, dial.Collapsed)

	tunnel := g.Edges[1]
	assert.Equal(t, core.LegTunnel, tunnel.Kind)
	assert.Equal(t, "example.com:443", tunnel.Target)
	assert.True(t, tunnel.Observed)
	assert.True(t, tunnel.Success)
	assert.Equal(t, time.Duration(0), tunnel.Duration,
		"the handshake toward the target rode inside the same call: real, but not separately timed")

	// The row Dial would record on failure carries this same graph; assert the
	// pointer identity the memoized snapshot guarantees.
	assert.Same(t, g, rec.Snapshot(), "Snapshot memoizes: one immutable graph per recorder")
}

// TestDispatchPathDirectHasNoProxyNode pins hopNodeID's "direct" rule: the
// direct protocol has no server of its own, so it gets NO vertex — the graph is
// local → target with one stack-level dial between them. The registered
// "testdirect" fake reports the protocol "testdirect", so this drives the real
// plugins/direct outbound against a loopback listener.
func TestDispatchPathDirectHasNoProxyNode(t *testing.T) {
	es := startEcho(t)
	echoAddr := es.ln.Addr().String()
	host, port := "127.0.0.1", portOf(t, echoAddr)

	r, _ := testRouter(t,
		[]*config.OutboundSpec{{ID: "dir", Protocol: "direct", Config: map[string]any{"id": "dir"}}},
		[]*config.Pool{{ID: "p1", OutboundIDs: []string{"dir"}, Selector: config.SelectorSpec{Type: "roundrobin"}}},
		[]*config.ChainSpec{{ID: "c1", Layers: []string{"p1"}}},
		[]*config.Inbound{inboundOnChain("in", "c1")},
	)

	meta, rec := dpathObservedMeta("in", host, port)
	conn, chain, err := r.Dial(context.Background(), meta)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()
	assert.Equal(t, []string{"dir"}, chain)

	// The direct hop really dialed through the engine stack: bytes flow.
	msg := []byte("dpath-direct")
	_, err = conn.Write(msg)
	require.NoError(t, err)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	buf := make([]byte, len(msg))
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	assert.Equal(t, msg, buf)

	g := rec.Snapshot()
	require.NotNil(t, g)
	assert.False(t, g.Partial)

	assert.Equal(t, []string{"local", "target"}, dpathNodeIDs(g), "a direct hop gets no proxy vertex")
	for _, n := range g.Nodes {
		assert.NotEqual(t, core.NodeProxy, n.Kind, "no proxy node for the direct protocol")
	}

	require.Len(t, g.Edges, 1)
	only := g.Edges[0]
	assert.Equal(t, "local", only.From)
	assert.Equal(t, "target", only.To)
	assert.Equal(t, core.LegDial, only.Kind)
	assert.Equal(t, echoAddr, only.Target)
	assert.True(t, only.Observed)
	assert.True(t, only.Success)
	assert.Greater(t, only.Duration, time.Duration(0))

	// The dial produced a real socket, so the target adopts the peer it really
	// connected to — the one address the engine can vouch for.
	target := dpathNode(t, g, "target")
	assert.Equal(t, host, target.Host)
	assert.Equal(t, port, target.Port)
	assert.Equal(t, echoAddr, target.Address)
	assert.Equal(t, core.AddrSocket, target.AddressSource)
}

// TestDispatchPathDirectHopWithUpstreamRoute pins the shape where a first hop
// has NO node of its own (protocol "direct", so hopNodeID returns "") but does
// have an upstream route: the plugin's own dial IS the route, so the route's
// two legs are the whole story. Dispatch's bracket around that call must add
// nothing — a local → target dial edge there would claim the engine host
// dialed the destination itself, and adopting the bracket's socket would stamp
// the route MEMBER's peer address onto the target node (which is what a client
// labels the destination with).
func TestDispatchPathDirectHopWithUpstreamRoute(t *testing.T) {
	memberSrv := socks5Server(t) // the route member's proxy server
	es := startEcho(t)
	echoAddr := es.ln.Addr().String()

	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			// "direct" with a route of its own: plugins/direct prefers the
			// injected upstream dialer over the engine stack.
			{ID: "d", Protocol: "direct", UpstreamProxy: "outbound:m1",
				Config: map[string]any{"id": "d"}},
			{ID: "m1", Protocol: "socks5", Config: map[string]any{"id": "m1", "address": memberSrv}},
		},
		[]*config.Pool{{ID: "p1", OutboundIDs: []string{"d"}, Selector: config.SelectorSpec{Type: "roundrobin"}}},
		[]*config.ChainSpec{{ID: "c1", Layers: []string{"p1"}}},
		[]*config.Inbound{inboundOnChain("in", "c1")},
	)

	meta, rec := dpathObservedMeta("in", "127.0.0.1", portOf(t, echoAddr))
	conn, chain, err := r.Dial(context.Background(), meta)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()
	assert.Equal(t, []string{"d"}, chain)

	// The route really carries the request: engine → m1 → echo.
	msg := []byte("dpath-direct-upstream")
	_, err = conn.Write(msg)
	require.NoError(t, err)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	buf := make([]byte, len(msg))
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	assert.Equal(t, msg, buf)

	g := rec.Snapshot()
	require.NotNil(t, g)
	assert.False(t, g.Partial)
	assert.Zero(t, g.Dropped)
	assert.Equal(t, []string{"local", "proxy:m1", "target"}, dpathNodeIDs(g),
		"a direct hop gets no vertex; the route member does")

	// The route's own two legs and nothing else: dispatch's bracket is the same
	// physical call the route already recorded, so it adds no third leg.
	require.Len(t, g.Edges, 2)
	assert.Equal(t, []string{"local->proxy:m1", "proxy:m1->target"}, dpathEdgePairs(g))
	assert.Empty(t, dpathEdgeIfExists(g, "local", "target"),
		"the engine host never dialed the target: the route member did")
	assert.Len(t, dpathEdgesFrom(g, "local"), 1)
	assert.Len(t, dpathEdgesTo(g, "target"), 1)

	member := dpathNode(t, g, "proxy:m1")
	assert.Equal(t, memberSrv, member.Address)
	assert.Equal(t, core.AddrSocket, member.AddressSource,
		"the member's server dial is the one leg that opened an engine socket")

	// The bracket adopts nothing, so the destination keeps the address config
	// named instead of the route member's socket peer.
	target := dpathNode(t, g, "target")
	assert.Equal(t, "127.0.0.1", target.Host)
	assert.Equal(t, portOf(t, echoAddr), target.Port)
	assert.Equal(t, echoAddr, target.Address)
	assert.Equal(t, core.AddrConfigured, target.AddressSource,
		"reached through the member's handshake, never off a socket of its own")
}

// --- dispatch: chains -------------------------------------------------------

// TestDispatchPathMultiHopChain pins a two-hop chain: the legs read in dial
// order (local → o0 → o1 → target) and the Chain is byte-identical to what it
// was before the graph existed.
func TestDispatchPathMultiHopChain(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "o0", Protocol: "testchain", Config: map[string]any{"id": "o0", "address": "203.0.113.61:1080"}},
			{ID: "o1", Protocol: "testchain", Config: map[string]any{"id": "o1", "address": "203.0.113.62:1080"}},
		},
		[]*config.Pool{
			{ID: "l0", OutboundIDs: []string{"o0"}, Selector: config.SelectorSpec{Type: "roundrobin"}},
			{ID: "l1", OutboundIDs: []string{"o1"}, Selector: config.SelectorSpec{Type: "roundrobin"}},
		},
		[]*config.ChainSpec{{ID: "c2", Layers: []string{"l0", "l1"}}},
		[]*config.Inbound{inboundOnChain("in", "c2")},
	)

	meta, rec := dpathObservedMeta("in", "example.com", "443")
	conn, chain, err := r.Dial(context.Background(), meta)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()

	// Several existing tests pattern-match Chain; the graph must not touch it.
	assert.Equal(t, []string{"o0", "o1"}, chain)

	g := rec.Snapshot()
	require.NotNil(t, g)
	assert.False(t, g.Partial)
	// Ordered, not set-matched: PathGraph.Nodes documents proxies in dial
	// order, and dialLayers must name hop 0's node before hop 1's.
	assert.Equal(t, []string{"local", "proxy:o0", "proxy:o1", "target"}, dpathNodeIDs(g))
	assert.Equal(t, "local", g.Nodes[0].ID, "the path starts at the engine host")
	assert.Equal(t, "target", g.Nodes[len(g.Nodes)-1].ID, "Snapshot puts the target last")

	// Hop 0 dials hop 1's SERVER address, so the first two legs name it.
	require.Len(t, g.Edges, 3)
	assert.Equal(t, []string{"local->proxy:o0", "proxy:o0->proxy:o1", "proxy:o1->target"}, dpathEdgePairs(g))

	first := g.Edges[0]
	assert.Equal(t, core.LegDial, first.Kind)
	assert.Equal(t, "203.0.113.62:1080", first.Target, "hop 0 dials hop 1's proxy-server address")
	assert.True(t, first.Success)
	assert.Greater(t, first.Duration, time.Duration(0))

	mid := g.Edges[1]
	assert.Equal(t, core.LegTunnel, mid.Kind)
	assert.Equal(t, "203.0.113.62:1080", mid.Target)
	assert.True(t, mid.Observed)
	assert.True(t, mid.Success)
	assert.Equal(t, time.Duration(0), mid.Duration, "one bracketed call covered both physical legs")

	last := g.Edges[2]
	assert.Equal(t, core.LegTunnel, last.Kind)
	assert.Equal(t, "example.com:443", last.Target, "only the final hop dials toward the request target")
	assert.True(t, last.Observed)
	assert.True(t, last.Success)
	assert.Greater(t, last.Duration, time.Duration(0), "the chained handshake was bracketed on its own")
	assert.False(t, last.Collapsed)
}

// TestDispatchPathMarksCollapsedHop pins what the graph says about a hop that
// could not be dialed through the previous conn and re-dialed from the engine
// host instead: the edge carries Collapsed, and the wasted tunnel the hop
// before it established is still in the graph (the graph reports what
// happened, not what was useful).
func TestDispatchPathMarksCollapsedHop(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "o0", Protocol: "testchain", Config: map[string]any{"id": "o0", "address": "203.0.113.51:1080"}},
			// No ChainDialer: this hop collapses to a dial from local.
			{ID: "o1", Protocol: "testdirect", Config: map[string]any{"id": "o1", "address": "203.0.113.52:1080"}},
		},
		[]*config.Pool{
			{ID: "l0", OutboundIDs: []string{"o0"}, Selector: config.SelectorSpec{Type: "roundrobin"}},
			{ID: "l1", OutboundIDs: []string{"o1"}, Selector: config.SelectorSpec{Type: "roundrobin"}},
		},
		[]*config.ChainSpec{{ID: "c2", Layers: []string{"l0", "l1"}}},
		[]*config.Inbound{inboundOnChain("in", "c2")},
	)

	meta, rec := dpathObservedMeta("in", "example.com", "443")
	conn, chain, err := r.Dial(context.Background(), meta)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()
	assert.Equal(t, []string{"o0", "o1"}, chain)

	g := rec.Snapshot()
	require.NotNil(t, g)
	assert.Equal(t,
		[]string{"local->proxy:o0", "proxy:o0->proxy:o1", "local->proxy:o1", "proxy:o1->target"},
		dpathEdgePairs(g),
		"the collapse adds a second, real leg out of local rather than reusing the tunnel it wasted")

	// The tunnel the previous hop established toward o1 is still recorded,
	// even though the collapse wasted it.
	prev := dpathEdge(t, g, "proxy:o0", "proxy:o1")
	assert.Equal(t, core.LegTunnel, prev.Kind)
	assert.False(t, prev.Collapsed)
	assert.True(t, prev.Success)

	var collapsed []core.PathEdge
	for _, e := range g.Edges {
		if e.Collapsed {
			collapsed = append(collapsed, e)
		}
	}
	require.Len(t, collapsed, 1, "exactly one leg re-dialed from the engine host")
	assert.True(t, collapsed[0].Success)
	assert.True(t, collapsed[0].Observed)
	assert.Equal(t, "example.com:443", collapsed[0].Target)

	// The collapsed leg reads From "local" as a LegDial, which is what
	// dialOutbound means by "the leg really did leave the engine host": the
	// nested-upstream suppression does not apply to it, because lastNode
	// matching this hop's server is a coincidence of the wasted tunnel, not
	// evidence that a route the engine recorded carried this dial.
	assert.Equal(t, core.NodeLocalID, collapsed[0].From)
	assert.Equal(t, "proxy:o1", collapsed[0].To)
	assert.Equal(t, core.LegDial, collapsed[0].Kind)
	assert.Greater(t, collapsed[0].Duration, time.Duration(0), "the re-dial was bracketed on its own")
	assert.Len(t, dpathEdgesFrom(g, core.NodeLocalID), 2,
		"two legs left the engine host: the first hop's dial and the collapsed re-dial")

	// The handshake the collapsed hop then performed toward the request target
	// is the second leg of that one bracketed call, so it is real but untimed.
	handshake := dpathEdge(t, g, "proxy:o1", "target")
	assert.Equal(t, core.LegTunnel, handshake.Kind)
	assert.False(t, handshake.Collapsed, "only the leg that left local carries Collapsed")
	assert.True(t, handshake.Observed)
	assert.True(t, handshake.Success)
	assert.Equal(t, time.Duration(0), handshake.Duration)

	// Address adoption follows the same rule as the edge shape: the socket a leg
	// produced belongs to the server that leg dialed. The fake outbound hands
	// back a pipe end whose peer is not a host:port, so nothing is adopted and
	// both nodes keep their configured addresses.
	assert.Equal(t, "203.0.113.52:1080", dpathNode(t, g, "proxy:o1").Address)
	assert.Equal(t, "example.com:443", dpathNode(t, g, "target").Address)
	assert.Equal(t, core.AddrConfigured, dpathNode(t, g, "target").AddressSource)
}

// TestDispatchPathThreeHopChainNodeOrder pins PathGraph.Nodes' documented
// contract on a chain long enough for the order to be non-trivial: "local
// first, proxies in dial order, target last". dialLayers names each hop's own
// node before the successor it dials toward, so a client that lays the route
// out in wire order reads it left to right without walking the edges.
func TestDispatchPathThreeHopChainNodeOrder(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "h0", Protocol: "testchain", Config: map[string]any{"id": "h0", "address": "203.0.113.81:1080"}},
			{ID: "h1", Protocol: "testchain", Config: map[string]any{"id": "h1", "address": "203.0.113.82:1080"}},
			{ID: "h2", Protocol: "testchain", Config: map[string]any{"id": "h2", "address": "203.0.113.83:1080"}},
		},
		[]*config.Pool{
			{ID: "l0", OutboundIDs: []string{"h0"}, Selector: config.SelectorSpec{Type: "roundrobin"}},
			{ID: "l1", OutboundIDs: []string{"h1"}, Selector: config.SelectorSpec{Type: "roundrobin"}},
			{ID: "l2", OutboundIDs: []string{"h2"}, Selector: config.SelectorSpec{Type: "roundrobin"}},
		},
		[]*config.ChainSpec{{ID: "c3", Layers: []string{"l0", "l1", "l2"}}},
		[]*config.Inbound{inboundOnChain("in", "c3")},
	)

	meta, rec := dpathObservedMeta("in", "example.com", "443")
	conn, chain, err := r.Dial(context.Background(), meta)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()
	assert.Equal(t, []string{"h0", "h1", "h2"}, chain)

	g := rec.Snapshot()
	require.NotNil(t, g)
	assert.False(t, g.Partial)

	// The hop order dispatch selected, verbatim — an ordered slice, not a set.
	assert.Equal(t, []string{"local", "proxy:h0", "proxy:h1", "proxy:h2", "target"}, dpathNodeIDs(g))
	for i, id := range chain {
		assert.Equalf(t, "proxy:"+id, g.Nodes[i+1].ID,
			"node %d is the %dth hop dialed (Nodes[0] is local)", i+1, i)
	}

	require.Len(t, g.Edges, 4)
	assert.Equal(t,
		[]string{"local->proxy:h0", "proxy:h0->proxy:h1", "proxy:h1->proxy:h2", "proxy:h2->target"},
		dpathEdgePairs(g), "the edges walk the node list front to back")

	// Hop i dials hop i+1's SERVER address; only the final hop dials toward the
	// request target.
	assert.Equal(t, "203.0.113.82:1080", g.Edges[0].Target)
	assert.Equal(t, "203.0.113.83:1080", g.Edges[2].Target)
	assert.Equal(t, "example.com:443", g.Edges[3].Target)
}

// TestDispatchPathUncoveredDirectHopMarksPartial pins dialOutbound's "hop with
// no server of its own" branch. A direct hop's DialThrough hands the previous
// conn back unchanged, so the call is not bracketed — and the previous hop's
// tunnel edge covers it ONLY when the recorder is standing at the node this hop
// was headed for. A direct hop that configures an address breaks that premise:
// hop 0 tunneled toward a bare server vertex for that address, not toward the
// request target. The graph then says it may be incomplete rather than
// inventing a leg into the target that nobody observed.
func TestDispatchPathUncoveredDirectHopMarksPartial(t *testing.T) {
	// build wires chain [relay, last]: relay is chain-capable, last is the real
	// "direct" protocol — no server of its own (hopNodeID returns "") but a
	// configured address when lastAddr is non-empty.
	build := func(t *testing.T, lastAddr string) *Router {
		t.Helper()
		lastCfg := map[string]any{"id": "last"}
		if lastAddr != "" {
			lastCfg["address"] = lastAddr
		}
		r, _ := testRouter(t,
			[]*config.OutboundSpec{
				{ID: "relay", Protocol: "testchain", Config: map[string]any{"id": "relay", "address": "203.0.113.91:1080"}},
				{ID: "last", Protocol: "direct", Config: lastCfg},
			},
			[]*config.Pool{
				{ID: "l0", OutboundIDs: []string{"relay"}, Selector: config.SelectorSpec{Type: "roundrobin"}},
				{ID: "l1", OutboundIDs: []string{"last"}, Selector: config.SelectorSpec{Type: "roundrobin"}},
			},
			[]*config.ChainSpec{{ID: "c2", Layers: []string{"l0", "l1"}}},
			[]*config.Inbound{inboundOnChain("in", "c2")},
		)
		return r
	}

	t.Run("direct hop with an address", func(t *testing.T) {
		r := build(t, "203.0.113.9:1080")

		meta, rec := dpathObservedMeta("in", "example.com", "443")
		conn, chain, err := r.Dial(context.Background(), meta)
		require.NoError(t, err)
		require.NotNil(t, conn)
		defer conn.Close()
		assert.Equal(t, []string{"relay", "last"}, chain, "the graph changes no dispatch decision")

		g := rec.Snapshot()
		require.NotNil(t, g)
		assert.Equal(t,
			[]string{"local", "proxy:relay", "server:203.0.113.9:1080", "target"}, dpathNodeIDs(g))
		assert.Equal(t,
			[]string{"local->proxy:relay", "proxy:relay->server:203.0.113.9:1080"}, dpathEdgePairs(g))
		assert.Empty(t, dpathEdgesTo(g, "target"), "no leg may be invented toward the target")
		assert.True(t, g.Partial,
			"the request succeeded but the graph cannot show the leg that reached the target")
		assert.Zero(t, g.Dropped, "nothing was dropped for size: this is a missing observation")
	})

	t.Run("direct hop with no address is covered", func(t *testing.T) {
		r := build(t, "")

		meta, rec := dpathObservedMeta("in", "example.com", "443")
		conn, chain, err := r.Dial(context.Background(), meta)
		require.NoError(t, err)
		require.NotNil(t, conn)
		defer conn.Close()
		assert.Equal(t, []string{"relay", "last"}, chain)

		g := rec.Snapshot()
		require.NotNil(t, g)
		// With no address of its own, hop 0 tunnels toward the request target
		// itself, so the recorder IS standing where hop 1 was headed and the
		// branch's premise holds: the leg needs no second record.
		assert.Equal(t, []string{"local", "proxy:relay", "target"}, dpathNodeIDs(g))
		assert.Equal(t, []string{"local->proxy:relay", "proxy:relay->target"}, dpathEdgePairs(g))
		assert.Len(t, dpathEdgesTo(g, "target"), 1)
		assert.False(t, g.Partial)
	})
}

// --- dispatch: failures -----------------------------------------------------

// TestDispatchPathFailureKeepsTargetNode pins that a total first-hop failure
// still names the destination: the target node was created up front, so the
// graph is [local, proxy, target] with one failed leg and an ISOLATED target —
// no leg is invented toward a destination nothing reached.
func TestDispatchPathFailureKeepsTargetNode(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{{ID: "ob1", Protocol: "testdirect",
			Config: map[string]any{"id": "ob1", "address": "203.0.113.71:1080"}}},
		[]*config.Pool{{ID: "p1", OutboundIDs: []string{"ob1"}, Selector: config.SelectorSpec{Type: "roundrobin"}}},
		[]*config.ChainSpec{{ID: "c1", Layers: []string{"p1"}}},
		[]*config.Inbound{inboundOnChain("in", "c1")},
	)
	fakeOutbound(t, r, "ob1").DialErr = dpathRefused()

	meta, rec := dpathObservedMeta("in", "example.com", "443")
	_, chain, err := r.Dial(context.Background(), meta)
	require.Error(t, err)
	require.ErrorContains(t, err, "layer 0 dial ob1")
	assert.Equal(t, []string{"ob1"}, chain, "a failed hop is still reported as selected")

	g := rec.Snapshot()
	require.NotNil(t, g)
	assert.Equal(t, []string{"local", "proxy:ob1", "target"}, dpathNodeIDs(g))
	assert.False(t, g.Partial, "a leg that never happened is not a missing observation")

	require.Len(t, g.Edges, 1)
	failed := g.Edges[0]
	assert.Equal(t, "local", failed.From)
	assert.Equal(t, "proxy:ob1", failed.To)
	assert.Equal(t, core.LegDial, failed.Kind)
	assert.False(t, failed.Success)
	assert.True(t, failed.Observed)
	assert.Contains(t, failed.Error, "connection refused")
	assert.Equal(t, reasonConnectRefused, failed.Reason, "the leg keeps its own classification")
	assert.Equal(t, phaseConnect, failed.Phase)
	assert.Greater(t, failed.Duration, time.Duration(0), "the failed dial was still bracketed")

	// The target node stands alone: nothing reached it.
	assert.Empty(t, dpathEdgesTo(g, "target"), "no leg may be invented toward the target")
	target := dpathNode(t, g, "target")
	assert.Equal(t, "example.com", target.Host)
	assert.Equal(t, "443", target.Port)

	// The failure row carries exactly this graph.
	row := dpathRow(t, r, func(m core.RequestMetric) bool { return !m.Success })
	assert.Equal(t, reasonConnectRefused, row.Reason)
	assert.Equal(t, g, row.Path, "the row carries the observed graph, not a copy of the recorder")
}

// TestDispatchPathRouteResolutionFailureHasNoEdges pins the other failure
// shape: route resolution died before any dial, so the graph has the two nodes
// Dial always creates and ZERO edges — while the row still carries the
// classified reason.
func TestDispatchPathRouteResolutionFailureHasNoEdges(t *testing.T) {
	r, _ := testRouter(t,
		nil, // no outbounds: the pool below is empty
		[]*config.Pool{{ID: "p1", OutboundIDs: []string{"ghost"}, Selector: config.SelectorSpec{Type: "roundrobin"}}},
		[]*config.ChainSpec{{ID: "c1", Layers: []string{"p1"}}},
		[]*config.Inbound{inboundOnChain("in", "c1")},
	)

	meta, rec := dpathObservedMeta("in", "example.com", "443")
	_, chain, err := r.Dial(context.Background(), meta)
	require.Error(t, err)
	require.ErrorContains(t, err, "empty pool")
	assert.Empty(t, chain)

	g := rec.Snapshot()
	require.NotNil(t, g, "a route that never dialled still names where it was headed")
	assert.Equal(t, []string{"local", "target"}, dpathNodeIDs(g))
	assert.Empty(t, g.Edges, "no leg was observed, so none is recorded")
	assert.False(t, g.Partial)
	assert.Zero(t, g.Dropped)

	target := dpathNode(t, g, "target")
	assert.Equal(t, "example.com", target.Host)
	assert.Equal(t, "443", target.Port)
	assert.Equal(t, "example.com:443", target.Address)
	assert.Equal(t, core.AddrConfigured, target.AddressSource)

	row := dpathRow(t, r, func(m core.RequestMetric) bool { return !m.Success })
	assert.Equal(t, classifyErr(err), row.Reason, "the row's reason still comes from the generic classifier")
	assert.Equal(t, reasonRouteUnavail, row.Reason)
	assert.Empty(t, row.Chain)
	assert.Equal(t, g, row.Path)
}

// --- nested upstream routes -------------------------------------------------

// TestNestedUpstreamRouteAppearsInPath pins the nested-upstream shape: an
// outbound whose own server connection rides an engine route produces
// local → (route member) → (outer server) → target, with NO second
// local → (outer server) leg. The outer hop's arrival is suppressed because
// the route already recorded how that server was reached — claiming a direct
// local leg as well would say the engine reached it twice, one of them a lie.
//
// Both hops are the real SOCKS5 plugin, so every leg here is a real socket:
// px dials its own server R through the engine stack and CONNECTs toward the
// outer server X; outer rides that tunnel and CONNECTs toward the echo target.
func TestNestedUpstreamRouteAppearsInPath(t *testing.T) {
	outerSrv := socks5Server(t)  // the outer outbound's own proxy server (X)
	memberSrv := socks5Server(t) // the route member's proxy server (R)
	es := startEcho(t)
	echoAddr := es.ln.Addr().String()

	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "outer", Protocol: "socks5", UpstreamProxy: "outbound:px",
				Config: map[string]any{"id": "outer", "address": outerSrv}},
			{ID: "px", Protocol: "socks5",
				Config: map[string]any{"id": "px", "address": memberSrv}},
		},
		[]*config.Pool{{ID: "p1", OutboundIDs: []string{"outer"}, Selector: config.SelectorSpec{Type: "roundrobin"}}},
		[]*config.ChainSpec{{ID: "c1", Layers: []string{"p1"}}},
		[]*config.Inbound{inboundOnChain("in", "c1")},
	)

	meta, rec := dpathObservedMeta("in", "127.0.0.1", portOf(t, echoAddr))
	conn, chain, err := r.Dial(context.Background(), meta)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()
	assert.Equal(t, []string{"outer"}, chain)

	// The whole nested path really carries bytes: engine → px(R) → outer(X) → echo.
	msg := []byte("dpath-nested-upstream")
	_, err = conn.Write(msg)
	require.NoError(t, err)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	buf := make([]byte, len(msg))
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	assert.Equal(t, msg, buf)

	g := rec.Snapshot()
	require.NotNil(t, g)
	assert.False(t, g.Partial)
	// Ordered, not set-matched: the outer hop's node is named by dialLayers
	// before the nested route names its own member, so proxy:outer precedes
	// proxy:px even though the leg between them runs the other way.
	assert.Equal(t, []string{"local", "proxy:outer", "proxy:px", "target"}, dpathNodeIDs(g))
	assert.Equal(t, "local", g.Nodes[0].ID)
	assert.Equal(t, "target", g.Nodes[len(g.Nodes)-1].ID)

	// The route member's server is one physical server the engine dialled, so
	// its node adopts the socket peer; the outer server was never dialled from
	// local, so it keeps its configured address.
	member := dpathNode(t, g, "proxy:px")
	assert.Equal(t, "px", member.OutboundID)
	assert.Equal(t, memberSrv, member.Address)
	assert.Equal(t, core.AddrSocket, member.AddressSource)

	outer := dpathNode(t, g, "proxy:outer")
	assert.Equal(t, "outer", outer.OutboundID)
	assert.Equal(t, outerSrv, outer.Address)
	assert.Equal(t, core.AddrConfigured, outer.AddressSource)

	require.Len(t, g.Edges, 3)
	assert.Equal(t, []string{"local->proxy:px", "proxy:px->proxy:outer", "proxy:outer->target"}, dpathEdgePairs(g))

	// The nested leg dialled the OUTER SERVER, not the request target.
	arrival := g.Edges[0]
	assert.Equal(t, core.LegDial, arrival.Kind)
	assert.Equal(t, outerSrv, arrival.Target)
	assert.True(t, arrival.Success)
	assert.Greater(t, arrival.Duration, time.Duration(0))

	// noteUpstreamArrival's leg: real, but not separately timed (it happened
	// inside the same plugin call the arrival was bracketed for).
	tunnel := g.Edges[1]
	assert.Equal(t, core.LegTunnel, tunnel.Kind)
	assert.Equal(t, outerSrv, tunnel.Target)
	assert.True(t, tunnel.Observed)
	assert.True(t, tunnel.Success)
	assert.Equal(t, time.Duration(0), tunnel.Duration)

	// The outer hop's own handshake toward the target, bracketed by dispatch.
	handshake := g.Edges[2]
	assert.Equal(t, core.LegTunnel, handshake.Kind)
	assert.Equal(t, echoAddr, handshake.Target)
	assert.True(t, handshake.Success)
	assert.Greater(t, handshake.Duration, time.Duration(0))

	// The point of the suppression: exactly one leg left the engine host, and
	// there is NO second local → outer-server leg.
	assert.Len(t, dpathEdgesFrom(g, "local"), 1)
	assert.Empty(t, dpathEdgeIfExists(g, "local", "proxy:outer"))
	assert.Len(t, dpathEdgesTo(g, "target"), 1)

	// The suppressed arrival leg adopts no socket: the conn it produced belongs
	// to the nested route's member, which the leg that really dialed it already
	// adopted. So the target keeps the configured echo address and never picks
	// up the route member's socket peer.
	target := dpathNode(t, g, "target")
	assert.Equal(t, "127.0.0.1", target.Host)
	assert.Equal(t, portOf(t, echoAddr), target.Port)
	assert.Equal(t, echoAddr, target.Address)
	assert.Equal(t, core.AddrConfigured, target.AddressSource,
		"reached through the outer hop's handshake, never off a socket of its own")
}

// TestNestedUpstreamRouteTwoLevelsKeepsOneLegPerHop is finding 5's shape: a
// route nested TWO levels deep (outer rides mid, mid rides deep). Only the
// innermost member dial really left the engine host, so the graph must read
// local → deep → mid → outer → target. A second leg out of local for `mid`
// would claim the engine holds a socket to a server it only reached through
// deep, and adopting that bracket's socket would hand deep's peer address to
// mid's node — two vertices claiming one server, mid's own address gone.
func TestNestedUpstreamRouteTwoLevelsKeepsOneLegPerHop(t *testing.T) {
	outerSrv := socks5Server(t) // the outer outbound's own proxy server (X)
	midSrv := socks5Server(t)   // the middle route member's server (M)
	deepSrv := socks5Server(t)  // the innermost route member's server (D)
	es := startEcho(t)
	echoAddr := es.ln.Addr().String()

	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "outer", Protocol: "socks5", UpstreamProxy: "outbound:mid",
				Config: map[string]any{"id": "outer", "address": outerSrv}},
			{ID: "mid", Protocol: "socks5", UpstreamProxy: "outbound:deep",
				Config: map[string]any{"id": "mid", "address": midSrv}},
			{ID: "deep", Protocol: "socks5",
				Config: map[string]any{"id": "deep", "address": deepSrv}},
		},
		[]*config.Pool{{ID: "p1", OutboundIDs: []string{"outer"}, Selector: config.SelectorSpec{Type: "roundrobin"}}},
		[]*config.ChainSpec{{ID: "c1", Layers: []string{"p1"}}},
		[]*config.Inbound{inboundOnChain("in", "c1")},
	)

	meta, rec := dpathObservedMeta("in", "127.0.0.1", portOf(t, echoAddr))
	conn, chain, err := r.Dial(context.Background(), meta)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()
	assert.Equal(t, []string{"outer"}, chain)

	// The whole nested path really carries bytes: engine → deep → mid → outer → echo.
	msg := []byte("dpath-two-level-nested")
	_, err = conn.Write(msg)
	require.NoError(t, err)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	buf := make([]byte, len(msg))
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	assert.Equal(t, msg, buf)

	g := rec.Snapshot()
	require.NotNil(t, g)
	assert.False(t, g.Partial)
	assert.Zero(t, g.Dropped)

	// Dispatch names the hop's node first, so the outer vertex precedes the two
	// the nested route names below it even though the legs run the other way.
	assert.Equal(t,
		[]string{"local", "proxy:outer", "proxy:mid", "proxy:deep", "target"}, dpathNodeIDs(g))

	require.Len(t, g.Edges, 4)
	assert.Equal(t,
		[]string{"local->proxy:deep", "proxy:deep->proxy:mid", "proxy:mid->proxy:outer", "proxy:outer->target"},
		dpathEdgePairs(g))

	// Exactly one leg left the engine host — the innermost member's dial. The
	// two levels above it were reached THROUGH that tunnel, never from local.
	assert.Len(t, dpathEdgesFrom(g, "local"), 1)
	assert.Empty(t, dpathEdgeIfExists(g, "local", "proxy:mid"),
		"the engine never dialed mid itself: it reached mid through deep")
	assert.Empty(t, dpathEdgeIfExists(g, "local", "proxy:outer"),
		"the engine never dialed outer itself: it reached outer through mid")

	// The innermost leg dialed MID's server (that is what its route was asked
	// for), and only that leg opened an engine socket.
	inner := g.Edges[0]
	assert.Equal(t, core.LegDial, inner.Kind)
	assert.Equal(t, midSrv, inner.Target)
	assert.True(t, inner.Success)
	assert.Greater(t, inner.Duration, time.Duration(0))
	assert.False(t, inner.Collapsed)

	// The two route-arrival legs happened inside the bracketed calls that
	// reached them, so they are real but not separately timed.
	for _, pair := range [][2]string{{"proxy:deep", "proxy:mid"}, {"proxy:mid", "proxy:outer"}} {
		leg := dpathEdge(t, g, pair[0], pair[1])
		assert.Equal(t, core.LegTunnel, leg.Kind)
		assert.True(t, leg.Observed)
		assert.True(t, leg.Success)
		assert.Equal(t, time.Duration(0), leg.Duration)
	}

	// The last leg is the outer hop's own handshake toward the request target,
	// rewritten from the suppressed arrival into a tunnel from its own server.
	handshake := dpathEdge(t, g, "proxy:outer", "target")
	assert.Equal(t, core.LegTunnel, handshake.Kind)
	assert.Equal(t, echoAddr, handshake.Target)
	assert.True(t, handshake.Success)
	assert.Greater(t, handshake.Duration, time.Duration(0),
		"the bracket around outer's own dial is the span this leg carries")

	// Addresses: deep adopted the socket it really dialed; mid and outer keep
	// their OWN configured addresses rather than the deeper hop's peer.
	assert.Equal(t, deepSrv, dpathNode(t, g, "proxy:deep").Address)
	assert.Equal(t, core.AddrSocket, dpathNode(t, g, "proxy:deep").AddressSource)
	assert.Equal(t, midSrv, dpathNode(t, g, "proxy:mid").Address,
		"mid keeps the address config named, not deep's socket peer")
	assert.Equal(t, core.AddrConfigured, dpathNode(t, g, "proxy:mid").AddressSource)
	assert.Equal(t, outerSrv, dpathNode(t, g, "proxy:outer").Address)
	assert.Equal(t, core.AddrConfigured, dpathNode(t, g, "proxy:outer").AddressSource)
	assert.Equal(t, echoAddr, dpathNode(t, g, "target").Address)
	assert.Equal(t, core.AddrConfigured, dpathNode(t, g, "target").AddressSource)

	// One physical server, one vertex claiming it.
	claiming := 0
	for _, n := range g.Nodes {
		if n.Address == deepSrv {
			claiming++
		}
	}
	assert.Equal(t, 1, claiming, "only deep's node may claim deep's server address")
}

// TestCollapsedHopWithUpstreamRouteKeepsItsOwnLeg is finding 6's shape: a chain
// hop that cannot be dialed through the previous conn (no ChainDialer, so it
// COLLAPSES to a dial from the engine host) and whose own dial rides an
// upstream route. Both legs really left local, and each belongs to the node
// that made it: the route member's dial is local → member (never
// hop-server → member), and the collapsed hop keeps its own configured address
// instead of adopting the member's socket peer.
func TestCollapsedHopWithUpstreamRouteKeepsItsOwnLeg(t *testing.T) {
	memberSrv := socks5Server(t) // the route member's proxy server
	es := startEcho(t)
	echoAddr := es.ln.Addr().String()

	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "o0", Protocol: "testchain", Config: map[string]any{"id": "o0", "address": "203.0.113.52:1080"}},
			// No ChainDialer: this hop collapses to a dial from local, and its
			// own dial rides the route below.
			{ID: "o1", Protocol: "testupstream", UpstreamProxy: "outbound:m1",
				Config: map[string]any{"id": "o1"}},
			{ID: "m1", Protocol: "socks5", Config: map[string]any{"id": "m1", "address": memberSrv}},
		},
		[]*config.Pool{
			{ID: "l0", OutboundIDs: []string{"o0"}, Selector: config.SelectorSpec{Type: "roundrobin"}},
			{ID: "l1", OutboundIDs: []string{"o1"}, Selector: config.SelectorSpec{Type: "roundrobin"}},
		},
		[]*config.ChainSpec{{ID: "c2", Layers: []string{"l0", "l1"}}},
		[]*config.Inbound{inboundOnChain("in", "c2")},
	)
	// The "testupstream" factory ignores cfg["address"], so give the hop the
	// server address its node must keep. It is read at dial time, when
	// hopNodeID names the node.
	unwrap(t, r, "o1").(*upstreamFakeOutbound).AddrVal = "203.0.113.53:1080"

	meta, rec := dpathObservedMeta("in", "127.0.0.1", portOf(t, echoAddr))
	conn, chain, err := r.Dial(context.Background(), meta)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()
	assert.Equal(t, []string{"o0", "o1"}, chain)

	// The route the collapsed hop rode really carries bytes to the target.
	msg := []byte("dpath-collapsed-upstream")
	_, err = conn.Write(msg)
	require.NoError(t, err)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	buf := make([]byte, len(msg))
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	assert.Equal(t, msg, buf)

	g := rec.Snapshot()
	require.NotNil(t, g)
	assert.False(t, g.Partial)
	assert.Equal(t,
		[]string{"local", "proxy:o0", "proxy:o1", "proxy:m1", "target"}, dpathNodeIDs(g))
	assert.Equal(t,
		[]string{
			"local->proxy:o0",
			"proxy:o0->proxy:o1",
			"local->proxy:m1",
			"proxy:m1->target",
			"local->proxy:o1",
			"proxy:o1->target",
		},
		dpathEdgePairs(g))

	// The route member's dial left the ENGINE HOST: attributing it to o1's
	// server would credit a proxy with a socket the engine's own stack opened.
	member := dpathEdge(t, g, "local", "proxy:m1")
	assert.Equal(t, core.LegDial, member.Kind)
	assert.Equal(t, echoAddr, member.Target)
	assert.True(t, member.Success)
	assert.Empty(t, dpathEdgeIfExists(g, "proxy:o1", "proxy:m1"),
		"o1's server dialed nothing: it was never reached in this shape")

	// Three legs left local: hop 0's dial, the route member's dial, and the
	// collapsed re-dial that threw hop 0's tunnel away.
	assert.Len(t, dpathEdgesFrom(g, "local"), 3)

	var collapsed []core.PathEdge
	for _, e := range g.Edges {
		if e.Collapsed {
			collapsed = append(collapsed, e)
		}
	}
	require.Len(t, collapsed, 1, "exactly one leg re-dialed from the engine host")
	assert.Equal(t, core.NodeLocalID, collapsed[0].From)
	assert.Equal(t, "proxy:o1", collapsed[0].To)
	assert.Equal(t, core.LegDial, collapsed[0].Kind)
	assert.True(t, collapsed[0].Success)
	assert.Greater(t, collapsed[0].Duration, time.Duration(0))

	// The collapsed leg's conn is the nested route's socket, so it adopts
	// nothing: o1 keeps the address config named and m1 keeps the socket it
	// really dialed. Only one vertex may claim that server.
	o1 := dpathNode(t, g, "proxy:o1")
	assert.Equal(t, "203.0.113.53:1080", o1.Address)
	assert.Equal(t, core.AddrConfigured, o1.AddressSource)
	m1 := dpathNode(t, g, "proxy:m1")
	assert.Equal(t, memberSrv, m1.Address)
	assert.Equal(t, core.AddrSocket, m1.AddressSource)
	claiming := 0
	for _, n := range g.Nodes {
		if n.Address == memberSrv {
			claiming++
		}
	}
	assert.Equal(t, 1, claiming, "only m1's node may claim m1's server address")
	assert.Equal(t, echoAddr, dpathNode(t, g, "target").Address)
	assert.Equal(t, core.AddrConfigured, dpathNode(t, g, "target").AddressSource)
}

// TestCollapsedHopUpstreamRouteKeepsDNSPhase pins the classification half of
// finding 6: the route member's name does not resolve. Its leg left the engine
// host, so classifyLeg keeps the genuine dns phase on it — a leg attributed to
// a proxy server instead (FromLocal false) would have that phase blanked, and
// the row would carry no phase at all for a resolver failure.
func TestCollapsedHopUpstreamRouteKeepsDNSPhase(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "o0", Protocol: "testchain", Config: map[string]any{"id": "o0", "address": "203.0.113.52:1080"}},
			{ID: "o1", Protocol: "testupstream", UpstreamProxy: "outbound:m1",
				Config: map[string]any{"id": "o1"}},
			// The member's name cannot resolve, so its dial fails in the
			// resolver — deterministically, without depending on a sandbox's DNS.
			{ID: "m1", Protocol: "testdirect",
				Config: map[string]any{"id": "m1", "address": "m1.invalid:1080"}},
		},
		[]*config.Pool{
			{ID: "l0", OutboundIDs: []string{"o0"}, Selector: config.SelectorSpec{Type: "roundrobin"}},
			{ID: "l1", OutboundIDs: []string{"o1"}, Selector: config.SelectorSpec{Type: "roundrobin"}},
		},
		[]*config.ChainSpec{{ID: "c2", Layers: []string{"l0", "l1"}}},
		[]*config.Inbound{inboundOnChain("in", "c2")},
	)
	unwrap(t, r, "o1").(*upstreamFakeOutbound).AddrVal = "203.0.113.53:1080"
	fakeOutbound(t, r, "m1").DialErr = dpathNXDomain("m1.invalid")

	meta, rec := dpathObservedMeta("in", "example.com", "443")
	_, chain, err := r.Dial(context.Background(), meta)
	require.Error(t, err)
	require.ErrorContains(t, err, "layer 1 dial o1")
	assert.Equal(t, []string{"o0", "o1"}, chain, "a failed hop is still reported as selected")

	g := rec.Snapshot()
	require.NotNil(t, g)
	assert.Equal(t,
		[]string{"local", "proxy:o0", "proxy:o1", "proxy:m1", "target"}, dpathNodeIDs(g))
	// The failed hop records its own leg and the member's, and nothing toward
	// the target: no handshake happened.
	assert.Equal(t,
		[]string{"local->proxy:o0", "proxy:o0->proxy:o1", "local->proxy:m1", "local->proxy:o1"},
		dpathEdgePairs(g))
	assert.Empty(t, dpathEdgesTo(g, "target"), "no leg may be invented toward the target")
	assert.Empty(t, dpathEdgeIfExists(g, "proxy:o1", "proxy:m1"))

	// The member's leg left local, so its DNS classification survives the
	// phase bound instead of being blanked.
	member := dpathEdge(t, g, "local", "proxy:m1")
	assert.False(t, member.Success)
	assert.Equal(t, core.NodeLocalID, member.From)
	assert.Equal(t, phaseDNS, member.Phase, "a leg that left the engine host ran the resolver")
	assert.Equal(t, reasonDNSNXDomain, member.Reason)

	// The collapsed leg carries the same failure up one level; it also left
	// local, so it keeps the phase too.
	collapsed := dpathEdge(t, g, "local", "proxy:o1")
	assert.True(t, collapsed.Collapsed)
	assert.False(t, collapsed.Success)
	assert.Equal(t, phaseDNS, collapsed.Phase)
	assert.Equal(t, reasonDNSNXDomain, collapsed.Reason)

	assert.Equal(t, "m1.invalid:1080", dpathNode(t, g, "proxy:m1").Address)
	assert.Equal(t, core.AddrConfigured, dpathNode(t, g, "proxy:m1").AddressSource,
		"the dial never produced a socket")
	assert.Equal(t, "203.0.113.53:1080", dpathNode(t, g, "proxy:o1").Address)
}

// dpathEdgeIfExists returns the edges from->to without failing when there are
// none, for "this leg must be absent" assertions.
func dpathEdgeIfExists(g *core.PathGraph, from, to string) []core.PathEdge {
	var out []core.PathEdge
	for _, e := range g.Edges {
		if e.From == from && e.To == to {
			out = append(out, e)
		}
	}
	return out
}

// --- pool failover ----------------------------------------------------------

// TestPoolFailoverRecordsEveryAttempt pins the per-attempt bookkeeping: a pool
// route that burns one member records one node AND one leg per attempt, so the
// failed member keeps its own address, error, phase and reason instead of
// being overwritten by the winner — and exactly one leg reaches the target.
func TestPoolFailoverRecordsEveryAttempt(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "m1", Protocol: "testdirect", Config: map[string]any{"id": "m1", "address": "203.0.113.11:1080"}},
			{ID: "m2", Protocol: "testdirect", Config: map[string]any{"id": "m2", "address": "203.0.113.12:1080"}},
		},
		[]*config.Pool{{ID: "pp", OutboundIDs: []string{"m1", "m2"}, Selector: config.SelectorSpec{Type: "roundrobin"}}},
		nil, nil,
	)
	fakeOutbound(t, r, "m1").DialErr = dpathRefused()

	ctx, rec := dpathRouteCtx("example.com", "443")
	d := r.UpstreamDialer(upstreamSpec("pool:pp"))
	require.NotNil(t, d)
	conn, err := d.DialUpstream(ctx, "tcp", "example.com:443")
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()

	g := rec.Snapshot()
	require.NotNil(t, g)
	assert.False(t, g.Partial)
	assert.Zero(t, g.Dropped)
	assert.Equal(t, []string{"local", "proxy:m1", "proxy:m2#2", "target"}, dpathNodeIDs(g),
		"one node per attempt, in dial order")
	assert.Equal(t, "local", g.Nodes[0].ID)
	assert.Equal(t, "target", g.Nodes[len(g.Nodes)-1].ID)

	// Attempt 1 keeps its own node: its own address, its own attempt number.
	loser := dpathNode(t, g, "proxy:m1")
	assert.Equal(t, core.NodeProxy, loser.Kind)
	assert.Equal(t, "m1", loser.OutboundID)
	assert.Equal(t, 1, loser.Attempt)
	assert.Equal(t, "203.0.113.11:1080", loser.Address)
	assert.Equal(t, core.AddrConfigured, loser.AddressSource)

	// Attempt 2 gets a numbered node so the two members cannot collide.
	winner := dpathNode(t, g, "proxy:m2#2")
	assert.Equal(t, "m2", winner.OutboundID)
	assert.Equal(t, 2, winner.Attempt)
	assert.Equal(t, "203.0.113.12:1080", winner.Address)

	// Three legs: the failed attempt, the winning attempt, and the winner's
	// arrival toward the destination.
	require.Len(t, g.Edges, 3)
	assert.Equal(t, []string{"local->proxy:m1", "local->proxy:m2#2", "proxy:m2#2->target"}, dpathEdgePairs(g))

	failed := g.Edges[0]
	assert.Equal(t, core.LegDial, failed.Kind)
	assert.False(t, failed.Success)
	assert.Contains(t, failed.Error, "connection refused")
	assert.Equal(t, reasonConnectRefused, failed.Reason, "the failed member keeps its own reason")
	assert.Equal(t, phaseConnect, failed.Phase)

	won := g.Edges[1]
	assert.Equal(t, core.LegDial, won.Kind)
	assert.True(t, won.Success)
	assert.Empty(t, won.Error)
	assert.Empty(t, won.Reason)
	assert.Greater(t, won.Duration, time.Duration(0))

	arrival := g.Edges[2]
	assert.Equal(t, core.LegTunnel, arrival.Kind)
	assert.True(t, arrival.Success, "the winner's arrival leg is a success")
	assert.True(t, arrival.Observed)
	assert.Equal(t, time.Duration(0), arrival.Duration, "the handshake rode inside the winning dial")
	assert.Equal(t, "example.com:443", arrival.Target)

	assert.Len(t, dpathEdgesTo(g, "target"), 1, "exactly one leg reaches the target")
}

// TestPoolFailoverAllMembersFailKeepsEveryError pins the total-failure shape:
// every attempt keeps its own distinct error and classification, no leg is
// invented toward the target, and the metric row's Reason is still what the
// generic classifier gives for the error dispatch returned (the per-leg
// classification does not leak into the row).
func TestPoolFailoverAllMembersFailKeepsEveryError(t *testing.T) {
	outerSrv := socks5Server(t) // never reached: the route dies first
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "outer", Protocol: "socks5", UpstreamProxy: "pool:pp",
				Config: map[string]any{"id": "outer", "address": outerSrv}},
			{ID: "m1", Protocol: "testdirect", Config: map[string]any{"id": "m1", "address": "203.0.113.21:1080"}},
			{ID: "m2", Protocol: "testdirect", Config: map[string]any{"id": "m2", "address": "203.0.113.22:1080"}},
		},
		[]*config.Pool{
			{ID: "pp", OutboundIDs: []string{"m1", "m2"}, Selector: config.SelectorSpec{Type: "roundrobin"}},
			{ID: "p1", OutboundIDs: []string{"outer"}, Selector: config.SelectorSpec{Type: "roundrobin"}},
		},
		[]*config.ChainSpec{{ID: "c1", Layers: []string{"p1"}}},
		[]*config.Inbound{inboundOnChain("in", "c1")},
	)
	fakeOutbound(t, r, "m1").DialErr = dpathRefused()
	boom := errors.New("dpath: member two exploded")
	fakeOutbound(t, r, "m2").DialErr = boom

	meta, rec := dpathObservedMeta("in", "example.com", "443")
	_, _, err := r.Dial(context.Background(), meta)
	require.Error(t, err)
	require.ErrorContains(t, err, "every member dial failed")

	g := rec.Snapshot()
	require.NotNil(t, g)
	// Ordered: the outer hop's node is named by dialLayers before the failing
	// route names its two attempts, and the target node is moved last by
	// Snapshot.
	assert.Equal(t,
		[]string{"local", "proxy:outer", "proxy:m1", "proxy:m2#2", "target"}, dpathNodeIDs(g))

	// Both attempts are there, each with its own address and its own error.
	assert.Equal(t, "203.0.113.21:1080", dpathNode(t, g, "proxy:m1").Address)
	assert.Equal(t, 1, dpathNode(t, g, "proxy:m1").Attempt)
	assert.Equal(t, "203.0.113.22:1080", dpathNode(t, g, "proxy:m2#2").Address)
	assert.Equal(t, 2, dpathNode(t, g, "proxy:m2#2").Attempt)

	require.Len(t, g.Edges, 3)
	assert.Equal(t,
		[]string{"local->proxy:m1", "local->proxy:m2#2", "local->proxy:outer"}, dpathEdgePairs(g))

	e1, e2 := g.Edges[0], g.Edges[1]
	assert.False(t, e1.Success)
	assert.False(t, e2.Success)
	assert.NotEqual(t, e1.Error, e2.Error, "each attempt keeps its own error")
	assert.Contains(t, e1.Error, "connection refused")
	assert.Equal(t, reasonConnectRefused, e1.Reason)
	assert.Equal(t, phaseConnect, e1.Phase)
	assert.Contains(t, e2.Error, "dpath: member two exploded")
	assert.Equal(t, reasonUnknown, e2.Reason)
	assert.Empty(t, e2.Phase)

	// The outer hop's own leg failed with the route's aggregate error.
	outerLeg := g.Edges[2]
	assert.False(t, outerLeg.Success)
	assert.Contains(t, outerLeg.Error, "every member dial failed")
	assert.Equal(t, reasonRouteUnavail, outerLeg.Reason)

	assert.Empty(t, dpathEdgesTo(g, "target"), "nothing reached the target, so no leg claims it did")

	row := dpathRow(t, r, func(m core.RequestMetric) bool { return !m.Success })
	assert.Equal(t, classifyErr(err), row.Reason, "the row's reason is still the generic classification")
	assert.Equal(t, reasonRouteUnavail, row.Reason)
	assert.Equal(t, g, row.Path)
}

// --- the by-value Metadata / ctx-less Bridge trap ---------------------------

// TestBridgeRowCarriesTheObservedPath is the end-to-end regression test for
// the trap this feature is built around: Metadata crosses Dial and Bridge BY
// VALUE and Bridge receives no context, so the only way the SUCCESS row can
// carry a graph is a recorder created at the seam that owns the request — the
// inbound listener — and handed to both calls. Drives a real HTTP CONNECT
// through inbound.Handler and reads the recorded row back out of the store.
func TestBridgeRowCarriesTheObservedPath(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{{ID: "ob1", Protocol: "testdirect",
			Config: map[string]any{"id": "ob1", "address": "203.0.113.31:1080"}}},
		[]*config.Pool{{ID: "p1", OutboundIDs: []string{"ob1"}, Selector: config.SelectorSpec{Type: "roundrobin"}}},
		[]*config.ChainSpec{{ID: "c1", Layers: []string{"p1"}}},
		[]*config.Inbound{inboundOnChain("in", "c1")},
	)

	ln, err := inbound.New("in", "http", "127.0.0.1:0",
		inbound.NewAuthStore(nil), func(string) string { return "" }, r)
	require.NoError(t, err)
	defer ln.Close()

	c, err := net.Dial("tcp", ln.Address())
	require.NoError(t, err)
	defer c.Close()
	require.NoError(t, c.SetDeadline(time.Now().Add(10*time.Second)))

	_, err = c.Write([]byte("CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n"))
	require.NoError(t, err)
	const ack = "HTTP/1.1 200 Connection Established\r\n\r\n"
	got := make([]byte, len(ack))
	_, err = io.ReadFull(c, got)
	require.NoError(t, err)
	require.Equal(t, ack, string(got))

	// The fake upstream writes its greeting and closes, which tears the bridge
	// down; reading to EOF is the synchronization point (Bridge has recorded
	// the row by the time the listener closes this conn).
	rest, _ := io.ReadAll(c)
	assert.Contains(t, string(rest), "hello from ob1")

	row := dpathRow(t, r, func(m core.RequestMetric) bool {
		return m.InboundID == "in" && m.Target == "example.com:443"
	})
	assert.True(t, row.Success, "the bridge ended cleanly: %s", row.Error)
	assert.Equal(t, []string{"ob1"}, row.Chain)

	require.NotNil(t, row.Path, "the success row Bridge recorded must carry the observed path")
	assert.Equal(t, []string{"local", "proxy:ob1", "target"}, dpathNodeIDs(row.Path))
	assert.Equal(t, []string{"local->proxy:ob1", "proxy:ob1->target"}, dpathEdgePairs(row.Path))
	assert.False(t, row.Path.Partial)

	target := dpathNode(t, row.Path, "target")
	assert.Equal(t, core.NodeTarget, target.Kind)
	assert.Equal(t, "example.com", target.Host)
	assert.Equal(t, "443", target.Port)
	assert.Equal(t, "example.com:443", target.Address)

	proxy := dpathNode(t, row.Path, "proxy:ob1")
	assert.Equal(t, "ob1", proxy.OutboundID)
	assert.Equal(t, "203.0.113.31:1080", proxy.Address)

	for _, e := range row.Path.Edges {
		assert.True(t, e.Success, "leg %s->%s should have succeeded", e.From, e.To)
		assert.True(t, e.Observed)
	}
	assert.Greater(t, row.Path.Edges[0].Duration, time.Duration(0))
}

// --- classification ---------------------------------------------------------

// TestClassifyLegBoundsPhasesToTheLeg pins classifyLeg's contract: the REASON
// always survives (it comes from typed and substring rules alone), but the
// PHASE is bounded to what the leg could actually have reached — a resolver on
// a leg that never left the engine host, TLS on a leg that never ended at the
// target, a proxy handshake on a bare stack dial to the target. "request" and
// "connect" are leg-independent and survive everywhere.
//
// (phaseHeaders and phaseBody cannot arise here at all: classifyProbeFailure
// only attributes them from a stage or a trace, and a leg has neither.)
func TestClassifyLegBoundsPhasesToTheLeg(t *testing.T) {
	var (
		dnsFailure = &net.DNSError{Err: "server misbehaving", Name: "example.com"}
		dnsTimeout = &net.DNSError{Err: "timeout", Name: "example.com", IsTimeout: true}
		certErr    = x509.UnknownAuthorityError{}
		recordErr  = tls.RecordHeaderError{Msg: "remote error: handshake failure"}
		resolver   = errors.New("gvisor: address api.ipify.org:443 is not an IP literal (resolver not configured)")
		routeGone  = errors.New(`router: upstream_proxy outbound "nope": not configured`)
		refused    = dpathRefused()
		emptyPool  = errors.New("router: layer 0 empty pool")
		proxyAuth  = errors.New("socks5 outbound: no acceptable auth method")
	)

	// The leg shapes a producer can report.
	const (
		fromLocal = true
		notLocal  = false
		toTarget  = true
		notTarget = false
	)
	localDial := core.LegInfo{Kind: core.LegDial, FromLocal: fromLocal, ToTarget: notTarget}
	localStackToTarget := core.LegInfo{Kind: core.LegStack, FromLocal: fromLocal, ToTarget: toTarget}
	remoteDial := core.LegInfo{Kind: core.LegDial, FromLocal: notLocal, ToTarget: notTarget}
	remoteTunnel := core.LegInfo{Kind: core.LegTunnel, FromLocal: notLocal, ToTarget: notTarget}
	remoteTunnelToTarget := core.LegInfo{Kind: core.LegTunnel, FromLocal: notLocal, ToTarget: toTarget}

	cases := []struct {
		name       string
		info       core.LegInfo
		err        error
		wantPhase  string
		wantReason string
	}{
		// DNS: only a leg that left the engine host ran a resolver.
		{"dns from local", localDial, dnsFailure, phaseDNS, reasonDNSFailure},
		{"dns from local keeps timeout reason", localDial, dnsTimeout, phaseDNS, reasonDNSTimeout},
		{"dns on a leg that did not leave the host", remoteDial, dnsFailure, "", reasonDNSFailure},
		{"dns timeout on a tunnel leg", remoteTunnel, dnsTimeout, "", reasonDNSTimeout},
		{"resolver not configured on a tunnel leg", remoteTunnel, resolver, "", reasonDNSFailure},
		{"resolver not configured from local", localDial, resolver, phaseDNS, reasonDNSFailure},

		// TLS: only a leg that ended at the target handshook with it.
		{"tls cert to the target", remoteTunnelToTarget, certErr, phaseTLS, reasonTLSCert},
		{"tls cert on a leg not ending at the target", remoteTunnel, certErr, "", reasonTLSCert},
		{"tls record error on a leg from local", localDial, recordErr, "", reasonTLSHandshake},
		{"tls record error to the target", localStackToTarget, recordErr, phaseTLS, reasonTLSHandshake},

		// Proxy handshake: a bare stack dial to the target speaks no proxy protocol.
		{"proxy auth on a stack leg to the target", localStackToTarget, proxyAuth, "", reasonProxyAuth},
		{"proxy auth on a tunnel leg to the target", remoteTunnelToTarget, proxyAuth, phaseProxy, reasonProxyAuth},
		{"proxy auth on a dial leg from local", localDial, proxyAuth, phaseProxy, reasonProxyAuth},

		// request and connect are leg-independent: they survive everywhere.
		{"route invalid on a tunnel leg", remoteTunnel, routeGone, phaseRequest, reasonRouteInvalid},
		{"route invalid on a stack leg to the target", localStackToTarget, routeGone, phaseRequest, reasonRouteInvalid},
		{"route invalid from local", localDial, routeGone, phaseRequest, reasonRouteInvalid},
		{"refused from local", localDial, refused, phaseConnect, reasonConnectRefused},
		{"refused on a tunnel leg", remoteTunnel, refused, phaseConnect, reasonConnectRefused},
		{"refused on a stack leg to the target", localStackToTarget, refused, phaseConnect, reasonConnectRefused},
		{"empty pool on a tunnel leg", remoteTunnel, emptyPool, phaseConnect, reasonRouteUnavail},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			phase, reason := classifyLeg(tc.info, tc.err)
			assert.Equal(t, tc.wantPhase, phase, "phase must be bounded to what the leg could reach")
			assert.Equal(t, tc.wantReason, reason, "the reason always survives bounding")
		})
	}

	t.Run("no error classifies to nothing", func(t *testing.T) {
		phase, reason := classifyLeg(localDial, nil)
		assert.Empty(t, phase)
		assert.Empty(t, reason)
	})
}

// --- the via note is unchanged ----------------------------------------------

// TestDialViaAndDialAddrUnchanged pins that adding the path graph did not
// disturb the probe trace's via note on the upstream-route side: DialVia still
// names the member that served the dial (last attempt wins on a failover, and
// on a total failure the member tried last), and DialAddr still falls back to
// that member's server address when no connect hook fired.
func TestDialViaAndDialAddrUnchanged(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "bad1", Protocol: "testdirect", Config: map[string]any{"id": "bad1", "address": "203.0.113.81:1080"}},
			{ID: "bad2", Protocol: "testdirect", Config: map[string]any{"id": "bad2", "address": "203.0.113.82:1080"}},
			{ID: "good", Protocol: "testdirect", Config: map[string]any{"id": "good", "address": "203.0.113.83:1080"}},
			{ID: "solo", Protocol: "testdirect", Config: map[string]any{"id": "solo", "address": "203.0.113.84:1080"}},
		},
		[]*config.Pool{
			{ID: "mix", OutboundIDs: []string{"bad1", "good"}, Selector: config.SelectorSpec{Type: "roundrobin"}},
			{ID: "dead", OutboundIDs: []string{"bad1", "bad2"}, Selector: config.SelectorSpec{Type: "roundrobin"}},
			{ID: "single", OutboundIDs: []string{"solo"}, Selector: config.SelectorSpec{Type: "roundrobin"}},
		},
		[]*config.ChainSpec{{ID: "cc", Layers: []string{"single"}}},
		nil,
	)
	fakeOutbound(t, r, "bad1").DialErr = dpathRefused()
	fakeOutbound(t, r, "bad2").DialErr = errors.New("dpath: bad2 exploded")

	cases := []struct {
		name     string
		route    string
		wantVia  string
		wantAddr string
		wantErr  string
	}{
		{
			name:     "pinned outbound names itself",
			route:    "outbound:good",
			wantVia:  "good",
			wantAddr: "203.0.113.83:1080",
		},
		{
			name:     "pool failover is last-wins",
			route:    "pool:mix",
			wantVia:  "good",
			wantAddr: "203.0.113.83:1080",
		},
		{
			name:     "pool total failure names the member tried last",
			route:    "pool:dead",
			wantVia:  "bad2",
			wantAddr: "203.0.113.82:1080",
			wantErr:  "every member dial failed",
		},
		{
			name:     "chain route names its first hop",
			route:    "chain:cc",
			wantVia:  "solo",
			wantAddr: "203.0.113.84:1080",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := newProbeTrace(time.Now())
			// A recorder rides along too: the via note and the path graph are
			// independent observations of the same dial.
			rec := core.NewPathRecorder()
			rec.SetClassifier(classifyLeg)
			rec.AddLocal()
			ctx := core.WithPathRecorder(withProbeTrace(context.Background(), tr), rec)

			d := r.UpstreamDialer(upstreamSpec(tc.route))
			require.NotNil(t, d)
			conn, err := d.DialUpstream(ctx, "tcp", "example.com:443")
			if tc.wantErr == "" {
				require.NoError(t, err)
				require.NotNil(t, conn)
				defer conn.Close()
			} else {
				require.ErrorContains(t, err, tc.wantErr)
			}

			p := tr.snapshot()
			assert.Equal(t, tc.wantVia, p.DialVia, "the via note names the member that served the dial")
			// No net.Dialer connect hooks fire for a fake outbound, so the
			// picked member's server address is the fallback DialAddr.
			assert.Equal(t, tc.wantAddr, p.DialAddr)
		})
	}
}

// --- the Dial fallback recorder ---------------------------------------------

// TestDispatchPathRecorderCreatedWhenAbsent pins Router.Dial's fallback
// branch: a caller that does not observe (no recorder on its Metadata) still
// gets a graph on the failure row, because Dial creates the recorder itself.
// The caller's copy of Metadata keeps Path nil — it is passed by value — which
// is exactly why the graph is only observable on the row.
func TestDispatchPathRecorderCreatedWhenAbsent(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{{ID: "ob1", Protocol: "testdirect",
			Config: map[string]any{"id": "ob1", "address": "203.0.113.91:1080"}}},
		[]*config.Pool{{ID: "p1", OutboundIDs: []string{"ob1"}, Selector: config.SelectorSpec{Type: "roundrobin"}}},
		[]*config.ChainSpec{{ID: "c1", Layers: []string{"p1"}}},
		[]*config.Inbound{inboundOnChain("in", "c1")},
	)
	fakeOutbound(t, r, "ob1").DialErr = dpathRefused()

	meta := metaFor("in", "example.com", "443")
	require.Nil(t, meta.Path, "this caller does not observe")
	_, _, err := r.Dial(context.Background(), meta)
	require.Error(t, err)
	assert.Nil(t, meta.Path, "Metadata is by value: Dial's recorder never reaches the caller's copy")

	row := dpathRow(t, r, func(m core.RequestMetric) bool { return !m.Success })
	require.NotNil(t, row.Path, "the fallback recorder still produced a graph for the failure row")
	assert.Equal(t, []string{"local", "proxy:ob1", "target"}, dpathNodeIDs(row.Path))
	assert.Equal(t, []string{"local->proxy:ob1"}, dpathEdgePairs(row.Path))

	failed := row.Path.Edges[0]
	assert.False(t, failed.Success)
	assert.Equal(t, reasonConnectRefused, failed.Reason)
	assert.Equal(t, phaseConnect, failed.Phase)
	assert.Equal(t, reasonConnectRefused, row.Reason)
	assert.Empty(t, dpathEdgesTo(row.Path, "target"))
}

// TestSharedAddressPoolFailoverArrivalLandsOnWinner is the router-level shape
// behind the recorder's address-index rule. Two pool members name ONE server
// address (same physical server, different credentials or different routes in
// front of it): m1 rides a dead relay and fails, m2 rides a working relay and
// wins. noteUpstreamArrival resolves the winning attempt's arrival through
// AddServer(shared address), so the vertex that owns the address index decides
// where that leg lands. It must be the attempt the engine is now talking to —
// the winner — and the outer hop's own bracket must then recognise its arrival
// as the same physical leg and suppress it. When the first registrant (the
// loser) kept the index instead, the success edge was drawn into the FAILED
// attempt and a fabricated direct local dial of the winner appeared beside it:
// the graph claimed the engine held a socket to a server it only reached
// through the relay, and the loser looked reached.
func TestSharedAddressPoolFailoverArrivalLandsOnWinner(t *testing.T) {
	shared := socks5Server(t) // both members' server: one physical address
	pxSrv := socks5Server(t)  // the working relay m2 rides
	dead := unboundLoopbackAddr(t)
	es := startEcho(t)
	echoAddr := es.ln.Addr().String()

	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "m1", Protocol: "socks5", UpstreamProxy: "outbound:dead",
				Config: map[string]any{"id": "m1", "address": shared}},
			{ID: "m2", Protocol: "socks5", UpstreamProxy: "outbound:px",
				Config: map[string]any{"id": "m2", "address": shared}},
			{ID: "dead", Protocol: "socks5", Config: map[string]any{"id": "dead", "address": dead}},
			{ID: "px", Protocol: "socks5", Config: map[string]any{"id": "px", "address": pxSrv}},
		},
		[]*config.Pool{{ID: "pp", OutboundIDs: []string{"m1", "m2"}, Selector: config.SelectorSpec{Type: "roundrobin"}}},
		nil, nil,
	)

	ctx, rec := dpathRouteCtx("127.0.0.1", portOf(t, echoAddr))
	d := r.UpstreamDialer(upstreamSpec("pool:pp"))
	require.NotNil(t, d)
	conn, err := d.DialUpstream(ctx, "tcp", echoAddr)
	require.NoError(t, err, "m2 rides the working relay, so the pool must succeed")
	require.NotNil(t, conn)
	defer conn.Close()

	g := rec.Snapshot()
	require.NotNil(t, g)
	assert.False(t, g.Partial)
	assert.Zero(t, g.Dropped)

	// Both attempts keep their own vertex: the shared address does not merge
	// them, and the loser keeps its own attempt number.
	assert.Equal(t,
		[]string{"local", "proxy:m1", "proxy:dead", "proxy:m2#2", "proxy:px", "target"}, dpathNodeIDs(g))
	assert.Equal(t, 1, dpathNode(t, g, "proxy:m1").Attempt)
	assert.Equal(t, shared, dpathNode(t, g, "proxy:m1").Address)
	assert.Equal(t, 2, dpathNode(t, g, "proxy:m2#2").Attempt)
	assert.Equal(t, shared, dpathNode(t, g, "proxy:m2#2").Address)

	// The loser's own legs stay failed and belong to it: the dead relay it rode,
	// and its own server dial that died with it.
	assert.Equal(t, []string{"local->proxy:dead", "local->proxy:m1"}, dpathEdgePairs(g)[:2])
	for _, id := range []string{"proxy:dead", "proxy:m1"} {
		e := dpathEdge(t, g, "local", id)
		assert.False(t, e.Success, "the failed attempt's leg to %s stays failed", id)
		assert.Equal(t, phaseConnect, e.Phase)
		assert.Equal(t, reasonConnectRefused, e.Reason)
	}

	// The winner rode the relay, so the arrival leg starts at the relay and
	// lands on the WINNER's vertex — not on the loser that registered the
	// shared address first.
	won := dpathEdge(t, g, "proxy:px", "proxy:m2#2")
	assert.Equal(t, core.LegTunnel, won.Kind)
	assert.True(t, won.Success)
	assert.Equal(t, shared, won.Target)
	// The only leg arriving at the loser is its own failed one: the winner's
	// success is not drawn into the attempt that failed.
	for _, e := range dpathEdgesTo(g, "proxy:m1") {
		assert.False(t, e.Success, "a success edge into the loser is the winner's arrival misresolved: %+v", e)
	}

	// No fabricated direct dial of the winner: the outer hop's bracket
	// recognised its arrival as the relay's leg and suppressed it.
	assert.Empty(t, dpathEdgeIfExists(g, "local", "proxy:m2#2"),
		"the winner was reached through the relay, so local holds no socket to it")

	// The winner's handshake toward the destination, and exactly one leg there.
	arrival := dpathEdge(t, g, "proxy:m2#2", "target")
	assert.Equal(t, core.LegTunnel, arrival.Kind)
	assert.True(t, arrival.Success)
	assert.Len(t, dpathEdgesTo(g, "target"), 1)
	assert.Len(t, dpathEdgesFrom(g, "local"), 3,
		"local really dialed the dead relay, m1's server and the working relay — nothing more")
}

// TestSharedAddressChainHopArrivalLandsOnDialedHop is finding 1's shape: a
// chain whose two hops are DIFFERENT outbounds pointing at the SAME proxy-server
// address, and whose first hop reaches that server through an upstream route.
// dialLayers names hop i+1 before hop i dials, so last-owner-wins on the
// address index hands the shared address to the SUCCESSOR while the
// predecessor's nested route is still in flight. The route's arrival was then
// drawn into the hop nobody had dialed yet, the predecessor's own bracket
// failed to recognise that arrival as the same physical leg, and a direct
// local → hop-0 dial appeared beside it: the graph claimed the engine held a
// socket to a server it only reached through the relay, and the successor
// looked reached before the predecessor did.
func TestSharedAddressChainHopArrivalLandsOnDialedHop(t *testing.T) {
	shared := socks5Server(t)   // BOTH hops' proxy server: one physical address
	relaySrv := socks5Server(t) // the relay hop 0's server dial rides
	es := startEcho(t)
	echoAddr := es.ln.Addr().String()

	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "h0", Protocol: "socks5", UpstreamProxy: "outbound:relay",
				Config: map[string]any{"id": "h0", "address": shared}},
			{ID: "h1", Protocol: "socks5",
				Config: map[string]any{"id": "h1", "address": shared}},
			{ID: "relay", Protocol: "socks5", Config: map[string]any{"id": "relay", "address": relaySrv}},
		},
		[]*config.Pool{
			{ID: "l0", OutboundIDs: []string{"h0"}, Selector: config.SelectorSpec{Type: "roundrobin"}},
			{ID: "l1", OutboundIDs: []string{"h1"}, Selector: config.SelectorSpec{Type: "roundrobin"}},
		},
		[]*config.ChainSpec{{ID: "c2", Layers: []string{"l0", "l1"}}},
		[]*config.Inbound{inboundOnChain("in", "c2")},
	)

	meta, rec := dpathObservedMeta("in", "127.0.0.1", portOf(t, echoAddr))
	conn, chain, err := r.Dial(context.Background(), meta)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()
	assert.Equal(t, []string{"h0", "h1"}, chain)

	// The whole path really carries bytes: engine → relay → shared server
	// (hop 0's ride) → the same server's handshake for hop 1 → echo.
	msg := []byte("dpath-shared-chain-hop")
	_, err = conn.Write(msg)
	require.NoError(t, err)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	buf := make([]byte, len(msg))
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	assert.Equal(t, msg, buf)

	g := rec.Snapshot()
	require.NotNil(t, g)
	assert.False(t, g.Partial)
	assert.Zero(t, g.Dropped)

	// Both hops keep their own vertex — two outbounds really did ride one
	// server — and the relay the first hop's dial rode is named after them:
	// dialLayers names both hops before anything dials.
	assert.Equal(t,
		[]string{"local", "proxy:h0", "proxy:h1", "proxy:relay", "target"}, dpathNodeIDs(g))
	assert.Equal(t, shared, dpathNode(t, g, "proxy:h0").Address)
	assert.Equal(t, shared, dpathNode(t, g, "proxy:h1").Address)

	// The relay is the ONE server the engine dialed off its own stack, so it is
	// the one vertex whose address is socket-verified.
	relay := dpathNode(t, g, "proxy:relay")
	assert.Equal(t, relaySrv, relay.Address)
	assert.Equal(t, core.AddrSocket, relay.AddressSource)

	// The invariant that matters: one leg per physical hop, in dial order, with
	// the nested route's arrival landing on the hop whose dial was in flight.
	assert.Equal(t, []string{
		"local->proxy:relay",
		"proxy:relay->proxy:h0",
		"proxy:h0->proxy:h1",
		"proxy:h1->target",
	}, dpathEdgePairs(g))

	// No fabricated direct dial of hop 0: it was reached through the relay, so
	// the engine host holds no socket to it.
	assert.Empty(t, dpathEdgeIfExists(g, "local", "proxy:h0"),
		"hop 0 was reached through the relay, so local holds no socket to it")
	assert.Len(t, dpathEdgesFrom(g, "local"), 1,
		"exactly one leg left the engine host: the relay dial")

	// The arrival leg belongs to the hop the relay was dialing, and it is real
	// but not separately timed (it happened inside the relay's own bracket).
	arrival := dpathEdge(t, g, "proxy:relay", "proxy:h0")
	assert.Equal(t, core.LegTunnel, arrival.Kind)
	assert.Equal(t, shared, arrival.Target)
	assert.True(t, arrival.Observed)
	assert.True(t, arrival.Success)
	assert.Equal(t, time.Duration(0), arrival.Duration)

	// The successor is reached over the shared server's own handshake, not by a
	// second leg out of the relay: the relay never dialed hop 1.
	assert.Empty(t, dpathEdgeIfExists(g, "proxy:relay", "proxy:h1"),
		"the relay's tunnel ended at hop 0's server; hop 1 rode hop 0's handshake")
	for _, e := range dpathEdgesTo(g, "proxy:h1") {
		assert.Equal(t, "proxy:h0", e.From,
			"only the hop that tunneled toward it may arrive at hop 1: %+v", e)
	}
	assert.Len(t, dpathEdgesTo(g, "target"), 1)
}

// TestAddServerPrefersInflightBracket pins the in-flight preference on the
// AddServer side of the resolver (ServerNodeForArrival has its own). A hop whose
// upstream_proxy is "direct" reaches its server through upstream.go's direct
// branch, which calls AddServer(target) INSIDE the hop's own open bracket — and
// dialLayers named the successor hop at that same address before this hop
// dialed, so the last-owner address index points at a hop nobody has reached
// yet. The bracket says who the engine is really talking to: resolving through
// it keeps the stack leg on the hop in flight and lets the hop's own bracket
// recognise the arrival as its own. Resolving through the index instead
// misattributes the stack leg to the successor AND leaves the hop's bracket
// un-suppressed, so a second, fabricated local dial of the hop appears beside
// it: two legs out of the engine host for one physical dial.
//
// This pins AddServer consulting the open-bracket stack before the address
// index — not the mid-bracket rename rewrite, which core's
// TestRenameMidBracketKeepsInflightClaim pins: nothing renames a node here,
// both hops register under their own names before the dial.
func TestAddServerPrefersInflightBracket(t *testing.T) {
	shared := socks5Server(t) // BOTH hops' proxy server: one physical address
	es := startEcho(t)
	echoAddr := es.ln.Addr().String()

	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			// m1's own server dial rides the "direct" route, so it is resolved by
			// AddServer inside m1's open bracket.
			{ID: "m1", Protocol: "socks5", UpstreamProxy: "direct",
				Config: map[string]any{"id": "m1", "address": shared}},
			{ID: "m2", Protocol: "socks5",
				Config: map[string]any{"id": "m2", "address": shared}},
		},
		[]*config.Pool{
			{ID: "l0", OutboundIDs: []string{"m1"}, Selector: config.SelectorSpec{Type: "roundrobin"}},
			{ID: "l1", OutboundIDs: []string{"m2"}, Selector: config.SelectorSpec{Type: "roundrobin"}},
		},
		[]*config.ChainSpec{{ID: "c2", Layers: []string{"l0", "l1"}}},
		[]*config.Inbound{inboundOnChain("in", "c2")},
	)

	meta, rec := dpathObservedMeta("in", "127.0.0.1", portOf(t, echoAddr))
	conn, chain, err := r.Dial(context.Background(), meta)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()
	assert.Equal(t, []string{"m1", "m2"}, chain)

	// The whole path really carries bytes: engine → the shared server (m1's
	// direct ride) → the same server's self-connect for m1 → m2's handshake → echo.
	msg := []byte("dpath-addserver-inflight")
	_, err = conn.Write(msg)
	require.NoError(t, err)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	buf := make([]byte, len(msg))
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	assert.Equal(t, msg, buf)

	g := rec.Snapshot()
	require.NotNil(t, g)
	assert.False(t, g.Partial)
	assert.Zero(t, g.Dropped)

	// Both hops keep their own vertex: one address, two outbound identities.
	assert.Equal(t, []string{"local", "proxy:m1", "proxy:m2", "target"}, dpathNodeIDs(g))

	// One leg per physical hop, in dial order. The stack leg belongs to the hop
	// whose bracket was open, not to the successor the chain named first.
	assert.Equal(t, []string{
		"local->proxy:m1",
		"proxy:m1->proxy:m2",
		"proxy:m2->target",
	}, dpathEdgePairs(g))

	// The engine host really dialed once: m1's own server, through the direct
	// route. The successor was reached over that server's handshake, so no leg of
	// its own left local.
	assert.Len(t, dpathEdgesFrom(g, "local"), 1,
		"exactly one leg left the engine host: the direct route's stack dial")
	assert.Empty(t, dpathEdgeIfExists(g, "local", "proxy:m2"),
		"m2 was reached over the shared server's handshake, not by a dial of its own")

	ride := dpathEdge(t, g, "local", "proxy:m1")
	assert.Equal(t, core.LegStack, ride.Kind)
	assert.Equal(t, shared, ride.Target)
	assert.True(t, ride.Observed)
	assert.True(t, ride.Success)

	// The hop's own bracket recognised the nested arrival as the same physical
	// leg and rewrote itself into the handshake toward the successor.
	arrival := dpathEdge(t, g, "proxy:m1", "proxy:m2")
	assert.Equal(t, core.LegTunnel, arrival.Kind)
	assert.Equal(t, shared, arrival.Target)
	assert.True(t, arrival.Observed)
	assert.True(t, arrival.Success)
	for _, e := range dpathEdgesTo(g, "proxy:m2") {
		assert.Equal(t, "proxy:m1", e.From,
			"only the hop that tunneled toward it may arrive at m2: %+v", e)
	}
	assert.Len(t, dpathEdgesTo(g, "target"), 1)

	// The one engine socket to that address belongs to the hop whose leg opened
	// it; the successor keeps the address config named.
	assert.Equal(t, core.AddrSocket, dpathNode(t, g, "proxy:m1").AddressSource)
	assert.Equal(t, shared, dpathNode(t, g, "proxy:m1").Address)
	assert.Equal(t, core.AddrConfigured, dpathNode(t, g, "proxy:m2").AddressSource)
	assert.Equal(t, shared, dpathNode(t, g, "proxy:m2").Address)
}

// TestSharedAddressHostnameRideKeepsOneVertex is finding 2's shape: a hop and
// the route member it rides are configured with the SAME hostname-spelled
// server address, while the socket the member dial really produced is the
// resolved one. Under last-owner-wins the member owns the address index, so
// adopting its socket peer deleted that index entry — and the arrival, resolved
// by address alone, then had nothing to unify with: it minted an orphan
// "server:localhost:P" vertex for a proxy the graph already had a name for, and
// the outer hop's bracket missed its suppression and fabricated a direct local
// dial beside it.
func TestSharedAddressHostnameRideKeepsOneVertex(t *testing.T) {
	srv := socks5Server(t) // listens on 127.0.0.1:P; both outbounds spell it localhost:P
	shared := net.JoinHostPort("localhost", portOf(t, srv))
	es := startEcho(t)
	echoAddr := es.ln.Addr().String()

	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "m1", Protocol: "socks5", UpstreamProxy: "outbound:px",
				Config: map[string]any{"id": "m1", "address": shared}},
			{ID: "px", Protocol: "socks5", Config: map[string]any{"id": "px", "address": shared}},
		},
		[]*config.Pool{{ID: "p1", OutboundIDs: []string{"m1"}, Selector: config.SelectorSpec{Type: "roundrobin"}}},
		[]*config.ChainSpec{{ID: "c1", Layers: []string{"p1"}}},
		[]*config.Inbound{inboundOnChain("in", "c1")},
	)

	meta, rec := dpathObservedMeta("in", "127.0.0.1", portOf(t, echoAddr))
	conn, chain, err := r.Dial(context.Background(), meta)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()
	assert.Equal(t, []string{"m1"}, chain)

	msg := []byte("dpath-shared-hostname-ride")
	_, err = conn.Write(msg)
	require.NoError(t, err)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	buf := make([]byte, len(msg))
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	assert.Equal(t, msg, buf)

	g := rec.Snapshot()
	require.NotNil(t, g)
	assert.False(t, g.Partial)
	assert.Zero(t, g.Dropped)

	// One vertex per outbound that really rode the server, and no bare
	// "server:localhost:P" orphan beside them.
	assert.Equal(t, []string{"local", "proxy:m1", "proxy:px", "target"}, dpathNodeIDs(g))
	for _, n := range g.Nodes {
		assert.False(t, strings.HasPrefix(n.ID, core.NodeServerPrefix),
			"the shared address already had a named vertex; no server node may be minted for it")
	}

	assert.Equal(t, []string{
		"local->proxy:px",
		"proxy:px->proxy:m1",
		"proxy:m1->target",
	}, dpathEdgePairs(g))
	assert.Empty(t, dpathEdgeIfExists(g, "local", "proxy:m1"),
		"m1's server was reached through px, so local holds no socket to it")
	assert.Len(t, dpathEdgesFrom(g, "local"), 1)
	assert.Len(t, dpathEdgesTo(g, "target"), 1)

	// The member's leg is the one that opened an engine socket, so it carries
	// the resolved peer; the hop that rode it keeps the address config named,
	// because no leg of its own ever reached that server from local.
	member := dpathNode(t, g, "proxy:px")
	assert.Equal(t, srv, member.Address)
	assert.Equal(t, core.AddrSocket, member.AddressSource)
	outer := dpathNode(t, g, "proxy:m1")
	assert.Equal(t, shared, outer.Address)
	assert.Equal(t, core.AddrConfigured, outer.AddressSource)
}

// TestSharedAddressIPRideKeepsArrivalEdge is the same ride with the shared
// address spelled the way the socket reports it, so the address index still
// resolves it — to the member, which is the arrival's own source. Resolving by
// address alone then dropped the arrival edge entirely (a vertex arriving at
// itself is no leg), leaving the outer hop's bracket un-suppressed and a
// fabricated direct local dial in its place. Excluding the arrival's source
// from the resolution is what keeps the leg.
func TestSharedAddressIPRideKeepsArrivalEdge(t *testing.T) {
	shared := socks5Server(t) // one physical server, spelled as the socket spells it
	es := startEcho(t)
	echoAddr := es.ln.Addr().String()

	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "m1", Protocol: "socks5", UpstreamProxy: "outbound:px",
				Config: map[string]any{"id": "m1", "address": shared}},
			{ID: "px", Protocol: "socks5", Config: map[string]any{"id": "px", "address": shared}},
		},
		[]*config.Pool{{ID: "p1", OutboundIDs: []string{"m1"}, Selector: config.SelectorSpec{Type: "roundrobin"}}},
		[]*config.ChainSpec{{ID: "c1", Layers: []string{"p1"}}},
		[]*config.Inbound{inboundOnChain("in", "c1")},
	)

	meta, rec := dpathObservedMeta("in", "127.0.0.1", portOf(t, echoAddr))
	conn, chain, err := r.Dial(context.Background(), meta)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()
	assert.Equal(t, []string{"m1"}, chain)

	msg := []byte("dpath-shared-ip-ride")
	_, err = conn.Write(msg)
	require.NoError(t, err)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	buf := make([]byte, len(msg))
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	assert.Equal(t, msg, buf)

	g := rec.Snapshot()
	require.NotNil(t, g)
	assert.False(t, g.Partial)
	assert.Zero(t, g.Dropped)

	assert.Equal(t, []string{"local", "proxy:m1", "proxy:px", "target"}, dpathNodeIDs(g))
	assert.Equal(t, []string{
		"local->proxy:px",
		"proxy:px->proxy:m1",
		"proxy:m1->target",
	}, dpathEdgePairs(g))

	// The arrival edge survives even though the member shares the address: it
	// is the leg from the member to the hop that asked for the ride.
	arrival := dpathEdge(t, g, "proxy:px", "proxy:m1")
	assert.Equal(t, core.LegTunnel, arrival.Kind)
	assert.Equal(t, shared, arrival.Target)
	assert.True(t, arrival.Observed)
	assert.True(t, arrival.Success)
	assert.Equal(t, time.Duration(0), arrival.Duration)

	assert.Empty(t, dpathEdgeIfExists(g, "local", "proxy:m1"),
		"m1's server was reached through px, so local holds no socket to it")
	assert.Len(t, dpathEdgesFrom(g, "local"), 1)
	assert.Len(t, dpathEdgesTo(g, "target"), 1)
}

// TestSharedAddressRideKeepsHopsOwnHandshakeLeg is the dispatch-side shape of
// the mid-bracket rename: a chain whose first hop reaches its own server
// through an upstream route, and whose SUCCESSOR is a direct hop configured at
// the route member's address. dialLayers names the successor before the first
// hop dials, and a direct hop has no node of its own, so what it names is a
// bare server:<addr> vertex for that shared address. The nested route then
// registers its member at the same address, and addProxyLocked's upgrade branch
// adopts the bare vertex: server:X is renamed to proxy:px WHILE the outer hop's
// bracket is open.
//
// The producer's leg strings were captured before fn ran, so the outer bracket
// still held the pre-rename spelling of its Through. renameLocked rewrites the
// recorder's own state but cannot reach a caller's locals, and the leg being
// recorded is not in the edge list yet, so nothing else could resolve it: the
// byID guard saw an unknown node, dropped the hop's own handshake leg and set
// Partial for the wrong reason. The graph then claimed the engine reached the
// hop's server through the route and stopped there — the hop's real, timed,
// successful handshake toward the shared address was missing.
func TestSharedAddressRideKeepsHopsOwnHandshakeLeg(t *testing.T) {
	sA := socks5Server(t) // hop A's own proxy server
	shared := socks5Server(t)
	es := startEcho(t)
	echoAddr := es.ln.Addr().String()

	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "A", Protocol: "socks5", UpstreamProxy: "outbound:px",
				Config: map[string]any{"id": "A", "address": sA}},
			// The route member and the successor share one physical address,
			// which is what forces the bare server vertex to be adopted.
			{ID: "px", Protocol: "socks5", Config: map[string]any{"id": "px", "address": shared}},
			{ID: "D", Protocol: "direct", Config: map[string]any{"id": "D", "address": shared}},
		},
		[]*config.Pool{
			{ID: "pA", OutboundIDs: []string{"A"}, Selector: config.SelectorSpec{Type: "roundrobin"}},
			{ID: "pD", OutboundIDs: []string{"D"}, Selector: config.SelectorSpec{Type: "roundrobin"}},
		},
		[]*config.ChainSpec{{ID: "c1", Layers: []string{"pA", "pD"}}},
		[]*config.Inbound{inboundOnChain("in", "c1")},
	)

	meta, rec := dpathObservedMeta("in", "127.0.0.1", portOf(t, echoAddr))
	conn, chain, err := r.Dial(context.Background(), meta)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()
	assert.Equal(t, []string{"A", "D"}, chain, "the graph changes no dispatch decision")

	// No payload round-trip here on purpose: the last hop is a direct hop, so it
	// performs no handshake of its own and hands back the tunnel hop A opened
	// toward `shared`. The leg this test pins is hop A's own handshake, which
	// the dial above really performed.

	g := rec.Snapshot()
	require.NotNil(t, g)
	assert.Zero(t, g.Dropped)
	// Partial is set by the topology, not by the leg under test: a direct hop
	// WITH an address cannot be covered by the previous hop's tunnel edge, so
	// dialOutbound says the graph may be incomplete (see
	// TestDispatchPathUncoveredDirectHopMarksPartial). It is true with and
	// without the fix, which is why the assertions below pin the restored edge
	// and the node set instead.
	assert.True(t, g.Partial)

	// The bare vertex the successor was named by got adopted by the route
	// member, so the shared address has exactly one vertex and it is the
	// member's.
	assert.Equal(t, []string{"local", "proxy:A", "proxy:px", "target"}, dpathNodeIDs(g))
	for _, n := range g.Nodes {
		assert.False(t, strings.HasPrefix(n.ID, core.NodeServerPrefix),
			"the shared address was adopted by the route member; no bare server node may survive")
	}
	px := dpathNode(t, g, "proxy:px")
	assert.Equal(t, shared, px.Address)
	assert.Equal(t, core.AddrSocket, px.AddressSource,
		"the member is the one server the engine dialed off its own stack")

	assert.Equal(t, []string{
		"local->proxy:px",
		"proxy:px->proxy:A",
		"proxy:A->proxy:px",
	}, dpathEdgePairs(g))
	assert.Empty(t, dpathEdgeIfExists(g, "local", "proxy:A"),
		"A's server was reached through px, so local holds no socket to it")
	assert.Len(t, dpathEdgesFrom(g, "local"), 1,
		"exactly one leg left the engine host: the route member dial")

	// The invariant this test exists for: hop A's own handshake toward the
	// shared address survived the rename of the node it was aimed at. It is
	// timed because the bracket that covered it is the hop's own dial.
	handshake := dpathEdge(t, g, "proxy:A", "proxy:px")
	assert.Equal(t, core.LegTunnel, handshake.Kind)
	assert.Equal(t, shared, handshake.Target)
	assert.True(t, handshake.Observed)
	assert.True(t, handshake.Success)
	assert.Greater(t, handshake.Duration, time.Duration(0))

	// The nested route's arrival at the hop that asked for the ride is real but
	// not separately timed: it happened inside the member's own bracket.
	arrival := dpathEdge(t, g, "proxy:px", "proxy:A")
	assert.Equal(t, core.LegTunnel, arrival.Kind)
	assert.Equal(t, sA, arrival.Target)
	assert.True(t, arrival.Success)
	assert.Equal(t, time.Duration(0), arrival.Duration)

	// The target is isolated: no hop ever handshaked toward the request's own
	// destination, and no leg may be invented for it.
	assert.Empty(t, dpathEdgesTo(g, "target"))
}

// TestEmptyIDHopSharedAddressRideKeepsOneLegFromLocal is the dispatch-side pin
// for what the in-flight bracket publishes when the ID it captured was already
// dead at push time.
//
// An outbound can carry an EMPTY ID in production, not only in this test:
// cmd/goose's loadConfig hands every config-file outbound to
// config.Store.SetOutbound with no ID validation, provider updates forward
// provider-supplied IDs unchanged (internal/provider/manager.go), and the
// router rebuild checks no ID either — only the HTTP API
// (internal/api/api.go) rejects "". That reachability is why this router-level
// pin matters: this is a dispatch shape a plain config file can produce.
// hopNodeID then has no identity to name, and AddProxy("", addr) falls
// through to a bare "server:<addr>" vertex for that hop. dialLayers names hop i
// and hop i+1 in the SAME iteration, so when the two hops share one address the
// successor's AddProxy adopts the bare vertex and renames it to proxy:h1 BEFORE
// dialOutbound opens its bracket — and the bracket pushes legs.hop, the
// pre-rename spelling renameLocked can no longer match and inflightForAddrLocked
// can no longer look up.
//
// With the claim dead, every arrival resolved inside that bracket silently falls
// back to the byAddr last-owner index — the exact failure the in-flight
// preference exists to prevent — and the nested route's arrival is dropped as a
// self-arrival. The outer hop's own bracket is then left un-suppressed and draws
// a SECOND leg out of the engine host: a chain holding one engine socket
// published as two local dials, claiming the engine reached h1 directly when it
// rode m1.
//
// The control phase names hop 0: nothing renames a dead ID there, so the graph
// is identical with and without the fix — the divergence belongs to the empty ID
// alone.
func TestEmptyIDHopSharedAddressRideKeepsOneLegFromLocal(t *testing.T) {
	shared := socks5Server(t) // one physical server: both hops and the route member
	es := startEcho(t)
	echoAddr := es.ln.Addr().String()

	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			// Hop 0 of the divergent chain, with the empty ID a config file can
			// carry. Its server dial rides m1, which is the same physical server.
			{ID: "", Protocol: "socks5", UpstreamProxy: "outbound:m1",
				Config: map[string]any{"id": "", "address": shared}},
			// The control's hop 0: identical in every respect but the ID.
			{ID: "h0", Protocol: "socks5", UpstreamProxy: "outbound:m1",
				Config: map[string]any{"id": "h0", "address": shared}},
			{ID: "m1", Protocol: "socks5", Config: map[string]any{"id": "m1", "address": shared}},
			{ID: "h1", Protocol: "socks5", Config: map[string]any{"id": "h1", "address": shared}},
		},
		[]*config.Pool{
			{ID: "lEmpty", OutboundIDs: []string{""}, Selector: config.SelectorSpec{Type: "roundrobin"}},
			{ID: "lNamed", OutboundIDs: []string{"h0"}, Selector: config.SelectorSpec{Type: "roundrobin"}},
			{ID: "l1", OutboundIDs: []string{"h1"}, Selector: config.SelectorSpec{Type: "roundrobin"}},
		},
		[]*config.ChainSpec{
			{ID: "cEmpty", Layers: []string{"lEmpty", "l1"}},
			{ID: "cNamed", Layers: []string{"lNamed", "l1"}},
		},
		[]*config.Inbound{inboundOnChain("inEmpty", "cEmpty"), inboundOnChain("inNamed", "cNamed")},
	)

	// dial drives one chain end to end and returns its graph.
	dial := func(t *testing.T, inboundID string, wantChain []string) *core.PathGraph {
		t.Helper()
		meta, rec := dpathObservedMeta(inboundID, "127.0.0.1", portOf(t, echoAddr))
		conn, chain, err := r.Dial(context.Background(), meta)
		require.NoError(t, err)
		require.NotNil(t, conn)
		defer conn.Close()
		assert.Equal(t, wantChain, chain, "the graph changes no dispatch decision")

		// The path really carries bytes: engine → shared (m1's socket) → the same
		// server's tunnel for hop 0 → hop 1's handshake → echo.
		msg := []byte("dpath-empty-id-ride")
		_, err = conn.Write(msg)
		require.NoError(t, err)
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
		buf := make([]byte, len(msg))
		_, err = io.ReadFull(conn, buf)
		require.NoError(t, err)
		assert.Equal(t, msg, buf)

		g := rec.Snapshot()
		require.NotNil(t, g)
		assert.Zero(t, g.Dropped)
		return g
	}

	// --- the control: hop 0 named ------------------------------------------
	// Nothing renames a dead ID when the hop has an identity of its own, so the
	// graph reads as the honest chain and is complete.
	control := dial(t, "inNamed", []string{"h0", "h1"})
	assert.False(t, control.Partial)
	assert.Equal(t, []string{"local", "proxy:h0", "proxy:h1", "proxy:m1", "target"}, dpathNodeIDs(control))
	assert.Equal(t, []string{
		"local->proxy:m1",
		"proxy:m1->proxy:h0",
		"proxy:h0->proxy:h1",
		"proxy:h1->target",
	}, dpathEdgePairs(control))
	assert.Len(t, dpathEdgesFrom(control, "local"), 1,
		"one engine socket, one local dial — with or without the fix")
	assert.Empty(t, dpathEdgeIfExists(control, "local", "proxy:h0"),
		"hop 0 was reached through the route member, so local holds no socket to it")

	// --- the empty-ID hop ---------------------------------------------------
	g := dial(t, "inEmpty", []string{"", "h1"})

	// Exactly ONE leg left the engine host — the route member's dial, which is
	// the only socket this chain holds.
	assert.Len(t, dpathEdgesFrom(g, "local"), 1,
		"the chain holds one engine socket, so it may publish one local dial")
	assert.Empty(t, dpathEdgeIfExists(g, "local", "proxy:h1"),
		"h1 was reached through the route member, so local holds no socket to it")

	assert.Equal(t, []string{"local", "proxy:h1", "proxy:m1", "target"}, dpathNodeIDs(g))
	assert.Equal(t, []string{
		"local->proxy:m1",
		"proxy:m1->proxy:h1",
		"proxy:h1->target",
	}, dpathEdgePairs(g))

	// The hop whose identity the config never named is a vertex the graph cannot
	// place a leg on: its own bracket resolved onto the successor it shares an
	// address with and found the path already standing there, so the leg it would
	// have drawn is a self-loop. Nothing honest can be recorded for that gap, so
	// the graph admits it.
	assert.True(t, g.Partial,
		"the empty-ID hop's own leg has no vertex to land on; the gap is admitted, not papered over")

	member := dpathNode(t, g, "proxy:m1")
	assert.Equal(t, shared, member.Address)
	assert.Equal(t, core.AddrSocket, member.AddressSource,
		"the route member is the one server the engine dialed off its own stack")
	h1 := dpathNode(t, g, "proxy:h1")
	assert.Equal(t, shared, h1.Address)
	assert.Equal(t, core.AddrConfigured, h1.AddressSource,
		"no leg of its own ever reached h1 from local, so config's address stands")

	// The nested route's arrival at the hop that asked for the ride survived the
	// dead in-flight claim: it is real, but it happened inside the member's own
	// bracket and is not separately timed.
	arrival := dpathEdge(t, g, "proxy:m1", "proxy:h1")
	assert.Equal(t, core.LegTunnel, arrival.Kind)
	assert.Equal(t, shared, arrival.Target)
	assert.True(t, arrival.Observed)
	assert.True(t, arrival.Success)
	assert.Equal(t, time.Duration(0), arrival.Duration)

	last := dpathEdge(t, g, "proxy:h1", "target")
	assert.Equal(t, core.LegTunnel, last.Kind)
	assert.Equal(t, echoAddr, last.Target)
	assert.True(t, last.Success)
	assert.Len(t, dpathEdgesTo(g, "target"), 1)
}
