// Path observation on the probe paths: the reachability dial probe, the HTTP
// probe round and the exit-IP check. Each records a different amount of the
// network path, and each one's silence is deliberate:
//
//   - a reachability dial probe measures the engine reaching one proxy server,
//     so its graph is two nodes and one leg — no target node, because the row's
//     Target field already names that same server;
//   - an HTTP probe measures a whole request, so its graph is the route's:
//     local, every proxy server the route really rode, and the target, with the
//     failed leg named and classified when it fails;
//   - an exit-IP check records no graph at all: core.ExitCheck has no Path
//     field and CheckDial is not instrumented.
//
// The probe row and the metric row beside it carry ONE graph — Snapshot is
// memoized — which is only observable as pointer identity at the assembly seam:
// the metrics store JSON round-trips every row it persists.
package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/core"
	"github.com/goose-network/goose/internal/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ppathNodeIDs lists a graph's node IDs in path order.
func ppathNodeIDs(g *core.PathGraph) []string {
	if g == nil {
		return nil
	}
	out := make([]string, 0, len(g.Nodes))
	for _, n := range g.Nodes {
		out = append(out, n.ID)
	}
	return out
}

// ppathNode returns the node with id, failing the test when the graph has none.
func ppathNode(t *testing.T, g *core.PathGraph, id string) core.PathNode {
	t.Helper()
	require.NotNil(t, g, "expected a path graph")
	for _, n := range g.Nodes {
		if n.ID == id {
			return n
		}
	}
	require.FailNowf(t, "no such path node", "graph holds %v, want %q", ppathNodeIDs(g), id)
	return core.PathNode{}
}

// ppathEdgesFrom returns every edge leaving from, in recorded order.
func ppathEdgesFrom(g *core.PathGraph, from string) []core.PathEdge {
	if g == nil {
		return nil
	}
	var out []core.PathEdge
	for _, e := range g.Edges {
		if e.From == from {
			out = append(out, e)
		}
	}
	return out
}

// ppathEdge returns the single edge from→to, failing when the graph does not
// hold exactly one.
func ppathEdge(t *testing.T, g *core.PathGraph, from, to string) core.PathEdge {
	t.Helper()
	require.NotNil(t, g, "expected a path graph")
	var found []core.PathEdge
	for _, e := range g.Edges {
		if e.From == from && e.To == to {
			found = append(found, e)
		}
	}
	require.Lenf(t, found, 1, "want exactly one edge %s→%s, graph edges are %+v", from, to, g.Edges)
	return found[0]
}

// ppathHostPort splits addr, failing the test when it is not a host:port.
func ppathHostPort(t *testing.T, addr string) (string, string) {
	t.Helper()
	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err, "address %q", addr)
	return host, port
}

// ppathRedirectTarget starts two servers on 127.0.0.1 and returns the URL of a
// probe against the first, whose /start answers 302 toward the second's /hop2.
// Following it forces the transport to dial twice — it keeps no pooled conn
// (DisableKeepAlives) — to a different port of the same host, so the second
// hop's legs cannot be mistaken for a continuation of the first's.
func ppathRedirectTarget(t *testing.T) (probeURL, firstAddr, secondAddr string) {
	t.Helper()
	hop2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		fmt.Fprintf(w, "hop2 %s", req.URL.Path)
	}))
	t.Cleanup(hop2.Close)
	hop1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		http.Redirect(w, req, hop2.URL+"/hop2", http.StatusFound)
	}))
	t.Cleanup(hop1.Close)

	u1, err := url.Parse(hop1.URL)
	require.NoError(t, err)
	u2, err := url.Parse(hop2.URL)
	require.NoError(t, err)
	return hop1.URL + "/start", u1.Host, u2.Host
}

// ppathSelfRedirectTarget starts one server whose /start answers 302 toward its
// own /hop2, and returns the probe URL and the server's host:port. Both hops
// dial one address, which is the shape that makes a second hop's leg look like
// a continuation of the first.
func ppathSelfRedirectTarget(t *testing.T) (probeURL, addr string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/start" {
			http.Redirect(w, req, "/hop2", http.StatusFound)
			return
		}
		fmt.Fprintf(w, "hop2 %s", req.URL.Path)
	}))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	return srv.URL + "/start", u.Host
}

// ppathRunProbe runs one (target, route) HTTP probe round and returns the
// single result.
func ppathRunProbe(t *testing.T, r *Router, target, route string, timeout time.Duration) core.HTTPProbe {
	t.Helper()
	res, err := r.RunHTTPProbes(context.Background(), core.HTTPProbeSpec{
		Targets: []string{target},
		Routes:  []string{route},
		Timeout: timeout,
	})
	require.NoError(t, err)
	require.Len(t, res, 1)
	return res[0]
}

// ppathLocalRow waits for the durable metric row a reachability dial probe
// recorded for outbound id (chain [core.HopLocal, id]).
func ppathLocalRow(t *testing.T, ms *metrics.Store, id string) core.RequestMetric {
	t.Helper()
	var row core.RequestMetric
	require.Eventually(t, func() bool {
		rows, err := ms.Recent(100)
		require.NoError(t, err)
		for _, m := range rows {
			if len(m.Chain) == 2 && m.Chain[0] == core.HopLocal && m.Chain[1] == id {
				row = m
				return true
			}
		}
		return false
	}, 10*time.Second, 50*time.Millisecond, "reachability metric row for outbound %q never landed", id)
	return row
}

// ppathProbeRows waits for n HTTP-probe metric rows (chain [core.HopProbe,
// route]) and returns them. The metrics writer is asynchronous, so the count
// is polled rather than read once.
func ppathProbeRows(t *testing.T, ms *metrics.Store, n int) []core.RequestMetric {
	t.Helper()
	var out []core.RequestMetric
	require.Eventually(t, func() bool {
		rows, err := ms.Recent(200)
		require.NoError(t, err)
		out = out[:0]
		for _, m := range rows {
			if len(m.Chain) == 2 && m.Chain[0] == core.HopProbe {
				out = append(out, m)
			}
		}
		return len(out) >= n
	}, 10*time.Second, 50*time.Millisecond, "only %d of %d probe metric rows landed", len(out), n)
	return out
}

// TestReachabilityProbePathHasNoTargetNode pins the reachability dial probe's
// graph: the engine host and the proxy server it dialed, joined by the one
// stack leg the probe bracketed. There is no target node — the row's Target
// field is that same server address, and a second vertex for it would read as
// the proxy dialing itself. The row's Chain is unchanged by the feature.
func TestReachabilityProbePathHasNoTargetNode(t *testing.T) {
	r, ms := newProbeOnlyRouter(t) // dial sweep disabled: the row is ours alone
	out, ok := r.lookupOutbound("work")
	require.True(t, ok, "outbound work not built")

	r.probeOne(context.Background(), 5*time.Second, out)

	row := ppathLocalRow(t, ms, "work")
	assert.Equal(t, []string{core.HopLocal, "work"}, row.Chain, "the feature adds a graph, not a hop")
	assert.True(t, row.Success, row.Error)

	g := row.Path
	require.NotNil(t, g, "a reachability probe observes its own dial")
	assert.Equal(t, []string{core.NodeLocalID, core.NodeProxyPrefix + "work"}, ppathNodeIDs(g))
	for _, n := range g.Nodes {
		assert.NotEqual(t, core.NodeTarget, n.Kind, "the row's Target field already names the server")
	}
	assert.False(t, g.Partial)
	assert.Zero(t, g.Dropped)

	local := ppathNode(t, g, core.NodeLocalID)
	assert.Equal(t, core.NodeLocal, local.Kind)
	assert.Empty(t, local.Address, "the engine host has no dial address")

	proxy := ppathNode(t, g, core.NodeProxyPrefix+"work")
	assert.Equal(t, core.NodeProxy, proxy.Kind)
	assert.Equal(t, "work", proxy.OutboundID)
	assert.Equal(t, out.Address(), proxy.Address, "the probe dialed the configured server address")
	assert.Equal(t, core.AddrSocket, proxy.AddressSource, "the address was taken off the conn the dial produced")
	assert.Zero(t, proxy.Attempt, "a dial probe makes one attempt")

	require.Len(t, g.Edges, 1)
	e := g.Edges[0]
	assert.Equal(t, core.NodeLocalID, e.From)
	assert.Equal(t, core.NodeProxyPrefix+"work", e.To)
	assert.Equal(t, core.LegStack, e.Kind, "a bare engine-stack dial, no proxy protocol")
	assert.Equal(t, row.Target, e.Target, "the leg dialed the row's target")
	assert.True(t, e.Observed)
	assert.True(t, e.Success)
	assert.Empty(t, e.Error)
	assert.Empty(t, e.Phase)
	assert.Empty(t, e.Reason)
}

// TestHTTPProbeDirectRoutePath: a direct route's graph is the engine host and
// the target joined by the one stack leg dialUpstream's direct branch brackets
// — no proxy node, because "direct" is the absence of a proxy rather than one.
func TestHTTPProbeDirectRoutePath(t *testing.T) {
	r, _ := newProbeOnlyRouter(t)
	target := probeTarget(t)
	u, err := url.Parse(target.URL)
	require.NoError(t, err)

	p := ppathRunProbe(t, r, target.URL+"/ip", "direct", 10*time.Second)
	require.True(t, p.Success, p.Error)

	g := p.Path
	require.NotNil(t, g)
	assert.Equal(t, []string{core.NodeLocalID, core.NodeTargetID}, ppathNodeIDs(g))
	for _, n := range g.Nodes {
		assert.NotEqual(t, core.NodeProxy, n.Kind, "a direct route rides no proxy server")
	}

	tn := ppathNode(t, g, core.NodeTargetID)
	assert.Equal(t, core.NodeTarget, tn.Kind)
	assert.Equal(t, u.Host, tn.Address)
	assert.Equal(t, core.AddrSocket, tn.AddressSource, "the stack dial produced the conn the address came off")
	host, port := ppathHostPort(t, u.Host)
	assert.Equal(t, host, tn.Host)
	assert.Equal(t, port, tn.Port)

	require.Len(t, g.Edges, 1)
	e := g.Edges[0]
	assert.Equal(t, core.NodeLocalID, e.From)
	assert.Equal(t, core.NodeTargetID, e.To)
	assert.Equal(t, core.LegStack, e.Kind)
	assert.Equal(t, u.Host, e.Target, "the leg dialed the target itself")
	assert.True(t, e.Observed)
	assert.True(t, e.Success)
	assert.Empty(t, e.Error)
	assert.Greater(t, e.Duration, time.Duration(0), "the one leg is the dial the transport timed")

	assert.Equal(t, u.Host, p.DialAddr, "the connect hook and the graph saw the same socket")
	assert.Empty(t, p.DialVia, "a direct route picks no outbound")
}

// TestHTTPProbeOutboundRoutePath: a pinned outbound's graph names the proxy
// server the route really rode, with the address taken off the socket, and the
// two physical legs one bracketed dial call covered — the timed local→proxy
// dial and the untimed proxy→target tunnel. The row's DialAddr and DialVia must
// agree with that graph rather than tell their own story.
func TestHTTPProbeOutboundRoutePath(t *testing.T) {
	f := newReachFixture(t, false)
	target := probeTarget(t)
	u, err := url.Parse(target.URL)
	require.NoError(t, err)

	p := ppathRunProbe(t, f.router, target.URL+"/ip", "outbound:relay", 10*time.Second)
	require.True(t, p.Success, p.Error)

	g := p.Path
	require.NotNil(t, g)
	proxyID := core.NodeProxyPrefix + "relay"
	assert.Equal(t, []string{core.NodeLocalID, proxyID, core.NodeTargetID}, ppathNodeIDs(g))

	proxy := ppathNode(t, g, proxyID)
	assert.Equal(t, core.NodeProxy, proxy.Kind)
	assert.Equal(t, "relay", proxy.OutboundID)
	assert.Equal(t, f.rAddr, proxy.Address, "the relay server the engine connected to")
	assert.Equal(t, core.AddrSocket, proxy.AddressSource)

	tn := ppathNode(t, g, core.NodeTargetID)
	assert.Equal(t, u.Hostname(), tn.Host)
	assert.Equal(t, portOf(t, u.Host), tn.Port)
	assert.Equal(t, core.AddrConfigured, tn.AddressSource, "the target was reached through the tunnel, never off a socket of its own")

	dial := ppathEdge(t, g, core.NodeLocalID, proxyID)
	assert.Equal(t, core.LegDial, dial.Kind)
	assert.True(t, dial.Success)
	assert.Greater(t, dial.Duration, time.Duration(0), "the local→proxy dial is separately timed")

	tun := ppathEdge(t, g, proxyID, core.NodeTargetID)
	assert.Equal(t, core.LegTunnel, tun.Kind, "the SOCKS5 handshake toward the target rode the same call")
	assert.True(t, tun.Observed, "an untimed leg is still an observed one")
	assert.True(t, tun.Success)
	assert.Zero(t, tun.Duration, "the bracket timed the dial, not the handshake")

	assert.Equal(t, proxy.Address, p.DialAddr, "the row's connect address is the graph's proxy node")
	assert.Equal(t, proxy.OutboundID, p.DialVia, "the row's chosen outbound is the graph's proxy node")
}

// TestHTTPProbePoolRoutePath: a pool route's graph names the member the
// selector picked — the same member DialVia reports — as attempt 1, because a
// first-try success is the first attempt.
func TestHTTPProbePoolRoutePath(t *testing.T) {
	f := newReachFixture(t, false)
	target := probeTarget(t)

	p := ppathRunProbe(t, f.router, target.URL+"/ip", "pool:relays", 10*time.Second)
	require.True(t, p.Success, p.Error)
	require.Equal(t, "relay", p.DialVia, "the fixture's pool has one member")

	g := p.Path
	require.NotNil(t, g)
	proxyID := core.NodeProxyPrefix + "relay"
	assert.Equal(t, []string{core.NodeLocalID, proxyID, core.NodeTargetID}, ppathNodeIDs(g),
		"the first attempt won, so it keeps the unnumbered node ID")

	proxy := ppathNode(t, g, proxyID)
	assert.Equal(t, p.DialVia, proxy.OutboundID, "the picked member is the proxy node")
	assert.Equal(t, p.DialAddr, proxy.Address)
	assert.Equal(t, 1, proxy.Attempt, "one attempt was made")

	dial := ppathEdge(t, g, core.NodeLocalID, proxyID)
	assert.Equal(t, core.LegDial, dial.Kind)
	assert.True(t, dial.Success)
	tun := ppathEdge(t, g, proxyID, core.NodeTargetID)
	assert.Equal(t, core.LegTunnel, tun.Kind)
	assert.True(t, tun.Success)
}

// ppathAssertNoLegLeavesTarget fails when any leg of g leaves the target node.
// The target is the destination the request was pointed at: nothing the engine
// observed dials onward from it, so a leg leaving it is invented topology.
func ppathAssertNoLegLeavesTarget(t *testing.T, g *core.PathGraph) {
	t.Helper()
	require.NotNil(t, g)
	for _, e := range g.Edges {
		assert.NotEqual(t, core.NodeTargetID, e.From, "no leg leaves the target: %+v", g.Edges)
	}
}

// TestProbeRedirectHopsLeaveLocal: a probe that follows a redirect dials twice —
// the transport keeps no pooled conn — and BOTH dials leave the engine host. The
// second is the one that needs saying: by then the recorder has arrived
// somewhere, so a second hop recorded as a continuation of that arrival either
// draws a leg leaving the node it reached (the target dialing onward, which
// nothing observed) or is swallowed by the recorder's nested-upstream
// suppression (a leg that really happened, missing from the graph without
// Partial saying so). probeTransport's NoteLocalDial is what makes each hop its
// own departure.
//
// Both routes and both redirect shapes are covered. A hop to another port names
// a second server vertex; a hop back to the same address is the shape
// suppression eats, and the one where the difference is visible as a leg count.
func TestProbeRedirectHopsLeaveLocal(t *testing.T) {
	t.Run("direct route", func(t *testing.T) {
		r, _ := newProbeOnlyRouter(t)

		t.Run("hop to another port", func(t *testing.T) {
			probeURL, firstAddr, secondAddr := ppathRedirectTarget(t)
			p := ppathRunProbe(t, r, probeURL, "direct", 10*time.Second)
			require.True(t, p.Success, p.Error)
			require.Contains(t, p.Body, "hop2", "the client really followed the 302")

			g := p.Path
			require.NotNil(t, g)
			second := core.NodeServerPrefix + secondAddr
			assert.Equal(t, []string{core.NodeLocalID, second, core.NodeTargetID}, ppathNodeIDs(g),
				"the redirected address is a server the engine dialed, not the destination it aimed at")
			ppathAssertNoLegLeavesTarget(t, g)

			require.Len(t, g.Edges, 2, "one stack leg per transport dial")
			assert.Equal(t, core.NodeTargetID, g.Edges[0].To, "the first dial reached the target")
			assert.Equal(t, firstAddr, g.Edges[0].Target)
			assert.Equal(t, second, g.Edges[1].To, "the second dial reached the redirected address")
			assert.Equal(t, secondAddr, g.Edges[1].Target)
			for _, e := range g.Edges {
				assert.Equal(t, core.NodeLocalID, e.From, "both dials left the engine host")
				assert.Equal(t, core.LegStack, e.Kind)
				assert.True(t, e.Success, e.Error)
				assert.Greater(t, e.Duration, time.Duration(0), "each dial is separately timed")
			}
			assert.False(t, g.Partial)
			assert.Zero(t, g.Dropped)
		})

		t.Run("hop back to the same address", func(t *testing.T) {
			probeURL, addr := ppathSelfRedirectTarget(t)
			p := ppathRunProbe(t, r, probeURL, "direct", 10*time.Second)
			require.True(t, p.Success, p.Error)
			require.Contains(t, p.Body, "hop2", "the client really followed the 302")

			g := p.Path
			require.NotNil(t, g)
			assert.Equal(t, []string{core.NodeLocalID, core.NodeTargetID}, ppathNodeIDs(g),
				"one server, dialed twice")
			ppathAssertNoLegLeavesTarget(t, g)

			// The regression: the second dial re-reaches the node the first one
			// arrived at, which is exactly the shape the recorder suppresses as
			// a nested route re-arrival. It is not one — the transport opened a
			// new connection — so both legs are drawn.
			require.Len(t, g.Edges, 2, "the second hop is a dial of its own: %+v", g.Edges)
			for _, e := range g.Edges {
				assert.Equal(t, core.NodeLocalID, e.From)
				assert.Equal(t, core.NodeTargetID, e.To)
				assert.Equal(t, addr, e.Target)
				assert.Equal(t, core.LegStack, e.Kind)
				assert.True(t, e.Success, e.Error)
			}
			assert.False(t, g.Partial, "no leg was dropped as a duplicate arrival")
			assert.Zero(t, g.Dropped)
		})
	})

	t.Run("outbound route", func(t *testing.T) {
		f := newReachFixture(t, false)
		proxyID := core.NodeProxyPrefix + "relay"

		t.Run("hop to another port", func(t *testing.T) {
			probeURL, firstAddr, secondAddr := ppathRedirectTarget(t)
			p := ppathRunProbe(t, f.router, probeURL, "outbound:relay", 10*time.Second)
			require.True(t, p.Success, p.Error)
			require.Contains(t, p.Body, "hop2", "the client really followed the 302")
			assert.Equal(t, "relay", p.DialVia)

			g := p.Path
			require.NotNil(t, g)
			second := core.NodeServerPrefix + secondAddr
			assert.Equal(t, []string{core.NodeLocalID, proxyID, second, core.NodeTargetID}, ppathNodeIDs(g))
			ppathAssertNoLegLeavesTarget(t, g)

			require.Len(t, g.Edges, 4, "each hop is a timed local dial and the proxy's handshake: %+v", g.Edges)
			assert.Equal(t, firstAddr, g.Edges[0].Target, "the first hop rode the route toward the target")
			assert.Equal(t, core.NodeTargetID, g.Edges[1].To)
			assert.Equal(t, secondAddr, g.Edges[2].Target, "the second hop rode the route toward the redirect")
			assert.Equal(t, second, g.Edges[3].To)
			for i := 0; i < 2; i++ {
				dial, tunnel := g.Edges[2*i], g.Edges[2*i+1]
				assert.Equal(t, core.NodeLocalID, dial.From, "hop %d left the engine host again", i+1)
				assert.Equal(t, proxyID, dial.To)
				assert.Equal(t, core.LegDial, dial.Kind)
				assert.Greater(t, dial.Duration, time.Duration(0), "each route dial is separately timed")
				assert.Equal(t, proxyID, tunnel.From)
				assert.Equal(t, core.LegTunnel, tunnel.Kind)
				assert.Zero(t, tunnel.Duration, "the handshake rode the dial it was timed inside")
				assert.Equal(t, dial.Target, tunnel.Target, "one hop, one destination")
			}
			assert.False(t, g.Partial)
			assert.Zero(t, g.Dropped)
		})

		t.Run("hop back to the same address", func(t *testing.T) {
			probeURL, addr := ppathSelfRedirectTarget(t)
			p := ppathRunProbe(t, f.router, probeURL, "outbound:relay", 10*time.Second)
			require.True(t, p.Success, p.Error)
			require.Contains(t, p.Body, "hop2", "the client really followed the 302")

			g := p.Path
			require.NotNil(t, g)
			assert.Equal(t, []string{core.NodeLocalID, proxyID, core.NodeTargetID}, ppathNodeIDs(g),
				"one proxy and one destination, each reached twice")
			ppathAssertNoLegLeavesTarget(t, g)

			require.Len(t, g.Edges, 4, "both hops rode the route: %+v", g.Edges)
			assert.Len(t, ppathEdgesFrom(g, core.NodeLocalID), 2, "both route dials left the engine host")
			for _, e := range g.Edges {
				assert.Equal(t, addr, e.Target, "both hops asked the route for the same address")
				assert.True(t, e.Success, e.Error)
			}
			assert.False(t, g.Partial)
		})
	})
}

// TestProbeTargetHostPortNamesTheDialedAddress: the target node a probe seeds is
// only the destination if it is named the way the dial names it. net/http's
// canonicalAddr — the address the dialer is handed, and the one every leg
// reports — punycodes a non-ASCII host and defaults the port from the scheme, so
// the helper must do exactly that and nothing more: an all-ASCII host is left as
// the URL spelled it (net/http punycodes only a non-ASCII one, so lowercasing
// here would miss the unification for an uppercase target instead), and so is a
// host the conversion rejects, which net/http hands to the resolver as-is.
func TestProbeTargetHostPortNamesTheDialedAddress(t *testing.T) {
	cases := []struct{ name, raw, host, port string }{
		{"http defaults to 80", "http://example.com/ip", "example.com", "80"},
		{"https defaults to 443", "https://example.com/ip", "example.com", "443"},
		{"explicit port wins", "http://127.0.0.1:8080/ip", "127.0.0.1", "8080"},
		{"IPv6 literal", "http://[::1]:8080/x", "::1", "8080"},
		{"internationalized name is punycoded", "http://exämple.invalid/ip", "xn--exmple-cua.invalid", "80"},
		{"punycoded with an explicit port", "https://exämple.example:8443/", "xn--exmple-cua.example", "8443"},
		{"non-Latin name", "http://例え.jp/", "xn--r8jz45g.jp", "80"},
		{"ASCII is left as spelled", "http://EXAMPLE.COM/ip", "EXAMPLE.COM", "80"},
		{"ASCII with a disallowed rune", "http://ex_ample.invalid/x", "ex_ample.invalid", "80"},
		// net/http keeps the raw spelling when the conversion fails and lets the
		// resolver be the one to reject it, so the helper must too.
		{"conversion rejects it", "http://ex_ämple.invalid/x", "ex_ämple.invalid", "80"},
		{"URL that does not parse", "://bad", "", ""},
		{"URL with no host", "/ip", "", ""},
		{"not a URL", "not a url", "", ""},
		{"empty", "", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			host, port := probeTargetHostPort(tc.raw)
			assert.Equal(t, tc.host, host, tc.raw)
			assert.Equal(t, tc.port, port, tc.raw)
		})
	}
}

// TestProbeInternationalTargetIsOneVertex: an internationalized target is seeded
// punycoded, and that is what keeps the graph honest — the address net/http
// dials then resolves to the seeded target node instead of growing a second
// origin vertex beside a target no leg ever reaches.
func TestProbeInternationalTargetIsOneVertex(t *testing.T) {
	const target = "http://exämple.invalid:8080/ip"
	// net/http's canonicalAddr for that URL: the punycoded host, the URL's port.
	const dialed = "xn--exmple-cua.invalid:8080"

	host, port := probeTargetHostPort(target)
	require.Equal(t, dialed, net.JoinHostPort(host, port), "the helper names the address the dial reports")

	t.Run("the dialed address names the seeded target", func(t *testing.T) {
		// probeOneHTTP's seeding, then what dialUpstream does with the address
		// net/http handed it: AddServer for a direct route, TargetNodeIDFor for
		// the arrival leg of a routed one.
		rec := core.NewPathRecorder()
		rec.SetClassifier(classifyLeg)
		rec.AddLocal()
		require.Equal(t, core.NodeTargetID, rec.AddTarget(host, port))
		assert.Equal(t, core.NodeTargetID, rec.AddServer(dialed),
			"one vertex for one destination")
		assert.Equal(t, core.NodeTargetID, rec.TargetNodeIDFor(host, port),
			"and a routed hop's arrival finds the same one")

		g := rec.Snapshot()
		require.NotNil(t, g)
		assert.Equal(t, []string{core.NodeLocalID, core.NodeTargetID}, ppathNodeIDs(g))
		tn := ppathNode(t, g, core.NodeTargetID)
		assert.Equal(t, dialed, tn.Address, "the target is named as the dial names it")
		assert.Equal(t, core.AddrConfigured, tn.AddressSource, "nothing was reached, so no socket was adopted")
	})

	t.Run("a probe toward it names one destination", func(t *testing.T) {
		r, _ := newProbeOnlyRouter(t)
		// .invalid never resolves (RFC 6761), so the probe fails in DNS or is
		// cut short by its own timeout — either way the destination the route
		// dialed must be the node the probe seeded.
		p := ppathRunProbe(t, r, target, "direct", 5*time.Second)
		require.False(t, p.Success)

		g := p.Path
		require.NotNil(t, g)
		assert.Equal(t, []string{core.NodeLocalID, core.NodeTargetID}, ppathNodeIDs(g),
			"no duplicate origin vertex beside the target")
		ppathAssertNoLegLeavesTarget(t, g)
		tn := ppathNode(t, g, core.NodeTargetID)
		assert.Equal(t, dialed, tn.Address)
		assert.Equal(t, host, tn.Host)
		assert.Equal(t, port, tn.Port)
		for _, e := range g.Edges {
			assert.Equal(t, core.NodeTargetID, e.To, "the leg that was attempted reached the target")
			assert.Equal(t, dialed, e.Target)
		}
	})

	// The regression this prevents: seeding the URL's own Unicode spelling
	// leaves the target unaddressed by anything the dial reports, so AddServer
	// grows a duplicate vertex and the target is orphaned — two servers for one
	// destination, and a probe that never shows reaching either.
	t.Run("control: the Unicode spelling orphans the target", func(t *testing.T) {
		u, err := url.Parse(target)
		require.NoError(t, err)
		rec := core.NewPathRecorder()
		rec.SetClassifier(classifyLeg)
		rec.AddLocal()
		rec.AddTarget(u.Hostname(), port)
		assert.Equal(t, core.NodeServerPrefix+dialed, rec.AddServer(dialed),
			"the dialed address matches no vertex the seeding made")
		assert.Empty(t, rec.TargetNodeIDFor(host, port))

		g := rec.Snapshot()
		require.NotNil(t, g)
		assert.Equal(t, []string{
			core.NodeLocalID, core.NodeServerPrefix + dialed, core.NodeTargetID,
		}, ppathNodeIDs(g), "one destination, two vertices")
		assert.Equal(t, u.Hostname()+":"+port, ppathNode(t, g, core.NodeTargetID).Address,
			"and the target keeps a spelling no leg reported")
	})
}

// TestHTTPProbeFailurePathNamesTheBrokenLeg: a failed probe's graph says WHICH
// leg broke. The failed edge's To is the node the leg was reaching for — the
// proxy server when the local network or the handshake failed, the target when
// the target itself refused — and it carries the leg's own classification,
// bounded to what that leg could have reached.
func TestHTTPProbeFailurePathNamesTheBrokenLeg(t *testing.T) {
	f := newReachFixture(t, true) // the local network blocks the work server
	dead := unboundLoopbackAddr(t)
	f.cfg.SetOutbound(&config.OutboundSpec{ID: "dead", Protocol: "socks5", Config: map[string]any{"address": dead}})
	stalled := stalledTCPServer(t)
	f.cfg.SetOutbound(&config.OutboundSpec{ID: "stalled", Protocol: "socks5", Config: map[string]any{"address": stalled}})
	f.router.Rebuild()
	target := probeTarget(t)

	cases := []struct {
		name string
		// route and target of the probe.
		route  string
		target string
		// what the probe row says.
		rowPhase  string
		rowReason string
		// the node the failed leg was reaching for, and its own classification.
		brokenTo   string
		edgeKind   core.LegKind
		edgePhase  string
		edgeReason string
	}{
		{
			name: "target port refused on a direct route", route: "direct", target: "http://127.0.0.1:1/ip",
			rowPhase: phaseConnect, rowReason: reasonConnectRefused,
			brokenTo: core.NodeTargetID, edgeKind: core.LegStack,
			edgePhase: phaseConnect, edgeReason: reasonConnectRefused,
		},
		{
			name: "proxy server port refused", route: "outbound:dead", target: target.URL,
			rowPhase: phaseConnect, rowReason: reasonConnectRefused,
			brokenTo: core.NodeProxyPrefix + "dead", edgeKind: core.LegDial,
			edgePhase: phaseConnect, edgeReason: reasonConnectRefused,
		},
		{
			name: "local network blocks the proxy server", route: "outbound:work", target: target.URL,
			rowPhase: phaseConnect, rowReason: reasonUnknown,
			brokenTo: core.NodeProxyPrefix + "work", edgeKind: core.LegDial,
			// classifyLeg bounds the phase to what this leg could have
			// reached: the stack's refusal carries no typed or vocabulary
			// signature, so the leg keeps the reason and asserts no phase.
			edgePhase: "", edgeReason: reasonUnknown,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := ppathRunProbe(t, f.router, tc.target, tc.route, 5*time.Second)
			require.False(t, p.Success, "expected a failure, got status %d", p.Status)
			assert.Equal(t, tc.rowPhase, p.Phase, "error was %q", p.Error)
			assert.Equal(t, tc.rowReason, p.Reason, "error was %q", p.Error)

			g := p.Path
			require.NotNil(t, g)
			assert.Equal(t, core.NodeTargetID, g.Nodes[len(g.Nodes)-1].ID, "a failure still names the destination")

			var failed []core.PathEdge
			for _, e := range g.Edges {
				if !e.Success {
					failed = append(failed, e)
				}
			}
			require.Len(t, failed, 1, "exactly one leg broke: %+v", g.Edges)
			e := failed[0]
			assert.Equal(t, tc.brokenTo, e.To, "the failed edge names the leg that broke")
			assert.Equal(t, tc.edgeKind, e.Kind)
			assert.Equal(t, tc.edgePhase, e.Phase)
			assert.Equal(t, tc.edgeReason, e.Reason)
			assert.NotEmpty(t, e.Error, "the leg keeps the full diagnostic")
			assert.True(t, e.Observed)
		})
	}

	// A proxy that accepts TCP and never answers its handshake: the request
	// dies inside the bracketed dial, so the row attributes the failure to the
	// proxy leg by phase while the graph names the nodes the route reached.
	//
	// The graph records NO edge, and Partial stays false — both deterministic,
	// for the same reason. The SOCKS5 handshake reads on a 30s socket deadline
	// and ignores the request context, so the bracketed dial is still open when
	// the client's 500ms timeout returns the failure and Snapshot seals the
	// recorder. Observe's post-call block is the only place an edge is
	// appended, and it early-returns on a sealed recorder: the abandoned leg
	// never reached it, so the recorder never dropped a leg it knew about —
	// which is the only claim Partial makes. The nodes are all there because
	// they were registered before the dial started.
	t.Run("proxy handshake stalls", func(t *testing.T) {
		p := ppathRunProbe(t, f.router, target.URL, "outbound:stalled", 500*time.Millisecond)
		require.False(t, p.Success)
		assert.Equal(t, phaseProxy, p.Phase, "error was %q", p.Error)
		assert.Equal(t, reasonProxyTimeout, p.Reason, "error was %q", p.Error)

		g := p.Path
		require.NotNil(t, g)
		assert.Equal(t, []string{
			core.NodeLocalID, core.NodeProxyPrefix + "stalled", core.NodeTargetID,
		}, ppathNodeIDs(g), "the route named its proxy server before the dial")
		assert.Empty(t, g.Edges, "the abandoned dial never closed its bracket")
		assert.False(t, g.Partial, "no leg the recorder knew about went unrecorded")
		assert.Zero(t, g.Dropped)

		// The proxy node keeps the configured address: the leg that would have
		// adopted a socket peer is the one that never returned.
		proxy := ppathNode(t, g, core.NodeProxyPrefix+"stalled")
		assert.Equal(t, stalled, proxy.Address)
		assert.Equal(t, core.AddrConfigured, proxy.AddressSource)
	})

	// A route that is not configured fails before any leg exists: the graph
	// keeps the nodes the probe seeded and records nothing between them.
	t.Run("route not configured", func(t *testing.T) {
		p := ppathRunProbe(t, f.router, target.URL, "outbound:nope", 5*time.Second)
		require.False(t, p.Success)
		assert.Equal(t, phaseRequest, p.Phase, "error was %q", p.Error)
		assert.Equal(t, reasonRouteInvalid, p.Reason, "error was %q", p.Error)

		g := p.Path
		require.NotNil(t, g)
		assert.Equal(t, []string{core.NodeLocalID, core.NodeTargetID}, ppathNodeIDs(g))
		assert.Empty(t, g.Edges, "no leg was ever attempted")
	})
}

// TestUnissuedProbeJobHasNoPath: a job the round ended before it was issued is
// reported to the caller but has no path — nothing was dialed, so there is
// nothing to draw — and it leaves no row behind, so history never shows a
// request that never happened.
//
// Handing RunHTTPProbes an already-cancelled context makes every job's select
// see both cases ready, and Go picks uniformly among ready cases, so the round
// splits into issued and unissued jobs differently on every run — a slot freed
// by a job that failed instantly can be won by a job the round has already
// ended. The split is therefore NOT asserted: what holds for any split is that
// every job lands in exactly one of the two classes, that each class keeps its
// own guarantees, and that the durable rows account for the issued jobs alone.
func TestUnissuedProbeJobHasNoPath(t *testing.T) {
	r, ms := newProbeOnlyRouter(t)

	targets := make([]string, 0, core.MaxProbeTargets)
	for i := 0; i < core.MaxProbeTargets; i++ {
		targets = append(targets, fmt.Sprintf("http://127.0.0.1:1/ip?n=%d", i))
	}
	require.Len(t, targets, core.MaxProbeTargets)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the round is over before it starts
	res, err := r.RunHTTPProbes(ctx, core.HTTPProbeSpec{
		Targets: targets, Routes: []string{"direct"}, Timeout: 5 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, res, len(targets))

	issued := 0
	unissued := 0
	for _, p := range res {
		if p.Reason != reasonRoundBudget {
			issued++
			// A job that was issued has a graph even when the round died
			// immediately: the probe seeds local and the target up front.
			require.NotNil(t, p.Path, "target %s was issued", p.Target)
			assert.Equal(t, []string{core.NodeLocalID, core.NodeTargetID}, ppathNodeIDs(p.Path))
			assert.Equal(t, reasonCanceled, p.Reason, "error was %q", p.Error)
			continue
		}
		unissued++
		assert.Nil(t, p.Path, "a request that never left has no path")
		assert.False(t, p.Success)
		assert.Contains(t, p.Error, "round ended before the request was issued")
		assert.Empty(t, p.Phase, "no leg of the path was involved")
		assert.Empty(t, p.DialAddr)
		assert.Empty(t, p.DialVia)
		assert.Zero(t, p.RouteDial, "no dial was ever started")
		assert.Zero(t, p.Status, "no response arrived")
		assert.Equal(t, p.Route, "direct", "the queue casualty still names its route")
	}
	// Accounting, not a bound: one result per job, each classified once.
	assert.Equal(t, len(targets), issued+unissued)
	// Non-vacuity. Reaching zero would take every one of the 32 goroutines
	// winning its select against an already-closed Done channel.
	require.Positive(t, unissued, "a round that is over before it starts leaves jobs unissued")

	// Probe rows are written synchronously, so the count is exact the moment
	// the round returns: one per issued job, none for the queue casualties.
	prows, err := ms.RecentHTTPProbes(200, "")
	require.NoError(t, err)
	assert.Len(t, prows, issued, "an unissued job must not stand in history as a failed probe")
	for _, row := range prows {
		assert.NotNil(t, row.Path, "every recorded probe row observed something")
	}

	// The metric rows are written asynchronously; each one that lands carries
	// the graph of the job that was really issued.
	rows := ppathProbeRows(t, ms, issued)
	assert.Len(t, rows, issued)
	for _, m := range rows {
		assert.NotNil(t, m.Path, "a recorded probe metric row carries its graph")
	}
}

// TestProbeRowAndMetricRowShareOneGraph: Snapshot is memoized, so the probe row
// and the metric row recordProbe writes beside it carry the identical immutable
// graph — they cannot disagree about the path. Pointer identity is observable
// at the assembly seam only; the metrics store JSON round-trips rows, so the
// persisted copies are equal in content but not in identity.
func TestProbeRowAndMetricRowShareOneGraph(t *testing.T) {
	r, ms := newProbeOnlyRouter(t)

	// The seam: two assemblies off one recorder share one snapshot pointer.
	rec := core.NewPathRecorder()
	rec.SetClassifier(classifyLeg)
	rec.AddLocal()
	to := rec.AddTarget("127.0.0.1", "8080")
	_, _ = rec.Observe(core.Leg{
		Kind: core.LegStack, To: to, Target: "127.0.0.1:8080",
	}, nil, func() (net.Conn, error) { return nil, errors.New("severed (test)") })

	spec := core.HTTPProbeSpec{Method: "GET"}
	out := probeOutcome{err: errors.New("severed (test)"), stage: stageDo, path: rec}
	probeRow := assembleProbe(spec, "http://127.0.0.1:8080/ip", "direct", time.Now(), out)
	metricSide := assembleProbe(spec, "http://127.0.0.1:8080/ip", "direct", time.Now(), out)
	require.NotNil(t, probeRow.Path)
	assert.Same(t, probeRow.Path, metricSide.Path, "Snapshot is memoized: one graph, two rows")
	assert.Equal(t, []string{core.NodeLocalID, core.NodeTargetID}, ppathNodeIDs(probeRow.Path))
	edge := ppathEdge(t, probeRow.Path, core.NodeLocalID, core.NodeTargetID)
	assert.Equal(t, reasonUnknown, edge.Reason)

	// End to end: the persisted probe row and the persisted metric row of one
	// real probe hold equal graphs.
	target := probeTarget(t)
	p := ppathRunProbe(t, r, target.URL+"/ip", "direct", 10*time.Second)
	require.True(t, p.Success, p.Error)
	require.NotNil(t, p.Path)

	prows, err := ms.RecentHTTPProbes(10, "direct")
	require.NoError(t, err)
	require.Len(t, prows, 1)
	assert.Equal(t, p.Path, prows[0].Path, "the durable probe row holds the same graph")

	rows := ppathProbeRows(t, ms, 1)
	require.Len(t, rows, 1)
	assert.Equal(t, []string{core.HopProbe, "direct"}, rows[0].Chain)
	assert.Equal(t, p.Path, rows[0].Path, "the metric row beside it holds the same graph")
	assert.NotSame(t, p.Path, rows[0].Path, "the store round-trips rows through JSON")
}

// TestExitCheckRecordsNoPath: the exit-IP check is deliberately outside the
// feature. core.ExitCheck has no Path field — a check asks one question ("what
// IP does this outbound exit from") and its record stays that small — and
// CheckDial, which the checker dials through, adds no leg to a recorder riding
// its context.
func TestExitCheckRecordsNoPath(t *testing.T) {
	// The persisted record has no path key at all.
	b, err := json.Marshal(core.ExitCheck{
		ID: "1-relay", OutboundID: "relay", Site: "ifconfig.me",
		ExitIP: "203.0.113.7", Success: true, StartedAt: time.Now(),
	})
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(b, &fields))
	assert.NotContains(t, fields, "path", "core.ExitCheck carries no observed path")
	assert.NotContains(t, string(b), "path")

	// A bare context has no recorder, so a producer that does not observe pays
	// one ctx.Value miss.
	assert.Nil(t, core.PathRecorderFromContext(context.Background()))

	r, _ := newProbeOnlyRouter(t)
	out, ok := r.lookupOutbound("work")
	require.True(t, ok)
	echo := startEcho(t)
	addr := echo.ln.Addr().String()

	// CheckDial still dials, with and without a recorder in the context.
	conn, err := r.CheckDial(context.Background(), out, core.NetworkTCP, addr)
	require.NoError(t, err)
	require.NotNil(t, conn)
	_, err = conn.Write([]byte("ping"))
	require.NoError(t, err)
	buf := make([]byte, 4)
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	assert.Equal(t, "ping", string(buf))
	require.NoError(t, conn.Close())

	rec := core.NewPathRecorder()
	rec.SetClassifier(classifyLeg)
	cctx := core.WithPathRecorder(context.Background(), rec)
	require.Same(t, rec, core.PathRecorderFromContext(cctx))
	conn2, err := r.CheckDial(cctx, out, core.NetworkTCP, addr)
	require.NoError(t, err)
	require.NotNil(t, conn2)
	require.NoError(t, conn2.Close())
	assert.Nil(t, rec.Snapshot(), "CheckDial records no leg: the exit-IP path is unchanged")
}

// TestProbeTransportDoesNotDoubleRecordNamedRoute: probeTransport brackets the
// transport's dial for the trace but draws no leg of its own — every leg of a
// probe's path comes from the route machinery the dialer reaches through the ctx
// carrier. A named route records its own legs inside DialUpstream, so bracketing
// the dial a second time would draw the same local→proxy leg twice.
func TestProbeTransportDoesNotDoubleRecordNamedRoute(t *testing.T) {
	f := newReachFixture(t, false)
	target := probeTarget(t)

	p := ppathRunProbe(t, f.router, target.URL+"/ip", "outbound:relay", 10*time.Second)
	require.True(t, p.Success, p.Error)

	g := p.Path
	require.NotNil(t, g)
	proxyID := core.NodeProxyPrefix + "relay"

	// One leg leaves the engine host, and it is the route's own dial.
	from := ppathEdgesFrom(g, core.NodeLocalID)
	require.Len(t, from, 1, "the transport closure must not add a leg of its own: %+v", g.Edges)
	assert.Equal(t, proxyID, from[0].To)
	assert.Equal(t, core.LegDial, from[0].Kind)

	// No stack leg anywhere: a named route rides a proxy protocol end to end, and
	// the only producer of a bare stack leg is dialUpstream's direct branch,
	// which this route never reaches.
	for _, e := range g.Edges {
		assert.NotEqual(t, core.LegStack, e.Kind, "a named route records no bare stack leg")
		assert.Empty(t, e.Error)
	}
	assert.Len(t, g.Edges, 2, "the timed dial and the untimed tunnel: %+v", g.Edges)
	assert.False(t, g.Partial)

	// The control: the direct route's single leg is the stack leg dialUpstream's
	// direct branch brackets — drawn once, by the route and not by the transport.
	direct := ppathRunProbe(t, f.router, target.URL+"/ip", "direct", 10*time.Second)
	require.True(t, direct.Success, direct.Error)
	require.NotNil(t, direct.Path)
	dfrom := ppathEdgesFrom(direct.Path, core.NodeLocalID)
	require.Len(t, dfrom, 1)
	assert.Equal(t, core.NodeTargetID, dfrom[0].To)
	assert.Equal(t, core.LegStack, dfrom[0].Kind)
	assert.Equal(t, "direct", direct.Route)
}

// handshakeKillingServer accepts TCP connections and closes them at once, so a
// proxy handshake over it dies with EOF after the TCP connect succeeded. It is
// the shape of a server that is up but cannot proxy.
func handshakeKillingServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	return ln.Addr().String()
}

// TestHTTPProbeNestedRouteFailureIsNotAllGreen: an HTTP probe through a route
// whose own server dial rides another route, aimed at an unreachable target so
// the inner server rejects CONNECT. The nested route really did arrive at the
// proxy server, and both its legs are drawn as successes — but the handshake
// toward the target then died INSIDE the plugin's own call, which is the call
// the recorder's nested-arrival rule suppresses: no leg it brackets covers that
// failure, and inventing a failed local→proxy leg would contradict the
// adjacent successful arrival at the same node. So the failure surfaces as
// Partial. Without it the row says "proxy_rejected" while its graph publishes
// an all-green route for a request that never arrived.
func TestHTTPProbeNestedRouteFailureIsNotAllGreen(t *testing.T) {
	midSrv := socks5Server(t)  // the route under probe
	deepSrv := socks5Server(t) // the nested route mid's own dial rides
	dead := unboundLoopbackAddr(t)

	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "mid", Protocol: "socks5", UpstreamProxy: "outbound:deep",
				Config: map[string]any{"id": "mid", "address": midSrv}},
			{ID: "deep", Protocol: "socks5",
				Config: map[string]any{"id": "deep", "address": deepSrv}},
		},
		nil, nil, nil)

	p := ppathRunProbe(t, r, "http://"+dead+"/ip", "outbound:mid", 10*time.Second)
	require.False(t, p.Success, "mid's server cannot reach the unbound target")
	assert.Equal(t, phaseProxy, p.Phase, "error was %q", p.Error)
	assert.Equal(t, reasonProxyRejected, p.Reason, "error was %q", p.Error)

	g := p.Path
	require.NotNil(t, g)
	assert.Equal(t,
		[]string{core.NodeLocalID, "proxy:mid", "proxy:deep", core.NodeTargetID}, ppathNodeIDs(g))
	// The nested route's two real legs: local reached deep, deep carried the
	// dial to mid's server. Both succeeded — that is all this recorder bracketed.
	assert.Equal(t,
		[]string{"local->proxy:deep", "proxy:deep->proxy:mid"}, dpathEdgePairs(g))
	for _, e := range g.Edges {
		assert.True(t, e.Success, "the drawn legs really arrived: %+v", e)
	}
	assert.True(t, g.Partial,
		"the handshake toward the target died inside a suppressed call: the graph "+
			"must say it is missing that leg rather than publish an all-green route")
	assert.Empty(t, ppathEdgesFrom(g, core.NodeTargetID), "nothing left the target")
}

// TestPoolEveryMemberFailsThroughNestedRouteMarksPartial: a pool whose members
// both ride a working relay, where both member servers kill the handshake. Each
// attempt's nested route really arrived at its own member, so both arrivals are
// drawn as successes; both handshakes then died inside the plugin calls the
// recorder suppresses. "Every member dial failed" must not publish an all-green
// graph — the missing legs are reported through Partial.
func TestPoolEveryMemberFailsThroughNestedRouteMarksPartial(t *testing.T) {
	relaySrv := socks5Server(t)        // works: both members ride it
	m1Srv := handshakeKillingServer(t) // up, cannot proxy
	m2Srv := handshakeKillingServer(t)
	target := probeTarget(t)

	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "m1", Protocol: "socks5", UpstreamProxy: "outbound:relay",
				Config: map[string]any{"id": "m1", "address": m1Srv}},
			{ID: "m2", Protocol: "socks5", UpstreamProxy: "outbound:relay",
				Config: map[string]any{"id": "m2", "address": m2Srv}},
			{ID: "relay", Protocol: "socks5",
				Config: map[string]any{"id": "relay", "address": relaySrv}},
		},
		[]*config.Pool{{ID: "P", OutboundIDs: []string{"m1", "m2"},
			Selector: config.SelectorSpec{Type: "roundrobin"}}},
		nil, nil)

	p := ppathRunProbe(t, r, target.URL+"/ip", "pool:P", 10*time.Second)
	require.False(t, p.Success, "both member servers kill the handshake")
	require.Contains(t, p.Error, "every member dial failed")

	g := p.Path
	require.NotNil(t, g)
	// One relay leg and one arrival leg per attempt; every attempt burned its
	// own relay dial, so the relay is dialed twice.
	assert.Equal(t, []string{
		"local->proxy:relay", "proxy:relay->proxy:m1",
		"local->proxy:relay", "proxy:relay->proxy:m2#2",
	}, dpathEdgePairs(g))
	for _, e := range g.Edges {
		assert.True(t, e.Success, "the drawn legs really arrived: %+v", e)
	}
	assert.True(t, g.Partial,
		"both handshakes died inside suppressed calls: the graph must say it is "+
			"missing those legs rather than publish an all-green route")
	assert.Empty(t, ppathEdgesFrom(g, core.NodeTargetID), "nothing reached the target")
}
