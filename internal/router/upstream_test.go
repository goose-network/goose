package router

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/core"
	"github.com/goose-network/goose/internal/core/fake"
	"github.com/stretchr/testify/require"
)

// fakeOutbound fetches the *fake.FakeOutbound behind the router's wrapper for
// id, so tests can drive its dial behavior (OnDial / DialErr).
func fakeOutbound(t *testing.T, r *Router, id string) *fake.FakeOutbound {
	t.Helper()
	out, ok := r.lookupOutbound(id)
	require.True(t, ok, "outbound %s not built", id)
	w, ok := out.(interface{ Unwrap() core.Outbound })
	require.True(t, ok, "outbound %s does not unwrap", id)
	f, ok := w.Unwrap().(*fake.FakeOutbound)
	require.True(t, ok, "outbound %s is not a FakeOutbound", id)
	return f
}

// upstreamSpec builds a provider spec with the given route.
func upstreamSpec(route string) *config.ProviderSpec {
	return &config.ProviderSpec{ID: "p", Provider: "subscription", UpstreamProxy: route}
}

// dialViaRoute resolves and dials target through route, asserting success,
// and returns the connection for the caller to close.
func dialViaRoute(t *testing.T, r *Router, route, target string) net.Conn {
	t.Helper()
	d := r.UpstreamDialer(upstreamSpec(route))
	require.NotNil(t, d, "route %q resolved to no dialer", route)
	conn, err := d.DialUpstream(context.Background(), "tcp", target)
	require.NoError(t, err, "route %q dial %s", route, target)
	return conn
}

// unboundLoopbackAddr returns a loopback address with nothing listening, so
// direct dials fail fast (refused) instead of timing out.
func unboundLoopbackAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// TestUpstreamDialerNilWithoutRoute: a spec without upstream_proxy gets no
// dialer — the plugin fetches directly, the pre-feature behavior.
func TestUpstreamDialerNilWithoutRoute(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{{ID: "o1", Protocol: "testdirect", Config: map[string]any{"id": "o1"}}},
		nil, nil, nil)
	require.Nil(t, r.UpstreamDialer(&config.ProviderSpec{ID: "p", Provider: "subscription"}))
	require.Nil(t, r.UpstreamDialer(nil))
}

// TestUpstreamDialerDirectRoute: "direct" dials through the engine stack —
// a real loopback connection to the target.
func TestUpstreamDialerDirectRoute(t *testing.T) {
	r, _ := testRouter(t, nil, nil, nil, nil)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	go func() { c, _ := ln.Accept(); c.Write([]byte("direct-ok")); c.Close() }()

	conn := dialViaRoute(t, r, "direct", ln.Addr().String())
	defer conn.Close()
	buf := make([]byte, 12)
	n, _ := conn.Read(buf)
	require.Equal(t, "direct-ok", string(buf[:n]), "direct route should reach the loopback target")
}

// TestUpstreamDialerOutboundRoute: "outbound:<id>" pins one specific proxy
// server; the dial goes through that outbound's DialContext.
func TestUpstreamDialerOutboundRoute(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "px", Protocol: "testdirect", Config: map[string]any{"id": "px", "address": "127.0.0.1:9"}},
		},
		nil, nil, nil)

	sawTarget := make(chan string, 1)
	fo := fakeOutbound(t, r, "px")
	fo.OnDial = func(_ core.Network, target string) (net.Conn, error) {
		sawTarget <- target
		a, b := net.Pipe()
		go a.Close()
		return b, nil
	}

	conn := dialViaRoute(t, r, "outbound:px", "example.com:443")
	defer conn.Close()
	require.Equal(t, "example.com:443", <-sawTarget, "outbound route should dial the fetch target through the named outbound")
}

// TestUpstreamDialerPoolRouteFailover: "pool:<id>" picks a member with the
// pool's selector and fails over to another member when the first dial
// fails.
func TestUpstreamDialerPoolRouteFailover(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "m1", Protocol: "testdirect", Config: map[string]any{"id": "m1"}},
			{ID: "m2", Protocol: "testdirect", Config: map[string]any{"id": "m2"}},
		},
		[]*config.Pool{{ID: "pp", OutboundIDs: []string{"m1", "m2"}}},
		nil, nil)

	fakeOutbound(t, r, "m1").DialErr = errors.New("member 1 down")
	conn := dialViaRoute(t, r, "pool:pp", "example.com:443")
	defer conn.Close()
	// m1 fails, so the failover loop must have re-picked and landed on m2.
	require.Equal(t, 1, fakeOutbound(t, r, "m2").DialCount, "pool route should fail over to the second member")
}

// TestUpstreamDialerPoolRouteAllMembersFail: every member failing surfaces
// an error naming the pool.
func TestUpstreamDialerPoolRouteAllMembersFail(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "f1", Protocol: "testdirect", Config: map[string]any{"id": "f1"}},
		},
		[]*config.Pool{{ID: "fp", OutboundIDs: []string{"f1"}}},
		nil, nil)
	fakeOutbound(t, r, "f1").DialErr = errors.New("boom")

	d := r.UpstreamDialer(upstreamSpec("pool:fp"))
	require.NotNil(t, d)
	_, err := d.DialUpstream(context.Background(), "tcp", "example.com:443")
	require.ErrorContains(t, err, "upstream_proxy pool fp")
	require.ErrorContains(t, err, "boom")
}

// TestUpstreamDialerBareIDRoute: a bare id resolves as a pool when one with
// that id exists — chain-layer precedence.
func TestUpstreamDialerBareIDRoute(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "shared", Protocol: "testdirect", Config: map[string]any{"id": "shared"}},
		},
		[]*config.Pool{{ID: "shared", OutboundIDs: []string{"shared"}}},
		nil, nil)
	// A pool AND an outbound both named "shared": the pool wins.
	d := r.UpstreamDialer(upstreamSpec("shared"))
	require.NotNil(t, d)
	_, err := d.DialUpstream(context.Background(), "tcp", "example.com:443")
	require.NoError(t, err)
	require.Equal(t, 1, fakeOutbound(t, r, "shared").DialCount, "bare id should resolve to the pool's member")
}

// TestUpstreamDialerUnknownRoute: an unknown prefix or a missing outbound
// errors clearly instead of dialing directly.
func TestUpstreamDialerUnknownRoute(t *testing.T) {
	r, _ := testRouter(t, nil, nil, nil, nil)
	for _, route := range []string{"bogus:thing", "outbound:nope", "pool:nope"} {
		d := r.UpstreamDialer(upstreamSpec(route))
		require.NotNil(t, d, route)
		_, err := d.DialUpstream(context.Background(), "tcp", "example.com:443")
		require.Error(t, err, "route %q should fail", route)
		// The diagnostic names the route so the operator can fix the spec.
		if route == "bogus:thing" {
			require.ErrorContains(t, err, "bogus")
			require.ErrorContains(t, err, "unknown prefix")
		} else {
			require.ErrorContains(t, err, "nope")
		}
	}
}

// TestUpstreamDialerAccessRouteDirectFirst: the "access" route dials the
// fetch target directly when the local network reaches it — no proxy hop.
func TestUpstreamDialerAccessRouteDirectFirst(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "relay", Protocol: "testdirect", Config: map[string]any{"id": "relay"}},
		},
		[]*config.Pool{
			// The pool the provider feeds; its access filter names the relay pool.
			{ID: "fed", Access: &config.AccessFilterSpec{AccessPoolID: "relays"}},
			{ID: "relays", OutboundIDs: []string{"relay"}},
		},
		nil, nil)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	go func() { c, _ := ln.Accept(); c.Write([]byte("access-direct")); c.Close() }()

	spec := upstreamSpec("access")
	spec.PoolIDs = []string{"fed"}
	d := r.UpstreamDialer(spec)
	require.NotNil(t, d)
	conn, err := d.DialUpstream(context.Background(), "tcp", ln.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	buf := make([]byte, 16)
	n, _ := conn.Read(buf)
	require.Equal(t, "access-direct", string(buf[:n]), "access route should dial directly when reachable")
	require.Zero(t, fakeOutbound(t, r, "relay").DialCount, "relay member should not be dialed when the direct attempt succeeds")
}

// TestUpstreamDialerAccessRouteFallsBack: when the direct dial fails, the
// access route falls back to the access pool of a pool the provider feeds.
func TestUpstreamDialerAccessRouteFallsBack(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "relay", Protocol: "testdirect", Config: map[string]any{"id": "relay"}},
		},
		[]*config.Pool{
			{ID: "fed", Access: &config.AccessFilterSpec{AccessPoolID: "relays"}},
			{ID: "relays", OutboundIDs: []string{"relay"}},
		},
		nil, nil)

	unbound := unboundLoopbackAddr(t)
	sawTarget := make(chan string, 1)
	fo := fakeOutbound(t, r, "relay")
	fo.OnDial = func(_ core.Network, target string) (net.Conn, error) {
		sawTarget <- target
		a, b := net.Pipe()
		go a.Close()
		return b, nil
	}

	spec := upstreamSpec("access")
	spec.PoolIDs = []string{"fed"}
	d := r.UpstreamDialer(spec)
	require.NotNil(t, d)
	conn, err := d.DialUpstream(context.Background(), "tcp", unbound)
	require.NoError(t, err, "access route should fall back to the relay pool")
	defer conn.Close()
	require.Equal(t, unbound, <-sawTarget, "relay should dial the fetch target")
}

// TestUpstreamDialerAccessRouteNoAccessPool: direct fails and no fed pool
// has an access filter: the error explains both, rather than silently
// dialing through nothing.
func TestUpstreamDialerAccessRouteNoAccessPool(t *testing.T) {
	r, _ := testRouter(t, nil, nil, nil, nil)
	unbound := unboundLoopbackAddr(t)

	d := r.UpstreamDialer(upstreamSpec("access"))
	require.NotNil(t, d)
	_, err := d.DialUpstream(context.Background(), "tcp", unbound)
	require.ErrorContains(t, err, "no access pool")
}

// TestUpstreamDialerHotReload: route resolution is live — a pool edit
// applies to the next dial of an already-resolved dialer, without
// re-creating the provider.
func TestUpstreamDialerHotReload(t *testing.T) {
	r, cfg := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "old", Protocol: "testdirect", Config: map[string]any{"id": "old"}},
			{ID: "new", Protocol: "testdirect", Config: map[string]any{"id": "new"}},
		},
		[]*config.Pool{{ID: "hp", OutboundIDs: []string{"old"}}},
		nil, nil)

	d := r.UpstreamDialer(upstreamSpec("pool:hp"))
	require.NotNil(t, d)

	cfg.SetPool(&config.Pool{ID: "hp", OutboundIDs: []string{"new"}})
	r.Rebuild()

	conn, err := d.DialUpstream(context.Background(), "tcp", "example.com:443")
	require.NoError(t, err)
	conn.Close()
	require.Equal(t, 1, fakeOutbound(t, r, "new").DialCount, "edited pool membership should apply to the next dial")
	require.Zero(t, fakeOutbound(t, r, "old").DialCount)
}
