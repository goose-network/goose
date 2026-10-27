package router

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"

	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/core"
	"github.com/goose-network/goose/internal/core/fake"
	pub "github.com/goose-network/goose/pkg/plugin"
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

// upstreamFakeOutbound is the test plugin behind the "testupstream" protocol:
// it models an outbound plugin that consumes the engine-injected upstream
// dialer — every dial it serves rides the route configured on the outbound
// spec, exactly like http/socks5 route their own server connection.
type upstreamFakeOutbound struct {
	fake.FakeOutbound
	upstream pub.UpstreamDialer
}

func (u *upstreamFakeOutbound) DialContext(ctx context.Context, network core.Network, target string) (net.Conn, error) {
	if u.upstream == nil {
		return u.FakeOutbound.DialContext(ctx, network, target)
	}
	return u.upstream.DialUpstream(ctx, string(network), target)
}

// TestUpstreamDialerChainRouteMultiHop: "chain:<id>" traverses a named chain
// with real multi-hop semantics — hop 1 dials hop 2's proxy-server address
// (not the fetch target), and hop 2 is dialed THROUGH hop 1's tunnel with
// the fetch target as its dial target.
func TestUpstreamDialerChainRouteMultiHop(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "hop1", Protocol: "testdirect", Config: map[string]any{"id": "hop1"}},
			{ID: "hop2", Protocol: "testchain", Config: map[string]any{"id": "hop2", "address": "hop2.example:1080"}},
		},
		[]*config.Pool{
			{ID: "c1", OutboundIDs: []string{"hop1"}, Selector: config.SelectorSpec{Type: "random"}},
			{ID: "c2", OutboundIDs: []string{"hop2"}, Selector: config.SelectorSpec{Type: "random"}},
		},
		[]*config.ChainSpec{{ID: "cc", Layers: []string{"c1", "c2"}}},
		nil)

	var hop1Mu sync.Mutex
	hop1Target := ""
	unwrap(t, r, "hop1").(*fake.FakeOutbound).OnDial = func(_ core.Network, target string) (net.Conn, error) {
		hop1Mu.Lock()
		hop1Target = target
		hop1Mu.Unlock()
		a, b := net.Pipe()
		go a.Close()
		return b, nil
	}
	hop2 := unwrap(t, r, "hop2").(*fake.ChainFakeOutbound)

	conn := dialViaRoute(t, r, "chain:cc", "example.com:443")
	defer conn.Close()
	hop1Mu.Lock()
	require.Equal(t, "hop2.example:1080", hop1Target, "hop 1 should dial hop 2's proxy-server address")
	hop1Mu.Unlock()
	require.Equal(t, 1, hop2.DialThroughCount, "hop 2 should be dialed exactly once")
	require.NotNil(t, hop2.LastVia, "hop 2 must be dialed THROUGH hop 1's tunnel")
}

// TestUpstreamDialerChainRouteSingleHop: a one-layer chain routes the fetch
// through that layer's pool.
func TestUpstreamDialerChainRouteSingleHop(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{{ID: "solo", Protocol: "testdirect", Config: map[string]any{"id": "solo"}}},
		[]*config.Pool{{ID: "sp", OutboundIDs: []string{"solo"}}},
		[]*config.ChainSpec{{ID: "sc", Layers: []string{"sp"}}},
		nil)

	sawTarget := make(chan string, 1)
	fo := fakeOutbound(t, r, "solo")
	fo.OnDial = func(_ core.Network, target string) (net.Conn, error) {
		sawTarget <- target
		a, b := net.Pipe()
		go a.Close()
		return b, nil
	}

	conn := dialViaRoute(t, r, "chain:sc", "example.com:443")
	defer conn.Close()
	require.Equal(t, "example.com:443", <-sawTarget, "single-hop chain should dial the fetch target")
}

// TestUpstreamDialerChainUnknown: a chain id that is not configured errors
// clearly instead of dialing directly.
func TestUpstreamDialerChainUnknown(t *testing.T) {
	r, _ := testRouter(t, nil, nil, nil, nil)
	d := r.UpstreamDialer(upstreamSpec("chain:nope"))
	require.NotNil(t, d)
	_, err := d.DialUpstream(context.Background(), "tcp", "example.com:443")
	require.ErrorContains(t, err, `chain "nope"`)
	require.ErrorContains(t, err, "not configured")
}

// TestOutboundUpstreamDialerInjection: an outbound spec with upstream_proxy
// gets the engine route dialer injected into its config map at build time,
// and the built outbound's dials ride the named route — here reaching the
// pinned outbound "px" for the target it was asked to dial.
func TestOutboundUpstreamDialerInjection(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "rider", Protocol: "testupstream", UpstreamProxy: "outbound:px",
				Config: map[string]any{"id": "rider"}},
			{ID: "px", Protocol: "testdirect", Config: map[string]any{"id": "px"}},
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

	// Dialing through "rider" must reach "px" via rider's injected route.
	conn := dialViaRoute(t, r, "outbound:rider", "example.com:443")
	defer conn.Close()
	require.Equal(t, "example.com:443", <-sawTarget, "rider's dial should ride its injected upstream route to px")

	// An empty route yields no dialer — the outbound dials on its own.
	require.Nil(t, r.OutboundUpstreamDialer(""), "empty outbound-level route must not produce a dialer")
}

// TestOutboundUpstreamDialerAccessIsBareID: the retired "access" form is now
// just a bare id — resolved as a pool when one exists, else an outbound — so
// with neither configured it errors as an unknown reference instead of
// carrying special semantics.
func TestOutboundUpstreamDialerAccessIsBareID(t *testing.T) {
	r, _ := testRouter(t, nil, nil, nil, nil)
	d := r.OutboundUpstreamDialer("access")
	require.NotNil(t, d)
	_, err := d.DialUpstream(context.Background(), "tcp", "example.com:443")
	require.ErrorContains(t, err, "not configured")
}

// TestUpstreamDialerCycleGuard: a cyclic reference (an outbound routed
// through a pool that contains the outbound itself) errors with a diagnostic
// once the nesting depth cap is reached, instead of recursing until the
// process dies. The testupstream plugin consumes the injected dialer, so the
// recursion path is exactly what a real outbound plugin produces.
func TestUpstreamDialerCycleGuard(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "cyc", Protocol: "testupstream", UpstreamProxy: "pool:cp",
				Config: map[string]any{"id": "cyc"}},
		},
		[]*config.Pool{{ID: "cp", OutboundIDs: []string{"cyc"}}},
		nil, nil)

	d := r.UpstreamDialer(upstreamSpec("outbound:cyc"))
	require.NotNil(t, d)
	_, err := d.DialUpstream(context.Background(), "tcp", "example.com:443")
	require.Error(t, err, "a cyclic route must fail, not succeed")
	require.ErrorContains(t, err, "upstream_proxy", "the failure should surface as an upstream-route error")
	// The depth cap fires with the cyclic-reference diagnostic; the wrapped
	// error chain carries it up through the pool and outbound hops.
	require.ErrorContains(t, err, "cyclic")
}

// TestUpstreamDialerDepthGuardAtCap: a context already at the depth cap
// rejects the dial immediately — the guard itself, independent of how the
// depth got there.
func TestUpstreamDialerDepthGuardAtCap(t *testing.T) {
	r, _ := testRouter(t, nil, nil, nil, nil)
	d := r.UpstreamDialer(upstreamSpec("direct"))
	require.NotNil(t, d)
	ctx := context.WithValue(context.Background(), upstreamDepthKey{}, maxUpstreamDepth)
	_, err := d.DialUpstream(ctx, "tcp", "example.com:443")
	require.ErrorContains(t, err, "cyclic")
}

// TestUpstreamDialerChainHotReload: the chain route resolves live like the
// others — editing the chain's pool applies to the next dial of an
// already-resolved dialer.
func TestUpstreamDialerChainHotReload(t *testing.T) {
	r, cfg := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "a", Protocol: "testdirect", Config: map[string]any{"id": "a"}},
			{ID: "b", Protocol: "testdirect", Config: map[string]any{"id": "b"}},
		},
		[]*config.Pool{{ID: "cp", OutboundIDs: []string{"a"}}},
		[]*config.ChainSpec{{ID: "cc", Layers: []string{"cp"}}},
		nil)

	d := r.UpstreamDialer(upstreamSpec("chain:cc"))
	require.NotNil(t, d)

	cfg.SetPool(&config.Pool{ID: "cp", OutboundIDs: []string{"b"}})
	r.Rebuild()

	conn, err := d.DialUpstream(context.Background(), "tcp", "example.com:443")
	require.NoError(t, err)
	conn.Close()
	require.Equal(t, 1, fakeOutbound(t, r, "b").DialCount, "edited chain pool membership should apply to the next dial")
	require.Zero(t, fakeOutbound(t, r, "a").DialCount)
}
