package router

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/core"
	"github.com/goose-network/goose/internal/core/fake"
	"github.com/goose-network/goose/internal/metrics"
	"github.com/goose-network/goose/internal/stack"
	pub "github.com/goose-network/goose/pkg/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// registerTestPlugins registers test-only outbound factories that return fake
// outbounds. It is idempotent (guarded by sync.Once) because pub.Register
// panics on duplicate registration and a test binary may construct many
// routers. "testdirect" returns a plain FakeOutbound (no ChainDialer);
// "testchain" returns a ChainFakeOutbound (implements ChainDialer);
// "testupstream" returns an outbound whose dials ride the engine-injected
// upstream-route dialer, modeling what http/socks5-style outbounds do with
// their own server connection.
var testPluginsOnce sync.Once

func registerTestPlugins() {
	testPluginsOnce.Do(func() {
		if _, ok := pub.Lookup("testdirect"); !ok {
			pub.Register("testdirect", func(cfg map[string]any) (core.Outbound, error) {
				return &fake.FakeOutbound{IDVal: cfg["id"].(string), Proto: "testdirect", AddrVal: cfgStr(cfg, "address")}, nil
			})
		}
		if _, ok := pub.Lookup("testchain"); !ok {
			pub.Register("testchain", func(cfg map[string]any) (core.Outbound, error) {
				return &fake.ChainFakeOutbound{FakeOutbound: fake.FakeOutbound{IDVal: cfg["id"].(string), Proto: "testchain", AddrVal: cfgStr(cfg, "address")}}, nil
			})
		}
		if _, ok := pub.Lookup("testupstream"); !ok {
			pub.Register("testupstream", func(cfg map[string]any) (core.Outbound, error) {
				return &upstreamFakeOutbound{
					FakeOutbound: fake.FakeOutbound{IDVal: cfg["id"].(string), Proto: "testupstream"},
					upstream:     pub.AsUpstreamDialer(cfg),
				}, nil
			})
		}
	})
}

// cfgStr reads a string value from a plugin config map, "" when absent or
// not a string.
func cfgStr(cfg map[string]any, key string) string {
	if v, ok := cfg[key].(string); ok {
		return v
	}
	return ""
}

// testRouter builds a router backed by a config store pre-populated with the
// given outbounds/pools/chains/inbounds, using the system stack and a temp
// metrics store. It returns the router and its config store so tests can
// mutate config for hot-reload assertions.
func testRouter(t *testing.T, outbounds []*config.OutboundSpec, pools []*config.Pool, chains []*config.ChainSpec, inbounds []*config.Inbound) (*Router, *config.Store) {
	t.Helper()
	registerTestPlugins()
	cfg := config.NewStore()
	for _, o := range outbounds {
		cfg.SetOutbound(o)
	}
	for _, p := range pools {
		cfg.SetPool(p)
	}
	for _, c := range chains {
		cfg.SetChain(c)
	}
	for _, in := range inbounds {
		cfg.SetInbound(in)
	}
	stk, err := stack.New("system")
	require.NoError(t, err)
	t.Cleanup(func() { stk.Close() })
	ms, err := metrics.Open(t.TempDir() + "/m.db")
	require.NoError(t, err)
	t.Cleanup(func() { ms.Close() })
	r := New(cfg, ms, stk)
	return r, cfg
}

// inboundOnChain returns an Inbound config whose id is inboundID and whose
// default policy resolves to chainID. Used to wire resolveChain in tests.
func inboundOnChain(inboundID, chainID string) *config.Inbound {
	return &config.Inbound{
		ID:       inboundID,
		Protocol: "http",
		Listen:   "127.0.0.1:0",
		Policy:   config.InboundPolicy{ChainID: chainID},
	}
}

// metaFor builds a core.Metadata for the given inbound id + target.
func metaFor(inboundID, host, port string) core.Metadata {
	return core.Metadata{
		InboundID:  inboundID,
		Network:    core.NetworkTCP,
		TargetHost: host,
		TargetPort: port,
		StartedAt:  time.Now(),
	}
}

// unwrap reaches the plugin-built outbound inside its outboundWrapper, so a
// test can configure hooks (OnDial/OnDialThrough/DialErr) on the fake.
func unwrap(t *testing.T, r *Router, id string) core.Outbound {
	t.Helper()
	r.mu.RLock()
	w := r.outbounds[id]
	r.mu.RUnlock()
	require.NotNil(t, w, "outbound %s not built", id)
	return w.(interface{ Unwrap() core.Outbound }).Unwrap()
}

// TestDialChainSingleHop dials through a one-layer chain and asserts the
// selected outbound's DialContext was called and a connection is returned.
func TestDialChainSingleHop(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{{ID: "ob1", Protocol: "testdirect", Config: map[string]any{"id": "ob1"}}},
		[]*config.Pool{{ID: "p1", OutboundIDs: []string{"ob1"}, Selector: config.SelectorSpec{Type: "random"}}},
		[]*config.ChainSpec{{ID: "c1", Layers: []string{"p1"}}},
		[]*config.Inbound{inboundOnChain("in", "c1")},
	)
	conn, chain, err := r.Dial(context.Background(), metaFor("in", "example.com", "80"))
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()
	assert.Equal(t, []string{"ob1"}, chain)
}

// TestDialChainMultiHopForwardsChainDialer is the regression test for the
// critical bug where outboundWrapper did not forward ChainDialer, so every
// multi-hop chain collapsed to a direct dial. With the Unwrap() fix, the
// second layer's DialThrough MUST be called with the first layer's conn as
// `via` — proving the chain actually traverses both hops.
func TestDialChainMultiHopForwardsChainDialer(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "hop1", Protocol: "testdirect", Config: map[string]any{"id": "hop1"}},
			{ID: "hop2", Protocol: "testchain", Config: map[string]any{"id": "hop2"}},
		},
		[]*config.Pool{
			{ID: "pool1", OutboundIDs: []string{"hop1"}, Selector: config.SelectorSpec{Type: "random"}},
			{ID: "pool2", OutboundIDs: []string{"hop2"}, Selector: config.SelectorSpec{Type: "random"}},
		},
		[]*config.ChainSpec{{ID: "c2", Layers: []string{"pool1", "pool2"}}},
		[]*config.Inbound{inboundOnChain("in", "c2")},
	)

	// Capture the via conn handed to hop2.DialThrough so we can assert it is
	// non-nil — i.e. hop2 was dialed THROUGH hop1's conn, not directly.
	var capturedVia net.Conn
	var viaMu sync.Mutex
	chainOb := unwrap(t, r, "hop2").(*fake.ChainFakeOutbound)
	chainOb.OnDialThrough = func(_ core.Network, _ string, via net.Conn) (net.Conn, error) {
		viaMu.Lock()
		capturedVia = via
		viaMu.Unlock()
		// Return a fresh pipe so the caller gets a usable conn.
		a, b := net.Pipe()
		go a.Close()
		return b, nil
	}

	conn, chainIDs, err := r.Dial(context.Background(), metaFor("in", "example.com", "80"))
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()
	assert.Equal(t, []string{"hop1", "hop2"}, chainIDs, "both hops should be in the chain")
	assert.Equal(t, 1, chainOb.DialThroughCount, "hop2.DialThrough must be called exactly once")
	require.NotNil(t, capturedVia, "hop2 must be dialed THROUGH hop1's conn (ChainDialer forwarded), not nil")
}

// closeRecorder wraps a net.Conn and records whether Close was called. It is
// used to detect that the router closed a previous-hop connection on the
// fallback / error paths (leak detection). Unlike net.Pipe, closing one end
// does not close the other, so the test retains a live handle to inspect.
type closeRecorder struct {
	net.Conn
	closed atomic.Bool
	mu     sync.Mutex
}

func (c *closeRecorder) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

// TestDialChainNonChainDialerFallsBackAndClosesPrev asserts that when a
// second-layer outbound does NOT implement ChainDialer, the router falls back
// to a direct dial AND closes the previous hop's connection (no leak).
func TestDialChainNonChainDialerFallsBackAndClosesPrev(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "hop1", Protocol: "testdirect", Config: map[string]any{"id": "hop1"}},
			{ID: "hop2", Protocol: "testdirect", Config: map[string]any{"id": "hop2"}}, // no ChainDialer
		},
		[]*config.Pool{
			{ID: "pool1", OutboundIDs: []string{"hop1"}, Selector: config.SelectorSpec{Type: "random"}},
			{ID: "pool2", OutboundIDs: []string{"hop2"}, Selector: config.SelectorSpec{Type: "random"}},
		},
		[]*config.ChainSpec{{ID: "c2", Layers: []string{"pool1", "pool2"}}},
		[]*config.Inbound{inboundOnChain("in", "c2")},
	)

	// hop1 returns a closeRecorder wrapping a pipe end. The router must close
	// this conn on the non-ChainDialer fallback path; we detect that via the
	// recorder's flag rather than by reading (a closed pipe's peer errors on
	// deadline-set, which is awkward to assert).
	a, b := net.Pipe()
	rec := &closeRecorder{Conn: b}
	go a.Close() // nothing writes; just keep the pipe from leaking
	hop1 := unwrap(t, r, "hop1").(*fake.FakeOutbound)
	hop1.OnDial = func(core.Network, string) (net.Conn, error) { return rec, nil }

	conn, chainIDs, err := r.Dial(context.Background(), metaFor("in", "example.com", "80"))
	require.NoError(t, err)
	defer conn.Close()
	assert.Equal(t, []string{"hop1", "hop2"}, chainIDs)
	assert.True(t, rec.closed.Load(), "hop1 conn should be closed by router on non-ChainDialer fallback (no leak)")
}

// TestDialChainLayerErrorClosesPriorConn asserts that when a later layer's
// dial fails, the connection established by the previous layer is closed
// (no leak on the error path).
func TestDialChainLayerErrorClosesPriorConn(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "hop1", Protocol: "testdirect", Config: map[string]any{"id": "hop1"}},
			{ID: "hop2", Protocol: "testchain", Config: map[string]any{"id": "hop2"}},
		},
		[]*config.Pool{
			{ID: "pool1", OutboundIDs: []string{"hop1"}, Selector: config.SelectorSpec{Type: "random"}},
			{ID: "pool2", OutboundIDs: []string{"hop2"}, Selector: config.SelectorSpec{Type: "random"}},
		},
		[]*config.ChainSpec{{ID: "c2", Layers: []string{"pool1", "pool2"}}},
		[]*config.Inbound{inboundOnChain("in", "c2")},
	)

	a, b := net.Pipe()
	rec := &closeRecorder{Conn: b}
	go a.Close()
	hop1 := unwrap(t, r, "hop1").(*fake.FakeOutbound)
	hop1.OnDial = func(core.Network, string) (net.Conn, error) { return rec, nil }
	hop2 := unwrap(t, r, "hop2").(*fake.ChainFakeOutbound)
	hop2.DialErr = errors.New("simulated layer-2 dial failure")

	_, _, err := r.Dial(context.Background(), metaFor("in", "example.com", "80"))
	require.Error(t, err)
	assert.True(t, rec.closed.Load(), "prior hop conn must be closed when a later layer's dial fails")
}

// TestDialChainEmptyPoolReturnsError asserts the empty-pool error path: a
// chain layer whose pool has no built outbounds (here, the pool references an
// outbound id that was never added) yields an error, and no connection leaks.
func TestDialChainEmptyPoolReturnsError(t *testing.T) {
	r, _ := testRouter(t,
		nil, // no outbounds => pool "p1" will be empty
		[]*config.Pool{{ID: "p1", OutboundIDs: []string{"ghost"}, Selector: config.SelectorSpec{Type: "random"}}},
		[]*config.ChainSpec{{ID: "c1", Layers: []string{"p1"}}},
		[]*config.Inbound{inboundOnChain("in", "c1")},
	)
	_, _, err := r.Dial(context.Background(), metaFor("in", "example.com", "80"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty pool")
}

// TestResolveChainFallback asserts that an inbound with no configured chain
// (no matching policy) falls back to a single layer that picks any available
// outbound and dials successfully.
func TestResolveChainFallback(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{{ID: "ob1", Protocol: "testdirect", Config: map[string]any{"id": "ob1"}}},
		nil, nil, nil, // no chains, no inbounds => fallback
	)
	// "in" has no policy entry, so resolveChain returns the fallback chain.
	conn, _, err := r.Dial(context.Background(), metaFor("in", "example.com", "80"))
	require.NoError(t, err)
	require.NotNil(t, conn)
	conn.Close()
}

// TestResolveChainFallbackRoundRobins asserts the shared fallback selector
// actually round-robins (its counter is shared across requests) rather than
// always picking pool[0]. With two outbounds and the fallback chain, two
// sequential dials should hit different outbounds.
func TestResolveChainFallbackRoundRobins(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "a", Protocol: "testdirect", Config: map[string]any{"id": "a"}},
			{ID: "b", Protocol: "testdirect", Config: map[string]any{"id": "b"}},
		},
		nil, nil, nil,
	)
	seen := map[string]int{}
	for i := 0; i < 4; i++ {
		conn, chain, err := r.Dial(context.Background(), metaFor("in", "example.com", "80"))
		require.NoError(t, err)
		conn.Close()
		require.Len(t, chain, 1)
		seen[chain[0]]++
	}
	assert.Len(t, seen, 2, "fallback selector should round-robin across both outbounds, not always pick the first")
}

// TestResolveChainPoolPolicy verifies goal 2's direct pool routing: an
// inbound whose policy names a pool_id (no chain) routes through that pool's
// outbounds only, ignoring outbounds outside the pool.
func TestResolveChainPoolPolicy(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "in-pool", Protocol: "testdirect", Config: map[string]any{"id": "in-pool"}},
			{ID: "out-pool", Protocol: "testdirect", Config: map[string]any{"id": "out-pool"}},
		},
		[]*config.Pool{{ID: "mypool", OutboundIDs: []string{"in-pool"}}},
		nil,
		[]*config.Inbound{{
			ID:       "in",
			Protocol: "http",
			Listen:   "127.0.0.1:0",
			Policy:   config.InboundPolicy{PoolID: "mypool"},
		}},
	)
	for i := 0; i < 4; i++ {
		conn, chain, err := r.Dial(context.Background(), metaFor("in", "example.com", "80"))
		require.NoError(t, err)
		conn.Close()
		require.Len(t, chain, 1)
		assert.Equal(t, "in-pool", chain[0], "pool policy should route only through the pool's outbounds")
	}
}

// TestResolveChainPoolPolicyFilters verifies the per-inbound filters narrow
// the pool: a protocol filter that excludes the pool's only member makes the
// dial fail, proving the filter is applied on top of the pool route.
func TestResolveChainPoolPolicyFilters(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "only", Protocol: "testdirect", Config: map[string]any{"id": "only"}},
		},
		[]*config.Pool{{ID: "mypool", OutboundIDs: []string{"only"}}},
		nil,
		[]*config.Inbound{{
			ID:       "in",
			Protocol: "http",
			Listen:   "127.0.0.1:0",
			Policy: config.InboundPolicy{
				PoolID:  "mypool",
				Filters: []config.FilterSpec{{Type: "protocol", Params: map[string]any{"allow": []any{"socks5"}}}},
			},
		}},
	)
	_, _, err := r.Dial(context.Background(), metaFor("in", "example.com", "80"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no outbound passed filters")
}

// TestResolveChainUserPoolPolicy verifies a per-user policy with only a pool
// route (no chain_id) overrides the inbound's default route for that user,
// while other users keep the default.
func TestResolveChainUserPoolPolicy(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "shared", Protocol: "testdirect", Config: map[string]any{"id": "shared"}},
			{ID: "vip", Protocol: "testdirect", Config: map[string]any{"id": "vip"}},
		},
		[]*config.Pool{
			{ID: "pool-default", OutboundIDs: []string{"shared"}},
			{ID: "pool-vip", OutboundIDs: []string{"vip"}},
		},
		nil,
		[]*config.Inbound{{
			ID:       "in",
			Protocol: "http",
			Listen:   "127.0.0.1:0",
			Policy:   config.InboundPolicy{PoolID: "pool-default"},
			Users: []config.User{{
				Username: "alice",
				Password: "pw",
				Policy:   &config.InboundPolicy{PoolID: "pool-vip"},
			}},
		}},
	)
	meta := metaFor("in", "example.com", "80")
	conn, chain, err := r.Dial(context.Background(), meta)
	require.NoError(t, err)
	conn.Close()
	assert.Equal(t, "shared", chain[0])

	meta = metaFor("in", "example.com", "80")
	meta.User = "alice"
	conn, chain, err = r.Dial(context.Background(), meta)
	require.NoError(t, err)
	conn.Close()
	assert.Equal(t, "vip", chain[0])
}

// TestResolveChainUserPoolOverridesInboundChain pins the group-replacement
// semantics: a user policy that sets a pool_id REPLACES the inbound's route
// as a group, so it wins over the inbound's chain_id. (The old resolver let
// a valid inbound chain_id silently beat a user pool policy — the quirk
// goal 3 fixes.)
func TestResolveChainUserPoolOverridesInboundChain(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "chain-hop", Protocol: "testdirect", Config: map[string]any{"id": "chain-hop"}},
			{ID: "vip", Protocol: "testdirect", Config: map[string]any{"id": "vip"}},
		},
		[]*config.Pool{
			{ID: "pool-chain", OutboundIDs: []string{"chain-hop"}},
			{ID: "pool-vip", OutboundIDs: []string{"vip"}},
		},
		[]*config.ChainSpec{{ID: "c1", Layers: []string{"pool-chain"}}},
		[]*config.Inbound{{
			ID:       "in",
			Protocol: "http",
			Listen:   "127.0.0.1:0",
			Policy:   config.InboundPolicy{ChainID: "c1"},
			Users: []config.User{{
				Username: "alice",
				Password: "pw",
				Policy:   &config.InboundPolicy{PoolID: "pool-vip"},
			}},
		}},
	)
	meta := metaFor("in", "example.com", "80")
	conn, chain, err := r.Dial(context.Background(), meta)
	require.NoError(t, err)
	conn.Close()
	assert.Equal(t, "chain-hop", chain[0], "anonymous traffic follows the inbound chain")

	meta = metaFor("in", "example.com", "80")
	meta.User = "alice"
	conn, chain, err = r.Dial(context.Background(), meta)
	require.NoError(t, err)
	conn.Close()
	assert.Equal(t, "vip", chain[0], "user pool policy must override the inbound chain (group replacement)")
}

// TestResolveChainOutboundPolicy verifies goal 3's server-level routing: a
// policy that names outbound_id pins every connection to that one server,
// even though pools with other members exist.
func TestResolveChainOutboundPolicy(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "pinned", Protocol: "testdirect", Config: map[string]any{"id": "pinned"}},
			{ID: "other", Protocol: "testdirect", Config: map[string]any{"id": "other"}},
		},
		[]*config.Pool{{ID: "mypool", OutboundIDs: []string{"other"}}},
		nil,
		[]*config.Inbound{{
			ID:       "in",
			Protocol: "http",
			Listen:   "127.0.0.1:0",
			Policy:   config.InboundPolicy{OutboundID: "pinned"},
		}},
	)
	for i := 0; i < 3; i++ {
		conn, chain, err := r.Dial(context.Background(), metaFor("in", "example.com", "80"))
		require.NoError(t, err)
		conn.Close()
		assert.Equal(t, []string{"pinned"}, chain, "outbound_id must pin the route to that server")
	}
}

// TestResolveChainUserOutboundPolicy verifies per-user server pinning: the
// user's outbound_id replaces the inbound's pool route (route fields are
// replaced as a group), while anonymous traffic keeps the pool.
func TestResolveChainUserOutboundPolicy(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "shared", Protocol: "testdirect", Config: map[string]any{"id": "shared"}},
			{ID: "vip-server", Protocol: "testdirect", Config: map[string]any{"id": "vip-server"}},
		},
		[]*config.Pool{{ID: "pool-default", OutboundIDs: []string{"shared"}}},
		nil,
		[]*config.Inbound{{
			ID:       "in",
			Protocol: "http",
			Listen:   "127.0.0.1:0",
			Policy:   config.InboundPolicy{PoolID: "pool-default"},
			Users: []config.User{{
				Username: "alice",
				Password: "pw",
				Policy:   &config.InboundPolicy{OutboundID: "vip-server"},
			}},
		}},
	)
	meta := metaFor("in", "example.com", "80")
	conn, chain, err := r.Dial(context.Background(), meta)
	require.NoError(t, err)
	conn.Close()
	assert.Equal(t, "shared", chain[0])

	meta = metaFor("in", "example.com", "80")
	meta.User = "alice"
	conn, chain, err = r.Dial(context.Background(), meta)
	require.NoError(t, err)
	conn.Close()
	assert.Equal(t, "vip-server", chain[0], "user outbound_id must override the inbound pool route")
}

// TestResolveChainUserFiltersReplaceInboundFilters verifies goal 2: filters
// are per-user. A user policy carrying only filters inherits the inbound's
// route but has its OWN filters (replacing, not merging with, the inbound's):
// alice's filters reject the pool's members while bob's accept them, and
// anonymous traffic keeps the inbound-level filters.
func TestResolveChainUserFiltersReplaceInboundFilters(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "only", Protocol: "testdirect", Config: map[string]any{"id": "only"}},
		},
		[]*config.Pool{{ID: "mypool", OutboundIDs: []string{"only"}}},
		nil,
		[]*config.Inbound{{
			ID:       "in",
			Protocol: "http",
			Listen:   "127.0.0.1:0",
			Policy: config.InboundPolicy{
				PoolID:  "mypool",
				Filters: []config.FilterSpec{{Type: "protocol", Params: map[string]any{"allow": []any{"testdirect"}}}},
			},
			Users: []config.User{
				{Username: "alice", Password: "pw", Policy: &config.InboundPolicy{
					// filters only: inherits the pool route, replaces the filters
					Filters: []config.FilterSpec{{Type: "protocol", Params: map[string]any{"allow": []any{"socks5"}}}},
				}},
				{Username: "bob", Password: "pw", Policy: &config.InboundPolicy{
					Filters: []config.FilterSpec{{Type: "protocol", Params: map[string]any{"allow": []any{"testdirect"}}}},
				}},
			},
		}},
	)
	meta := metaFor("in", "example.com", "80")
	conn, chain, err := r.Dial(context.Background(), meta)
	require.NoError(t, err)
	conn.Close()
	assert.Equal(t, "only", chain[0], "anonymous traffic uses the inbound-level filters")

	meta.User = "alice"
	_, _, err = r.Dial(context.Background(), meta)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no outbound passed filters", "alice's filters must replace the inbound's")

	meta = metaFor("in", "example.com", "80")
	meta.User = "bob"
	conn, chain, err = r.Dial(context.Background(), meta)
	require.NoError(t, err)
	conn.Close()
	assert.Equal(t, "only", chain[0], "bob's own filters accept the pool member")
}

// TestResolveChainUserFiltersNarrowEveryChainLayer verifies that per-user
// filters apply to a NAMED chain, not just pool routes: alice's protocol
// filter rejects layer 0's candidates so her dial fails, while anonymous
// traffic traverses both hops.
func TestResolveChainUserFiltersNarrowEveryChainLayer(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "ob-a", Protocol: "testdirect", Config: map[string]any{"id": "ob-a"}},
			{ID: "ob-b", Protocol: "testchain", Config: map[string]any{"id": "ob-b"}},
		},
		[]*config.Pool{
			{ID: "pool-a", OutboundIDs: []string{"ob-a"}, Selector: config.SelectorSpec{Type: "random"}},
			{ID: "pool-b", OutboundIDs: []string{"ob-b"}, Selector: config.SelectorSpec{Type: "random"}},
		},
		[]*config.ChainSpec{{ID: "c2", Layers: []string{"pool-a", "pool-b"}}},
		[]*config.Inbound{{
			ID:       "in",
			Protocol: "http",
			Listen:   "127.0.0.1:0",
			Policy:   config.InboundPolicy{ChainID: "c2"},
			Users: []config.User{{
				Username: "alice",
				Password: "pw",
				Policy: &config.InboundPolicy{
					Filters: []config.FilterSpec{{Type: "protocol", Params: map[string]any{"allow": []any{"socks5"}}}},
				},
			}},
		}},
	)
	meta := metaFor("in", "example.com", "80")
	conn, chain, err := r.Dial(context.Background(), meta)
	require.NoError(t, err)
	conn.Close()
	assert.Equal(t, []string{"ob-a", "ob-b"}, chain)

	meta.User = "alice"
	_, _, err = r.Dial(context.Background(), meta)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no outbound passed filters", "user filters must narrow every layer of the named chain")
}

// TestChainLayersPinOutboundServers verifies goal 3's chain-level change: a
// chain layer may name a single outbound server (not just a pool). Both hops
// are pinned servers; hop 1 is dialed through hop 0's connection.
func TestChainLayersPinOutboundServers(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "hop0", Protocol: "testchain", Config: map[string]any{"id": "hop0"}},
			{ID: "hop1", Protocol: "testchain", Config: map[string]any{"id": "hop1"}},
		},
		nil,
		[]*config.ChainSpec{{ID: "c3", Layers: []string{"hop0", "hop1"}}},
		[]*config.Inbound{inboundOnChain("in", "c3")},
	)
	hop1 := unwrap(t, r, "hop1").(*fake.ChainFakeOutbound)
	hop1.OnDialThrough = func(_ core.Network, _ string, via net.Conn) (net.Conn, error) {
		require.NotNil(t, via, "hop1 must be dialed through hop0's conn")
		a, b := net.Pipe()
		go a.Close()
		return b, nil
	}
	conn, chainIDs, err := r.Dial(context.Background(), metaFor("in", "example.com", "80"))
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()
	assert.Equal(t, []string{"hop0", "hop1"}, chainIDs, "both pinned hops should be traversed")
}

// TestResolveChainPolicyPrecedence verifies the documented resolution order
// chain_id > outbound_id > pool_id, including fall-through when a reference
// is unresolvable (missing chain, then missing outbound).
func TestResolveChainPolicyPrecedence(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			{ID: "ob-a", Protocol: "testdirect", Config: map[string]any{"id": "ob-a"}},
			{ID: "ob-b", Protocol: "testdirect", Config: map[string]any{"id": "ob-b"}},
			{ID: "ob-x", Protocol: "testdirect", Config: map[string]any{"id": "ob-x"}},
		},
		[]*config.Pool{
			{ID: "pool-a", OutboundIDs: []string{"ob-a"}},
			{ID: "pool-b", OutboundIDs: []string{"ob-b"}},
		},
		[]*config.ChainSpec{{ID: "c1", Layers: []string{"pool-b"}}},
		[]*config.Inbound{
			{ID: "in-all", Protocol: "http", Listen: "127.0.0.1:0",
				Policy: config.InboundPolicy{ChainID: "c1", OutboundID: "ob-x", PoolID: "pool-a"}},
			{ID: "in-out", Protocol: "http", Listen: "127.0.0.1:0",
				Policy: config.InboundPolicy{ChainID: "missing", OutboundID: "ob-x", PoolID: "pool-a"}},
			{ID: "in-pool", Protocol: "http", Listen: "127.0.0.1:0",
				Policy: config.InboundPolicy{ChainID: "missing", OutboundID: "missing", PoolID: "pool-a"}},
		},
	)
	conn, chain, err := r.Dial(context.Background(), metaFor("in-all", "example.com", "80"))
	require.NoError(t, err)
	conn.Close()
	assert.Equal(t, "ob-b", chain[0], "chain_id must beat outbound_id and pool_id")

	conn, chain, err = r.Dial(context.Background(), metaFor("in-out", "example.com", "80"))
	require.NoError(t, err)
	conn.Close()
	assert.Equal(t, "ob-x", chain[0], "unresolvable chain falls through to outbound_id")

	conn, chain, err = r.Dial(context.Background(), metaFor("in-pool", "example.com", "80"))
	require.NoError(t, err)
	conn.Close()
	assert.Equal(t, "ob-a", chain[0], "unresolvable chain+outbound falls through to pool_id")
}
