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
// "testchain" returns a ChainFakeOutbound (implements ChainDialer).
var testPluginsOnce sync.Once

func registerTestPlugins() {
	testPluginsOnce.Do(func() {
		if _, ok := pub.Lookup("testdirect"); !ok {
			pub.Register("testdirect", func(cfg map[string]any) (core.Outbound, error) {
				return &fake.FakeOutbound{IDVal: cfg["id"].(string), Proto: "testdirect"}, nil
			})
		}
		if _, ok := pub.Lookup("testchain"); !ok {
			pub.Register("testchain", func(cfg map[string]any) (core.Outbound, error) {
				return &fake.ChainFakeOutbound{FakeOutbound: fake.FakeOutbound{IDVal: cfg["id"].(string), Proto: "testchain"}}, nil
			})
		}
	})
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
