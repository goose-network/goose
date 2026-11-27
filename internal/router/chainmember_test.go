package router

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/core"
	"github.com/goose-network/goose/internal/core/fake"
	"github.com/goose-network/goose/internal/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// memberOutbound builds an outbound spec for the test plugins with a server
// address, so path graphs and entry-reachability have real addresses to name.
func memberOutbound(id, proto, addr string) *config.OutboundSpec {
	return &config.OutboundSpec{ID: id, Protocol: proto, Config: map[string]any{"id": id, "address": addr}}
}

// poolOnMembers wires an inbound whose default policy routes through a pool
// of chain members — the only dispatch surface a path-pool is reachable on
// (besides upstream pool routes).
func poolOnMembers(inboundID, poolID string, chainIDs []string, sel string, filters []config.FilterSpec) (*config.Pool, *config.Inbound) {
	return &config.Pool{ID: poolID, ChainIDs: chainIDs, Selector: config.SelectorSpec{Type: sel}, Filters: filters},
		&config.Inbound{ID: inboundID, Protocol: "http", Listen: "127.0.0.1:0", Policy: config.InboundPolicy{PoolID: poolID}}
}

// seedProbeVerdict records a reachability-probe row ([local, id]) directly,
// standing in for a probe round: observe updates the in-memory verdict
// synchronously, so the next dial sees it.
func seedProbeVerdict(t *testing.T, ms *metrics.Store, id string, ok bool, at time.Time) {
	t.Helper()
	require.NoError(t, ms.Record(core.RequestMetric{
		Chain: []string{core.HopLocal, id}, Success: ok,
		StartedAt: at, FinishedAt: at,
	}))
}

// TestChainMemberExpansionExactHopsAndPath is the core mechanism-A contract:
// picking a chain member splices its pre-pinned hops into the hop list, the
// request Chain is the REAL hop IDs (no chain-ID prepend), hop 2 is dialed
// THROUGH hop 1's conn, and the path graph names both proxies in dial order.
func TestChainMemberExpansionExactHopsAndPath(t *testing.T) {
	pool, in := poolOnMembers("in", "pp", []string{"c1"}, "roundrobin", nil)
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			memberOutbound("h1", "testdirect", "127.0.0.1:9101"),
			memberOutbound("h2", "testchain", "127.0.0.1:9102"),
		},
		[]*config.Pool{pool},
		[]*config.ChainSpec{{ID: "c1", Layers: []string{"h1", "h2"}}},
		[]*config.Inbound{in},
	)
	t.Cleanup(r.Close)

	var capturedVia net.Conn
	h2 := unwrap(t, r, "h2").(*fake.ChainFakeOutbound)
	h2.OnDialThrough = func(_ core.Network, _ string, via net.Conn) (net.Conn, error) {
		capturedVia = via
		a, b := net.Pipe()
		go a.Close()
		return b, nil
	}

	meta := metaFor("in", "example.com", "80")
	meta.Path = core.NewPathRecorder()
	conn, chain, err := r.Dial(context.Background(), meta)
	require.NoError(t, err)
	defer conn.Close()
	assert.Equal(t, []string{"h1", "h2"}, chain, "Chain must be the real hop IDs, not the member chain ID")
	assert.Equal(t, 1, h2.DialThroughCount, "exit hop must be dialed through the entry hop's conn")
	require.NotNil(t, capturedVia)

	g := meta.Path.Snapshot()
	var nodeIDs []string
	for _, n := range g.Nodes {
		nodeIDs = append(nodeIDs, n.ID)
	}
	assert.Equal(t, []string{"local", "proxy:h1", "proxy:h2", "target"}, nodeIDs)
	legs := map[[2]string]bool{}
	for _, e := range g.Edges {
		legs[[2]string{e.From, e.To}] = true
	}
	assert.True(t, legs[[2]string{"local", "proxy:h1"}], "local dials the entry hop")
	assert.True(t, legs[[2]string{"proxy:h1", "proxy:h2"}], "entry hop tunnels to the exit hop")
	assert.True(t, legs[[2]string{"proxy:h2", "target"}], "exit hop tunnels to the target")
}

// TestChainMemberFirstHopVerdictRevival pins the observe attribution: a
// successful request row keyed on the real first hop revives a stale failed
// probe verdict for that hop, and no verdict ever lands on the member ID.
func TestChainMemberFirstHopVerdictRevival(t *testing.T) {
	pool, in := poolOnMembers("in", "pp", []string{"c1"}, "roundrobin", nil)
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			memberOutbound("h1", "testdirect", "127.0.0.1:9101"),
			memberOutbound("h2", "testchain", "127.0.0.1:9102"),
		},
		[]*config.Pool{pool},
		[]*config.ChainSpec{{ID: "c1", Layers: []string{"h1", "h2"}}},
		[]*config.Inbound{in},
	)
	t.Cleanup(r.Close)

	// A stale failed probe drops the member: its ENTRY hop is unreachable.
	seedProbeVerdict(t, r.metrics, "h1", false, time.Now().Add(-time.Hour))
	_, _, err := r.Dial(context.Background(), metaFor("in", "example.com", "80"))
	require.ErrorContains(t, err, "no reachable outbound")

	// A successful request row (real hop IDs, as dispatch returns them)
	// revives the entry hop's verdict.
	now := time.Now()
	require.NoError(t, r.metrics.Record(core.RequestMetric{
		Chain: []string{"h1", "h2"}, Success: true, StartedAt: now, FinishedAt: now,
	}))
	ok, _, measured := r.metrics.Reachability("h1")
	require.True(t, measured)
	require.True(t, ok, "success request row must revive the entry hop")

	conn, chain, err := r.Dial(context.Background(), metaFor("in", "example.com", "80"))
	require.NoError(t, err)
	conn.Close()
	assert.Equal(t, []string{"h1", "h2"}, chain)

	// The row the listener records for this request attributes to h1 — the
	// member ID itself is never measured.
	meta := metaFor("in", "example.com", "80")
	meta.Chain = chain
	r.record(meta, chain, true, nil, time.Millisecond)
	_, _, memberMeasured := r.metrics.Reachability("c1")
	assert.False(t, memberMeasured, "no verdict may land on the selection-only member ID")
}

// TestChainMemberEntryReachabilityDropsDeadEntry verifies narrowing keys on
// the ENTRY hop: a member whose entry server is locally unreachable drops
// out of selection (live members keep serving), while a failed verdict on a
// LATER hop does not drop the member — that hop rides the tunnel.
func TestChainMemberEntryReachabilityDropsDeadEntry(t *testing.T) {
	pool, in := poolOnMembers("in", "pp", []string{"dead", "live"}, "roundrobin", nil)
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			memberOutbound("d1", "testdirect", "127.0.0.1:9201"),
			memberOutbound("d2", "testchain", "127.0.0.1:9202"),
			memberOutbound("l1", "testdirect", "127.0.0.1:9203"),
			memberOutbound("l2", "testchain", "127.0.0.1:9204"),
		},
		[]*config.Pool{pool},
		[]*config.ChainSpec{
			{ID: "dead", Layers: []string{"d1", "d2"}},
			{ID: "live", Layers: []string{"l1", "l2"}},
		},
		[]*config.Inbound{in},
	)
	t.Cleanup(r.Close)

	seedProbeVerdict(t, r.metrics, "d1", false, time.Now())
	for i := 0; i < 4; i++ {
		conn, chain, err := r.Dial(context.Background(), metaFor("in", "example.com", "80"))
		require.NoError(t, err)
		conn.Close()
		assert.Equal(t, []string{"l1", "l2"}, chain, "dead-entry member must drop out of selection")
	}

	// A failed verdict on the EXIT hop of the live member changes nothing:
	// only the entry hop rides the local network.
	seedProbeVerdict(t, r.metrics, "l2", false, time.Now())
	conn, chain, err := r.Dial(context.Background(), metaFor("in", "example.com", "80"))
	require.NoError(t, err)
	conn.Close()
	assert.Equal(t, []string{"l1", "l2"}, chain)

	// With every member's entry dead, dispatch fails before any dial.
	seedProbeVerdict(t, r.metrics, "l1", false, time.Now())
	_, _, err = r.Dial(context.Background(), metaFor("in", "example.com", "80"))
	require.ErrorContains(t, err, "no reachable outbound")
}

// TestChainMemberExitDelegationFilters verifies egress filters see the EXIT
// hop's attributes: a protocol filter matches the member by its last hop's
// protocol, not its entry's.
func TestChainMemberExitDelegationFilters(t *testing.T) {
	outs := []*config.OutboundSpec{
		memberOutbound("h1", "testdirect", "127.0.0.1:9301"),
		memberOutbound("h2", "testchain", "127.0.0.1:9302"),
	}
	chains := []*config.ChainSpec{{ID: "c1", Layers: []string{"h1", "h2"}}}

	allowPool, allowIn := poolOnMembers("in", "pp", []string{"c1"}, "roundrobin",
		[]config.FilterSpec{{Type: "protocol", Params: map[string]any{"allow": []any{"testchain"}}}})
	r, _ := testRouter(t, outs, []*config.Pool{allowPool}, chains, []*config.Inbound{allowIn})
	t.Cleanup(r.Close)
	conn, chain, err := r.Dial(context.Background(), metaFor("in", "example.com", "80"))
	require.NoError(t, err, "exit protocol testchain must satisfy the allow filter")
	conn.Close()
	assert.Equal(t, []string{"h1", "h2"}, chain)

	// The token delegates Protocol/Address to the exit hop.
	r.mu.RLock()
	token := r.pools["pp"].outbounds[0]
	r.mu.RUnlock()
	require.Equal(t, "c1", token.ID())
	assert.Equal(t, "testchain", token.Protocol())
	assert.Equal(t, "127.0.0.1:9302", token.Address())

	denyPool, denyIn := poolOnMembers("in", "pp", []string{"c1"}, "roundrobin",
		[]config.FilterSpec{{Type: "protocol", Params: map[string]any{"allow": []any{"testdirect"}}}})
	r2, _ := testRouter(t, outs, []*config.Pool{denyPool}, chains, []*config.Inbound{denyIn})
	t.Cleanup(r2.Close)
	_, _, err = r2.Dial(context.Background(), metaFor("in", "example.com", "80"))
	require.ErrorContains(t, err, "no outbound passed filters", "entry protocol must NOT satisfy an egress filter")
}

// TestChainMemberSelectors runs all four selection strategies over a pool of
// two chain members: each picks whole members, and expansion always yields
// the picked member's exact hop list.
func TestChainMemberSelectors(t *testing.T) {
	outs := []*config.OutboundSpec{
		memberOutbound("a1", "testchain", "127.0.0.1:9401"),
		memberOutbound("a2", "testchain", "127.0.0.1:9402"),
		memberOutbound("b1", "testchain", "127.0.0.1:9403"),
		memberOutbound("b2", "testchain", "127.0.0.1:9404"),
	}
	chains := []*config.ChainSpec{
		{ID: "ca", Layers: []string{"a1", "a2"}},
		{ID: "cb", Layers: []string{"b1", "b2"}},
	}
	ca, cb := []string{"a1", "a2"}, []string{"b1", "b2"}

	t.Run("roundrobin", func(t *testing.T) {
		pool, in := poolOnMembers("in", "pp", []string{"ca", "cb"}, "roundrobin", nil)
		r, _ := testRouter(t, outs, []*config.Pool{pool}, chains, []*config.Inbound{in})
		t.Cleanup(r.Close)
		want := [][]string{ca, cb, ca, cb}
		for i, w := range want {
			conn, chain, err := r.Dial(context.Background(), metaFor("in", "example.com", "80"))
			require.NoError(t, err)
			conn.Close()
			assert.Equal(t, w, chain, "dial %d", i)
		}
	})

	t.Run("random", func(t *testing.T) {
		pool, in := poolOnMembers("in", "pp", []string{"ca", "cb"}, "random", nil)
		r, _ := testRouter(t, outs, []*config.Pool{pool}, chains, []*config.Inbound{in})
		t.Cleanup(r.Close)
		for i := 0; i < 4; i++ {
			conn, chain, err := r.Dial(context.Background(), metaFor("in", "example.com", "80"))
			require.NoError(t, err)
			conn.Close()
			assert.Contains(t, [][]string{ca, cb}, chain)
		}
	})

	t.Run("sticky", func(t *testing.T) {
		pool, in := poolOnMembers("in", "pp", []string{"ca", "cb"}, "sticky", nil)
		r, _ := testRouter(t, outs, []*config.Pool{pool}, chains, []*config.Inbound{in})
		t.Cleanup(r.Close)
		var first []string
		for i := 0; i < 3; i++ {
			conn, chain, err := r.Dial(context.Background(), metaFor("in", "example.com", "80"))
			require.NoError(t, err)
			conn.Close()
			if i == 0 {
				first = chain
			} else {
				assert.Equal(t, first, chain, "same domain must pin the same member")
			}
		}
	})

	t.Run("leastlatency", func(t *testing.T) {
		pool, in := poolOnMembers("in", "pp", []string{"ca", "cb"}, "leastlatency", nil)
		r, _ := testRouter(t, outs, []*config.Pool{pool}, chains, []*config.Inbound{in})
		t.Cleanup(r.Close)
		// Shape the hops' live stats: ca's hops are fast (sum 20ms), cb's
		// slow (sum 1s). The member aggregate is the sum, so ca wins.
		now := time.Now()
		for id, lat := range map[string]time.Duration{
			"a1": 10 * time.Millisecond, "a2": 10 * time.Millisecond,
			"b1": 500 * time.Millisecond, "b2": 500 * time.Millisecond,
		} {
			require.NoError(t, r.metrics.Record(core.RequestMetric{
				Chain: []string{id}, Success: true, Latency: lat, StartedAt: now, FinishedAt: now,
			}))
		}
		for i := 0; i < 3; i++ {
			conn, chain, err := r.Dial(context.Background(), metaFor("in", "example.com", "80"))
			require.NoError(t, err)
			conn.Close()
			assert.Equal(t, ca, chain, "leastlatency must pick the faster aggregate path")
		}
	})
}

// TestChainMemberMixedPool verifies OutboundIDs and ChainIDs coexist: plain
// members dial as single hops, chain members expand, and the selector
// rotates across both kinds in config order.
func TestChainMemberMixedPool(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			memberOutbound("solo", "testdirect", "127.0.0.1:9501"),
			memberOutbound("h1", "testdirect", "127.0.0.1:9502"),
			memberOutbound("h2", "testchain", "127.0.0.1:9503"),
		},
		[]*config.Pool{{
			ID: "pp", OutboundIDs: []string{"solo"}, ChainIDs: []string{"c1"},
			Selector: config.SelectorSpec{Type: "roundrobin"},
		}},
		[]*config.ChainSpec{{ID: "c1", Layers: []string{"h1", "h2"}}},
		[]*config.Inbound{{ID: "in", Protocol: "http", Listen: "127.0.0.1:0", Policy: config.InboundPolicy{PoolID: "pp"}}},
	)
	t.Cleanup(r.Close)
	want := [][]string{{"solo"}, {"h1", "h2"}, {"solo"}, {"h1", "h2"}}
	for i, w := range want {
		conn, chain, err := r.Dial(context.Background(), metaFor("in", "example.com", "80"))
		require.NoError(t, err)
		conn.Close()
		assert.Equal(t, w, chain, "dial %d", i)
	}
}

// TestChainMemberForbiddenPathPoolChainSkippedWhole is the rebuild backstop
// for rule 2: a chain whose layer references a path-pool is skipped WHOLE
// (never silently shortened), while valid chains keep building.
func TestChainMemberForbiddenPathPoolChainSkippedWhole(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			memberOutbound("h1", "testdirect", "127.0.0.1:9601"),
			memberOutbound("h2", "testchain", "127.0.0.1:9602"),
		},
		[]*config.Pool{{ID: "pp", ChainIDs: []string{"good"}, Selector: config.SelectorSpec{Type: "roundrobin"}}},
		[]*config.ChainSpec{
			{ID: "good", Layers: []string{"h1", "h2"}},
			{ID: "bad", Layers: []string{"h1", "pp"}},
			{ID: "badfirst", Layers: []string{"pp"}},
		},
		nil,
	)
	t.Cleanup(r.Close)
	r.mu.RLock()
	_, badExists := r.chains["bad"]
	_, badFirstExists := r.chains["badfirst"]
	good := r.chains["good"]
	r.mu.RUnlock()
	assert.False(t, badExists, "chain traversing a path-pool must be skipped whole")
	assert.False(t, badFirstExists)
	assert.Len(t, good, 2, "valid chains keep building")
}

// TestChainMemberIneligibleSkipped verifies the member-attach pass resolves
// each member chain from its config spec against built outbounds directly:
// a missing chain, a pool-layer chain, or a chain with an unbuilt layer
// (which the built-chain pass would silently SHORTEN) is skipped entirely —
// only the fully eligible member attaches.
func TestChainMemberIneligibleSkipped(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			memberOutbound("h1", "testdirect", "127.0.0.1:9701"),
			memberOutbound("h2", "testchain", "127.0.0.1:9702"),
			memberOutbound("inner", "testdirect", "127.0.0.1:9703"),
		},
		[]*config.Pool{
			{ID: "innerpool", OutboundIDs: []string{"inner"}},
			{ID: "pp", ChainIDs: []string{"missing", "poolshadow", "shortened", "good"}, Selector: config.SelectorSpec{Type: "roundrobin"}},
		},
		[]*config.ChainSpec{
			{ID: "poolshadow", Layers: []string{"innerpool"}},
			{ID: "shortened", Layers: []string{"h1", "ghost"}},
			{ID: "good", Layers: []string{"h1", "h2"}},
		},
		[]*config.Inbound{{ID: "in", Protocol: "http", Listen: "127.0.0.1:0", Policy: config.InboundPolicy{PoolID: "pp"}}},
	)
	t.Cleanup(r.Close)

	r.mu.RLock()
	members := r.pools["pp"].outbounds
	r.mu.RUnlock()
	require.Len(t, members, 1, "only the fully eligible member may attach")
	assert.Equal(t, "good", members[0].ID())

	for i := 0; i < 3; i++ {
		conn, chain, err := r.Dial(context.Background(), metaFor("in", "example.com", "80"))
		require.NoError(t, err)
		conn.Close()
		assert.Equal(t, []string{"h1", "h2"}, chain, "a shortened member must never expand to a partial path")
	}
}

// TestChainMemberSpliceBudget verifies the expansion bound: a member whose
// hops smuggle in another member (a cyclic reference past validation) fails
// with a diagnostic instead of looping.
func TestChainMemberSpliceBudget(t *testing.T) {
	r, _ := testRouter(t, nil, nil, nil, nil)
	t.Cleanup(r.Close)
	cm := &chainMember{id: "loop", firstHopID: "loop", ms: r.metrics}
	cm.hops = []core.Outbound{cm} // expands into itself
	layers := []snapshottedLayer{{
		selector: &poolSelector{Selector: &fixedSelector{out: cm}, pool: []core.Outbound{cm}},
		pool:     []core.Outbound{cm},
	}}
	_, _, err := r.dialLayersExpanding(context.Background(), layers, metaFor("in", "example.com", "80"))
	require.ErrorContains(t, err, "cyclic pool/chain reference?")
}

// TestChainMemberTokenIsSelectionOnly pins the facade's safety properties:
// dialing it directly is a loud error, and it exposes no Unwrap — chainDialer
// must never find a ChainDialer on the token.
func TestChainMemberTokenIsSelectionOnly(t *testing.T) {
	cm := &chainMember{id: "c1"}
	_, err := cm.DialContext(context.Background(), core.NetworkTCP, "example.com:80")
	require.ErrorContains(t, err, "must be expanded, not dialed")
	_, unwrappable := any(cm).(interface{ Unwrap() core.Outbound })
	assert.False(t, unwrappable, "the token must not expose Unwrap")
	_, chainable := any(cm).(ChainDialer)
	assert.False(t, chainable)
}

// TestChainMemberStatsAggregate pins the bias-fixed path-cost aggregate:
// latency sums with unmeasured hops at 1s (all-unmeasured reads 0), the
// success rate is the minimum over MEASURED hops only (a measured zero is a
// real zero, an unmeasured hop is skipped), LastSeen is the newest success.
func TestChainMemberStatsAggregate(t *testing.T) {
	ms, err := metrics.Open(t.TempDir() + "/m.db")
	require.NoError(t, err)
	t.Cleanup(func() { ms.Close() })
	h1 := &fake.FakeOutbound{IDVal: "s1"}
	h2 := &fake.FakeOutbound{IDVal: "s2"}
	cm := &chainMember{id: "c", hops: []core.Outbound{h1, h2}, ms: ms}

	// All unmeasured: reads as an unmeasured outbound (0/0), so member order
	// breaks ties instead of a fabricated 2s cost.
	st := cm.Stats()
	assert.Zero(t, st.RecentLatency)
	assert.Zero(t, st.SuccessRate)
	assert.True(t, st.LastSeen.IsZero())

	// One measured hop: its latency plus the 1s unmeasured default; the
	// success rate considers only the measured hop (fail-open on the other).
	t1 := time.Now()
	require.NoError(t, ms.Record(core.RequestMetric{
		Chain: []string{"s1"}, Success: true, Latency: 100 * time.Millisecond, StartedAt: t1, FinishedAt: t1,
	}))
	st = cm.Stats()
	assert.Equal(t, 100*time.Millisecond+time.Second, st.RecentLatency)
	assert.Equal(t, 1.0, st.SuccessRate)
	assert.Equal(t, t1, st.LastSeen)

	// A measured ZERO (samples, no successes) is a real zero — the weakest
	// measured link dominates, unlike an unmeasured hop which is skipped.
	require.NoError(t, ms.Record(core.RequestMetric{
		Chain: []string{"s2"}, Success: false, Latency: 50 * time.Millisecond, StartedAt: t1, FinishedAt: t1,
	}))
	st = cm.Stats()
	assert.Equal(t, 150*time.Millisecond, st.RecentLatency)
	assert.Zero(t, st.SuccessRate, "measured zero success must not fail open")
	assert.Equal(t, t1, st.LastSeen)

	// LastSeen is the newest success across hops.
	t2 := t1.Add(time.Minute)
	require.NoError(t, ms.Record(core.RequestMetric{
		Chain: []string{"s2"}, Success: true, Latency: 50 * time.Millisecond, StartedAt: t2, FinishedAt: t2,
	}))
	st = cm.Stats()
	assert.Equal(t, t2, st.LastSeen)
	assert.Equal(t, 0.5, st.SuccessRate, "min over measured hops: s2 now 1/2")
}

// TestUpstreamChainMemberDialsWholePath verifies the upstream seam expands a
// picked member into its pinned hops (no selection, no per-hop failover) and
// names the member on the attempt node.
func TestUpstreamChainMemberDialsWholePath(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			memberOutbound("h1", "testdirect", "127.0.0.1:9801"),
			memberOutbound("h2", "testchain", "127.0.0.1:9802"),
		},
		[]*config.Pool{{ID: "pp", ChainIDs: []string{"c1"}, Selector: config.SelectorSpec{Type: "roundrobin"}}},
		[]*config.ChainSpec{{ID: "c1", Layers: []string{"h1", "h2"}}},
		nil,
	)
	t.Cleanup(r.Close)
	conn := dialViaRoute(t, r, "pool:pp", "example.com:443")
	conn.Close()
	assert.Equal(t, 1, unwrap(t, r, "h2").(*fake.ChainFakeOutbound).DialThroughCount)
}

// TestUpstreamChainMemberFailover verifies pool-route failover across chain
// members: a member whose entry dial fails is dropped BY ID and the next
// member serves the fetch.
func TestUpstreamChainMemberFailover(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			memberOutbound("a1", "testdirect", "127.0.0.1:9811"),
			memberOutbound("a2", "testchain", "127.0.0.1:9812"),
			memberOutbound("b1", "testdirect", "127.0.0.1:9813"),
			memberOutbound("b2", "testchain", "127.0.0.1:9814"),
		},
		[]*config.Pool{{ID: "pp", ChainIDs: []string{"ca", "cb"}, Selector: config.SelectorSpec{Type: "roundrobin"}}},
		[]*config.ChainSpec{
			{ID: "ca", Layers: []string{"a1", "a2"}},
			{ID: "cb", Layers: []string{"b1", "b2"}},
		},
		nil,
	)
	t.Cleanup(r.Close)
	fakeOutbound(t, r, "a1").DialErr = errors.New("entry down")
	conn := dialViaRoute(t, r, "pool:pp", "example.com:443")
	conn.Close()
	assert.Equal(t, 1, unwrap(t, r, "b2").(*fake.ChainFakeOutbound).DialThroughCount, "failover must land on the second member's full path")
}

// TestUpstreamStaleStickyPickNormalized is the defensive-insurance
// regression: a sticky selector's cache can hand back a pointer from an
// earlier rebuild epoch. The pick must be normalized to the snapshot's live
// instance BY ID (the stale wrapper is never dialed), and failover removal
// must also key on ID so the loop terminates.
func TestUpstreamStaleStickyPickNormalized(t *testing.T) {
	r, cfg := testRouter(t,
		[]*config.OutboundSpec{
			memberOutbound("m1", "testdirect", "127.0.0.1:9901"),
			memberOutbound("m2", "testdirect", "127.0.0.1:9902"),
		},
		[]*config.Pool{{ID: "pp", OutboundIDs: []string{"m1", "m2"}, Selector: config.SelectorSpec{Type: "sticky"}}},
		nil, nil,
	)
	t.Cleanup(r.Close)

	meta := metaFor("", "example.com", "443")
	snap1, ok := r.snapshotPool("pp")
	require.True(t, ok)
	// Prime the sticky cache with an epoch-1 pointer.
	conn, err := r.dialPoolSnapshot(context.Background(), "pool pp", snap1, meta)
	require.NoError(t, err)
	conn.Close()
	old1, old2 := fakeOutbound(t, r, "m1"), fakeOutbound(t, r, "m2")
	cachedOld := old1
	if old1.DialCount == 0 {
		cachedOld = old2
	}
	require.Equal(t, 1, cachedOld.DialCount)

	// Rebuild: fresh wrappers, but keep the OLD selector (its cache holds the
	// epoch-1 pointer) paired with the NEW candidates — the stale-pick shape.
	cfg.SetPool(&config.Pool{ID: "pp", OutboundIDs: []string{"m1", "m2"}, Selector: config.SelectorSpec{Type: "sticky"}})
	r.Rebuild()
	snap2, ok := r.snapshotPool("pp")
	require.True(t, ok)
	stale := poolSnapshot{candidates: snap2.candidates, filters: snap2.filters, selector: snap1.selector}

	// The cached member's LIVE instance is down: normalization must dial it
	// (and fail), ID-removal must drop it, and the other member must win.
	new1, new2 := fakeOutbound(t, r, "m1"), fakeOutbound(t, r, "m2")
	cachedNew, otherNew := new1, new2
	if cachedOld == old2 {
		cachedNew, otherNew = new2, new1
	}
	cachedNew.DialErr = errors.New("live instance down")
	cachedOld.DialErr = errors.New("stale epoch must never be dialed")

	conn, err = r.dialPoolSnapshot(context.Background(), "pool pp", stale, meta)
	require.NoError(t, err, "stale pick must normalize and fail over by ID")
	conn.Close()
	assert.Equal(t, 1, cachedNew.DialCount, "the live instance of the cached ID must be the one dialed")
	assert.Equal(t, 1, otherNew.DialCount, "failover must land on the other member")
	assert.Equal(t, 1, cachedOld.DialCount, "the stale-epoch wrapper must never be dialed again")
}

// TestWithoutOutboundByID pins the ID-based failover removal directly: a
// stale-epoch pointer with a live ID removes the live member.
func TestWithoutOutboundByID(t *testing.T) {
	live := &fake.FakeOutbound{IDVal: "m1"}
	stale := &fake.FakeOutbound{IDVal: "m1"}
	got := withoutOutbound([]core.Outbound{live}, stale)
	assert.Empty(t, got, "removal must key on ID, not pointer identity")
}

// TestChainMemberRebuildDuringDial exercises the snapshot discipline under
// -race: a dial holding pre-pinned member hops must survive concurrent
// rebuilds replacing every map wholesale.
func TestChainMemberRebuildDuringDial(t *testing.T) {
	pool, in := poolOnMembers("in", "pp", []string{"c1"}, "roundrobin", nil)
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			memberOutbound("h1", "testdirect", "127.0.0.1:9911"),
			memberOutbound("h2", "testchain", "127.0.0.1:9912"),
		},
		[]*config.Pool{pool},
		[]*config.ChainSpec{{ID: "c1", Layers: []string{"h1", "h2"}}},
		[]*config.Inbound{in},
	)
	t.Cleanup(r.Close)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				r.Rebuild()
			}
		}
	}()
	for i := 0; i < 20; i++ {
		conn, chain, err := r.Dial(context.Background(), metaFor("in", "example.com", "80"))
		require.NoError(t, err)
		conn.Close()
		assert.Equal(t, []string{"h1", "h2"}, chain)
	}
	close(stop)
	wg.Wait()
}
