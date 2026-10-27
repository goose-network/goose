// Reachability tests: probes measure which proxy servers the local network
// can dial, record the verdicts as outbound metric rows (chain
// [core.HopLocal, id]), and routing filters a pool's first-hop candidates by
// those metrics. A server the local network cannot reach is dropped from
// first-hop selection; reaching it is explicit config — a chain layer (or an
// outbound's upstream_proxy) — exactly the model the config diagram shows:
// flat outbounds, pools and chains as views over them, metrics as the
// measured truth.
//
// The router-package tests simulate the "local ISP blocks some proxy
// servers" scenario with a blocking stack: the engine's stack refuses the
// blocked addresses, while a relay SOCKS5 server (whose handler dials with
// the plain system dialer) can still reach them.
package router

import (
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/core"
	"github.com/goose-network/goose/internal/inbound"
	"github.com/goose-network/goose/internal/metrics"
	"github.com/goose-network/goose/internal/stack"
	_ "github.com/goose-network/goose/plugins/socks5" // real SOCKS5 outbound plugin
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// blockingStack wraps the system stack and refuses a configured set of
// addresses, simulating an ISP that blocks those proxy servers.
type blockingStack struct {
	stack.Stack
	blocked map[string]bool
}

func (b *blockingStack) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	if b.blocked[address] {
		return nil, errors.New("blocked by local network (test)")
	}
	return b.Stack.Dial(ctx, network, address)
}

// directHandler is an inbound Handler that dials every target directly with
// the plain system dialer — a proxy server with unrestricted egress. Used
// for the relay and work SOCKS5 servers in these tests.
type directHandler struct{}

func (directHandler) Dial(ctx context.Context, meta core.Metadata) (net.Conn, []string, error) {
	var d net.Dialer
	c, err := d.DialContext(ctx, "tcp", meta.Target())
	return c, nil, err
}

func (directHandler) Bridge(in, up net.Conn, _ core.Metadata) {
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(up, in); done <- struct{}{} }()
	go func() { _, _ = io.Copy(in, up); done <- struct{}{} }()
	<-done
	_ = in.Close()
	_ = up.Close()
}

// echoServer is a TCP server that echoes bytes back, with a counter of
// connections it served.
type echoServer struct {
	ln    net.Listener
	conns int
	mu    sync.Mutex
}

func startEcho(t *testing.T) *echoServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	es := &echoServer{ln: ln}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			es.mu.Lock()
			es.conns++
			es.mu.Unlock()
			go func(c net.Conn) {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}(c)
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return es
}

// socks5Server starts a real SOCKS5 server (the engine's own inbound
// listener) whose handler dials targets directly.
func socks5Server(t *testing.T) string {
	t.Helper()
	ln, err := inbound.New("test-relay-"+strconv.Itoa(time.Now().Nanosecond()), "socks5", "127.0.0.1:0", inbound.NewAuthStore(nil), func(string) string { return "" }, directHandler{})
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })
	return ln.Address()
}

// reachFixture assembles the blocked-work-proxy topology:
//
//	echo E  <-  work X (socks5 server)  <-  relay R (socks5 server)  <-  engine
//
// The engine's stack blocks X's address when blocked=true, so X is reachable
// only through R — via an explicit chain, the way the config model expresses
// it now that no hidden access pool exists.
type reachFixture struct {
	echo   *echoServer
	router *Router
	cfg    *config.Store
	ms     *metrics.Store
	xAddr  string
	rAddr  string
}

// newReachFixture builds the fixture with probes enabled. blocked swaps in
// the blocking stack for the work server's address.
func newReachFixture(t *testing.T, blocked bool) *reachFixture {
	t.Helper()
	registerTestPlugins()
	es := startEcho(t)
	xAddr := socks5Server(t)
	rAddr := socks5Server(t)

	stk, err := stack.New("system")
	require.NoError(t, err)
	t.Cleanup(func() { stk.Close() })
	var dialStack stack.Stack = stk
	if blocked {
		dialStack = &blockingStack{Stack: stk, blocked: map[string]bool{xAddr: true}}
	}

	cfg := config.NewStore()
	eng := cfg.Engine()
	eng.Probes = config.ProbeConfig{Enabled: true, IntervalSec: 10, TimeoutSec: 2}
	cfg.SetEngine(eng)
	cfg.SetOutbound(&config.OutboundSpec{ID: "relay", Protocol: "socks5", Config: map[string]any{"address": rAddr}})
	cfg.SetOutbound(&config.OutboundSpec{ID: "work", Protocol: "socks5", Config: map[string]any{"address": xAddr}})
	cfg.SetPool(&config.Pool{ID: "relays", OutboundIDs: []string{"relay"}, Selector: config.SelectorSpec{Type: "random"}})
	cfg.SetPool(&config.Pool{ID: "work", OutboundIDs: []string{"work"}, Selector: config.SelectorSpec{Type: "random"}})
	// The explicit chain that reaches a locally-blocked server: hop 0 rides
	// the local network, hop 1 rides hop 0's tunnel.
	cfg.SetChain(&config.ChainSpec{ID: "via-relay", Layers: []string{"relay", "work"}})
	cfg.SetInbound(&config.Inbound{
		ID:       "in",
		Protocol: "http",
		Listen:   "127.0.0.1:0",
		Policy:   config.InboundPolicy{PoolID: "work"},
	})
	cfg.SetInbound(&config.Inbound{
		ID:       "inchain",
		Protocol: "http",
		Listen:   "127.0.0.1:0",
		Policy:   config.InboundPolicy{ChainID: "via-relay"},
	})
	ms, err := metrics.Open(t.TempDir() + "/m.db")
	require.NoError(t, err)
	t.Cleanup(func() { ms.Close() })
	r := New(cfg, ms, dialStack)
	t.Cleanup(r.Close)
	return &reachFixture{echo: es, router: r, cfg: cfg, ms: ms, xAddr: xAddr, rAddr: rAddr}
}

// awaitVerdict waits until the metrics store holds a reachability verdict
// for id (a probe row landed).
func (f *reachFixture) awaitVerdict(t *testing.T, id string) (ok bool) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, _, measured := f.ms.Reachability(id)
		return measured
	}, 10*time.Second, 50*time.Millisecond, "reachability verdict for outbound %q never appeared", id)
	ok, _, _ = f.ms.Reachability(id)
	return ok
}

// probeRow finds the durable metric row a probe round recorded for id.
func (f *reachFixture) probeRow(t *testing.T, id string) core.RequestMetric {
	t.Helper()
	var row core.RequestMetric
	require.Eventually(t, func() bool {
		rows, err := f.ms.Recent(100)
		require.NoError(t, err)
		for _, m := range rows {
			if len(m.Chain) == 2 && m.Chain[0] == core.HopLocal && m.Chain[1] == id {
				row = m
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond, "probe metric row for outbound %q never landed", id)
	return row
}

// TestReachabilityProbeRecordsMetricRows verifies a probe round records one
// metric row per outbound with an address: the blocked work server fails,
// the relay succeeds — both visible in the metrics table.
func TestReachabilityProbeRecordsMetricRows(t *testing.T) {
	f := newReachFixture(t, true)

	assert.False(t, f.awaitVerdict(t, "work"), "blocked server should measure unreachable")
	assert.True(t, f.awaitVerdict(t, "relay"), "relay server should measure reachable")

	work := f.probeRow(t, "work")
	assert.False(t, work.Success)
	assert.NotEmpty(t, work.Error, "failed probe row should carry the dial error")
	assert.Equal(t, f.xAddr, work.Target, "probe row target is the outbound's server address")

	relay := f.probeRow(t, "relay")
	assert.True(t, relay.Success)
	assert.Greater(t, relay.Latency, time.Duration(0), "probe row should record a latency")
}

// TestReachabilityDropsBlockedFirstHop verifies the metrics filter: with a
// failed local verdict for the pool's only member, first-hop selection drops
// it and dispatch fails without attempting a doomed dial.
func TestReachabilityDropsBlockedFirstHop(t *testing.T) {
	f := newReachFixture(t, true)
	require.False(t, f.awaitVerdict(t, "work"))

	meta := core.Metadata{
		InboundID:  "in",
		Network:    core.NetworkTCP,
		TargetHost: "127.0.0.1",
		TargetPort: portOf(t, f.echo.ln.Addr().String()),
		StartedAt:  time.Now(),
	}
	_, _, err := f.router.Dial(context.Background(), meta)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no reachable outbound")
}

// TestReachabilityExplicitChainReachesBlocked verifies the config-model
// replacement for the hidden access pool: the blocked server is reached
// through an explicit chain (relay then work), and the failed local verdict
// for work does not drop it from hop 1 — that hop rides hop 0's tunnel, not
// the local network.
func TestReachabilityExplicitChainReachesBlocked(t *testing.T) {
	f := newReachFixture(t, true)
	require.False(t, f.awaitVerdict(t, "work"))

	meta := core.Metadata{
		InboundID:  "inchain",
		Network:    core.NetworkTCP,
		TargetHost: "127.0.0.1",
		TargetPort: portOf(t, f.echo.ln.Addr().String()),
		StartedAt:  time.Now(),
	}
	conn, chain, err := f.router.Dial(context.Background(), meta)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()
	assert.Equal(t, []string{"relay", "work"}, chain)

	// The full path must actually carry bytes.
	msg := []byte("ping-through-explicit-chain")
	_, err = conn.Write(msg)
	require.NoError(t, err)
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, len(msg))
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	assert.Equal(t, msg, buf)
}

// TestReachabilityDirectWhenReachable verifies a measured-reachable member
// is dialed directly (single hop) with probes on.
func TestReachabilityDirectWhenReachable(t *testing.T) {
	f := newReachFixture(t, false) // no blocking: work server directly reachable
	require.True(t, f.awaitVerdict(t, "work"))

	meta := core.Metadata{
		InboundID:  "in",
		Network:    core.NetworkTCP,
		TargetHost: "127.0.0.1",
		TargetPort: portOf(t, f.echo.ln.Addr().String()),
		StartedAt:  time.Now(),
	}
	conn, chain, err := f.router.Dial(context.Background(), meta)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()
	assert.Equal(t, []string{"work"}, chain, "reachable member must be dialed directly")

	msg := []byte("ping-direct")
	_, err = conn.Write(msg)
	require.NoError(t, err)
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, len(msg))
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	assert.Equal(t, msg, buf)
}

// TestReachabilityFailOpenWithoutProbes verifies that with probes disabled
// nothing is filtered by metrics: the blocked member stays a candidate and
// the failure is a dial error, not a reachability rejection.
func TestReachabilityFailOpenWithoutProbes(t *testing.T) {
	registerTestPlugins()
	es := startEcho(t)
	xAddr := socks5Server(t)

	stk, err := stack.New("system")
	require.NoError(t, err)
	t.Cleanup(func() { stk.Close() })
	dialStack := &blockingStack{Stack: stk, blocked: map[string]bool{xAddr: true}}

	cfg := config.NewStore() // Probes zero value: disabled
	cfg.SetOutbound(&config.OutboundSpec{ID: "work", Protocol: "socks5", Config: map[string]any{"address": xAddr}})
	cfg.SetPool(&config.Pool{ID: "work", OutboundIDs: []string{"work"}, Selector: config.SelectorSpec{Type: "random"}})
	cfg.SetInbound(&config.Inbound{
		ID:       "in",
		Protocol: "http",
		Listen:   "127.0.0.1:0",
		Policy:   config.InboundPolicy{PoolID: "work"},
	})
	ms, err := metrics.Open(t.TempDir() + "/m.db")
	require.NoError(t, err)
	t.Cleanup(func() { ms.Close() })
	r := New(cfg, ms, dialStack)
	t.Cleanup(r.Close)

	meta := core.Metadata{
		InboundID:  "in",
		Network:    core.NetworkTCP,
		TargetHost: "127.0.0.1",
		TargetPort: portOf(t, es.ln.Addr().String()),
		StartedAt:  time.Now(),
	}
	_, _, err = r.Dial(context.Background(), meta)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "no reachable outbound", "unmeasured candidates must fail open")
}

// TestReachabilitySuccessRowRevives verifies the metrics filter reads every
// row kind: a successful request whose first hop is a previously-failed
// outbound overwrites the stale failed verdict (real traffic proved the
// server reachable), while a failed request row never marks a server
// unreachable.
func TestReachabilitySuccessRowRevives(t *testing.T) {
	f := newReachFixture(t, true)
	require.False(t, f.awaitVerdict(t, "work"))

	at := time.Now()
	require.NoError(t, f.ms.Record(core.RequestMetric{
		Chain:     []string{"work"},
		Success:   true,
		StartedAt: at,
	}))
	ok, _, measured := f.ms.Reachability("work")
	assert.True(t, measured)
	assert.True(t, ok, "a successful first hop proves local reachability")

	require.NoError(t, f.ms.Record(core.RequestMetric{
		Chain:     []string{"work"},
		Success:   false,
		StartedAt: at.Add(time.Second),
	}))
	ok, _, _ = f.ms.Reachability("work")
	assert.True(t, ok, "a failed request row must not mark the server unreachable")
}

// TestReachabilityProberLifecycle verifies the probe loop follows the engine
// probe config: running while enabled, stopped when disabled.
func TestReachabilityProberLifecycle(t *testing.T) {
	f := newReachFixture(t, true)
	f.awaitVerdict(t, "work") // prober running

	eng := f.cfg.Engine()
	eng.Probes.Enabled = false
	f.cfg.SetEngine(eng)
	f.router.Rebuild()

	f.router.probeCtlMu.Lock()
	ctl := f.router.probeCtl
	f.router.probeCtlMu.Unlock()
	assert.Nil(t, ctl, "probe loop should stop when engine probes are disabled")
}

// TestReachabilityHotReloadAddsMember verifies a newly added outbound is
// probed promptly (the rebuild signals a refresh) instead of waiting a full
// interval.
func TestReachabilityHotReloadAddsMember(t *testing.T) {
	f := newReachFixture(t, true)
	f.awaitVerdict(t, "work")

	x2 := socks5Server(t)
	f.cfg.SetOutbound(&config.OutboundSpec{ID: "work2", Protocol: "socks5", Config: map[string]any{"address": x2}})
	f.router.Rebuild()

	require.Eventually(t, func() bool {
		_, _, measured := f.ms.Reachability("work2")
		return measured
	}, 10*time.Second, 50*time.Millisecond, "newly added outbound should be probed promptly")
	ok, _, _ := f.ms.Reachability("work2")
	assert.True(t, ok, "the new member's server is directly reachable")
}

func portOf(t *testing.T, addr string) string {
	t.Helper()
	_, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	return port
}
