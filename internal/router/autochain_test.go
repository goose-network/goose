// Auto-chain tests: pools with an access filter route members whose servers
// the local network cannot reach through an access proxy (relay hop). The
// router-package tests simulate the "local ISP blocks some proxy servers"
// scenario with a blocking stack: the engine's stack refuses the blocked
// addresses, while the access proxy (a real SOCKS5 server whose handler
// dials with the plain system dialer) can still reach them — exactly the
// topology the feature targets.
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

// autoChainFixture assembles the blocked-work-proxy topology:
//
//	echo E  <-  work X (socks5 server)  <-  relay R (socks5 server)  <-  engine
//
// The engine's stack blocks X's address, so X is reachable only through R.
type autoChainFixture struct {
	echo   *echoServer
	router *Router
	cfg    *config.Store
	xAddr  string
	rAddr  string
}

// newAutoChain builds the fixture with a real system stack (X reachable
// directly). blocked=true swaps in the blocking stack.
func newAutoChain(t *testing.T, blocked bool) *autoChainFixture {
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
	cfg.SetOutbound(&config.OutboundSpec{ID: "relay", Protocol: "socks5", Config: map[string]any{"address": rAddr}})
	cfg.SetOutbound(&config.OutboundSpec{ID: "work", Protocol: "socks5", Config: map[string]any{"address": xAddr}})
	cfg.SetPool(&config.Pool{ID: "access", OutboundIDs: []string{"relay"}, Selector: config.SelectorSpec{Type: "random"}})
	cfg.SetPool(&config.Pool{
		ID:          "work",
		OutboundIDs: []string{"work"},
		Selector:    config.SelectorSpec{Type: "random"},
		Access:      &config.AccessFilterSpec{AccessPoolID: "access", ProbeIntervalSec: 10, ProbeTimeoutSec: 2},
	})
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
	return &autoChainFixture{echo: es, router: r, cfg: cfg, xAddr: xAddr, rAddr: rAddr}
}

// awaitProbe waits until the prober has a verdict for the work outbound.
func (f *autoChainFixture) awaitProbe(t *testing.T) core.ProbeStatus {
	t.Helper()
	var got core.ProbeStatus
	require.Eventually(t, func() bool {
		for _, st := range f.router.ChainProbeStatus() {
			if st.OutboundID == "work" {
				got = st
				return true
			}
		}
		return false
	}, 10*time.Second, 50*time.Millisecond, "probe result for outbound %q never appeared", "work")
	return got
}

// TestAutoChainRelaysBlockedProxy verifies the core scenario: the work
// proxy's server is unreachable from the engine's local network, so the
// router prepends the probed access proxy and the request traverses
// engine -> relay -> work -> target.
func TestAutoChainRelaysBlockedProxy(t *testing.T) {
	f := newAutoChain(t, true)

	st := f.awaitProbe(t)
	assert.False(t, st.Direct.OK, "direct probe should fail (stack blocks the work server)")
	assert.True(t, st.Relay.OK, "relay probe should succeed through the access proxy")
	assert.Equal(t, "relay", st.RelayVia)

	// A request through the work pool must be relayed: chain [relay, work].
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
	assert.Equal(t, []string{"relay", "work"}, chain, "blocked member must be dialed through the access proxy")

	// The full path must actually carry bytes: write through the tunnel and
	// read the echo back.
	msg := []byte("ping-through-auto-chain")
	_, err = conn.Write(msg)
	require.NoError(t, err)
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, len(msg))
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	assert.Equal(t, msg, buf)
}

// TestAutoChainDirectWhenReachable verifies reachable members are dialed
// directly (no relay hop) even though their pool has an access filter.
func TestAutoChainDirectWhenReachable(t *testing.T) {
	f := newAutoChain(t, false) // no blocking: work server directly reachable

	st := f.awaitProbe(t)
	assert.True(t, st.Direct.OK, "direct probe should succeed")
	assert.Equal(t, "", st.RelayVia, "no relay should be recorded for a directly reachable member")

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

// TestAutoChainDropsUnreachableMember verifies a member that is reachable
// neither directly nor through the access pool is dropped: dispatch fails
// with the "no reachable outbound" error instead of attempting a doomed dial.
func TestAutoChainDropsUnreachableMember(t *testing.T) {
	registerTestPlugins()
	es := startEcho(t)

	stk, err := stack.New("system")
	require.NoError(t, err)
	t.Cleanup(func() { stk.Close() })
	// Block BOTH the work server and the relay server: nothing works.
	dialStack := &blockingStack{Stack: stk, blocked: map[string]bool{
		"127.0.0.1:1": true, // the work outbound's (dead) address
		"127.0.0.1:2": true, // hmm: also dead addresses fail on their own
	}}
	_ = dialStack

	// Simpler and more honest: work outbound points at a closed port, relay
	// at a live SOCKS5 server that also cannot reach the closed port.
	rAddr := socks5Server(t)
	cfg := config.NewStore()
	cfg.SetOutbound(&config.OutboundSpec{ID: "relay", Protocol: "socks5", Config: map[string]any{"address": rAddr}})
	cfg.SetOutbound(&config.OutboundSpec{ID: "dead", Protocol: "socks5", Config: map[string]any{"address": "127.0.0.1:1"}})
	cfg.SetPool(&config.Pool{ID: "access", OutboundIDs: []string{"relay"}, Selector: config.SelectorSpec{Type: "random"}})
	cfg.SetPool(&config.Pool{
		ID:          "work",
		OutboundIDs: []string{"dead"},
		Selector:    config.SelectorSpec{Type: "random"},
		Access:      &config.AccessFilterSpec{AccessPoolID: "access", ProbeIntervalSec: 10, ProbeTimeoutSec: 1},
	})
	cfg.SetInbound(&config.Inbound{
		ID:       "in",
		Protocol: "http",
		Listen:   "127.0.0.1:0",
		Policy:   config.InboundPolicy{PoolID: "work"},
	})
	ms, err := metrics.Open(t.TempDir() + "/m.db")
	require.NoError(t, err)
	t.Cleanup(func() { ms.Close() })
	r := New(cfg, ms, stk)
	t.Cleanup(r.Close)

	// Wait for the probe verdict: both paths fail for the dead member.
	require.Eventually(t, func() bool {
		for _, st := range r.ChainProbeStatus() {
			if st.OutboundID == "dead" && !st.Direct.OK && !st.Relay.OK {
				return true
			}
		}
		return false
	}, 10*time.Second, 50*time.Millisecond)

	meta := core.Metadata{
		InboundID:  "in",
		Network:    core.NetworkTCP,
		TargetHost: "127.0.0.1",
		TargetPort: portOf(t, es.ln.Addr().String()),
		StartedAt:  time.Now(),
	}
	_, _, err = r.Dial(context.Background(), meta)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no reachable outbound")
}

// TestAutoChainProberLifecycle verifies the probe loop starts when a pool
// gains an access filter and stops when the last one is removed.
func TestAutoChainProberLifecycle(t *testing.T) {
	f := newAutoChain(t, true)
	f.awaitProbe(t) // prober running

	f.cfg.SetPool(&config.Pool{ID: "work", OutboundIDs: []string{"work"}, Selector: config.SelectorSpec{Type: "random"}}) // access removed
	f.router.Rebuild()

	f.router.probeCtlMu.Lock()
	ctl := f.router.probeCtl
	f.router.probeCtlMu.Unlock()
	assert.Nil(t, ctl, "probe loop should stop when no pool has an access filter")
}

// TestAutoChainChainStatusVerdicts probes the verdict fields on the API
// surface type (latency recorded, direct error surfaced).
func TestAutoChainChainStatusVerdicts(t *testing.T) {
	f := newAutoChain(t, true)
	st := f.awaitProbe(t)
	assert.NotEmpty(t, st.Direct.Error, "direct probe failure should carry the dial error")
	assert.Greater(t, st.Relay.Latency, time.Duration(0), "relay probe should record a latency")
	assert.Equal(t, "work", st.OutboundID)
	assert.Equal(t, f.xAddr, st.Address)
	assert.Equal(t, "work", st.PoolID)
}

// TestAutoChainHotReloadAddsMember verifies a newly added pool member is
// probed promptly (the rebuild signals a refresh) and then routed.
func TestAutoChainHotReloadAddsMember(t *testing.T) {
	f := newAutoChain(t, true)
	f.awaitProbe(t)

	// Add a second work proxy that is directly reachable.
	x2 := socks5Server(t)
	f.cfg.SetOutbound(&config.OutboundSpec{ID: "work2", Protocol: "socks5", Config: map[string]any{"address": x2}})
	f.cfg.SetPool(&config.Pool{
		ID:          "work",
		OutboundIDs: []string{"work", "work2"},
		Selector:    config.SelectorSpec{Type: "random"},
		Access:      &config.AccessFilterSpec{AccessPoolID: "access", ProbeIntervalSec: 10, ProbeTimeoutSec: 2},
	})
	f.router.Rebuild()

	// work2 gets probed via the rebuild signal without waiting the full
	// interval.
	require.Eventually(t, func() bool {
		for _, st := range f.router.ChainProbeStatus() {
			if st.OutboundID == "work2" {
				return st.Direct.OK
			}
		}
		return false
	}, 10*time.Second, 50*time.Millisecond, "newly added member should be probed promptly")
}

func portOf(t *testing.T, addr string) string {
	t.Helper()
	_, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	return port
}
