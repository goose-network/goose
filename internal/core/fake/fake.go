// Package fake provides a test Outbound implementation shared across the
// engine's test suites. It is a normal (non-_test) package so tests in other
// packages can import it; production code does not use it.
package fake

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/goose-network/goose/internal/core"
)

// FakeOutbound is a test Outbound that records dials and optionally dials a
// real loopback listener to simulate an upstream. It does NOT implement the
// router's ChainDialer interface; use ChainFakeOutbound for chaining tests.
type FakeOutbound struct {
	IDVal    string
	Proto    string
	AddrVal  string
	Loc      *core.Location
	StatsVal core.OutboundStats
	DialCount int
	mu       sync.Mutex
	// OnDial, if set, is returned as the dialed connection. If nil, a
	// net.Pipe pair is returned (one end to the caller, one discarded).
	OnDial   func(network core.Network, target string) (net.Conn, error)
	DialErr  error
}

func (f *FakeOutbound) ID() string           { return f.IDVal }
func (f *FakeOutbound) Protocol() string     { return f.Proto }
func (f *FakeOutbound) Address() string      { return f.AddrVal }
func (f *FakeOutbound) Location() *core.Location { return f.Loc }
func (f *FakeOutbound) Stats() core.OutboundStats { return f.StatsVal }

func (f *FakeOutbound) DialContext(ctx context.Context, network core.Network, target string) (net.Conn, error) {
	f.mu.Lock()
	f.DialCount++
	f.mu.Unlock()
	if f.DialErr != nil {
		return nil, f.DialErr
	}
	if f.OnDial != nil {
		return f.OnDial(network, target)
	}
	a, b := net.Pipe()
	go func() { _, _ = a.Write([]byte("hello from " + f.IDVal + "\n")); a.Close() }()
	return b, nil
}

// WithLatency sets a smoothed latency stat and returns the outbound.
func (f *FakeOutbound) WithLatency(d time.Duration) *FakeOutbound {
	f.StatsVal.RecentLatency = d
	return f
}

// WithSuccess sets a success rate stat and returns the outbound.
func (f *FakeOutbound) WithSuccess(r float64) *FakeOutbound {
	f.StatsVal.SuccessRate = r
	return f
}

// ChainFakeOutbound is a FakeOutbound that also implements the router's
// ChainDialer interface (DialThrough), for multi-hop chaining tests. Use it
// as a non-first chain layer; the router asserts ChainDialer on the unwrapped
// outbound, so the DialThrough method must be present for chaining to occur.
type ChainFakeOutbound struct {
	FakeOutbound
	// DialThroughCount counts how many times DialThrough was called.
	DialThroughCount int
	// LastVia records the net.Conn passed to the most recent DialThrough.
	LastVia net.Conn
	// OnDialThrough, if set, overrides DialThrough's return. If nil, a
	// net.Pipe pair is returned.
	OnDialThrough func(network core.Network, target string, via net.Conn) (net.Conn, error)
}

// DialThrough implements the router's ChainDialer interface.
func (f *ChainFakeOutbound) DialThrough(ctx context.Context, network core.Network, target string, via net.Conn) (net.Conn, error) {
	f.mu.Lock()
	f.DialThroughCount++
	f.LastVia = via
	f.mu.Unlock()
	if f.DialErr != nil {
		return nil, f.DialErr
	}
	if f.OnDialThrough != nil {
		return f.OnDialThrough(network, target, via)
	}
	a, b := net.Pipe()
	go func() { _, _ = a.Write([]byte("tunneled via " + f.IDVal + "\n")); a.Close() }()
	return b, nil
}
