// Probe trace: per-request observation of the connection path an HTTP probe
// rides. Requirement: a failed probe must say WHICH leg failed — the local
// network reaching the proxy server, or the proxy reaching the target — and
// a slow one must show where the time went.
//
// Two layers of observation, each covering what the other cannot:
//
//   - The route-dial timing wraps the transport's DialContext closure, so it
//     is present for every stack and every protocol: system net, gvisor, and
//     tunnel plugins (psiphon/tor/geph) whose DialContext acquires an
//     in-memory stream. For a proxy route the closure spans the local→proxy
//     TCP connect plus the proxy's handshake toward the target; for a direct
//     route it is the local→target connect.
//   - The httptrace hooks (DNS, TCP connect, TLS, first byte) fire when the
//     dial actually runs through net.Dialer — the system stack and plugins
//     that detour their server dial through it. They break the route dial
//     apart: ConnectStart/ConnectDone carry the address of the first hop
//     (the PROXY SERVER for a proxy route), which is exactly the local→proxy
//     leg. Under gvisor or a tunnel protocol they never fire and the
//     corresponding fields stay zero — the record degrades to the route-dial
//     total rather than lying about phases it did not see.
//
// The trace travels on the request context: net/http passes the request ctx
// (values intact) to Transport.DialContext, and the upstream-route machinery
// (upstream.go) derives from it, so dialPoolSnapshot can report which member
// a failover landed on. Production dispatch carries no trace value; the
// lookup is one ctx.Value miss per dial.
package router

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http/httptrace"
	"sync"
	"time"
)

// probeTraceKey is the context key carrying the request's *probeTrace.
type probeTraceKey struct{}

// probeTrace records what one probe request observed about its own path.
// All fields are best-effort (see the file comment); the mutex guards the
// hooks, which net/http may call from the request and read-loop goroutines.
type probeTrace struct {
	start time.Time

	mu sync.Mutex
	// First-hop DNS (net.Dialer path only).
	dnsStart, dnsEnd time.Time
	// First-hop TCP connect: addr is the address actually connected to —
	// the proxy server for a proxy route, the target for a direct one.
	connectAddr      string
	connectStart     time.Time
	connectEnd       time.Time
	connectDoneFired bool
	connectErrNonNil bool
	// End-to-end TLS handshake with the target, over the route.
	tlsStart, tlsEnd time.Time
	tlsDoneFired     bool
	// First response byte.
	ttfb time.Time
	// The whole route dial (the transport's DialContext call).
	routeDialStart time.Time
	routeDialEnd   time.Time
	routeDialErr   error
	// The outbound that served the dial when the route picks one at dial
	// time (pool failover, chain first hop). Last attempt wins: on a
	// total failure it names the member that was tried last.
	via     string
	viaAddr string
}

func newProbeTrace(start time.Time) *probeTrace {
	return &probeTrace{start: start}
}

// withProbeTrace attaches tr to ctx for the upstream dialers to find, and
// installs the httptrace hooks that feed it.
func withProbeTrace(ctx context.Context, tr *probeTrace) context.Context {
	ctx = context.WithValue(ctx, probeTraceKey{}, tr)
	return httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		DNSStart: func(httptrace.DNSStartInfo) {
			tr.mu.Lock()
			tr.dnsStart = time.Now()
			tr.mu.Unlock()
		},
		DNSDone: func(httptrace.DNSDoneInfo) {
			tr.mu.Lock()
			tr.dnsEnd = time.Now()
			tr.mu.Unlock()
		},
		ConnectStart: func(_, addr string) {
			tr.mu.Lock()
			defer tr.mu.Unlock()
			// Happy Eyeballs may start a second racer; once a winner is
			// recorded, a late start must not move the address away from
			// the socket that actually connected.
			if tr.connectDoneFired && !tr.connectErrNonNil {
				return
			}
			tr.connectAddr = addr
			tr.connectStart = time.Now()
		},
		ConnectDone: func(_, addr string, err error) {
			tr.mu.Lock()
			defer tr.mu.Unlock()
			// Under Happy Eyeballs (RFC 8305) net.Dialer races two address
			// families and cancels the loser, whose ConnectDone then fires
			// AFTER the winner's with context.Canceled. Left unguarded, the
			// loser overwrites connectAddr with an address that never
			// connected and flips connectErrNonNil — inverting the leg
			// attribution downstream (errclass.go's proxy branch needs
			// connectDone && !connectFailed). So: a recorded success
			// freezes the state; a canceled done following any recorded
			// result is racer noise; a canceled done with nothing recorded
			// is the probe's own context dying and is kept.
			if tr.connectDoneFired && (!tr.connectErrNonNil || errors.Is(err, context.Canceled)) {
				return
			}
			tr.connectAddr = addr
			tr.connectEnd = time.Now()
			tr.connectDoneFired = true
			tr.connectErrNonNil = err != nil
		},
		TLSHandshakeStart: func() {
			tr.mu.Lock()
			tr.tlsStart = time.Now()
			tr.mu.Unlock()
		},
		TLSHandshakeDone: func(tls.ConnectionState, error) {
			tr.mu.Lock()
			tr.tlsEnd = time.Now()
			tr.tlsDoneFired = true
			tr.mu.Unlock()
		},
		GotFirstResponseByte: func() {
			tr.mu.Lock()
			tr.ttfb = time.Now()
			tr.mu.Unlock()
		},
	})
}

// traceFromCtx returns the probe trace carried by ctx, or nil. Called by the
// upstream dial path, where production traffic has no trace and the miss
// must stay free of side effects.
func traceFromCtx(ctx context.Context) *probeTrace {
	tr, _ := ctx.Value(probeTraceKey{}).(*probeTrace)
	return tr
}

// noteUpstreamVia records the outbound actually serving an upstream-route
// dial (pool failover, pinned outbound, chain first hop).
func noteUpstreamVia(ctx context.Context, id, addr string) {
	if tr := traceFromCtx(ctx); tr != nil {
		tr.mu.Lock()
		tr.via, tr.viaAddr = id, addr
		tr.mu.Unlock()
	}
}

// noteRouteDial brackets the transport's whole DialContext call.
func (tr *probeTrace) noteRouteDialStart() {
	tr.mu.Lock()
	tr.routeDialStart = time.Now()
	tr.mu.Unlock()
}

func (tr *probeTrace) noteRouteDialEnd(err error) {
	tr.mu.Lock()
	tr.routeDialEnd = time.Now()
	tr.routeDialErr = err
	tr.mu.Unlock()
}

// phaseTimings is the trace flattened into record fields: durations relative
// to the request start, zero for phases that did not run or were not
// observable.
type phaseTimings struct {
	RouteDial time.Duration
	DNS       time.Duration
	Connect   time.Duration
	TLS       time.Duration
	TTFB      time.Duration
	DialAddr  string
	DialVia   string
	// route-dial state the classifier reads.
	routeDialErr  error
	routeDialDone bool
	connectDone   bool
	connectFailed bool
	tlsStarted    bool
	tlsDone       bool
}

func (tr *probeTrace) snapshot() phaseTimings {
	if tr == nil {
		return phaseTimings{}
	}
	tr.mu.Lock()
	defer tr.mu.Unlock()
	p := phaseTimings{
		routeDialErr:  tr.routeDialErr,
		connectFailed: tr.connectErrNonNil,
		tlsDone:       tr.tlsDoneFired,
	}
	dur := func(a, b time.Time) time.Duration {
		if a.IsZero() || b.IsZero() || b.Before(a) {
			return 0
		}
		return b.Sub(a)
	}
	p.RouteDial = dur(tr.routeDialStart, tr.routeDialEnd)
	p.DNS = dur(tr.dnsStart, tr.dnsEnd)
	p.Connect = dur(tr.connectStart, tr.connectEnd)
	p.TLS = dur(tr.tlsStart, tr.tlsEnd)
	if !tr.ttfb.IsZero() && !tr.start.IsZero() {
		p.TTFB = tr.ttfb.Sub(tr.start)
	}
	if !tr.routeDialEnd.IsZero() {
		p.routeDialDone = true
	}
	p.connectDone = tr.connectDoneFired
	p.tlsStarted = !tr.tlsStart.IsZero()
	// The most specific first-hop address wins: the connected socket's
	// address (what net.Dialer actually reached), else the picked outbound's
	// server address (tunnel protocols, gvisor), else nothing — the route
	// dial's own addr is the TARGET, which would mislabel a proxy leg.
	switch {
	case tr.connectAddr != "":
		p.DialAddr = tr.connectAddr
	case tr.viaAddr != "":
		p.DialAddr = tr.viaAddr
	}
	p.DialVia = tr.via
	return p
}
