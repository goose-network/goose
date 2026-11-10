// Failure classification tests: pin the reason vocabulary. Typed errors must
// classify without string matching, plugin handshake strings must classify to
// proxy-phase reasons, and the trace must attribute an untyped failure to the
// leg the request reached.
package router

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/url"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClassifyProbeFailure(t *testing.T) {
	// The shapes errors arrive in: net/http wraps transport failures in
	// *url.Error, socket failures in *net.OpError.
	urlErr := func(err error) error { return &url.Error{Op: "Get", URL: "http://t/", Err: err} }
	opErr := func(err error) error { return &net.OpError{Op: "dial", Net: "tcp", Err: err} }

	// Trace states: how far the path got before the failure.
	none := phaseTimings{}                                           // nothing observed
	blocked := phaseTimings{routeDialErr: errors.New("dial failed")} // the route dial failed
	connectOnly := phaseTimings{connectDone: true}                   // proxy TCP up, dial stalled
	dialed := phaseTimings{routeDialDone: true}                      // route dial finished
	dialedTLS := phaseTimings{routeDialDone: true, tlsStarted: true} // stalled mid-TLS

	cases := []struct {
		name   string
		stage  string
		err    error
		t      phaseTimings
		phase  string
		reason string
	}{
		// Typed errors: no strings involved.
		{"dns timeout", stageDo, urlErr(opErr(&net.DNSError{Err: "timeout", IsTimeout: true})), none, phaseDNS, reasonDNSTimeout},
		{"dns nxdomain", stageDo, urlErr(&net.DNSError{Err: "no such host", IsNotFound: true}), none, phaseDNS, reasonDNSNXDomain},
		{"dns failure", stageDo, urlErr(&net.DNSError{Err: "server misbehaving"}), none, phaseDNS, reasonDNSFailure},
		{"refused", stageDo, urlErr(opErr(syscall.ECONNREFUSED)), none, phaseConnect, reasonConnectRefused},
		{"unreachable", stageDo, urlErr(opErr(syscall.EHOSTUNREACH)), blocked, phaseConnect, reasonConnectUnreach},
		{"socket timeout", stageDo, urlErr(opErr(syscall.ETIMEDOUT)), blocked, phaseConnect, reasonConnectTimeout},
		{"tls cert", stageDo, urlErr(x509.UnknownAuthorityError{}), dialed, phaseTLS, reasonTLSCert},
		{"tls handshake", stageDo, urlErr(tls.RecordHeaderError{Msg: "remote error: handshake failure"}), dialed, phaseTLS, reasonTLSHandshake},

		// Timeouts: the stage and the trace decide which deadline was missed.
		{"header timeout", stageDo, urlErr(context.DeadlineExceeded), dialed, phaseHeaders, reasonHeaderTimeout},
		{"tls timeout", stageDo, urlErr(context.DeadlineExceeded), dialedTLS, phaseTLS, reasonTLSTimeout},
		{"proxy handshake timeout", stageDo, urlErr(context.DeadlineExceeded), connectOnly, phaseProxy, reasonProxyTimeout},
		{"connect timeout", stageDo, urlErr(context.DeadlineExceeded), blocked, phaseConnect, reasonConnectTimeout},
		{"connect timeout unobserved", stageDo, urlErr(context.DeadlineExceeded), none, phaseConnect, reasonConnectTimeout},
		{"body timeout", stageBody, context.DeadlineExceeded, dialed, phaseBody, reasonBodyTimeout},
		{"generic timeout", "", context.DeadlineExceeded, none, "", reasonTimeout},
		{"canceled", stageDo, urlErr(context.Canceled), none, phaseConnect, reasonCanceled},

		// Mid-flight drops.
		{"body reset", stageBody, io.ErrUnexpectedEOF, dialed, phaseBody, reasonResetEOF},
		{"socket reset", stageDo, urlErr(opErr(syscall.ECONNRESET)), dialed, phaseHeaders, reasonResetEOF},

		// An untyped failure while the trace says the handshake was still
		// running pins the reached-refinement: the timeout rule above has
		// its own tlsStarted check, so without reached=phaseTLS this case
		// would fall through to the headers phase.
		{"untyped tls alert mid-handshake", stageDo, urlErr(errors.New("remote error: tls: internal error")), dialedTLS, phaseTLS, reasonUnknown},

		// Body decode.
		{"gzip header", stageDecode, errors.New("gzip: invalid header"), dialed, phaseBody, reasonDecodeError},

		// Route resolution and configuration.
		{"route missing", stageDo, urlErr(errors.New(`router: upstream_proxy outbound "nope": not configured`)), none, phaseRequest, reasonRouteInvalid},
		{"route cyclic", stageDo, urlErr(errors.New(`router: upstream_proxy "pool:a": route nesting deeper than 4 levels (cyclic reference?)`)), none, phaseRequest, reasonRouteInvalid},
		{"pool dead", stageDo, urlErr(errors.New("router: upstream_proxy pool p: no reachable member")), none, phaseConnect, reasonRouteUnavail},
		{"chain layer dead", stageDo, urlErr(errors.New("router: layer 0 no reachable outbound (local reachability probes failed)")), none, phaseConnect, reasonRouteUnavail},
		// The gvisor stack rejects hostname dials with its own "resolver
		// not configured" wording — a name-resolution limitation, which
		// must not be mistaken for the router's ": not configured".
		{"gvisor hostname dial", stageDo, urlErr(errors.New("gvisor: address api.ipify.org:443 is not an IP literal (resolver not configured)")), none, phaseDNS, reasonDNSFailure},

		// Proxy handshake vocabularies (plugins/socks5, plugins/http).
		{"socks5 no method", stageDo, urlErr(errors.New("socks5 outbound: no acceptable auth method")), none, phaseProxy, reasonProxyAuth},
		{"socks5 auth failed", stageDo, urlErr(errors.New("socks5 outbound: auth failed")), none, phaseProxy, reasonProxyAuth},
		{"http connect 407", stageDo, urlErr(errors.New("http outbound: CONNECT example.com:443 failed: 407 Proxy Authentication Required")), none, phaseProxy, reasonProxyAuth},
		{"http connect 502", stageDo, urlErr(errors.New("http outbound: CONNECT example.com:443 failed: 502 Bad Gateway")), none, phaseProxy, reasonProxyRejected},
		{"socks5 connect code", stageDo, urlErr(errors.New("socks5 outbound: CONNECT failed code 2")), none, phaseProxy, reasonProxyRejected},

		// Tunnel vocabularies.
		{"tunnel down", stageDo, urlErr(errors.New("psiphon outbound: tunnel not established")), none, phaseProxy, reasonTunnelUnavail},
		{"tunnel no provider", stageDo, urlErr(errors.New("psiphon outbound: no live provider")), none, phaseProxy, reasonTunnelUnavail},

		// Degradation: untyped errors get the trace-attributed phase.
		{"untyped dial failure", stageDo, urlErr(errors.New("blocked by local network (test)")), blocked, phaseConnect, reasonUnknown},
		{"untyped beyond proxy", stageDo, urlErr(errors.New("weird")), dialed, phaseHeaders, reasonUnknown},
		{"untyped generic", "", errors.New("weird"), none, "", reasonUnknown},

		// Request building.
		{"bad spec", stageRequest, errors.New(`parse "://bad": missing protocol scheme`), none, phaseRequest, reasonBadRequest},
		{"round budget", stageQueue, context.Canceled, none, "", reasonRoundBudget},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			phase, reason := classifyProbeFailure(tc.stage, tc.err, tc.t)
			assert.Equal(t, tc.phase, phase)
			assert.Equal(t, tc.reason, reason)
		})
	}
}

func TestClassifyErr(t *testing.T) {
	assert.Empty(t, classifyErr(nil))
	assert.Equal(t, reasonDNSTimeout, classifyErr(&net.DNSError{IsTimeout: true}))
	assert.Equal(t, reasonConnectRefused, classifyErr(&net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}))
	assert.Equal(t, reasonTimeout, classifyErr(context.DeadlineExceeded))
	assert.Equal(t, reasonUnknown, classifyErr(errors.New("something else")))
}

// TestClassifyFalsePositives guards the substring fallbacks against the
// obvious misfires: an address or port containing a magic number must not
// read as a proxy failure.
func TestClassifyFalsePositives(t *testing.T) {
	_, reason := classifyProbeFailure(stageDo,
		&url.Error{Op: "Get", Err: errors.New("dial tcp 10.0.0.1:4073: weird")}, phaseTimings{})
	assert.Equal(t, reasonUnknown, reason, "a port containing 407 is not proxy auth")

	_, reason = classifyProbeFailure(stageDo,
		&url.Error{Op: "Get", Err: errors.New("connection failed by peer")}, phaseTimings{})
	assert.Equal(t, reasonUnknown, reason, "lowercase 'connect...failed' is not a CONNECT rejection")
}
