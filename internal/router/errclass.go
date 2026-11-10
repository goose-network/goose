// Failure classification: one vocabulary of phase and reason slugs shared by
// HTTP probe records and request metric rows, so a table column or a filter
// can group failures without parsing prose. The Error string keeps the full
// diagnostic; these slugs answer "which leg failed, and why" in a stable,
// machine-readable form.
//
// Classification is typed-first: DNS errors, x509/TLS errors, socket errnos
// and timeouts are unwrapped from the error chain (net/http wraps everything
// in *url.Error, which Unwrap sees through). Only the proxy-handshake and
// tunnel vocabularies — which plugins report as opaque strings — fall back to
// minimal substring matches, and an unmatched error degrades to "unknown"
// with the phase attributed from the request's own trace.
package router

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"syscall"
)

// Failure phases: where along the request path the failure happened. "dns"
// and "connect" are the local→proxy-server leg (local→target for a direct
// route), "proxy" is the proxy's own handshake or relay setup after its TCP
// port was reached, and "tls", "headers" and "body" are beyond the proxy on
// the proxy→target leg. "request" is a malformed spec or route reference.
const (
	phaseRequest = "request"
	phaseDNS     = "dns"
	phaseConnect = "connect"
	phaseProxy   = "proxy"
	phaseTLS     = "tls"
	phaseHeaders = "headers"
	phaseBody    = "body"
)

// Reason slugs. Stable identifiers; the doc in core.HTTPProbe.Reason lists
// them for the API.
const (
	reasonDNSTimeout     = "dns_timeout"
	reasonDNSNXDomain    = "dns_nxdomain"
	reasonDNSFailure     = "dns_failure"
	reasonConnectRefused = "connect_refused"
	reasonConnectTimeout = "connect_timeout"
	reasonConnectUnreach = "connect_unreachable"
	reasonRouteInvalid   = "route_invalid"
	reasonRouteUnavail   = "route_unavailable"
	reasonProxyAuth      = "proxy_auth"
	reasonProxyRejected  = "proxy_rejected"
	reasonProxyTimeout   = "proxy_timeout"
	reasonTunnelUnavail  = "tunnel_unavailable"
	reasonTLSHandshake   = "tls_handshake"
	reasonTLSCert        = "tls_cert"
	reasonTLSTimeout     = "tls_timeout"
	reasonHeaderTimeout  = "header_timeout"
	reasonBodyTimeout    = "body_timeout"
	reasonTimeout        = "timeout"
	reasonResetEOF       = "reset_eof"
	reasonDecodeError    = "decode_error"
	reasonCanceled       = "canceled"
	reasonRoundBudget    = "round_budget"
	reasonBadRequest     = "bad_request"
	reasonUnknown        = "unknown"
)

// Stages: which step of probeOneHTTP produced the error. The classifier
// combines the stage with the probe trace to attribute the failure to a leg.
const (
	stageRequest = "request" // building the request failed
	stageDo      = "do"      // client.Do failed (no complete response)
	stageDecode  = "decode"  // gzip header parse failed
	stageBody    = "body"    // body read failed
	stageQueue   = "queue"   // the round ended before the request was issued
)

// classifyProbeFailure maps an error, the stage that produced it and the
// request's trace snapshot to (phase, reason). stage "" is the generic form
// used by metric rows, which carry no trace: it classifies the reason and
// leaves the phase empty unless the typed error itself names one.
func classifyProbeFailure(stage string, err error, t phaseTimings) (phase, reason string) {
	if err == nil {
		return "", ""
	}
	if stage == stageQueue {
		// The round's budget ran out or it was aborted before this request
		// was issued; no leg of the path was involved.
		return "", reasonRoundBudget
	}

	// How far the path got, for attributing errors the typed rules do not
	// localize (and for the do-stage default). A failed route dial leaves
	// the failure at the leg the trace last saw complete.
	reached := phaseConnect
	switch {
	case t.routeDialErr == nil && t.routeDialDone:
		reached = phaseHeaders
		if t.tlsStarted && !t.tlsDone {
			reached = phaseTLS
		}
	case t.connectDone && !t.connectFailed:
		// The proxy server's TCP port was reached but the dial never
		// finished: the proxy's own handshake stalled or failed.
		reached = phaseProxy
	}
	stagePhase := func() string {
		switch stage {
		case stageRequest:
			return phaseRequest
		case stageDecode, stageBody:
			return phaseBody
		case stageDo:
			return reached
		default:
			return ""
		}
	}

	// 1. Explicit cancellation (the round or the server shut down).
	if errors.Is(err, context.Canceled) {
		return stagePhase(), reasonCanceled
	}

	// 2. First-hop DNS.
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		switch {
		case dnsErr.IsTimeout:
			return phaseDNS, reasonDNSTimeout
		case dnsErr.IsNotFound:
			return phaseDNS, reasonDNSNXDomain
		default:
			return phaseDNS, reasonDNSFailure
		}
	}

	// 3. TLS with the target. Certificate problems first: they can ride
	// inside a RecordHeaderError.
	var (
		unknownAuth x509.UnknownAuthorityError
		certInvalid x509.CertificateInvalidError
		hostErr     x509.HostnameError
		constraint  x509.ConstraintViolationError
	)
	if errors.As(err, &unknownAuth) || errors.As(err, &certInvalid) ||
		errors.As(err, &hostErr) || errors.As(err, &constraint) {
		return phaseTLS, reasonTLSCert
	}
	var recordErr tls.RecordHeaderError
	if errors.As(err, &recordErr) {
		return phaseTLS, reasonTLSHandshake
	}

	// 4. Socket-level failures on the local→proxy connect.
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case syscall.ECONNREFUSED:
			return phaseConnect, reasonConnectRefused
		case syscall.EHOSTUNREACH, syscall.ENETUNREACH:
			return phaseConnect, reasonConnectUnreach
		case syscall.ETIMEDOUT:
			return phaseConnect, reasonConnectTimeout
		case syscall.ECONNRESET, syscall.EPIPE:
			return stagePhase(), reasonResetEOF
		}
	}

	// 5. Timeouts. The stage says which deadline was being waited on; the
	// trace disambiguates a Do timeout between the legs.
	if isTimeoutErr(err) {
		switch {
		case stage == stageDecode || stage == stageBody:
			return phaseBody, reasonBodyTimeout
		case t.routeDialErr == nil && t.routeDialDone:
			// The route dial finished: the wait was beyond the proxy.
			if t.tlsStarted && !t.tlsDone {
				return phaseTLS, reasonTLSTimeout
			}
			return phaseHeaders, reasonHeaderTimeout
		case t.connectDone && !t.connectFailed:
			// The proxy answered TCP but its handshake stalled.
			return phaseProxy, reasonProxyTimeout
		case stage == stageDo:
			return phaseConnect, reasonConnectTimeout
		default:
			// Generic rows: a timeout, leg unknown.
			return "", reasonTimeout
		}
	}

	// 6. The connection dropped mid-flight.
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return stagePhase(), reasonResetEOF
	}

	// 7. Response-body decode (gzip).
	if stage == stageDecode {
		return phaseBody, reasonDecodeError
	}

	msg := err.Error()
	lower := strings.ToLower(msg)

	// 8. Route resolution and configuration failures (the router's own
	// errors, wrapped by the transport). The gvisor stack's rejection of a
	// hostname dial comes first: "resolver not configured" is the stack
	// declining name resolution, not a bad route reference, and it must
	// not fall into the router's ": not configured" match below.
	switch {
	case strings.Contains(msg, "resolver not configured"):
		return phaseDNS, reasonDNSFailure
	case strings.Contains(msg, "route nesting deeper than"),
		strings.Contains(msg, "unknown prefix"),
		strings.Contains(msg, ": not configured"),
		strings.Contains(msg, "missing address"),
		strings.Contains(msg, "empty chain"):
		return phaseRequest, reasonRouteInvalid
	case strings.Contains(msg, "no reachable member"),
		strings.Contains(msg, "no member passed filters"),
		strings.Contains(msg, "every member dial failed"),
		strings.Contains(msg, "empty pool"),
		strings.Contains(msg, "no reachable outbound"),
		strings.Contains(msg, "no outbound passed filters"):
		return phaseConnect, reasonRouteUnavail
	}

	// 9. Proxy-protocol failures: the first hop's TCP was up, but the
	// handshake toward the target failed. Minimal substring matches on the
	// known plugin vocabularies only (plugins/socks5, plugins/http, and the
	// tunnel providers); anything else must not be guessed at here.
	switch {
	case strings.Contains(lower, "no acceptable auth method"),
		strings.Contains(lower, "auth failed"),
		strings.Contains(lower, "proxy authentication required"),
		strings.Contains(lower, "407 proxy"):
		return phaseProxy, reasonProxyAuth
	case strings.Contains(msg, "CONNECT") && strings.Contains(lower, "failed"):
		return phaseProxy, reasonProxyRejected
	case strings.Contains(lower, "not established"),
		strings.Contains(lower, "did not establish"),
		strings.Contains(lower, "stopped while establishing"),
		strings.Contains(lower, "no live provider"),
		strings.Contains(lower, "no live session"),
		strings.Contains(lower, "client not running"):
		return phaseProxy, reasonTunnelUnavail
	}

	// 10. A malformed request spec.
	if stage == stageRequest {
		return phaseRequest, reasonBadRequest
	}
	return stagePhase(), reasonUnknown
}

// classifyErr is the generic form for request metric rows: reason only, no
// stage and no trace.
func classifyErr(err error) string {
	if err == nil {
		return ""
	}
	_, reason := classifyProbeFailure("", err, phaseTimings{})
	return reason
}

func isTimeoutErr(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}
