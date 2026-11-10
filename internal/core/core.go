// Package core defines the central, engine-internal value types and
// interfaces that goose's own packages program against. The shared plugin
// contract types (Network, Location, Outbound, OutboundFactory, etc.) are now
// owned by github.com/goose-network/goose-plugin-api and re-exported here as
// type aliases, so core's existing API surface is preserved while the
// canonical definitions live in the dependency-free api module. Nothing in
// core imports another goose package: it is the seam that lets inbound,
// router, selector, filter, metrics, stack and plugins interoperate without
// cycles.
package core

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	api "github.com/goose-network/goose-plugin-api"
)

// Network names a transport network for a target address. It is an alias to
// the canonical definition in the api module.
type Network = api.Network

// Re-export the network consts from the api module. Because Network is an
// alias to api.Network (a string), these consts are untyped string constants
// assignable to Network.
const (
	NetworkTCP = api.NetworkTCP
	NetworkUDP = api.NetworkUDP
)

// Metadata describes a single proxied request as it flows through the
// engine. It is created by the inbound listener, enriched by the router,
// and finally handed to the metrics recorder.
type Metadata struct {
	// Inbound identifies the listener + user that accepted the request.
	InboundID string
	// User is the authenticated inbound user, empty if none.
	User string

	// Network is the requested transport (tcp/udp).
	Network Network
	// TargetHost / TargetPort is the destination the client asked for.
	// TargetHost may be a domain or an IP literal.
	TargetHost string
	TargetPort string

	// SourceIP is the client's address (for logging/metrics).
	SourceIP net.IP

	// Chain is the ordered list of outbound IDs selected to serve this
	// request, filled in as the chain executes. Chain[0] is the first hop.
	Chain []string

	// StartedAt is when the inbound accepted the request.
	StartedAt time.Time
}

// Target returns the dial string for the destination.
func (m Metadata) Target() string {
	return net.JoinHostPort(m.TargetHost, m.TargetPort)
}

// Location is the geographic/network location of an outbound, resolved from
// its dial address by the geo package against an offline IP database. It is an
// alias to the canonical definition in the api module.
type Location = api.Location

// LatencySample is one historical latency observation of an outbound. It is an
// alias to the canonical definition in the api module.
type LatencySample = api.LatencySample

// OutboundStats is the live, in-memory performance view of an outbound,
// maintained by the metrics/health subsystem and read by selectors/filters.
// It is an alias to the canonical definition in the api module.
type OutboundStats = api.OutboundStats

// Outbound is a single upstream proxy the engine can dial through. It is the
// contract implemented by every plugin (built-in or external). It is an alias
// to the canonical definition in the api module.
type Outbound = api.Outbound

// OutboundFactory builds an Outbound from a plugin-specific config map. The
// engine calls factories registered with the plugin registry. It is an alias
// to the canonical definition in the api module.
type OutboundFactory = api.OutboundFactory

// Selector picks one outbound from a pool according to a load-balancing
// strategy. Implementations live in internal/selector.
type Selector interface {
	// Name is the strategy name, e.g. "random", "roundrobin", "sticky".
	Name() string
	// Pick chooses one outbound from pool for the given request metadata.
	// It must be safe for concurrent use.
	Pick(pool []Outbound, meta Metadata) (Outbound, error)
}

// Filter is a predicate over a candidate outbound for a given request. The
// router composes filters (location, latency, protocol) per inbound policy.
type Filter interface {
	// Name is the filter name, e.g. "location", "latency", "protocol".
	Name() string
	// Allow reports whether the outbound is admissible for this request.
	Allow(o Outbound, meta Metadata) bool
}

// ChainLayer is one hop of a proxy chain: a filter that narrows the pool,
// followed by a selector that picks the next outbound from what remains.
type ChainLayer struct {
	Filters  []Filter
	Selector Selector
}

// Chain is an ordered sequence of layers. Layer 0 is the first hop the
// inbound connects to; each subsequent layer is dialed through the previous.
type Chain []ChainLayer

// Recorder persists a finished request's metrics. Implemented by
// internal/metrics against bbolt.
type Recorder interface {
	// Record writes one request metric. It must be safe for concurrent use
	// and must not block the data path for long (batch internally).
	Record(m RequestMetric) error
}

// RequestMetric is the persisted record of one proxied request.
type RequestMetric struct {
	ID        string   `json:"id"`
	InboundID string   `json:"inboundID"`
	User      string   `json:"user"`
	Network   Network  `json:"network"`
	Target    string   `json:"target"`
	Chain     []string `json:"chain"`
	Success   bool     `json:"success"`
	Error     string   `json:"error"`
	// Reason is a stable slug classifying the failure (dns_timeout,
	// connect_refused, tls_handshake, header_timeout, body_timeout, ...),
	// empty on success and on rows recorded before classification existed.
	// The Error string keeps the full diagnostic; Reason is what a table
	// column or a filter can group on without parsing prose.
	Reason     string        `json:"reason,omitempty"`
	Latency    time.Duration `json:"latency"`
	StartedAt  time.Time     `json:"startedAt"`
	FinishedAt time.Time     `json:"finishedAt"`
}

// HopLocal is the pseudo first-hop recorded on reachability-probe metric
// rows: the engine's own (local) network dialed the outbound's proxy server,
// rather than a request riding a proxy chain. A probe row's Chain is
// [HopLocal, outboundID] and its Target is the outbound's server address, so
// the outbound metrics table holds one row kind per measured path — single
// hop, chain, and local reachability — and routing filters candidates by
// exactly those rows.
const HopLocal = "local"

// ExitCheck is the persisted record of one exit-IP check: the public IP
// observed when the engine dialed an exit-IP site through one outbound. A
// proxy's connect address is not its exit address, and the exit can vary by
// target site and over time, so results are recorded per (outbound, site).
type ExitCheck struct {
	ID         string `json:"id"`
	OutboundID string `json:"outboundID"`
	// Site is the host of the check site that was asked (e.g.
	// "ifconfig.me").
	Site      string        `json:"site"`
	ExitIP    string        `json:"exitIP"`
	Success   bool          `json:"success"`
	Error     string        `json:"error"`
	Latency   time.Duration `json:"latency"`
	StartedAt time.Time     `json:"startedAt"`
}

// HopProbe is the pseudo first-hop recorded on HTTP probe metric rows: the
// engine issued an application-level HTTP request toward a target through a
// route (an outbound, a pool, a chain, or its own stack). A probe row's Chain
// is [HopProbe, route] and its Target is the target's host:port, like every
// other metric row; the full URL lives on the HTTPProbe record.
//
// These rows are informational: unlike a HopLocal row (a bare TCP dial of an
// outbound's own server, where a failure really means "the local network
// cannot reach it"), an HTTP probe can fail because of the TARGET — a blocked
// site, a TLS mismatch, a 5xx — while the route itself is perfectly usable.
// Feeding those failures into the reachability verdicts would drop a healthy
// outbound from routing because one probed site disliked it, so the metrics
// store records the rows for display and skips them for verdicts.
const HopProbe = "probe"

// HTTPProbeSpec is one HTTP reachability probe round: issue this request
// toward each target through each route, and record what came back.
//
// Routes are the same reference forms upstream_proxy and chain layers accept
// ("outbound:<id>", "pool:<id>", "chain:<id>", a bare id, or "direct"); an
// empty route list means every built outbound. Targets must be absolute
// http(s) URLs.
type HTTPProbeSpec struct {
	// Targets are the absolute http(s) URLs to request.
	Targets []string `json:"targets"`
	// Method is the HTTP method; empty means GET.
	Method string `json:"method"`
	// Headers are added to every request. Host and Content-Length are set
	// from the URL and Body; a User-Agent is added when none is given.
	Headers map[string]string `json:"headers"`
	// Body is the request body, sent verbatim (empty for a bodyless method).
	Body string `json:"body"`
	// Routes are the route references to probe through; empty means every
	// built outbound, one probe per (target, outbound).
	Routes []string `json:"routes"`
	// Timeout bounds one request. Zero uses the caller's default.
	Timeout time.Duration `json:"timeout"`
	// MaxBodyBytes caps how much of the response body is kept. Zero uses the
	// caller's default.
	MaxBodyBytes int `json:"max_body_bytes"`
}

// MaxProbeTargets bounds one HTTP probe round's target list, so a config typo
// cannot turn a round into a request storm.
const MaxProbeTargets = 32

// ValidProbeTargets checks a probe's target list: at least one target, every
// entry an absolute http(s) URL, duplicates dropped and surrounding space
// trimmed. It is the one rule both callers apply — the config validator
// rejects a bad list at PUT time, the router refuses to run one — so a target
// that passes validation always runs.
func ValidProbeTargets(targets []string) ([]string, error) {
	out := make([]string, 0, len(targets))
	seen := make(map[string]bool, len(targets))
	for _, t := range targets {
		t = strings.TrimSpace(t)
		if t == "" || seen[t] {
			continue
		}
		u, err := url.Parse(t)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return nil, fmt.Errorf("probe target %q: want an absolute http(s) URL", t)
		}
		seen[t] = true
		out = append(out, t)
	}
	if len(out) == 0 {
		return nil, errors.New("probe needs at least one target")
	}
	if len(out) > MaxProbeTargets {
		return nil, fmt.Errorf("%d probe targets exceeds the %d limit", len(out), MaxProbeTargets)
	}
	return out, nil
}

// HTTPProbe is the persisted record of one HTTP probe: one (target, route)
// pair's response, kept so a manual run and a scheduled round leave the same
// readable history.
type HTTPProbe struct {
	ID     string `json:"id"`
	Route  string `json:"route"`
	Target string `json:"target"`
	Method string `json:"method"`
	// Status is the HTTP response status, or 0 when no response arrived.
	Status int `json:"status"`
	// Headers are the response headers, one value per name; a repeated
	// header's values are joined with ", ".
	Headers map[string]string `json:"headers,omitempty"`
	// Body is the response body, capped at the probe's MaxBodyBytes.
	Body string `json:"body"`
	// Truncated reports that the body was capped, so a regex over it may
	// miss a match that a full download would have found.
	Truncated bool   `json:"truncated"`
	Success   bool   `json:"success"`
	Error     string `json:"error"`
	// Phase locates a failure along the request path: "dns" and "connect"
	// are the local→proxy-server leg (for a direct route, local→target),
	// "proxy" is the proxy's own handshake or relay setup after its TCP
	// port was reached, and "tls", "headers" and "body" are beyond the
	// proxy on the proxy→target leg. "request" is a malformed spec. Empty
	// on success and on rows recorded before phases existed.
	Phase string `json:"phase,omitempty"`
	// Reason is a stable slug classifying the failure (dns_timeout,
	// dns_nxdomain, dns_failure, connect_refused, connect_timeout,
	// connect_unreachable, route_invalid, route_unavailable, proxy_auth,
	// proxy_rejected, proxy_timeout, tunnel_unavailable, tls_handshake,
	// tls_cert, tls_timeout, header_timeout, body_timeout, timeout,
	// reset_eof, decode_error, canceled, round_budget, bad_request,
	// unknown). Empty on success.
	Reason string `json:"reason,omitempty"`
	// DialAddr is the address the first hop actually connected to — the
	// proxy server for a proxy route, the target itself for a direct one.
	// Empty when the dial was not observable (a tunnel protocol whose
	// server connections ride a background context, a stack that bypasses
	// net.Dialer, an older row).
	DialAddr string `json:"dialAddr,omitempty"`
	// DialVia is the outbound that served the route's dial: for a pool or
	// chain route it names the member picked at dial time (last attempt wins
	// on a failover), for an "outbound:<id>" route it repeats the id. Empty
	// for a direct route, and when the route failed before an outbound was
	// picked.
	DialVia string `json:"dialVia,omitempty"`
	// Phase timings, each the span of its own phase — except TTFB, which
	// is measured from the request start — and zero when a phase did not
	// run or was not observable. RouteDial spans the whole route
	// dial — the local→proxy TCP connect plus the proxy's handshake toward
	// the target (a tunnel protocol's stream acquisition) — and is the one
	// timing every stack provides. DNS and Connect break out the first
	// hop's resolution and TCP connect when the stack fires the standard
	// trace hooks; TLS is the end-to-end handshake with the target; TTFB
	// is when the first response byte arrived.
	RouteDial time.Duration `json:"routeDial,omitempty"`
	DNS       time.Duration `json:"dns,omitempty"`
	Connect   time.Duration `json:"connect,omitempty"`
	TLS       time.Duration `json:"tls,omitempty"`
	TTFB      time.Duration `json:"ttfb,omitempty"`

	Latency   time.Duration `json:"latency"`
	StartedAt time.Time     `json:"startedAt"`
}
