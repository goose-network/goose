// Package core defines the central, dependency-free interfaces and value
// types that every other goose package programs against. Nothing in core
// imports another goose package: it is the seam that lets inbound, router,
// selector, filter, metrics, stack and plugins interoperate without cycles.
package core

import (
	"context"
	"net"
	"time"
)

// Network names a transport network for a target address.
type Network string

const (
	NetworkTCP Network = "tcp"
	NetworkUDP Network = "udp"
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
// its dial address by the geo package against an offline IP database.
type Location struct {
	Country     string
	Province    string
	City        string
	ISP         string
	// Raw is the original lookup record, kept for debugging/serialization.
	Raw string
}

// LatencySample is one historical latency observation of an outbound.
type LatencySample struct {
	At      time.Time
	Latency time.Duration
	OK      bool
}

// OutboundStats is the live, in-memory performance view of an outbound,
// maintained by the metrics/health subsystem and read by selectors/filters.
type OutboundStats struct {
	// RecentLatency is an exponentially-smoothed estimate of dial latency.
	RecentLatency time.Duration
	// SuccessRate is the fraction of recent dials that succeeded (0..1).
	SuccessRate float64
	// LastSeen is when this outbound was last used successfully.
	LastSeen time.Time
	// Samples is the raw recent window, for selectors that need it.
	Samples []LatencySample
}

// Outbound is a single upstream proxy the engine can dial through. It is the
// contract implemented by every plugin (built-in or external).
type Outbound interface {
	// ID is the stable, unique identifier of this outbound within the pool.
	ID() string
	// Protocol is the plugin name, e.g. "direct", "http", "socks5".
	Protocol() string
	// DialContext dials the target through this outbound. The returned
	// connection is the first hop of the chain; the router is responsible
	// for further chaining if configured.
	DialContext(ctx context.Context, network Network, target string) (net.Conn, error)
	// Address is the outbound's own dial endpoint (host:port), or empty for
	// "direct". Used for geo-location and metrics.
	Address() string
	// Location returns the resolved location, or nil if unknown.
	Location() *Location
	// Stats returns a snapshot of recent performance.
	Stats() OutboundStats
}

// OutboundFactory builds an Outbound from a plugin-specific config map. The
// engine calls factories registered with the plugin registry.
type OutboundFactory func(cfg map[string]any) (Outbound, error)

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
	ID         string
	InboundID  string
	User       string
	Network    Network
	Target     string
	Chain      []string
	Success    bool
	Error      string
	Latency    time.Duration
	StartedAt  time.Time
	FinishedAt time.Time
}
