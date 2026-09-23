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
	"net"
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
