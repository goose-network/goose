// Package plugin is a compatibility shim that re-exports the canonical plugin
// contract from github.com/goose-network/goose-plugin-api. The api module is
// the neutral, dependency-free contract that both goose and external plugin
// repositories import; this package re-exports its types and registry
// functions under the historical github.com/goose-network/goose/pkg/plugin
// path so existing imports keep compiling.
//
// An external repo implements Outbound and calls Register in an init()
// function, then adds itself to the engine build via a blank import:
//
//	import _ "github.com/example/goose-plugin-shadowsocks"
//
// Built-in protocols (direct, http, socks5) live under
// github.com/goose-network/goose/plugins and register the same way.
//
// # Providers
//
// In addition to static outbounds configured in the config store, a plugin
// can act as a Provider: a dynamic source of outbound configs that the engine
// polls and merges into a managed pool. This is how a plugin that discovers
// its own proxy servers (e.g. Psiphon, which fetches and refreshes a remote
// server list) feeds those servers into the engine's pool. A Provider is
// registered with RegisterProvider and instantiated from a ProviderSpec in
// the config; the engine runs a goroutine that calls Outbounds, diffs the
// result against the config store, and rebuilds the pool on change.
package plugin

import (
	"context"
	"net"

	api "github.com/goose-network/goose-plugin-api"
)

// Outbound is re-exported from the api module. It is the exact same interface.
type Outbound = api.Outbound

// Factory builds an Outbound from a plugin-specific config map.
type Factory = api.OutboundFactory

// OutboundConfig is the config-form description of a single outbound as
// produced by a Provider.
type OutboundConfig = api.OutboundConfig

// Provider is a dynamic source of outbound configs.
type Provider = api.Provider

// UpstreamDialer dials a fetch target through the upstream proxy configured on
// a provider spec (config.ProviderSpec.UpstreamProxy — a specific outbound, a
// pool, or an access-pool fallback). The engine resolves the reference
// against live config and injects the dialer into the provider's config map
// under the reserved "_upstream_dialer" key before instantiating the plugin;
// URL-fetching providers type-assert it out and wire it as their HTTP
// transport's DialContext so their downloads honor the route. The key is
// absent (or the assertion fails) when no upstream proxy is configured —
// fetch directly in that case.
//
// The returned connection is a tunnel to address: the upstream's proxy
// handshake has already been performed, and the target protocol (TLS, HTTP,
// ...) runs over it directly.
//
// This contract lives in the goose module for now; it is expected to graduate
// to github.com/goose-network/goose-plugin-api (alongside Dialer) once
// external provider plugins adopt routed fetches.
type UpstreamDialer interface {
	// DialUpstream dials address (host:port) through the provider's
	// configured upstream proxy. network is "tcp" or "udp".
	DialUpstream(ctx context.Context, network, address string) (net.Conn, error)
}

// ProviderFactory builds a Provider from a plugin-specific config map.
type ProviderFactory = api.ProviderFactory

// DefaultProvider is re-exported from the api module: a provider plugin's
// declaration of the zero-config default the engine seeds on a fresh
// deployment (a config store with no providers). See the api module's docs
// for the declaration contract.
type DefaultProvider = api.DefaultProvider

// RegisterDefaultProvider declares this plugin as the fresh-deployment
// default provider. Intended for init(); panics when another plugin already
// declared a default.
func RegisterDefaultProvider(dp DefaultProvider) { api.RegisterDefaultProvider(dp) }

// LookupDefaultProvider returns the declared fresh-deployment default
// provider, or false when no plugin declared one.
func LookupDefaultProvider() (DefaultProvider, bool) { return api.LookupDefaultProvider() }

// UpstreamDialerKey is the reserved provider config-map key the engine uses
// to inject an UpstreamDialer (config.ProviderSpec.UpstreamProxy) into a
// provider before instantiation. Plugin configs must not use the
// "_upstream_dialer" name for their own fields.
const UpstreamDialerKey = "_upstream_dialer"

// AsUpstreamDialer extracts the engine-injected upstream dialer from a
// provider config map, or nil when no upstream proxy is configured.
func AsUpstreamDialer(cfg map[string]any) UpstreamDialer {
	if cfg == nil {
		return nil
	}
	d, _ := cfg[UpstreamDialerKey].(UpstreamDialer)
	return d
}

// Dialer is the minimal dial capability a plugin needs from the engine.
type Dialer = api.Dialer

// Network names a transport network for a target address.
type Network = api.Network

// Location is the geographic/network location of an outbound.
type Location = api.Location

// OutboundStats is the live, in-memory performance view of an outbound.
type OutboundStats = api.OutboundStats

// Register associates a protocol name with its factory. It is intended to be
// called from init(). Register panics on duplicate registration to surface
// plugin conflicts at startup.
func Register(name string, f Factory) { api.Register(name, f) }

// Lookup returns the factory for a protocol, or false if unregistered.
func Lookup(name string) (Factory, bool) { return api.Lookup(name) }

// Names returns all registered protocol names.
func Names() []string { return api.Names() }

// RegisterProvider associates a provider name with its factory. Intended to be
// called from init(). Panics on duplicate registration.
func RegisterProvider(name string, f ProviderFactory) { api.RegisterProvider(name, f) }

// LookupProvider returns the provider factory for a name, or false if
// unregistered.
func LookupProvider(name string) (ProviderFactory, bool) {
	return api.LookupProvider(name)
}

// ProviderNames returns all registered provider names.
func ProviderNames() []string { return api.ProviderNames() }
