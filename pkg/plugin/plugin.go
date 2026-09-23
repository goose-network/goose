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

// ProviderFactory builds a Provider from a plugin-specific config map.
type ProviderFactory = api.ProviderFactory

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
