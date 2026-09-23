// Package plugin is the public contract that external outbound-protocol
// repositories import to plug into the goose engine.
//
// An external repo implements Outbound and calls Register in an init()
// function, then adds itself to the engine build via a blank import:
//
//	import _ "github.com/example/goose-plugin-shadowsocks"
//
// Built-in protocols (direct, http, socks5) live under
// github.com/goose-network/goose/plugins and register the same way.
package plugin

import (
	"context"
	"net"
	"sync"

	"github.com/goose-network/goose/internal/core"
)

// Outbound is re-exported from core so plugin authors do not need to import
// the internal package. It is the exact same interface.
type Outbound = core.Outbound

// Factory builds an Outbound from a plugin-specific config map.
type Factory = core.OutboundFactory

// Dialer is the minimal dial capability a plugin needs from the engine: it
// dials a next-hop address (used when chaining). The engine provides an
// implementation that honors the configured stack (system or gvisor).
type Dialer interface {
	// DialNext dials the given address as the next hop of a chain.
	DialNext(ctx context.Context, network string, address string) (net.Conn, error)
}

// registry holds all registered outbound factories by protocol name.
var (
	mu       sync.RWMutex
	factories = map[string]Factory{}
)

// Register associates a protocol name with its factory. It is intended to be
// called from init(). Register panics on duplicate registration to surface
// plugin conflicts at startup.
func Register(name string, f Factory) {
	mu.Lock()
	defer mu.Unlock()
	if f == nil {
		panic("plugin: Register(" + name + ") with nil factory")
	}
	if _, dup := factories[name]; dup {
		panic("plugin: Register called twice for " + name)
	}
	factories[name] = f
}

// Lookup returns the factory for a protocol, or false if unregistered.
func Lookup(name string) (Factory, bool) {
	mu.RLock()
	defer mu.RUnlock()
	f, ok := factories[name]
	return f, ok
}

// Names returns all registered protocol names.
func Names() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(factories))
	for n := range factories {
		out = append(out, n)
	}
	return out
}
