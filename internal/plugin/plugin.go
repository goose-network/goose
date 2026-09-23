// Package plugin (internal) wraps the public pkg/plugin registry and builds
// Outbound instances from config. It is the engine-internal counterpart to
// the public contract: the engine talks to this package, plugin authors
// talk to pkg/plugin.
package plugin

import (
	"fmt"

	pub "github.com/goose-network/goose/pkg/plugin"
	"github.com/goose-network/goose/internal/core"
)

// Build constructs an Outbound from a protocol name and a config map by
// dispatching to the registered factory.
func Build(protocol string, cfg map[string]any) (core.Outbound, error) {
	f, ok := pub.Lookup(protocol)
	if !ok {
		return nil, fmt.Errorf("plugin: unknown outbound protocol %q (registered: %v)", protocol, pub.Names())
	}
	return f(cfg)
}

// Registered returns the names of all available outbound protocols.
func Registered() []string { return pub.Names() }
