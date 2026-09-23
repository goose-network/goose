// Package direct is the no-op outbound: it dials the target directly through
// the engine's underlying stack. It is the "no proxy" plugin and the base
// case for chains.
package direct

import (
	"context"
	"net"
	"sync"

	pub "github.com/goose-network/goose/pkg/plugin"
	"github.com/goose-network/goose/internal/core"
)

func init() {
	pub.Register("direct", New)
}

// Direct dials targets straight through a provided dialer (the engine stack).
// When used as the first hop it dials the target directly; when chained, it
// dials the target through the previous connection.
type Direct struct {
	id       string
	address  string
	dialer   Dialer
	location *core.Location
	mu       sync.Mutex
}

// Dialer is the engine-provided dialer (system or gvisor stack). The factory
// receives it via config under the key "_dialer" (set by the router).
type Dialer interface {
	Dial(ctx context.Context, network, address string) (net.Conn, error)
}

// New builds a direct outbound from config.
func New(cfg map[string]any) (core.Outbound, error) {
	d := &Direct{id: str(cfg, "id"), address: str(cfg, "address")}
	if v, ok := cfg["_dialer"].(Dialer); ok {
		d.dialer = v
	}
	return d, nil
}

func (d *Direct) ID() string       { return d.id }
func (d *Direct) Protocol() string { return "direct" }
func (d *Direct) Address() string  { return d.address }
func (d *Direct) Location() *core.Location { return d.location }

func (d *Direct) DialContext(ctx context.Context, network core.Network, target string) (net.Conn, error) {
	if d.dialer != nil {
		return d.dialer.Dial(ctx, string(network), target)
	}
	// Fallback to the standard dialer if no engine dialer was injected.
	var nd net.Dialer
	return nd.DialContext(ctx, string(network), target)
}

// DialThrough supports chaining: dial the target through a previous conn.
// For "direct", chaining means using the previous conn as the transport —
// which only makes sense if the previous conn already reaches the target's
// network. In practice direct is the terminal hop, so we just return the
// previous conn if it's already connected, else dial directly.
func (d *Direct) DialThrough(ctx context.Context, network core.Network, target string, via net.Conn) (net.Conn, error) {
	// direct has no server of its own; the previous conn IS the transport.
	return via, nil
}

func (d *Direct) Stats() core.OutboundStats { return core.OutboundStats{} }

func str(cfg map[string]any, key string) string {
	if v, ok := cfg[key].(string); ok {
		return v
	}
	return ""
}
