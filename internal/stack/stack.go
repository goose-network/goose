// Package stack abstracts the underlying network stack so the engine can
// dial outbounds either through the host's system network stack or through
// a userspace gvisor netstack. The system stack is the default and needs no
// extra dependencies; the gvisor stack is selected by config and is built
// behind a build tag so the (heavy) gvisor import is opt-in.
package stack

import (
	"context"
	"errors"
	"net"
)

// Dialer dials a next-hop address for the engine. Outbound plugins receive a
// Dialer when they need to reach their own server (e.g. the HTTP proxy
// outbound dials its proxy server through this), which lets the configured
// stack govern all outbound transport.
type Dialer interface {
	Dial(ctx context.Context, network, address string) (net.Conn, error)
}

// Stack is the factory + lifecycle handle for a network stack.
type Stack interface {
	Dialer
	// Name returns "system" or "gvisor".
	Name() string
	// Close releases stack resources.
	Close() error
}

// New constructs the configured stack. typ is "system" or "gvisor".
func New(typ string) (Stack, error) {
	switch typ {
	case "", "system":
		return newSystem(), nil
	case "gvisor":
		return newGvisor()
	default:
		return nil, errors.New("stack: unknown type " + typ)
	}
}
