package inbound

import (
	"context"
	"net"

	"github.com/goose-network/goose/internal/core"
)

// Handler is what an inbound listener calls once it has authenticated a
// connection and extracted the target. It is implemented by the router.
//
// The split into Dial + Bridge lets the listener write its protocol-specific
// success reply (e.g. the SOCKS5 reply, the HTTP 200 for CONNECT) between
// the dial succeeding and the bidirectional copy starting.
type Handler interface {
	// Dial dials the target through the resolved chain and returns the
	// upstream connection plus the ordered list of outbound IDs used. The
	// caller owns closing both the inbound conn and the upstream conn.
	Dial(ctx context.Context, meta core.Metadata) (upstream net.Conn, chain []string, err error)
	// Bridge copies bytes bidirectionally between the inbound and upstream
	// connections until one side closes, then records the request metric.
	// meta.Chain should be populated with the chain returned by Dial.
	Bridge(inbound, upstream net.Conn, meta core.Metadata)
}

// Listener is a running inbound listener.
type Listener interface {
	// ID is the inbound config id.
	ID() string
	// Address is the listen address.
	Address() string
	// Protocol is "http" or "socks5".
	Protocol() string
	// Close stops the listener.
	Close() error
}

// New builds and starts a listener from a protocol + address + auth + the
// per-user policy resolver + the connection handler.
//
// resolvePolicy maps an authenticated username to its chain id. If the
// listener has no auth, "" is passed and resolvePolicy("") is used.
type PolicyResolver func(user string) (chainID string)

// New constructs and starts the listener.
func New(id, protocol, listen string, auth *AuthStore, resolve PolicyResolver, h Handler) (Listener, error) {
	switch protocol {
	case "http":
		return newHTTP(id, listen, auth, resolve, h)
	case "socks5":
		return newSOCKS5(id, listen, auth, resolve, h)
	default:
		return nil, &net.OpError{Op: "listen", Err: errUnknownProtocol{protocol}}
	}
}

type errUnknownProtocol struct{ proto string }

func (e errUnknownProtocol) Error() string { return "inbound: unknown protocol " + e.proto }
