//go:build gvisor

// Package stack — gvisor build. This file wires a userspace gvisor netstack
// (stack.Stack + gonet TCP/UDP dialers) so the engine can perform layer 3/4
// functions entirely in userspace, without the host kernel. It is compiled
// only with the "gvisor" build tag to keep the default build light.
package stack

import (
	"context"
	"fmt"
	"net"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv6"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/udp"
)

// gvisorStack holds a userspace netstack. The engine dials through gonet,
// which speaks TCP/UDP over the in-kernel-less stack.
type gvisorStack struct {
	s *stack.Stack
}

func newGvisor() (*gvisorStack, error) {
	s := stack.New(stack.Options{
		NetworkProtocols:   []stack.NetworkProtocolFactory{ipv4.NewProtocol, ipv6.NewProtocol},
		TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol, udp.NewProtocol},
	})
	// No NIC is attached here by default: a real deployment attaches a
	// TUN/channel endpoint and configures addresses. For outbound-only
	// dialing through a userspace stack, callers attach a link endpoint
	// (e.g. channel.Endpoint) and add an address before dialing. This
	// skeleton keeps the engine buildable and is the integration point for
	// layer-2/3/4 features (packet capture, NAT, transparent proxying).
	gs := &gvisorStack{s: s}
	return gs, nil
}

func (g *gvisorStack) Name() string { return "gvisor" }

func (g *gvisorStack) Close() error {
	g.s.Close()
	return nil
}

// Dial dials a TCP (or UDP) address through the gvisor netstack via gonet.
// The address must resolve to an IP the stack can route; in a full
// deployment a resolver/NIC is configured on the stack.
func (g *gvisorStack) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("gvisor: split %s: %w", address, err)
	}
	p, err := parsePort(port)
	if err != nil {
		return nil, err
	}
	addr := tcpip.FullAddress{Port: p}
	ip := net.ParseIP(host)
	switch {
	case ip.To4() != nil:
		addr.NIC = 1
		addr.Addr = tcpip.AddrFromSlice(ip.To4())
	case ip.To16() != nil:
		addr.NIC = 1
		addr.Addr = tcpip.AddrFromSlice(ip.To16())
	default:
		return nil, fmt.Errorf("gvisor: address %s is not an IP literal (resolver not configured)", host)
	}
	dl, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	switch network {
	case "tcp", "tcp4", "tcp6":
		var proto tcpip.NetworkProtocolNumber
		if ip.To4() != nil {
			proto = ipv4.ProtocolNumber
		} else {
			proto = ipv6.ProtocolNumber
		}
		return gonet.DialContextTCP(dl, g.s, addr, proto)
	case "udp", "udp4", "udp6":
		var proto tcpip.NetworkProtocolNumber
		if ip.To4() != nil {
			proto = ipv4.ProtocolNumber
		} else {
			proto = ipv6.ProtocolNumber
		}
		return gonet.DialUDP(g.s, nil, &addr, proto)
	default:
		return nil, fmt.Errorf("gvisor: unsupported network %q", network)
	}
}

func parsePort(s string) (uint16, error) {
	var p int
	if _, err := fmt.Sscanf(s, "%d", &p); err != nil || p < 0 || p > 65535 {
		return 0, fmt.Errorf("gvisor: bad port %q", s)
	}
	return uint16(p), nil
}
