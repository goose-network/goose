package stack

import (
	"context"
	"net"
	"time"
)

// systemStack dials via the host network stack (net.Dialer).
type systemStack struct {
	d net.Dialer
}

func newSystem() *systemStack {
	return &systemStack{d: net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}}
}

func (s *systemStack) Name() string { return "system" }

func (s *systemStack) Dial(ctx context.Context, network, address string) (net.Conn, error) {
	return s.d.DialContext(ctx, network, address)
}

func (s *systemStack) Close() error { return nil }
