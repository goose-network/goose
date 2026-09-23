//go:build !gvisor

// Package stack — default (non-gvisor) build. The gvisor stack is unavailable
// unless the binary is built with -tags gvisor. This stub returns a clear
// error so callers know to rebuild with the tag.
package stack

import (
	"context"
	"errors"
	"net"
)

func newGvisor() (*gvisorStack, error) {
	return nil, errors.New("stack: gvisor support not compiled in; rebuild with -tags gvisor")
}

// gvisorStack is a placeholder type so the system build has a consistent
// symbol set. It is never instantiated in this build.
type gvisorStack struct{}

func (*gvisorStack) Name() string { return "gvisor" }
func (*gvisorStack) Close() error { return nil }
func (*gvisorStack) Dial(_ context.Context, _, _ string) (net.Conn, error) {
	return nil, errors.New("stack: gvisor not compiled in")
}
