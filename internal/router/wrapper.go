package router

import (
	"sync/atomic"

	"github.com/goose-network/goose/internal/core"
	"github.com/goose-network/goose/internal/metrics"
	"github.com/goose-network/goose/internal/selector"
)

// outboundWrapper decorates a plugin-built Outbound with the engine's id,
// protocol, and live stats (sourced from the metrics store). Plugins return
// a bare Outbound; the router wraps it so selectors/filters see consistent
// identity and performance data.
//
// The wrapper is transparent to the optional ChainDialer capability:
// dialOutbound unwraps via Unwrap() and asserts ChainDialer on the inner
// outbound, so multi-hop chains actually dial through the previous hop
// instead of collapsing to a direct dial.
type outboundWrapper struct {
	core.Outbound
	id       string
	protocol string
	ms       *metrics.Store
}

func (w *outboundWrapper) ID() string       { return w.id }
func (w *outboundWrapper) Protocol() string { return w.protocol }

// Unwrap returns the underlying plugin-built Outbound, so callers can assert
// optional capabilities (e.g. ChainDialer) on the real implementation.
func (w *outboundWrapper) Unwrap() core.Outbound { return w.Outbound }

// Stats returns the live performance snapshot from the metrics store,
// keyed by the outbound id. This is what least-latency / latency filters
// read on the hot path.
func (w *outboundWrapper) Stats() core.OutboundStats {
	return w.ms.Stats(w.id)
}

// fallbackSelector picks the first available outbound. It is used when a
// pool has no configured selector or as the default-chain selector.
type fallbackSelector struct{ n uint64 }

func (*fallbackSelector) Name() string { return "fallback" }
func (f *fallbackSelector) Pick(pool []core.Outbound, _ core.Metadata) (core.Outbound, error) {
	if len(pool) == 0 {
		return nil, selector.ErrEmptyPool
	}
	// cheap round-robin to avoid always hitting the first outbound.
	idx := atomic.AddUint64(&f.n, 1) - 1
	return pool[int(idx%uint64(len(pool)))], nil
}
