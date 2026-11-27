// chainMember is a pool member that stands for a whole chain: a selection-only
// token the router attaches to a pool's candidate list when the pool names
// ChainIDs. Filters and selectors see it as one outbound (identity = the chain
// ID, egress attributes = the exit hop, stats = an aggregate over the hops);
// dispatch never dials it — it splices the member's pre-pinned hops into the
// hop list and dials those (see dialLayersExpanding / dialPoolSnapshot).
//
// The hops are pinned at rebuild time from the chain's config spec: every
// layer must resolve to a built outbound that no pool shadows, so the member
// always expands to exactly the proved path. The pinned slice is immutable
// (rebuild replaces pools wholesale), so a dispatch holding a snapshot can
// read it without locks.
package router

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/goose-network/goose/internal/core"
	"github.com/goose-network/goose/internal/metrics"
)

type chainMember struct {
	id   string
	hops []core.Outbound
	// firstHopID/firstHopAddr name the entry hop: the only hop the engine's
	// local network dials, so reachability narrowing keys on it.
	firstHopID, firstHopAddr string
	// exit is the last hop; egress attributes delegate to it.
	exit core.Outbound
	ms   *metrics.Store
}

func (c *chainMember) ID() string { return c.id }

// Protocol/Address/Location delegate to the exit hop: an egress filter asks
// "where does traffic leave the proxy network", and for a chain that is the
// last hop's server, not the entry's.
func (c *chainMember) Protocol() string         { return c.exit.Protocol() }
func (c *chainMember) Address() string          { return c.exit.Address() }
func (c *chainMember) Location() *core.Location { return c.exit.Location() }

// Stats aggregates the hops' live stats into an approximate path cost:
//
//   - RecentLatency: the sum of the hops' smoothed latencies, each unmeasured
//     hop counted as 1s (the selector's own unmeasured default) — except when
//     NO hop is measured, where the sum is 0 so a fresh chain reads as
//     unmeasured and member order breaks the tie.
//   - SuccessRate: the minimum over MEASURED hops (a chain is as reliable as
//     its weakest measured link). Unmeasured hops are skipped (fail-open,
//     matching an unmeasured outbound); when none is measured the rate is 0,
//     exactly what Stats returns for an unmeasured outbound. "Measured" means
//     the window holds samples or a success was ever seen — SuccessRate alone
//     cannot distinguish a real 0 from unmeasured.
//   - LastSeen: the newest success across the hops.
func (c *chainMember) Stats() core.OutboundStats {
	var sum time.Duration
	minRate := 1.0
	anyMeasured, anyRate := false, false
	var lastSeen time.Time
	for _, h := range c.hops {
		st := c.ms.Stats(h.ID())
		lat := st.RecentLatency
		if lat == 0 {
			lat = time.Second
		} else {
			anyMeasured = true
		}
		sum += lat
		if len(st.Samples) > 0 || !st.LastSeen.IsZero() {
			anyRate = true
			if st.SuccessRate < minRate {
				minRate = st.SuccessRate
			}
		}
		if st.LastSeen.After(lastSeen) {
			lastSeen = st.LastSeen
		}
	}
	if !anyMeasured {
		sum = 0
	}
	if !anyRate {
		minRate = 0
	}
	return core.OutboundStats{RecentLatency: sum, SuccessRate: minRate, LastSeen: lastSeen}
}

// DialContext must never run: dispatch expands the member into its hops
// before dialing. A token reaching a dial is a routing bug, so fail loudly
// instead of silently dialing the exit server from the local network.
func (c *chainMember) DialContext(_ context.Context, _ core.Network, _ string) (net.Conn, error) {
	return nil, fmt.Errorf("router: chain member %s must be expanded, not dialed", c.id)
}

// No Unwrap(): chainDialer must never find a ChainDialer on the token — it is
// not an outbound and never enters r.outbounds.
