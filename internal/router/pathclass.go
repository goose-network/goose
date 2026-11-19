// Path classification: one leg's error mapped to the phase/reason vocabulary
// core.HTTPProbe uses, bounded to what that leg could have reached. The
// helpers here are the router's side of core.PathRecorder — producers on the
// dispatch path install classifyLeg so a recorded leg carries a classification
// that cannot overstate it.
package router

import (
	"github.com/goose-network/goose/internal/core"
)

// classifyLeg maps one leg's error to its (phase, reason). It reuses
// classifyProbeFailure's vocabulary but bounds the phase to what the leg could
// have reached: classifyProbeFailure's defaults and substring rules are
// calibrated against a whole-request trace ("resolver not configured" → dns,
// "every member dial failed" → connect), and a single leg has no such trace,
// so an unbounded answer would assert a phase this leg never touched. The
// reason is always kept — it comes from the typed and substring rules alone
// and is leg-independent.
func classifyLeg(info core.LegInfo, err error) (phase, reason string) {
	if err == nil {
		return "", ""
	}
	phase, reason = classifyProbeFailure("", err, phaseTimings{})
	switch phase {
	case phaseDNS:
		// Only a leg that left the engine host ran its resolver.
		if !info.FromLocal {
			phase = ""
		}
	case phaseTLS, phaseHeaders, phaseBody:
		// Nothing beyond the proxy was attempted on a leg that did not end at
		// the target.
		if !info.ToTarget {
			phase = ""
		}
	case phaseProxy:
		// A bare stack dial to the target speaks no proxy protocol.
		if info.ToTarget && info.Kind == core.LegStack {
			phase = ""
		}
	}
	return phase, reason
}

// hopNodeID returns the path node for one chain hop, or "" for a hop with no
// server of its own. "direct" is the absence of a proxy, not one: Direct has
// no server and its DialThrough hands the previous conn back, so a node for it
// would be a vertex with no physical referent. A tunnel plugin
// (psiphon/tor/geph) still gets a node — the request really rode that
// outbound — with an empty Address when its shape is not a dialable host:port.
func hopNodeID(rec *core.PathRecorder, o core.Outbound) string {
	if rec == nil || o.Protocol() == "direct" {
		return ""
	}
	return rec.AddProxy(o.ID(), o.Address())
}

// chainDialer unwraps o and asserts the optional chaining capability — the
// same steps dialOutbound performed inline before the path recorder needed
// the answer one level up.
func chainDialer(o core.Outbound) (ChainDialer, bool) {
	inner := o
	if w, ok := o.(interface{ Unwrap() core.Outbound }); ok {
		inner = w.Unwrap()
	}
	cd, ok := inner.(ChainDialer)
	return cd, ok
}

// hopLegs is one chain hop's path bookkeeping, computed by dialLayers (which
// knows the whole hop list) and consumed by dialOutbound (which knows only
// this hop).
type hopLegs struct {
	hop    string // this hop's own node ID, "" when it has no server
	to     string // the node this hop's dial reaches toward
	target string // the dial string handed to the plugin
}
