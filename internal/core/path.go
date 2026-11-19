// The observed network path: one request's trip through the network as an
// adjacency list. The engine host, every proxy server the request actually
// rode, and the target are nodes; every leg the engine observed between them
// is an edge carrying that leg's own span, outcome and — on failure — its
// classification.
//
// The graph is strictly additive. Chain keeps the configured route identity
// the metrics store's reachability verdicts key off; the graph says what the
// dispatch path really did, including the attempts a pool failover burned and
// the legs a plugin dialed where the engine could not see.
//
// Honesty rules, all enforced here rather than left to producers:
//
//   - an address is only recorded when it is a valid host:port with a numeric
//     port; a plugin shape like "geph://tunnel" or "tor-circuit" yields an
//     empty Address, not a parsed fragment;
//   - a socket peer address is adopted only from a conn the leg dialed itself:
//     never from the conn it was handed, and never when the leg's own call
//     recorded other legs inside it — a nested route's socket belongs to the
//     member that dialed it, not to the hop that asked for the route;
//   - every leg the engine records is Observed: true, and a leg it could not
//     bracket is omitted with Partial set — never inferred from configuration.
//     Observed: false is reserved for a producer outside the engine;
//   - a leg that was not separately timed OMITS its duration on the wire (the
//     JSON key is absent, never a literal 0); a consumer must read absent as
//     "not separately timed", not as "instant".
package core

import (
	"context"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// NodeKind classifies a node's role on the path.
type NodeKind string

const (
	// NodeLocal is the engine host: where every path starts.
	NodeLocal NodeKind = "local"
	// NodeProxy is a proxy server — an engine outbound, or a bare server
	// address reached with no outbound behind it.
	NodeProxy NodeKind = "proxy"
	// NodeTarget is the request's destination.
	NodeTarget NodeKind = "target"
)

// Node IDs. The local and target nodes are singletons per graph, so their IDs
// are the constants; a proxy node's ID is NodeProxyPrefix plus the outbound ID
// (plus "#n" for the n-th failover attempt), and a server node's is
// NodeServerPrefix plus its address.
const (
	NodeLocalID  = "local"
	NodeTargetID = "target"

	NodeProxyPrefix  = "proxy:"
	NodeServerPrefix = "server:"
)

// LegKind classifies how a leg was established.
type LegKind string

const (
	// LegDial is the engine reaching a server over its own network.
	LegDial LegKind = "dial"
	// LegTunnel is a proxy handshake or tunnel stream over an existing conn.
	LegTunnel LegKind = "tunnel"
	// LegStack is a bare engine-stack dial with no proxy protocol behind it.
	LegStack LegKind = "stack"
)

// AddrSource says where a node's Address came from, which is what makes the
// address trustworthy: a socket address is the peer the engine really
// connected to, a configured one is what the config says the server is (and a
// proxy may sit in front of it).
type AddrSource string

const (
	AddrSocket     AddrSource = "socket"
	AddrConfigured AddrSource = "configured"
)

// Caps. Metric and probe rows are persisted forever — there is no retention
// or pruning in internal/metrics — so a graph is bounded regardless of how
// deep or how failover-heavy a configuration is. maxUpstreamDepth already
// bounds nesting and a pool's member count bounds attempts, so these only bite
// on a pathological config; what they guarantee is that one row's bbolt value
// cannot grow without limit.
const (
	MaxPathNodes    = 32
	MaxPathEdges    = 64
	MaxPathAttempts = 8
)

// PathNode is one vertex of the observed path.
type PathNode struct {
	// ID is unique within the graph: "local", "target", "proxy:<outboundID>",
	// "proxy:<outboundID>#<n>" for a repeat, or "server:<host:port>" for a
	// server reached with no outbound behind it.
	ID string `json:"id"`
	// Kind is one of NodeLocal, NodeProxy, NodeTarget.
	Kind NodeKind `json:"kind"`
	// OutboundID names the engine outbound this proxy server is; empty for
	// local and target nodes and for a server node no outbound was named for.
	OutboundID string `json:"outboundID,omitempty"`
	// Address is host:port, empty when it was not observable (a tunnel
	// protocol whose shape is not a dialable address).
	Address string `json:"address,omitempty"`
	// AddressSource says how Address was learned; omitted when Address is
	// empty.
	AddressSource AddrSource `json:"addressSource,omitempty"`
	// Host and Port are the requested destination, on the target node only.
	// Address may differ from them: a dial resolves the name, and the socket
	// peer the engine adopted is the resolved one.
	Host string `json:"host,omitempty"`
	Port string `json:"port,omitempty"`
	// Attempt is the 1-based failover attempt that produced this proxy node;
	// 0 when the node was not one of several attempts.
	Attempt int `json:"attempt,omitempty"`
}

// PathEdge is one directed leg of the path.
type PathEdge struct {
	// From and To are node IDs. From is never empty: the local node's ID is
	// written out so an edge list reads on its own.
	From string `json:"from"`
	To   string `json:"to"`
	// Kind is one of LegDial, LegTunnel, LegStack. It is persisted exactly as
	// the producer labelled it: only the producer knows what the leg did, so
	// the recorder never reclassifies one from what a call returned.
	Kind LegKind `json:"kind"`
	// Target is the dial string handed to this leg (host:port).
	Target string `json:"target,omitempty"`
	// Duration is this leg's own span. For a LegDial edge arriving at a proxy
	// node it spans the whole hop dial — the server connect plus the proxy's
	// handshake toward the next node — because a plugin performs both inside
	// one call; that is the same span core.HTTPProbe.RouteDial documents. A leg
	// that was not separately timed OMITS this field: the JSON key is absent, so
	// a consumer reads absent as "not separately timed", never as "instant"
	// (the in-memory Go value is 0 in that case, and omitempty keeps it off the
	// wire).
	Duration time.Duration `json:"duration,omitempty"`
	// Observed is true on every leg the engine recorded: an engine-recorded
	// leg was bracketed by definition, and a leg the engine could not bracket
	// is not emitted at all — the graph says so through Partial instead. The
	// field exists for a producer outside the engine that synthesizes a path
	// from configuration; no engine code path writes false.
	Observed bool `json:"observed"`
	// Collapsed is true when a chained hop could not be dialed through the
	// previous conn and re-dialed from the local network instead, dropping
	// the hop before it.
	Collapsed bool   `json:"collapsed,omitempty"`
	Success   bool   `json:"success"`
	Error     string `json:"error,omitempty"`
	// Phase and Reason classify a failed leg with the same vocabulary
	// core.HTTPProbe uses, bounded to what this leg could have reached.
	Phase  string `json:"phase,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// PathGraph is the adjacency list: every node, every leg between them.
type PathGraph struct {
	// Nodes is in path order: local first, proxies in dial order, target last.
	Nodes []PathNode `json:"nodes"`
	// Edges is in observation order.
	Edges []PathEdge `json:"edges"`
	// Partial is true when the graph may be missing legs: a plugin dialed
	// where the engine could not see, or a cap dropped an attempt. It does
	// NOT mean "some leg is untimed" — that is every successful hop.
	Partial bool `json:"partial,omitempty"`
	// Dropped counts the nodes a cap cut.
	Dropped int `json:"dropped,omitempty"`
}

// LegInfo tells a Classifier what one leg could have observed, so it cannot
// claim a phase the leg never reached.
type LegInfo struct {
	Kind LegKind
	// FromLocal is true when the leg left the engine host.
	FromLocal bool
	// ToTarget is true when the leg's destination is the request target.
	ToTarget bool
}

// Classifier maps one leg's error to its (phase, reason) slugs.
// internal/router installs its own; a recorder with none leaves Phase and
// Reason empty rather than guessing.
type Classifier func(info LegInfo, err error) (phase, reason string)

// Leg describes one bracketed dial: where it started, the server it reached,
// and the node beyond that server the hop tunnels toward.
type Leg struct {
	Kind LegKind
	// From is the node ID the leg starts at; "" is the local node.
	From string
	// To is the node ID of the server this leg reached, "" when the hop has
	// no server of its own (a direct hop) and the leg runs straight to
	// Through.
	To string
	// Through is the node ID the hop tunnels toward beyond To; "" for none.
	Through string
	// Target is the dial string handed to the plugin.
	Target string
	// Collapsed marks a chained hop that re-dialed from local.
	Collapsed bool
	// Untimed records Duration 0 even though the call was bracketed: the span
	// measured something other than this leg (a no-op closure standing in for
	// a leg the engine knows happened but cannot time).
	Untimed bool
}

// inflightEntry is one open Observe bracket. The token, not the ID, keys the
// pop: fn may rename the destination node mid-bracket (a nested route
// upgrading a bare server vertex), and a pop matching the pre-fn spelling
// would miss and strand the claim for the rest of the request.
type inflightEntry struct {
	id    string
	token uint64
}

// PathRecorder accumulates one request's observed path. It is created per
// request by the inbound listener, carried on Metadata.Path (so it survives
// the by-value copies into the ctx-less Bridge) and on the request context
// (so the dial seams that see no Metadata can find it).
//
// Every method is safe for concurrent use and is a no-op on a nil receiver,
// so a producer that does not observe — an exit-IP check, a provider fetch —
// pays one ctx.Value miss and nothing else.
type PathRecorder struct {
	mu       sync.Mutex
	classify Classifier
	nodes    []PathNode
	edges    []PathEdge
	byID     map[string]int    // node ID -> index in nodes
	byAddr   map[string]string // address -> node ID
	// inflight is the stack of Observe brackets currently open on this
	// goroutine's nesting (innermost last). A nested route resolving an arrival
	// by address belongs to the bracket that is dialing that address right now,
	// which registration order alone cannot say: a chain names hop i+1 before
	// hop i dials, and a pool attempt's own bracket is the winner's context.
	// An entry is keyed by its token rather than by the destination ID alone,
	// because fn may rename that destination and the closing pop must still find
	// the entry it pushed; inflightSeq hands out the tokens under mu.
	inflight    []inflightEntry
	inflightSeq uint64
	// renamed maps a node ID the graph no longer has to the ID that replaced it.
	// It exists for the one class of state renameLocked cannot reach: ID strings
	// a caller captured BEFORE the rename. A producer composes its Leg from the
	// IDs it holds then, and fn — the very call the leg brackets — can rename one
	// of them (a nested route's AddProxy adopting a bare server:<addr> vertex for
	// an outbound identity); a caller's exclusion argument has the same problem.
	// renameLocked rewrites the nodes, both indexes, every recorded edge endpoint
	// and the in-flight claims, but the leg being recorded is not in that edge
	// list yet, so without this lookup the byID guards in Observe would reject the
	// bracket's own destination as unknown and drop the leg it really observed.
	// Lazily created at the first rename, and bounded by the number of renames,
	// which is bounded by MaxPathNodes: an entry is added only when a node is
	// adopted, never per leg.
	renamed  map[string]string
	lastNode string // where the most recent successful leg arrived
	dropped  int
	partial  bool
	sealed   bool
	snapshot *PathGraph
}

// NewPathRecorder returns an empty recorder.
func NewPathRecorder() *PathRecorder {
	return &PathRecorder{
		byID:   map[string]int{},
		byAddr: map[string]string{},
	}
}

// SetClassifier installs the leg classifier. Called once before any leg is
// observed (Router.Dial, and the router's own probe producers), so the only
// ordering it needs is happening-before the first Observe.
func (r *PathRecorder) SetClassifier(c Classifier) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.classify = c
	r.mu.Unlock()
}

// AddLocal records the engine host as node "local". Idempotent.
func (r *PathRecorder) AddLocal() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.addLocked(PathNode{ID: NodeLocalID, Kind: NodeLocal})
}

// AddTarget records the request's destination and returns its node ID (always
// NodeTargetID). Idempotent; a second call with a different host/port is
// ignored, because one request has one destination.
func (r *PathRecorder) AddTarget(host, port string) string {
	if r == nil {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.byID[NodeTargetID]; ok {
		return NodeTargetID
	}
	n := PathNode{ID: NodeTargetID, Kind: NodeTarget, Host: host, Port: port}
	if addr := net.JoinHostPort(host, port); validHostPort(addr) {
		n.Address, n.AddressSource = addr, AddrConfigured
	}
	if r.addLocked(n) == "" {
		return ""
	}
	return NodeTargetID
}

// TargetNodeID returns the target node's ID when one exists, else "".
func (r *PathRecorder) TargetNodeID() string {
	if r == nil {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.byID[NodeTargetID]; !ok {
		return ""
	}
	return NodeTargetID
}

// TargetNodeIDFor returns the target node's ID when one exists AND it names
// this destination, else "". The identity check matters: a nested upstream
// route shares the outer request's recorder while dialing a different address
// (the outer hop's server), so "the graph's target node" is not automatically
// "where this dial was headed".
func (r *PathRecorder) TargetNodeIDFor(host, port string) string {
	if r == nil {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	i, ok := r.byID[NodeTargetID]
	if !ok || i >= len(r.nodes) {
		return ""
	}
	if r.nodes[i].Host != host || r.nodes[i].Port != port {
		return ""
	}
	return NodeTargetID
}

// AddProxy records (or finds) the proxy-server node for outboundID and returns
// its ID, or "" when the node cap is hit or the recorder is sealed. address is
// adopted only when it is a valid host:port.
//
// An address another node already stands for does NOT merge two outbounds into
// one vertex: a bare "server:<address>" node — which has no outbound identity
// of its own — is upgraded in place, because that is one physical server the
// config just named, but a node belonging to a different outbound or a
// different attempt keeps its identity and the new one is registered beside
// it. Two vertices naming one server is honest; one vertex wearing two
// outbounds' legs attributes one member's failure to the other.
func (r *PathRecorder) AddProxy(outboundID, address string) string {
	if r == nil {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.addProxyLocked(outboundID, address, 0)
}

// AddProxyAttempt is AddProxy for one attempt of a failover loop: attempt is
// 1-based, and attempts past the first get their own node
// ("proxy:<id>#<n>") so every failed member keeps its own address, error and
// classification instead of being overwritten by the member that won. Returns
// "" past MaxPathAttempts or the node cap.
func (r *PathRecorder) AddProxyAttempt(outboundID, address string, attempt int) string {
	if r == nil {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sealed {
		return ""
	}
	if attempt > MaxPathAttempts {
		r.dropped++
		return ""
	}
	return r.addProxyLocked(outboundID, address, attempt)
}

// AddServer records (or finds) the node for the server at address, for a dial
// with no outbound behind it (an upstream route resolving to "direct").
// Returns the existing node's ID when one is already registered for that
// address — so an outbound whose upstream_proxy is direct produces one node,
// not a duplicate — else "server:<address>", or "" when address is not a valid
// host:port, the cap is hit or the recorder is sealed. An address several
// vertices name resolves to the one whose dial bracket is in flight, else to
// the most recent registrant (addLocked).
func (r *PathRecorder) AddServer(address string) string {
	if r == nil {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if id := r.inflightForAddrLocked(address, ""); id != "" {
		return id
	}
	if id, ok := r.byAddr[address]; ok {
		return id
	}
	return r.addServerLocked(address)
}

// inflightForAddrLocked returns the innermost open Observe bracket whose
// destination node stands for address, or "" when no open bracket claims it.
// An arrival resolved while a bracket is in flight belongs to the node that
// bracket is dialing - the engine is talking to THAT server right now - even
// when a later registration (a chain's next hop, a nested route's member)
// stole the address index. exclude names the vertex the resolution is the
// arrival FROM, which may itself share the address and must not win. The walk
// resolves each entry by its id, which renameLocked keeps current across a
// mid-bracket rename; the entry's token keys only the closing pop.
func (r *PathRecorder) inflightForAddrLocked(address, exclude string) string {
	if address == "" {
		return ""
	}
	for i := len(r.inflight) - 1; i >= 0; i-- {
		e := r.inflight[i]
		if e.id == exclude {
			continue
		}
		if j, ok := r.byID[e.id]; ok && r.nodes[j].Address == address {
			return e.id
		}
	}
	return ""
}

// ServerNodeForArrival resolves the node an arrival at address belongs to,
// excluding from (the vertex the nested route delivered FROM, which may itself
// name the same address as the hop that asked for the route). It prefers the
// innermost open Observe bracket dialing that address over the address index:
// registration order cannot say who the engine is talking to, the dial in
// flight can.
func (r *PathRecorder) ServerNodeForArrival(address, from string) string {
	if r == nil {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// `from` is the caller's, captured before the dial it names, and that dial can
	// rename the node: the in-flight entries renameLocked keeps current, so an
	// un-resolved pre-rename spelling simply stops matching and the self-loop
	// exclusion is lost — the arrival would resolve to the vertex it arrived FROM
	// and the leg would be dropped as a self-arrival. That is a live shape in
	// tree, so resolveLocked is load-bearing rather than uniformity: callers pass
	// whatever AddProxy/AddProxyAttempt returned (upstream.go's
	// dialUpstreamOutbound and dialPoolSnapshot, via noteUpstreamArrival), and for
	// an EMPTY outbound ID that is the bare "server:<addr>" spelling
	// addProxyLocked's empty-ID fall-through mints — precisely the vertex its
	// upgrade gate renames when a named outbound claims the same address. Empty
	// IDs reach the recorder from cmd/goose's loadConfig (config-file outbounds
	// are stored with no ID validation) and from provider updates (IDs forwarded
	// unchanged); only the HTTP API rejects "".
	if id := r.inflightForAddrLocked(address, r.resolveLocked(from)); id != "" {
		return id
	}
	if id, ok := r.byAddr[address]; ok {
		return id
	}
	return r.addServerLocked(address)
}

// LastNodeID returns the ID of the node the most recent successful leg arrived
// at, or "" when no leg has succeeded yet (so a caller's From falls back to the
// local node).
func (r *PathRecorder) LastNodeID() string {
	if r == nil {
		return ""
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastNode
}

// NoteLocalDial records that the next leg leaves the engine host, forgetting
// where the last one arrived. A producer calls it before a dial it knows is a
// fresh one from local rather than a continuation of the path so far: a
// collapsed re-dial, a probe redirect that opens its own connection. Without
// it the recorder would read such a leg as a nested route re-reaching a server
// the path already arrived at, and suppress an arrival that really happened.
// Inert on a nil receiver and once Snapshot has sealed the recorder.
func (r *PathRecorder) NoteLocalDial() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sealed {
		return
	}
	r.lastNode = ""
}

// NotePartial records that a leg the engine knows about could not be observed.
// Like every other mutator it is inert once Snapshot has sealed the recorder:
// the graph Snapshot already handed out is the one readers keep, so a
// completeness claim made afterwards has nothing left to attach to.
func (r *PathRecorder) NotePartial() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sealed {
		return
	}
	r.partial = true
}

// Observe times fn and records the leg it performed. The returned conn is fn's,
// unmodified, and leg.Kind is recorded exactly as the producer labelled it:
// only the producer knows what the leg did, and a call that hands back the conn
// it was given is still the handshake it was bracketed for.
//
// via is the conn the leg was handed, nil when it dialed from the engine's own
// stack. It gates address adoption and nothing else: a socket peer describes
// the conn that produced it, so a leg handed one cannot vouch for an address —
// in a chain the engine holds exactly one socket and its peer is an earlier
// hop's server.
func (r *PathRecorder) Observe(leg Leg, via net.Conn, fn func() (net.Conn, error)) (net.Conn, error) {
	if r == nil {
		return fn()
	}
	// What fn does inside the bracket decides whether this leg may adopt an
	// address, and it is only visible as a change in the recorder's own state:
	// a plugin that reached its server through an upstream route dials that
	// route — recording its legs — inside this call.
	r.mu.Lock()
	edgesBefore, lastBefore, partialBefore := len(r.edges), r.lastNode, r.partial
	// Publish this bracket's destination to nested resolutions while fn runs:
	// the engine is dialing leg.To's server right now, so an arrival at that
	// address belongs to that node. Registration order alone cannot say this -
	// a chain names hop i+1 before hop i dials, and a route member registers
	// beside the hop that is riding it.
	dest := leg.To
	if dest == "" {
		dest = leg.Through
	}
	pushed := dest != ""
	// The token, not dest, keys the pop below: fn can rename dest out from
	// under this bracket, and a pop matching the pre-fn spelling would leave
	// the entry on the stack for the rest of the request, where it keeps
	// answering address resolutions for a server the engine already left.
	//
	// The ID is pushed RESOLVED: the caller captured dest before this lock was
	// taken, so a rename in between (a chain names hop i+1 while hop i still
	// dials, adopting the bare vertex hop i got) leaves a spelling the graph no
	// longer has. renameLocked re-points entries it can match, but it matches by
	// the pre-rename spelling, and inflightForAddrLocked looks the entry up in
	// byID — a dead one misses both, so every arrival inside this bracket would
	// fall back to the address index's last owner.
	var token uint64
	if pushed {
		token = r.inflightSeq
		r.inflightSeq++
		r.inflight = append(r.inflight, inflightEntry{id: r.resolveLocked(dest), token: token})
	}
	r.mu.Unlock()

	start := time.Now()
	conn, err := fn()
	dur := time.Since(start)

	r.mu.Lock()
	defer r.mu.Unlock()
	if pushed {
		for i := len(r.inflight) - 1; i >= 0; i-- {
			if r.inflight[i].token == token {
				r.inflight = append(r.inflight[:i], r.inflight[i+1:]...)
				break
			}
		}
	}
	if r.sealed {
		return conn, err
	}
	if len(r.edges) >= MaxPathEdges {
		r.partial = true
		return conn, err
	}
	nested := len(r.edges) != edgesBefore || r.lastNode != lastBefore || r.partial != partialBefore

	// Resolve the producer's endpoint strings before anything reads them as IDs.
	// They were captured before fn ran, and fn can rename one of them: a nested
	// route's AddProxy adopts the bare server vertex the producer named, and
	// renameLocked cannot reach these locals. Every consumer below — the To/Through
	// swap, the suppression test (which compares against lastNode, itself
	// rename-rewritten) and the byID guards — needs the current spelling, or a leg
	// this bracket really observed is dropped for a rename that lost nothing.
	// leg.From is still read raw where it means producer intent rather than a node
	// lookup (the suppression condition and classifyLocked's FromLocal).
	from := r.resolveLocked(leg.From)
	if from == "" {
		from = NodeLocalID
	}
	kind := leg.Kind
	to, through := r.resolveLocked(leg.To), r.resolveLocked(leg.Through)

	// A hop with no server of its own: the leg runs straight to Through.
	if to == "" {
		to, through = through, ""
	}
	// Nested-upstream suppression: the plugin reached this server through a
	// route the engine already recorded (lastNode is that server), so what this
	// call adds is only the hop's own handshake toward Through. Recording the
	// arrival as well would claim the engine reached that server directly.
	//
	// A collapsed re-dial is NOT that case even though it matches the shape:
	// leg.Collapsed says the hop threw the previous tunnel away and dialed from
	// the engine host, so suppressing its arrival would claim the hop rode a
	// path it discarded and hide the one leg that really left local.
	//
	// Suppression that leaves nothing to record (no Through: the bracketed call
	// IS the nested route) can still end in an error — the hop's handshake
	// toward the target died inside the plugin's call. That failure belongs to
	// no edge this recorder brackets, so it surfaces as Partial rather than
	// being dropped on the floor; see the branch below.
	suppressed := false
	if leg.From == "" && !leg.Collapsed && to != "" && r.lastNode == to {
		if through == "" {
			// There is no handshake to record either: this bracketed call IS
			// the nested route, which already recorded its own arrival at `to`.
			// An edge here would be a second local dial of a server the engine
			// reached through a tunnel.
			//
			// But the call can still fail, and then what failed is the hop's
			// handshake TOWARD the target, inside the plugin's call: no leg this
			// recorder brackets covers it, and no honest from→to edge exists for
			// it (recording a failed local→to would contradict the adjacent
			// successful arrival at `to` the nested route just drew). The failure
			// would otherwise vanish from the graph entirely — an all-green route
			// for a request that never arrived — so the graph says it is missing
			// one leg instead, the way every other un-placeable leg here does.
			if err != nil {
				r.partial = true
			}
			return conn, err
		}
		from, to, through = to, through, ""
		kind = LegTunnel
		suppressed = true
	}
	if _, ok := r.byID[to]; to == "" || !ok {
		r.partial = true
		return conn, err
	}
	if _, ok := r.byID[from]; !ok {
		if _, ok := r.byID[NodeLocalID]; ok {
			from = NodeLocalID
		} else {
			r.partial = true
			return conn, err
		}
	}
	if from == to {
		// A self-loop: the leg's own destination is where the previous leg
		// already arrived (a cyclic upstream reference, bounded by
		// maxUpstreamDepth). Recording it would claim a leg between one
		// server and itself, so the graph says the path is incomplete
		// instead of inventing topology.
		r.partial = true
		return conn, err
	}
	if through != "" {
		if _, ok := r.byID[through]; !ok {
			// The producer named a second physical leg toward a node the graph
			// does not have — nothing else can make an ID unknown mid-call than
			// a nested route recording inside fn. The leg is real and is being
			// dropped, so say the graph may be incomplete rather than quietly
			// losing it.
			r.partial = true
			through = ""
		}
	}

	e := PathEdge{
		From: from, To: to, Kind: kind, Target: leg.Target,
		Duration: dur, Observed: true, Collapsed: leg.Collapsed,
		Success: err == nil,
	}
	if leg.Untimed {
		e.Duration = 0
	}
	if err != nil {
		e.Error = err.Error()
		e.Phase, e.Reason = r.classifyLocked(LegInfo{
			Kind:      kind,
			FromLocal: leg.From == "",
			ToTarget:  to == NodeTargetID,
		}, err)
	}
	r.edges = append(r.edges, e)

	// A socket address belongs to the leg that dialed it, so this call adopts
	// one only when it dialed from the engine's own stack (via == nil), added
	// the leg itself rather than being the nested route that already recorded
	// and adopted it (nested, suppressed), and reached `to` — the far end of
	// the leg this call really established. A chained handshake fails the
	// first test: the engine holds one socket per chain and its peer is an
	// earlier hop's server, which that hop's own leg already adopted, so
	// taking it here would stamp another server's address onto this hop and
	// claim it was socket-verified.
	if via == nil && !nested && !suppressed {
		if addr := peerAddr(conn); addr != "" {
			r.adoptSocketLocked(to, addr)
		}
	}
	if err != nil {
		return conn, err
	}
	if through != "" && through != to {
		if len(r.edges) < MaxPathEdges {
			// One bracketed call covered two physical legs: the plugin dialed its
			// server and handshaked toward the next node inside the same call, so
			// the second leg is real but not separately timed.
			r.edges = append(r.edges, PathEdge{
				From: to, To: through, Kind: LegTunnel, Target: leg.Target,
				Observed: true, Success: true,
			})
			r.lastNode = through
			return conn, err
		}
		// The arrival edge filled the list exactly, so this call's second leg has
		// nowhere to go. The graph is missing a leg it knows about, which is what
		// Partial says; lastNode stays at To, because as far as the graph is
		// concerned nothing arrived at Through.
		r.partial = true
	}
	r.lastNode = to
	return conn, err
}

// Snapshot returns the immutable, deep-copied graph, memoized so a probe row
// and its metric row carry the identical pointer. It seals the recorder: after
// the first call every mutator is a no-op, which is what makes the copy safe
// to marshal on the metrics writer's goroutine long after Record returned.
// Returns nil when nothing was observed, so the row's omitempty field
// disappears rather than serializing an empty graph.
func (r *PathRecorder) Snapshot() *PathGraph {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.snapshot != nil {
		return r.snapshot
	}
	r.sealed = true
	if len(r.nodes) == 0 && len(r.edges) == 0 {
		return nil
	}
	// Path order: local first, proxies in dial order, target last. The target
	// node is created up front — before any dial — so a total failure still
	// names the destination, which puts it in the middle of nodes.
	nodes := make([]PathNode, 0, len(r.nodes))
	var target *PathNode
	for i := range r.nodes {
		if r.nodes[i].Kind == NodeTarget {
			t := r.nodes[i]
			target = &t
			continue
		}
		nodes = append(nodes, r.nodes[i])
	}
	if target != nil {
		nodes = append(nodes, *target)
	}
	edges := make([]PathEdge, len(r.edges))
	copy(edges, r.edges)
	r.snapshot = &PathGraph{
		Nodes:   nodes,
		Edges:   edges,
		Partial: r.partial,
		Dropped: r.dropped,
	}
	return r.snapshot
}

// addLocked registers n unless a node with its ID already exists, and returns
// the ID (or "" when the cap is hit).
func (r *PathRecorder) addLocked(n PathNode) string {
	if r.sealed {
		if _, ok := r.byID[n.ID]; ok {
			return n.ID
		}
		return ""
	}
	if _, ok := r.byID[n.ID]; ok {
		return n.ID
	}
	if len(r.nodes) >= MaxPathNodes {
		r.dropped++
		return ""
	}
	r.byID[n.ID] = len(r.nodes)
	r.nodes = append(r.nodes, n)
	// A `renamed` key may only name an ID the graph no longer has. This
	// registration just made n.ID live again — the rename that mapped it away
	// dropped byID[oldID], and adoptSocketLocked can drop the byAddr key too, so
	// both unifying lookups miss and a fresh vertex is minted under the old
	// spelling. Leaving the mapping in place would hijack every later leg naming
	// this live vertex onto the node the dead one was renamed to. Cleared only on
	// the successful-registration path: an ID that was already registered, a
	// node the cap refused, and a sealed recorder leave the graph as it was.
	delete(r.renamed, n.ID)
	if n.Address != "" {
		// The MOST RECENTLY registered vertex for an address owns the index.
		// Two vertices can name one physical server — pool members that share a
		// server, or the same hop reached by two attempts — and they are kept
		// apart (different outbound identities, different attempt numbers), but
		// AddServer resolves by address only. Whoever registered last is the one
		// the engine is currently talking to, so that is the one a later arrival
		// belongs to: a pool failing over from member 1 to member 2 at the same
		// server address must draw the winning attempt's arrival into the winner
		// vertex, not into the loser's.
		r.byAddr[n.Address] = n.ID
	}
	return n.ID
}

// addProxyLocked is AddProxy/AddProxyAttempt: attempt 0 or 1 is the hop's own
// node, later attempts get a numbered one.
func (r *PathRecorder) addProxyLocked(outboundID, address string, attempt int) string {
	if outboundID == "" {
		// No outbound was named for this hop; a bare server address is all the
		// graph can say about it.
		return r.addServerLocked(address)
	}
	id := NodeProxyPrefix + outboundID
	if attempt > 1 {
		id = NodeProxyPrefix + outboundID + "#" + strconv.Itoa(attempt)
	}
	if r.sealed {
		// The published graph is fixed. An ID it already holds is still
		// answered — sealing stops new state, it does not make the recorder lie
		// about the graph it has — but nothing is adopted, renamed or indexed.
		if _, ok := r.byID[id]; ok {
			return id
		}
		return ""
	}
	if existing, ok := r.byID[id]; ok {
		r.adoptConfiguredLocked(existing, address)
		return id
	}
	if address != "" {
		if owner, ok := r.byAddr[address]; ok && owner != id {
			// Only a vertex with no outbound identity of its own — the bare
			// "server:<address>" node a direct upstream route registered — is
			// upgraded in place: naming the outbound that owns that address is
			// new information about the same physical server, and the edges that
			// referenced it are rewritten to the new ID. The target node has no
			// OutboundID either, so the server prefix is what tells them apart;
			// renaming the destination the request is headed to would be worse
			// than a duplicate vertex.
			//
			// An owner that IS another proxy identity — a different outbound, or
			// another attempt of one — keeps its vertex. Two outbounds really did
			// dial one server (a pool of same-address members with different
			// credentials, a chain whose hops share a relay), and folding them
			// together hands one member's legs, address and errors to the other:
			// the loser of a failover disappears and its classification is
			// published on the winner.
			if i, ok := r.byID[owner]; ok && strings.HasPrefix(owner, NodeServerPrefix) && r.nodes[i].OutboundID == "" {
				r.renameLocked(owner, id)
				if j, ok := r.byID[id]; ok {
					r.nodes[j].OutboundID = outboundID
					r.nodes[j].Attempt = attempt
				}
				return id
			}
		}
	}
	n := PathNode{ID: id, Kind: NodeProxy, OutboundID: outboundID, Attempt: attempt}
	if validHostPort(address) {
		n.Address, n.AddressSource = address, AddrConfigured
	}
	return r.addLocked(n)
}

// addServerLocked registers the node for a server reached with no outbound
// behind it, unifying with any node already registered for that address.
func (r *PathRecorder) addServerLocked(address string) string {
	if !validHostPort(address) {
		return ""
	}
	if id := r.inflightForAddrLocked(address, ""); id != "" {
		return id
	}
	if id, ok := r.byAddr[address]; ok {
		return id
	}
	return r.addLocked(PathNode{
		ID:            NodeServerPrefix + address,
		Kind:          NodeProxy,
		Address:       address,
		AddressSource: AddrConfigured,
	})
}

// adoptConfiguredLocked fills in an existing node's address when it has none.
// A socket-learned address is never overwritten by a configured one.
func (r *PathRecorder) adoptConfiguredLocked(i int, address string) {
	if i < 0 || i >= len(r.nodes) || !validHostPort(address) {
		return
	}
	if r.nodes[i].Address != "" {
		return
	}
	r.nodes[i].Address, r.nodes[i].AddressSource = address, AddrConfigured
	// The address is a fact about this node, but filling it in is not
	// registering a vertex: the index stays with whoever registered the address
	// (addLocked), so an adoption never repoints what AddServer resolves to.
	if _, taken := r.byAddr[address]; !taken {
		r.byAddr[address] = r.nodes[i].ID
	}
}

// adoptSocketLocked records the address the engine really connected to.
func (r *PathRecorder) adoptSocketLocked(id, address string) {
	i, ok := r.byID[id]
	if !ok || i >= len(r.nodes) {
		return
	}
	n := &r.nodes[i]
	if n.Address == address && n.AddressSource == AddrSocket {
		return
	}
	if n.Address != "" {
		// Drop only the index entry this node owns: another vertex may name the
		// same physical server, and deleting its entry would un-register it.
		if owner, ok := r.byAddr[n.Address]; ok && owner == id {
			delete(r.byAddr, n.Address)
		}
	}
	n.Address, n.AddressSource = address, AddrSocket
	// The socket peer is a fact about this node, not a registration: the index
	// stays with whoever registered the address (addLocked), so adopting a peer
	// address here does not repoint what AddServer resolves to.
	if _, taken := r.byAddr[address]; !taken {
		r.byAddr[address] = n.ID
	}
}

// resolveLocked maps an ID captured before a rename onto the node's current
// spelling, returning id unchanged when nothing renamed it ("" resolves to "":
// the absence of a node is not a rename). Callers hold r.mu. It is the read side
// of r.renamed — the only way to make a caller's pre-fn string mean the node it
// meant when the caller composed it.
func (r *PathRecorder) resolveLocked(id string) string {
	if id == "" || r.renamed == nil {
		return id
	}
	// A live vertex wins over the map: r.renamed keys dead IDs only, so a
	// mapping that names a node the graph HAS is stale (the spelling came back,
	// a resurrection addLocked clears but no caller may depend on). Resolving it
	// would hand the leg to the other node and orphan this one.
	if _, live := r.byID[id]; live {
		return id
	}
	if cur, ok := r.renamed[id]; ok {
		return cur
	}
	return id
}

// renameLocked rewrites a node's ID everywhere: the node, both indexes, and
// every edge that referenced the old one.
func (r *PathRecorder) renameLocked(oldID, newID string) {
	i, ok := r.byID[oldID]
	if !ok {
		return
	}
	if _, taken := r.byID[newID]; taken {
		return
	}
	if r.renamed == nil {
		r.renamed = map[string]string{}
	}
	// Re-point the entries that already lead HERE before adding the new one, so
	// the map stays one hop deep: a caller holding the spelling from two renames
	// ago must land on a node the graph still has, never on an ID that is itself
	// dead. Chain-free by construction, which is what makes resolveLocked a
	// single lookup.
	for stale, cur := range r.renamed {
		if cur == oldID {
			r.renamed[stale] = newID
		}
	}
	r.renamed[oldID] = newID
	delete(r.byID, oldID)
	r.byID[newID] = i
	r.nodes[i].ID = newID
	if addr := r.nodes[i].Address; addr != "" {
		r.byAddr[addr] = newID
	}
	for j := range r.edges {
		if r.edges[j].From == oldID {
			r.edges[j].From = newID
		}
		if r.edges[j].To == oldID {
			r.edges[j].To = newID
		}
	}
	if r.lastNode == oldID {
		r.lastNode = newID
	}
	// An open Observe bracket may be dialing this very node: its entry names the
	// destination by ID, and a rename must not void the in-flight claim — the
	// bracket is dialing the same physical server under a new identity, so an
	// arrival resolved inside it still belongs here. Left stale, the entry would
	// miss byID and the resolution would fall back to the last-owner address
	// index, handing the leg to whichever vertex registered the address most
	// recently rather than to the node being dialed.
	for i := range r.inflight {
		if r.inflight[i].id == oldID {
			r.inflight[i].id = newID
		}
	}
}

// classifyLocked applies the installed classifier, if any.
func (r *PathRecorder) classifyLocked(info LegInfo, err error) (phase, reason string) {
	if r.classify == nil || err == nil {
		return "", ""
	}
	return r.classify(info, err)
}

// pathRecorderKey is the context key carrying the request's *PathRecorder.
type pathRecorderKey struct{}

// WithPathRecorder attaches rec to ctx for the dial seams that see no
// Metadata: CheckDial's callers and routeDialer.DialUpstream, which builds a
// fresh Metadata of its own.
func WithPathRecorder(ctx context.Context, rec *PathRecorder) context.Context {
	if rec == nil {
		return ctx
	}
	return context.WithValue(ctx, pathRecorderKey{}, rec)
}

// PathRecorderFromContext returns ctx's recorder, or nil.
func PathRecorderFromContext(ctx context.Context) *PathRecorder {
	if ctx == nil {
		return nil
	}
	rec, _ := ctx.Value(pathRecorderKey{}).(*PathRecorder)
	return rec
}

// validHostPort reports whether s is a host:port with a numeric port in range.
// net.SplitHostPort alone is not enough: it accepts "geph://tunnel" as host
// "geph", port "//tunnel", and a tunnel protocol's address shape is exactly
// the kind of string a plugin's Address() returns.
func validHostPort(s string) bool {
	if s == "" {
		return false
	}
	host, port, err := net.SplitHostPort(s)
	if err != nil || host == "" || strings.ContainsAny(host, "/ \t\r\n") {
		return false
	}
	n, err := strconv.Atoi(port)
	return err == nil && n > 0 && n <= 65535
}

// peerAddr returns conn's remote address when it is a valid host:port, else
// "" (a unix socket, a pipe, an in-memory tunnel conn).
func peerAddr(conn net.Conn) string {
	if conn == nil {
		return ""
	}
	a := conn.RemoteAddr()
	if a == nil {
		return ""
	}
	s := a.String()
	if !validHostPort(s) {
		return ""
	}
	return s
}
