// Tests for the observed-network-path graph: the node IDs and address honesty
// rules, what one bracketed Observe call records, the caps that bound a
// persisted row, and the snapshot/seal contract that lets the metrics writer
// marshal a graph long after the request finished.
//
// This is an internal (package core) test file on purpose: the honesty rules
// live in unexported helpers — validHostPort, peerAddr, addLocked,
// renameLocked — and the deep-copy assertion has to compare against the
// recorder's own backing arrays.
package core

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

// pthAddr is a net.Addr with a caller-chosen shape, so a test can present the
// recorder a unix socket, a pipe or an in-memory tunnel address.
type pthAddr struct {
	network string
	str     string
}

func (a pthAddr) Network() string { return a.network }
func (a pthAddr) String() string  { return a.str }

// pthConn is an inert net.Conn whose RemoteAddr the test picks. Nothing reads
// or writes it: the recorder only ever looks at the peer address and at conn
// identity (to tell a produced conn from the one a leg was handed).
type pthConn struct {
	remote net.Addr
}

func (c *pthConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *pthConn) Write(b []byte) (int, error)      { return len(b), nil }
func (c *pthConn) Close() error                     { return nil }
func (c *pthConn) LocalAddr() net.Addr              { return pthAddr{"tcp", "127.0.0.1:0"} }
func (c *pthConn) RemoteAddr() net.Addr             { return c.remote }
func (c *pthConn) SetDeadline(time.Time) error      { return nil }
func (c *pthConn) SetReadDeadline(time.Time) error  { return nil }
func (c *pthConn) SetWriteDeadline(time.Time) error { return nil }

// pthTCPConn returns a conn whose socket peer is addr.
func pthTCPConn(addr string) net.Conn {
	return &pthConn{remote: pthAddr{"tcp", addr}}
}

// pthOK returns an Observe closure that produces conn and no error.
func pthOK(conn net.Conn) func() (net.Conn, error) {
	return func() (net.Conn, error) { return conn, nil }
}

// pthFail returns an Observe closure that produces no conn and err.
func pthFail(err error) func() (net.Conn, error) {
	return func() (net.Conn, error) { return nil, err }
}

// pthIDs lists a graph's node IDs in order.
func pthIDs(g *PathGraph) []string {
	out := make([]string, 0, len(g.Nodes))
	for _, n := range g.Nodes {
		out = append(out, n.ID)
	}
	return out
}

// pthNode finds a node by ID, nil when the graph has none.
func pthNode(g *PathGraph, id string) *PathNode {
	for i := range g.Nodes {
		if g.Nodes[i].ID == id {
			return &g.Nodes[i]
		}
	}
	return nil
}

// TestPathRecorderNilReceiverIsInert pins the nil-safety contract the dispatch
// path depends on: a producer that does not observe — an exit-IP check, a
// provider fetch, a route whose ctx carries no recorder — calls these methods
// on a nil *PathRecorder and pays nothing but a ctx.Value miss. Observe must
// still run fn and hand its results back untouched, or a nil recorder would
// change behaviour rather than only skip bookkeeping.
func TestPathRecorderNilReceiverIsInert(t *testing.T) {
	var rec *PathRecorder

	rec.SetClassifier(func(LegInfo, error) (string, string) { return "never", "called" })
	rec.AddLocal()
	rec.NotePartial()

	if got := rec.AddTarget("example.com", "443"); got != "" {
		t.Errorf("nil AddTarget = %q, want \"\"", got)
	}
	if got := rec.TargetNodeID(); got != "" {
		t.Errorf("nil TargetNodeID = %q, want \"\"", got)
	}
	if got := rec.TargetNodeIDFor("example.com", "443"); got != "" {
		t.Errorf("nil TargetNodeIDFor = %q, want \"\"", got)
	}
	if got := rec.AddProxy("ob1", "1.2.3.4:1080"); got != "" {
		t.Errorf("nil AddProxy = %q, want \"\"", got)
	}
	if got := rec.AddProxyAttempt("ob1", "1.2.3.4:1080", 1); got != "" {
		t.Errorf("nil AddProxyAttempt = %q, want \"\"", got)
	}
	if got := rec.AddServer("1.2.3.4:1080"); got != "" {
		t.Errorf("nil AddServer = %q, want \"\"", got)
	}
	if got := rec.LastNodeID(); got != "" {
		t.Errorf("nil LastNodeID = %q, want \"\"", got)
	}
	if got := rec.Snapshot(); got != nil {
		t.Errorf("nil Snapshot = %+v, want nil", got)
	}

	// Observe on a nil recorder is a plain call-through: fn runs exactly once
	// and both results come back unchanged.
	calls := 0
	wantConn := pthTCPConn("127.0.0.1:9999")
	wantErr := errors.New("pth: dial failed")
	gotConn, gotErr := rec.Observe(Leg{Kind: LegDial, To: NodeTargetID}, nil, func() (net.Conn, error) {
		calls++
		return wantConn, wantErr
	})
	if calls != 1 {
		t.Errorf("nil Observe called fn %d times, want 1", calls)
	}
	if gotConn != wantConn {
		t.Errorf("nil Observe conn = %v, want the conn fn returned", gotConn)
	}
	if gotErr != wantErr {
		t.Errorf("nil Observe err = %v, want %v", gotErr, wantErr)
	}

	// The same, for a leg that produces nothing.
	calls = 0
	gotConn, gotErr = rec.Observe(Leg{Kind: LegStack, To: "unknown"}, wantConn, func() (net.Conn, error) {
		calls++
		return nil, nil
	})
	if calls != 1 || gotConn != nil || gotErr != nil {
		t.Errorf("nil Observe (empty result) = (%v, %v) after %d calls, want (nil, nil) after 1", gotConn, gotErr, calls)
	}
}

// TestValidHostPortRejectsPluginShapes pins the trap the address rule guards:
// net.SplitHostPort alone accepts "geph://tunnel" (host "geph", port
// "//tunnel"), and a tunnel protocol's Address() returns exactly that shape.
// Only a numeric port in 1..65535 with a slash-free, space-free host counts.
func TestValidHostPortRejectsPluginShapes(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		// Plugin shapes: SplitHostPort parses some of these, the port is not a
		// number, so none is a dialable address.
		{"geph://tunnel", false},
		{"tor-circuit", false},
		{"", false},
		{"host:0", false},
		{"host:70000", false},
		{"host:-1", false},
		{"host:port", false},
		{":1080", false},
		{"host with space:443", false},
		{"a/b:443", false},
		// Real addresses.
		{"1.2.3.4:1080", true},
		{"[::1]:443", true},
		{"example.com:443", true},
		{"host:65535", true},
		{"host:1", true},
	}
	for _, tc := range cases {
		if got := validHostPort(tc.in); got != tc.want {
			t.Errorf("validHostPort(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}

	// The trap, spelled out: the stdlib split succeeds where the rule must not.
	if host, port, err := net.SplitHostPort("geph://tunnel"); err != nil {
		t.Fatalf("net.SplitHostPort(%q) unexpectedly failed: %v", "geph://tunnel", err)
	} else if host != "geph" || port != "//tunnel" {
		t.Fatalf("net.SplitHostPort(%q) = (%q, %q), want (geph, //tunnel) — the shape this rule exists to reject", "geph://tunnel", host, port)
	}
}

// TestAddProxyIgnoresInvalidAddress: a node is always recorded for an outbound
// that was really dialed — the request rode it — but its Address stays empty
// when the plugin's address shape is not a dialable host:port, and no
// AddressSource is claimed for an address that was never learned.
func TestAddProxyIgnoresInvalidAddress(t *testing.T) {
	rec := NewPathRecorder()
	rec.AddLocal()

	id := rec.AddProxy("geph1", "geph://tunnel")
	if id != "proxy:geph1" {
		t.Fatalf("AddProxy(geph1, geph://tunnel) = %q, want %q", id, "proxy:geph1")
	}
	g := rec.Snapshot()
	n := pthNode(g, "proxy:geph1")
	if n == nil {
		t.Fatalf("node %q missing from graph %v", "proxy:geph1", pthIDs(g))
	}
	if n.Address != "" {
		t.Errorf("plugin-shaped address was adopted: Address = %q, want \"\"", n.Address)
	}
	if n.AddressSource != "" {
		t.Errorf("AddressSource = %q for an empty Address, want \"\"", n.AddressSource)
	}
	if n.OutboundID != "geph1" {
		t.Errorf("OutboundID = %q, want %q", n.OutboundID, "geph1")
	}
	if n.Kind != NodeProxy {
		t.Errorf("Kind = %q, want %q", n.Kind, NodeProxy)
	}

	// The same rule on the other two producers of an address.
	rec2 := NewPathRecorder()
	if got := rec2.AddServer("tor-circuit"); got != "" {
		t.Errorf("AddServer(tor-circuit) = %q, want \"\" (no node for an address that is not host:port)", got)
	}
	if got := rec2.AddServer("geph://tunnel"); got != "" {
		t.Errorf("AddServer(geph://tunnel) = %q, want \"\"", got)
	}
	rec2.AddLocal()
	if got := rec2.AddTarget("geph", "//tunnel"); got != NodeTargetID {
		t.Errorf("AddTarget(geph, //tunnel) = %q, want %q (the node exists, its address does not)", got, NodeTargetID)
	}
	g2 := rec2.Snapshot()
	tn := pthNode(g2, NodeTargetID)
	if tn == nil {
		t.Fatalf("target node missing from graph %v", pthIDs(g2))
	}
	if tn.Address != "" || tn.AddressSource != "" {
		t.Errorf("target Address = %q, AddressSource = %q; want both empty for a plugin-shaped port", tn.Address, tn.AddressSource)
	}
	if tn.Host != "geph" || tn.Port != "//tunnel" {
		t.Errorf("target Host/Port = %q/%q, want the requested %q/%q", tn.Host, tn.Port, "geph", "//tunnel")
	}

	// An attempt node obeys the same rule.
	rec3 := NewPathRecorder()
	rec3.AddLocal()
	if got := rec3.AddProxyAttempt("tor1", "tor-circuit", 2); got != "proxy:tor1#2" {
		t.Errorf("AddProxyAttempt(tor1, tor-circuit, 2) = %q, want %q", got, "proxy:tor1#2")
	}
	if n := pthNode(rec3.Snapshot(), "proxy:tor1#2"); n == nil || n.Address != "" {
		t.Errorf("attempt node = %+v, want a node with an empty Address", n)
	}
}

// TestAddProxyAttemptNumbersFailoverNodes: the first attempt is the hop's own
// node, later attempts get a numbered one, so every failed pool member keeps
// its own address, error, phase and reason instead of being overwritten by the
// member that won.
func TestAddProxyAttemptNumbersFailoverNodes(t *testing.T) {
	rec := NewPathRecorder()
	rec.AddLocal()

	type attempt struct {
		outboundID string
		address    string
		n          int
		wantID     string
	}
	attempts := []attempt{
		{"a", "10.0.0.1:1080", 1, "proxy:a"},
		{"b", "10.0.0.2:1080", 2, "proxy:b#2"},
		{"c", "10.0.0.3:1080", 3, "proxy:c#3"},
	}
	// Every node must be added before the first Snapshot: Snapshot seals.
	for _, a := range attempts {
		if id := rec.AddProxyAttempt(a.outboundID, a.address, a.n); id != a.wantID {
			t.Errorf("AddProxyAttempt(%q, %q, %d) = %q, want %q", a.outboundID, a.address, a.n, id, a.wantID)
		}
	}
	// Attempt 0 and a repeat of attempt 1 are the hop's own node, not a new one.
	if id := rec.AddProxyAttempt("a", "10.0.0.1:1080", 0); id != "proxy:a" {
		t.Errorf("AddProxyAttempt(a, ..., 0) = %q, want %q", id, "proxy:a")
	}
	if id := rec.AddProxy("a", "10.0.0.1:1080"); id != "proxy:a" {
		t.Errorf("AddProxy(a, ...) = %q, want %q", id, "proxy:a")
	}

	g := rec.Snapshot()
	// Three attempts are three vertices: the graph keeps the losers.
	if len(g.Nodes) != 4 {
		t.Errorf("nodes = %v, want local plus one node per attempt", pthIDs(g))
	}
	for _, a := range attempts {
		n := pthNode(g, a.wantID)
		if n == nil {
			t.Errorf("node %q missing from %v", a.wantID, pthIDs(g))
			continue
		}
		if n.Attempt != a.n {
			t.Errorf("node %q Attempt = %d, want %d", n.ID, n.Attempt, a.n)
		}
		if n.OutboundID != a.outboundID {
			t.Errorf("node %q OutboundID = %q, want %q", n.ID, n.OutboundID, a.outboundID)
		}
		if n.Address != a.address || n.AddressSource != AddrConfigured {
			t.Errorf("node %q Address = %q (%q), want %q (configured)", n.ID, n.Address, n.AddressSource, a.address)
		}
		if n.Kind != NodeProxy {
			t.Errorf("node %q Kind = %q, want %q", n.ID, n.Kind, NodeProxy)
		}
	}
}

// TestAddServerMergesWithNamedProxy: one physical server is one vertex, in
// either order. Naming an outbound for an address a server node already stands
// for upgrades that node in place and rewrites the edges (and the arrival
// pointer) that referenced the old ID.
func TestAddServerMergesWithNamedProxy(t *testing.T) {
	const addr = "1.2.3.4:1080"

	// Proxy first: AddServer finds the node registered for that address.
	rec := NewPathRecorder()
	rec.AddLocal()
	proxyID := rec.AddProxy("ob1", addr)
	if proxyID != "proxy:ob1" {
		t.Fatalf("AddProxy(ob1, %q) = %q, want %q", addr, proxyID, "proxy:ob1")
	}
	if got := rec.AddServer(addr); got != proxyID {
		t.Errorf("AddServer(%q) after AddProxy = %q, want the proxy node %q", addr, got, proxyID)
	}
	if g := rec.Snapshot(); len(g.Nodes) != 2 {
		t.Errorf("nodes = %v, want [local proxy:ob1] — no second vertex for one server", pthIDs(g))
	}

	// Server first: the edge recorded against "server:<addr>" is rewritten when
	// an outbound is named for that address.
	rec2 := NewPathRecorder()
	rec2.AddLocal()
	srvID := rec2.AddServer(addr)
	if srvID != "server:"+addr {
		t.Fatalf("AddServer(%q) = %q, want %q", addr, srvID, "server:"+addr)
	}
	if _, err := rec2.Observe(Leg{Kind: LegDial, To: srvID, Target: addr}, nil, pthOK(pthTCPConn(addr))); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if got := rec2.LastNodeID(); got != srvID {
		t.Fatalf("LastNodeID = %q, want %q before the rename", got, srvID)
	}

	namedID := rec2.AddProxy("ob1", addr)
	if namedID != "proxy:ob1" {
		t.Fatalf("AddProxy(ob1, %q) over a server node = %q, want %q", addr, namedID, "proxy:ob1")
	}
	g := rec2.Snapshot()
	if len(g.Edges) != 1 {
		t.Fatalf("edges = %+v, want the one leg recorded before the rename", g.Edges)
	}
	if g.Edges[0].To != "proxy:ob1" {
		t.Errorf("edge To = %q, want %q — the rename must rewrite edges that referenced the old ID", g.Edges[0].To, "proxy:ob1")
	}
	if g.Edges[0].From != NodeLocalID {
		t.Errorf("edge From = %q, want %q", g.Edges[0].From, NodeLocalID)
	}
	if pthNode(g, "server:"+addr) != nil {
		t.Errorf("nodes = %v, want the server node upgraded in place, not duplicated", pthIDs(g))
	}
	upgraded := pthNode(g, "proxy:ob1")
	if upgraded == nil {
		t.Fatalf("node %q missing from %v", "proxy:ob1", pthIDs(g))
	}
	if upgraded.OutboundID != "ob1" {
		t.Errorf("upgraded node OutboundID = %q, want %q", upgraded.OutboundID, "ob1")
	}
	if upgraded.Address != addr {
		t.Errorf("upgraded node Address = %q, want %q", upgraded.Address, addr)
	}
	// The address was learned off the socket before the rename; naming an
	// outbound for it must not downgrade that to a configured address.
	if upgraded.AddressSource != AddrSocket {
		t.Errorf("upgraded node AddressSource = %q, want %q (a socket address is never overwritten)", upgraded.AddressSource, AddrSocket)
	}
	if got := rec2.LastNodeID(); got != "proxy:ob1" {
		t.Errorf("LastNodeID = %q, want %q — the rename rewrites the arrival pointer too", got, "proxy:ob1")
	}

	// A server address that is not host:port gets no node at all.
	rec3 := NewPathRecorder()
	if got := rec3.AddServer("geph://tunnel"); got != "" {
		t.Errorf("AddServer(geph://tunnel) = %q, want \"\"", got)
	}
	if got := rec3.Snapshot(); got != nil {
		t.Errorf("Snapshot of a recorder with nothing observed = %+v, want nil", got)
	}
}

// TestAddTargetIsIdempotentAndOrderedLast: one request has one destination, so
// a second AddTarget is ignored even when it names something else; and although
// the target node is created up front — before any dial, so a total failure
// still names the destination — Snapshot puts it last, because that is path
// order.
func TestAddTargetIsIdempotentAndOrderedLast(t *testing.T) {
	rec := NewPathRecorder()
	rec.AddLocal()

	first := rec.AddTarget("example.com", "443")
	if first != NodeTargetID {
		t.Fatalf("AddTarget = %q, want %q", first, NodeTargetID)
	}
	// A proxy added after the target lands after it in the raw node list, which
	// is exactly what Snapshot has to reorder.
	if id := rec.AddProxy("ob1", "1.2.3.4:1080"); id != "proxy:ob1" {
		t.Fatalf("AddProxy = %q, want %q", id, "proxy:ob1")
	}
	second := rec.AddTarget("other.example", "80")
	if second != NodeTargetID {
		t.Errorf("second AddTarget = %q, want %q", second, NodeTargetID)
	}

	g := rec.Snapshot()
	ids := pthIDs(g)
	want := []string{NodeLocalID, "proxy:ob1", NodeTargetID}
	if len(ids) != len(want) {
		t.Fatalf("nodes = %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Errorf("nodes[%d] = %q, want %q (target last, proxies in dial order)", i, ids[i], want[i])
		}
	}
	tn := pthNode(g, NodeTargetID)
	if tn == nil {
		t.Fatalf("target node missing from %v", ids)
	}
	if tn.Host != "example.com" || tn.Port != "443" {
		t.Errorf("target Host/Port = %q/%q, want example.com/443 — a second AddTarget must not rewrite it", tn.Host, tn.Port)
	}
	if tn.Address != "example.com:443" || tn.AddressSource != AddrConfigured {
		t.Errorf("target Address = %q (%q), want example.com:443 (configured)", tn.Address, tn.AddressSource)
	}
	if tn.Kind != NodeTarget {
		t.Errorf("target Kind = %q, want %q", tn.Kind, NodeTarget)
	}

	// The identity check: the graph's target node is not automatically "where
	// this dial was headed" — a nested upstream route shares the recorder while
	// dialing a different address.
	if got := rec.TargetNodeID(); got != NodeTargetID {
		t.Errorf("TargetNodeID = %q, want %q", got, NodeTargetID)
	}
	if got := rec.TargetNodeIDFor("example.com", "443"); got != NodeTargetID {
		t.Errorf("TargetNodeIDFor(example.com, 443) = %q, want %q", got, NodeTargetID)
	}
	if got := rec.TargetNodeIDFor("other.example", "80"); got != "" {
		t.Errorf("TargetNodeIDFor(other.example, 80) = %q, want \"\" — the target node names a different destination", got)
	}
	if got := rec.TargetNodeIDFor("example.com", "80"); got != "" {
		t.Errorf("TargetNodeIDFor(example.com, 80) = %q, want \"\"", got)
	}

	// With no target node at all both lookups are empty.
	rec2 := NewPathRecorder()
	rec2.AddLocal()
	if got := rec2.TargetNodeID(); got != "" {
		t.Errorf("TargetNodeID with no target = %q, want \"\"", got)
	}
	if got := rec2.TargetNodeIDFor("example.com", "443"); got != "" {
		t.Errorf("TargetNodeIDFor with no target = %q, want \"\"", got)
	}
}

// TestObserveAdoptsSocketPeerOntoArrivalNode: the address the engine really
// connected to outranks the configured one — a dial resolves the name, and the
// socket peer is the resolved address.
func TestObserveAdoptsSocketPeerOntoArrivalNode(t *testing.T) {
	rec := NewPathRecorder()
	rec.AddLocal()
	to := rec.AddProxy("ob1", "example.com:1080")

	if _, err := rec.Observe(Leg{Kind: LegDial, To: to, Target: "example.com:1080"}, nil, pthOK(pthTCPConn("127.0.0.1:9999"))); err != nil {
		t.Fatalf("Observe: %v", err)
	}

	g := rec.Snapshot()
	n := pthNode(g, to)
	if n == nil {
		t.Fatalf("node %q missing from %v", to, pthIDs(g))
	}
	if n.Address != "127.0.0.1:9999" {
		t.Errorf("Address = %q, want the socket peer 127.0.0.1:9999", n.Address)
	}
	if n.AddressSource != AddrSocket {
		t.Errorf("AddressSource = %q, want %q", n.AddressSource, AddrSocket)
	}
	if len(g.Edges) != 1 {
		t.Fatalf("edges = %+v, want exactly one arrival leg", g.Edges)
	}
	e := g.Edges[0]
	if e.From != NodeLocalID || e.To != to || e.Kind != LegDial {
		t.Errorf("edge = %s -> %s (%s), want local -> %s (dial)", e.From, e.To, e.Kind, to)
	}
	if !e.Observed || !e.Success {
		t.Errorf("edge Observed/Success = %v/%v, want true/true", e.Observed, e.Success)
	}
	if e.Target != "example.com:1080" {
		t.Errorf("edge Target = %q, want the dial string handed to the leg", e.Target)
	}
	if e.Error != "" || e.Phase != "" || e.Reason != "" {
		t.Errorf("successful edge carries Error/Phase/Reason = %q/%q/%q, want all empty", e.Error, e.Phase, e.Reason)
	}
	// The arrival leg was bracketed, so it is timed; 0 would claim "not
	// separately timed", which is only what Untimed means. Assert only that the
	// leg is marked observed — wall-clock spans are not test material.
	if got := rec.LastNodeID(); got != to {
		t.Errorf("LastNodeID = %q, want %q", got, to)
	}

	// A socket peer never overwrites a *socket* peer with a configured one:
	// re-dialing the same node keeps what the socket said.
	rec2 := NewPathRecorder()
	rec2.AddLocal()
	to2 := rec2.AddProxy("ob1", "example.com:1080")
	_, _ = rec2.Observe(Leg{Kind: LegDial, To: to2}, nil, pthOK(pthTCPConn("127.0.0.1:9999")))
	// A later configured adoption for the same node must not downgrade it.
	if id := rec2.AddProxy("ob1", "10.9.8.7:1080"); id != to2 {
		t.Fatalf("repeat AddProxy = %q, want %q", id, to2)
	}
	if n := pthNode(rec2.Snapshot(), to2); n == nil {
		t.Fatalf("node %q missing", to2)
	} else if n.Address != "127.0.0.1:9999" || n.AddressSource != AddrSocket {
		t.Errorf("node = %q (%q), want the socket peer 127.0.0.1:9999 to survive a configured address", n.Address, n.AddressSource)
	}
}

// TestObserveDoesNotAdoptFromHandedConn: a leg that was handed a conn did not
// dial it, so the peer address that conn carries describes the PREVIOUS leg and
// must not be adopted. The leg's kind is untouched: handing the conn back is
// what a chained socks5 handshake does after it really performed the handshake,
// and the recorder has no business reclassifying what the producer labelled.
func TestObserveDoesNotAdoptFromHandedConn(t *testing.T) {
	rec := NewPathRecorder()
	rec.AddLocal()
	hop := rec.AddProxy("ob1", "1.2.3.4:1080")
	to := rec.AddTarget("example.com", "443")

	via := pthTCPConn("10.0.0.1:1111") // the previous leg's socket peer
	got, err := rec.Observe(Leg{Kind: LegTunnel, From: hop, To: to, Target: "example.com:443"}, via, pthOK(via))
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if got != via {
		t.Errorf("Observe conn = %v, want the handed conn back unchanged", got)
	}

	g := rec.Snapshot()
	if len(g.Edges) != 1 {
		t.Fatalf("edges = %+v, want the one tunnel leg", g.Edges)
	}
	if g.Edges[0].Kind != LegTunnel {
		t.Errorf("edge Kind = %q, want %q — the producer's label survives, whatever the call returned", g.Edges[0].Kind, LegTunnel)
	}
	for _, id := range []string{hop, to} {
		n := pthNode(g, id)
		if n == nil {
			t.Fatalf("node %q missing from %v", id, pthIDs(g))
		}
		if n.AddressSource == AddrSocket {
			t.Errorf("node %q adopted the handed conn's peer: Address = %q (%q), want the configured address", id, n.Address, n.AddressSource)
		}
	}
	if hn := pthNode(g, hop); hn == nil {
		t.Errorf("hop node %q missing from %v", hop, pthIDs(g))
	} else if hn.Address != "1.2.3.4:1080" {
		t.Errorf("hop Address = %q, want the configured 1.2.3.4:1080", hn.Address)
	}
	// The leg still arrives somewhere, so the arrival pointer moves.
	if got := rec.LastNodeID(); got != to {
		t.Errorf("LastNodeID = %q, want %q", got, to)
	}
}

// TestObserveChainedLegAdoptsNothing: in a chain the engine holds exactly one
// socket and its peer is an earlier hop's server, so a leg handed a conn can
// vouch for no address at all — not for To, and not for From either. The old
// rule adopted the peer onto From whenever the returned conn was a different
// object from via, which stamped the PREVIOUS hop's server onto this hop's node
// as socket-verified (plugins/http returns a wrapper conn whenever the CONNECT
// response over-reads), deleted this hop's real address from the index, and left
// two nodes claiming one address.
func TestObserveChainedLegAdoptsNothing(t *testing.T) {
	rec := NewPathRecorder()
	rec.AddLocal()
	from := rec.AddProxy("ob1", "1.2.3.4:1080")
	to := rec.AddProxy("ob2", "5.6.7.8:1080")
	rec.AddTarget("example.com", "443")

	// The plugin was handed the previous hop's socket and returns a wrapper
	// over it — a different object with the same peer, exactly the shape
	// plugins/http produces when the CONNECT response over-reads.
	via := pthTCPConn("10.0.0.1:1111")
	wrapper := pthTCPConn("10.0.0.1:1111")
	if _, err := rec.Observe(Leg{Kind: LegTunnel, From: from, To: to, Target: "5.6.7.8:1080"}, via, pthOK(wrapper)); err != nil {
		t.Fatalf("Observe: %v", err)
	}

	// Neither endpoint moved: both keep the address the config named, and the
	// address index still resolves each server to its own vertex rather than to
	// whichever node adopted last.
	if got := rec.AddServer("1.2.3.4:1080"); got != from {
		t.Errorf("AddServer(1.2.3.4:1080) = %q, want %q", got, from)
	}
	if got := rec.AddServer("5.6.7.8:1080"); got != to {
		t.Errorf("AddServer(5.6.7.8:1080) = %q, want %q — the index must not be repointed by a chained leg", got, to)
	}

	g := rec.Snapshot()
	for _, tc := range []struct{ id, want string }{{from, "1.2.3.4:1080"}, {to, "5.6.7.8:1080"}} {
		n := pthNode(g, tc.id)
		if n == nil {
			t.Fatalf("node %q missing from %v", tc.id, pthIDs(g))
		}
		if n.Address != tc.want || n.AddressSource != AddrConfigured {
			t.Errorf("node %q Address = %q (%q), want the configured %q — a chained leg adopts nothing", n.ID, n.Address, n.AddressSource, tc.want)
		}
	}
	if len(g.Edges) != 1 {
		t.Fatalf("edges = %+v, want exactly the tunnel leg", g.Edges)
	}
	if e := g.Edges[0]; e.From != from || e.To != to || e.Kind != LegTunnel {
		t.Errorf("edge = %s -> %s (%s), want %s -> %s (tunnel)", e.From, e.To, e.Kind, from, to)
	}
	if got := rec.LastNodeID(); got != to {
		t.Errorf("LastNodeID = %q, want %q", got, to)
	}

	// The same leg returning via itself — what plugins/socks5's connect does —
	// is not a different kind either: a timed successful handshake must not read
	// as a no-op, and a failed one must not read as something else.
	rec2 := NewPathRecorder()
	rec2.AddLocal()
	from2 := rec2.AddProxy("ob1", "1.2.3.4:1080")
	to2 := rec2.AddTarget("example.com", "443")
	via2 := pthTCPConn("1.2.3.4:1080")
	got2, err := rec2.Observe(Leg{Kind: LegTunnel, From: from2, To: to2}, via2, pthOK(via2))
	if got2 != via2 || err != nil {
		t.Errorf("Observe = (%v, %v), want the handed conn and no error", got2, err)
	}
	g2 := rec2.Snapshot()
	if len(g2.Edges) != 1 || g2.Edges[0].Kind != LegTunnel {
		t.Fatalf("edges = %+v, want one tunnel leg — conn == via is not a kind", g2.Edges)
	}
	if n := pthNode(g2, from2); n == nil {
		t.Fatalf("node %q missing from %v", from2, pthIDs(g2))
	} else if n.Address != "1.2.3.4:1080" || n.AddressSource != AddrConfigured {
		t.Errorf("hop Address = %q (%q), want the configured 1.2.3.4:1080", n.Address, n.AddressSource)
	}
	if n := pthNode(g2, to2); n == nil {
		t.Fatalf("node %q missing from %v", to2, pthIDs(g2))
	} else if n.AddressSource == AddrSocket {
		t.Errorf("target adopted the handed conn's peer: Address = %q (%q)", n.Address, n.AddressSource)
	}
}

// TestObserveEmitsTunnelEdgeForThrough: one bracketed call that covered two
// physical legs — the plugin dialed its server AND handshaked toward the next
// node inside the same call — yields two edges. The second is real but not
// separately timed, so its Duration is 0 and it is still marked Observed.
func TestObserveEmitsTunnelEdgeForThrough(t *testing.T) {
	rec := NewPathRecorder()
	rec.AddLocal()
	hop := rec.AddProxy("ob1", "1.2.3.4:1080")
	target := rec.AddTarget("example.com", "443")

	conn := pthTCPConn("127.0.0.1:9999")
	got, err := rec.Observe(Leg{
		Kind: LegDial, To: hop, Through: target, Target: "1.2.3.4:1080",
	}, nil, pthOK(conn))
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if got != conn {
		t.Errorf("Observe conn = %v, want the conn fn produced", got)
	}

	g := rec.Snapshot()
	if len(g.Edges) != 2 {
		t.Fatalf("edges = %+v, want the timed arrival plus the untimed tunnel leg", g.Edges)
	}
	arrival, tunnel := g.Edges[0], g.Edges[1]
	if arrival.From != NodeLocalID || arrival.To != hop || arrival.Kind != LegDial {
		t.Errorf("arrival edge = %s -> %s (%s), want local -> %s (dial)", arrival.From, arrival.To, arrival.Kind, hop)
	}
	if !arrival.Observed || !arrival.Success {
		t.Errorf("arrival Observed/Success = %v/%v, want true/true", arrival.Observed, arrival.Success)
	}
	if tunnel.From != hop || tunnel.To != target {
		t.Errorf("tunnel edge = %s -> %s, want %s -> %s", tunnel.From, tunnel.To, hop, target)
	}
	if tunnel.Kind != LegTunnel {
		t.Errorf("tunnel Kind = %q, want %q", tunnel.Kind, LegTunnel)
	}
	if tunnel.Duration != 0 {
		t.Errorf("tunnel Duration = %v, want 0 — the leg was not separately timed", tunnel.Duration)
	}
	if !tunnel.Observed {
		t.Errorf("tunnel Observed = false, want true — untimed is not unobserved")
	}
	if !tunnel.Success {
		t.Errorf("tunnel Success = false, want true")
	}
	if tunnel.Target != "1.2.3.4:1080" {
		t.Errorf("tunnel Target = %q, want the dial string handed to the leg", tunnel.Target)
	}
	// The socket peer of that one call is the server it connected to, so it
	// lands on the hop, not on the target.
	hn := pthNode(g, hop)
	if hn == nil {
		t.Fatalf("hop node %q missing from %v", hop, pthIDs(g))
	}
	if hn.Address != "127.0.0.1:9999" || hn.AddressSource != AddrSocket {
		t.Errorf("hop Address = %q (%q), want 127.0.0.1:9999 (socket)", hn.Address, hn.AddressSource)
	}
	tn := pthNode(g, target)
	if tn != nil && tn.AddressSource == AddrSocket {
		t.Errorf("target adopted the socket peer: Address = %q, want the configured example.com:443", tn.Address)
	}
	if got := rec.LastNodeID(); got != target {
		t.Errorf("LastNodeID = %q, want %q — the path really reached Through", got, target)
	}

	// Untimed says the same thing from the producer's side: a leg the engine
	// knows happened but bracketed with a no-op closure records Duration 0.
	rec2 := NewPathRecorder()
	rec2.AddLocal()
	a := rec2.AddProxy("a", "10.0.0.1:1080")
	b := rec2.AddServer("10.0.0.2:1080")
	if _, err := rec2.Observe(Leg{Kind: LegTunnel, From: a, To: b, Untimed: true}, nil, pthOK(nil)); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	g2 := rec2.Snapshot()
	if len(g2.Edges) != 1 {
		t.Fatalf("edges = %+v, want one leg", g2.Edges)
	}
	if g2.Edges[0].Duration != 0 {
		t.Errorf("Untimed leg Duration = %v, want 0 even though the call was bracketed", g2.Edges[0].Duration)
	}
	if !g2.Edges[0].Observed || !g2.Edges[0].Success {
		t.Errorf("Untimed leg Observed/Success = %v/%v, want true/true", g2.Edges[0].Observed, g2.Edges[0].Success)
	}
	if got := rec2.LastNodeID(); got != b {
		t.Errorf("LastNodeID = %q, want %q", got, b)
	}
}

// TestObserveFailureEmitsOneEdgeWithClassification: a failed leg records once,
// with the installed classifier's verdict and the LegInfo that bounds it, and
// the arrival pointer does not move — the path did not get there.
func TestObserveFailureEmitsOneEdgeWithClassification(t *testing.T) {
	rec := NewPathRecorder()
	rec.AddLocal()
	target := rec.AddTarget("example.com", "443")
	rec.AddProxy("ob1", "1.2.3.4:1080")

	var gotInfo LegInfo
	calls := 0
	rec.SetClassifier(func(info LegInfo, err error) (string, string) {
		calls++
		gotInfo = info
		return "pth-phase", "pth-reason"
	})

	// A previous success, so "the pointer did not move" is a real assertion.
	prev := rec.AddServer("10.0.0.9:1080")
	if _, err := rec.Observe(Leg{Kind: LegStack, To: prev}, nil, pthOK(nil)); err != nil {
		t.Fatalf("Observe (setup): %v", err)
	}

	boom := errors.New("pth: connect refused")
	conn, err := rec.Observe(Leg{Kind: LegStack, To: target, Target: "example.com:443"}, nil, pthFail(boom))
	if conn != nil {
		t.Errorf("Observe conn = %v, want nil", conn)
	}
	if err != boom {
		t.Errorf("Observe err = %v, want %v unchanged", err, boom)
	}
	if calls != 1 {
		t.Errorf("classifier called %d times, want 1", calls)
	}
	if gotInfo.Kind != LegStack {
		t.Errorf("LegInfo.Kind = %q, want %q", gotInfo.Kind, LegStack)
	}
	if !gotInfo.FromLocal {
		t.Errorf("LegInfo.FromLocal = false, want true — the leg left the engine host")
	}
	if !gotInfo.ToTarget {
		t.Errorf("LegInfo.ToTarget = false, want true — the leg's destination is the target")
	}
	if got := rec.LastNodeID(); got != prev {
		t.Errorf("LastNodeID = %q, want %q unchanged after a failure", got, prev)
	}

	g := rec.Snapshot()
	if len(g.Edges) != 2 {
		t.Fatalf("edges = %+v, want the setup leg plus exactly one failure (no tunnel edge)", g.Edges)
	}
	e := g.Edges[1]
	if e.From != NodeLocalID || e.To != target {
		t.Errorf("edge = %s -> %s, want local -> %s", e.From, e.To, target)
	}
	if e.Success {
		t.Errorf("edge Success = true, want false")
	}
	if e.Error != boom.Error() {
		t.Errorf("edge Error = %q, want %q", e.Error, boom.Error())
	}
	if e.Phase != "pth-phase" || e.Reason != "pth-reason" {
		t.Errorf("edge Phase/Reason = %q/%q, want pth-phase/pth-reason", e.Phase, e.Reason)
	}
	if !e.Observed {
		t.Errorf("edge Observed = false, want true — the engine bracketed this leg")
	}

	// With no classifier installed the recorder leaves the verdict empty rather
	// than guessing.
	rec2 := NewPathRecorder()
	rec2.AddLocal()
	rec2.AddTarget("example.com", "443")
	_, _ = rec2.Observe(Leg{Kind: LegStack, To: NodeTargetID}, nil, pthFail(boom))
	e2 := rec2.Snapshot().Edges[0]
	if e2.Phase != "" || e2.Reason != "" {
		t.Errorf("unclassified edge Phase/Reason = %q/%q, want both empty", e2.Phase, e2.Reason)
	}
	if e2.Error != boom.Error() {
		t.Errorf("unclassified edge Error = %q, want %q", e2.Error, boom.Error())
	}
}

// TestNestedUpstreamSuppressesDuplicateArrival: a nested route reached this
// server through a path the engine already recorded, so what its own call adds
// is only the hop's handshake toward the next node. Recording the arrival as
// well would claim the engine reached that server directly.
func TestNestedUpstreamSuppressesDuplicateArrival(t *testing.T) {
	rec := NewPathRecorder()
	rec.AddLocal()
	x := rec.AddProxy("outer", "1.2.3.4:1080")
	y := rec.AddProxy("route", "5.6.7.8:1080")

	// The outer request's leg really did arrive at X.
	if _, err := rec.Observe(Leg{Kind: LegDial, To: x, Target: "1.2.3.4:1080"}, nil, pthOK(pthTCPConn("1.2.3.4:1080"))); err != nil {
		t.Fatalf("Observe (arrival at X): %v", err)
	}
	if got := rec.LastNodeID(); got != x {
		t.Fatalf("LastNodeID = %q, want %q before the nested leg", got, x)
	}

	// The nested upstream route dials X (the address it was asked for) through
	// member Y: From "" and To == lastNode, so the arrival is suppressed.
	produced := pthTCPConn("5.6.7.8:1080")
	if _, err := rec.Observe(Leg{Kind: LegDial, From: "", To: x, Through: y, Target: "1.2.3.4:1080"}, nil, pthOK(produced)); err != nil {
		t.Fatalf("Observe (nested): %v", err)
	}

	g := rec.Snapshot()
	if len(g.Edges) != 2 {
		t.Fatalf("edges = %+v, want the arrival at X plus exactly one X -> Y tunnel", g.Edges)
	}
	e := g.Edges[1]
	if e.From != x || e.To != y {
		t.Errorf("nested edge = %s -> %s, want %s -> %s", e.From, e.To, x, y)
	}
	if e.Kind != LegTunnel {
		t.Errorf("nested edge Kind = %q, want %q", e.Kind, LegTunnel)
	}
	if !e.Observed || !e.Success {
		t.Errorf("nested edge Observed/Success = %v/%v, want true/true", e.Observed, e.Success)
	}
	if got := rec.LastNodeID(); got != y {
		t.Errorf("LastNodeID = %q, want %q", got, y)
	}
	// Three nodes, no duplicate vertex for X.
	if ids := pthIDs(g); len(ids) != 3 {
		t.Errorf("nodes = %v, want [local proxy:outer proxy:route]", ids)
	}
	// The suppressed leg adopts nothing: the conn it returned belongs to the
	// nested route, whose own leg already adopted it onto its own node.
	if yn := pthNode(g, y); yn == nil {
		t.Errorf("node %q missing from %v", y, pthIDs(g))
	} else if yn.Address != "5.6.7.8:1080" || yn.AddressSource != AddrConfigured {
		t.Errorf("node %q Address = %q (%q), want the configured 5.6.7.8:1080 — a suppressed leg adopts nothing", y, yn.Address, yn.AddressSource)
	}

	// Without the matching lastNode the arrival is recorded: the engine really
	// did reach that server itself.
	rec2 := NewPathRecorder()
	rec2.AddLocal()
	x2 := rec2.AddProxy("outer", "1.2.3.4:1080")
	y2 := rec2.AddProxy("route", "5.6.7.8:1080")
	if _, err := rec2.Observe(Leg{Kind: LegDial, To: x2, Through: y2}, nil, pthOK(nil)); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	g2 := rec2.Snapshot()
	if len(g2.Edges) != 2 {
		t.Fatalf("edges = %+v, want an arrival plus its tunnel leg when nothing was suppressed", g2.Edges)
	}
	if g2.Edges[0].From != NodeLocalID || g2.Edges[0].To != x2 || g2.Edges[0].Kind != LegDial {
		t.Errorf("first edge = %s -> %s (%s), want local -> %s (dial)", g2.Edges[0].From, g2.Edges[0].To, g2.Edges[0].Kind, x2)
	}
}

// TestNestedUpstreamSuppressionOfFailedCallMarksPartial: the bracketed call
// that IS a nested route (From "", no Through) and fails after that nested
// route already drew its arrival at `to`. What died is the hop's handshake
// toward the target, inside the plugin's own call: this recorder bracketed no
// leg for it, and no honest from→to edge exists (a failed local→to would
// contradict the adjacent successful arrival at the same node). So the failure
// is not drawn — but it must not vanish either: the graph says Partial, like
// every other leg this recorder cannot place. Without that, a route whose
// request never arrived publishes an all-green graph.
func TestNestedUpstreamSuppressionOfFailedCallMarksPartial(t *testing.T) {
	boom := errors.New("pth: handshake toward the target died")

	// pthNestedArrival runs the nested route inside a bracketed call: a real
	// dial from local to deep, then the arrival at x it rode through. It leaves
	// lastNode == x, which is what makes the enclosing call suppressible.
	pthNestedArrival := func(t *testing.T, rec *PathRecorder, deep, x string) {
		t.Helper()
		if _, err := rec.Observe(Leg{Kind: LegDial, To: deep, Target: "9.9.9.9:1080"}, nil, pthOK(pthTCPConn("9.9.9.9:1080"))); err != nil {
			t.Fatalf("Observe (nested dial): %v", err)
		}
		if _, err := rec.Observe(Leg{Kind: LegTunnel, From: deep, To: x, Untimed: true}, nil, pthOK(nil)); err != nil {
			t.Fatalf("Observe (nested arrival): %v", err)
		}
	}

	rec := NewPathRecorder()
	rec.SetClassifier(func(LegInfo, error) (string, string) { return "proxy", "proxy_rejected" })
	rec.AddLocal()
	x := rec.AddProxy("outer", "1.2.3.4:1080")
	deep := rec.AddProxy("deep", "9.9.9.9:1080")
	rec.AddTarget("example.com", "443")

	// The outer hop's bracketed dial: inside it the nested route reaches X, then
	// the call fails — the plugin's handshake toward the target never came back.
	conn, err := rec.Observe(Leg{Kind: LegDial, From: "", To: x, Target: "1.2.3.4:1080"}, nil,
		func() (net.Conn, error) {
			pthNestedArrival(t, rec, deep, x)
			return nil, boom
		})
	if err != boom {
		t.Fatalf("Observe err = %v, want %v — the call's own result is passed through", err, boom)
	}
	if conn != nil {
		t.Errorf("Observe conn = %v, want nil", conn)
	}
	if got := rec.LastNodeID(); got != x {
		t.Errorf("LastNodeID = %q, want %q — the nested arrival is still what the graph last saw", got, x)
	}

	g := rec.Snapshot()
	if g == nil {
		t.Fatalf("Snapshot = nil, want a graph")
	}
	if !g.Partial {
		t.Errorf("Partial = false, want true — the failed handshake toward the target "+
			"belongs to no edge this recorder brackets, and the graph must say so "+
			"instead of publishing an all-green route (edges = %+v)", g.Edges)
	}
	// Nothing fabricated for the failure: the only legs drawn are the nested
	// route's own two, both successful.
	if len(g.Edges) != 2 {
		t.Fatalf("edges = %+v, want exactly the nested route's two legs", g.Edges)
	}
	for _, e := range g.Edges {
		if !e.Success {
			t.Errorf("edge %s -> %s is a failure leg — the suppressed call must not invent one (%+v)", e.From, e.To, e)
		}
	}

	// The same call SUCCEEDING leaves the graph complete: nothing was lost, so
	// Partial stays false.
	rec2 := NewPathRecorder()
	rec2.SetClassifier(func(LegInfo, error) (string, string) { return "proxy", "proxy_rejected" })
	rec2.AddLocal()
	x2 := rec2.AddProxy("outer", "1.2.3.4:1080")
	deep2 := rec2.AddProxy("deep", "9.9.9.9:1080")
	rec2.AddTarget("example.com", "443")
	if _, err := rec2.Observe(Leg{Kind: LegDial, From: "", To: x2, Target: "1.2.3.4:1080"}, nil,
		func() (net.Conn, error) {
			pthNestedArrival(t, rec2, deep2, x2)
			return pthTCPConn("1.2.3.4:1080"), nil
		}); err != nil {
		t.Fatalf("Observe (success): %v", err)
	}
	g2 := rec2.Snapshot()
	if g2 == nil {
		t.Fatalf("Snapshot = nil, want a graph")
	}
	if g2.Partial {
		t.Errorf("Partial = true after a suppressed call that SUCCEEDED, want false — "+
			"the nested route drew everything there was to draw (edges = %+v)", g2.Edges)
	}
	if len(g2.Edges) != 2 {
		t.Errorf("edges = %+v, want exactly the nested route's two legs", g2.Edges)
	}
}

// TestCapsBoundTheGraph: metric and probe rows are persisted forever, so a
// pathological configuration cannot grow one row's value without limit. A node
// cap counts what it cut; an edge cap says the graph is partial; an attempt cap
// stops naming failover members.
func TestCapsBoundTheGraph(t *testing.T) {
	// Node cap: local plus MaxPathNodes-1 proxies fill the graph; everything
	// after that is dropped and counted, not recorded.
	rec := NewPathRecorder()
	rec.AddLocal()
	dropped := 0
	for i := 0; i < MaxPathNodes+6; i++ {
		addr := fmt.Sprintf("10.0.%d.%d:1080", i/256, i%256)
		if id := rec.AddProxy(fmt.Sprintf("ob%d", i), addr); id == "" {
			dropped++
		}
	}
	if dropped == 0 {
		t.Errorf("no AddProxy call hit the node cap after %d nodes", MaxPathNodes+6)
	}
	g := rec.Snapshot()
	if len(g.Nodes) > MaxPathNodes {
		t.Errorf("len(Nodes) = %d, want <= MaxPathNodes (%d)", len(g.Nodes), MaxPathNodes)
	}
	if len(g.Nodes) != MaxPathNodes {
		t.Errorf("len(Nodes) = %d, want exactly the cap %d", len(g.Nodes), MaxPathNodes)
	}
	if g.Dropped != dropped {
		t.Errorf("Dropped = %d, want %d — one per node the cap cut", g.Dropped, dropped)
	}
	if g.Dropped <= 0 {
		t.Errorf("Dropped = %d, want > 0", g.Dropped)
	}
	if g.Partial {
		t.Errorf("Partial = true, want false — the node cap counts drops, it does not claim missing legs")
	}

	// Edge cap: legs past MaxPathEdges are not recorded and the graph says it
	// may be missing legs.
	rec2 := NewPathRecorder()
	rec2.AddLocal()
	a := rec2.AddProxy("a", "10.0.0.1:1080")
	b := rec2.AddProxy("b", "10.0.0.2:1080")
	for i := 0; i < MaxPathEdges+6; i++ {
		to := a
		if i%2 == 1 {
			to = b
		}
		if _, err := rec2.Observe(Leg{Kind: LegDial, To: to}, nil, pthOK(nil)); err != nil {
			t.Fatalf("Observe %d: %v", i, err)
		}
	}
	g2 := rec2.Snapshot()
	if len(g2.Edges) > MaxPathEdges {
		t.Errorf("len(Edges) = %d, want <= MaxPathEdges (%d)", len(g2.Edges), MaxPathEdges)
	}
	if len(g2.Edges) != MaxPathEdges {
		t.Errorf("len(Edges) = %d, want exactly the cap %d", len(g2.Edges), MaxPathEdges)
	}
	if !g2.Partial {
		t.Errorf("Partial = false, want true once the edge cap bit")
	}
	if g2.Dropped != 0 {
		t.Errorf("Dropped = %d, want 0 — the edge cap drops no nodes", g2.Dropped)
	}

	// Attempt cap: a failover loop deeper than MaxPathAttempts stops naming
	// members, and says so through Dropped.
	rec3 := NewPathRecorder()
	rec3.AddLocal()
	if id := rec3.AddProxyAttempt("z", "10.0.0.1:1080", MaxPathAttempts); id != fmt.Sprintf("proxy:z#%d", MaxPathAttempts) {
		t.Errorf("AddProxyAttempt at the cap = %q, want %q", id, fmt.Sprintf("proxy:z#%d", MaxPathAttempts))
	}
	if id := rec3.AddProxyAttempt("z", "10.0.0.2:1080", MaxPathAttempts+1); id != "" {
		t.Errorf("AddProxyAttempt past the cap = %q, want \"\"", id)
	}
	if id := rec3.AddProxyAttempt("z", "10.0.0.3:1080", MaxPathAttempts+9); id != "" {
		t.Errorf("AddProxyAttempt far past the cap = %q, want \"\"", id)
	}
	g3 := rec3.Snapshot()
	if g3.Dropped != 2 {
		t.Errorf("Dropped = %d, want 2 — one per attempt past the cap", g3.Dropped)
	}
	if len(g3.Nodes) != 2 {
		t.Errorf("nodes = %v, want [local proxy:z#8]", pthIDs(g3))
	}

	// The tunnel edge of a two-leg call is bounded by the same cap: filling the
	// edge list to one short of the cap and then observing a hop with a Through
	// records the arrival and drops the tunnel leg.
	rec5 := NewPathRecorder()
	rec5.AddLocal()
	hop := rec5.AddProxy("hop", "10.0.0.1:1080")
	other := rec5.AddProxy("other", "10.0.0.2:1080")
	far := rec5.AddTarget("example.com", "443")
	for i := 0; i < MaxPathEdges-1; i++ {
		// Alternate strictly: a leg toward the node the previous leg already
		// arrived at reads as a nested upstream re-arrival and is suppressed,
		// which would leave the filler short of the cap.
		to := hop
		if i%2 == 1 {
			to = other
		}
		if _, err := rec5.Observe(Leg{Kind: LegDial, To: to}, nil, pthOK(nil)); err != nil {
			t.Fatalf("Observe (filler %d): %v", i, err)
		}
	}
	if got := rec5.LastNodeID(); got != hop {
		t.Fatalf("LastNodeID = %q, want %q — the filler run must end off the final hop's node", got, hop)
	}
	// The final two-leg call arrives at `other`, which the filler left behind.
	if _, err := rec5.Observe(Leg{Kind: LegDial, To: other, Through: far}, nil, pthOK(nil)); err != nil {
		t.Fatalf("Observe (two-leg call at the cap): %v", err)
	}
	g5 := rec5.Snapshot()
	if len(g5.Edges) != MaxPathEdges {
		t.Errorf("len(Edges) = %d, want the cap %d", len(g5.Edges), MaxPathEdges)
	}
	last := g5.Edges[len(g5.Edges)-1]
	if last.From != NodeLocalID || last.To != other || last.Kind != LegDial {
		t.Errorf("last edge = %s -> %s (%s), want the arrival local -> %s (dial)", last.From, last.To, last.Kind, other)
	}
	if !g5.Partial {
		t.Errorf("Partial = false after the tunnel edge was dropped at the cap, want true — "+
			"a graph missing a leg it knows about must say so (edges = %d)", len(g5.Edges))
	}
	if got := rec5.LastNodeID(); got != other {
		t.Errorf("LastNodeID = %q, want %q — the tunnel leg toward %q was not recorded", got, other, far)
	}

	// NotePartial is the producer's own way of saying the graph is incomplete.
	rec4 := NewPathRecorder()
	rec4.AddLocal()
	rec4.NotePartial()
	if g4 := rec4.Snapshot(); !g4.Partial {
		t.Errorf("Partial = false after NotePartial, want true")
	}
}

// TestSnapshotIsSealedAndMemoized: the snapshot is the immutable value a metric
// row and its probe row share, and the seal is what makes it safe for the
// metrics writer's json.Marshal to read it on another goroutine long after
// Record returned.
func TestSnapshotIsSealedAndMemoized(t *testing.T) {
	// Nothing observed: no empty graph is serialized, the row's omitempty field
	// disappears instead. The recorder is still sealed.
	empty := NewPathRecorder()
	if g := empty.Snapshot(); g != nil {
		t.Errorf("Snapshot of an empty recorder = %+v, want nil", g)
	}
	if g := empty.Snapshot(); g != nil {
		t.Errorf("second Snapshot of an empty recorder = %+v, want nil", g)
	}
	empty.AddLocal()
	if g := empty.Snapshot(); g != nil {
		t.Errorf("Snapshot after a sealed AddLocal = %+v, want nil — the mutator must be inert", g)
	}

	rec := NewPathRecorder()
	rec.AddLocal()
	target := rec.AddTarget("example.com", "443")
	hop := rec.AddProxy("ob1", "1.2.3.4:1080")
	// Two more nodes, which the post-seal pokes below aim at: one with no
	// address of its own (a tunnel plugin's shape), which a late AddProxy would
	// fill in place, and a bare server node, which a late AddProxy would rename.
	tunnel := rec.AddProxy("tunnel", "geph://tunnel")
	srv := rec.AddServer("203.0.113.9:1080")
	if _, err := rec.Observe(Leg{Kind: LegDial, To: hop, Through: target}, nil, pthOK(pthTCPConn("127.0.0.1:9999"))); err != nil {
		t.Fatalf("Observe: %v", err)
	}

	first := rec.Snapshot()
	if first == nil {
		t.Fatalf("Snapshot = nil for a recorder with nodes and edges")
	}
	second := rec.Snapshot()
	if first != second {
		t.Errorf("second Snapshot = %p, want the identical pointer %p (memoized)", second, first)
	}
	nodesBefore := len(first.Nodes)
	edgesBefore := len(first.Edges)
	partialBefore := first.Partial
	droppedBefore := first.Dropped
	before := make([]PathNode, len(first.Nodes))
	copy(before, first.Nodes)
	// The recorder's own slots, not the copy Snapshot handed out: the seal has
	// to protect these too, or "every mutator is a no-op" is only true of the
	// published graph and a maintainer is one deep-copy removal away from a
	// write racing the metrics writer's json.Marshal.
	recNodesBefore := make([]PathNode, len(rec.nodes))
	copy(recNodesBefore, rec.nodes)

	// Every mutator after the seal is a no-op — including the ones that would
	// add a vertex, an edge or a claim about completeness.
	if id := rec.AddProxy("late", "10.0.0.9:1080"); id != "" {
		t.Errorf("AddProxy after Snapshot = %q, want \"\"", id)
	}
	if id := rec.AddProxyAttempt("late", "10.0.0.9:1080", 2); id != "" {
		t.Errorf("AddProxyAttempt after Snapshot = %q, want \"\"", id)
	}
	if id := rec.AddServer("10.0.0.10:1080"); id != "" {
		t.Errorf("AddServer after Snapshot = %q, want \"\"", id)
	}
	// The pokes that would otherwise write IN PLACE rather than add: filling a
	// node with no address, and renaming a bare server node to a proxy ID. A
	// sealed recorder still answers an ID it holds — sealing stops new state,
	// it does not make the recorder lie about the graph it has — but adopts
	// nothing behind it.
	if id := rec.AddProxy("tunnel", "198.51.100.10:1080"); id != tunnel {
		t.Errorf("AddProxy for a sealed address-less node = %q, want the existing %q", id, tunnel)
	}
	if id := rec.AddProxy("renamed", "203.0.113.9:1080"); id != "" {
		t.Errorf("AddProxy over a sealed server node = %q, want \"\" — no vertex is renamed after the seal", id)
	}
	if id := rec.AddTarget("other.example", "80"); id != NodeTargetID {
		// AddTarget answers from its index before the seal is consulted, so it
		// still names the existing node — what the seal guarantees is that no
		// second destination is recorded.
		t.Errorf("AddTarget after Snapshot = %q, want %q (the existing node, not a new one)", id, NodeTargetID)
	}
	rec.AddLocal()
	rec.NotePartial()
	late := pthTCPConn("10.0.0.11:1080")
	conn, err := rec.Observe(Leg{Kind: LegDial, To: hop}, nil, pthOK(late))
	if conn != late || err != nil {
		t.Errorf("sealed Observe = (%v, %v), want the conn fn produced and no error", conn, err)
	}
	// A sealed Observe still runs fn exactly once.
	calls := 0
	_, _ = rec.Observe(Leg{Kind: LegStack, To: target}, nil, func() (net.Conn, error) { calls++; return nil, nil })
	if calls != 1 {
		t.Errorf("sealed Observe called fn %d times, want 1", calls)
	}

	after := rec.Snapshot()
	if after != first {
		t.Errorf("Snapshot after mutators = %p, want the same pointer %p", after, first)
	}
	if len(after.Nodes) != nodesBefore {
		t.Errorf("nodes grew after the seal: %v, want %d", pthIDs(after), nodesBefore)
	}
	if len(after.Edges) != edgesBefore {
		t.Errorf("edges grew after the seal: %d, want %d", len(after.Edges), edgesBefore)
	}
	if after.Partial != partialBefore {
		t.Errorf("Partial = %v after the seal, want %v", after.Partial, partialBefore)
	}
	if after.Dropped != droppedBefore {
		t.Errorf("Dropped = %d after the seal, want %d", after.Dropped, droppedBefore)
	}
	for i := range before {
		if after.Nodes[i] != before[i] {
			t.Errorf("node %d changed after the seal: %+v, want %+v", i, after.Nodes[i], before[i])
		}
	}
	// The recorder's own node and edge lists are what the seal protects, so
	// check them directly: no mutator after the first Snapshot may grow them,
	// and none may rewrite a slot in place — neither the address adoption that
	// fills a node with no address, nor the rename that would give a bare
	// server node a proxy ID and rewrite the edges pointing at it.
	if len(rec.nodes) != len(recNodesBefore) {
		t.Errorf("recorder holds %d nodes after the seal, want the %d it was snapshotted with", len(rec.nodes), len(recNodesBefore))
	}
	for i := range recNodesBefore {
		if i >= len(rec.nodes) {
			break
		}
		if rec.nodes[i] != recNodesBefore[i] {
			t.Errorf("recorder node %d changed after the seal: %+v, want %+v", i, rec.nodes[i], recNodesBefore[i])
		}
	}
	if len(rec.edges) != 2 {
		t.Errorf("recorder holds %d edges after the seal, want the 2 it was snapshotted with", len(rec.edges))
	}
	if rec.dropped != 0 {
		t.Errorf("recorder dropped = %d after the seal, want 0", rec.dropped)
	}
	// The two in-place pokes, named: the address-less node kept its empty
	// address and the server node kept its ID.
	if i, ok := rec.byID[tunnel]; !ok {
		t.Errorf("node %q vanished from the recorder's index", tunnel)
	} else if rec.nodes[i].Address != "" || rec.nodes[i].AddressSource != "" {
		t.Errorf("sealed recorder adopted a late address onto %q: %+v, want the empty address it was snapshotted with", tunnel, rec.nodes[i])
	}
	if _, ok := rec.byID[srv]; !ok {
		t.Errorf("node %q was renamed away after the seal", srv)
	}
	if _, ok := rec.byID["proxy:renamed"]; ok {
		t.Errorf("a sealed AddProxy registered %q, want no new vertex", "proxy:renamed")
	}
	// NotePartial is seal-guarded like every other mutator: the graph Snapshot
	// handed out is the one readers keep, so a completeness claim made after the
	// seal writes nothing at all — not even the recorder's own flag.
	if rec.partial {
		t.Errorf("recorder partial = true after a sealed NotePartial, want false (every mutator is a no-op once Snapshot sealed)")
	}
	// Readers that only ask questions keep working on a sealed recorder.
	if got := rec.LastNodeID(); got != target {
		t.Errorf("LastNodeID = %q, want %q", got, target)
	}
	if got := rec.TargetNodeID(); got != target {
		t.Errorf("TargetNodeID = %q, want %q", got, target)
	}
	if got := rec.TargetNodeIDFor("example.com", "443"); got != target {
		t.Errorf("TargetNodeIDFor = %q, want %q", got, target)
	}
	// An already-known ID is still returned: sealing stops new state, it does
	// not make the recorder lie about the graph it holds.
	if got := rec.AddProxy("ob1", "1.2.3.4:1080"); got != hop {
		t.Errorf("AddProxy for an existing node after the seal = %q, want %q", got, hop)
	}
}

// TestSnapshotDeepCopyIsIndependent: the returned slices are copies, so the
// metrics writer's marshal and a caller's own reading cannot see the recorder's
// mutable state — and the recorder's state cannot be reached through them.
func TestSnapshotDeepCopyIsIndependent(t *testing.T) {
	rec := NewPathRecorder()
	rec.AddLocal()
	target := rec.AddTarget("example.com", "443")
	hop := rec.AddProxy("ob1", "1.2.3.4:1080")
	if _, err := rec.Observe(Leg{Kind: LegDial, To: hop, Through: target}, nil, pthOK(pthTCPConn("127.0.0.1:9999"))); err != nil {
		t.Fatalf("Observe: %v", err)
	}

	g := rec.Snapshot()
	if len(g.Nodes) != 3 || len(g.Edges) != 2 {
		t.Fatalf("graph = %d nodes / %d edges, want 3 / 2", len(g.Nodes), len(g.Edges))
	}
	// Not the recorder's backing arrays: different element addresses.
	if &g.Nodes[0] == &rec.nodes[0] {
		t.Errorf("Snapshot().Nodes aliases the recorder's node array")
	}
	if &g.Edges[0] == &rec.edges[0] {
		t.Errorf("Snapshot().Edges aliases the recorder's edge array")
	}
	nodesBefore := make([]PathNode, len(rec.nodes))
	copy(nodesBefore, rec.nodes)
	edgesBefore := make([]PathEdge, len(rec.edges))
	copy(edgesBefore, rec.edges)

	// Mutating what Snapshot handed back must not reach the recorder — that is
	// the copy the writer goroutine marshals from, and the recorder's own arrays
	// are what a later read compares against.
	for i := range g.Nodes {
		g.Nodes[i].ID = fmt.Sprintf("clobbered-%d", i)
		g.Nodes[i].Address = "clobbered"
		g.Nodes[i].AddressSource = AddrSource("clobbered")
	}
	for i := range g.Edges {
		g.Edges[i].From = "clobbered"
		g.Edges[i].To = "clobbered"
		g.Edges[i].Kind = LegKind("clobbered")
		g.Edges[i].Error = "clobbered"
	}
	for i := range rec.nodes {
		if rec.nodes[i] != nodesBefore[i] {
			t.Errorf("recorder node %d changed through the snapshot: %+v, want %+v", i, rec.nodes[i], nodesBefore[i])
		}
	}
	for i := range rec.edges {
		if rec.edges[i] != edgesBefore[i] {
			t.Errorf("recorder edge %d changed through the snapshot: %+v, want %+v", i, rec.edges[i], edgesBefore[i])
		}
	}
	for _, id := range []string{"clobbered", "clobbered-0"} {
		if _, ok := rec.byID[id]; ok {
			t.Errorf("recorder index now resolves %q, want the copy to be unreachable", id)
		}
	}
}

// TestPathRecorderContextRoundTrip: the ctx carrier is how the dial seams that
// see no Metadata — CheckDial's callers and routeDialer.DialUpstream, which
// builds a fresh Metadata of its own — find the request's recorder. A nil
// recorder installs nothing, so those seams fall back to inert no-ops.
func TestPathRecorderContextRoundTrip(t *testing.T) {
	rec := NewPathRecorder()
	base := context.Background()

	if got := PathRecorderFromContext(base); got != nil {
		t.Errorf("PathRecorderFromContext(background) = %v, want nil", got)
	}
	if got := PathRecorderFromContext(nil); got != nil {
		t.Errorf("PathRecorderFromContext(nil) = %v, want nil", got)
	}

	ctx := WithPathRecorder(base, rec)
	if got := PathRecorderFromContext(ctx); got != rec {
		t.Errorf("PathRecorderFromContext = %p, want the installed recorder %p", got, rec)
	}

	// WithPathRecorder(ctx, nil) installs nothing and changes nothing, so a
	// caller holding a nil recorder does not shadow one further up the chain.
	if got := WithPathRecorder(base, nil); got != base {
		t.Errorf("WithPathRecorder(ctx, nil) returned a derived ctx, want ctx unchanged")
	}
	if got := PathRecorderFromContext(WithPathRecorder(base, nil)); got != nil {
		t.Errorf("PathRecorderFromContext(WithPathRecorder(ctx, nil)) = %v, want nil", got)
	}
	outer := WithPathRecorder(base, rec)
	if got := PathRecorderFromContext(WithPathRecorder(outer, nil)); got != rec {
		t.Errorf("WithPathRecorder(ctx, nil) shadowed the installed recorder: got %p, want %p", got, rec)
	}

	// The carrier survives derivation, which is what nested DialUpstream calls
	// rely on: each one derives a WithValue/WithTimeout ctx of its own.
	derived, cancel := context.WithTimeout(WithPathRecorder(outer, rec), time.Minute)
	defer cancel()
	if got := PathRecorderFromContext(derived); got != rec {
		t.Errorf("PathRecorderFromContext(derived) = %p, want %p", got, rec)
	}
	wrapped := context.WithValue(derived, pthOtherKey{}, "other")
	if got := PathRecorderFromContext(wrapped); got != rec {
		t.Errorf("PathRecorderFromContext(wrapped) = %p, want %p", got, rec)
	}

	// A different key type does not collide with the carrier.
	if got := PathRecorderFromContext(context.WithValue(base, pthOtherKey{}, rec)); got != nil {
		t.Errorf("PathRecorderFromContext found a recorder under a foreign key: %v, want nil", got)
	}
}

// pthOtherKey is a second context key type, to prove the carrier is keyed on
// its own unexported type.
type pthOtherKey struct{}

// TestObserveSelfLoopRecordsNothingAndMarksPartial: a leg from a server to
// itself (a cyclic upstream reference) is not topology, so the graph says it is
// incomplete rather than inventing an edge.
func TestObserveSelfLoopRecordsNothingAndMarksPartial(t *testing.T) {
	rec := NewPathRecorder()
	rec.AddLocal()
	x := rec.AddProxy("ob1", "1.2.3.4:1080")

	conn := pthTCPConn("1.2.3.4:1080")
	got, err := rec.Observe(Leg{Kind: LegDial, From: x, To: x, Target: "1.2.3.4:1080"}, nil, pthOK(conn))
	if got != conn || err != nil {
		t.Errorf("Observe = (%v, %v), want the conn fn produced and no error", got, err)
	}
	g := rec.Snapshot()
	if len(g.Edges) != 0 {
		t.Errorf("edges = %+v, want none for a self-loop", g.Edges)
	}
	if !g.Partial {
		t.Errorf("Partial = false, want true — the graph is missing the leg it refused to invent")
	}
	if got := rec.LastNodeID(); got != "" {
		t.Errorf("LastNodeID = %q, want \"\" — no leg arrived", got)
	}
	// The self-loop's socket peer is not adopted either: nothing was reached.
	if n := pthNode(g, x); n == nil {
		t.Errorf("node %q missing from %v", x, pthIDs(g))
	} else if n.AddressSource == AddrSocket {
		t.Errorf("node %q adopted %q (%q) from a self-loop, want the configured address", x, n.Address, n.AddressSource)
	}

	// The same for the local node addressed as a destination.
	rec2 := NewPathRecorder()
	rec2.AddLocal()
	if _, err := rec2.Observe(Leg{Kind: LegStack, From: NodeLocalID, To: NodeLocalID}, nil, pthOK(nil)); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if g2 := rec2.Snapshot(); len(g2.Edges) != 0 || !g2.Partial {
		t.Errorf("local self-loop = %d edges, Partial %v; want 0 edges and Partial true", len(g2.Edges), g2.Partial)
	}
}

// TestObserveUnknownToMarksPartial: a leg whose destination was never named has
// no vertex to point at, so nothing is recorded and the graph is marked
// incomplete rather than gaining an edge to a node that does not exist.
func TestObserveUnknownToMarksPartial(t *testing.T) {
	rec := NewPathRecorder()
	rec.AddLocal()
	rec.AddProxy("ob1", "1.2.3.4:1080")

	if _, err := rec.Observe(Leg{Kind: LegDial, To: "proxy:never-added", Target: "1.2.3.4:1080"}, nil, pthOK(nil)); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	g := rec.Snapshot()
	if len(g.Edges) != 0 {
		t.Errorf("edges = %+v, want none for an unknown To", g.Edges)
	}
	if !g.Partial {
		t.Errorf("Partial = false, want true")
	}
	if got := rec.LastNodeID(); got != "" {
		t.Errorf("LastNodeID = %q, want \"\"", got)
	}

	// An empty To with no Through is the same case: there is nowhere to point.
	rec2 := NewPathRecorder()
	rec2.AddLocal()
	if _, err := rec2.Observe(Leg{Kind: LegDial, To: "", Through: ""}, nil, pthOK(nil)); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if g2 := rec2.Snapshot(); len(g2.Edges) != 0 || !g2.Partial {
		t.Errorf("empty To = %d edges, Partial %v; want 0 edges and Partial true", len(g2.Edges), g2.Partial)
	}

	// A hop with no server of its own (direct) runs straight to Through, so an
	// unknown Through with no To is also unrecordable.
	rec3 := NewPathRecorder()
	rec3.AddLocal()
	if _, err := rec3.Observe(Leg{Kind: LegStack, To: "", Through: "target"}, nil, pthOK(nil)); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if g3 := rec3.Snapshot(); len(g3.Edges) != 0 || !g3.Partial {
		t.Errorf("unknown Through = %d edges, Partial %v; want 0 edges and Partial true", len(g3.Edges), g3.Partial)
	}

	// An unknown From falls back to the local node when there is one, so the leg
	// is still recorded — it really did leave the engine host.
	rec4 := NewPathRecorder()
	rec4.AddLocal()
	rec4.AddTarget("example.com", "443")
	if _, err := rec4.Observe(Leg{Kind: LegStack, From: "proxy:gone", To: NodeTargetID}, nil, pthOK(nil)); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	g4 := rec4.Snapshot()
	if len(g4.Edges) != 1 || g4.Edges[0].From != NodeLocalID {
		t.Errorf("edges = %+v, want one leg from %q", g4.Edges, NodeLocalID)
	}
	if g4.Partial {
		t.Errorf("Partial = true, want false — the leg was recorded against the local node")
	}

	// With no local node to fall back to, an unknown From is unrecordable.
	rec5 := NewPathRecorder()
	to5 := rec5.AddProxy("ob1", "1.2.3.4:1080")
	if _, err := rec5.Observe(Leg{Kind: LegTunnel, From: "proxy:gone", To: to5}, nil, pthOK(nil)); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if g5 := rec5.Snapshot(); len(g5.Edges) != 0 || !g5.Partial {
		t.Errorf("unknown From with no local node = %d edges, Partial %v; want 0 and true", len(g5.Edges), g5.Partial)
	}

	// A Through that was never named does not kill the leg: the arrival is
	// recorded and the un-nameable tunnel edge is dropped — with Partial set,
	// because the graph is missing a leg it was explicitly told about.
	rec6 := NewPathRecorder()
	rec6.AddLocal()
	hop6 := rec6.AddProxy("ob1", "1.2.3.4:1080")
	if _, err := rec6.Observe(Leg{Kind: LegDial, To: hop6, Through: "proxy:gone"}, nil, pthOK(nil)); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	g6 := rec6.Snapshot()
	if len(g6.Edges) != 1 || g6.Edges[0].To != hop6 {
		t.Errorf("edges = %+v, want the arrival leg only", g6.Edges)
	}
	if got := rec6.LastNodeID(); got != hop6 {
		t.Errorf("LastNodeID = %q, want %q", got, hop6)
	}
	if !g6.Partial {
		t.Errorf("Partial = false, want true — dropping a named tunnel edge is losing a leg")
	}
}

// TestObserveNestedRouteKeepsItsOwnSocket: when the plugin reached its server
// through a route the engine recorded inside the same bracketed call, the conn
// it returns is that route's socket. The nested leg adopted it already, so the
// outer leg adopts nothing onto To — the address it carries describes a dial the
// outer leg never made.
func TestObserveNestedRouteKeepsItsOwnSocket(t *testing.T) {
	rec := NewPathRecorder()
	rec.AddLocal()
	x := rec.AddProxy("x", "1.2.3.4:1080")
	member := rec.AddProxy("y", "5.6.7.8:1080")
	if member == "" {
		t.Fatalf("setup: AddProxy(y) returned no ID")
	}

	// Inside the bracket a nested route records its own leg, so the recorder
	// is demonstrably not in the state it was when this call started.
	conn := pthTCPConn("5.6.7.8:1080")
	if _, err := rec.Observe(Leg{Kind: LegDial, To: x, Through: member}, nil, func() (net.Conn, error) {
		_, _ = rec.Observe(Leg{Kind: LegStack, To: member}, nil, pthOK(conn))
		return conn, nil
	}); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	g := rec.Snapshot()
	if len(g.Edges) != 3 {
		t.Fatalf("edges = %+v, want the nested leg, the arrival and its tunnel toward %q", g.Edges, member)
	}
	if e := g.Edges[1]; e.From != NodeLocalID || e.To != x || e.Kind != LegDial {
		t.Errorf("arrival edge = %s -> %s (%s), want local -> %s (dial)", e.From, e.To, e.Kind, x)
	}
	// The outer call's own leg dialed nothing fresh — the nested route did — so
	// the address it returns belongs to the leg that really dialed it.
	if n := pthNode(g, x); n == nil {
		t.Fatalf("node %q missing from %v", x, pthIDs(g))
	} else if n.Address != "1.2.3.4:1080" || n.AddressSource != AddrConfigured {
		t.Errorf("node %q Address = %q (%q), want the configured 1.2.3.4:1080 — nested recording suppresses adoption", x, n.Address, n.AddressSource)
	}
	if n := pthNode(g, member); n == nil {
		t.Fatalf("node %q missing from %v", member, pthIDs(g))
	} else if n.Address != "5.6.7.8:1080" || n.AddressSource != AddrSocket {
		t.Errorf("node %q Address = %q (%q), want the nested leg's socket peer 5.6.7.8:1080", member, n.Address, n.AddressSource)
	}
}

// TestObserveNestedRecordingSuppressesAdoption: a leg whose own call recorded
// other legs inside it did not dial the socket it returns — a nested route did —
// so it adopts nothing onto To, even when it is Collapsed (which only exempts it
// from arrival suppression, not from the address rule).
func TestObserveNestedRecordingSuppressesAdoption(t *testing.T) {
	rec := NewPathRecorder()
	rec.AddLocal()
	hop := rec.AddProxy("hop", "1.2.3.4:1080")
	far := rec.AddProxy("far", "5.6.7.8:1080")

	conn := pthTCPConn("10.9.9.9:1080")
	if _, err := rec.Observe(Leg{Kind: LegDial, To: hop, Collapsed: true}, nil, func() (net.Conn, error) {
		// The hop reached its server through a route the engine recorded: the
		// nested leg's own socket peer is what the returned conn carries.
		_, _ = rec.Observe(Leg{Kind: LegStack, To: far}, nil, pthOK(conn))
		return conn, nil
	}); err != nil {
		t.Fatalf("Observe: %v", err)
	}

	g := rec.Snapshot()
	if len(g.Edges) != 2 {
		t.Fatalf("edges = %+v, want the nested leg plus the collapsed arrival", g.Edges)
	}
	if e := g.Edges[1]; !e.Collapsed || e.To != hop {
		t.Errorf("second edge = %+v, want the collapsed arrival at %q", e, hop)
	}
	if n := pthNode(g, hop); n == nil {
		t.Fatalf("node %q missing from %v", hop, pthIDs(g))
	} else if n.Address != "1.2.3.4:1080" || n.AddressSource != AddrConfigured {
		t.Errorf("node %q Address = %q (%q), want the configured 1.2.3.4:1080 — the socket belongs to the nested leg", hop, n.Address, n.AddressSource)
	}
	if n := pthNode(g, far); n == nil {
		t.Fatalf("node %q missing from %v", far, pthIDs(g))
	} else if n.Address != "10.9.9.9:1080" || n.AddressSource != AddrSocket {
		t.Errorf("node %q Address = %q (%q), want the socket peer 10.9.9.9:1080 — the leg that dialed it adopts it", far, n.Address, n.AddressSource)
	}
	if got := rec.LastNodeID(); got != hop {
		t.Errorf("LastNodeID = %q, want %q", got, hop)
	}
}

// TestTwoIdentitiesSharingOneAddressStayTwoVertices: two outbounds pointed at
// one physical server — a pool of same-address members with different
// credentials, or a chain whose hops share a relay — must not be folded into one
// vertex. Renaming hands the loser's address, error and classification to the
// winner and deletes a hop the request really rode; a bare server node, which
// has no identity of its own, IS upgraded in place.
func TestTwoIdentitiesSharingOneAddressStayTwoVertices(t *testing.T) {
	const addr = "203.0.113.11:1080"
	boom := errors.New("pth: connection refused")
	rec := NewPathRecorder()
	rec.SetClassifier(func(LegInfo, error) (string, string) { return "connect", "connect_refused" })
	rec.AddLocal()

	m1 := rec.AddProxyAttempt("m1", addr, 1)
	if m1 != "proxy:m1" {
		t.Fatalf("AddProxyAttempt(m1) = %q, want %q", m1, "proxy:m1")
	}
	if _, err := rec.Observe(Leg{Kind: LegDial, To: m1, Target: addr}, nil, pthFail(boom)); err != boom {
		t.Fatalf("Observe: %v", err)
	}
	m2 := rec.AddProxyAttempt("m2", addr, 2)
	if m2 != "proxy:m2#2" {
		t.Fatalf("AddProxyAttempt(m2) = %q, want %q", m2, "proxy:m2#2")
	}
	if _, err := rec.Observe(Leg{Kind: LegDial, To: m2, Target: addr}, nil, pthOK(pthTCPConn(addr))); err != nil {
		t.Fatalf("Observe: %v", err)
	}

	g := rec.Snapshot()
	// D1: one edge per attempt, and the failed member keeps its own identity.
	if ids := pthIDs(g); len(ids) != 3 {
		t.Fatalf("nodes = %v, want [local proxy:m1 proxy:m2#2]", ids)
	}
	if len(g.Edges) != 2 {
		t.Fatalf("edges = %+v, want one leg per attempt", g.Edges)
	}
	lost := g.Edges[0]
	if lost.To != m1 || lost.Success {
		t.Errorf("first edge = %+v, want the failed attempt at %q", lost, m1)
	}
	if lost.Phase != "connect" || lost.Reason != "connect_refused" {
		t.Errorf("failed edge Phase/Reason = %q/%q, want connect/connect_refused — the loser keeps its own classification", lost.Phase, lost.Reason)
	}
	if won := g.Edges[1]; won.To != m2 || !won.Success {
		t.Errorf("second edge = %+v, want the winning attempt at %q", won, m2)
	}
	n1 := pthNode(g, m1)
	if n1 == nil {
		t.Fatalf("node %q missing from %v", m1, pthIDs(g))
	}
	if n1.OutboundID != "m1" || n1.Attempt != 1 {
		t.Errorf("node %q OutboundID/Attempt = %q/%d, want m1/1 — no identity is overwritten", m1, n1.OutboundID, n1.Attempt)
	}
	if n1.Address != addr || n1.AddressSource != AddrConfigured {
		t.Errorf("node %q Address = %q (%q), want the configured %q", m1, n1.Address, n1.AddressSource, addr)
	}
	n2 := pthNode(g, m2)
	if n2 == nil {
		t.Fatalf("node %q missing from %v", m2, pthIDs(g))
	}
	if n2.OutboundID != "m2" || n2.Attempt != 2 {
		t.Errorf("node %q OutboundID/Attempt = %q/%d, want m2/2", m2, n2.OutboundID, n2.Attempt)
	}
	if n2.Address != addr || n2.AddressSource != AddrSocket {
		t.Errorf("node %q Address = %q (%q), want the socket peer %q", m2, n2.Address, n2.AddressSource, addr)
	}
	if g.Partial {
		t.Errorf("Partial = true, want false — nothing was lost, both hops are named")
	}
	// The MOST RECENTLY registered vertex owns the address index, so a later
	// AddServer resolves to the attempt the engine is now talking to. That is
	// what makes a shared-address pool failover honest: noteUpstreamArrival
	// resolves the winning attempt's arrival through AddServer(target), and it
	// must land on the winner (m2#2), not on the loser (m1) that was registered
	// first — otherwise the success edge is drawn into the failed attempt and a
	// fabricated direct dial of the winner appears beside it.
	if got := rec.AddServer(addr); got != m2 {
		t.Errorf("AddServer(%q) = %q, want %q — the most recent registrant owns the address index", addr, got, m2)
	}

	// Two chain hops sharing one relay are the same case with AddProxy.
	rec2 := NewPathRecorder()
	rec2.AddLocal()
	h0 := rec2.AddProxy("h0", addr)
	h1 := rec2.AddProxy("h1", addr)
	if h0 == "" || h1 == "" || h0 == h1 {
		t.Fatalf("AddProxy over one address = %q / %q, want two distinct vertices", h0, h1)
	}
	if g2 := rec2.Snapshot(); len(g2.Nodes) != 3 {
		t.Errorf("nodes = %v, want [local proxy:h0 proxy:h1]", pthIDs(g2))
	}

	// A bare server node has no identity to protect, so naming the outbound
	// that owns its address upgrades it in place and rewrites the edges.
	rec3 := NewPathRecorder()
	rec3.AddLocal()
	srv := rec3.AddServer(addr)
	if srv != "server:"+addr {
		t.Fatalf("AddServer = %q, want %q", srv, "server:"+addr)
	}
	if _, err := rec3.Observe(Leg{Kind: LegStack, To: srv, Target: addr}, nil, pthOK(nil)); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	named := rec3.AddProxy("ob1", addr)
	if named != "proxy:ob1" {
		t.Fatalf("AddProxy over a server node = %q, want %q", named, "proxy:ob1")
	}
	g3 := rec3.Snapshot()
	if len(g3.Nodes) != 2 {
		t.Errorf("nodes = %v, want [local proxy:ob1] — the server node is upgraded, not duplicated", pthIDs(g3))
	}
	if len(g3.Edges) != 1 || g3.Edges[0].To != "proxy:ob1" {
		t.Errorf("edges = %+v, want the one leg rewritten to %q", g3.Edges, "proxy:ob1")
	}
	if got := rec3.LastNodeID(); got != "proxy:ob1" {
		t.Errorf("LastNodeID = %q, want %q — the rename rewrites the arrival pointer", got, "proxy:ob1")
	}
	if n := pthNode(g3, "proxy:ob1"); n == nil {
		t.Fatalf("node %q missing from %v", "proxy:ob1", pthIDs(g3))
	} else if n.OutboundID != "ob1" || n.Address != addr {
		t.Errorf("upgraded node OutboundID/Address = %q/%q, want ob1/%q", n.OutboundID, n.Address, addr)
	}
}

// TestInflightArrivalPrefersInnermostBracket: two vertices naming one physical
// server can both have a dial bracket open — a chain hop riding a route whose
// member is the same server — and the arrival belongs to the INNERMOST one,
// because that is the server the engine is talking to right now. The outer
// bracket is only waiting on the inner dial to finish; resolving to it would
// draw the nested route's arrival into a hop that has not been reached yet.
//
// The claim is scoped to the bracket that made it: once the inner one closes,
// the outer bracket is innermost again.
func TestInflightArrivalPrefersInnermostBracket(t *testing.T) {
	const addr = "203.0.113.71:1080"
	rec := NewPathRecorder()
	rec.AddLocal()
	x := rec.AddProxy("x", addr)
	y := rec.AddProxy("y", addr)
	if x != "proxy:x" || y != "proxy:y" {
		t.Fatalf("AddProxy over one address = %q / %q, want two distinct vertices proxy:x / proxy:y", x, y)
	}

	var innermost, afterInner string
	if _, err := rec.Observe(Leg{Kind: LegDial, To: x, Target: addr}, nil, func() (net.Conn, error) {
		if _, err := rec.Observe(Leg{Kind: LegDial, From: x, To: y, Target: addr}, nil, func() (net.Conn, error) {
			innermost = rec.ServerNodeForArrival(addr, "")
			return pthTCPConn(addr), nil
		}); err != nil {
			t.Fatalf("inner Observe: %v", err)
		}
		afterInner = rec.ServerNodeForArrival(addr, "")
		return pthTCPConn(addr), nil
	}); err != nil {
		t.Fatalf("Observe: %v", err)
	}

	if innermost != y {
		t.Errorf("ServerNodeForArrival inside both brackets = %q, want %q — the innermost bracket is the dial in flight", innermost, y)
	}
	if afterInner != x {
		t.Errorf("ServerNodeForArrival after the inner bracket closed = %q, want %q — the claim ends with the bracket that made it", afterInner, x)
	}
}

// TestServerNodeForArrivalExcludesSource pins `exclude`: the vertex a nested
// route delivered FROM may itself name the address being resolved (a member
// riding the very server the hop asked it to reach), and it must not win — an
// arrival from y into y is no leg at all, and noteUpstreamArrival drops it, so
// the honest edge would vanish from the graph.
func TestServerNodeForArrivalExcludesSource(t *testing.T) {
	const addr = "203.0.113.72:1080"
	rec := NewPathRecorder()
	rec.AddLocal()
	x := rec.AddProxy("x", addr)
	y := rec.AddProxy("y", addr)
	if x != "proxy:x" || y != "proxy:y" {
		t.Fatalf("AddProxy over one address = %q / %q, want two distinct vertices proxy:x / proxy:y", x, y)
	}

	var got string
	if _, err := rec.Observe(Leg{Kind: LegDial, To: x, Target: addr}, nil, func() (net.Conn, error) {
		if _, err := rec.Observe(Leg{Kind: LegDial, From: x, To: y, Target: addr}, nil, func() (net.Conn, error) {
			got = rec.ServerNodeForArrival(addr, y)
			return pthTCPConn(addr), nil
		}); err != nil {
			t.Fatalf("inner Observe: %v", err)
		}
		return pthTCPConn(addr), nil
	}); err != nil {
		t.Fatalf("Observe: %v", err)
	}

	if got != x {
		t.Errorf("ServerNodeForArrival(%q, %q) = %q, want %q — the arrival's own source is skipped, the bracket outside it answers", addr, y, got, x)
	}
}

// TestRenameMidBracketKeepsInflightClaim: a bracket's destination is a node ID,
// and a bare "server:<addr>" vertex can be renamed out from under the bracket
// that is dialing it — AddProxy names the outbound that owns the address and
// upgrades the vertex in place. In-tree: hopNodeID returns "" for a
// protocol-"direct" hop, so dialLayers hands dialOutbound a bare server node as
// the bracket destination, and a nested route inside that bracket names an
// outbound at the same address. The rename must carry the in-flight claim to the
// new ID, because the bracket is dialing the same physical server under a new
// identity: left pointing at an ID the graph no longer has, the claim resolves
// to nothing and the arrival falls back to the last-owner address index — which
// a second vertex at that address has just stolen, so the leg lands on a node
// nobody dialed.
func TestRenameMidBracketKeepsInflightClaim(t *testing.T) {
	const addr = "203.0.113.73:1080"
	rec := NewPathRecorder()
	rec.AddLocal()
	srv := rec.AddServer(addr)
	if srv != NodeServerPrefix+addr {
		t.Fatalf("AddServer = %q, want %q", srv, NodeServerPrefix+addr)
	}

	var m1, m2, got string
	if _, err := rec.Observe(Leg{Kind: LegDial, Through: srv, Target: addr}, nil, func() (net.Conn, error) {
		// The config names the outbound that owns this address: the bare server
		// vertex is upgraded in place, renaming the bracket's own destination.
		m1 = rec.AddProxy("m1", addr)
		// A second identity at the same address keeps its own vertex and, as the
		// most recent registrant, takes the address index.
		m2 = rec.AddProxy("m2", addr)
		got = rec.ServerNodeForArrival(addr, "")
		return pthTCPConn(addr), nil
	}); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if m1 != "proxy:m1" || m2 != "proxy:m2" {
		t.Fatalf("AddProxy over the shared address = %q / %q, want proxy:m1 / proxy:m2", m1, m2)
	}
	if got != m1 {
		t.Errorf("ServerNodeForArrival inside the bracket = %q, want %q — the bracket is dialing the renamed node; %q is a vertex nobody dialed", got, m1, m2)
	}

	// The claim must also END with the bracket: the entry was pushed under the
	// pre-rename spelling, so a pop matching that spelling misses and strands
	// the claim for the rest of the request. With proxy:m2 the honest last
	// owner of the address, both resolvers have to fall through to the address
	// index once Observe returns — a surviving entry keeps answering as if the
	// engine were still dialing proxy:m1, and every later arrival in this
	// request (a post-dial AddServer, a nested route's noteUpstreamArrival, a
	// real Router.Dial's tunnel leg) lands on a node nobody is dialing.
	if after := rec.ServerNodeForArrival(addr, ""); after != m2 {
		t.Errorf("ServerNodeForArrival after the bracket = %q, want %q — the renamed bracket entry was stranded, not popped", after, m2)
	}
	if after := rec.AddServer(addr); after != m2 {
		t.Errorf("AddServer after the bracket = %q, want %q — a claim that outlives its bracket misattributes every later arrival", after, m2)
	}
}

// TestInflightPushResolvesStaleDestination pins the RESOLVED push in Observe
// (inflightEntry{id: r.resolveLocked(dest), ...}): the producer captured dest
// before Observe took the lock, and a rename in between leaves a spelling
// renameLocked cannot rescue — its in-flight re-pointing matches entries that
// are ALREADY on the stack, and this entry does not exist yet at rename time,
// while a raw push would put a dead ID on the stack that
// inflightForAddrLocked's byID lookup misses. Every arrival inside the bracket
// would then fall through to the address index's last owner, a vertex nobody
// is dialing.
//
// The in-tree shape is the empty outbound ID: hopNodeID's AddProxy("", addr)
// falls through to the bare "server:<addr>" vertex (addProxyLocked's empty-ID
// fall-through) and dialLayers names the successor in the SAME iteration, so
// two hops sharing one address have the successor's AddProxy rename that
// vertex BEFORE dialOutbound opens the bracket.
// TestRenameMidBracketKeepsInflightClaim is the twin with the rename landing
// after the push, where renameLocked carries the live claim; the end-to-end
// dispatch pin is TestEmptyIDHopSharedAddressRideKeepsOneLegFromLocal in
// internal/router.
func TestInflightPushResolvesStaleDestination(t *testing.T) {
	const addr = "203.0.113.74:1080"
	rec := NewPathRecorder()
	rec.AddLocal()
	// The producer's captured destination: a bare server vertex, the same
	// spelling AddProxy("", addr) returns for an empty-ID outbound.
	srv := rec.AddServer(addr)
	if srv != NodeServerPrefix+addr {
		t.Fatalf("AddServer = %q, want %q", srv, NodeServerPrefix+addr)
	}
	// The rename runs BEFORE the producer brackets: the config names the
	// outbound that owns this address, so the upgrade gate adopts the bare
	// vertex in place and srv is a dead spelling from here on.
	m1 := rec.AddProxy("m1", addr)
	// A competing identity at the same address keeps its own vertex and, as
	// the most recent registrant, takes the address index: the node a lost
	// claim would wrongly resolve to.
	m2 := rec.AddProxy("m2", addr)
	if m1 != "proxy:m1" || m2 != "proxy:m2" {
		t.Fatalf("AddProxy over the shared address = %q / %q, want proxy:m1 / proxy:m2", m1, m2)
	}
	if owner := rec.byAddr[addr]; owner != m2 {
		t.Fatalf("byAddr[%q] = %q, want %q — the setup must leave the index on the competitor", addr, owner, m2)
	}
	if _, live := rec.byID[srv]; live {
		t.Fatalf("byID still holds %q — the captured spelling must be dead before the bracket opens", srv)
	}

	// The producer brackets with the dead spelling it captured, exactly as
	// dialUpstreamOutbound hands the AddProxy return to Observe.
	var inside string
	if _, err := rec.Observe(Leg{Kind: LegDial, To: srv, Target: addr}, nil, func() (net.Conn, error) {
		inside = rec.ServerNodeForArrival(addr, "")
		return pthTCPConn(addr), nil
	}); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if inside != m1 {
		t.Errorf("ServerNodeForArrival inside the bracket = %q, want %q — the push must resolve the captured spelling; %q is the address index's last owner, a vertex nobody dialed", inside, m1, m2)
	}

	// The claim ends with the bracket: the entry pops by token, so afterwards
	// the honest last owner answers again.
	if after := rec.ServerNodeForArrival(addr, ""); after != m2 {
		t.Errorf("ServerNodeForArrival after the bracket = %q, want %q — the claim outlived the bracket that made it", after, m2)
	}
	if after := rec.AddServer(addr); after != m2 {
		t.Errorf("AddServer after the bracket = %q, want %q — the honest last owner of the address", after, m2)
	}

	// The bracket's own leg lands on the renamed node: the post-fn resolve of
	// leg.To is a separate guard from the push, and both must land on m1.
	g := rec.Snapshot()
	if len(g.Edges) != 1 || g.Edges[0].From != NodeLocalID || g.Edges[0].To != m1 || !g.Edges[0].Success {
		t.Fatalf("edges = %+v, want one successful %s -> %s dial", g.Edges, NodeLocalID, m1)
	}
	if g.Partial {
		t.Errorf("Partial = true, want false — the leg was observed and drawn (edges %+v)", g.Edges)
	}
	if g.Dropped != 0 {
		t.Errorf("Dropped = %d, want 0", g.Dropped)
	}
}

// TestInflightPoppedAfterEveryBracket pins push/pop symmetry on the stack of
// open Observe brackets: a claim lives exactly as long as the call that made
// it. The resolvers prefer an open bracket over the address index because the
// bracket is the dial in flight, so an entry that survives its own bracket
// keeps overriding the honest last-owner index for the rest of the request —
// and the graph then publishes legs through a server the engine finished with.
//
// The shapes are the three ways a bracket can close: unchanged, renamed by a
// nested route, and nested with itself.
func TestInflightPoppedAfterEveryBracket(t *testing.T) {
	// (a) A plain bracket: the destination keeps its identity, so the claim is
	// visible inside fn and gone the moment Observe returns.
	const plain = "203.0.113.81:1080"
	rec := NewPathRecorder()
	rec.AddLocal()
	x := rec.AddProxy("x", plain)
	y := rec.AddProxy("y", plain)
	if x != "proxy:x" || y != "proxy:y" {
		t.Fatalf("AddProxy over one address = %q / %q, want two distinct vertices proxy:x / proxy:y", x, y)
	}

	var inside string
	if _, err := rec.Observe(Leg{Kind: LegDial, To: x, Target: plain}, nil, func() (net.Conn, error) {
		inside = rec.ServerNodeForArrival(plain, "")
		return pthTCPConn(plain), nil
	}); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if inside != x {
		t.Errorf("ServerNodeForArrival inside the bracket = %q, want %q — the bracket is dialing that server right now", inside, x)
	}
	if after := rec.ServerNodeForArrival(plain, ""); after != y {
		t.Errorf("ServerNodeForArrival after the bracket = %q, want %q — the claim ends with the bracket that made it", after, y)
	}
	if after := rec.AddServer(plain); after != y {
		t.Errorf("AddServer after the bracket = %q, want %q — the honest last owner of the address", after, y)
	}

	// (b) Two brackets open on ONE destination, renamed from inside the
	// innermost: both entries were pushed under the pre-rename spelling, so one
	// rename rewrites two of them and a spelling-matching pop strands both.
	const nested = "203.0.113.82:1080"
	rec2 := NewPathRecorder()
	rec2.AddLocal()
	srv := rec2.AddServer(nested)
	if srv != NodeServerPrefix+nested {
		t.Fatalf("AddServer = %q, want %q", srv, NodeServerPrefix+nested)
	}

	var n1, n2, innermost, afterInner string
	if _, err := rec2.Observe(Leg{Kind: LegDial, Through: srv, Target: nested}, nil, func() (net.Conn, error) {
		if _, err := rec2.Observe(Leg{Kind: LegDial, To: srv, Target: nested}, nil, func() (net.Conn, error) {
			// The config names the outbound that owns this address, upgrading the
			// bare server vertex both brackets named; a second identity at the
			// same address then takes the address index.
			n1 = rec2.AddProxy("n1", nested)
			n2 = rec2.AddProxy("n2", nested)
			innermost = rec2.ServerNodeForArrival(nested, "")
			return pthTCPConn(nested), nil
		}); err != nil {
			t.Fatalf("inner Observe: %v", err)
		}
		afterInner = rec2.ServerNodeForArrival(nested, "")
		return pthTCPConn(nested), nil
	}); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if n1 != "proxy:n1" || n2 != "proxy:n2" {
		t.Fatalf("AddProxy over the shared address = %q / %q, want proxy:n1 / proxy:n2", n1, n2)
	}
	if innermost != n1 {
		t.Errorf("ServerNodeForArrival inside both brackets = %q, want %q — the innermost bracket is the dial in flight", innermost, n1)
	}
	if afterInner != n1 {
		t.Errorf("ServerNodeForArrival after the inner bracket closed = %q, want %q — the outer bracket is dialing the same server under its new identity", afterInner, n1)
	}
	if afterBoth := rec2.ServerNodeForArrival(nested, ""); afterBoth != n2 {
		t.Errorf("ServerNodeForArrival after both brackets = %q, want %q — no claim survives, so the honest last owner answers", afterBoth, n2)
	}
	if after := rec2.AddServer(nested); after != n2 {
		t.Errorf("AddServer after both brackets = %q, want %q — a nested pair must not leak two claims", after, n2)
	}

	// (c) A rename inside one bracket must not corrupt the NEXT bracket on the
	// same address: the stack has to be empty between them, or the stranded
	// entry outlives the request's remaining dials and keeps answering for a
	// server the engine already left.
	const renamed = "203.0.113.83:1080"
	rec3 := NewPathRecorder()
	rec3.AddLocal()
	srv3 := rec3.AddServer(renamed)
	var m1, m2 string
	if _, err := rec3.Observe(Leg{Kind: LegDial, Through: srv3, Target: renamed}, nil, func() (net.Conn, error) {
		m1 = rec3.AddProxy("m1", renamed)
		m2 = rec3.AddProxy("m2", renamed)
		return pthTCPConn(renamed), nil
	}); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if m1 != "proxy:m1" || m2 != "proxy:m2" {
		t.Fatalf("AddProxy over the shared address = %q / %q, want proxy:m1 / proxy:m2", m1, m2)
	}

	var insideNext string
	if _, err := rec3.Observe(Leg{Kind: LegDial, To: m2, Target: renamed}, nil, func() (net.Conn, error) {
		insideNext = rec3.ServerNodeForArrival(renamed, "")
		return pthTCPConn(renamed), nil
	}); err != nil {
		t.Fatalf("Observe (next bracket): %v", err)
	}
	if insideNext != m2 {
		t.Errorf("ServerNodeForArrival inside the next bracket = %q, want %q", insideNext, m2)
	}
	if after := rec3.ServerNodeForArrival(renamed, ""); after != m2 {
		t.Errorf("ServerNodeForArrival after the next bracket = %q, want %q — the renamed entry from the first bracket was stranded", after, m2)
	}
}

// TestNoteLocalDialResetsLastNode: a producer that knows its next dial leaves
// the engine host — a collapsed re-dial, a probe redirect — says so, and the
// recorder stops reading the arrival as a nested route re-reaching a server the
// path already arrived at.
func TestNoteLocalDialResetsLastNode(t *testing.T) {
	rec := NewPathRecorder()
	rec.AddLocal()
	x := rec.AddProxy("ob1", "1.2.3.4:1080")
	if _, err := rec.Observe(Leg{Kind: LegDial, To: x}, nil, pthOK(pthTCPConn("1.2.3.4:1080"))); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if got := rec.LastNodeID(); got != x {
		t.Fatalf("LastNodeID = %q, want %q", got, x)
	}

	rec.NoteLocalDial()
	if got := rec.LastNodeID(); got != "" {
		t.Errorf("LastNodeID after NoteLocalDial = %q, want \"\" — the next leg leaves the engine host", got)
	}

	// With the pointer reset, a fresh dial toward the same server is recorded as
	// the arrival it really was instead of being suppressed as a nested route.
	if _, err := rec.Observe(Leg{Kind: LegDial, To: x, Collapsed: true}, nil, pthOK(pthTCPConn("1.2.3.4:1080"))); err != nil {
		t.Fatalf("Observe (re-dial): %v", err)
	}
	g := rec.Snapshot()
	if len(g.Edges) != 2 {
		t.Fatalf("edges = %+v, want the arrival plus the fresh local re-dial", g.Edges)
	}
	if e := g.Edges[1]; e.From != NodeLocalID || e.To != x {
		t.Errorf("second edge = %s -> %s, want %s -> %s", e.From, e.To, NodeLocalID, x)
	}

	// Inert on a nil receiver, and once Snapshot has sealed the recorder.
	var nilRec *PathRecorder
	nilRec.NoteLocalDial()

	rec2 := NewPathRecorder()
	rec2.AddLocal()
	y := rec2.AddProxy("ob2", "5.6.7.8:1080")
	if _, err := rec2.Observe(Leg{Kind: LegDial, To: y}, nil, pthOK(nil)); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if rec2.Snapshot() == nil {
		t.Fatalf("Snapshot = nil for a recorder with nodes and edges")
	}
	rec2.NoteLocalDial()
	if got := rec2.LastNodeID(); got != y {
		t.Errorf("LastNodeID after a sealed NoteLocalDial = %q, want %q — every mutator is inert once sealed", got, y)
	}
}

// TestPeerAddrRejectsNonHostPortConns: only a real socket peer is adopted. A
// unix socket, a net.Pipe, an in-memory tunnel conn and a nil address all mean
// "the engine could not see an address here", which is an empty Address rather
// than a fragment of one.
func TestPeerAddrRejectsNonHostPortConns(t *testing.T) {
	if got := peerAddr(nil); got != "" {
		t.Errorf("peerAddr(nil) = %q, want \"\"", got)
	}

	cases := []struct {
		name string
		addr net.Addr
		want string
	}{
		{"tcp", pthAddr{"tcp", "127.0.0.1:9999"}, "127.0.0.1:9999"},
		{"tcp ipv6", pthAddr{"tcp", "[::1]:443"}, "[::1]:443"},
		{"unix socket", pthAddr{"unix", "/tmp/goose.sock"}, ""},
		{"unix abstract", pthAddr{"unix", "@/tmp/hidden sock"}, ""},
		{"pipe", pthAddr{"pipe", "pipe"}, ""},
		{"no port", pthAddr{"tcp", "1.2.3.4"}, ""},
		{"empty", pthAddr{"tcp", ""}, ""},
		{"plugin shape", pthAddr{"tcp", "geph://tunnel"}, ""},
		{"non-numeric port", pthAddr{"tcp", "host:socks"}, ""},
		{"nil addr", nil, ""},
	}
	for _, tc := range cases {
		got := peerAddr(&pthConn{remote: tc.addr})
		if got != tc.want {
			t.Errorf("peerAddr(%s %v) = %q, want %q", tc.name, tc.addr, got, tc.want)
		}
	}

	// A real net.Pipe conn — what an in-memory tunnel hands the engine — is not
	// a socket peer.
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	if got := peerAddr(a); got != "" {
		t.Errorf("peerAddr(net.Pipe conn) = %q, want \"\"", got)
	}
}

// pthEdgeShapes renders a graph's edges as comparable strings, renaming every
// ID through ids first. Two runs of one shape — a no-rename twin and a run whose
// node was renamed inside the bracket — differ only in that node's spelling, so
// mapping both spellings onto one placeholder makes "same shape" assertable
// directly: same legs, same order, same kinds, same verdicts, same timing.
func pthEdgeShapes(g *PathGraph, ids map[string]string) []string {
	at := func(id string) string {
		if v, ok := ids[id]; ok {
			return v
		}
		return id
	}
	out := make([]string, 0, len(g.Edges))
	for _, e := range g.Edges {
		out = append(out, fmt.Sprintf("%s->%s %s success=%v timed=%v",
			at(e.From), at(e.To), e.Kind, e.Success, e.Duration > 0))
	}
	return out
}

// TestRenamedThroughDestinationKeepsItsLeg: a producer captures its leg's node
// IDs BEFORE the call it brackets, and the call can rename one of them — a
// nested upstream route naming the outbound that owns a bare "server:<addr>"
// vertex upgrades that vertex in place. The bracket's own destination is then a
// string the graph no longer has, so the byID guard dropped the leg and marked
// the graph Partial for a leg that was really observed: the engine's dial of a
// server it demonstrably reached vanished from the path.
//
// In-tree this is dialOutbound's first hop with no server of its own: hopNodeID
// returns "" for a protocol-"direct" hop, so the leg carries To "" and
// Through = the successor's bare server node, and the hop's own upstream route
// (upstream.go's AddProxy) renames it mid-call.
//
// The twin is the ground truth: the same shape at a sibling address with no
// rename inside the bracket. Both runs must publish one leg.
func TestRenamedThroughDestinationKeepsItsLeg(t *testing.T) {
	// run drives the shape once. outboundID "" leaves the bare server vertex
	// alone (the twin); a non-empty ID names the outbound that owns the address
	// inside fn, which renames the bracket's own destination.
	run := func(t *testing.T, addr, outboundID string) *PathGraph {
		t.Helper()
		rec := NewPathRecorder()
		rec.AddLocal()
		srv := rec.AddServer(addr)
		if srv != NodeServerPrefix+addr {
			t.Fatalf("AddServer = %q, want %q", srv, NodeServerPrefix+addr)
		}
		if _, err := rec.Observe(Leg{Kind: LegDial, Through: srv, Target: addr}, nil,
			func() (net.Conn, error) {
				if outboundID != "" {
					if id := rec.AddProxy(outboundID, addr); id != NodeProxyPrefix+outboundID {
						t.Fatalf("AddProxy = %q, want %q", id, NodeProxyPrefix+outboundID)
					}
				}
				return pthTCPConn(addr), nil
			}); err != nil {
			t.Fatalf("Observe: %v", err)
		}
		g := rec.Snapshot()
		if g == nil {
			t.Fatalf("Snapshot = nil, want a graph")
		}
		return g
	}

	const twinAddr = "203.0.113.91:1080"
	const renamedAddr = "203.0.113.92:1080"
	twin := run(t, twinAddr, "")
	renamed := run(t, renamedAddr, "m1")

	// The renamed run's leg is the twin's leg under the upgraded spelling.
	twinShape := pthEdgeShapes(twin, map[string]string{NodeServerPrefix + twinAddr: "@"})
	renamedShape := pthEdgeShapes(renamed, map[string]string{NodeProxyPrefix + "m1": "@"})
	if fmt.Sprint(twinShape) != fmt.Sprint(renamedShape) {
		t.Errorf("renamed run recorded %v, want the no-rename twin's shape %v", renamedShape, twinShape)
	}

	if len(renamed.Edges) != 1 {
		t.Fatalf("edges = %+v, want exactly one arrival leg — the bracket's own dial", renamed.Edges)
	}
	e := renamed.Edges[0]
	if e.From != NodeLocalID || e.To != "proxy:m1" || e.Kind != LegDial {
		t.Errorf("edge = %s -> %s (%s), want local -> proxy:m1 (dial)", e.From, e.To, e.Kind)
	}
	if !e.Observed || !e.Success {
		t.Errorf("edge Observed/Success = %v/%v, want true/true", e.Observed, e.Success)
	}
	if e.Target != renamedAddr {
		t.Errorf("edge Target = %q, want the dial string handed to the leg", e.Target)
	}
	if renamed.Partial != twin.Partial {
		t.Errorf("Partial = %v, want %v like the twin — the leg was observed, so nothing is missing (edges %+v)",
			renamed.Partial, twin.Partial, renamed.Edges)
	}
	if renamed.Partial {
		t.Errorf("Partial = true, want false: the renamed destination is the same vertex the leg was drawn to")
	}
}

// TestRenamedSuppressedDestinationKeepsItsTunnelLeg is the same rename inside
// the nested-upstream suppression shape: the plugin reached this server through
// a route the engine already recorded (lastNode is the bracket's To), so what
// the call adds is the hop's own handshake toward Through — and Through is the
// vertex fn renamed. Suppression promotes Through into the leg's destination
// slot, so the stale spelling lands in the guard that drops the whole leg: the
// handshake vanished and Partial claimed a leg that was observed.
func TestRenamedSuppressedDestinationKeepsItsTunnelLeg(t *testing.T) {
	run := func(t *testing.T, hopAddr, srvAddr, outboundID string) *PathGraph {
		t.Helper()
		rec := NewPathRecorder()
		rec.AddLocal()
		hop := rec.AddProxy("hop", hopAddr)
		srv := rec.AddServer(srvAddr)
		if hop != "proxy:hop" || srv != NodeServerPrefix+srvAddr {
			t.Fatalf("AddProxy/AddServer = %q / %q, want proxy:hop / %q", hop, srv, NodeServerPrefix+srvAddr)
		}
		if _, err := rec.Observe(Leg{Kind: LegDial, To: hop, Through: srv, Target: srvAddr}, nil,
			func() (net.Conn, error) {
				// The nested route already drew the arrival at hop, which is what
				// makes the enclosing bracket suppressible.
				if _, err := rec.Observe(Leg{Kind: LegDial, To: hop, Target: hopAddr}, nil, pthOK(pthTCPConn(hopAddr))); err != nil {
					t.Fatalf("Observe (nested arrival): %v", err)
				}
				if outboundID != "" {
					if id := rec.AddProxy(outboundID, srvAddr); id != NodeProxyPrefix+outboundID {
						t.Fatalf("AddProxy = %q, want %q", id, NodeProxyPrefix+outboundID)
					}
				}
				return pthTCPConn(srvAddr), nil
			}); err != nil {
			t.Fatalf("Observe: %v", err)
		}
		g := rec.Snapshot()
		if g == nil {
			t.Fatalf("Snapshot = nil, want a graph")
		}
		return g
	}

	twin := run(t, "203.0.113.93:1080", "203.0.113.94:1080", "")
	renamed := run(t, "203.0.113.93:1080", "203.0.113.96:1080", "m1")

	twinShape := pthEdgeShapes(twin, map[string]string{"server:203.0.113.94:1080": "@"})
	renamedShape := pthEdgeShapes(renamed, map[string]string{"proxy:m1": "@"})
	if fmt.Sprint(twinShape) != fmt.Sprint(renamedShape) {
		t.Errorf("renamed run recorded %v, want the no-rename twin's shape %v", renamedShape, twinShape)
	}

	if len(renamed.Edges) != 2 {
		t.Fatalf("edges = %+v, want the nested arrival plus the hop's own handshake", renamed.Edges)
	}
	handshake := renamed.Edges[1]
	if handshake.From != "proxy:hop" || handshake.To != "proxy:m1" {
		t.Errorf("handshake edge = %s -> %s, want proxy:hop -> proxy:m1", handshake.From, handshake.To)
	}
	if handshake.Kind != LegTunnel {
		t.Errorf("handshake Kind = %q, want %q — suppression rewrites the leg", handshake.Kind, LegTunnel)
	}
	if !handshake.Observed || !handshake.Success {
		t.Errorf("handshake Observed/Success = %v/%v, want true/true", handshake.Observed, handshake.Success)
	}
	if renamed.Partial {
		t.Errorf("Partial = true, want false — both legs were observed (edges %+v)", renamed.Edges)
	}
	if got := pthIDs(renamed); len(got) != 3 {
		t.Errorf("nodes = %v, want [local proxy:hop proxy:m1] — the renamed vertex is the same one", got)
	}
}

// TestRenamedThroughKeepsSecondLeg: one bracketed call that covered two
// physical legs — the arrival at To plus the handshake toward Through — loses
// its second leg when fn renamed Through. The byID guard on Through cannot
// distinguish "a leg toward a node the graph does not have" from "a leg toward
// a node the graph renamed", so it dropped a real leg and said Partial.
func TestRenamedThroughKeepsSecondLeg(t *testing.T) {
	run := func(t *testing.T, hopAddr, srvAddr, outboundID string) *PathGraph {
		t.Helper()
		rec := NewPathRecorder()
		rec.AddLocal()
		hop := rec.AddProxy("hop", hopAddr)
		srv := rec.AddServer(srvAddr)
		if hop != "proxy:hop" || srv != NodeServerPrefix+srvAddr {
			t.Fatalf("AddProxy/AddServer = %q / %q, want proxy:hop / %q", hop, srv, NodeServerPrefix+srvAddr)
		}
		if _, err := rec.Observe(Leg{Kind: LegTunnel, To: hop, Through: srv, Target: srvAddr}, nil,
			func() (net.Conn, error) {
				if outboundID != "" {
					if id := rec.AddProxy(outboundID, srvAddr); id != NodeProxyPrefix+outboundID {
						t.Fatalf("AddProxy = %q, want %q", id, NodeProxyPrefix+outboundID)
					}
				}
				return pthTCPConn(srvAddr), nil
			}); err != nil {
			t.Fatalf("Observe: %v", err)
		}
		g := rec.Snapshot()
		if g == nil {
			t.Fatalf("Snapshot = nil, want a graph")
		}
		return g
	}

	twin := run(t, "203.0.113.97:1080", "203.0.113.98:1080", "")
	renamed := run(t, "203.0.113.97:1080", "203.0.113.99:1080", "m1")

	twinShape := pthEdgeShapes(twin, map[string]string{"server:203.0.113.98:1080": "@"})
	renamedShape := pthEdgeShapes(renamed, map[string]string{"proxy:m1": "@"})
	if fmt.Sprint(twinShape) != fmt.Sprint(renamedShape) {
		t.Errorf("renamed run recorded %v, want the no-rename twin's shape %v", renamedShape, twinShape)
	}

	if len(renamed.Edges) != 2 {
		t.Fatalf("edges = %+v, want the timed arrival plus the untimed tunnel leg", renamed.Edges)
	}
	arrival, tunnel := renamed.Edges[0], renamed.Edges[1]
	if arrival.From != NodeLocalID || arrival.To != "proxy:hop" || arrival.Kind != LegTunnel {
		t.Errorf("arrival edge = %s -> %s (%s), want local -> proxy:hop (tunnel)", arrival.From, arrival.To, arrival.Kind)
	}
	if tunnel.From != "proxy:hop" || tunnel.To != "proxy:m1" || tunnel.Kind != LegTunnel {
		t.Errorf("second leg = %s -> %s (%s), want proxy:hop -> proxy:m1 (tunnel)", tunnel.From, tunnel.To, tunnel.Kind)
	}
	if tunnel.Duration != 0 {
		t.Errorf("second leg Duration = %v, want 0 — one bracketed call covered both physical legs", tunnel.Duration)
	}
	if !tunnel.Observed || !tunnel.Success {
		t.Errorf("second leg Observed/Success = %v/%v, want true/true", tunnel.Observed, tunnel.Success)
	}
	if renamed.Partial {
		t.Errorf("Partial = true, want false — the second leg was drawn, not lost (edges %+v)", renamed.Edges)
	}
}

// TestServerNodeForArrivalResolvesStaleExclude pins `exclude` on the resolver
// side of the same rename: ServerNodeForArrival's caller captured the vertex it
// arrived FROM before the call that renamed it, and the in-flight entries the
// resolver walks carry the CURRENT spelling. Comparing the two raw loses the
// exclusion, so the arrival's own source wins the resolution — and
// noteUpstreamArrival drops a leg whose From equals its To, or draws a
// self-loop: either way the honest edge disappears.
func TestServerNodeForArrivalResolvesStaleExclude(t *testing.T) {
	const addr = "203.0.113.100:1080"
	rec := NewPathRecorder()
	rec.AddLocal()
	srv := rec.AddServer(addr)
	if srv != NodeServerPrefix+addr {
		t.Fatalf("AddServer = %q, want %q", srv, NodeServerPrefix+addr)
	}

	var y, z, byStale, byCurrent string
	if _, err := rec.Observe(Leg{Kind: LegDial, To: srv, Target: addr}, nil, func() (net.Conn, error) {
		// The bare vertex is upgraded in place: srv and y are one node under two
		// spellings.
		y = rec.AddProxy("Y", addr)
		// A second identity at the same address keeps its own vertex and, as the
		// most recent registrant, owns the address index.
		z = rec.AddProxy("Z", addr)
		byStale = rec.ServerNodeForArrival(addr, srv)
		byCurrent = rec.ServerNodeForArrival(addr, y)
		return pthTCPConn(addr), nil
	}); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if y != "proxy:Y" || z != "proxy:Z" {
		t.Fatalf("AddProxy over the shared address = %q / %q, want proxy:Y / proxy:Z", y, z)
	}

	if byStale == y {
		t.Errorf("ServerNodeForArrival(%q, %q) = %q — the arrival's own source won under its pre-rename spelling, so the leg it should name is dropped as a self-arrival", addr, srv, byStale)
	}
	if byStale != z || byCurrent != z {
		t.Errorf("ServerNodeForArrival by stale/current spelling = %q / %q, want %q both ways — the exclusion must not depend on when the caller captured the ID", byStale, byCurrent, z)
	}

	// The bracket's own leg is unaffected: this is about the resolver, not about
	// the rename dropping legs (the tests above pin that).
	g := rec.Snapshot()
	if len(g.Edges) != 1 || g.Edges[0].From != NodeLocalID || g.Edges[0].To != y {
		t.Fatalf("edges = %+v, want one local -> %s dial", g.Edges, y)
	}
}

// TestResurrectedServerVertexKeepsItsOwnLegs pins the one invariant r.renamed
// must hold: a key names an ID the graph NO LONGER has. renameLocked deletes
// byID[oldID], so the old spelling is dead — but nothing keeps it dead. A later
// AddServer/AddProxy with that exact spelling registers a NEW live vertex under
// it (the rename removed the byID entry, and adoptSocketLocked can remove the
// byAddr entry too, so both lookups miss and addLocked mints a fresh node).
// Once the ID is live again, resolveLocked hijacks every leg that names the new
// vertex onto the node the old one was renamed to: a real, bracketed, successful
// dial is swallowed by the nested-suppression test, drawn twice, or dropped as a
// self-loop — and the new vertex is left orphaned with the wrong address.
//
// The in-tree shape is a hostname ride: the bare server vertex a hostname-spelled
// address got is adopted by an outbound identity (rename), a successful dial then
// adopts the RESOLVED peer and drops the hostname key from the address index, and
// any later naming of that same hostname spelling (dialLayers' successor,
// dialUpstream's direct branch, ServerNodeForArrival) resurrects the dead ID.
func TestResurrectedServerVertexKeepsItsOwnLegs(t *testing.T) {
	const hostAddr = "proxy.example.com:1080"
	const resolved = "203.0.113.7:1080"

	// mapInvariant asserts the resurrected ID is not a `renamed` key: keys are
	// dead IDs only, so a live one must never be mapped away.
	mapInvariant := func(t *testing.T, rec *PathRecorder, live string) {
		t.Helper()
		if cur, ok := rec.renamed[live]; ok {
			t.Errorf("renamed[%q] = %q — a `renamed` key must only ever name an ID the graph no longer has, and %q is a live vertex, so every later leg naming it is hijacked onto %q",
				live, cur, live, cur)
		}
	}

	// setup drives the recorder to the resurrected state and returns it with the
	// resurrected vertex's ID and the identity that renamed the original away.
	setup := func(t *testing.T) (*PathRecorder, string, string) {
		t.Helper()
		rec := NewPathRecorder()
		rec.AddLocal()
		srv := rec.AddServer(hostAddr)
		if srv != NodeServerPrefix+hostAddr {
			t.Fatalf("AddServer = %q, want %q", srv, NodeServerPrefix+hostAddr)
		}
		// The config names the outbound that owns this address: the bare vertex is
		// adopted in place, so srv's spelling is now dead.
		m := rec.AddProxy("m", hostAddr)
		if m != "proxy:m" {
			t.Fatalf("AddProxy = %q, want %q", m, "proxy:m")
		}
		// A successful dial adopts the RESOLVED peer, which drops the hostname key
		// from the address index — the second lookup that would have unified a
		// later naming of the same hostname with this vertex.
		if _, err := rec.Observe(Leg{Kind: LegDial, To: m, Target: hostAddr}, nil, pthOK(pthTCPConn(resolved))); err != nil {
			t.Fatalf("Observe: %v", err)
		}
		if owner, ok := rec.byAddr[hostAddr]; ok {
			t.Fatalf("byAddr[%q] = %q after the socket adoption, want the hostname key dropped", hostAddr, owner)
		}
		srv2 := rec.AddServer(hostAddr)
		if srv2 != srv {
			t.Fatalf("AddServer(%q) after the rename = %q, want the same spelling %q", hostAddr, srv2, srv)
		}
		if i, live := rec.byID[srv2]; !live {
			t.Fatalf("byID has no %q — the resurrection registered no live vertex", srv2)
		} else if rec.nodes[i].Address != hostAddr || rec.nodes[i].AddressSource != AddrConfigured {
			t.Fatalf("resurrected node = %+v, want a fresh vertex configured at %q", rec.nodes[i], hostAddr)
		}
		return rec, srv2, m
	}

	// (a) The suppression hijack: lastNode is the identity the dead spelling was
	// renamed to, so a leg naming the LIVE vertex resolves onto it, matches the
	// nested-upstream suppression test and vanishes — a successful engine-stack
	// dial that really happened disappears from the graph, with Partial false.
	t.Run("suppression", func(t *testing.T) {
		rec, srv2, m := setup(t)
		mapInvariant(t, rec, srv2)
		if got := rec.LastNodeID(); got != m {
			t.Fatalf("LastNodeID = %q, want %q", got, m)
		}

		if _, err := rec.Observe(Leg{Kind: LegStack, To: srv2, Target: hostAddr}, nil, pthOK(pthTCPConn(resolved))); err != nil {
			t.Fatalf("Observe: %v", err)
		}

		g := rec.Snapshot()
		if len(g.Edges) != 2 {
			t.Fatalf("edges = %+v, want the proxy dial plus the resurrected vertex's own stack leg", g.Edges)
		}
		if e := g.Edges[1]; e.From != NodeLocalID || e.To != srv2 || e.Kind != LegStack || !e.Success {
			t.Errorf("second edge = %s -> %s (%s, success=%v), want %s -> %s (stack, success=true) — the leg naming the live vertex was hijacked onto %q",
				e.From, e.To, e.Kind, e.Success, NodeLocalID, srv2, m)
		}
		if g.Partial {
			t.Errorf("Partial = true, want false — the leg was observed and drawn (edges %+v)", g.Edges)
		}
		if g.Dropped != 0 {
			t.Errorf("Dropped = %d, want 0", g.Dropped)
		}
		if n := pthNode(g, srv2); n == nil {
			t.Errorf("node %q missing from %v — the resurrected vertex is orphaned", srv2, pthIDs(g))
		}
		// The hijack also clobbers: the wrong leg's peer would be adopted onto the
		// node the mapping points at, overwriting the address that node's OWN dial
		// socket-verified.
		if n := pthNode(g, m); n == nil {
			t.Fatalf("node %q missing from %v", m, pthIDs(g))
		} else if n.Address != resolved || n.AddressSource != AddrSocket {
			t.Errorf("node %q Address = %q (%q), want the socket peer %q its own dial verified", m, n.Address, n.AddressSource, resolved)
		}
	})

	// (b) The misattribution variant: with the arrival pointer moved off the
	// mapping's target, the suppression test does not fire and the leg is drawn —
	// onto the WRONG vertex. The graph then claims a second local dial of a
	// server the engine reached once, and stamps that leg's peer onto it.
	t.Run("misattribution", func(t *testing.T) {
		rec, srv2, m := setup(t)
		mapInvariant(t, rec, srv2)
		rec.NoteLocalDial()

		const otherPeer = "198.51.100.9:1080"
		if _, err := rec.Observe(Leg{Kind: LegStack, To: srv2, Target: hostAddr}, nil, pthOK(pthTCPConn(otherPeer))); err != nil {
			t.Fatalf("Observe: %v", err)
		}

		g := rec.Snapshot()
		if len(g.Edges) != 2 {
			t.Fatalf("edges = %+v, want the proxy dial plus the resurrected vertex's own stack leg", g.Edges)
		}
		if e := g.Edges[1]; e.To != srv2 {
			t.Errorf("second edge = %s -> %s, want %s -> %s — the leg belongs to the live vertex, not to the identity the dead spelling was renamed to",
				e.From, e.To, NodeLocalID, srv2)
		}
		if dup := pthEdgesTo(g, m); len(dup) != 1 {
			t.Errorf("edges arriving at %q = %+v, want exactly one — its own dial", m, dup)
		}
		if n := pthNode(g, m); n == nil {
			t.Fatalf("node %q missing from %v", m, pthIDs(g))
		} else if n.Address != resolved || n.AddressSource != AddrSocket {
			t.Errorf("node %q Address = %q (%q), want the socket peer %q its own dial verified — another leg's peer clobbered it",
				m, n.Address, n.AddressSource, resolved)
		}
		if n := pthNode(g, srv2); n == nil {
			t.Fatalf("node %q missing from %v", srv2, pthIDs(g))
		} else if n.Address != otherPeer || n.AddressSource != AddrSocket {
			t.Errorf("node %q Address = %q (%q), want the socket peer %q this leg really reached", srv2, n.Address, n.AddressSource, otherPeer)
		}
		if g.Partial {
			t.Errorf("Partial = true, want false — both legs were observed and drawn (edges %+v)", g.Edges)
		}
	})

	// (c) The structural half of the fix: a LIVE ID never resolves away, however
	// the map came to key it. addLocked dropping the key is what keeps the map
	// honest in tree; this pins resolveLocked itself, so a registration path that
	// ever forgets to drop it cannot re-open the hijack. The map is populated
	// directly because no honest sequence can leave a live ID in it once addLocked
	// clears it.
	t.Run("live ID never resolves away", func(t *testing.T) {
		const addr = "203.0.113.101:1080"
		rec := NewPathRecorder()
		rec.AddLocal()
		live := rec.AddProxy("live", addr)
		if live != "proxy:live" {
			t.Fatalf("AddProxy = %q, want %q", live, "proxy:live")
		}
		// A stale mapping a producer could still be holding: live's own entry
		// (which the graph must ignore, live being a vertex it has) plus one for
		// a vertex that is genuinely gone.
		rec.renamed = map[string]string{live: "proxy:gone", "server:dead": "proxy:gone"}

		if got := rec.resolveLocked(live); got != live {
			t.Errorf("resolveLocked(%q) = %q, want %q — a live ID must never resolve away", live, got, live)
		}
		if got := rec.resolveLocked(""); got != "" {
			t.Errorf("resolveLocked(\"\") = %q, want \"\"", got)
		}
		if got := rec.resolveLocked("server:dead"); got != "proxy:gone" {
			t.Errorf("resolveLocked of a dead ID = %q, want %q — the mapping still resolves what the graph no longer has", got, "proxy:gone")
		}

		if _, err := rec.Observe(Leg{Kind: LegDial, To: live, Target: addr}, nil, pthOK(pthTCPConn(addr))); err != nil {
			t.Fatalf("Observe: %v", err)
		}
		g := rec.Snapshot()
		if len(g.Edges) != 1 {
			t.Fatalf("edges = %+v, want the one arrival leg at the live vertex", g.Edges)
		}
		if e := g.Edges[0]; e.From != NodeLocalID || e.To != live || !e.Success {
			t.Errorf("edge = %s -> %s (success=%v), want %s -> %s (success=true)", e.From, e.To, e.Success, NodeLocalID, live)
		}
		if g.Partial {
			t.Errorf("Partial = true, want false — the leg named a vertex the graph has (edges %+v)", g.Edges)
		}
	})
}

// pthEdgesTo returns every edge arriving at id.
func pthEdgesTo(g *PathGraph, id string) []PathEdge {
	var out []PathEdge
	for _, e := range g.Edges {
		if e.To == id {
			out = append(out, e)
		}
	}
	return out
}
