package metrics

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
)

// This file covers the persistence half of the observed-path feature. A row
// carries an immutable *core.PathGraph snapshot, never the recorder, so the
// store's writer goroutine can json.Marshal it hundreds of milliseconds later
// and on another goroutine.

// --- fixtures -------------------------------------------------------------

// racePAddr is a fixed net.Addr for the inert conns an observed leg returns.
type racePAddr string

func (a racePAddr) Network() string { return "tcp" }
func (a racePAddr) String() string  { return string(a) }

// racePConn is the net.Conn an observed leg "produces". Nothing ever reads or
// writes it — the recorder only asks for RemoteAddr, so the leg can adopt the
// socket peer it really connected to. An empty remote address stands for a
// conn whose peer is not a dialable host:port (an in-memory tunnel conn),
// which the recorder must NOT adopt.
type racePConn struct{ remote net.Addr }

func (c racePConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c racePConn) Write(b []byte) (int, error)      { return len(b), nil }
func (c racePConn) Close() error                     { return nil }
func (c racePConn) LocalAddr() net.Addr              { return racePAddr("127.0.0.1:4444") }
func (c racePConn) RemoteAddr() net.Addr             { return c.remote }
func (c racePConn) SetDeadline(time.Time) error      { return nil }
func (c racePConn) SetReadDeadline(time.Time) error  { return nil }
func (c racePConn) SetWriteDeadline(time.Time) error { return nil }

var racePErrRefused = errors.New("dial tcp 203.0.113.7:1080: connect: connection refused")

const (
	racePTarget     = "example.com:443"
	racePTargetHost = "example.com"
	racePTargetPort = "443"

	racePFirstAddr  = "203.0.113.7:1080" // failover attempt 1: refused
	racePSecondAddr = "203.0.113.8:1080" // failover attempt 2: arrives

	// A tunnel plugin's address shape. net.SplitHostPort alone "accepts" it as
	// host "geph", port "//tunnel" — the trap the recorder guards.
	racePTunnelAddr = "geph://tunnel"

	racePProxyID  = "ob1"
	racePTunnelID = "tunnel"
	racePPhase    = "connect"
	racePReason   = "connect_refused"

	// Addresses handed to a sealed recorder: none of them may reach the graph
	// Snapshot already produced.
	racePLateProxyAddr  = "198.51.100.9:1080"
	racePLateTunnelAddr = "198.51.100.10:1080"
	racePLateServerAddr = "203.0.113.99:1080"
)

// Node IDs in the order Snapshot publishes them: local first, proxies in dial
// order, target LAST (the target node is created up front, before any dial, so
// Snapshot has to move it).
const (
	racePIDLocal   = core.NodeLocalID
	racePIDFirst   = core.NodeProxyPrefix + racePProxyID
	racePIDSecond  = core.NodeProxyPrefix + racePProxyID + "#2"
	racePIDTunnel  = core.NodeProxyPrefix + racePTunnelID
	racePIDTarget  = core.NodeTargetID
	racePIDNewNode = core.NodeServerPrefix + racePLateServerAddr
)

func racePNodeIDs() []string {
	return []string{racePIDLocal, racePIDFirst, racePIDSecond, racePIDTunnel, racePIDTarget}
}

// racePBuildGraph drives a REAL recorder through a pool failover into a tunnel
// hop and returns it with its sealed snapshot — exactly how router.recordRow
// produces a row's graph. The shape:
//
//   - attempt 1 refused at the server's TCP port,
//   - attempt 2 arrives at its own server AND handshakes toward the tunnel hop
//     inside the same bracketed call, so that one call yields TWO edges: the
//     timed arrival, then a tunnel edge with Duration 0,
//   - the tunnel hop then carries the request to the target.
//
// The recorder is returned too, so a test can keep poking it after the
// snapshot was handed to Record: that is what proves the persisted graph is an
// immutable deep copy rather than a view onto recorder state.
func racePBuildGraph() (*core.PathRecorder, *core.PathGraph) {
	rec := core.NewPathRecorder()
	rec.SetClassifier(func(core.LegInfo, error) (string, string) {
		return racePPhase, racePReason
	})
	rec.AddLocal()
	target := rec.AddTarget(racePTargetHost, racePTargetPort)

	// Attempt 1: refused. Its node keeps the configured address, because no
	// socket was ever reached.
	first := rec.AddProxyAttempt(racePProxyID, racePFirstAddr, 1)
	_, _ = rec.Observe(core.Leg{
		Kind: core.LegDial, To: first, Through: target, Target: racePTarget,
	}, nil, func() (net.Conn, error) {
		return nil, racePErrRefused
	})

	// Attempt 2's node is registered before the next hop's, so the dial order
	// in the graph is attempt 1, attempt 2, tunnel.
	second := rec.AddProxyAttempt(racePProxyID, racePSecondAddr, 2)

	// A tunnel plugin still gets a node — the request really rode it — with an
	// empty Address because its shape is not a dialable host:port.
	tunnel := rec.AddProxy(racePTunnelID, racePTunnelAddr)
	_, _ = rec.Observe(core.Leg{
		Kind: core.LegDial, To: second, Through: tunnel, Target: racePTarget,
	}, nil, func() (net.Conn, error) {
		return racePConn{remote: racePAddr(racePSecondAddr)}, nil
	})

	// The tunnel hop's own stream toward the target. Its conn has no dialable
	// peer, so nothing is adopted.
	_, _ = rec.Observe(core.Leg{
		Kind: core.LegTunnel, From: tunnel, To: target, Target: racePTarget,
	}, nil, func() (net.Conn, error) {
		return racePConn{remote: racePAddr("")}, nil
	})

	return rec, rec.Snapshot()
}

// racePPokeSealed mutates a sealed recorder while the store's writer goroutine
// may be marshaling the graph Snapshot already handed out. Every poke must be
// inert with respect to that graph AND with respect to the recorder's own
// state: the seal (core's addProxyLocked/addLocked early returns) stops
// mutators from adopting addresses, renaming vertices or moving lastNode in
// place, not just from growing the published graph. The recorder's node slots
// are package-private to core — its path_test asserts them directly — so from
// here the guarantee is pinned through the public surface: every mutator
// answers only an ID the sealed graph already holds (or ""), and the readers
// report the pre-seal state.
//
// The interesting poke is AddProxy on the tunnel node: its Address is empty, so
// an unsealed recorder would fill the node slot IN PLACE. If the snapshot
// aliased the recorder's nodes slice instead of deep-copying it, that write
// would also race with json.Marshal and the detector would fire.
func racePPokeSealed(t *testing.T, rec *core.PathRecorder, sealed *core.PathGraph) {
	t.Helper()
	rec.AddLocal()
	// AddTarget answers from its index before the seal is consulted, so it
	// still names the existing node — what the seal guarantees is that no
	// second destination is recorded and the node is not rewritten.
	assert.Equal(t, racePIDTarget, rec.AddTarget("other.example.com", "8080"),
		"a sealed AddTarget must answer the existing node and record no second destination")
	// A sealed recorder still answers an ID it holds — sealing stops new state,
	// it does not make the recorder lie about the graph it has — but adopts the
	// late address onto neither node: not the one that already has an address,
	// and not the address-less tunnel node an unsealed recorder would fill in
	// place.
	assert.Equal(t, racePIDFirst, rec.AddProxy(racePProxyID, racePLateProxyAddr),
		"a sealed AddProxy must answer the existing node and adopt nothing behind it")
	assert.Equal(t, racePIDTunnel, rec.AddProxy(racePTunnelID, racePLateTunnelAddr),
		"the address-less tunnel node is answered, not filled in place")
	assert.Empty(t, rec.AddServer(racePLateServerAddr),
		"no new server vertex may be minted after the seal")
	assert.Empty(t, rec.AddProxyAttempt(racePProxyID, racePLateProxyAddr, 3),
		"no new attempt node may be minted after the seal")
	rec.NotePartial()
	conn, err := rec.Observe(core.Leg{
		Kind: core.LegTunnel, From: racePIDSecond, To: racePIDTarget, Target: racePTarget,
	}, nil, func() (net.Conn, error) {
		return racePConn{remote: racePAddr(racePLateProxyAddr)}, nil
	})
	assert.NoError(t, err)
	assert.NotNil(t, conn, "a sealed Observe still runs fn and hands back its conn")

	// Snapshot is memoized: a later caller gets the very graph already
	// persisted, so no poke above can produce a second, different one.
	assert.Same(t, sealed, rec.Snapshot(), "Snapshot must memoize")
	assert.False(t, sealed.Partial, "NotePartial after Snapshot must not leak into the sealed graph")
	assert.Zero(t, sealed.Dropped)
	assert.Equal(t, racePNodeIDs(), racePIDs(sealed.Nodes), "poking a sealed recorder must not add or rename nodes")
	assert.Len(t, sealed.Edges, 4, "poking a sealed recorder must not add edges")
	for _, n := range sealed.Nodes {
		switch n.ID {
		case racePIDTunnel:
			assert.Empty(t, n.Address, "a late configured address must not reach the sealed tunnel node")
		case racePIDFirst:
			assert.Equal(t, racePFirstAddr, n.Address)
		}
	}
	assert.NotContains(t, racePIDs(sealed.Nodes), racePIDNewNode)

	// The recorder's own read state survived the pokes: lastNode still names
	// where the last REAL leg arrived (the sealed Observe did not move it), and
	// the destination is the one the graph was built with — the sealed AddTarget
	// did not re-point it at the poke's host.
	assert.Equal(t, racePIDTarget, rec.LastNodeID(),
		"a sealed poke must not move the recorder's own last-node state")
	assert.Equal(t, racePIDTarget, rec.TargetNodeID())
	assert.Equal(t, racePIDTarget, rec.TargetNodeIDFor(racePTargetHost, racePTargetPort))
	assert.Empty(t, rec.TargetNodeIDFor("other.example.com", "8080"),
		"the sealed AddTarget must not re-point the destination")
}

// racePIDs projects a node list to its IDs, in order.
func racePIDs(nodes []core.PathNode) []string {
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.ID)
	}
	return out
}

// racePAssertGraph asserts the failover graph is intact. The same assertion
// serves a freshly snapshotted graph and one that has been through the store's
// json.Marshal → bbolt → json.Unmarshal.
func racePAssertGraph(t *testing.T, g *core.PathGraph) {
	t.Helper()
	require.NotNil(t, g, "the row must carry a graph")

	require.Len(t, g.Nodes, 5)
	assert.Equal(t, racePNodeIDs(), racePIDs(g.Nodes), "local first, proxies in dial order, target last")

	local, first, second, tunnel, target := g.Nodes[0], g.Nodes[1], g.Nodes[2], g.Nodes[3], g.Nodes[4]

	assert.Equal(t, core.NodeLocal, local.Kind)
	assert.Empty(t, local.Address, "the engine host has no address")
	assert.Empty(t, local.OutboundID)

	// A pool failover records one node PER ATTEMPT, so the member that lost
	// keeps its own address instead of being overwritten by the one that won.
	assert.Equal(t, core.NodeProxy, first.Kind)
	assert.Equal(t, racePProxyID, first.OutboundID)
	assert.Equal(t, 1, first.Attempt)
	assert.Equal(t, racePFirstAddr, first.Address)
	assert.Equal(t, core.AddrConfigured, first.AddressSource,
		"attempt 1 never connected, so its address stays the configured one")

	assert.Equal(t, core.NodeProxy, second.Kind)
	assert.Equal(t, racePProxyID, second.OutboundID)
	assert.Equal(t, 2, second.Attempt)
	assert.Equal(t, racePSecondAddr, second.Address)
	assert.Equal(t, core.AddrSocket, second.AddressSource,
		"the winner adopted the socket peer it really connected to")

	// A tunnel plugin's node exists, but its non-dialable address shape is
	// dropped rather than parsed into a fragment.
	assert.Equal(t, core.NodeProxy, tunnel.Kind)
	assert.Equal(t, racePTunnelID, tunnel.OutboundID)
	assert.Empty(t, tunnel.Address, "%q must not be adopted as a host:port", racePTunnelAddr)
	assert.Empty(t, tunnel.AddressSource)
	assert.Zero(t, tunnel.Attempt)

	assert.Equal(t, core.NodeTarget, target.Kind)
	assert.Empty(t, target.OutboundID)
	assert.Equal(t, racePTargetHost, target.Host)
	assert.Equal(t, racePTargetPort, target.Port)
	assert.Equal(t, racePTarget, target.Address)
	assert.Equal(t, core.AddrConfigured, target.AddressSource)

	// Four edges: the refused attempt, the winning arrival, the winning hop's
	// untimed handshake toward the tunnel node (same bracketed call), and the
	// tunnel hop's stream to the target.
	require.Len(t, g.Edges, 4)

	lost := g.Edges[0]
	assert.Equal(t, racePIDLocal, lost.From, "From is always written out, never empty")
	assert.Equal(t, racePIDFirst, lost.To)
	assert.Equal(t, core.LegDial, lost.Kind)
	assert.Equal(t, racePTarget, lost.Target)
	assert.True(t, lost.Observed)
	assert.False(t, lost.Success)
	assert.False(t, lost.Collapsed)
	assert.Equal(t, racePErrRefused.Error(), lost.Error)
	assert.Equal(t, racePPhase, lost.Phase)
	assert.Equal(t, racePReason, lost.Reason)

	won := g.Edges[1]
	assert.Equal(t, racePIDLocal, won.From)
	assert.Equal(t, racePIDSecond, won.To)
	assert.Equal(t, core.LegDial, won.Kind)
	assert.True(t, won.Observed)
	assert.True(t, won.Success)
	assert.Empty(t, won.Error)
	assert.Empty(t, won.Phase, "a successful leg carries no classification")
	assert.Empty(t, won.Reason)

	handshake := g.Edges[2]
	assert.Equal(t, racePIDSecond, handshake.From)
	assert.Equal(t, racePIDTunnel, handshake.To)
	assert.Equal(t, core.LegTunnel, handshake.Kind)
	assert.Equal(t, racePTarget, handshake.Target)
	assert.True(t, handshake.Observed)
	assert.True(t, handshake.Success)
	assert.Zero(t, handshake.Duration,
		"the second leg of one bracketed call is real but not separately timed: 0 means untimed, never instant")

	toTarget := g.Edges[3]
	assert.Equal(t, racePIDTunnel, toTarget.From)
	assert.Equal(t, racePIDTarget, toTarget.To)
	assert.Equal(t, core.LegTunnel, toTarget.Kind)
	assert.True(t, toTarget.Observed)
	assert.True(t, toTarget.Success)

	assert.False(t, g.Partial)
	assert.Zero(t, g.Dropped)
}

// racePRawRows reads the persisted request rows straight out of bbolt as raw
// JSON keyed by metric ID, so a test can assert on key PRESENCE (omitempty)
// rather than on decoded zero values.
func racePRawRows(t *testing.T, s *Store) map[string]json.RawMessage {
	t.Helper()
	out := map[string]json.RawMessage{}
	require.NoError(t, s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketRequests).ForEach(func(k, v []byte) error {
			// Keys are "<unixnano>-<metric id>"; the nano prefix never holds a
			// dash, so the first one splits it off.
			id := string(k)
			if i := strings.Index(id, "-"); i >= 0 {
				id = id[i+1:]
			}
			out[id] = append(json.RawMessage(nil), v...)
			return nil
		})
	}))
	return out
}

// racePKeys unmarshals a JSON object's top-level keys.
func racePKeys(t *testing.T, raw []byte) map[string]json.RawMessage {
	t.Helper()
	var m map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &m), "not a JSON object: %s", raw)
	return m
}

// --- tests ----------------------------------------------------------------

// TestRecordWithPathIsRaceFreeUnderFlush hammers Record with rows carrying a
// populated graph while the store's own flush paths marshal them on the writer
// goroutine — and while the recording goroutines keep poking the recorders
// those graphs came from. All three flush paths are provably exercised:
//
//  1. the 500ms ticker: a first batch of fewer than 64 rows is recorded and
//     then awaited WITHOUT closing the store — the batch threshold cannot have
//     fired and Close has not run, so the rows can only have reached bbolt via
//     a tick;
//  2. the 64-row batch: a full writer burst enqueues far more than one batch
//     while the loop drains concurrently, so the batch flush runs repeatedly;
//  3. the final flush on Close: after the writers finish, every row whose
//     Record succeeded must be persisted once Close returns — the loop drains
//     the channel and flushes before it exits, so the count is exact.
//
// The graphs are built with a real PathRecorder and Snapshot(), not by hand:
// the property under test is that Record enqueues an immutable deep copy, so
// json.Marshal on the writer goroutine never touches recorder state. A row
// that carried the recorder itself — or a snapshot aliasing its slices, or a
// recorder Snapshot failed to seal — would be a data race here, and the burst
// phase is where the marshal and the pokes genuinely overlap.
func TestRecordWithPathIsRaceFreeUnderFlush(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "path-race.db")
	s, err := Open(dbPath)
	require.NoError(t, err)

	const writers = 16
	const rowsPerWriter = 200
	// Deliberately below the store's 64-row batch threshold: phase 1's rows
	// can only reach bbolt through the ticker while the store is open.
	const tickerRows = 8

	base := time.Now().Add(-time.Hour)

	// Phase 1 — the ticker. Record a handful of rows, then wait for them to
	// surface WITHOUT closing the store. No batch flush can have fired (fewer
	// than 64 rows enqueued) and Close has not run, so persistence proves the
	// tick path marshals graph-carrying rows on the writer goroutine.
	for i := 0; i < tickerRows; i++ {
		_, graph := racePBuildGraph()
		started := base.Add(time.Duration(i) * time.Millisecond)
		require.NoError(t, s.Record(core.RequestMetric{
			ID:         fmt.Sprintf("tick%d", i),
			InboundID:  "in",
			Network:    core.NetworkTCP,
			Target:     racePTarget,
			Chain:      []string{racePProxyID},
			Success:    true,
			Latency:    time.Millisecond,
			StartedAt:  started,
			FinishedAt: started.Add(time.Millisecond),
			Path:       graph,
		}))
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		rows, rerr := s.Recent(tickerRows + 1)
		require.NoError(t, rerr)
		if len(rows) >= tickerRows {
			break
		}
		require.True(t, time.Now().Before(deadline),
			"the 500ms ticker never flushed the %d sub-batch rows", tickerRows)
		time.Sleep(25 * time.Millisecond)
	}

	// Phase 2 — the batch flush, under the race detector. Writers Record and
	// then keep poking the sealed recorder while the store's loop drains the
	// channel in 64-row batches and marshals each graph on its own goroutine.
	// Record is non-blocking: a full write buffer drops the row with an error,
	// so count the successes — Close must persist exactly those.
	var succeeded int64
	var wg sync.WaitGroup
	wg.Add(writers)
	for w := 0; w < writers; w++ {
		go func(w int) {
			defer wg.Done()
			for i := 0; i < rowsPerWriter; i++ {
				rec, graph := racePBuildGraph()
				started := base.Add(time.Duration(i) * time.Millisecond)
				err := s.Record(core.RequestMetric{
					ID:         fmt.Sprintf("w%dr%d", w, i),
					InboundID:  "in",
					Network:    core.NetworkTCP,
					Target:     racePTarget,
					Chain:      []string{racePProxyID},
					Success:    true,
					Latency:    time.Millisecond,
					StartedAt:  started,
					FinishedAt: started.Add(time.Millisecond),
					Path:       graph,
				})
				if err == nil {
					atomic.AddInt64(&succeeded, 1)
				}
				racePPokeSealed(t, rec, graph)
			}
		}(w)
	}
	wg.Wait()
	enqueued := atomic.LoadInt64(&succeeded)
	require.GreaterOrEqual(t, enqueued, int64(64),
		"the burst must enqueue at least one full batch for the batch flush to fire")

	// Phase 3 — the final flush on Close: every row that made it into the
	// channel is persisted before Close returns.
	require.NoError(t, s.Close())

	// Whatever the flushes persisted must be complete and byte-intact.
	s2, err := Open(dbPath)
	require.NoError(t, err)
	defer func() { _ = s2.Close() }()

	rows, err := s2.Recent(tickerRows + writers*rowsPerWriter)
	require.NoError(t, err)
	require.Len(t, rows, tickerRows+int(enqueued),
		"every enqueued row must survive its flush path: %d ticker rows + %d burst rows", tickerRows, enqueued)
	for _, row := range rows {
		require.NotNil(t, row.Path, "row %s lost its graph", row.ID)
		racePAssertGraph(t, row.Path)
	}
}

// TestRecentRoundTripsThePath asserts a graph survives the store's
// json.Marshal → bbolt → json.Unmarshal round trip intact, that a row recorded
// WITHOUT a graph comes back with Path == nil (the older-engine row case), and
// that the persisted JSON of a path-less row has no "path" key at all —
// omitempty dropping the field, not a null.
func TestRecentRoundTripsThePath(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "path-roundtrip.db")
	s, err := Open(dbPath)
	require.NoError(t, err)

	_, graph := racePBuildGraph()
	racePAssertGraph(t, graph)

	base := time.Now().Add(-time.Minute)
	withPath := core.RequestMetric{
		ID: "withpath", InboundID: "in", Network: core.NetworkTCP,
		Target: racePTarget, Chain: []string{racePProxyID}, Success: true,
		Latency:   40 * time.Millisecond,
		StartedAt: base, FinishedAt: base.Add(40 * time.Millisecond),
		Path: graph,
	}
	noPath := withPath
	noPath.ID = "nopath"
	noPath.Path = nil
	// Recorded later so Recent(1) hands back the path-less row.
	noPath.StartedAt = base.Add(time.Second)
	noPath.FinishedAt = base.Add(time.Second + 40*time.Millisecond)

	require.NoError(t, s.Record(withPath))
	require.NoError(t, s.Record(noPath))
	require.NoError(t, s.Close(), "Close flushes the pending batch")

	s2, err := Open(dbPath)
	require.NoError(t, err)
	defer func() { _ = s2.Close() }()

	// The newest row is the path-less one: an older engine's row decodes to a
	// nil Path rather than an empty graph.
	one, err := s2.Recent(1)
	require.NoError(t, err)
	require.Len(t, one, 1)
	require.Equal(t, "nopath", one[0].ID)
	assert.Nil(t, one[0].Path, "a row recorded without a graph must come back path-less")

	rows, err := s2.Recent(5)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, "withpath", rows[1].ID)
	racePAssertGraph(t, rows[1].Path)
	assert.Equal(t, graph, rows[1].Path,
		"every node and edge must survive the bbolt JSON round trip")

	// A path-less row re-serializes with no "path" key at all.
	raw, err := json.Marshal(rows[0])
	require.NoError(t, err)
	_, hasPath := racePKeys(t, raw)["path"]
	assert.False(t, hasPath, "a path-less row must omit the key entirely; got %s", raw)

	// And the bytes actually in bbolt agree.
	persisted := racePRawRows(t, s2)
	require.Contains(t, persisted, "nopath")
	require.Contains(t, persisted, "withpath")

	_, storedHasPath := racePKeys(t, persisted["nopath"])["path"]
	assert.False(t, storedHasPath, `the stored path-less row must omit "path"; got %s`, persisted["nopath"])

	storedRow := racePKeys(t, persisted["withpath"])
	require.Contains(t, storedRow, "path")
	storedGraph := racePKeys(t, storedRow["path"])
	assert.Contains(t, storedGraph, "nodes")
	assert.Contains(t, storedGraph, "edges")
	_, hasPartial := storedGraph["partial"]
	assert.False(t, hasPartial, "a fully observed graph omits partial")
	_, hasDropped := storedGraph["dropped"]
	assert.False(t, hasDropped, "a graph under every cap omits dropped")
}
