// persist_test.go — the durable-config contract: the snapshot excludes
// provider-regenerated state, the written file round-trips into a restart,
// and the persister writes on mutations (skipping no-op writes) with a
// flush on Stop.

package config

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// seedStore builds a store carrying one of everything: operator objects
// (inbound, outbounds, pools, chain) plus a provider whose poll populated a
// managed pool with provider-owned outbounds.
func seedStore(t *testing.T) *Store {
	t.Helper()
	s := NewStore()
	s.SetInbound(&Inbound{ID: "in-1", Protocol: "http", Listen: "127.0.0.1:8080"})
	s.SetOutbound(&OutboundSpec{ID: "zz-out", Protocol: "socks5", Config: map[string]any{"address": "127.0.0.1:1"}})
	s.SetOutbound(&OutboundSpec{ID: "aa-out", Protocol: "socks5", Config: map[string]any{"address": "127.0.0.2:1"}})
	// The "default" pool exists as an operator object first (with a selector
	// tuned by the operator); the provider's first poll then adopts it.
	s.SetPool(&Pool{ID: "default", Selector: SelectorSpec{Type: "leastlatency"}})
	s.SetPool(&Pool{ID: "op-pool", OutboundIDs: []string{"aa-out"}})
	s.SetChain(&ChainSpec{ID: "ch-1", Layers: []string{"op-pool"}})
	s.SetProvider(&ProviderSpec{ID: "prov-1", Provider: "psiphon", PoolIDs: []string{"default"}})
	s.SetProviderOutbounds("prov-1", []*OutboundSpec{
		{ID: "srv-b", Protocol: "psiphon"},
		{ID: "srv-a", Protocol: "psiphon"},
	})
	return s
}

// TestSnapshotExcludesProviderManagedState pins the core rule: operator
// objects persist; provider-owned outbounds do not; provider-managed pools
// persist without their regenerated membership but with operator tuning.
func TestSnapshotExcludesProviderManagedState(t *testing.T) {
	doc := seedStore(t).Snapshot()

	if len(doc.Inbounds) != 1 || doc.Inbounds[0].ID != "in-1" {
		t.Fatalf("inbounds = %+v, want just in-1", doc.Inbounds)
	}
	if len(doc.Chains) != 1 || doc.Chains[0].ID != "ch-1" {
		t.Fatalf("chains = %+v, want just ch-1", doc.Chains)
	}
	if len(doc.Providers) != 1 || doc.Providers[0].ID != "prov-1" {
		t.Fatalf("providers = %+v, want just prov-1", doc.Providers)
	}

	// Outbounds: the two operator ones, sorted; neither provider server.
	var gotIDs []string
	for _, o := range doc.Outbounds {
		gotIDs = append(gotIDs, o.ID)
	}
	if want := []string{"aa-out", "zz-out"}; !equalStrings(gotIDs, want) {
		t.Fatalf("outbound ids = %v, want %v (provider servers excluded, sorted)", gotIDs, want)
	}

	// Pools: sorted, default first. The managed "default" pool survives with
	// its operator selector but WITHOUT the provider-built membership; the
	// operator pool keeps its membership.
	if len(doc.Pools) != 2 || doc.Pools[0].ID != "default" || doc.Pools[1].ID != "op-pool" {
		t.Fatalf("pool ids = %v, want [default op-pool]", snapshotPoolIDs(doc.Pools))
	}
	if dp := doc.Pools[0]; len(dp.OutboundIDs) != 0 {
		t.Fatalf("managed pool default keeps membership %v, want it stripped", dp.OutboundIDs)
	} else if dp.Selector.Type != "leastlatency" {
		t.Fatalf("managed pool default lost its operator selector (%q)", dp.Selector.Type)
	}
	if op := doc.Pools[1]; !equalStrings(op.OutboundIDs, []string{"aa-out"}) {
		t.Fatalf("operator pool membership = %v, want [aa-out]", op.OutboundIDs)
	}
}

// TestSnapshotRoundTripsThroughFile writes a snapshot and reads it back as
// the same document — a restart must restore exactly what was persisted, and
// the atomic write must not leave temp files behind.
func TestSnapshotRoundTripsThroughFile(t *testing.T) {
	s := seedStore(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "goose.json")

	doc := s.Snapshot()
	if err := WriteDoc(path, doc); err != nil {
		t.Fatalf("WriteDoc: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != "goose.json" {
			t.Fatalf("write left %s behind (want only goose.json)", e.Name())
		}
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var back Doc
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("read back: %v", err)
	}
	want, _ := json.Marshal(doc)
	got, _ := json.Marshal(back)
	if !bytes.Equal(want, got) {
		t.Fatalf("round trip mismatch:\nwant %s\ngot  %s", want, got)
	}
}

// TestPersistSkipsIdenticalBytes drives persist() directly: the first call
// writes, an identical snapshot is skipped, and a mutation writes again.
func TestPersistSkipsIdenticalBytes(t *testing.T) {
	s := NewStore()
	path := filepath.Join(t.TempDir(), "goose.json")
	p := NewPersister(s, path)

	p.persist()
	if n := p.writeCount(); n != 1 {
		t.Fatalf("writes after startup persist = %d, want 1", n)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("startup persist did not create %s: %v", path, err)
	}

	p.persist() // byte-identical snapshot → no write
	if n := p.writeCount(); n != 1 {
		t.Fatalf("writes after identical persist = %d, want 1 (skip)", n)
	}

	s.SetOutbound(&OutboundSpec{ID: "op-x", Protocol: "socks5"})
	p.persist()
	if n := p.writeCount(); n != 2 {
		t.Fatalf("writes after mutation = %d, want 2", n)
	}
	data, _ := os.ReadFile(path)
	if !bytes.Contains(data, []byte(`"op-x"`)) {
		t.Fatal("mutation was not written to the file")
	}
}

// TestPersisterWritesOnBumpsAndFlushesOnStop runs the real loop: bumps
// schedule debounced writes, and an edit immediately before Stop is captured
// by the final flush even though the debounce window never elapsed.
func TestPersisterWritesOnBumpsAndFlushesOnStop(t *testing.T) {
	s := NewStore()
	path := filepath.Join(t.TempDir(), "goose.json")
	p := NewPersister(s, path)
	go p.Run(20 * time.Millisecond)
	defer p.Stop()

	waitForFile(t, path, `"engine"`) // the startup write

	s.SetOutbound(&OutboundSpec{ID: "op-a", Protocol: "socks5"})
	waitForFile(t, path, `"op-a"`)

	s.SetOutbound(&OutboundSpec{ID: "op-b", Protocol: "socks5"})
	p.Stop() // flush must capture op-b despite the debounce window
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{`"op-a"`, `"op-b"`} {
		if !bytes.Contains(data, []byte(id)) {
			t.Fatalf("final file is missing %s after Stop flush", id)
		}
	}
}

func waitForFile(t *testing.T, path, substr string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(path); err == nil && bytes.Contains(data, []byte(substr)) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s never appeared in %s", substr, path)
}

func snapshotPoolIDs(pools []*Pool) []string {
	out := make([]string, 0, len(pools))
	for _, p := range pools {
		out = append(out, p.ID)
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
