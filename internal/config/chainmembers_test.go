// chainmembers_test.go — pool chain members (chain_ids): the config-level
// contract. The field round-trips through JSON and the persisted document,
// provider polls preserve operator-set chain members exactly like
// filters/selector, and the managed-pool snapshot strip keeps them.

package config

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// TestPoolChainIDsJSONRoundTrip pins the additive wire contract: a pool with
// chain members carries chain_ids, a plain pool's JSON is byte-identical to
// before the field existed (omitempty).
func TestPoolChainIDsJSONRoundTrip(t *testing.T) {
	plain := &Pool{ID: "p", OutboundIDs: []string{"a"}}
	b, err := json.Marshal(plain)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(b), "chain_ids") {
		t.Fatalf("plain pool JSON must omit chain_ids: %s", b)
	}

	withChains := &Pool{ID: "p", OutboundIDs: []string{"a"}, ChainIDs: []string{"c1", "c2"}}
	b, err = json.Marshal(withChains)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(b), `"chain_ids":["c1","c2"]`) {
		t.Fatalf("pool JSON missing chain_ids: %s", b)
	}
	var back Pool
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(back.ChainIDs, []string{"c1", "c2"}) {
		t.Fatalf("round trip ChainIDs = %v", back.ChainIDs)
	}
}

// TestProviderPollPreservesChainIDs verifies an operator pool with chain
// members survives provider adoption and later polls: the provider rebuilds
// OutboundIDs but keeps the operator's ChainIDs (and filters/selector).
func TestProviderPollPreservesChainIDs(t *testing.T) {
	s := NewStore()
	s.SetChain(&ChainSpec{ID: "c1", Layers: []string{"x"}})
	s.SetPool(&Pool{
		ID:       "default",
		ChainIDs: []string{"c1"},
		Filters:  []FilterSpec{{Type: "location"}},
		Selector: SelectorSpec{Type: "leastlatency"},
	})
	s.SetProvider(&ProviderSpec{ID: "prov", Provider: "psiphon", PoolIDs: []string{"default"}})
	s.SetProviderOutbounds("prov", []*OutboundSpec{{ID: "srv-a", Protocol: "psiphon"}})

	p, ok := s.Pool("default")
	if !ok {
		t.Fatal("pool vanished on adoption")
	}
	if !reflect.DeepEqual(p.ChainIDs, []string{"c1"}) {
		t.Fatalf("adoption dropped ChainIDs: %v", p.ChainIDs)
	}

	// A later poll with changed membership keeps them too.
	s.SetProviderOutbounds("prov", []*OutboundSpec{{ID: "srv-b", Protocol: "psiphon"}})
	p, _ = s.Pool("default")
	if !reflect.DeepEqual(p.ChainIDs, []string{"c1"}) {
		t.Fatalf("provider poll dropped ChainIDs: %v", p.ChainIDs)
	}
	if p.Selector.Type != "leastlatency" || len(p.Filters) != 1 {
		t.Fatalf("provider poll dropped operator tuning: %+v", p)
	}
	if !reflect.DeepEqual(p.OutboundIDs, []string{"srv-b"}) {
		t.Fatalf("provider membership = %v, want [srv-b]", p.OutboundIDs)
	}
}

// TestSnapshotManagedPoolKeepsChainIDs pins the persist contract: the
// managed-pool strip drops only the provider-regenerated OutboundIDs; the
// operator's chain members ride the shallow copy into the document and
// survive a file round-trip.
func TestSnapshotManagedPoolKeepsChainIDs(t *testing.T) {
	s := NewStore()
	s.SetChain(&ChainSpec{ID: "c1", Layers: []string{"op-x"}})
	s.SetOutbound(&OutboundSpec{ID: "op-x", Protocol: "socks5"})
	s.SetPool(&Pool{ID: "default", ChainIDs: []string{"c1"}, Selector: SelectorSpec{Type: "roundrobin"}})
	s.SetProvider(&ProviderSpec{ID: "prov", Provider: "psiphon", PoolIDs: []string{"default"}})
	s.SetProviderOutbounds("prov", []*OutboundSpec{{ID: "srv-a", Protocol: "psiphon"}})

	doc := s.Snapshot()
	var dp *Pool
	for _, p := range doc.Pools {
		if p.ID == "default" {
			dp = p
		}
	}
	if dp == nil {
		t.Fatal("managed pool missing from snapshot")
	}
	if len(dp.OutboundIDs) != 0 {
		t.Fatalf("managed membership not stripped: %v", dp.OutboundIDs)
	}
	if !reflect.DeepEqual(dp.ChainIDs, []string{"c1"}) {
		t.Fatalf("snapshot dropped ChainIDs: %v", dp.ChainIDs)
	}

	// The stored pool pointer must not have been mutated by the strip.
	live, _ := s.Pool("default")
	if len(live.OutboundIDs) != 1 {
		t.Fatalf("snapshot strip mutated the live pool: %v", live.OutboundIDs)
	}

	want, _ := json.Marshal(doc)
	var back Doc
	if err := json.Unmarshal(want, &back); err != nil {
		t.Fatalf("read back: %v", err)
	}
	got, _ := json.Marshal(back)
	if !bytes.Equal(want, got) {
		t.Fatalf("round trip mismatch:\nwant %s\ngot  %s", want, got)
	}
}

// TestSetPoolUnAdoptKeepsChainIDs verifies an operator rewrite of a managed
// pool stores the new ChainIDs verbatim and un-adopts it: a later provider
// poll re-adopts the pool and preserves the operator's members.
func TestSetPoolUnAdoptKeepsChainIDs(t *testing.T) {
	s := NewStore()
	s.SetChain(&ChainSpec{ID: "c1", Layers: []string{"op-x"}})
	s.SetChain(&ChainSpec{ID: "c2", Layers: []string{"op-x"}})
	s.SetOutbound(&OutboundSpec{ID: "op-x", Protocol: "socks5"})
	s.SetProvider(&ProviderSpec{ID: "prov", Provider: "psiphon", PoolIDs: []string{"default"}})
	s.SetProviderOutbounds("prov", []*OutboundSpec{{ID: "srv-a", Protocol: "psiphon"}})

	// Operator rewrite: owns membership now, including chain members.
	s.SetPool(&Pool{ID: "default", OutboundIDs: []string{"op-x"}, ChainIDs: []string{"c1", "c2"}})
	p, _ := s.Pool("default")
	if !reflect.DeepEqual(p.OutboundIDs, []string{"op-x"}) || !reflect.DeepEqual(p.ChainIDs, []string{"c1", "c2"}) {
		t.Fatalf("SetPool did not store members verbatim: %+v", p)
	}

	// The next poll re-adopts (provider still targets the pool) and keeps
	// the operator's chain members.
	s.SetProviderOutbounds("prov", []*OutboundSpec{{ID: "srv-b", Protocol: "psiphon"}})
	p, _ = s.Pool("default")
	if !reflect.DeepEqual(p.ChainIDs, []string{"c1", "c2"}) {
		t.Fatalf("re-adoption dropped ChainIDs: %v", p.ChainIDs)
	}
}
