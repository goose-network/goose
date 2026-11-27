package router

import (
	"bytes"
	"log"
	"sync"
	"testing"

	"github.com/goose-network/goose/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRebuildMemberAttachSkipsUnbuiltPool drives the member-attach pass
// directly with the mismatched-epoch shapes the old three-read rebuild
// could produce under concurrent config writes: a path-pool spec whose ID
// was never built into r.pools (absent key), and one whose built entry is
// nil. The pass must skip those pools' members with a log — never nil-deref
// the *builtPool — and still attach eligible members of pools that WERE
// built.
func TestRebuildMemberAttachSkipsUnbuiltPool(t *testing.T) {
	r, _ := testRouter(t,
		[]*config.OutboundSpec{
			memberOutbound("h1", "testdirect", "127.0.0.1:9931"),
			memberOutbound("h2", "testchain", "127.0.0.1:9932"),
		},
		[]*config.Pool{{ID: "built", ChainIDs: []string{"c1"}, Selector: config.SelectorSpec{Type: "roundrobin"}}},
		[]*config.ChainSpec{{ID: "c1", Layers: []string{"h1", "h2"}}},
		nil,
	)
	t.Cleanup(r.Close)

	var logs bytes.Buffer
	old := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(old) })

	// Mismatched snapshots: `ghost` exists only in the pool-spec list, not
	// in r.pools — exactly what a SetPool landing between the pools pass
	// and a freshly-read member-attach list used to yield. `nilpool` covers
	// the other guard branch: a present-but-nil built entry.
	ghost := &config.Pool{ID: "ghost", ChainIDs: []string{"c1"}, Selector: config.SelectorSpec{Type: "roundrobin"}}
	nilPool := &config.Pool{ID: "nilpool", ChainIDs: []string{"c1"}, Selector: config.SelectorSpec{Type: "roundrobin"}}
	chainSpecs := map[string]*config.ChainSpec{"c1": {ID: "c1", Layers: []string{"h1", "h2"}}}
	r.mu.Lock()
	r.pools["nilpool"] = nil
	before := len(r.pools["built"].outbounds)
	require.NotPanics(t, func() {
		r.attachChainMembersLocked([]*config.Pool{ghost, nilPool, {ID: "built", ChainIDs: []string{"c1"}}}, chainSpecs)
	})
	after := len(r.pools["built"].outbounds)
	r.mu.Unlock()

	assert.Contains(t, logs.String(), "pool ghost has chain members but was not built")
	assert.Contains(t, logs.String(), "pool nilpool has chain members but was not built")
	r.mu.RLock()
	_, ghostBuilt := r.pools["ghost"]
	member := r.pools["built"].outbounds[after-1]
	r.mu.RUnlock()
	assert.False(t, ghostBuilt, "an unbuilt pool must never be conjured into r.pools")
	assert.Equal(t, before+1, after, "the built pool's eligible member must still attach")
	assert.Equal(t, "c1", member.ID(), "the attached candidate is the member token")
}

// TestRebuildSingleEpochUnderConcurrentWrites is the crash regression for
// the multi-read hazard: the engine watch goroutine rebuilds while a writer
// hammers SetChain/SetPool/SetOutbound/SetInbound with overlapping IDs —
// the exact back-to-back write pattern of a pool-mode full-probe apply
// (SetChain xN -> SetPool -> SetInbound), plus pool IDs appearing and
// disappearing, chains whose layers name path-pools (forbidden-layer
// backstop), and member chains referencing pools (pool-shadow backstop).
// Before the single-snapshot fix, a SetPool landing between rebuild's
// pools read and its member-attach read made the attach pass deref a nil
// *builtPool and crash the process. Must pass under -race with no panic.
func TestRebuildSingleEpochUnderConcurrentWrites(t *testing.T) {
	pool, in := poolOnMembers("in", "pp", []string{"c1", "c2"}, "roundrobin", nil)
	r, cfg := testRouter(t,
		[]*config.OutboundSpec{
			memberOutbound("h1", "testdirect", "127.0.0.1:9941"),
			memberOutbound("h2", "testchain", "127.0.0.1:9942"),
			memberOutbound("h3", "testdirect", "127.0.0.1:9943"),
			memberOutbound("h4", "testchain", "127.0.0.1:9944"),
		},
		[]*config.Pool{pool},
		[]*config.ChainSpec{
			{ID: "c1", Layers: []string{"h1", "h2"}},
			{ID: "c2", Layers: []string{"h3", "h4"}},
		},
		[]*config.Inbound{in},
	)
	t.Cleanup(r.Close)

	const iters = 400
	var wg sync.WaitGroup
	wg.Add(2)

	// Rebuild side: the public Rebuild entry point, exactly what the engine
	// watch goroutine calls on every config version bump.
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			r.Rebuild()
		}
	}()

	// Writer side: fresh immutable specs per write (the store hands spec
	// pointers to readers, so specs are replaced, never mutated).
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			// Member chains rewritten first, as full-probe apply does.
			cfg.SetChain(&config.ChainSpec{ID: "c1", Layers: []string{"h1", "h2"}})
			cfg.SetChain(&config.ChainSpec{ID: "c2", Layers: []string{"h3", "h4"}})
			// A chain traversing the path-pool exercises the whole-chain
			// skip backstop; a member chain naming a pool exercises the
			// pool-shadow skip.
			cfg.SetChain(&config.ChainSpec{ID: "bad", Layers: []string{"h1", "pp"}})
			cfg.SetChain(&config.ChainSpec{ID: "shadowed", Layers: []string{"pp"}})

			// Path-pool membership churn: pp gains/loses members and even
			// flips between path-pool and plain pool between passes.
			if i%2 == 0 {
				cfg.SetPool(&config.Pool{ID: "pp", ChainIDs: []string{"c1", "c2", "shadowed", "gone"}, Selector: config.SelectorSpec{Type: "roundrobin"}})
			} else {
				cfg.SetPool(&config.Pool{ID: "pp", OutboundIDs: []string{"h1"}, ChainIDs: []string{"c1"}, Selector: config.SelectorSpec{Type: "random"}})
			}
			// A pool that appears only AFTER the chains above were written:
			// under multi-read rebuild this ID could enter the attach pass
			// without ever having been built.
			cfg.SetPool(&config.Pool{ID: "ephemeral", ChainIDs: []string{"c2", "bad"}, Selector: config.SelectorSpec{Type: "roundrobin"}})

			cfg.SetOutbound(memberOutbound("h1", "testdirect", "127.0.0.1:9941"))
			cfg.SetOutbound(memberOutbound("h4", "testchain", "127.0.0.1:9944"))
			cfg.SetInbound(&config.Inbound{ID: "in", Protocol: "http", Listen: "127.0.0.1:0", Policy: config.InboundPolicy{PoolID: "pp"}})

			// Deletions make IDs vanish between passes, too.
			cfg.DeletePool("ephemeral")
			cfg.DeleteChain("bad")
			cfg.DeleteOutbound("h4")
			cfg.SetOutbound(memberOutbound("h4", "testchain", "127.0.0.1:9944"))
			cfg.SetChain(&config.ChainSpec{ID: "bad", Layers: []string{"h1", "pp"}})
		}
	}()
	wg.Wait()

	// The stress must actually have exercised the member-attach pass: after
	// a final clean config + rebuild, pp holds its chain-member tokens.
	cfg.DeletePool("ephemeral")
	cfg.SetPool(&config.Pool{ID: "pp", ChainIDs: []string{"c1", "c2"}, Selector: config.SelectorSpec{Type: "roundrobin"}})
	r.Rebuild()
	r.mu.RLock()
	members := r.pools["pp"].outbounds
	r.mu.RUnlock()
	require.Len(t, members, 2, "member-attach pass must run: pp holds both chain members")
	for _, m := range members {
		_, isMember := m.(*chainMember)
		assert.True(t, isMember, "pool candidates must be chain-member tokens, got %T", m)
	}
}
