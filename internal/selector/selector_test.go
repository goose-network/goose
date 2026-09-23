package selector

import (
	"fmt"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/core"
	"github.com/goose-network/goose/internal/core/fake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mkPool(n int) []core.Outbound {
	out := make([]core.Outbound, n)
	for i := range out {
		out[i] = &fake.FakeOutbound{IDVal: string(rune('a' + i)), Proto: "direct"}
	}
	return out
}

func TestRandomPicksFromPool(t *testing.T) {
	s := &Random{}
	pool := mkPool(4)
	o, err := s.Pick(pool, core.Metadata{})
	require.NoError(t, err)
	assert.Contains(t, []string{"a", "b", "c", "d"}, o.ID())
}

func TestRandomEmptyPool(t *testing.T) {
	_, err := (&Random{}).Pick(nil, core.Metadata{})
	assert.ErrorIs(t, err, ErrEmptyPool)
}

func TestRoundRobinCycles(t *testing.T) {
	s := &RoundRobin{}
	pool := mkPool(3)
	got := map[string]int{}
	for i := 0; i < 6; i++ {
		o, err := s.Pick(pool, core.Metadata{})
		require.NoError(t, err)
		got[o.ID()]++
	}
	// each of 3 outbounds picked exactly twice over 6 calls
	assert.Equal(t, 2, got["a"])
	assert.Equal(t, 2, got["b"])
	assert.Equal(t, 2, got["c"])
}

func TestStickyByDomainConsistent(t *testing.T) {
	s, err := newSticky(map[string]any{"key": "domain"})
	require.NoError(t, err)
	pool := mkPool(5)
	meta := core.Metadata{TargetHost: "api.example.com", TargetPort: "443"}
	first, err := s.Pick(pool, meta)
	require.NoError(t, err)
	// same domain must always resolve to the same outbound
	for i := 0; i < 10; i++ {
		o, err := s.Pick(pool, meta)
		require.NoError(t, err)
		assert.Equal(t, first.ID(), o.ID(), "sticky must be consistent for same domain")
	}
}

func TestStickyETLDPlusOne(t *testing.T) {
	// subdomains of the same registrable domain should stick together.
	s, _ := newSticky(map[string]any{"key": "domain"})
	pool := mkPool(8)
	a, _ := s.Pick(pool, core.Metadata{TargetHost: "www.example.com"})
	b, _ := s.Pick(pool, core.Metadata{TargetHost: "api.example.com"})
	c, _ := s.Pick(pool, core.Metadata{TargetHost: "example.com"})
	assert.Equal(t, a.ID(), b.ID())
	assert.Equal(t, a.ID(), c.ID())
}

func TestStickyIPFallsBackToRandom(t *testing.T) {
	s, _ := newSticky(map[string]any{"key": "domain"})
	pool := mkPool(4)
	// IP targets have no domain key; must not error and must return one.
	o, err := s.Pick(pool, core.Metadata{TargetHost: "1.2.3.4"})
	require.NoError(t, err)
	assert.NotNil(t, o)
}

func TestLeastLatencyPicksFastest(t *testing.T) {
	s, _ := newLeastLatency(nil)
	pool := []core.Outbound{
		(&fake.FakeOutbound{IDVal: "slow", Proto: "direct"}).WithLatency(500 * time.Millisecond),
		(&fake.FakeOutbound{IDVal: "fast", Proto: "direct"}).WithLatency(20 * time.Millisecond),
		(&fake.FakeOutbound{IDVal: "mid", Proto: "direct"}).WithLatency(100 * time.Millisecond),
	}
	o, err := s.Pick(pool, core.Metadata{})
	require.NoError(t, err)
	assert.Equal(t, "fast", o.ID())
}

func TestLeastLatencyToleranceHysteresis(t *testing.T) {
	s, _ := newLeastLatency(map[string]any{"tolerance_ms": 200.0})
	pool := []core.Outbound{
		(&fake.FakeOutbound{IDVal: "a", Proto: "direct"}).WithLatency(100 * time.Millisecond),
		(&fake.FakeOutbound{IDVal: "b", Proto: "direct"}).WithLatency(150 * time.Millisecond),
	}
	first, _ := s.Pick(pool, core.Metadata{})
	// 'a' is fastest (100ms); 'b' is within 200ms tolerance, so 'a' stays.
	assert.Equal(t, "a", first.ID())
	second, _ := s.Pick(pool, core.Metadata{})
	assert.Equal(t, "a", second.ID(), "hysteresis should keep current pick")
}

func TestNewUnknownType(t *testing.T) {
	_, err := New("bogus", nil)
	assert.Error(t, err)
}

func TestNewDefaultsToRandom(t *testing.T) {
	s, err := New("", nil)
	require.NoError(t, err)
	assert.Equal(t, "random", s.Name())
}

func TestJumpHashDistribution(t *testing.T) {
	// jump hash should distribute keys roughly uniformly.
	counts := map[int]int{}
	for i := 0; i < 10000; i++ {
		counts[jumpHash(fmt.Sprintf("key-%d", i), 10)]++
	}
	for b, c := range counts {
		if c < 500 || c > 1500 {
			t.Fatalf("bucket %d out of balance: %d", b, c)
		}
	}
}
