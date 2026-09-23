package filter

import (
	"testing"
	"time"

	"github.com/goose-network/goose/internal/core"
	"github.com/goose-network/goose/internal/core/fake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocationAllowlist(t *testing.T) {
	f, err := newLocation(map[string]any{"countries": []any{"US", "CA"}})
	require.NoError(t, err)
	us := &fake.FakeOutbound{IDVal: "us", Loc: &core.Location{Country: "US"}}
	jp := &fake.FakeOutbound{IDVal: "jp", Loc: &core.Location{Country: "JP"}}
	assert.True(t, f.Allow(us, core.Metadata{}))
	assert.False(t, f.Allow(jp, core.Metadata{}))
}

func TestLocationExclude(t *testing.T) {
	f, _ := newLocation(map[string]any{"exclude": []any{"CN"}})
	cn := &fake.FakeOutbound{IDVal: "cn", Loc: &core.Location{Country: "CN"}}
	us := &fake.FakeOutbound{IDVal: "us", Loc: &core.Location{Country: "US"}}
	assert.False(t, f.Allow(cn, core.Metadata{}))
	assert.True(t, f.Allow(us, core.Metadata{}))
}

func TestLocationNoLocationAdmittedOnlyWithoutAllowlist(t *testing.T) {
	f, _ := newLocation(map[string]any{"countries": []any{"US"}})
	unknown := &fake.FakeOutbound{IDVal: "x", Loc: nil}
	assert.False(t, f.Allow(unknown, core.Metadata{}))

	f2, _ := newLocation(map[string]any{"exclude": []any{"CN"}})
	assert.True(t, f2.Allow(unknown, core.Metadata{}))
}

func TestLatencyFilter(t *testing.T) {
	f, _ := newLatency(map[string]any{"max_ms": 200.0, "min_success": 0.9})
	fast := (&fake.FakeOutbound{IDVal: "fast"}).WithLatency(50 * time.Millisecond).WithSuccess(0.99)
	slow := (&fake.FakeOutbound{IDVal: "slow"}).WithLatency(500 * time.Millisecond).WithSuccess(0.99)
	flaky := (&fake.FakeOutbound{IDVal: "flaky"}).WithLatency(50 * time.Millisecond).WithSuccess(0.5)
	unmeasured := &fake.FakeOutbound{IDVal: "new"}
	assert.True(t, f.Allow(fast, core.Metadata{}))
	assert.False(t, f.Allow(slow, core.Metadata{}))
	assert.False(t, f.Allow(flaky, core.Metadata{}))
	assert.True(t, f.Allow(unmeasured, core.Metadata{}), "unmeasured should be admitted")
}

func TestProtocolWhitelist(t *testing.T) {
	f, _ := newProtocol(map[string]any{"allow": []any{"socks5", "http"}})
	s5 := &fake.FakeOutbound{IDVal: "s5", Proto: "socks5"}
	direct := &fake.FakeOutbound{IDVal: "d", Proto: "direct"}
	assert.True(t, f.Allow(s5, core.Metadata{}))
	assert.False(t, f.Allow(direct, core.Metadata{}))
}

func TestProtocolBlacklist(t *testing.T) {
	f, _ := newProtocol(map[string]any{"deny": []any{"direct"}})
	s5 := &fake.FakeOutbound{IDVal: "s5", Proto: "socks5"}
	direct := &fake.FakeOutbound{IDVal: "d", Proto: "direct"}
	assert.True(t, f.Allow(s5, core.Metadata{}))
	assert.False(t, f.Allow(direct, core.Metadata{}))
}

func TestNewUnknownFilter(t *testing.T) {
	_, err := New("bogus", nil)
	assert.Error(t, err)
}
