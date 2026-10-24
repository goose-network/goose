package exitip

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeOutbound is an outbound whose DialContext dials a chosen local
// address. Its "exit IP" is whatever that server answers.
type fakeOutbound struct {
	id   string
	addr string
}

func (f *fakeOutbound) ID() string                { return f.id }
func (f *fakeOutbound) Protocol() string          { return "fake" }
func (f *fakeOutbound) Address() string           { return f.addr }
func (f *fakeOutbound) Close() error              { return nil }
func (f *fakeOutbound) Location() *core.Location  { return nil }
func (f *fakeOutbound) Stats() core.OutboundStats { return core.OutboundStats{} }
func (f *fakeOutbound) DialContext(ctx context.Context, n core.Network, target string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "tcp", target)
}

// fakeSource supplies fixed outbounds and forwards dials (no relay hop in
// these tests).
type fakeSource struct {
	outs []core.Outbound
}

func (f *fakeSource) CheckOutbounds() []core.Outbound { return f.outs }
func (f *fakeSource) CheckDial(ctx context.Context, out core.Outbound, n core.Network, target string) (net.Conn, error) {
	return out.DialContext(ctx, n, target)
}

// fakeRecorder collects recorded checks in memory.
type fakeRecorder struct {
	mu     sync.Mutex
	checks []core.ExitCheck
}

func (f *fakeRecorder) RecordExitCheck(c core.ExitCheck) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.checks = append(f.checks, c)
	return nil
}

func (f *fakeRecorder) RecentExitChecks(n int, outboundID string) ([]core.ExitCheck, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []core.ExitCheck
	for i := len(f.checks) - 1; i >= 0 && len(out) < n; i-- {
		if outboundID == "" || f.checks[i].OutboundID == outboundID {
			out = append(out, f.checks[i])
		}
	}
	return out, nil
}

// siteServer is an "what is my IP" style check site: it answers with the
// caller's address.
func siteServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, _ := net.SplitHostPort(r.RemoteAddr)
		fmt.Fprint(w, host)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestManagerRoundChecksAllOutbounds verifies a single round (triggered
// through CheckNow with a running scheduler) checks every address-bearing
// outbound and records the observed exit IP.
func TestManagerRoundChecksAllOutbounds(t *testing.T) {
	site := siteServer(t)

	// Two outbounds: one with a dial address, one without (must be skipped).
	src := &fakeSource{outs: []core.Outbound{
		&fakeOutbound{id: "a", addr: "127.0.0.1:1"},
		&directOutbound{id: "direct"},
	}}
	rec := &fakeRecorder{}
	m := NewManager(rec, src)
	defer m.Close()

	m.Reconfigure(config.ExitCheckConfig{
		Enabled:     true,
		IntervalSec: 3600, // far future: only the immediate first round runs
		Sites:       []string{site.URL},
		TimeoutSec:  5,
	})
	require.Eventually(t, func() bool { return len(rec.snapshot()) >= 1 }, 5*time.Second, 20*time.Millisecond)

	checks := rec.snapshot()
	require.Len(t, checks, 1)
	assert.Equal(t, "a", checks[0].OutboundID)
	assert.Equal(t, "127.0.0.1", checks[0].ExitIP)
	assert.True(t, checks[0].Success)
	assert.NotEmpty(t, checks[0].StartedAt)
}

// TestManagerRoundRotatesSites verifies each successive outbound in a round
// is assigned a different site (round-robin), spreading batch load.
func TestManagerRoundRotatesSites(t *testing.T) {
	site1, site2 := siteServer(t), siteServer(t)
	src := &fakeSource{outs: []core.Outbound{
		&fakeOutbound{id: "a", addr: "127.0.0.1:1"},
		&fakeOutbound{id: "b", addr: "127.0.0.1:2"},
	}}
	rec := &fakeRecorder{}
	m := NewManager(rec, src)
	defer m.Close()

	m.Reconfigure(config.ExitCheckConfig{
		Enabled:     true,
		IntervalSec: 3600,
		Sites:       []string{site1.URL, site2.URL},
		TimeoutSec:  5,
	})
	require.Eventually(t, func() bool { return len(rec.snapshot()) == 2 }, 5*time.Second, 20*time.Millisecond)

	byOut := map[string]string{}
	for _, c := range rec.snapshot() {
		byOut[c.OutboundID] = c.Site
	}
	assert.NotEqual(t, byOut["a"], byOut["b"], "successive outbounds should hit different sites")
}

// TestManagerRecordsFailures verifies a failing outbound (connection refused)
// records a check with Success=false and the dial error, not silence.
func TestManagerRecordsFailures(t *testing.T) {
	site := siteServer(t)
	src := &fakeSource{outs: []core.Outbound{
		&fakeOutbound{id: "a", addr: "127.0.0.1:1"},
		&brokenOutbound{id: "bad"},
	}}
	rec := &fakeRecorder{}
	m := NewManager(rec, src)
	defer m.Close()

	m.Reconfigure(config.ExitCheckConfig{
		Enabled:     true,
		IntervalSec: 3600,
		Sites:       []string{site.URL},
		TimeoutSec:  5,
	})
	require.Eventually(t, func() bool { return len(rec.snapshot()) >= 2 }, 5*time.Second, 20*time.Millisecond)

	var bad *core.ExitCheck
	for i := range rec.snapshot() {
		if rec.snapshot()[i].OutboundID == "bad" {
			bad = &rec.snapshot()[i]
		}
	}
	require.NotNil(t, bad)
	assert.False(t, bad.Success)
	assert.NotEmpty(t, bad.Error)
}

// brokenOutbound always fails to dial.
type brokenOutbound struct{ id string }

func (b *brokenOutbound) ID() string                { return b.id }
func (b *brokenOutbound) Protocol() string          { return "fake" }
func (b *brokenOutbound) Address() string           { return "127.0.0.1:1" }
func (b *brokenOutbound) Close() error              { return nil }
func (b *brokenOutbound) Location() *core.Location  { return nil }
func (b *brokenOutbound) Stats() core.OutboundStats { return core.OutboundStats{} }
func (b *brokenOutbound) DialContext(ctx context.Context, n core.Network, target string) (net.Conn, error) {
	return nil, fmt.Errorf("simulated dial failure")
}

// directOutbound has no server address (like the direct/tunnel outbounds):
// the manager must skip it in rounds.
type directOutbound struct{ id string }

func (d *directOutbound) ID() string                { return d.id }
func (d *directOutbound) Protocol() string          { return "fake" }
func (d *directOutbound) Address() string           { return "" }
func (d *directOutbound) Close() error              { return nil }
func (d *directOutbound) Location() *core.Location  { return nil }
func (d *directOutbound) Stats() core.OutboundStats { return core.OutboundStats{} }
func (d *directOutbound) DialContext(ctx context.Context, n core.Network, target string) (net.Conn, error) {
	var nd net.Dialer
	return nd.DialContext(ctx, "tcp", target)
}

// TestManagerDisabledDoesNothing verifies a disabled (zero) config leaves
// the manager idle: no rounds, no recorded checks.
func TestManagerDisabledDoesNothing(t *testing.T) {
	src := &fakeSource{outs: []core.Outbound{&fakeOutbound{id: "a", addr: "127.0.0.1:1"}}}
	rec := &fakeRecorder{}
	m := NewManager(rec, src)
	defer m.Close()

	m.Reconfigure(config.ExitCheckConfig{Enabled: false})

	time.Sleep(200 * time.Millisecond)
	assert.Empty(t, rec.snapshot())
}

// TestManagerReconfigureStopsLoop verifies disabling after running cancels
// the loop and further rounds do not run.
func TestManagerReconfigureStopsLoop(t *testing.T) {
	site := siteServer(t)
	src := &fakeSource{outs: []core.Outbound{&fakeOutbound{id: "a", addr: "127.0.0.1:1"}}}
	rec := &fakeRecorder{}
	m := NewManager(rec, src)
	defer m.Close()

	m.Reconfigure(config.ExitCheckConfig{Enabled: true, IntervalSec: 3600, Sites: []string{site.URL}, TimeoutSec: 5})
	require.Eventually(t, func() bool { return len(rec.snapshot()) >= 1 }, 5*time.Second, 20*time.Millisecond)

	m.Reconfigure(config.ExitCheckConfig{Enabled: false})
	time.Sleep(200 * time.Millisecond)
	n := len(rec.snapshot())
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, n, len(rec.snapshot()), "no further rounds after disable")
}

// TestManagerCheckNowWithoutConfig does nothing (engine never enabled the
// subsystem — no sites/timeout to check with).
func TestManagerCheckNowWithoutConfig(t *testing.T) {
	src := &fakeSource{outs: []core.Outbound{&fakeOutbound{id: "a", addr: "127.0.0.1:1"}}}
	rec := &fakeRecorder{}
	m := NewManager(rec, src)
	defer m.Close()

	m.CheckNow()
	time.Sleep(200 * time.Millisecond)
	assert.Empty(t, rec.snapshot())
}

func (f *fakeRecorder) snapshot() []core.ExitCheck {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]core.ExitCheck(nil), f.checks...)
}

// TestManagerCronScheduling verifies a cron expression schedules the second
// round at the next matching minute (first round runs immediately).
func TestManagerCronScheduling(t *testing.T) {
	site := siteServer(t)
	src := &fakeSource{outs: []core.Outbound{&fakeOutbound{id: "a", addr: "127.0.0.1:1"}}}
	rec := &fakeRecorder{}
	m := NewManager(rec, src)
	defer m.Close()

	// Every-minute schedule so the next fire is within a minute.
	m.Reconfigure(config.ExitCheckConfig{
		Enabled:    true,
		Cron:       "* * * * *",
		Sites:      []string{site.URL},
		TimeoutSec: 5,
	})
	require.Eventually(t, func() bool { return len(rec.snapshot()) >= 1 }, 5*time.Second, 20*time.Millisecond)

	// Second check arrives at the next cron fire, not on any interval.
	s, _ := ParseCron("* * * * *")
	next, err := s.Next(time.Now())
	require.NoError(t, err)
	deadline := next.Add(10 * time.Second)
	for len(rec.snapshot()) < 2 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	assert.GreaterOrEqual(t, len(rec.snapshot()), 2, "cron schedule should fire a second round")

	if len(rec.snapshot()) >= 2 {
		gap := rec.snapshot()[1].StartedAt.Sub(rec.snapshot()[0].StartedAt)
		assert.Greater(t, gap, 5*time.Second, "second round should come from cron, not a tight loop")
	}
}
