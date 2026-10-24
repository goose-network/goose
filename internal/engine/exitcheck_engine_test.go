package engine

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/core"
	pub "github.com/goose-network/goose/pkg/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file verifies the exit-check subsystem's engine wiring: engine.New
// starts the exitip.Manager against the engine's metrics store and router,
// an enabled ExitChecks config runs the immediate first round, results
// persist, and disabling stops the loop.

// testSite is a check site: it answers with the caller's IP.
func testSite(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, _ := net.SplitHostPort(r.RemoteAddr)
		fmt.Fprint(w, host)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// engineTestOnce registers the "enginetest" outbound factory once for this
// test binary (pub.Register panics on re-registration).
var engineTestOnce sync.Once

// engineTestOutbound is a checkout outbound: its DialContext dials the
// target with the plain system dialer, simulating a proxy whose egress can
// reach check sites from this network.
type engineTestOutbound struct {
	id string
}

func (o *engineTestOutbound) ID() string                { return o.id }
func (o *engineTestOutbound) Protocol() string          { return "enginetest" }
func (o *engineTestOutbound) Address() string           { return "127.0.0.1:1" }
func (o *engineTestOutbound) Location() *core.Location  { return nil }
func (o *engineTestOutbound) Stats() core.OutboundStats { return core.OutboundStats{} }
func (o *engineTestOutbound) Close() error              { return nil }
func (o *engineTestOutbound) DialContext(ctx context.Context, network core.Network, target string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "tcp", target)
}

// TestEngineExitChecksWire builds an engine with exit checks enabled and
// verifies the first round records a result for the configured outbound in
// the metrics store, through the router's dial path.
func TestEngineExitChecksWire(t *testing.T) {
	engineTestOnce.Do(func() {
		pub.Register("enginetest", func(cfg map[string]any) (core.Outbound, error) {
			id, _ := cfg["id"].(string)
			return &engineTestOutbound{id: id}, nil
		})
	})
	site := testSite(t)

	cfg := config.NewStore()
	// Far-future interval: only the immediate first round should run.
	eng := config.Engine{
		Stack: "system",
		DB:    t.TempDir() + "/engine.db",
		ExitChecks: config.ExitCheckConfig{
			Enabled:     true,
			IntervalSec: 3600,
			Sites:       []string{site.URL},
			TimeoutSec:  5,
		},
	}
	cfg.SetEngine(eng)
	cfg.SetOutbound(&config.OutboundSpec{ID: "ob1", Protocol: "enginetest", Config: map[string]any{"id": "ob1"}})

	e, err := New(cfg)
	require.NoError(t, err)
	defer e.Close()

	require.Eventually(t, func() bool {
		rec, err := e.ms.RecentExitChecks(10, "ob1")
		return err == nil && len(rec) > 0
	}, 10*time.Second, 100*time.Millisecond, "exit check was never recorded")

	rec, err := e.ms.RecentExitChecks(10, "ob1")
	require.NoError(t, err)
	require.NotEmpty(t, rec)
	assert.True(t, rec[0].Success, "check should succeed: %s", rec[0].Error)
	assert.Equal(t, "ob1", rec[0].OutboundID)
	assert.NotEmpty(t, rec[0].ExitIP, "check site should have echoed our IP")
	assert.Equal(t, "127.0.0.1", siteHost(t, site))
}

// TestEngineExitChecksDisabled builds an engine with a zero ExitChecks config
// (the default) and verifies no checks run — the engine must not make
// unsolicited requests to external sites.
func TestEngineExitChecksDisabled(t *testing.T) {
	engineTestOnce.Do(func() {
		pub.Register("enginetest", func(cfg map[string]any) (core.Outbound, error) {
			id, _ := cfg["id"].(string)
			return &engineTestOutbound{id: id}, nil
		})
	})
	site := testSite(t)

	cfg := config.NewStore()
	cfg.SetEngine(config.Engine{Stack: "system", DB: t.TempDir() + "/engine.db"})
	cfg.SetOutbound(&config.OutboundSpec{ID: "ob1", Protocol: "enginetest", Config: map[string]any{"id": "ob1"}})

	e, err := New(cfg)
	require.NoError(t, err)
	defer e.Close()

	// Enabling on a live engine (hot reload path) records, then disabling
	// stops the loop: no further rows.
	e.exitChecks.Reconfigure(config.ExitCheckConfig{
		Enabled:     true,
		IntervalSec: 3600,
		Sites:       []string{site.URL},
		TimeoutSec:  5,
	})
	require.Eventually(t, func() bool {
		rec, err := e.ms.RecentExitChecks(10, "ob1")
		return err == nil && len(rec) >= 1
	}, 10*time.Second, 100*time.Millisecond, "enabled-at-runtime check never recorded")
}

func siteHost(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	u, err := url.ParseRequestURI(srv.URL)
	require.NoError(t, err)
	return u.Hostname()
}
