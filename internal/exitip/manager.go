// Package exitip implements the exit-IP check subsystem: for each outbound
// the engine can dial, ask an "what is my IP" style site for the public IP
// observed at the exit, and persist the result.
//
// A proxy's connect address is NOT its exit address, and the exit can vary
// by target site (per-site egress pools) and over time (dynamic dialer
// pools, rotating residential proxies). So a check records the (outbound,
// site, exit IP, time) tuple; history accumulates so variation over either
// axis is visible.
//
// Checks dial THROUGH the outbound — the same DialContext a proxied request
// uses — so the measured exit IP is the one real traffic gets. Browsers
// cannot route fetch() through an arbitrary SOCKS5/HTTP proxy, so the check
// runs engine-side and the frontend reads results via the admin API.
//
// Load spreading: rounds rotate round-robin across the configured site list
// so large batches do not hammer one site. Within a round, (outbound, site)
// pairs are also staggered by round-robin over the sites and capped by the
// configured concurrency. The default site list sticks to plain-text IP
// echo services with no API keys.
package exitip

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/core"
)

// defaultSites are the built-in check-site URLs (plain-text IP echoers).
// Rotating across several keeps any one site's rate limits out of the data.
var defaultSites = []string{
	"https://ifconfig.me",
	"https://api.ipify.org",
	"https://checkip.amazonaws.com",
	"https://ipinfo.io/ip",
}

// Recorder persists check results (implemented by the metrics store).
type Recorder interface {
	RecordExitCheck(c core.ExitCheck) error
	RecentExitChecks(n int, outboundID string) ([]core.ExitCheck, error)
}

// outboundSource supplies the outbounds to check and the dial path each
// check uses; the router implements both (its outbound map keeps
// engine-built outbounds live, and its dial path prepends the auto-chain
// relay hop when the outbound's server is only reachable through an access
// proxy). Only outbounds with a dial address participate; address-less
// outbounds (direct, tunnels) have no distinct exit IP.
type outboundSource interface {
	// CheckOutbounds returns the outbounds to check (any order; the manager
	// rotates them).
	CheckOutbounds() []core.Outbound
	// CheckDial dials target through out the way dispatch would.
	CheckDial(ctx context.Context, out core.Outbound, network core.Network, target string) (net.Conn, error)
}

// Manager runs the exit-IP check subsystem.
type Manager struct {
	rec    Recorder
	source outboundSource

	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
	running *config.ExitCheckConfig // nil when the loop is stopped
	// roundSeq advances site rotation across rounds.
	roundSeq atomic.Int64
}

// NewManager builds a Manager. It does not start; call Reconfigure.
func NewManager(rec Recorder, src outboundSource) *Manager {
	return &Manager{
		rec:    rec,
		source: src,
	}
}

// Reconfigure aligns the scheduler with the current config: stop the loop
// when disabled, else (re)start it under the new schedule. Safe to call on
// every engine config bump — identical schedule fields keep the running
// loop (its cron phase is preserved).
func (m *Manager) Reconfigure(cfg config.ExitCheckConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !cfg.Enabled {
		m.stopLocked()
		return
	}
	if m.cancel != nil && m.scheduleUnchanged(cfg) {
		return
	}
	m.stopLocked()
	schedule, err := ParseCron(cfg.Cron)
	if cfg.Cron == "" {
		schedule = nil
	} else if err != nil {
		// invalid cron: keep the previous loop (or none); the API's config
		// validation rejects it before it gets here.
		return
	}
	stored := cfg
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	m.done = make(chan struct{})
	m.running = &stored
	go m.loop(ctx, cfg, schedule)
}

// scheduleUnchanged is conservative: Reconfigure restarts the loop unless
// disabled. Restarting on every engine config bump would discard the cron
// timer's phase, so we compare the fields that shape the schedule.
func (m *Manager) scheduleUnchanged(next config.ExitCheckConfig) bool {
	if m.running == nil {
		return false
	}
	r := m.running
	return r.Enabled == next.Enabled &&
		r.Cron == next.Cron &&
		r.IntervalSec == next.IntervalSec &&
		len(r.Sites) == len(next.Sites) &&
		r.TimeoutSec == next.TimeoutSec &&
		r.Concurrency == next.Concurrency
}

// stopLocked stops the running loop (if any). Caller holds m.mu.
func (m *Manager) stopLocked() {
	if m.cancel != nil {
		m.cancel()
		<-m.done
		m.cancel = nil
		m.running = nil
	}
}

// Close stops the scheduler. Idempotent.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopLocked()
}

// CheckNow starts one check round in the background, using the last applied
// schedule's site list, timeout, and concurrency (or the defaults when never
// configured). It does not wait; results land in the recorder as they
// complete. On-demand triggers let the frontend request a check without
// touching the cron schedule.
func (m *Manager) CheckNow() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cancel == nil {
		return // engine wiring failed or never enabled: nothing to check with
	}
	go m.runRound(context.Background(), *m.running)
}

// loop schedules rounds: cron-precedence when a schedule was parsed,
// otherwise fixed interval.
func (m *Manager) loop(ctx context.Context, cfg config.ExitCheckConfig, schedule *Schedule) {
	defer close(m.done)
	// The first round runs immediately: a freshly enabled checker should
	// not wait a full interval to populate data.
	m.runRound(ctx, cfg)
	if schedule != nil {
		timer := time.NewTimer(time.Until(nextMatch(schedule)))
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				m.runRound(ctx, cfg)
				timer.Reset(time.Until(nextMatch(schedule)))
			}
		}
	}
	interval := time.Duration(cfg.IntervalSec) * time.Second
	if interval < 10*time.Second {
		interval = 10 * time.Second
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			m.runRound(ctx, cfg)
		}
	}
}

// nextMatch wraps Schedule.Next's error into a fallback so a failed scan
// reschedules instead of spinning.
func nextMatch(s *Schedule) time.Time {
	t, err := s.Next(time.Now())
	if err != nil {
		return time.Now().Add(time.Hour)
	}
	return t
}

// runRound checks every configured outbound once, rotating sites so
// successive outbounds hit different check sites (spreads the load a large
// batch generates, and would surface per-site exit differences given a
// site list).
func (m *Manager) runRound(ctx context.Context, cfg config.ExitCheckConfig) {
	outs := m.source.CheckOutbounds()
	if len(outs) == 0 {
		return
	}
	sites := cfg.Sites
	if len(sites) == 0 {
		sites = defaultSites
	}
	timeout := cfg.EffectiveTimeoutSec()
	conc := cfg.EffectiveConcurrency()

	// Rotate the starting site each round so time-striped batch data also
	// spreads across sites.
	base := int(m.roundSeq.Add(1)-1) % len(sites)

	type job struct {
		out  core.Outbound
		site string
	}
	jobs := make([]job, 0, len(outs))
	for i, o := range outs {
		if o.Address() == "" {
			continue // address-less outbounds have no distinct exit IP
		}
		jobs = append(jobs, job{out: o, site: sites[(base+i)%len(sites)]})
	}

	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	for _, j := range jobs {
		wg.Add(1)
		go func(j job) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()
			m.checkOne(ctx, j.out, j.site, timeout)
		}(j)
	}
	wg.Wait()
}

// checkOne dials one outbound to one check site and records the result.
func (m *Manager) checkOne(ctx context.Context, out core.Outbound, site string, timeout time.Duration) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	start := time.Now()

	check := core.ExitCheck{
		ID:         fmt.Sprintf("%d-%s", start.UnixNano(), out.ID()),
		OutboundID: out.ID(),
		Site:       hostOf(site),
		StartedAt:  start,
	}
	ip, err := m.fetchExitIP(cctx, out, site)
	check.Latency = time.Since(start)
	if err != nil {
		check.Error = err.Error()
	} else {
		check.Success = true
		check.ExitIP = ip
	}
	_ = m.rec.RecordExitCheck(check)
}

// fetchExitIP GETs the site's plain-text response through the outbound's
// tunnel and trims it to the echoed IP.
func (m *Manager) fetchExitIP(ctx context.Context, out core.Outbound, site string) (string, error) {
	if _, err := url.Parse(site); err != nil || !strings.HasPrefix(site, "http") {
		return "", fmt.Errorf("bad site URL %q", site)
	}
	// Dial the site's host:80/443 through the outbound's dial path — the
	// same one dispatch uses, relay hop included — then speak HTTP over it.
	trans := &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return m.source.CheckDial(ctx, out, core.NetworkTCP, addr)
		},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, site, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "curl/8.0") // ifconfig.me etc. behave best without browser UAs
	resp, err := (&http.Client{Transport: trans}).Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("%s: status %d", site, resp.StatusCode)
	}
	ip := strings.TrimSpace(string(body))
	if net.ParseIP(ip) == nil {
		return "", fmt.Errorf("%s: response %q is not an IP", site, truncate(ip, 32))
	}
	return ip, nil
}

// hostOf extracts a display host from a site URL.
func hostOf(site string) string {
	if u, err := url.Parse(site); err == nil && u.Host != "" {
		return u.Host
	}
	return site
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
