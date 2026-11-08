// HTTP reachability probes: issue a real HTTP request toward a target
// through a chosen route — one outbound, a pool, a chain, or the engine's own
// stack — and record what came back.
//
// The TCP dial prober (probe.go) answers "can the local network reach this
// proxy server at all". That is necessary but not sufficient: a server can
// accept a TCP connection and still fail to proxy (dead credentials, an
// expired tunnel, a protocol mismatch, a captive portal in front of it). An
// HTTP probe answers the question an operator actually has — "does traffic
// through this route work, end to end, right now?" — and its evidence is a
// real response: status, headers, body.
//
// The dial path is the one dispatch uses: routes resolve through
// OutboundUpstreamDialer (upstream.go), so a probe through "pool:x" applies
// that pool's filters, selector and failover exactly as a proxied request
// would, and a probe through "outbound:y" rides y's own upstream_proxy if it
// has one.
//
// Probe rows are recorded twice: as a durable core.HTTPProbe (the response
// detail, read back by GET /api/probes/results) and as a core.RequestMetric
// row with Chain [core.HopProbe, route] so the metrics table shows probe
// traffic alongside request and dial-probe rows. Neither feeds the
// reachability verdicts routing filters on — see core.HopProbe for why.
package router

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/goose-network/goose/internal/core"
)

const (
	// probeHTTPConcurrency caps parallel HTTP probes per round, so probing
	// 200 outbounds is a measured sweep rather than a connection storm.
	probeHTTPConcurrency = 8
	// defaultProbeTimeout bounds one HTTP probe request when the spec sets
	// none.
	defaultProbeTimeout = 15 * time.Second
	// maxProbeTimeout caps a spec's timeout so a probe round cannot hang the
	// scheduler for minutes.
	maxProbeTimeout = 5 * time.Minute
	// defaultProbeBodyBytes is how much of a response body a probe keeps when
	// the spec sets no limit; maxProbeBodyBytes is the ceiling. A body is kept
	// to assert against, not to download.
	defaultProbeBodyBytes = 4 << 10
	maxProbeBodyBytes     = 1 << 20
	// maxProbeFanout bounds one round's (target × route) request count.
	maxProbeFanout = 512
	// maxProbeRoutes bounds the spec's route list, so the fan-out check has
	// something small to reject first.
	maxProbeRoutes = 128
)

// RunHTTPProbes issues spec's request toward every target through every route
// and returns one result per (target, route) pair, targets in spec order and
// routes in expanded order. Each result is recorded durably before it is
// returned, so a caller that times out on the response still finds the rows
// in GET /api/probes/results.
//
// It returns an error only for a spec that cannot be run at all (no targets,
// a non-HTTP target, a fan-out beyond maxProbeFanout); per-request failures
// are results, not errors — a route that cannot be dialed is exactly what the
// probe is for.
func (r *Router) RunHTTPProbes(ctx context.Context, spec core.HTTPProbeSpec) ([]core.HTTPProbe, error) {
	targets, err := core.ValidProbeTargets(spec.Targets)
	if err != nil {
		return nil, err
	}
	routes := r.probeRoutes(spec.Routes)
	if len(routes) == 0 {
		return nil, fmt.Errorf("router: no routes to probe (no outbounds built and no routes given)")
	}
	if len(routes) > maxProbeRoutes {
		return nil, fmt.Errorf("router: %d probe routes exceeds the %d limit", len(routes), maxProbeRoutes)
	}
	if len(targets)*len(routes) > maxProbeFanout {
		return nil, fmt.Errorf("router: probe fan-out %d targets × %d routes exceeds %d requests per round",
			len(targets), len(routes), maxProbeFanout)
	}

	timeout := spec.Timeout
	if timeout <= 0 {
		timeout = defaultProbeTimeout
	}
	if timeout > maxProbeTimeout {
		timeout = maxProbeTimeout
	}
	bodyLimit := spec.MaxBodyBytes
	if bodyLimit <= 0 {
		bodyLimit = defaultProbeBodyBytes
	}
	if bodyLimit > maxProbeBodyBytes {
		bodyLimit = maxProbeBodyBytes
	}

	type job struct {
		target string
		route  string
	}
	jobs := make([]job, 0, len(targets)*len(routes))
	for _, t := range targets {
		for _, route := range routes {
			jobs = append(jobs, job{target: t, route: route})
		}
	}

	results := make([]core.HTTPProbe, len(jobs))
	var wg sync.WaitGroup
	sem := make(chan struct{}, probeHTTPConcurrency)
	for i, j := range jobs {
		wg.Add(1)
		go func(i int, j job) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				results[i] = r.probeResult(spec, j.target, j.route, time.Now(), 0, nil, "", false, ctx.Err())
				return
			}
			defer func() { <-sem }()
			results[i] = r.probeOneHTTP(ctx, spec, j.target, j.route, timeout, bodyLimit)
		}(i, j)
	}
	wg.Wait()
	return results, nil
}

// probeRoutes expands a probe's route list: the references as given (with
// duplicates dropped, order kept), or — when the list is empty — one
// "outbound:<id>" reference per built outbound, sorted. Address-less
// outbounds (direct, tunnels) are included: unlike a TCP dial probe, an HTTP
// probe through them is a meaningful baseline of the engine's own path.
func (r *Router) probeRoutes(routes []string) []string {
	if len(routes) > 0 {
		out := make([]string, 0, len(routes))
		seen := make(map[string]bool, len(routes))
		for _, route := range routes {
			route = strings.TrimSpace(route)
			if route == "" || seen[route] {
				continue
			}
			seen[route] = true
			out = append(out, route)
		}
		return out
	}
	r.mu.RLock()
	out := make([]string, 0, len(r.outbounds))
	for id := range r.outbounds {
		out = append(out, "outbound:"+id)
	}
	r.mu.RUnlock()
	sort.Strings(out)
	return out
}

// probeOneHTTP performs one (target, route) request and records it.
func (r *Router) probeOneHTTP(ctx context.Context, spec core.HTTPProbeSpec, target, route string, timeout time.Duration, bodyLimit int) core.HTTPProbe {
	pctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	start := time.Now()

	req, err := newProbeRequest(pctx, spec, target)
	if err != nil {
		return r.probeResult(spec, target, route, start, 0, nil, "", false, err)
	}

	client := &http.Client{Transport: r.probeTransport(route), Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return r.probeResult(spec, target, route, start, 0, nil, "", false, err)
	}
	defer resp.Body.Close()

	// Read one byte past the limit so truncation is a fact, not a guess.
	raw, readErr := io.ReadAll(io.LimitReader(resp.Body, int64(bodyLimit)+1))
	body := string(raw)
	truncated := len(raw) > bodyLimit
	if truncated {
		body = string(raw[:bodyLimit])
	}
	if readErr != nil && err == nil {
		err = readErr
	}

	return r.probeResult(spec, target, route, start, resp.StatusCode, resp.Header, body, truncated, err)
}

// probeTransport builds the transport whose dials ride route: the route
// dialer for a named route, the engine stack for none (which is also what
// "direct" resolves to, one layer down).
func (r *Router) probeTransport(route string) *http.Transport {
	trans := &http.Transport{
		DisableKeepAlives: true, // one request per route: no pooled conn reuse
		DialContext:       r.stack.Dial,
	}
	if d := r.OutboundUpstreamDialer(route); d != nil {
		trans.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			return d.DialUpstream(ctx, network, addr)
		}
	}
	return trans
}

// newProbeRequest builds the request: spec's method (GET by default), headers
// and body, with a User-Agent so targets that reject an empty one still
// answer. Host and Content-Length are the transport's to set.
func newProbeRequest(ctx context.Context, spec core.HTTPProbeSpec, target string) (*http.Request, error) {
	method := strings.ToUpper(strings.TrimSpace(spec.Method))
	if method == "" {
		method = http.MethodGet
	}
	var body io.Reader
	if spec.Body != "" {
		body = strings.NewReader(spec.Body)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return nil, err
	}
	for name, value := range spec.Headers {
		if name == "" {
			continue
		}
		req.Header.Set(name, value)
	}
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "goose-probe/1.0")
	}
	return req, nil
}

// probeResult assembles one result, records it (probe row + metric row) and
// returns it. success is transport-level: a response arrived. A 500 or a
// failed assertion is still a reachable route — the caller's expectations
// decide whether the response was the right one.
func (r *Router) probeResult(spec core.HTTPProbeSpec, target, route string, start time.Time, status int, header http.Header, body string, truncated bool, err error) core.HTTPProbe {
	latency := time.Since(start)
	method := strings.ToUpper(strings.TrimSpace(spec.Method))
	if method == "" {
		method = http.MethodGet
	}
	p := core.HTTPProbe{
		ID:        fmt.Sprintf("%d-%s", start.UnixNano(), route),
		Route:     route,
		Target:    target,
		Method:    method,
		Status:    status,
		Headers:   flattenHeaders(header),
		Body:      body,
		Truncated: truncated,
		Success:   err == nil,
		Latency:   latency,
		StartedAt: start,
	}
	if err != nil {
		p.Error = err.Error()
	}
	_ = r.metrics.RecordHTTPProbe(p)

	// The metric row carries the target's host:port like every other row, so
	// the metrics table reads consistently; the full URL lives on the probe
	// record.
	meta := core.Metadata{Network: core.NetworkTCP, StartedAt: start}
	if u, uerr := url.Parse(target); uerr == nil && u.Host != "" {
		meta.TargetHost = u.Hostname()
		meta.TargetPort = u.Port()
		if meta.TargetPort == "" {
			if u.Scheme == "https" {
				meta.TargetPort = "443"
			} else {
				meta.TargetPort = "80"
			}
		}
	}
	r.record(meta, []string{core.HopProbe, route}, err == nil, err, latency)
	return p
}

// flattenHeaders joins a repeated response header's values with ", " so one
// result row holds one value per name.
func flattenHeaders(h http.Header) map[string]string {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string]string, len(h))
	for name, values := range h {
		out[name] = strings.Join(values, ", ")
	}
	return out
}

// validTargets lives in core (core.ValidProbeTargets) so the API's config
// validation and this runner apply one rule.
