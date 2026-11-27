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
//
// Every row carries the path's phase timings and, on failure, a classified
// phase and reason (probetrace.go, errclass.go) answering which leg failed —
// local→proxy or proxy→target — and why.
package router

import (
	"compress/gzip"
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
	"golang.org/x/net/idna"
)

const (
	// probeHTTPConcurrency caps parallel HTTP probes per round, so sweeping a
	// whole fleet is a measured walk rather than a connection storm. Each job
	// gets a goroutine, but only this many hold the semaphore and dial at once;
	// the rest wait on it, and a round the budget cuts short leaves them
	// queued rather than issued.
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
	// maxProbeFanout bounds one round's (target × route) request count. This
	// is the cap that actually bounds a round's cost: the result slice is
	// sized to it, the response carries every row, and the round budget has to
	// account for it. Raising it raises the memory and response size of one
	// API call, so it stays where the round budget can still serve it.
	maxProbeFanout = 512
	// maxProbeRoutes bounds the spec's route list. It is deliberately far
	// above maxProbeFanout: naming a whole fleet (or a pool that expands to
	// one) is a legitimate thing for a caller to write down, and it is the
	// fan-out — routes × targets — that decides whether the round can run.
	// Keeping this small instead would reject a 400-outbound selection
	// against a single target, which is well within the fan-out and the
	// budget, just because the list is long.
	maxProbeRoutes = 65535
	// probeRoundSlack is added to a round's worst-case budget for dial and
	// scheduling overhead beyond the per-request timeouts.
	probeRoundSlack = 30 * time.Second
	// maxProbeRunBudget caps the derived round budget: at the allowed maxima
	// (512 jobs, 5-minute timeout) the worst case is hours, which no caller
	// should wait for. A round the cap cuts short still returns and records
	// what completed; jobs it cancels before issue are reported, not recorded.
	maxProbeRunBudget = 10 * time.Minute
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

	// Bound the round by its own worst case — every concurrency wave spending
	// the full per-request timeout — plus slack. Without a budget a round of
	// black-holing routes holds its caller (the synchronous API handler, or a
	// scheduled slot) for as long as the timeouts multiply out; with one, a
	// cut-short round still returns and records what completed.
	waves := (len(jobs) + probeHTTPConcurrency - 1) / probeHTTPConcurrency
	budget := time.Duration(waves)*timeout + probeRoundSlack
	if budget > maxProbeRunBudget {
		budget = maxProbeRunBudget
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

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
				// The round ended before this request was issued. The
				// caller's response still accounts for the job, but a
				// request that never left must not stand in history as a
				// failed probe: it would displace real rows and read as a
				// dead route.
				results[i] = assembleProbe(spec, j.target, j.route, time.Now(), probeOutcome{
					err:   fmt.Errorf("round ended before the request was issued: %w", ctx.Err()),
					stage: stageQueue,
				})
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

// probeOutcome is everything one probe request produced: the response data
// (kept even when a later step failed — a body read error must not discard
// the status and headers that arrived) plus how it failed: the stage that
// errored and the trace of the path the request took.
type probeOutcome struct {
	status    int
	header    http.Header
	body      string
	truncated bool
	err       error
	stage     string
	timings   phaseTimings
	// path is the request's observed network path, nil for a job that never
	// left (the round's budget cancelled it) — a request that was never
	// issued has no path to show.
	path *core.PathRecorder
}

// probeDialFunc dials one connection toward addr through a probe's route. It
// is the seam between the HTTP request machinery (performProbeAttempt) and
// whatever establishes the path: a single route's upstream dialer for a probe
// round, or a pinned multi-hop chain for a full-probe attempt.
type probeDialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// probeOneHTTP performs one (target, route) request and records it.
func (r *Router) probeOneHTTP(ctx context.Context, spec core.HTTPProbeSpec, target, route string, timeout time.Duration, bodyLimit int) core.HTTPProbe {
	pctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	start := time.Now()
	// The dial path is the one dispatch uses: the route's upstream dialer, or
	// the engine stack for an empty/direct route (probeTransport's old job).
	d := r.OutboundUpstreamDialer(route)
	base := r.stack.Dial
	if d != nil {
		base = d.DialUpstream
	}
	out := r.performProbeAttempt(pctx, spec, target, timeout, bodyLimit, func(ctx context.Context, network, addr string) (net.Conn, error) {
		// The transport keeps no pooled conn (DisableKeepAlives), so a redirect
		// hop is a fresh dial from the engine host rather than a continuation of
		// the path the previous response arrived on.
		core.PathRecorderFromContext(ctx).NoteLocalDial()
		return base(ctx, network, addr)
	})
	return r.probeResult(spec, target, route, start, out)
}

// performProbeAttempt issues one HTTP request toward target through dial and
// returns everything it observed: the response data, the phase trace, and the
// path recorder the route machinery drew its legs into. It records NOTHING —
// the caller decides how to persist (probeResult writes a probe row + metric
// row for a probe round; a full-probe attempt writes only a metric row on
// success). The recorder is seeded here, before the dial, so a probe that
// fails before any dial still names what it was reaching for, and every leg
// the route draws inside dial lands in out.path.
func (r *Router) performProbeAttempt(ctx context.Context, spec core.HTTPProbeSpec, target string, timeout time.Duration, bodyLimit int, dial probeDialFunc) probeOutcome {
	start := time.Now()
	tr := newProbeTrace(start)
	ctx = withProbeTrace(ctx, tr)
	rec := core.NewPathRecorder()
	rec.SetClassifier(classifyLeg)
	rec.AddLocal()
	if host, port := probeTargetHostPort(target); host != "" {
		rec.AddTarget(host, port)
	}
	ctx = core.WithPathRecorder(ctx, rec)

	req, err := newProbeRequest(ctx, spec, target)
	if err != nil {
		return probeOutcome{err: err, stage: stageRequest, timings: tr.snapshot(), path: rec}
	}

	client := &http.Client{Transport: probeTransportFor(dial, tr), Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return probeOutcome{err: err, stage: stageDo, timings: tr.snapshot(), path: rec}
	}
	defer resp.Body.Close()

	// A caller-set Accept-Encoding switches the transport's transparent
	// decode off, which leaves Content-Encoding on the response; decode here
	// so assertions and the body pane see the body the target actually sent
	// rather than the compressed bytes. When the transport decoded it, the
	// header is already gone and this branch does not fire.
	out := probeOutcome{
		status: resp.StatusCode, header: resp.Header, timings: tr.snapshot(), path: rec,
	}
	// bodyLimit <= 0 means "don't keep the body": a full-probe attempt cares
	// only that a response arrived, not what it said, so it skips the read (and
	// the gzip decode) entirely rather than buffering bytes it will discard.
	if bodyLimit <= 0 {
		return out
	}

	// A caller-set Accept-Encoding switches the transport's transparent
	// decode off, which leaves Content-Encoding on the response; decode here
	// so assertions and the body pane see the body the target actually sent
	// rather than the compressed bytes. When the transport decoded it, the
	// header is already gone and this branch does not fire.
	reader := io.Reader(resp.Body)
	if strings.EqualFold(resp.Header.Get("Content-Encoding"), "gzip") {
		gz, gerr := gzip.NewReader(resp.Body)
		if gerr != nil {
			out.err, out.stage = gerr, stageDecode
			return out
		}
		defer gz.Close()
		reader = gz
	}

	// Read one byte past the limit so truncation is a fact, not a guess.
	raw, readErr := io.ReadAll(io.LimitReader(reader, int64(bodyLimit)+1))
	body := string(raw)
	truncated := len(raw) > bodyLimit
	if truncated {
		body = string(raw[:bodyLimit])
	}
	out.body, out.truncated, out.err = body, truncated, readErr
	if readErr != nil {
		out.stage = stageBody
	}
	return out
}

// probeTransportFor builds the transport whose dials ride dial, bracketing
// each for the trace: RouteDial is the one timing every stack and protocol
// provides, and its error is what attributes a Do failure to the local→proxy
// leg rather than the proxy→target one.
//
// It draws no leg of its own. Every leg of a probe's path — including a direct
// route's single stack dial — is recorded inside the route machinery
// (dialUpstream/dialChain/dialPoolSnapshot, or dialHops for a full-probe
// attempt), which dial reaches through the ctx carrier; bracketing the dial
// here as well would draw the same legs twice. What the closure does owe the
// recorder is where this dial leaves FROM: the transport keeps no pooled conn
// (DisableKeepAlives), so a redirect hop is a fresh dial from the engine host
// rather than a continuation of the path the previous response arrived on —
// the dial func itself calls NoteLocalDial to say so.
func probeTransportFor(dial probeDialFunc, tr *probeTrace) *http.Transport {
	return &http.Transport{
		DisableKeepAlives: true, // one request per route: no pooled conn reuse
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			tr.noteRouteDialStart()
			conn, err := dial(ctx, network, addr)
			tr.noteRouteDialEnd(err)
			return conn, err
		},
	}
}

// probeTargetHostPort splits a probe target URL into the host and port the
// transport will dial: the URL's own port or its scheme's default, and the host
// in the spelling net/http dials with. An internationalized name is punycoded
// first — net/http's canonicalAddr does the same before handing the address to
// the dialer — so the recorder's target node and the address the dial reports
// name one server; seeding the Unicode spelling instead misses AddServer's
// address unification and the graph grows a duplicate origin vertex beside an
// orphaned target. An all-ASCII host is left exactly as the URL spelled it
// (net/http only punycodes a non-ASCII one), and so is a host the conversion
// rejects: net/http keeps the raw spelling there too and lets the resolver
// fail. Returns ("", "") for a URL that does not parse or names no host.
func probeTargetHostPort(rawURL string) (host, port string) {
	u, err := url.Parse(rawURL)
	if err != nil || u.Host == "" {
		return "", ""
	}
	host = u.Hostname()
	ascii := true
	for i := 0; i < len(host); i++ {
		if host[i] >= 0x80 {
			ascii = false
			break
		}
	}
	if !ascii {
		if punycoded, ierr := idna.Lookup.ToASCII(host); ierr == nil {
			host = punycoded
		}
	}
	port = u.Port()
	if port == "" {
		if u.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	return host, port
}

// newProbeRequest builds the request: spec's method (GET by default), headers
// and body, with a User-Agent so targets that reject an empty one still
// answer. A spec Host becomes req.Host — net/http writes the Host line from
// there and would silently drop it from the header map — and Content-Length
// is left to the transport.
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
		if strings.EqualFold(name, "Host") {
			req.Host = value
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
func (r *Router) probeResult(spec core.HTTPProbeSpec, target, route string, start time.Time, out probeOutcome) core.HTTPProbe {
	p := assembleProbe(spec, target, route, start, out)
	r.recordProbe(p, target, out.err, start)
	return p
}

// assembleProbe builds one result row without recording it, for jobs whose
// request never left (a round cancelled while they waited for a slot) and as
// the shared assembly step of probeResult.
func assembleProbe(spec core.HTTPProbeSpec, target, route string, start time.Time, out probeOutcome) core.HTTPProbe {
	latency := time.Since(start)
	method := strings.ToUpper(strings.TrimSpace(spec.Method))
	if method == "" {
		method = http.MethodGet
	}
	p := core.HTTPProbe{
		// Target is part of the ID: concurrent probes of the same route to
		// different targets can share a start nanosecond, and the metrics
		// key (start-ns + ID) would otherwise collapse them into one row.
		ID:        fmt.Sprintf("%d-%s-%s", start.UnixNano(), route, target),
		Route:     route,
		Target:    target,
		Method:    method,
		Status:    out.status,
		Headers:   flattenHeaders(out.header),
		Body:      out.body,
		Truncated: out.truncated,
		Success:   out.err == nil,
		Latency:   latency,
		StartedAt: start,
		RouteDial: out.timings.RouteDial,
		DNS:       out.timings.DNS,
		Connect:   out.timings.Connect,
		TLS:       out.timings.TLS,
		TTFB:      out.timings.TTFB,
		DialAddr:  out.timings.DialAddr,
		DialVia:   out.timings.DialVia,
	}
	if out.err != nil {
		p.Error = out.err.Error()
		p.Phase, p.Reason = classifyProbeFailure(out.stage, out.err, out.timings)
	}
	// Snapshot is memoized and seals the recorder, so the probe row and the
	// metric row recordProbe writes carry the identical, immutable graph.
	p.Path = out.path.Snapshot()
	return p
}

// recordProbe writes the probe row and the metric row for one result. The
// metric row carries the target's host:port like every other row, so the
// metrics table reads consistently; the full URL lives on the probe record.
func (r *Router) recordProbe(p core.HTTPProbe, target string, err error, start time.Time) {
	_ = r.metrics.RecordHTTPProbe(p)
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
	// The metric row carries the probe's own classified reason; re-running
	// the generic classifier here would lose the stage and trace the probe
	// row was built from. It carries the probe's own graph too, already
	// snapshotted — the same immutable pointer, so the two rows cannot
	// disagree about the path.
	r.recordRow(meta, []string{core.HopProbe, p.Route}, err == nil, err, p.Latency, p.Reason, p.Path)
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
