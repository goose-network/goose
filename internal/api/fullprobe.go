// Full active probing endpoints: start a multi-level sweep, watch its
// progress, cancel it, and turn its successful paths into chains + inbounds.
// The sweep itself lives in the router (internal/router/fullprobe.go); this
// file is the admin surface over it, plus the apply step that writes the
// discovered paths into the live config store.
package api

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"syscall"
	"time"

	"github.com/goose-network/goose/internal/config"
	"github.com/goose-network/goose/internal/core"
	"github.com/goose-network/goose/internal/metrics"
	"github.com/goose-network/goose/internal/selector"
)

// fullProbeRunner runs full active probing jobs (the router implements it).
type fullProbeRunner interface {
	StartFullProbe(spec core.FullProbeSpec) (*core.FullProbeJob, error)
	FullProbeJob(id string) (*core.FullProbeJob, bool)
	FullProbeJobs(n int) []*core.FullProbeJob
	CancelFullProbe(id string) bool
}

// SetFullProbeRunner wires the full active probing runner (the router).
// Optional: without it, POST /api/probes/full reports 503 and the job views
// fall back to the durable history.
func (s *Server) SetFullProbeRunner(p fullProbeRunner) { s.full = p }

// fullProbeRunRequest is the body of POST /api/probes/full. It mirrors
// core.FullProbeSpec with the API's seconds-based timeout convention; every
// numeric field is defaulted and capped by the runner.
type fullProbeRunRequest struct {
	// Target is the absolute http(s) URL every attempt requests. Required.
	Target string `json:"target"`
	// Method is the HTTP method; empty means GET.
	Method string `json:"method,omitempty"`
	// Headers are added to every request.
	Headers map[string]string `json:"headers,omitempty"`
	// Body is the request body, sent verbatim.
	Body string `json:"body,omitempty"`
	// TimeoutSec bounds one attempt. Default 10; must be 0..60 (0 = default).
	TimeoutSec int `json:"timeout_seconds,omitempty"`
	// MaxDepth is the deepest chain length to explore (1..5). Default 5.
	MaxDepth int `json:"max_depth,omitempty"`
	// RPS is the probe request rate limit (1..10000). Default 100.
	RPS int `json:"rps,omitempty"`
	// Concurrency caps in-flight attempts. Default 2×RPS (min 16, max 16384).
	Concurrency int `json:"concurrency,omitempty"`
	// MaxAttemptsPerLevel caps one level's fan-out. Default 10000.
	MaxAttemptsPerLevel int `json:"max_attempts_per_level,omitempty"`
	// MaxPathsPerLevel caps the successful paths kept as next-level prefixes
	// (fastest first). Default 256.
	MaxPathsPerLevel int `json:"max_paths_per_level,omitempty"`
	// MaxTotalAttempts caps the whole job. Default 100000.
	MaxTotalAttempts int `json:"max_total_attempts,omitempty"`
	// CatchupSeconds bounds the post-sweep settling tail that probes
	// outbounds registered after the job started (provider fleets grow for
	// minutes) at depth 1, folding them into level 1. Default 600; must be
	// -1..900 (-1 disables the tail; one final top-up pass still runs).
	CatchupSeconds int `json:"catchup_seconds,omitempty"`
}

// fullProbes dispatches /api/probes/full: POST starts a job, GET lists jobs.
func (s *Server) fullProbes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.fullProbeStart(w, r)
	case http.MethodGet:
		s.fullProbeList(w, r)
	default:
		writeJSON(w, errResponse{"method not allowed"}, http.StatusMethodNotAllowed)
	}
}

// getFullProbe godoc
//
//	@Summary      Get a full active probing job
//	@Description  Returns one job's current snapshot: spec, state (running/done/canceled/error), per-level progress and results, and the total attempt count. While the job runs the snapshot is live; after it finishes the durable history serves it.
//	@Tags         probes
//	@Produce      json
//	@Param        id path string true "Job id"
//	@Success      200 {object} core.FullProbeJob
//	@Failure      401 {object} errResponse
//	@Failure      404 {object} errResponse
//	@Router       /api/probes/full/{id} [get]
//
// cancelFullProbe godoc
//
//	@Summary      Cancel a full active probing job
//	@Description  Stops a running job's issuance; in-flight attempts finish under their own timeouts and the job lands in state "canceled" with the levels it completed. A job that already finished answers 409.
//	@Tags         probes
//	@Param        id path string true "Job id"
//	@Success      204 "No content"
//	@Failure      401 {object} errResponse
//	@Failure      404 {object} errResponse
//	@Failure      409 {object} errResponse
//	@Router       /api/probes/full/{id} [delete]
func (s *Server) fullProbeItem(w http.ResponseWriter, r *http.Request) {
	id := resourceID(r.URL.Path, "/api/probes/full")
	if rest, ok := strings.CutSuffix(id, "/results"); ok {
		if r.Method != http.MethodGet {
			writeJSON(w, errResponse{"method not allowed"}, http.StatusMethodNotAllowed)
			return
		}
		s.fullProbeResults(w, r, rest)
		return
	}
	if rest, ok := strings.CutSuffix(id, "/inbounds"); ok {
		if r.Method != http.MethodPost {
			writeJSON(w, errResponse{"method not allowed"}, http.StatusMethodNotAllowed)
			return
		}
		s.fullProbeApply(w, r, rest)
		return
	}
	if id == "" {
		writeJSON(w, errResponse{"missing id"}, http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		if j, ok := s.fullProbeJob(id); ok {
			// A job snapshot carries every level's success paths + failed
			// items — MB-scale on a working fleet, and the UI polls it every
			// 1.5s while a sweep runs. Same n×row-size class as the history
			// GETs, so it gets the extended write deadline too.
			writeListJSON(w, j)
		} else {
			writeJSON(w, errResponse{"not found"}, http.StatusNotFound)
		}
	case http.MethodDelete:
		if s.full == nil {
			writeJSON(w, errResponse{"full probing not wired"}, http.StatusServiceUnavailable)
			return
		}
		if s.full.CancelFullProbe(id) {
			w.WriteHeader(http.StatusNoContent)
		} else if _, ok := s.fullProbeJob(id); ok {
			writeJSON(w, errResponse{"job is not running"}, http.StatusConflict)
		} else {
			writeJSON(w, errResponse{"not found"}, http.StatusNotFound)
		}
	default:
		writeJSON(w, errResponse{"method not allowed"}, http.StatusMethodNotAllowed)
	}
}

// fullProbeJob reads one job: the live runner first, the durable history as
// fallback (a finished job after an engine restart).
func (s *Server) fullProbeJob(id string) (*core.FullProbeJob, bool) {
	if s.full != nil {
		if j, ok := s.full.FullProbeJob(id); ok {
			return j, true
		}
	}
	return s.ms.FullProbeJob(id)
}

// listFullProbeResults godoc
//
//	@Summary      List a full-probe job's attempt results
//	@Description  Returns up to n of one job's durable attempt rows, newest first — every issued attempt, success and failure alike, in the same model as GET /api/probes/results (each row carries full_probe_id linking it to the job and depth, the chain length it probed). Failures carry error, phase and reason plus the partial path; successes the response status and timings. Filters narrow server-side: outcome (success/failed), depth (exact chain length), reason (exact slug), route (exact hop-join "a>b"), hop (an outbound at any position of the route) and last_hop (the route's final outbound). before is a pagination cursor: pass the previous page's last row id to receive only rows strictly older than it. A known job whose rows were pruned or that issued no attempts answers 200 with an empty array; retention is reported by the job snapshot's rows_pruned flag and by comparing the loaded row count against total_attempts. An unknown job answers 404.
//	@Tags         probes
//	@Produce      json
//	@Param        id path string true "Job id"
//	@Param        n query int false "Number of rows to return (default 200, max 10000)" default(200) maximum(10000)
//	@Param        before query string false "Row id cursor: only rows strictly older than this id"
//	@Param        outcome query string false "Only rows with this outcome (success or failed)"
//	@Param        depth query int false "Only rows at this exact chain depth (>= 1)"
//	@Param        reason query string false "Only rows with this failure reason slug"
//	@Param        route query string false "Only rows with this exact hop-join route (a>b)"
//	@Param        hop query string false "Only rows whose route contains this outbound at any position"
//	@Param        last_hop query string false "Only rows whose route ends in this outbound"
//	@Success      200 {array} core.HTTPProbe
//	@Failure      400 {object} errResponse
//	@Failure      401 {object} errResponse
//	@Failure      404 {object} errResponse
//	@Failure      405 {object} errResponse
//	@Failure      500 {object} errResponse
//	@Router       /api/probes/full/{id}/results [get]
func (s *Server) fullProbeResults(w http.ResponseWriter, r *http.Request, id string) {
	if _, ok := s.fullProbeJob(id); !ok {
		writeJSON(w, errResponse{"not found"}, http.StatusNotFound)
		return
	}
	q := r.URL.Query()
	n := 200
	if v := q.Get("n"); v != "" {
		parsed, err := parseInt(v)
		if err != nil {
			writeJSON(w, errResponse{"bad n: " + err.Error()}, http.StatusBadRequest)
			return
		}
		if parsed > 0 {
			n = parsed
		}
	}
	if n > maxMetricsRows {
		n = maxMetricsRows
	}
	f := metrics.FullProbeResultFilter{
		Outcome: q.Get("outcome"),
		Reason:  q.Get("reason"),
		Route:   q.Get("route"),
		Hop:     q.Get("hop"),
		LastHop: q.Get("last_hop"),
	}
	if v := q.Get("depth"); v != "" {
		parsed, err := parseInt(v)
		if err != nil {
			writeJSON(w, errResponse{"bad depth: " + err.Error()}, http.StatusBadRequest)
			return
		}
		f.Depth = parsed
	}
	rows, err := s.ms.FullProbeResults(id, n, q.Get("before"), f)
	if err != nil {
		writeJSON(w, errResponse{err.Error()}, http.StatusInternalServerError)
		return
	}
	writeListJSON(w, rows)
}

// startFullProbe godoc
//
//	@Summary      Start a full active probing job
//	@Description  Launches a multi-level sweep that discovers which proxy paths reach the target: level 1 probes local → every outbound (successes A, failures B); level 2 probes local → a → b for every a ∈ A, b ∈ B (successes C, failures D); level 3 extends the surviving paths with the new failures, and so on up to max_depth (5). Every attempt is a real HTTP request through the pinned path, so a success proves the whole chain proxies. Provider fleets keep growing for minutes after a start (server lists download in bursts), so after the sweep a bounded settling tail keeps probing outbounds registered late at depth 1 — folding them into level 1 — until the fleet has been quiet for a while or catchup_seconds expires (default 600, max 900; -1 disables the tail, one final top-up pass still runs; engines without providers take only that final pass). Late arrivals are measured at depth 1 only. The job runs detached; poll GET /api/probes/full/{id} for per-level progress and success paths, then POST /api/probes/full/{id}/inbounds to turn them into chains + inbounds. rps caps the probe request rate (up to 10000); concurrency, max_attempts_per_level, max_paths_per_level (the beam of fastest paths kept per level) and max_total_attempts bound the fan-out. At most two sweeps run at once; a start beyond that answers 409 until one finishes.
//	@Tags         probes
//	@Accept       json
//	@Produce      json
//	@Param        spec body fullProbeRunRequest true "The sweep to run"
//	@Success      201 {object} core.FullProbeJob
//	@Failure      400 {object} errResponse
//	@Failure      401 {object} errResponse
//	@Failure      405 {object} errResponse
//	@Failure      409 {object} errResponse
//	@Failure      503 {object} errResponse
//	@Router       /api/probes/full [post]
func (s *Server) fullProbeStart(w http.ResponseWriter, r *http.Request) {
	if s.full == nil {
		writeJSON(w, errResponse{"full probing not wired"}, http.StatusServiceUnavailable)
		return
	}
	var req fullProbeRunRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, errResponse{err.Error()}, http.StatusBadRequest)
		return
	}
	// Bound the seconds before scaling to nanoseconds: an unbounded int
	// times time.Second wraps int64, and a wrapped positive value under 60s
	// would slip past NormalizeFullProbeSpec's cap as a tiny timeout.
	if req.TimeoutSec < 0 || req.TimeoutSec > int(core.FullProbeMaxTimeout/time.Second) {
		writeJSON(w, errResponse{"timeout_seconds out of range"}, http.StatusBadRequest)
		return
	}
	// -1 is the explicit tail opt-out; the runner normalizes 0 to the
	// default bound and clamps anything above the cap.
	if req.CatchupSeconds < -1 || req.CatchupSeconds > core.FullProbeMaxCatchupSeconds {
		writeJSON(w, errResponse{"catchup_seconds out of range"}, http.StatusBadRequest)
		return
	}
	spec := core.FullProbeSpec{
		Target:              req.Target,
		Method:              req.Method,
		Headers:             req.Headers,
		Body:                req.Body,
		Timeout:             time.Duration(req.TimeoutSec) * time.Second,
		MaxDepth:            req.MaxDepth,
		RPS:                 req.RPS,
		Concurrency:         req.Concurrency,
		MaxAttemptsPerLevel: req.MaxAttemptsPerLevel,
		MaxPathsPerLevel:    req.MaxPathsPerLevel,
		MaxTotalAttempts:    req.MaxTotalAttempts,
		CatchupSeconds:      req.CatchupSeconds,
	}
	j, err := s.full.StartFullProbe(spec)
	if err != nil {
		// Too many running jobs is a transient conflict, not a bad spec:
		// retrying the same body once one finishes succeeds.
		if errors.Is(err, core.ErrFullProbeCapacity) {
			writeJSON(w, errResponse{err.Error()}, http.StatusConflict)
			return
		}
		writeJSON(w, errResponse{err.Error()}, http.StatusBadRequest)
		return
	}
	writeJSON(w, j, http.StatusCreated)
}

// listFullProbes godoc
//
//	@Summary      List full active probing jobs
//	@Description  Returns up to n full-probe job snapshots, newest first: live jobs with their current per-level progress, plus the durable history of finished jobs. Each level carries its depth, planned/finished/success/failed counts, the successful paths (ordered outbound hop lists with latency and status, fastest first) and the failed items aggregated by last-hop outbound.
//	@Tags         probes
//	@Produce      json
//	@Param        n query int false "Number of jobs to return (default 20, max 64)" default(20) maximum(64)
//	@Success      200 {array} core.FullProbeJob
//	@Failure      401 {object} errResponse
//	@Failure      405 {object} errResponse
//	@Router       /api/probes/full [get]
func (s *Server) fullProbeList(w http.ResponseWriter, r *http.Request) {
	n := 20
	if v := r.URL.Query().Get("n"); v != "" {
		if parsed, err := parseInt(v); err == nil && parsed > 0 {
			n = parsed
		}
	}
	if n > 64 {
		n = 64
	}
	var out []*core.FullProbeJob
	if s.full != nil {
		out = s.full.FullProbeJobs(n)
	} else if persisted, err := s.ms.RecentFullProbeJobs(n); err == nil {
		out = persisted
	}
	if out == nil {
		out = []*core.FullProbeJob{}
	}
	// Up to 64 full snapshots, each MB-scale on a working fleet — tens of MB
	// bounded only by n × snapshot-size, the writeListJSON class.
	writeListJSON(w, out)
}

// fullProbeApplyResponse is the body of POST /api/probes/full/{id}/inbounds.
type fullProbeApplyResponse struct {
	// JobID is the job the paths came from.
	JobID string `json:"job_id"`
	// Results are the generated chain + inbound pairs, one per applied path.
	// In pool mode each entry names a member chain (InboundID/Listen/Protocol
	// are empty — the one listener is described by the fields below).
	Results []core.FullProbeApplyResult `json:"results"`
	// PoolID is the generated pool (pool mode only).
	PoolID string `json:"pool_id,omitempty"`
	// InboundID is the single generated inbound routed through the pool
	// (pool mode only).
	InboundID string `json:"inbound_id,omitempty"`
	// Listen is that inbound's listen address (pool mode only).
	Listen string `json:"listen,omitempty"`
	// Protocol is that inbound's listener protocol (pool mode only).
	Protocol string `json:"protocol,omitempty"`
	// Selector is the pool's selection strategy (pool mode only).
	Selector *core.FullProbeSelectorSpec `json:"selector,omitempty"`
}

// applyFullProbe godoc
//
//	@Summary      Create inbounds from a job's successful paths
//	@Description  Turns the job's successful paths into chains + inbounds in the live config: one chain per path (its hops as pinned outbound layers) and one inbound listener routed through it. With no body (or no paths given) the fastest successful path per depth is applied — local→A, local→A→C, local→A→C→E, ...; naming paths explicitly applies exactly those (each must be one of the job's successful paths, at most 64 per call). Every hop must still exist as an outbound and must not be shadowed by a pool of the same name (routing resolves pools first); in pool mode pool_id must also not be referenced as a layer by an existing chain (a path-pool cannot be a chain layer). protocol defaults to socks5, listen_prefix to 127.0.0.1; port names the first port (each further inbound takes the next, and must be bindable), and port 0 picks a free port per inbound. mode "pool" instead builds ONE pool load-balancing over the proved paths — every path becomes a member chain (chain_ids), and a single inbound (id = id_prefix) routes through the pool. Pool-mode defaults: ALL successful paths deduped fastest-first (cap 256 members; explicit paths override, same cap), pool_id = "<id_prefix>-pool", selector roundrobin, one port. The response then carries pool_id/inbound_id/listen/protocol/selector at the top level and one results entry per member chain (inbound fields empty). Changes apply live: the engine starts the listeners on the next config version bump.
//	@Tags         probes
//	@Accept       json
//	@Produce      json
//	@Param        id path string true "Job id"
//	@Param        spec body core.FullProbeApplySpec false "Which paths to apply and how to name/listen the generated inbounds"
//	@Success      201 {object} fullProbeApplyResponse
//	@Failure      400 {object} errResponse
//	@Failure      401 {object} errResponse
//	@Failure      404 {object} errResponse
//	@Failure      409 {object} errResponse
//	@Router       /api/probes/full/{id}/inbounds [post]
func (s *Server) fullProbeApply(w http.ResponseWriter, r *http.Request, id string) {
	j, ok := s.fullProbeJob(id)
	if !ok {
		writeJSON(w, errResponse{"not found"}, http.StatusNotFound)
		return
	}
	if j.State == core.FullProbeRunning {
		writeJSON(w, errResponse{"job is still running; wait for it to finish or cancel it first"}, http.StatusConflict)
		return
	}
	spec, err := readOptionalSpec[core.FullProbeApplySpec](r)
	if err != nil {
		writeJSON(w, errResponse{err.Error()}, http.StatusBadRequest)
		return
	}
	if spec == nil {
		spec = &core.FullProbeApplySpec{}
	}
	switch spec.Mode {
	case "", "inbounds":
		// The shipped one-inbound-per-path flow below, unchanged.
	case "pool":
		s.fullProbeApplyPool(w, j, spec)
		return
	default:
		writeJSON(w, errResponse{`mode must be "inbounds" or "pool"`}, http.StatusBadRequest)
		return
	}
	// Serialize validate+commit with every other config write (Server.writeMu):
	// the hop-existence, shadow, and id-collision scans below must not race a
	// concurrent write landing before SetChain/SetInbound. Acquired only on the
	// inbounds path — pool mode returned above and locks in fullProbeApplyPool
	// instead, so each request takes the mutex exactly once. The job lookup and
	// spec parse above touch no config writes and stay outside the lock.
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if len(spec.Paths) > core.FullProbeMaxApplyPaths {
		writeJSON(w, errResponse{fmt.Sprintf("too many paths (%d); at most %d inbounds can be created per apply", len(spec.Paths), core.FullProbeMaxApplyPaths)}, http.StatusBadRequest)
		return
	}
	paths := j.BestPaths()
	if len(spec.Paths) > 0 {
		// Explicit selection: every named path must be one the job proved.
		proved := map[string]core.FullProbePath{}
		for _, l := range j.Levels {
			for _, p := range l.SuccessPaths {
				proved[strings.Join(p.Hops, ">")] = p
			}
		}
		paths = nil
		for _, hops := range spec.Paths {
			p, ok := proved[strings.Join(hops, ">")]
			if !ok {
				writeJSON(w, errResponse{fmt.Sprintf("path %s is not a successful path of this job", strings.Join(hops, " → "))}, http.StatusBadRequest)
				return
			}
			paths = append(paths, p)
		}
	}
	if len(paths) == 0 {
		writeJSON(w, errResponse{"job has no successful paths to apply"}, http.StatusBadRequest)
		return
	}
	protocol := spec.Protocol
	if protocol == "" {
		protocol = "socks5"
	}
	if protocol != "socks5" && protocol != "http" {
		writeJSON(w, errResponse{"protocol must be http or socks5"}, http.StatusBadRequest)
		return
	}
	host := spec.ListenPrefix
	if host == "" {
		host = "127.0.0.1"
	}
	if spec.Port < 0 || spec.Port > 65535 {
		writeJSON(w, errResponse{"port out of range"}, http.StatusBadRequest)
		return
	}
	prefix := spec.IDPrefix
	if prefix == "" {
		prefix = "fullprobe-" + j.ID
	}

	// Validate everything before writing anything: hops must still exist
	// as outbounds (a pool of the same name would shadow the hop — routing
	// resolves pools first, rerouting traffic off the proved path), the
	// generated IDs must not collide with live config, and every listen
	// address must be free and unique.
	type planned struct {
		chainID, inboundID, listen string
		path                       core.FullProbePath
	}
	var plans []planned
	used := map[string]bool{}
	listens := map[string]bool{}
	for i, p := range paths {
		if len(p.Hops) == 0 || len(p.Hops) > core.FullProbeMaxDepth {
			writeJSON(w, errResponse{"path depth out of range"}, http.StatusBadRequest)
			return
		}
		for _, hop := range p.Hops {
			if _, ok := s.cfg.Outbound(hop); !ok {
				writeJSON(w, errResponse{fmt.Sprintf("outbound %q no longer exists", hop)}, http.StatusBadRequest)
				return
			}
			if _, ok := s.cfg.Pool(hop); ok {
				writeJSON(w, errResponse{fmt.Sprintf("hop %q is also a pool; routing would resolve the pool and reroute traffic off the proved path — rename the pool or the outbound", hop)}, http.StatusBadRequest)
				return
			}
		}
		chainID := fmt.Sprintf("%s-d%d-%d", prefix, len(p.Hops), i+1)
		if _, exists := s.cfg.Chain(chainID); exists || used[chainID] {
			writeJSON(w, errResponse{fmt.Sprintf("chain %q already exists; choose another id_prefix", chainID)}, http.StatusConflict)
			return
		}
		if _, exists := s.cfg.Inbound(chainID); exists {
			writeJSON(w, errResponse{fmt.Sprintf("inbound %q already exists; choose another id_prefix", chainID)}, http.StatusConflict)
			return
		}
		used[chainID] = true
		port := spec.Port
		if port > 0 {
			port += i
			if port > 65535 {
				writeJSON(w, errResponse{"port range exhausted"}, http.StatusBadRequest)
				return
			}
			// An explicit port must be bindable now: the engine's
			// reconcile logs and skips a listener that cannot bind, so a
			// collision here would silently produce a dead inbound.
			if err := portFree(host, port); err != nil {
				// Only an address-in-use collision is a transient conflict
				// (409 — retryable once the other listener goes away).
				// EACCES on a privileged port or a non-local listen address
				// is a permanent bad request, matching the auto-port branch.
				status := http.StatusBadRequest
				if errors.Is(err, syscall.EADDRINUSE) {
					status = http.StatusConflict
				}
				writeJSON(w, errResponse{fmt.Sprintf("port %d: %v", port, err)}, status)
				return
			}
		} else {
			// freePort closes its probe listener before returning, so the
			// OS may hand out the same port twice in one apply; re-roll
			// until every generated inbound has a distinct port.
			var fp int
			for tries := 0; ; tries++ {
				var perr error
				fp, perr = freePort(host)
				if perr != nil {
					writeJSON(w, errResponse{perr.Error()}, http.StatusBadRequest)
					return
				}
				if !listens[net.JoinHostPort(host, fmt.Sprintf("%d", fp))] {
					break
				}
				if tries >= 16 {
					writeJSON(w, errResponse{"cannot find enough distinct free ports"}, http.StatusConflict)
					return
				}
			}
			port = fp
		}
		listen := net.JoinHostPort(host, fmt.Sprintf("%d", port))
		if listens[listen] {
			writeJSON(w, errResponse{fmt.Sprintf("listen %s assigned twice", listen)}, http.StatusConflict)
			return
		}
		listens[listen] = true
		plans = append(plans, planned{
			chainID:   chainID,
			inboundID: chainID,
			listen:    listen,
			path:      p,
		})
	}
	results := make([]core.FullProbeApplyResult, 0, len(plans))
	for _, pl := range plans {
		s.cfg.SetChain(&config.ChainSpec{ID: pl.chainID, Layers: pl.path.Hops})
		s.cfg.SetInbound(&config.Inbound{
			ID:       pl.inboundID,
			Protocol: protocol,
			Listen:   pl.listen,
			Policy:   config.InboundPolicy{ChainID: pl.chainID},
		})
		results = append(results, core.FullProbeApplyResult{
			ChainID:   pl.chainID,
			InboundID: pl.inboundID,
			Listen:    pl.listen,
			Protocol:  protocol,
			Path:      pl.path,
		})
	}
	writeJSON(w, fullProbeApplyResponse{JobID: j.ID, Results: results}, http.StatusCreated)
}

// fullProbeApplyPool is the pool-mode apply: every selected path becomes a
// member chain, the chains become one pool's chain_ids, and a single inbound
// routes through that pool. Validation runs to completion before the first
// config write, exactly like the inbounds flow; a failure mid-write rolls
// back the chains this call created (best effort — the pool/inbound writes
// cannot fail once validation passed).
func (s *Server) fullProbeApplyPool(w http.ResponseWriter, j *core.FullProbeJob, spec *core.FullProbeApplySpec) {
	// Sole writeMu acquisition for a pool-mode apply (fullProbeApply returns
	// into here before its own lock point): the reverse-reference scan and
	// the chain→pool→inbound writes must be one validate+commit atomic
	// against concurrent admin writes — a chain naming pool_id landing
	// between the scan and SetPool would defeat the write-time guard.
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if len(spec.Paths) > core.FullProbeMaxApplyPoolMembers {
		writeJSON(w, errResponse{fmt.Sprintf("too many paths (%d); at most %d pool members can be created per apply", len(spec.Paths), core.FullProbeMaxApplyPoolMembers)}, http.StatusBadRequest)
		return
	}
	paths := j.AllPathsFastest()
	if len(paths) > core.FullProbeMaxApplyPoolMembers {
		paths = paths[:core.FullProbeMaxApplyPoolMembers]
	}
	if len(spec.Paths) > 0 {
		// Explicit selection: every named path must be one the job proved.
		proved := map[string]core.FullProbePath{}
		for _, l := range j.Levels {
			for _, p := range l.SuccessPaths {
				proved[strings.Join(p.Hops, ">")] = p
			}
		}
		paths = nil
		for _, hops := range spec.Paths {
			p, ok := proved[strings.Join(hops, ">")]
			if !ok {
				writeJSON(w, errResponse{fmt.Sprintf("path %s is not a successful path of this job", strings.Join(hops, " → "))}, http.StatusBadRequest)
				return
			}
			paths = append(paths, p)
		}
	}
	if len(paths) == 0 {
		writeJSON(w, errResponse{"job has no successful paths to apply"}, http.StatusBadRequest)
		return
	}
	protocol := spec.Protocol
	if protocol == "" {
		protocol = "socks5"
	}
	if protocol != "socks5" && protocol != "http" {
		writeJSON(w, errResponse{"protocol must be http or socks5"}, http.StatusBadRequest)
		return
	}
	host := spec.ListenPrefix
	if host == "" {
		host = "127.0.0.1"
	}
	if spec.Port < 0 || spec.Port > 65535 {
		writeJSON(w, errResponse{"port out of range"}, http.StatusBadRequest)
		return
	}
	prefix := spec.IDPrefix
	if prefix == "" {
		prefix = "fullprobe-" + j.ID
	}
	poolID := spec.PoolID
	if poolID == "" {
		poolID = prefix + "-pool"
	}
	sel := spec.Selector
	if sel == nil {
		sel = &core.FullProbeSelectorSpec{Type: "roundrobin"}
	}
	if _, err := selector.New(sel.Type, sel.Params); err != nil {
		writeJSON(w, errResponse{err.Error()}, http.StatusBadRequest)
		return
	}
	if _, exists := s.cfg.Pool(poolID); exists {
		writeJSON(w, errResponse{fmt.Sprintf("pool %q already exists; choose another pool_id or id_prefix", poolID)}, http.StatusConflict)
		return
	}
	if _, exists := s.cfg.Outbound(poolID); exists {
		writeJSON(w, errResponse{fmt.Sprintf("pool_id %q is also an outbound; routing would resolve the pool and shadow the outbound — rename one of them", poolID)}, http.StatusBadRequest)
		return
	}
	// Reverse reference scan, mirroring the pools write guard: an EXISTING
	// chain may name pool_id as a layer (legal while no such pool exists —
	// the layer resolves as an outbound or dangles). Creating a path-pool
	// under that name would make rebuild skip the referencing chain whole,
	// silently rerouting its traffic to the fallback. The member chains
	// generated below reference hops, never poolID (the hop==poolID guard
	// covers that), so this scan only ever hits pre-existing chains.
	if ref, ok := s.chainLayerReferencer(poolID); ok {
		writeJSON(w, errResponse{fmt.Sprintf("pool_id %q is referenced as a layer by chain %q; a pool with chain members cannot be a chain layer — remove that reference or choose another pool_id", poolID, ref)}, http.StatusBadRequest)
		return
	}
	if _, exists := s.cfg.Inbound(prefix); exists {
		writeJSON(w, errResponse{fmt.Sprintf("inbound %q already exists; choose another id_prefix", prefix)}, http.StatusConflict)
		return
	}

	// Validate every member chain before writing anything, with the same
	// guards the inbounds flow applies per path.
	type planned struct {
		chainID string
		path    core.FullProbePath
	}
	var plans []planned
	used := map[string]bool{}
	for i, p := range paths {
		if len(p.Hops) == 0 || len(p.Hops) > core.FullProbeMaxDepth {
			writeJSON(w, errResponse{"path depth out of range"}, http.StatusBadRequest)
			return
		}
		for _, hop := range p.Hops {
			if _, ok := s.cfg.Outbound(hop); !ok {
				writeJSON(w, errResponse{fmt.Sprintf("outbound %q no longer exists", hop)}, http.StatusBadRequest)
				return
			}
			if _, ok := s.cfg.Pool(hop); ok {
				writeJSON(w, errResponse{fmt.Sprintf("hop %q is also a pool; routing would resolve the pool and reroute traffic off the proved path — rename the pool or the outbound", hop)}, http.StatusBadRequest)
				return
			}
			if hop == poolID {
				writeJSON(w, errResponse{fmt.Sprintf("pool_id %q names hop outbound %q; the pool would shadow it — choose another pool_id", poolID, hop)}, http.StatusBadRequest)
				return
			}
		}
		chainID := fmt.Sprintf("%s-d%d-%d", prefix, len(p.Hops), i+1)
		if _, exists := s.cfg.Chain(chainID); exists || used[chainID] {
			writeJSON(w, errResponse{fmt.Sprintf("chain %q already exists; choose another id_prefix", chainID)}, http.StatusConflict)
			return
		}
		if _, exists := s.cfg.Inbound(chainID); exists {
			writeJSON(w, errResponse{fmt.Sprintf("inbound %q already exists; choose another id_prefix", chainID)}, http.StatusConflict)
			return
		}
		if chainID == poolID {
			writeJSON(w, errResponse{fmt.Sprintf("pool_id %q collides with generated chain %q — choose another pool_id", poolID, chainID)}, http.StatusBadRequest)
			return
		}
		used[chainID] = true
		plans = append(plans, planned{chainID: chainID, path: p})
	}

	// One listener: the same port rules as a single-inbound apply.
	port := spec.Port
	if port > 0 {
		if err := portFree(host, port); err != nil {
			status := http.StatusBadRequest
			if errors.Is(err, syscall.EADDRINUSE) {
				status = http.StatusConflict
			}
			writeJSON(w, errResponse{fmt.Sprintf("port %d: %v", port, err)}, status)
			return
		}
	} else {
		fp, perr := freePort(host)
		if perr != nil {
			writeJSON(w, errResponse{perr.Error()}, http.StatusBadRequest)
			return
		}
		port = fp
	}
	listen := net.JoinHostPort(host, fmt.Sprintf("%d", port))

	// Write chains → pool → inbound. The store writes cannot fail, so once
	// validation has passed there is no mid-sequence failure point; were one
	// added between these writes, only the chain IDs created here (written)
	// may be rolled back — never pre-existing config.
	results := make([]core.FullProbeApplyResult, 0, len(plans))
	var written []string
	for _, pl := range plans {
		s.cfg.SetChain(&config.ChainSpec{ID: pl.chainID, Layers: pl.path.Hops})
		written = append(written, pl.chainID)
		results = append(results, core.FullProbeApplyResult{
			ChainID: pl.chainID,
			Path:    pl.path,
		})
	}
	s.cfg.SetPool(&config.Pool{
		ID:       poolID,
		ChainIDs: written,
		Selector: config.SelectorSpec{Type: sel.Type, Params: sel.Params},
	})
	s.cfg.SetInbound(&config.Inbound{
		ID:       prefix,
		Protocol: protocol,
		Listen:   listen,
		Policy:   config.InboundPolicy{PoolID: poolID},
	})
	writeJSON(w, fullProbeApplyResponse{
		JobID:     j.ID,
		Results:   results,
		PoolID:    poolID,
		InboundID: prefix,
		Listen:    listen,
		Protocol:  protocol,
		Selector:  sel,
	}, http.StatusCreated)
}

// freePort asks the OS for a free TCP port on host by opening and closing a
// listener, so the generated inbound config names a concrete port. The
// usual race (another process grabbing it before the engine listens) is
// accepted: the engine's reconcile logs a listener that cannot bind.
func freePort(host string) (int, error) {
	ln, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
	if err != nil {
		return 0, fmt.Errorf("cannot find a free port on %s: %w", host, err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port, nil
}

// portFree reports whether host:port can be bound right now, by opening and
// closing a listener on it. Used for explicitly requested ports, where a
// collision with a live listener must be a refusal, not a silent dead
// inbound.
func portFree(host string, port int) error {
	ln, err := net.Listen("tcp", net.JoinHostPort(host, fmt.Sprintf("%d", port)))
	if err != nil {
		return fmt.Errorf("cannot bind %s:%d: %w", host, port, err)
	}
	return ln.Close()
}
