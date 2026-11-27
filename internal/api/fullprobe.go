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
			writeJSON(w, j, http.StatusOK)
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

// startFullProbe godoc
//
//	@Summary      Start a full active probing job
//	@Description  Launches a multi-level sweep that discovers which proxy paths reach the target: level 1 probes local → every outbound (successes A, failures B); level 2 probes local → a → b for every a ∈ A, b ∈ B (successes C, failures D); level 3 extends the surviving paths with the new failures, and so on up to max_depth (5). Every attempt is a real HTTP request through the pinned path, so a success proves the whole chain proxies. The job runs detached; poll GET /api/probes/full/{id} for per-level progress and success paths, then POST /api/probes/full/{id}/inbounds to turn them into chains + inbounds. rps caps the probe request rate (up to 10000); concurrency, max_attempts_per_level, max_paths_per_level (the beam of fastest paths kept per level) and max_total_attempts bound the fan-out. At most two sweeps run at once; a start beyond that answers 409 until one finishes.
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
	writeJSON(w, out, http.StatusOK)
}

// fullProbeApplyResponse is the body of POST /api/probes/full/{id}/inbounds.
type fullProbeApplyResponse struct {
	// JobID is the job the paths came from.
	JobID string `json:"job_id"`
	// Results are the generated chain + inbound pairs, one per applied path.
	Results []core.FullProbeApplyResult `json:"results"`
}

// applyFullProbe godoc
//
//	@Summary      Create inbounds from a job's successful paths
//	@Description  Turns the job's successful paths into chains + inbounds in the live config: one chain per path (its hops as pinned outbound layers) and one inbound listener routed through it. With no body (or no paths given) the fastest successful path per depth is applied — local→A, local→A→C, local→A→C→E, ...; naming paths explicitly applies exactly those (each must be one of the job's successful paths, at most 64 per call). Every hop must still exist as an outbound and must not be shadowed by a pool of the same name (routing resolves pools first). protocol defaults to socks5, listen_prefix to 127.0.0.1; port names the first port (each further inbound takes the next, and must be bindable), and port 0 picks a free port per inbound. Changes apply live: the engine starts the listeners on the next config version bump.
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
