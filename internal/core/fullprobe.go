// Full active probing: a multi-level sweep that discovers which proxy paths
// actually reach a target, then hands the successful paths to inbound
// creation.
//
// The workflow one job runs:
//
//  1. probe local → every outbound toward the target; the successes are A,
//     the failures B;
//  2. probe local → a → b for every a ∈ A, b ∈ B (a failed outbound relayed
//     through a working one); successes C, failures D;
//  3. probe local → a → c → d for every successful path (a,c) ∈ C and every
//     d ∈ D; successes E, failures F;
//  4. repeat until MaxDepth (at most 5) or a level yields no successful path
//     to extend or no failed item to try;
//  5. the successful paths (local→A, local→A→C, local→A→C→E, ...) are what
//     POST /api/probes/full/{id}/inbounds turns into chains + inbounds.
//
// Every attempt is a real HTTP request toward the target through the pinned
// path — the same dial machinery dispatch uses (dialHops), so a success
// proves the whole chain proxies, not just that a server answers TCP.
package core

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// ErrFullProbeCapacity marks a start rejected because too many jobs are
// already running — a transient conflict (retry when one finishes), not a
// bad spec, so the API answers 409 rather than 400.
var ErrFullProbeCapacity = errors.New("full probe: too many jobs already running")

// Full probe job states.
const (
	FullProbeRunning  = "running"
	FullProbeDone     = "done"
	FullProbeCanceled = "canceled"
	FullProbeError    = "error"
)

// Full probe limits. The sweep is a fan-out search — level k tries every
// surviving prefix against every failed item — so the caps, not the depth,
// are what bound its cost. The rate limit is the throughput knob: a job may
// issue up to FullProbeMaxRPS probe requests per second.
const (
	// FullProbeMaxDepth caps the chain length a job explores (local + 5 hops).
	FullProbeMaxDepth = 5
	// FullProbeMaxRPS caps a job's probe request rate.
	FullProbeMaxRPS = 10000
	// FullProbeMaxConcurrency caps a job's in-flight probe dials. Sustaining
	// RPS r at latency L needs r×L in flight; this is the ceiling on that.
	FullProbeMaxConcurrency = 16384
	// FullProbeMaxAttemptsPerLevel caps one level's fan-out (prefixes ×
	// failed items, after cycle skipping).
	FullProbeMaxAttemptsPerLevel = 100000
	// FullProbeMaxPathsPerLevel caps how many successful paths a level keeps
	// (the fastest ones) as prefixes for the next level — the beam width.
	FullProbeMaxPathsPerLevel = 4096
	// FullProbeMaxTotalAttempts caps a whole job's attempt count.
	FullProbeMaxTotalAttempts = 500000
	// FullProbeMaxTimeout caps one attempt's request timeout.
	FullProbeMaxTimeout = 60 * time.Second
	// FullProbeMaxRunningJobs caps concurrently running jobs per engine.
	FullProbeMaxRunningJobs = 2
	// FullProbeMaxApplyPaths caps how many paths one apply call turns into
	// chains + inbounds. Each path becomes a live listener; an unbounded
	// explicit selection could mint thousands of them in one request.
	FullProbeMaxApplyPaths = 64
	// FullProbeMaxApplyPoolMembers caps how many proved paths one pool-mode
	// apply call turns into pool member chains. Members share a single
	// inbound listener, so the ceiling is on selection breadth, not ports.
	FullProbeMaxApplyPoolMembers = 256

	// Defaults for an omitted spec field.
	DefaultFullProbeDepth            = 5
	DefaultFullProbeRPS              = 100
	DefaultFullProbeTimeout          = 10 * time.Second
	DefaultFullProbeAttemptsPerLevel = 10000
	DefaultFullProbePathsPerLevel    = 256
	DefaultFullProbeTotalAttempts    = 100000
)

// FullProbeSpec describes one full active probing job. The zero value probes
// every built outbound toward Target at the default cadence; every numeric
// field is defaulted and clamped by NormalizeFullProbeSpec.
type FullProbeSpec struct {
	// Target is the absolute http(s) URL every attempt requests. Required.
	Target string `json:"target"`
	// Method is the HTTP method; empty means GET.
	Method string `json:"method,omitempty"`
	// Headers are added to every request.
	Headers map[string]string `json:"headers,omitempty"`
	// Body is the request body, sent verbatim.
	Body string `json:"body,omitempty"`
	// Timeout bounds one attempt. Zero uses DefaultFullProbeTimeout; capped
	// at FullProbeMaxTimeout.
	Timeout time.Duration `json:"timeout,omitempty"`
	// MaxDepth is the deepest chain length to explore (1..FullProbeMaxDepth).
	// Zero uses DefaultFullProbeDepth.
	MaxDepth int `json:"max_depth,omitempty"`
	// RPS is the probe request rate limit (1..FullProbeMaxRPS). Zero uses
	// DefaultFullProbeRPS.
	RPS int `json:"rps,omitempty"`
	// Concurrency caps in-flight attempts. Zero derives it from RPS
	// (2×RPS, clamped to 16..FullProbeMaxConcurrency).
	Concurrency int `json:"concurrency,omitempty"`
	// MaxAttemptsPerLevel caps one level's fan-out. Zero uses
	// DefaultFullProbeAttemptsPerLevel.
	MaxAttemptsPerLevel int `json:"max_attempts_per_level,omitempty"`
	// MaxPathsPerLevel caps the successful paths kept as next-level prefixes
	// (fastest first). Zero uses DefaultFullProbePathsPerLevel.
	MaxPathsPerLevel int `json:"max_paths_per_level,omitempty"`
	// MaxTotalAttempts caps the whole job. Zero uses
	// DefaultFullProbeTotalAttempts.
	MaxTotalAttempts int `json:"max_total_attempts,omitempty"`
}

// NormalizeFullProbeSpec validates the target and applies defaults and caps,
// returning the effective spec. It is the one rule the API and the runner
// both apply, so a spec that passes validation always runs.
func NormalizeFullProbeSpec(spec FullProbeSpec) (FullProbeSpec, error) {
	targets, err := ValidProbeTargets([]string{spec.Target})
	if err != nil {
		return spec, fmt.Errorf("full probe: %w", err)
	}
	spec.Target = targets[0]
	if spec.MaxDepth <= 0 {
		spec.MaxDepth = DefaultFullProbeDepth
	}
	if spec.MaxDepth > FullProbeMaxDepth {
		spec.MaxDepth = FullProbeMaxDepth
	}
	if spec.RPS <= 0 {
		spec.RPS = DefaultFullProbeRPS
	}
	if spec.RPS > FullProbeMaxRPS {
		spec.RPS = FullProbeMaxRPS
	}
	if spec.Timeout <= 0 {
		spec.Timeout = DefaultFullProbeTimeout
	}
	if spec.Timeout > FullProbeMaxTimeout {
		spec.Timeout = FullProbeMaxTimeout
	}
	if spec.Concurrency <= 0 {
		spec.Concurrency = 2 * spec.RPS
		if spec.Concurrency < 16 {
			spec.Concurrency = 16
		}
	}
	if spec.Concurrency > FullProbeMaxConcurrency {
		spec.Concurrency = FullProbeMaxConcurrency
	}
	if spec.MaxAttemptsPerLevel <= 0 {
		spec.MaxAttemptsPerLevel = DefaultFullProbeAttemptsPerLevel
	}
	if spec.MaxAttemptsPerLevel > FullProbeMaxAttemptsPerLevel {
		spec.MaxAttemptsPerLevel = FullProbeMaxAttemptsPerLevel
	}
	if spec.MaxPathsPerLevel <= 0 {
		spec.MaxPathsPerLevel = DefaultFullProbePathsPerLevel
	}
	if spec.MaxPathsPerLevel > FullProbeMaxPathsPerLevel {
		spec.MaxPathsPerLevel = FullProbeMaxPathsPerLevel
	}
	if spec.MaxTotalAttempts <= 0 {
		spec.MaxTotalAttempts = DefaultFullProbeTotalAttempts
	}
	if spec.MaxTotalAttempts > FullProbeMaxTotalAttempts {
		spec.MaxTotalAttempts = FullProbeMaxTotalAttempts
	}
	return spec, nil
}

// FullProbePath is one successful path: the ordered outbound IDs of a chain
// that proxied a request to the target, and how long that took.
type FullProbePath struct {
	// Hops are the outbound IDs in dial order; the engine host ("local") is
	// the implicit first node and the target the implicit last.
	Hops []string `json:"hops"`
	// Latency is the successful attempt's whole-request span.
	Latency time.Duration `json:"latency"`
	// Status is the HTTP response status the attempt received.
	Status int `json:"status,omitempty"`
}

// FullProbeFailedItem aggregates one level's failures by the outbound that
// was the appended (last) hop: the candidate set the next level retries
// behind its successful prefixes.
type FullProbeFailedItem struct {
	// ID is the outbound that failed as the level's last hop.
	ID string `json:"id"`
	// Address is the outbound's server address, when it has one.
	Address string `json:"address,omitempty"`
	// Count is how many attempts ending in this outbound failed.
	Count int `json:"count"`
	// Reason is the most recent failure's classified reason slug.
	Reason string `json:"reason,omitempty"`
	// Error is the most recent failure's error string (a sample, not all).
	Error string `json:"error,omitempty"`
}

// FullProbeLevel is one depth's sweep: what was tried, what worked, what
// failed. While the level runs, Done/Success/Failed are live counters.
type FullProbeLevel struct {
	// Depth is the chain length this level probed (1 = local → outbound).
	Depth int `json:"depth"`
	// Prefixes is how many successful paths from the previous level were
	// extended (1 at depth 1: the empty local prefix).
	Prefixes int `json:"prefixes"`
	// Candidates is how many distinct outbounds were tried as the last hop.
	Candidates int `json:"candidates"`
	// Total is the planned attempt count after the fan-out caps; Skipped
	// counts the pairs the caps (or a cancel) cut.
	Total   int `json:"total"`
	Skipped int `json:"skipped,omitempty"`
	// Done is how many attempts finished (success or failure).
	Done int `json:"done"`
	// Success / Failed partition Done.
	Success int `json:"success"`
	Failed  int `json:"failed"`
	// SuccessPaths are the level's successful paths, fastest first, capped
	// at the spec's MaxPathsPerLevel. These are the next level's prefixes
	// and what inbound creation builds from.
	SuccessPaths []FullProbePath `json:"success_paths"`
	// FailedItems aggregates the failures by last-hop outbound, sorted by
	// ID. These are the next level's candidates.
	FailedItems []FullProbeFailedItem `json:"failed_items"`
	StartedAt   time.Time             `json:"startedAt"`
	FinishedAt  time.Time             `json:"finishedAt,omitempty"`
}

// FullProbeJob is one full active probing job's state: its spec, its
// progress, and every level's results. The API serves snapshots; the runner
// owns the live copy.
type FullProbeJob struct {
	ID    string        `json:"id"`
	Spec  FullProbeSpec `json:"spec"`
	State string        `json:"state"`
	// Error is why the job ended in state "error" (a spec that cannot run,
	// no outbounds to probe); empty otherwise.
	Error string `json:"error,omitempty"`
	// Levels are the completed and in-flight levels, depth ascending.
	Levels []FullProbeLevel `json:"levels"`
	// TotalAttempts is how many attempts have finished across all levels.
	TotalAttempts int       `json:"total_attempts"`
	StartedAt     time.Time `json:"startedAt"`
	FinishedAt    time.Time `json:"finishedAt,omitempty"`
	// RowsPruned reports that the job's durable attempt-result rows
	// (GET /api/probes/full/{id}/results) were pruned — the per-job or global
	// retention budget cut them, or the job's result bucket was evicted
	// wholesale. The attempt counts above stay exact; only the row detail
	// is partial.
	RowsPruned bool `json:"rows_pruned,omitempty"`
}

// FullProbeApplySpec selects which successful paths of a finished job
// POST /api/probes/full/{id}/inbounds turns into chains + inbounds.
//
// The zero value applies BestPaths — the fastest successful path per depth
// (local→A, local→A→C, local→A→C→E, ...) — one inbound per path. Naming
// Paths explicitly overrides that with the exact chains to build.
type FullProbeApplySpec struct {
	// Paths are the exact hop lists to build, each an ordered list of
	// outbound IDs. Empty means BestPaths().
	Paths [][]string `json:"paths,omitempty"`
	// Protocol is the inbound listener protocol ("http" or "socks5"); empty
	// means "socks5".
	Protocol string `json:"protocol,omitempty"`
	// ListenPrefix is the host every generated inbound listens on; empty
	// means 127.0.0.1.
	ListenPrefix string `json:"listen_prefix,omitempty"`
	// Port is the first port to assign; each further inbound takes the next
	// one. Zero picks a free port per inbound (probed by opening and closing
	// a socket, so the generated config names a concrete port).
	Port int `json:"port,omitempty"`
	// IDPrefix names the generated chains and inbounds
	// ("<prefix>-d<depth>-<n>"); empty means "fullprobe-<jobID>".
	IDPrefix string `json:"id_prefix,omitempty"`
	// Mode selects what apply builds: "" or "inbounds" keeps one inbound
	// listener per path (the default); "pool" builds one pool load-balancing
	// over the proved paths (each path becomes a member chain) behind a
	// single inbound routed through it.
	Mode string `json:"mode,omitempty"`
	// PoolID names the generated pool in pool mode; empty means
	// "<IDPrefix>-pool". Ignored in inbounds mode.
	PoolID string `json:"pool_id,omitempty"`
	// Selector is the generated pool's selection strategy in pool mode; nil
	// means roundrobin. Ignored in inbounds mode.
	Selector *FullProbeSelectorSpec `json:"selector,omitempty"`
}

// FullProbeSelectorSpec mirrors config.SelectorSpec for the apply request.
// core cannot import config (config cannot import core either), so the API
// handler maps this onto the config form when it writes the pool.
type FullProbeSelectorSpec struct {
	// Type is "random", "roundrobin", "sticky", "leastlatency".
	Type string `json:"type"`
	// Params is strategy-specific config, e.g. sticky: {"key":"domain"}.
	Params map[string]any `json:"params,omitempty"`
}

// FullProbeApplyResult reports one generated chain + inbound pair.
type FullProbeApplyResult struct {
	ChainID   string        `json:"chain_id"`
	InboundID string        `json:"inbound_id"`
	Listen    string        `json:"listen"`
	Protocol  string        `json:"protocol"`
	Path      FullProbePath `json:"path"`
}

// Clone deep-copies a job so a snapshot can be marshaled while the runner
// keeps mutating its own copy.
func (j *FullProbeJob) Clone() *FullProbeJob {
	if j == nil {
		return nil
	}
	out := *j
	out.Levels = make([]FullProbeLevel, len(j.Levels))
	for i, l := range j.Levels {
		out.Levels[i] = l
		out.Levels[i].SuccessPaths = append([]FullProbePath(nil), l.SuccessPaths...)
		for k, p := range l.SuccessPaths {
			out.Levels[i].SuccessPaths[k].Hops = append([]string(nil), p.Hops...)
		}
		out.Levels[i].FailedItems = append([]FullProbeFailedItem(nil), l.FailedItems...)
	}
	return &out
}

// BestPaths returns the fastest successful path per depth, depth ascending —
// the default selection inbound creation builds from: local→A, local→A→C,
// local→A→C→E, ...
func (j *FullProbeJob) BestPaths() []FullProbePath {
	var out []FullProbePath
	for _, l := range j.Levels {
		if len(l.SuccessPaths) == 0 {
			continue
		}
		best := l.SuccessPaths[0]
		for _, p := range l.SuccessPaths[1:] {
			if p.Latency < best.Latency {
				best = p
			}
		}
		out = append(out, best)
	}
	return out
}

// AllPathsFastest returns every successful path across all levels, deduped by
// its hop-join and ordered fastest first — the default selection pool mode
// builds its member chains from. Ties on latency keep the shallower path
// first (fewer hops for the same cost). The caller caps the result.
func (j *FullProbeJob) AllPathsFastest() []FullProbePath {
	seen := map[string]bool{}
	var out []FullProbePath
	for _, l := range j.Levels {
		for _, p := range l.SuccessPaths {
			key := strings.Join(p.Hops, ">")
			if seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, p)
		}
	}
	sort.SliceStable(out, func(a, b int) bool {
		if out[a].Latency != out[b].Latency {
			return out[a].Latency < out[b].Latency
		}
		return len(out[a].Hops) < len(out[b].Hops)
	})
	return out
}
