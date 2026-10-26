// status.go: the provider-manager observability view.
//
// The engine knows things about every provider regardless of plugin
// cooperation — when it started, when it last reported outbounds, what its
// last Outbounds error was, and how many outbounds its poll produced. A
// plugin can add more by implementing pub.StatusReporter: its live
// self-assessment (Psiphon: per-tunnel establishment, server list state)
// is merged into the same snapshot. The admin API exposes the merged view
// at /api/providers/status and the web UI polls it, so "is this provider
// healthy, and what is it doing?" is answerable without reading engine
// logs.
package provider

import (
	"sort"
	"time"

	"github.com/goose-network/goose/internal/config"
	pub "github.com/goose-network/goose/pkg/plugin"
)

// ProviderStatus is the merged engine-observed + plugin-reported view of one
// running provider, as served by /api/providers/status.
type ProviderStatus struct {
	// ID is the provider spec ID.
	ID string `json:"id"`
	// Provider is the plugin name, e.g. "psiphon".
	Provider string `json:"provider"`
	// Pools lists the pool IDs the provider feeds.
	Pools []string `json:"pools"`

	// State is the engine's phase word: "running" (goroutine alive, last
	// poll succeeded), "error" (last poll failed — pools kept stale), or
	// "starting" (instantiated but not yet polled).
	State string `json:"state"`
	// Message carries the last Outbounds error when State is "error".
	Message string `json:"message"`

	// StartedAt is when the provider instance was instantiated.
	StartedAt time.Time `json:"started_at"`
	// LastPollAt is when Outbounds was last called, successfully or not.
	LastPollAt time.Time `json:"last_poll_at"`
	// LastOKAt is when Outbounds last succeeded.
	LastOKAt time.Time `json:"last_ok_at"`
	// OutboundCount is the last successful poll's outbound count.
	OutboundCount int `json:"outbound_count"`

	// Plugin holds the provider's own self-assessment, when it implements
	// pub.StatusReporter. Nil (and omitted from JSON) when it does not.
	Plugin *pub.ProviderStatus `json:"plugin,omitempty"`
}

// recordStart notes a provider instance coming up: fresh lifecycle
// timestamps and no poll results yet.
func (m *Manager) recordStart(spec *config.ProviderSpec) {
	m.statusMu.Lock()
	m.status[spec.ID] = &ProviderStatus{
		ID:        spec.ID,
		Provider:  spec.Provider,
		Pools:     spec.EffectivePoolIDs(),
		State:     "starting",
		StartedAt: time.Now(),
	}
	m.statusMu.Unlock()
}

// recordResult updates a provider's snapshot with one poll outcome: the
// timestamps, the outbound count on success, or the error on failure.
func (m *Manager) recordResult(id string, outboundCount int, err error) {
	m.statusMu.Lock()
	defer m.statusMu.Unlock()
	st, ok := m.status[id]
	if !ok {
		st = &ProviderStatus{ID: id}
		m.status[id] = st
	}
	st.LastPollAt = time.Now()
	if err != nil {
		st.State = "error"
		st.Message = err.Error()
		return
	}
	st.State = "running"
	st.Message = ""
	st.LastOKAt = time.Now()
	st.OutboundCount = outboundCount
}

// recordStop moves a stopped provider's snapshot to a terminal state rather
// than dropping it: an operator asking "why is my pool empty" should see
// that its provider failed to start, not silence. The entry is overwritten
// when a spec with the same ID starts again.
func (m *Manager) recordStop(id string, startErr error) {
	m.statusMu.Lock()
	defer m.statusMu.Unlock()
	st, ok := m.status[id]
	if !ok {
		st = &ProviderStatus{ID: id}
		m.status[id] = st
	}
	st.State = "stopped"
	st.Message = ""
	if startErr != nil {
		st.State = "error"
		st.Message = startErr.Error()
	}
}

// Status returns the merged status of every provider the manager tracks —
// running ones plus stopped/failed ones that were never replaced — sorted
// by ID. Providers implementing pub.StatusReporter contribute their live
// self-assessment; a plugin's Status is called outside the manager's locks
// so a slow or misbehaving implementation cannot stall reconcile.
func (m *Manager) Status() []ProviderStatus {
	// Live plugin reports first, gathered without holding any manager lock
	// longer than the snapshot copy: Status is expected to be cheap, but a
	// plugin bug must not wedge poll/reconcile.
	type live struct {
		id string
		st pub.ProviderStatus
	}
	var reports []live
	seen := map[string]struct{}{}
	func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		for _, sc := range m.running {
			sr, ok := sc.prov.(pub.StatusReporter)
			if !ok {
				continue
			}
			reports = append(reports, live{id: sc.spec.ID, st: sr.Status()})
			seen[sc.spec.ID] = struct{}{}
		}
	}()

	m.statusMu.Lock()
	out := make([]ProviderStatus, 0, len(m.status))
	for id, st := range m.status {
		s := *st
		if _, ok := seen[id]; ok {
			for _, r := range reports {
				if r.id == id {
					cp := r.st
					s.Plugin = &cp
					break
				}
			}
		}
		out = append(out, s)
	}
	m.statusMu.Unlock()

	// Providers instantiated but not yet in the status map (a race between
	// startOne and Status) still deserve an entry with their plugin report.
	for _, r := range reports {
		if _, ok := statusSnapshotHas(out, r.id); ok {
			continue
		}
		out = append(out, ProviderStatus{ID: r.id, State: "starting", Plugin: &r.st})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// statusSnapshotHas reports whether the snapshot slice already carries id.
func statusSnapshotHas(out []ProviderStatus, id string) (ProviderStatus, bool) {
	for _, s := range out {
		if s.ID == id {
			return s, true
		}
	}
	return ProviderStatus{}, false
}
