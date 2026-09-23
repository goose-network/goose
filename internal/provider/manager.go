// Package provider manages dynamic outbound providers: plugins that source
// their own proxy server configs and feed them into the engine's pool. This
// is the engine-side counterpart to pkg/plugin.Provider.
//
// A Manager is constructed with the config store and a set of ProviderSpecs.
// For each spec it instantiates the registered provider, then runs a goroutine
// that:
//   - calls Outbounds immediately (bootstrap);
//   - re-calls Outbounds on each Watch() signal (provider-driven refresh, e.g.
//     Psiphon's remote server list fetcher completing a download);
//   - re-calls Outbounds on a periodic interval (poll fallback, in case the
//     provider has no Watch signal or misses one).
//
// Each poll result is merged into the config store via
// Store.SetProviderOutbounds, which replaces the provider's managed outbounds
// and rebuilds its pool, bumping the config version so the router rebuilds.
package provider

import (
	"context"
	"fmt"
	"time"

	"github.com/goose-network/goose/internal/config"
	pub "github.com/goose-network/goose/pkg/plugin"
)

// pollInterval is the periodic re-poll fallback. Providers that emit Watch
// signals refresh faster; this only bounds staleness when a signal is missed
// or absent.
const pollInterval = 2 * time.Minute

// Manager runs the engine's dynamic outbound providers.
type Manager struct {
	cfg *config.Store

	cancel  context.CancelFunc
	running []context.CancelFunc // per-provider goroutine cancels
}

// New constructs a Manager. It does not start polling; call Start.
func New(cfg *config.Store) *Manager {
	return &Manager{cfg: cfg}
}

// Start instantiates and begins polling every provider currently in the config
// store. It is safe to call once at engine startup. (Reconciliation on
// provider add/remove is a future enhancement; for now providers are declared
// in the initial config.)
func (m *Manager) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel

	for _, spec := range m.cfg.Providers() {
		if err := m.startOne(ctx, spec); err != nil {
			return fmt.Errorf("provider %s: %w", spec.ID, err)
		}
	}
	return nil
}

// startOne launches the poll/refresh goroutine for one provider spec.
func (m *Manager) startOne(ctx context.Context, spec *config.ProviderSpec) error {
	factory, ok := pub.LookupProvider(spec.Provider)
	if !ok {
		return fmt.Errorf("unknown provider plugin %q (registered: %v)", spec.Provider, pub.ProviderNames())
	}
	prov, err := factory(spec.Config)
	if err != nil {
		return fmt.Errorf("instantiate provider %q: %w", spec.Provider, err)
	}

	provCtx, cancel := context.WithCancel(ctx)
	m.running = append(m.running, cancel)

	go m.run(provCtx, spec, prov)
	return nil
}

// run is the per-provider loop: bootstrap, then select on Watch + ticker.
func (m *Manager) run(ctx context.Context, spec *config.ProviderSpec, prov pub.Provider) {
	// Bootstrap: populate the pool immediately so the engine doesn't start
	// with an empty provider pool. If the provider isn't ready yet (e.g.
	// Psiphon hasn't established its tunnel), Outbounds returns an empty set
	// and the pool fills in on the next refresh signal.
	m.poll(ctx, spec, prov)

	watch := prov.Watch()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.poll(ctx, spec, prov)
		case <-watch:
			// Coalesce: drain any pending signals, then poll once.
			for {
				select {
				case <-watch:
					continue
				default:
				}
				break
			}
			m.poll(ctx, spec, prov)
		}
	}
}

// poll calls the provider's Outbounds and merges the result into the store.
func (m *Manager) poll(ctx context.Context, spec *config.ProviderSpec, prov pub.Provider) {
	cfgs, err := prov.Outbounds(ctx)
	if err != nil {
		// Keep the previous pool; a transient fetch failure shouldn't drain
		// the pool. The provider's next refresh will retry.
		fmt.Printf("provider: %s Outbounds: %v (keeping existing pool)\n", spec.ID, err)
		return
	}
	specs := make([]*config.OutboundSpec, 0, len(cfgs))
	for _, c := range cfgs {
		specs = append(specs, &config.OutboundSpec{
			ID:       c.ID,
			Protocol: c.Protocol,
			Config:   c.Config,
		})
	}
	m.cfg.SetProviderOutbounds(spec.ID, spec.PoolID, specs)
}

// Close stops all provider goroutines. It is idempotent.
func (m *Manager) Close() error {
	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
	for _, c := range m.running {
		c()
	}
	m.running = nil
	return nil
}
