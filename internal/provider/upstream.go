// upstream_proxy wiring: routing a provider's own URL fetches through an
// engine-managed proxy instead of the local network.
//
// A ProviderSpec's upstream_proxy field references a specific outbound
// ("outbound:x" or a bare id), a pool ("pool:x"), a chain ("chain:x"), or an
// explicit "direct". The manager
// resolves that reference into a pkg/plugin.UpstreamDialer — implemented by
// the router against live config — and injects it into the provider's config
// map under pkg/plugin.UpstreamDialerKey before instantiation, so
// URL-fetching providers (subscription, mihomo, singbox, ...) download their
// subscription/config through the configured route.
//
// Resolution of the reference happens at DIAL time, not once at provider
// start, so pool membership changes, chain edits, and rebuilt outbounds
// apply to running providers without a restart. Editing the reference itself
// does restart the provider (providerChanged compares it).
// The manager also injects the engine's layer-3/4 stack dialer
// (pkg/plugin.StackDialerKey) into every provider's config, so plugins that
// dial their own servers route those connections through the engine's
// configured network stack (system kernel stack or userspace gvisor) —
// the "plugin uses the engine's L3/L4 ability" side of the plugin seam.
package provider

import (
	"github.com/goose-network/goose/internal/config"
	pub "github.com/goose-network/goose/pkg/plugin"
)

// upstreamSource supplies the engine-resolved fetch dialer for a provider
// spec's upstream_proxy reference. The router implements it; the manager
// holds it as an interface so tests can stand in or omit it.
type upstreamSource interface {
	// UpstreamDialer returns the dialer routing spec's fetches through its
	// upstream_proxy, or nil when the spec configures none.
	UpstreamDialer(spec *config.ProviderSpec) pub.UpstreamDialer
}

// upstreamDialerFor returns the dialer to inject into spec's plugin config:
// nil when no upstream_proxy is configured (the plugin dials fetches
// directly, exactly the pre-feature behavior), else the engine-resolved
// dialer.
func upstreamDialerFor(src upstreamSource, spec *config.ProviderSpec) pub.UpstreamDialer {
	if spec.UpstreamProxy == "" || src == nil {
		return nil
	}
	return src.UpstreamDialer(spec)
}

// prepareProviderConfig returns the config map to instantiate spec's plugin
// with: the spec's own map carrying the engine-reserved injection keys — the
// stack dialer under StackDialerKey, and the upstream-proxy dialer under
// UpstreamDialerKey when the spec routes fetches through one. The spec's
// config map is shared with the config store and with providerChanged's
// comparison, so it is copied, never mutated. When neither dialer applies
// the original map passes through as-is.
func (m *Manager) prepareProviderConfig(spec *config.ProviderSpec) map[string]any {
	cfg := spec.Config
	if cfg == nil {
		cfg = map[string]any{}
	}

	var stackDialer pub.StackDialer
	if m.stack != nil {
		stackDialer = m.stack.StackDialer()
	}
	upstream := upstreamDialerFor(m.upstream, spec)
	if stackDialer == nil && upstream == nil {
		return cfg
	}

	out := make(map[string]any, len(cfg)+2)
	for k, v := range cfg {
		out[k] = v
	}
	if stackDialer != nil {
		out[pub.StackDialerKey] = stackDialer
	}
	if upstream != nil {
		out[pub.UpstreamDialerKey] = upstream
	}
	return out
}
