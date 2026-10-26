// upstream_proxy wiring: routing a provider's own URL fetches through an
// engine-managed proxy instead of the local network.
//
// A ProviderSpec's upstream_proxy field references a specific outbound
// ("outbound:x" or a bare id), a pool ("pool:x"), the access pool of the
// pools the provider feeds ("access"), or an explicit "direct". The manager
// resolves that reference into a pkg/plugin.UpstreamDialer — implemented by
// the router against live config — and injects it into the provider's config
// map under pkg/plugin.UpstreamDialerKey before instantiation, so
// URL-fetching providers (subscription, mihomo, singbox, ...) download their
// subscription/config through the configured route.
//
// Resolution of the reference happens at DIAL time, not once at provider
// start, so pool membership changes, access-pool edits, and rebuilt outbounds
// apply to running providers without a restart. Editing the reference itself
// does restart the provider (providerChanged compares it).
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

// injectUpstreamDialer returns cfg with d stored under the reserved
// upstream-dialer key. The spec's config map is shared with the config store
// and with providerChanged's comparison, so it is copied, never mutated.
func injectUpstreamDialer(cfg map[string]any, d pub.UpstreamDialer) map[string]any {
	if d == nil {
		return cfg
	}
	out := make(map[string]any, len(cfg)+1)
	for k, v := range cfg {
		out[k] = v
	}
	out[pub.UpstreamDialerKey] = d
	return out
}
