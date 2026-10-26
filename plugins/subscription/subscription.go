// Package subscription is the subscription-link outbound provider plugin for
// the goose proxy-pool engine.
//
// A subscription link is a URL that returns a list of proxy servers — the
// format popularized by V2Ray/clash clients. The provider fetches the URL and
// parses the body into outbound configs, so the managed pool always mirrors
// the current contents of the link; the engine re-polls on the provider's
// refresh interval (spec config "interval_seconds", default 120s), so edits
// on the remote side flow into the pool without an engine restart.
//
// # Body formats
//
// Two shapes are recognized:
//
//  1. one proxy URI per line (blank lines and #-comments ignored):
//
//     socks5://user:pass@1.2.3.4:1080
//     http://1.2.3.4:8080
//
//  2. the same list base64-encoded as a whole body (the classic V2RayN
//     subscription format). Detected when the body contains no "://" and
//     decodes as base64.
//
// Only schemes the engine can dial are imported: socks5:// (alias socks://,
// with optional user:pass) and http://. Other schemes (ss://, vmess://,
// trojan://, ...) are skipped.
//
// Outbound IDs are derived from the server address (e.g. "sub-socks5-
// 1.2.3.4:1080") so they are stable across refreshes: reordering the list
// moves pool members instead of recreating them.
//
// # Upstream routing
//
// When the provider spec sets an upstream_proxy (see config.ProviderSpec),
// the engine injects a routed dialer into this provider's config map (see
// pkg/plugin.AsUpstreamDialer). The subscription download then dials through
// the engine route the spec names — a specific outbound, a pool, or the
// access pool — instead of the engine host's local network, so blocked
// subscription URLs (GFW-style) can be fetched through a working proxy.
//
// # Config
//
// The provider config is a JSON object:
//
//	{
//	  "url":    "https://example.com/sub",  // required
//	  "prefix": "sub"                       // optional, outbound-id prefix
//	}
package subscription

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	pub "github.com/goose-network/goose/pkg/plugin"
)

func init() {
	pub.RegisterProvider("subscription", New)
}

const (
	// fetchTimeout bounds one subscription download.
	fetchTimeout = 30 * time.Second
	// maxBodyBytes caps the subscription body so a hostile link cannot
	// exhaust engine memory.
	maxBodyBytes = 4 << 20
	// defaultPrefix is the outbound-id prefix when the config omits one.
	defaultPrefix = "sub"
)

// Provider is a goose outbound provider backed by a subscription link. It
// implements pub.Provider.
type Provider struct {
	url    string
	prefix string
	client *http.Client
}

// New builds a subscription provider from config:
//
//	{"url":"https://...","prefix":"sub"}
//
// When the engine injected an upstream dialer (the spec's upstream_proxy is
// set), the provider's HTTP transport dials through it; otherwise it uses
// the default transport (direct fetches).
func New(cfg map[string]any) (pub.Provider, error) {
	u, _ := cfg["url"].(string)
	if u == "" {
		return nil, fmt.Errorf("subscription provider: missing url")
	}
	prefix, _ := cfg["prefix"].(string)
	if prefix == "" {
		prefix = defaultPrefix
	}
	client := &http.Client{Timeout: fetchTimeout}
	if d := pub.AsUpstreamDialer(cfg); d != nil {
		// Route the fetch's TCP dials through the engine-resolved upstream.
		// Clone the default transport so its timeouts and HTTP/2 negotiation
		// apply over the tunnel, but drop its ambient proxy support
		// (HTTP_PROXY et al.): the upstream dialer is the route, and layering
		// an env proxy on top of it would double-proxy.
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.Proxy = nil
		transport.DialContext = d.DialUpstream
		client.Transport = transport
	}
	return &Provider{
		url:    u,
		prefix: prefix,
		client: client,
	}, nil
}

// Name identifies the plugin.
func (p *Provider) Name() string { return "subscription" }

// Watch is nil: the provider has no change signal of its own, so the engine
// falls back to periodic polling of Outbounds.
func (p *Provider) Watch() <-chan struct{} { return nil }

// Outbounds fetches the subscription and parses it into outbound configs.
func (p *Provider) Outbounds(ctx context.Context) ([]pub.OutboundConfig, error) {
	body, err := p.fetch(ctx)
	if err != nil {
		return nil, err
	}
	return parse(body, p.prefix)
}

// fetch downloads the subscription body.
func (p *Provider) fetch(ctx context.Context) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.url, nil)
	if err != nil {
		return nil, fmt.Errorf("subscription provider: bad url: %w", err)
	}
	res, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("subscription provider: fetch %s: %w", p.url, err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("subscription provider: %s: status %s", p.url, res.Status)
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, maxBodyBytes))
	if err != nil {
		return nil, fmt.Errorf("subscription provider: read %s: %w", p.url, err)
	}
	return body, nil
}

// parse turns a subscription body into outbound configs. The whole-body
// base64 form is unwrapped first when the body carries no "://" marker.
func parse(body []byte, prefix string) ([]pub.OutboundConfig, error) {
	text := string(body)
	if !strings.Contains(text, "://") {
		if dec, err := base64.StdEncoding.DecodeString(strings.TrimSpace(text)); err == nil && len(dec) > 0 {
			text = string(dec)
		}
	}

	var out []pub.OutboundConfig
	seen := map[string]int{}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(line), "\r"))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		oc, ok := parseURI(line)
		if !ok {
			continue
		}
		// Derive a stable id from protocol + address so a refresh that
		// reorders the list reuses ids instead of churning the pool.
		id := fmt.Sprintf("%s-%s-%s", prefix, oc.Protocol, oc.Config["address"])
		seen[id]++
		if n := seen[id]; n > 1 {
			id = fmt.Sprintf("%s-%d", id, n)
		}
		oc.ID = id
		out = append(out, oc)
	}
	return out, nil
}

// parseURI converts one proxy URI into an outbound config. It returns ok=false
// for URIs the engine cannot dial (missing port, unsupported scheme, ...).
func parseURI(line string) (pub.OutboundConfig, bool) {
	u, err := url.Parse(strings.TrimSpace(line))
	if err != nil {
		return pub.OutboundConfig{}, false
	}
	var protocol string
	switch strings.ToLower(u.Scheme) {
	case "socks5", "socks":
		protocol = "socks5"
	case "http":
		protocol = "http"
	default:
		return pub.OutboundConfig{}, false
	}
	host, port := u.Hostname(), u.Port()
	if host == "" || port == "" {
		return pub.OutboundConfig{}, false
	}
	cfg := map[string]any{"address": net.JoinHostPort(host, port)}
	if u.User != nil {
		cfg["username"] = u.User.Username()
		if pass, ok := u.User.Password(); ok {
			cfg["password"] = pass
		}
	}
	return pub.OutboundConfig{Protocol: protocol, Config: cfg}, true
}
