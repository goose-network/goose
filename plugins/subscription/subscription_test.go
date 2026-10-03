package subscription

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	pub "github.com/goose-network/goose/pkg/plugin"
)

// TestParsePlainList covers the one-URI-per-line body form.
func TestParsePlainList(t *testing.T) {
	body := `
# a comment line
socks5://user:pass@10.0.0.1:1080
http://10.0.0.2:8080

socks://10.0.0.3:1080
ss://unsupported-scheme
vmess://also-unsupported
socks5://10.0.0.4
`
	out, err := parse([]byte(body), "sub")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(out) != 3 {
		t.Fatalf("want 3 outbounds, got %d: %+v", len(out), out)
	}

	first := out[0]
	if first.Protocol != "socks5" {
		t.Errorf("first protocol = %q, want socks5", first.Protocol)
	}
	addr, _ := first.Config["address"].(string)
	if addr != "10.0.0.1:1080" {
		t.Errorf("first address = %q, want 10.0.0.1:1080", addr)
	}
	if u, _ := first.Config["username"].(string); u != "user" {
		t.Errorf("username = %q, want user", u)
	}
	if p, _ := first.Config["password"].(string); p != "pass" {
		t.Errorf("password = %q, want pass", p)
	}
	if first.ID != "sub-socks5-10.0.0.1:1080" {
		t.Errorf("id = %q, want sub-socks5-10.0.0.1:1080", first.ID)
	}

	// http:// with no credentials
	second := out[1]
	if second.Protocol != "http" {
		t.Errorf("second protocol = %q, want http", second.Protocol)
	}
	if _, hasUser := second.Config["username"]; hasUser {
		t.Errorf("second should carry no username, got %v", second.Config)
	}

	// socks:// alias maps to socks5
	third := out[2]
	if third.Protocol != "socks5" {
		t.Errorf("third protocol = %q, want socks5 (socks alias)", third.Protocol)
	}
}

// TestParseBase64Body covers the classic V2RayN whole-body base64 form.
func TestParseBase64Body(t *testing.T) {
	plain := "socks5://a:b@1.1.1.1:1080\nhttp://2.2.2.2:3128\n"
	encoded := base64.StdEncoding.EncodeToString([]byte(plain))
	out, err := parse([]byte(encoded), "sub")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("want 2 outbounds, got %d", len(out))
	}
	if addr, _ := out[0].Config["address"].(string); addr != "1.1.1.1:1080" {
		t.Errorf("address = %q", addr)
	}
}

// TestParseDuplicateServers covers duplicate lines colliding on the same id:
// the second occurrence gets a numeric suffix instead of overwriting.
func TestParseDuplicateServers(t *testing.T) {
	out, err := parse([]byte("http://9.9.9.9:8080\nhttp://9.9.9.9:8080\n"), "sub")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("want 2 outbounds, got %d", len(out))
	}
	if out[0].ID == out[1].ID {
		t.Fatalf("duplicate ids: %q", out[0].ID)
	}
	if !strings.HasSuffix(out[1].ID, "-2") {
		t.Errorf("second duplicate id = %q, want -2 suffix", out[1].ID)
	}
}

// TestNewRequiresURL asserts the factory rejects a spec without a URL.
func TestNewRequiresURL(t *testing.T) {
	if _, err := New(map[string]any{}); err == nil {
		t.Fatal("missing url should be rejected")
	}
	if _, err := New(map[string]any{"url": "http://x"}); err != nil {
		t.Fatalf("url present should be accepted: %v", err)
	}
}

// TestOutboundsFetchesAndParses drives the full provider against a local
// HTTP server serving a subscription body.
func TestOutboundsFetchesAndParses(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("socks5://u:p@127.0.0.1:9999\n"))
	}))
	defer srv.Close()

	p, err := New(map[string]any{"url": srv.URL})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if p.Name() != "subscription" {
		t.Errorf("Name = %q", p.Name())
	}
	if p.Watch() != nil {
		t.Errorf("Watch should be nil (poll-only provider)")
	}

	out, err := p.Outbounds(context.Background())
	if err != nil {
		t.Fatalf("Outbounds: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("want 1 outbound, got %d", len(out))
	}
	if addr, _ := out[0].Config["address"].(string); addr != "127.0.0.1:9999" {
		t.Errorf("address = %q", addr)
	}
}

// TestOutboundsSurfacesFetchError asserts a non-200 subscription response
// reports an error (so the manager keeps the previous pool).
func TestOutboundsSurfacesFetchError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	p, err := New(map[string]any{"url": srv.URL})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := p.Outbounds(context.Background()); err == nil {
		t.Fatal("a 500 subscription should surface an error")
	}
}

// Compile-time check that Provider implements the plugin contract.
var _ pub.Provider = (*Provider)(nil)
