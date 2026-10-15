# Goose — Proxy Pool Universal Engine

Goose is a universal proxy-pool engine. It accepts inbound HTTP/SOCKS5
connections, selects an outbound proxy from a pool using a configurable
load-balancing strategy, optionally chains outbounds, and persists
per-request metrics. Outbound protocols and dynamic server sources are
**plugins**.

See [DESIGN.md](DESIGN.md) for the full architecture.

## Build

```bash
GOTOOLCHAIN=go1.26.8 go build ./cmd/goose
```

The Go 1.26.8 toolchain pin is required because the bundled Psiphon
provider plugin pulls in `psiphon-tunnel-core`, whose `psiphon-tls` is
ABI-incompatible with Go 1.27.

## Run

```bash
./goose -config config.json
```

Config is a JSON document with `engine`, `inbounds`, `outbounds`, `pools`,
`chains`, and `providers` (all optional). Example — a SOCKS5 inbound whose
pool is populated dynamically by the Psiphon provider:

```json
{
  "engine": {
    "stack": "system",
    "api": { "listen": "127.0.0.1:9099" },
    "db": "goose.db"
  },
  "inbounds": [
    { "id": "in-socks", "protocol": "socks5", "listen": "127.0.0.1:11080" }
  ],
  "providers": [
    {
      "id": "psiphon-prov",
      "provider": "psiphon",
      "pool_id": "psiphon-pool",
      "config": {
        "config_path": "/path/to/psiphon.config",
        "server_list_path": "/path/to/server_list.dat",
        "data_dir": "/path/to/data-root",
        "upstream_proxy": "socks5://127.0.0.1:7890",
        "establish_timeout_seconds": 180,
        "outbound_id": "psiphon"
      }
    }
  ]
}
```

Then route through it:

```bash
curl --socks5-hostname 127.0.0.1:11080 https://api.ipify.org
```

The admin API (`GET /api/pools`, `/api/outbounds`, ...) reports the live
pool, including outbounds contributed by providers.

### Geph provider

The bundled [geph5-go](../geph5-go) client is wired in as a provider too. It
embeds the full geph5 client (a wire-compatible Go port: broker protocol,
X25519+ed25519 exit handshake, sosistab3/picomux tunnels) and, once its first
tunnel to a geph5 exit is live, contributes the client's loopback SOCKS5
listener as one `socks5` outbound:

```json
{
  "providers": [
    {
      "id": "geph-prov",
      "provider": "geph",
      "pool_id": "geph-pool",
      "config": {
        "exit_constraint": { "direct": "203.0.113.7:9000/<exit-pubkey-hex>" },
        "secret": "user-secret"
      }
    }
  ]
}
```

Config keys (broker mode, `config_path` YAML base, `socks_listen`,
`outbound_id`, ...) are documented in
[geph5-go's plugin README](../geph5-go/plugins/geph/README.md).

The provider can also run **fully standalone — one process, no Rust
`geph5-exit`, no broker**: `"embedded_exit"` boots geph5-go's Go exit server
(`internal/exitlib`) inside the engine's own process and pins the embedded
client at it:

```json
{
  "providers": [
    {
      "id": "geph-prov",
      "provider": "geph",
      "pool_id": "geph-pool",
      "config": {
        "embedded_exit": { "allow_loopback": true },
        "secret": "any-value"
      }
    }
  ]
}
```

`allow_loopback` relaxes the exit's global-routability rules so it will proxy
to loopback targets in the same process — useful for single-box setups;
`listen` and `seed_hex` (a stable signing key) are the other knobs. Traffic
then rides the real geph5 tunnels end to end (X25519+ed25519 handshake,
picomux, ChaCha20-Poly1305), just with the exit in-process. This shape is
continuously verified in the default test suite
(`TestE2E_GephProviderStandalone`).

### Tor provider

The bundled [tor-go](../tor-go) client is wired in as a provider too. It
embeds a from-scratch Go Tor client (link v3 handshake, ntor circuits,
directory fetch with signature verification) and, once the client is serving,
contributes its loopback SOCKS5 listener as one `socks5` outbound — so the
pool's traffic rides real 3-hop Tor circuits:

```json
{
  "providers": [
    {
      "id": "tor-prov",
      "provider": "tor",
      "pool_id": "tor-pool",
      "config": {
        "upstream_proxy": "127.0.0.1:7890"
      }
    }
  ]
}
```

First bootstrap fetches a signed consensus + microdescriptors (expect ~a
minute before the pool fills in). Config keys (`path_len`,
`verify_signatures` — default on, `use_fallbacks`, timeouts, ...) are
documented in [tor-go's plugin README](../tor-go/plugins/tor/README.md). To
verify the full path: `curl -s -x socks5h://127.0.0.1:11080
https://check.torproject.org/api/ip` should answer `{"IsTor":true,...}`.

## OpenAPI spec & SDK

The admin API is documented by an OpenAPI (Swagger 2.0) spec generated from
swag annotations in `internal/api/api.go`:

```bash
make openapi   # regenerates internal/api/docs/swagger.json
```

The committed spec is the source for the TypeScript SDK in
[goose-sdk-ts](https://github.com/goose-network/goose-sdk-ts). CI here
(`.github/workflows/openapi.yml`) regenerates the spec on every change to
`internal/api/` and fails if the committed spec is stale; on every push to
main it also checks out goose-sdk-ts with the org `GH_TOKEN` secret,
regenerates the SDK from this commit's spec, and pushes the result to
goose-sdk-ts's main branch — so the SDK always tracks goose main.

## Plugins

The plugin contract lives in a separate, dependency-free module,
[`goose-plugin-api`](../goose-plugin-api). A plugin implements the
`Outbound` interface (a protocol) and/or the `Provider` interface (a
dynamic server source), registers it in `init()`, and is loaded by a
blank import in [`include/register.go`](include/register.go).

**The import boundary:** a plugin imports `goose-plugin-api` only — it
never imports goose. Goose imports the plugin. This keeps the module graph
acyclic.

- **Outbound protocol plugin** — implement `Outbound`, call
  `plugin.Register("myproto", factory)` in `init()`.
- **Provider plugin** — implement `Provider` (a dynamic source of outbound
  configs), call `plugin.RegisterProvider("myprov", factory)` in `init()`.
  The engine polls `Outbounds` and merges the result into a managed pool.

Built-in protocols (`direct`, `http`, `socks5`) live under `plugins/`. The
bundled [Psiphon provider](../psiphon), the
[geph provider](../geph5-go/plugins/geph) (part of the geph5-go client port),
and the [tor provider](../tor-go/plugins/tor) (part of the tor-go client)
are wired in by default.

### Adding a provider plugin

1. Create a module importing only `goose-plugin-api`; implement `Provider`;
   register in `init()` with `plugin.RegisterProvider`.
2. In goose's `go.mod`, add a `require` pointing at the plugin — a local
   `replace` only while it is unpublished (tor-go is required straight from
   the module proxy; the geph and Psiphon plugins still use a replace).
3. Add `_ "your/plugin/module"` to `include/register.go`.

A plugin may be its own repo (Psiphon) or live inside the project it embeds —
the geph provider sits in geph5-go's `plugins/geph` and the tor provider in
tor-go's `plugins/tor`, because each embeds that module's `internal/` client
packages. The tor provider also demonstrates the published variant: tor-go is
tagged as `github.com/goose-network/tor-go`, so goose requires the versioned
module directly (no `replace`) — plain `require` + blank-import wiring.

## Tests

```bash
GOTOOLCHAIN=go1.26.8 go test ./...
```

The geph provider has a real-network e2e (boots a Rust `geph5-exit` and pushes
HTTP through inbound → pool → geph tunnel); it is skipped unless
`GEPH_EXIT_BIN` is set:

```bash
GEPH_EXIT_BIN=../geph5/bin-out/geph5-exit \
GEPH_TEST_TARGET=223.5.5.5:80 \
GOTOOLCHAIN=go1.26.8 go test -run TestE2E_Geph -v ./test/e2e/
```

Its standalone counterpart — `TestE2E_GephProviderStandalone` — needs no env
vars and no Rust binary: the provider's `embedded_exit` option runs the Go
exit inside the test process, so the whole chain (engine, admin API, geph
client, geph exit, target) is one process. It runs in the default suite
above.

The tor provider's real-network check (a full engine with an inbound routed
through the tor pool, asserting `IsTor:true` from check.torproject.org) is
run manually — the recipe is in
[tor-go's plugin README](../tor-go/plugins/tor/README.md).
