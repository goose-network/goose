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

## OpenAPI spec & SDK

The admin API is documented by an OpenAPI (Swagger 2.0) spec generated from
swag annotations in `internal/api/api.go`:

```bash
make openapi   # regenerates internal/api/docs/swagger.json
```

The committed spec is the source for the TypeScript SDK in
[goose-sdk-ts](https://github.com/goose-network/goose-sdk-ts): its
"Generate SDK from OpenAPI spec" workflow fetches this file, converts it to
OpenAPI 3.0, and regenerates the SDK's types via openapi-typescript,
opening a PR with the result. CI here (`.github/workflows/openapi.yml`)
regenerates the spec on every change to `internal/api/` and fails if the
committed spec is stale.

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
bundled [Psiphon provider](../psiphon) is wired in by default.

### Adding a provider plugin

1. Create a module importing only `goose-plugin-api`; implement `Provider`;
   register in `init()` with `plugin.RegisterProvider`.
2. In goose's `go.mod`, add `require` + `replace` pointing at the plugin.
3. Add `_ "your/plugin/module"` to `include/register.go`.

## Tests

```bash
GOTOOLCHAIN=go1.26.8 go test ./...
```
