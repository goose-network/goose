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

## Docker

CI publishes a multi-arch (`linux/amd64`, `linux/arm64`) engine image to
GHCR on every push to main:

```bash
docker pull ghcr.io/goose-network/goose:latest
docker run -d -p 9090:9090 -v goose-data:/data \
    ghcr.io/goose-network/goose:latest
```

The image is distroless (static, nonroot). With no `-config` argument the
engine starts with empty defaults and is configured through the admin API.
State (the metrics bbolt DB) lives in `/data`.

**Configuration is durable**: every edit made through the admin API is
written back to the config file the engine was started with — `-config`
when given, else `GOOSE_PERSIST_PATH`, else `goose.json` in the working
directory (loaded on the next start when present) — so API edits survive
a restart. Provider-managed state (the outbounds and pool membership a
provider's polls regenerate) is exempt: only operator objects persist.

A fresh deployment — no `-config`, and nothing yet configured through the
admin API — seeds the **plugin-declared default provider**: the zero-config
Psiphon plugin declares itself the default, so the engine starts it
automatically and its tunnels begin filling the `default` pool with no
configuration at all. The web UI (and `GET /api/providers`) shows the
provider immediately instead of an empty list. The behavior is generic
engine code, not Psiphon-specific: a plugin declares itself via
`plugin.RegisterDefaultProvider`, and the engine seeds whatever was
declared whenever its config store holds no providers — seeding happens
once at startup and never overrides an explicit provider list. Run with
`-e GOOSE_NO_DEFAULT_PROVIDER=1` for a fully manual start.

The image sets `GOOSE_API_LISTEN=0.0.0.0:9090` so the published port
reaches the admin API without any config — without it the engine's
loopback default would bind the container's own 127.0.0.1 and the
published port would get connection-reset (502 behind the goose-web
nginx proxy). Run with `-e GOOSE_API_LISTEN=127.0.0.1:9090` or a
`-config` file to restore a loopback bind. Anyone who can reach the
published port can drive the admin API unless a token is set in the
engine config.

Only the admin API port is declaratively exposed; inbound proxy ports are
config-driven — publish them with `-p` as they appear in your config.

The build re-points the dev-tree replace directives at published pins via
`scripts/pin-replacements.sh` (single source of truth; the OpenAPI and
Docker workflows use the same script) and cross-compiles, so no QEMU is
involved. The one private dependency (`openrung-go`) is fetched with an
authenticated git BuildKit secret.

## Run

```bash
./goose -config config.json
```

Config is a JSON document with `engine`, `inbounds`, `outbounds`, `pools`,
`chains`, and `providers` (all optional). The engine writes the document
back on every change — its own startup state plus any edit made through
the admin API — atomically (temp file + rename), debounced and skipped
when nothing persistable changed. Example — a SOCKS5 inbound whose
pool is populated dynamically by the Psiphon provider, here with no config at
all (the plugin's embedded production defaults auto-retrieve the signed
server list):

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
    { "id": "psiphon-prov", "provider": "psiphon", "pool_ids": ["psiphon-pool"] }
  ]
}
```

A provider may feed **several pools at once** — list them in `pool_ids` and
the provider's whole outbound set lands in each, so one subscription can back
both a "fast" and a "fallback" pool without duplicating the fetch (the legacy
singular `pool_id` still works as a one-entry list):

```json
"providers": [
  {
    "id": "psiphon-prov",
    "provider": "psiphon",
    "pool_ids": ["fast", "fallback"],
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
```

Every field of the psiphon `config` is optional; with no `config_path` the
provider embeds the production Psiphon network parameters (channel, sponsor,
remote-server-list URLs and signing keys, extracted from the official
client) and fetches the server list itself — see
[the plugin README](../psiphon/README.md).

### Routed provider fetches (`upstream_proxy`)

A provider's own URL fetches — the subscription download, the Psiphon
server-list fetch, ... — dial from the engine host's local network by
default. Set the spec-level `upstream_proxy` field to route them through
the engine instead, so a subscription URL the host cannot reach directly
still resolves through a working proxy:

```json
"providers": [
  {
    "id": "blocked-sub",
    "provider": "subscription",
    "upstream_proxy": "pool:relays",
    "config": { "url": "https://example.com/sub" }
  }
]
```

The reference forms mirror the rest of the engine's reference syntax:

| Route | Meaning |
|---|---|
| `""` / `"direct"` | dial the fetch target directly (the default) |
| `"outbound:<id>"` | one specific proxy server |
| `"pool:<id>"` | a pool: its selector picks a member, with failover across members; members the local network cannot reach are dropped by the measured reachability verdicts |
| bare id | a pool with that id when one exists, else an outbound (same precedence as chain layers) |

References resolve at dial time against live config — a pool edit applies to
the next fetch without restarting the provider, while editing the field
itself restarts it. The resolved dialer is injected into the provider's
config map for plugins that support routed fetches (the subscription plugin
does; see `pkg/plugin.UpstreamDialer`).

Note the name collision with the Psiphon plugin's *config-level*
`upstream_proxy` (a SOCKS URL its tunnels dial through, in the example
above): that one lives inside the provider's `config` object and is handled
by the plugin itself. The fetch-routing field described here is a sibling of
`provider`/`pool_ids` on the spec.

Then route through it:

```bash
curl --socks5-hostname 127.0.0.1:11080 https://api.ipify.org
```

The admin API (`GET /api/pools`, `/api/outbounds`, ...) reports the live
pool, including outbounds contributed by providers.

### Reachability metrics and explicit chains

Some proxy servers are directly reachable from the local network; others are
blocked by the local ISP and can only be used through a relay proxy. The
config model keeps no hidden relay pool for this: reachability is *measured*
and recorded in the outbound metrics, and routing filters by those metrics.

With probes enabled, each round dials every outbound's server from the
engine's local network and records the outcome as a metric row whose chain
is `["local", <outbound>]` — the same table (`GET /api/metrics`) that holds
single-hop request rows and multi-hop chain rows:

```json
{
  "engine": {
    "probes": { "enabled": true, "interval_seconds": 300, "timeout_seconds": 5 }
  }
}
```

A pool's first-hop candidates are then narrowed by the latest verdicts: an
outbound whose server the local network cannot reach is dropped from
first-hop selection until a later round — or a successful request — measures
it reachable again. Fail-open: before anything is measured, every candidate
stays admissible.

Reaching a blocked server is explicit config, the same way the diagram
shows chains: put the relay in front of it as a chain layer (or as the
server's own `upstream_proxy`), and each hop's success is measured per hop:

```json
{
  "outbounds": [
    { "id": "front-door", "protocol": "socks5", "config": { "address": "203.0.113.9:1080" } },
    { "id": "blocked", "protocol": "socks5", "config": { "address": "198.51.100.7:1080" } }
  ],
  "chains": [
    { "id": "via-relay", "layers": ["front-door", "blocked"] }
  ]
}
```

The `latency` filter (`min_success`, `max_ms`) narrows candidates by the same
metrics on any pool or inbound policy, so a flaky hop stops serving traffic
without any reachability machinery of its own.

### Exit-IP checks (cron)

A proxy's connect address is not its exit address — the public IP at the
proxy's exit can vary by target site and over time. The engine can check it
per outbound, on a schedule, and record the history:

```json
{
  "engine": {
    "exit_checks": {
      "enabled": true,
      "cron": "*/15 * * * *",
      "sites": ["https://ifconfig.me", "https://api.ipify.org"],
      "timeout_seconds": 15,
      "concurrency": 4
    }
  }
}
```

Each round dials every outbound *through the outbound itself* and asks one
of the configured "what is my IP" sites, rotating
round-robin across the list so large batches spread load instead of
tripping one site's rate limits. Results are recorded per
(outbound, site, exit IP, time) and served by `GET /api/exit-checks`
(filter with `?outbound_id=`); `POST /api/exit-checks/run` triggers an
extra round on demand. `interval_seconds` is the alternative to `cron`
(cron wins when both are set). Zero value (the default) keeps the engine
from making any unsolicited outbound requests. The web UI's Exit IP page
drives all of this.

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
  The engine polls `Outbounds` and merges the result into each managed pool
  named by the spec's `pool_ids` (one provider can feed several pools).

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
