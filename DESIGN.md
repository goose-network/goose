# Goose — Proxy Pool Universal Engine

Goose is a universal proxy-pool engine. It accepts inbound HTTP/SOCKS5
connections (multiple ports, multiple users per port, all managed via a
RESTful API), selects an outbound proxy from an imported pool using a
configurable load-balancing strategy, optionally chains several outbounds
together, and persists per-request metrics to a local database.

## Goals (from the design brief)

1. **Inbound** — HTTP and SOCKS5 proxy with auth. Multiple listen ports and
   multiple users per port, all created/managed through a RESTful API.
2. **Import & geo-locate** — import proxies; auto-identify location using an
   offline IP database (nali-style: MMDB / qqwry / zxinc).
3. **Load balancing** — session stickiness by destination domain, random,
   round-robin, least-latency, etc.
4. **Per-inbound policy** — each inbound (port + auth) can set its own
   load-balance method and filter rules.
5. **Filter dimensions** — location, historical performance (latency),
   outbound protocol whitelist/blacklist.
6. **Metrics persistence** — local DB (bbolt) stores per-request metrics:
   inbound, outbound, target, success/failure, latency, timestamp.
7. **Proxy chains** — API to create an inbound whose traffic traverses a
   chain of outbounds, each layer with its own filters.
8. **Pluggable outbounds** — outbound protocols are plugins living in other
   repos; the engine defines the plugin interface and loads them. Together
   the mihomo and sing-box plugins register every dialable protocol their
   upstream projects offer (mihomo's full set; sing-box's socks, http,
   shadowsocks, vmess, vless, trojan, snell, ssh, anytls, shadowtls, tor,
   plus hysteria/hysteria2/tuic behind the with_quic build tag, which the
   engine's builds enable by default).
9. **System stack or gvisor** — the engine can run on the host network stack
   or on a userspace gvisor netstack for layer 2/3/4 functions.
10. **Dynamic providers** — a plugin can act as a *provider*: a dynamic source
    of outbound configs that the engine polls and merges into a managed pool.
    This is how a plugin that discovers its own proxy servers (e.g. Psiphon,
    which fetches and refreshes a remote server list) feeds those servers into
    the engine's pool without static config.

## Architecture

```
                       ┌─────────────── RESTful API (admin) ───────────────┐
                       │   /inbounds  /pools  /outbounds  /chains  /metrics │
                       └───────────────────────┬───────────────────────────┘
                                               │ config (live, hot-reload)
   client ──HTTP/SOCKS5──▶ ┌──────── Inbound Listener ────────┐
                            │  port + auth → InboundPolicy     │
                            └───────────────┬──────────────────┘
                                            │ conn + target (Metadata)
                            ┌───────────────▼──────────────────┐
                            │           Router / Chain          │
                            │  filter → selector(LB) → dial     │
                            └───────────────┬──────────────────┘
                                            │ DialContext(target)
                            ┌───────────────▼──────────────────┐
                            │        Outbound (plugin)          │
                            │  direct / http / socks5 / ...     │
                            └───────────────┬──────────────────┘
                                            │
                            ┌───────────────▼──────────────────┐
                            │   Stack: system net or gvisor     │
                            └───────────────┬──────────────────┘
                                            │
                            ┌───────────────▼──────────────────┐
                            │        Metrics Recorder           │──▶ bbolt DB
                            └───────────────────────────────────┘
```

Outbounds enter the pool two ways: **static** outbounds declared in config,
and **dynamic** outbounds contributed by a *provider* plugin. A provider
(e.g. the Psiphon plugin) runs a background goroutine that discovers proxy
servers and calls into the config store, which merges the result into a
managed pool and bumps the config version so the router rebuilds:

```
   provider plugin ──Outbounds()──▶ ┌──────── Provider Manager ───────┐
                                    │  poll + Watch() coalesce         │
                                    └───────────────┬──────────────────┘
                                                    │ SetProviderOutbounds(id, pool, specs)
                                    ┌───────────────▼──────────────────┐
                                    │        Config Store              │
                                    │  merge managed outbounds + pool  │
                                    │  bump version                    │
                                    └───────────────┬──────────────────┘
                                                    │ version signal
                                    ┌───────────────▼──────────────────┐
                                    │        Router (rebuild)          │
                                    └──────────────────────────────────┘
```

## Package layout

```
goose/
├── go.mod
├── cmd/goose/              # main entrypoint
├── internal/
│   ├── config/             # config schema + live store (SetProviderOutbounds)
│   ├── core/               # core interfaces: Outbound, Inbound, Metadata
│   ├── plugin/             # plugin registry + loader (outbound protocols)
│   ├── provider/           # dynamic provider manager (poll + Watch loop)
│   ├── inbound/            # http, socks5 listeners + auth + multi-user
│   ├── router/             # chain execution, filter, selector dispatch
│   ├── selector/           # LB strategies: random, roundrobin, sticky, leastlatency
│   ├── filter/             # location, latency, protocol whitelist/blacklist
│   ├── geo/                # offline IP db (mmdb/qqwry) lookup
│   ├── metrics/            # bbolt-backed request metrics store
│   ├── stack/              # system stack + gvisor netstack adapter
│   └── api/                # RESTful admin server
├── pkg/                    # public, importable by plugin repos
│   └── plugin/             # compatibility shim re-exporting goose-plugin-api
├── include/                # blank-imports that register built-in + bundled plugins
└── plugins/                # built-in outbound plugins (direct, http, socks5)
    ├── direct/
    ├── http/
    └── socks5/
```

### Plugin contract and the import boundary

The plugin contract lives in a **separate, dependency-free module**,
[`goose-plugin-api`](../goose-plugin-api) (`github.com/goose-network/goose-plugin-api`).
It owns the `Outbound` / `OutboundFactory` / `Provider` / `ProviderFactory`
types and the `Register` / `RegisterProvider` registries. Both goose and
external plugin repositories import it; **a plugin never imports goose**.
This keeps the dependency graph acyclic: goose may depend on a plugin
(blank-import it to trigger its `init()` registration) without creating a
cycle, because the plugin's only goose-facing dependency is the neutral
`goose-plugin-api` module.

`pkg/plugin` is a thin compatibility shim that re-exports the
`goose-plugin-api` types and delegates the registry calls, so existing
internal callers and built-in plugins keep importing
`github.com/goose-network/goose/pkg/plugin` unchanged. `internal/core`
type-aliases the plugin-facing types (`Outbound`, `OutboundFactory`,
`Network`, `Location`, `LatencySample`, `OutboundStats`) from
`goose-plugin-api` so engine-internal code keeps using `core.Outbound` etc.

Built-in plugins live under `plugins/` and register via `init()`. External
plugins (e.g. `goose-plugin-psiphon`) are pulled in by a `require` + a
blank import in `include/register.go`.

## Key interfaces (see internal/core)

- `Metadata` — who is asking, for what target, over which inbound.
- `Outbound` — `DialContext(ctx, target) (net.Conn, error)` + metadata
  (protocol, location, latency history).
- `Selector` — `Pick(pool, meta) (Outbound, error)` for a given LB strategy.
- `Filter` — `Allow(outbound, meta) bool` predicate composing the filter
  dimensions.
- `Chain` — ordered list of (filter, selector) layers executed left-to-right.
- `Provider` — a dynamic source of outbound configs. `Outbounds(ctx)` returns
  the current set of proxy servers; `Watch()` returns a channel signaled when
  that set may have changed (e.g. after a remote server-list refresh). The
  engine's `internal/provider.Manager` polls each provider, merges the result
  into the config store via `SetProviderOutbounds`, and rebuilds the managed
  pool. Defined in `goose-plugin-api`; see `internal/provider/manager.go`.

## Testing

- **Unit** — per-package `_test.go` covering selectors, filters, geo lookup,
  metrics store, auth, config store.
- **Integration** — `test/integration` exercises inbound→router→outbound
  with real loopback listeners and a stub upstream.
- **E2E** — `test/e2e` boots the full engine from config, drives an HTTP
  client through the SOCKS5/HTTP inbound, and asserts the request lands on a
  recorded upstream and is persisted in the metrics DB.
- **Provider** — `internal/provider/manager_test.go` covers the merge loop:
  a fake provider's `Outbounds` result lands in a managed pool, a `Watch`
  signal triggers a refresh that adds/removes outbounds, and a provider
  error does not drain the pool or crash the manager.

## Inbound policy and routing

An `InboundPolicy` binds a route to an inbound or to one user of an inbound.
The route is one of, in resolution order (an unresolvable reference falls
through to the next):

1. `chain_id` — a named chain from the chain store;
2. `outbound_id` — one specific outbound server, pinned;
3. `pool_id` — a single pool layer;
4. none set — any available outbound (shared fallback selector).

`filters` are not part of the route choice: they narrow whichever route
resolved — on top of a pool's own filters, and on *every* layer of a named
chain.

**Per-user policies.** Each user on a port can carry their own policy. Route
fields are replaced *as a group*: a user policy that sets any of
`chain_id` / `outbound_id` / `pool_id` ignores the inbound's route entirely;
one that sets none inherits it. Filters are the user's own either way — a
user policy with filters replaces the inbound-level filters (per-user
narrowing), while other users keep theirs. Routes are resolved into
dialable chains at rebuild time, so dispatch is a map lookup.

**Chain layers.** A chain layer references either a pool (which carries its
own filters + selector) or a single outbound server, pinned for that hop —
so a chain can mix pool hops and fixed servers. When a layer id matches
both a pool and an outbound, the pool wins.

## Dynamic providers

A *provider* is a plugin that sources its own proxy servers and feeds them
into the engine's pool, instead of being statically configured. Config
declares a provider under `providers`:

```json
{
  "providers": [
    {
      "id": "psiphon-prov",
      "provider": "psiphon",
      "pool_id": "psiphon-pool",
      "config": { "config_path": "/path/to/psiphon.config", "...": "..." }
    }
  ]
}
```

The engine instantiates the `"psiphon"` provider via its registered factory,
runs a manager goroutine that calls `Outbounds` immediately (bootstrap), on
each `Watch()` signal (provider-driven refresh), and on a 2-minute poll
fallback. Each result is merged into the store as managed outbounds plus a
pool named `pool_id`, bumping the config version so the router rebuilds.
Stale outbounds (present last poll, absent this poll) are removed. On
`Outbounds` error the existing pool is kept (no drain, no crash).

A provider spec may also set `upstream_proxy`, routing the provider's own
URL fetches (the subscription download, the server-list fetch, ...) through
an engine-managed route instead of the engine host's local network:
`""`/`"direct"`, `"outbound:<id>"` (one specific proxy server), `"pool:<id>"`
(the pool's selector with member failover, access filter included), or
`"access"` (dial direct first, then fall back to the access pool of the
pools this provider feeds). The router resolves the reference against live
config at dial time and hands the provider a dialer injected into its
config map under `pkg/plugin.UpstreamDialerKey`; URL-fetching plugins wire
it as their HTTP transport's DialContext. The same
selection/failover/access-filter machinery that serves user traffic thereby
also serves provider fetches, so a URL blocked on the host network can flow
through a reachable proxy — and edits to pools apply to the next fetch
without restarting the provider, while editing the field restarts it.

When the config store holds no providers at startup, the engine seeds the
**default provider a plugin declared** through the plugin contract
(`RegisterDefaultProvider`; the registry panics when two plugins declare
one). This keeps the engine generic — it never learns *which* plugin is the
default — while a zero-config deployment (the docker image run with no
config file) still comes up with a working provider. Seeding is an
entrypoint concern (`cmd/goose`), not `engine.New`: libraries and tests
build engines directly and must not spontaneously start live tunnels. An
explicitly configured provider always wins, and `GOOSE_NO_DEFAULT_PROVIDER=1`
opts out entirely.

See the [Psiphon provider plugin](../psiphon) for a real example: it starts
a tunnel-core tunnel with the background remote-server-list fetcher enabled
(so the server list auto-updates like the Psiphon Windows client) and
exposes the tunnel's local SOCKS proxy as a single socks5 outbound in the
managed pool.

The [geph provider](../geph5-go/plugins/geph) is a second bundled example of
the same shape: it embeds the geph5-go client (a wire-compatible Go port of
the geph5 client core) and contributes the client's loopback SOCKS5 listener
as a single socks5 outbound — but only after the first tunnel to a geph5 exit
is live, since CONNECTs fail before that; readiness is reported via the
`Watch()` signal so the pool fills as soon as the tunnel comes up. It also
demonstrates the variant of plugin placement where the plugin lives inside
the embedded project's repo (to use its `internal/` packages) and goose pulls
it in via `require` + `replace`. Its `embedded_exit` config option goes one
step further and boots the geph5 exit itself (a Go port of the Rust
`geph5-exit` serving path) inside the engine's process, pinned by an
auto-generated direct constraint — the fully standalone deployment where
goose + geph client + geph exit are one binary, verified in the default test
suite (`TestE2E_GephProviderStandalone`).

The [tor provider](../tor-go/plugins/tor) is a third example — a slow-boot
one. It embeds tor-go (a from-scratch Go Tor client) and contributes the
client's loopback SOCKS5 listener as a single socks5 outbound once the client
is serving; until then `Outbounds` stays empty and bootstrap failures are
retried with backoff inside the provider, so a dead network leaves the pool
empty rather than killing the provider. It demonstrates the *published*
placement variant: tor-go is tagged as its own versioned module
(`github.com/goose-network/tor-go`), so goose requires it directly from the
module proxy with no local `replace` — the wiring every sibling plugin
converges on once it is tagged.

## Auto-built chains (access pools)

Local networks reach only some proxy servers directly; an ISP may block the
rest. A pool carrying an `access` filter (`access_pool_id`,
`probe_interval_seconds`, `probe_timeout_seconds`) closes the gap without
manual chain editing:

- **Prober** (router-internal loop, `internal/router/probe.go`): for every
  member of every access-filtered pool, dial the member's server (a) through
  the engine stack ("direct") and, on failure, (b) through each access-pool
  candidate ("relay"). Rounds run on the smallest configured interval and
  are nudged on rebuild so new members are probed promptly. Verdicts are
  kept per outbound and exposed via `GET /api/chains/probes`.
- **Dispatch** narrows each access-filtered pool to its reachable members
  (fail-open before the first verdict) and prepends the recorded relay
  (engine → access → member) for members that need it. Members unreachable
  both ways are dropped rather than dialed-into-failure.
- **Per-hop dialing** (`dialLayers`): a chain's layers are selected up
  front, so hop *i* CONNECTs toward hop *i+1*'s proxy-server address — real
  multi-hop semantics. The relay-prepended auto chain is exactly this with
  the access proxy as hop 0.

## Exit-IP checks

The exit IP of a proxy differs from its connect address and varies by
target site and time, so `internal/exitip` records `(outbound, site, exit
IP, latency, time)` tuples:

- **Scheduler** — `engine.exit_checks` (disabled by default): cron (5-field
  Vixie semantics, DOM/DOW OR when both restricted — `internal/exitip/cron.go`)
  or a fixed interval; an immediate first round populates data without
  waiting out the schedule. Hot-reloads on config bumps without losing the
  cron phase; invalid cron is rejected at the API (400).
- **Checks dial through the outbound** via the router's dispatch dial path
  (relay hop included), then GET a plain-text "what is my IP" site with
  `curl`-ish UA and validate the response with `net.ParseIP`. Browsers
  cannot route `fetch()` through an arbitrary SOCKS5/HTTP proxy, so checks
  run engine-side; the frontend reads results through the admin API.
- **Load spreading** — rounds rotate round-robin across the site list, and
  within a round successive outbounds hit different sites, with a
  concurrency cap; a large batch therefore spreads over several sites
  instead of hammering one (rate limits / accidental DDoS).
- **Persistence** — the metrics store keeps results in the `exitchecks`
  bucket; `GET /api/exit-checks?n=&outbound_id=` lists them newest-first,
  `POST /api/exit-checks/run` triggers a round on demand, and the web UI's
  Exit IP page renders latest-per-outbound, history, and the scheduler.
