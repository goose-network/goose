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
   repos; the engine defines the plugin interface and loads them.
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

See the [Psiphon provider plugin](../psiphon) for a real example: it starts
a tunnel-core tunnel with the background remote-server-list fetcher enabled
(so the server list auto-updates like the Psiphon Windows client) and
exposes the tunnel's local SOCKS proxy as a single socks5 outbound in the
managed pool.
