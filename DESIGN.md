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

## Package layout

```
goose/
├── go.mod
├── cmd/goose/              # main entrypoint
├── internal/
│   ├── config/             # config schema + live store
│   ├── core/               # core interfaces: Outbound, Inbound, Metadata
│   ├── plugin/             # plugin registry + loader (outbound protocols)
│   ├── inbound/            # http, socks5 listeners + auth + multi-user
│   ├── router/             # chain execution, filter, selector dispatch
│   ├── selector/           # LB strategies: random, roundrobin, sticky, leastlatency
│   ├── filter/             # location, latency, protocol whitelist/blacklist
│   ├── geo/                # offline IP db (mmdb/qqwry) lookup
│   ├── metrics/            # bbolt-backed request metrics store
│   ├── stack/              # system stack + gvisor netstack adapter
│   └── api/                # RESTful admin server
├── pkg/                    # public, importable by plugin repos
│   └── plugin/             # the plugin contract (OutboundFactory, Register)
└── plugins/                # built-in outbound plugins (direct, http, socks5)
    ├── direct/
    ├── http/
    └── socks5/
```

`pkg/plugin` is the only package external plugin repos import. It exposes
`Register(name, factory)` and the `Outbound` interface. Built-in plugins
live under `plugins/` and register via `init()`; external repos call
`plugin.Register` in their own `init()` and are loaded by build (or, later,
by a Go plugin `.so` loader).

## Key interfaces (see internal/core)

- `Metadata` — who is asking, for what target, over which inbound.
- `Outbound` — `DialContext(ctx, target) (net.Conn, error)` + metadata
  (protocol, location, latency history).
- `Selector` — `Pick(pool, meta) (Outbound, error)` for a given LB strategy.
- `Filter` — `Allow(outbound, meta) bool` predicate composing the filter
  dimensions.
- `Chain` — ordered list of (filter, selector) layers executed left-to-right.

## Testing

- **Unit** — per-package `_test.go` covering selectors, filters, geo lookup,
  metrics store, auth, config store.
- **Integration** — `test/integration` exercises inbound→router→outbound
  with real loopback listeners and a stub upstream.
- **E2E** — `test/e2e` boots the full engine from config, drives an HTTP
  client through the SOCKS5/HTTP inbound, and asserts the request lands on a
  recorded upstream and is persisted in the metrics DB.
