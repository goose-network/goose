# Multi-stage Dockerfile for the goose engine.
#
# Build (multi-arch, as CI does):
#   docker buildx build --platform linux/amd64,linux/arm64 \
#     --secret id=github_token,src=<token-file> \
#     -t ghcr.io/goose-network/goose:main .
#
# The committed go.mod points the replaced modules at sibling checkouts
# (../goose-plugin-api, ../geph5-go, ...) that only exist in the dev tree.
# scripts/pin-replacements.sh re-points them at the published GitHub
# repositories, pinned to the exact revisions the local tree was tested
# against; it is the single source of truth for the pins (CI workflows call
# it too). The github_token secret is only needed for the one private
# dependency (openrung-go); everything else resolves through the Go module
# proxy. The token is a BuildKit secret mount, consumed through the
# environment, so it never lands in an image or cache layer.
#
# The runtime stage is distroless static (ca-certificates + tzdata
# included) and runs as nonroot. State (the metrics bbolt DB) lives in
# /data, which is the volume mount point.

FROM golang:1.26.8 AS build

# Cross-compile for the target arch from the runner's native arch: goose is
# pure Go with CGO disabled, so neither arch needs QEMU emulation.
ARG TARGETOS
ARG TARGETARCH

# Module proxy: the default suits CI; builders in restricted networks can
# override with --build-arg GOPROXY=https://goproxy.cn,direct.
ARG GOPROXY=proxy.golang.org,direct
ENV GOPROXY=${GOPROXY}

WORKDIR /src
COPY go.mod go.sum ./
COPY scripts/ scripts/

# Re-point the sibling replace directives at the published pins. Pure
# go mod edit: no network, no source needed, so this layer only rebuilds
# when the pins change.
RUN scripts/pin-replacements.sh

COPY . .

# go mod tidy runs inside scripts/pin-replacements.sh --tidy, as a child
# of the script so it inherits the private-repo git credentials — exports
# do not survive into this RUN shell. Tidy needs the source present: it
# computes the require set from the import graph and records the pinned
# versions' checksums in go.sum. The module cache rides a BuildKit cache
# mount so rebuilt images skip re-downloading the world. The token secret
# authenticates the private openrung-go fetch through git's environment.
RUN --mount=type=cache,sharing=locked,id=modcache-${TARGETOS}-${TARGETARCH},target=/go/pkg/mod \
    --mount=type=secret,id=github_token \
    if [ -f /run/secrets/github_token ]; then \
      export GOSE_GITHUB_TOKEN="$(cat /run/secrets/github_token)"; \
    fi; \
    scripts/pin-replacements.sh --tidy && \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -tags with_quic -o /out/goose ./cmd/goose && \
    mkdir -p /out/state

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/goose /usr/local/bin/goose

# The empty state dir seeds /data with nonroot ownership so a named volume
# created from it stays writable; bind mounts must be chowned 65532:65532
# on the host. With no -config argument the engine starts with empty
# defaults and is configured through the admin API; pass
# "-config /config/goose.json" to start from a mounted config instead.
COPY --from=build --chown=nonroot:nonroot /out/state/ /data/
WORKDIR /data
VOLUME /data

# Container default: bind the admin API on all interfaces so the
# published port (-p 9090:9090) can actually reach it; the built-in
# default is the container's own loopback, which nothing outside the
# netns can talk to. Set -e GOOSE_API_LISTEN=127.0.0.1:9090 to restore
# the loopback bind, or pass a -config file whose engine.api.listen
# overrides both (config always wins over the env default).
ENV GOOSE_API_LISTEN=0.0.0.0:9090

# Fresh deployments (no -config providers) seed the plugin-declared default
# provider — the zero-config psiphon plugin, when linked into the build — so
# the engine is routable and the admin UI shows a provider out of the box.
# Run with -e GOOSE_NO_DEFAULT_PROVIDER=1 for a fully manual start.

# Admin API port; the engine binds 0.0.0.0:9090 by default here (see
# GOOSE_API_LISTEN above) — anyone who can reach the published port can
# drive the admin API unless a token is set in the engine config.
# Inbound proxy ports are config-driven — publish them with -p as needed.
EXPOSE 9090

ENTRYPOINT ["/usr/local/bin/goose"]
