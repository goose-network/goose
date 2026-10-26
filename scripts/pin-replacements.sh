#!/usr/bin/env bash
# Re-point goose's local replace directives (../sibling repos) at the
# published GitHub repositories, pinned to the exact revisions the local
# tree was tested against. CI and the Dockerfile both run this script, so
# the pins live in exactly one place; keep them at the local HEADs of the
# sibling repos at the time of the last green build.
#
# OpenRung-go is a private repository: callers must pass its access token
# (a GitHub PAT with repo read) via GOSE_GITHUB_TOKEN (or GITHUB_TOKEN);
# the token is exported to git through GIT_CONFIG_* environment variables
# so it never lands in any file.
#
# Usage: scripts/pin-replacements.sh [--tidy] [--check]
#   --tidy   after re-pointing, run "go mod tidy" as a child of this
#            script so it inherits the git credentials exported above
#            (exports do not survive into a calling shell).
#   --check  compare the pins against local sibling HEADs and exit 1 if
#            any has drifted (CI guard, run after local builds).

set -euo pipefail

cd "$(dirname "$0")/.."

pins=(
  github.com/goose-network/goose-plugin-api=github.com/goose-network/goose-plugin-api@v0.0.0-20261026144344-7d923b10a2d5
  github.com/goose-network/goose-plugin-psiphon=github.com/goose-network/psiphon@v0.0.0-20261026144438-57057ba0eb1d
  github.com/Psiphon-Labs/psiphon-tunnel-core=github.com/goose-network/psiphon-tunnel-core@v0.0.0-20261026140314-5d979dcb6091
  github.com/goose-network/goose-plugin-mihomo=github.com/goose-network/goose-plugin-mihomo@v0.0.0-20261026124232-79094f230b24
  github.com/goose-network/goose-plugin-singbox=github.com/goose-network/goose-plugin-singbox@v0.0.0-20261026124251-8f50bf5efc05
  github.com/Psiphon-Labs/quic-go=github.com/goose-network/psiphon-quic-go@v0.0.0-20261026053528-9822823f4fad
  github.com/metacubex/mihomo=github.com/metacubex/mihomo@v0.0.0-20260922124647-afe94dafef8c
  github.com/sagernet/sing-box=github.com/sagernet/sing-box@v0.0.0-20260922113237-e5e705ed802d
  github.com/geph-official/geph5-go=github.com/goose-network/geph5-go@v0.0.0-20261026234138-0ae835e0ae87
  github.com/goose-network/openrung-go=github.com/goose-network/openrung-go@v0.0.0-20261026124742-1328643f58ce
  github.com/openrung/openrung/brokerapi=github.com/openrung/openrung/brokerapi@v0.7.1-0.20260923074427-404d84a15812
)

# Map replace module -> sibling checkout directory (repo name only; all
# siblings live as siblings of the goose repo in the dev tree).
declare -A sibling_dir=(
  [github.com/goose-network/goose-plugin-api]=goose-plugin-api
  [github.com/goose-network/goose-plugin-psiphon]=psiphon
  [github.com/Psiphon-Labs/psiphon-tunnel-core]=psiphon-tunnel-core
  [github.com/goose-network/goose-plugin-mihomo]=mihomo-plugin
  [github.com/goose-network/goose-plugin-singbox]=singbox-plugin
  [github.com/Psiphon-Labs/quic-go]=psiphon-quic-go
  [github.com/metacubex/mihomo]=mihomo
  [github.com/sagernet/sing-box]=sing-box
  [github.com/geph-official/geph5-go]=geph5-go
  [github.com/goose-network/openrung-go]=openrung-go
  [github.com/openrung/openrung/brokerapi]=openrung
)

tidy=0
if [[ "${1:-}" == "--check" ]]; then
  tidy=-1 # check mode: never tidy
elif [[ "${1:-}" == "--tidy" ]]; then
  tidy=1
fi

if [[ $tidy -eq -1 ]]; then
  # Check mode: pins must match the local sibling HEADs. Siblings that do
  # not exist locally are skipped (not all dev trees have every repo).
  status=0
  for spec in "${pins[@]}"; do
    mod="${spec%%=*}"
    dir="../${sibling_dir[$mod]}"
    if [[ ! -d "$dir/.git" ]]; then
      echo "skip (no local checkout): $mod"
      continue
    fi
    head=$(git -C "$dir" rev-parse HEAD)
    short="${head:0:12}"
    if [[ "$spec" != *"-$short" ]]; then
      echo "DRIFTED: $mod pin=$spec local=$short" >&2
      status=1
    fi
  done
  exit $status
fi

# Apply mode: re-point every local replace at its published pin.
for spec in "${pins[@]}"; do
  mod="${spec%%=*}"
  # shellcheck disable=SC2086
  go mod edit -replace="$spec"
done

# Private dependency: go must fetch openrung-go directly from authenticated
# git instead of the sum database / module proxy (both 404 on private
# repos). The token comes from the environment and is only visible to the
# go tool's child git processes.
if [[ -n "${GOSE_GITHUB_TOKEN:-${GH_TOKEN:-${GITHUB_TOKEN:-}}}" ]]; then
  export GOPRIVATE=github.com/goose-network/openrung-go
  export GIT_CONFIG_COUNT=1
  export GIT_CONFIG_KEY_0="url.https://x-access-token:${GOSE_GITHUB_TOKEN:-${GH_TOKEN:-$GITHUB_TOKEN}}@github.com/.insteadOf"
  export GIT_CONFIG_VALUE_0="https://github.com/"
else
  echo "scripts/pin-replacements.sh: note: no token set; private deps (openrung-go) will only resolve if git already has credentials." >&2
fi

echo "scripts/pin-replacements.sh: replace directives re-pointed at published pins."

# Tidy must run here — imports of tidy in the calling RUN shell don't see
# the exports above, and GOPRIVATE alone doesn't authenticate git.
if [[ $tidy -eq 1 ]]; then
  go mod tidy
fi
