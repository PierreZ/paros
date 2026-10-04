#!/usr/bin/env bash
# The toy's smoke test (#196): a fresh Compose cell answers `init`, one
# `parosctl write` and one `parosctl read`. Run after `docker compose build`.
set -euo pipefail
cd "$(dirname "$0")/.."
compose() { docker compose "$@"; }
trap 'compose down --volumes --remove-orphans >/dev/null 2>&1 || true' EXIT
compose up -d
# No frame is fixed: init prints the user journal it drew.
init=$(compose run --rm init)
echo "$init"
journal=$(sed -n 's/.* journals=\([0-9]*\/[0-9]*\).*/\1/p' <<<"$init")
test -n "$journal"
compose run --rm parosctl --json write "$journal" hello world --owner 7
out=$(compose run --rm parosctl --json read "$journal")
echo "$out"
grep -q '"data":"hello"' <<<"$out"
grep -q '"data":"world"' <<<"$out"
echo "smoke: init, write and read answered"
