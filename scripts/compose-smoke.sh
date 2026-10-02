#!/usr/bin/env bash
# The toy's smoke test (#196): a fresh Compose cell answers `init`, one
# `parosctl write` and one `parosctl read`. Run after `docker compose build`.
set -euo pipefail
cd "$(dirname "$0")/.."
compose() { docker compose "$@"; }
trap 'compose down --volumes --remove-orphans >/dev/null 2>&1 || true' EXIT
compose up -d
compose run --rm init
compose run --rm parosctl --json write 256 hello world --owner 7
out=$(compose run --rm parosctl --json read 256)
echo "$out"
grep -q '"data":"hello"' <<<"$out"
grep -q '"data":"world"' <<<"$out"
echo "smoke: init, write and read answered"
