#!/usr/bin/env bash
# A seed of paros-sim is the same run on wasm32-unknown-unknown as natively
# (#307 (paros-sim in wasm)).
#
#   scripts/check-sim-wasm.sh <seed>...
#
# Builds `paros-sim-wasm-check` natively and for wasm32, runs the seeds through
# both (the wasm bundle under node), and fails when any line differs. Run
# inside `nix develop` (wasm-bindgen-cli, node and a clang for wasm32 come from
# the flake).
set -euo pipefail
cd "$(dirname "$0")/.."

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <seed>..." >&2
  exit 2
fi

OUT=target/sim-wasm-check
rm -rf "$OUT"
mkdir -p "$OUT"

echo "building paros-sim-wasm-check natively and for wasm32-unknown-unknown…"
cargo build --release -p paros-sim-wasm-check --bin sim-seed-fingerprint
cargo build --release --target wasm32-unknown-unknown -p paros-sim-wasm-check --lib
wasm-bindgen --target nodejs --no-typescript --out-dir "$OUT/pkg" \
  target/wasm32-unknown-unknown/release/paros_sim_wasm_check.wasm

echo "running seeds $* natively…"
target/release/sim-seed-fingerprint "$@" >"$OUT/native.txt"
echo "running seeds $* in wasm under node…"
node -e '
  const check = require(process.argv[1]);
  process.stdout.write(check.fingerprints(process.argv.slice(2).join(" ")));
' "$PWD/$OUT/pkg/paros_sim_wasm_check.js" "$@" >"$OUT/wasm.txt"

if ! diff -u "$OUT/native.txt" "$OUT/wasm.txt"; then
  echo "a seed ran differently on wasm32 than natively" >&2
  exit 1
fi
cat "$OUT/native.txt"
echo "every seed is the same run on wasm32 as natively"
