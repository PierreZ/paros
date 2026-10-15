#!/usr/bin/env bash
# Build the seed widget (#307 (paros-sim in wasm)): one seed of the paros simulation in the
# browser, and stage it into the site at /sim-seed/.
#
#   scripts/build-sim-seed.sh              wasm + widget, staged into website/zola/public/sim-seed/
#   scripts/build-sim-seed.sh --wasm-only  only the wasm-bindgen output in website/sim-seed/wasm/
#   scripts/build-sim-seed.sh --check SEED...
#                                          run each seed natively (paros-sim-seed) and under node
#                                          (the same wasm), and fail unless the JSON is identical
#
# Run inside `nix develop` (wasm-bindgen-cli, wasm-opt, node and a clang for ring's C code come
# from the flake). The wasm-bindgen CLI version must equal the `wasm-bindgen` crate pin in
# crates/paros-sim-web/Cargo.toml. `website/zola/build.sh` must have run first when staging.
set -euo pipefail
cd "$(dirname "$0")/.."

WEB=website/sim-seed
PKG="$WEB/wasm"
WASM=target/wasm32-unknown-unknown/release/paros_sim_web.wasm

echo "building paros-sim-web for wasm32-unknown-unknown…"
cargo build --release --target wasm32-unknown-unknown -p paros-sim-web --lib

if [ "${1:-}" = "--check" ]; then
  shift
  [ "$#" -gt 0 ] || { echo "usage: $0 --check SEED..." >&2; exit 2; }
  out=$(mktemp -d)
  trap 'rm -rf "$out"' EXIT
  wasm-bindgen --target nodejs --no-typescript --out-dir "$out/node" "$WASM"
  cargo run --release -q -p paros-sim-web --bin paros-sim-seed -- "$@" > "$out/native.jsonl"
  node -e '
    const m = require(process.argv[1]);
    for (const seed of process.argv.slice(2)) process.stdout.write(m.runSeed(seed) + "\n");
  ' "$out/node/paros_sim_web.js" "$@" > "$out/wasm.jsonl"
  if ! cmp -s "$out/native.jsonl" "$out/wasm.jsonl"; then
    echo "a seed runs differently in wasm than natively:" >&2
    diff <(cut -c1-200 "$out/native.jsonl") <(cut -c1-200 "$out/wasm.jsonl") >&2 || true
    exit 1
  fi
  echo "$# seeds: the wasm run is byte for byte the native run"
  exit 0
fi

rm -rf "$PKG"
wasm-bindgen --target web --no-typescript --out-dir "$PKG" "$WASM"
if command -v wasm-opt >/dev/null 2>&1; then
  wasm-opt -Os --enable-bulk-memory --enable-nontrapping-float-to-int \
    -o "$PKG/paros_sim_web_bg.wasm" "$PKG/paros_sim_web_bg.wasm"
fi
echo "wasm staged in $PKG"

if [ "${1:-}" = "--wasm-only" ]; then
  exit 0
fi

if [ ! -d website/zola/public ]; then
  echo "website/zola/public/ is missing: run \`website/zola/build.sh\` first" >&2
  exit 1
fi
rm -rf website/zola/public/sim-seed
mkdir -p website/zola/public/sim-seed
cp "$WEB"/seed.js "$WEB"/seed.css "$WEB"/worker.js website/zola/public/sim-seed/
cp -r "$PKG" website/zola/public/sim-seed/wasm
echo "done: website/zola/public/sim-seed/"
