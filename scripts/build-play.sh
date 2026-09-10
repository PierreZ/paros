#!/usr/bin/env bash
# Build paros play — the interactive Paxos game — and stage it beside the book.
#
#   scripts/build-play.sh            wasm + web app, staged into book/output/play/
#   scripts/build-play.sh --wasm-only  only the wasm-bindgen output (for `npm run dev`)
#
# Run inside `nix develop` (wasm-bindgen-cli, wasm-opt, node and npm come from the
# flake). The wasm-bindgen CLI version must equal the `wasm-bindgen` crate pin in
# crates/paros-play/Cargo.toml; the flake and the pin are bumped together.
# `mdbook build` must have run first when staging (book/output/ is its output).
set -euo pipefail
cd "$(dirname "$0")/.."

WEB=web/play
PKG="$WEB/src/wasm"

echo "building paros-play for wasm32-unknown-unknown…"
# Compile the game engine without the sancov RUSTC_WRAPPER's instrumentation
# (SANCOV_CRATES is unset here, so the wrapper is a plain passthrough).
cargo build --release --target wasm32-unknown-unknown -p paros-play --lib
rm -rf "$PKG"
wasm-bindgen --target web --out-dir "$PKG" \
  target/wasm32-unknown-unknown/release/paros_play.wasm
if command -v wasm-opt >/dev/null 2>&1; then
  wasm-opt -Os -o "$PKG/paros_play_bg.wasm" "$PKG/paros_play_bg.wasm"
fi
echo "wasm staged in $PKG"

if [ "${1:-}" = "--wasm-only" ]; then
  exit 0
fi

echo "building the web app…"
(cd "$WEB" && npm ci --no-audit --no-fund && npm run build)

echo "staging into book/output/play/…"
if [ ! -d book/output ]; then
  echo "book/output/ is missing: run \`mdbook build\` first" >&2
  exit 1
fi
rm -rf book/output/play
cp -r "$WEB/dist" book/output/play
echo "done: book/output/play/"
