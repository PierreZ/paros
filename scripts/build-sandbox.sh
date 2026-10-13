#!/usr/bin/env bash
# Build the paros sandbox (#518 (Paxos visualization sandbox)) and stage it into the site
# at /sandbox/app/.
#
#   scripts/build-sandbox.sh             wasm + page, staged into website/zola/public/sandbox/app/
#   scripts/build-sandbox.sh --wasm-only  only the wasm-bindgen output, for a local preview
#                                         (serve website/sandbox/ with any static server)
#
# Run inside `nix develop` (wasm-bindgen-cli and wasm-opt come from the flake). The
# wasm-bindgen CLI version must equal the `wasm-bindgen` crate pin in
# crates/paros-sandbox/Cargo.toml. `website/zola/build.sh` must have run first when staging.
set -euo pipefail
cd "$(dirname "$0")/.."

WEB=website/sandbox
PKG="$WEB/wasm"

echo "building paros-sandbox for wasm32-unknown-unknown…"
cargo build --release --target wasm32-unknown-unknown -p paros-sandbox --lib
rm -rf "$PKG"
wasm-bindgen --target web --no-typescript --out-dir "$PKG" \
  target/wasm32-unknown-unknown/release/paros_sandbox.wasm
if command -v wasm-opt >/dev/null 2>&1; then
  wasm-opt -Os --enable-bulk-memory --enable-nontrapping-float-to-int \
    -o "$PKG/paros_sandbox_bg.wasm" "$PKG/paros_sandbox_bg.wasm"
fi
echo "wasm staged in $PKG"

if [ "${1:-}" = "--wasm-only" ]; then
  exit 0
fi

if [ ! -d website/zola/public/sandbox ]; then
  echo "website/zola/public/sandbox/ is missing: run \`website/zola/build.sh\` first" >&2
  exit 1
fi
rm -rf website/zola/public/sandbox/app
mkdir -p website/zola/public/sandbox/app
cp "$WEB"/index.html "$WEB"/main.js "$WEB"/stage.js "$WEB"/worker.js "$WEB"/style.css website/zola/public/sandbox/app/
cp -r "$PKG" website/zola/public/sandbox/app/wasm
echo "done: website/zola/public/sandbox/app/"
