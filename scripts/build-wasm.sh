#!/usr/bin/env bash
# Build the site's wasm tools from the one crate `paros-wasm` (#540), one bundle
# per cargo feature, and stage them into the site.
#
#   scripts/build-wasm.sh              both tools: wasm + pages, staged into
#                                      website/zola/public/play/ and
#                                      website/zola/public/sandbox/app/
#   scripts/build-wasm.sh --wasm-only  only the wasm-bindgen output of both tools,
#                                      for `npm run dev` and a local preview
#
# The `paxos` feature is the Paxos game (website/play/, bundle paros_paxos); the
# `playground` feature is the paros playground (website/sandbox/, bundle
# paros_playground). Each is built alone, so a Paxos page never downloads the
# simulator.
#
# Run inside `nix develop` (wasm-bindgen-cli, wasm-opt, node and npm come from
# the flake). The wasm-bindgen CLI version must equal the `wasm-bindgen` crate
# pin in crates/support/paros-wasm/Cargo.toml; the flake and the pin are bumped
# together. `website/zola/build.sh` must have run first when staging
# (website/zola/public/ is its output).
set -euo pipefail
cd "$(dirname "$0")/.."

WASM=target/wasm32-unknown-unknown/release/paros_wasm.wasm

# build <feature> <out-dir> <out-name> [wasm-bindgen flags...]
build() {
  local feature=$1 out=$2 name=$3
  shift 3
  echo "building paros-wasm (${feature}) for wasm32-unknown-unknown…"
  # Compile without the sancov RUSTC_WRAPPER's instrumentation (SANCOV_CRATES
  # is unset here, so the wrapper is a plain passthrough).
  cargo build --release --target wasm32-unknown-unknown -p paros-wasm --lib \
    --no-default-features --features "$feature"
  rm -rf "$out"
  wasm-bindgen --target web --out-dir "$out" --out-name "$name" "$@" "$WASM"
  if command -v wasm-opt >/dev/null 2>&1; then
    wasm-opt -Os --enable-bulk-memory --enable-nontrapping-float-to-int \
      -o "$out/${name}_bg.wasm" "$out/${name}_bg.wasm"
  fi
  echo "wasm staged in $out"
}

build paxos website/play/src/wasm paros_paxos
build playground website/sandbox/wasm paros_playground --no-typescript

if [ "${1:-}" = "--wasm-only" ]; then
  exit 0
fi

if [ ! -d website/zola/public/sandbox ]; then
  echo "website/zola/public/sandbox/ is missing: run \`website/zola/build.sh\` first" >&2
  exit 1
fi

echo "building the game's web app…"
(cd website/play && npm ci --no-audit --no-fund && npm run build)
echo "staging the game into website/zola/public/play/…"
rm -rf website/zola/public/play
cp -r website/play/dist website/zola/public/play

echo "staging the playground into website/zola/public/sandbox/app/…"
rm -rf website/zola/public/sandbox/app
mkdir -p website/zola/public/sandbox/app
cp website/sandbox/index.html website/sandbox/main.js website/sandbox/stage.js \
  website/sandbox/worker.js website/sandbox/style.css website/zola/public/sandbox/app/
cp -r website/sandbox/wasm website/zola/public/sandbox/app/wasm
# The shared tokens, components and fonts (#804 (flat engineering redesign)).
# website/sandbox/shared is a link to them for a local preview.
cp -rL website/shared website/zola/public/sandbox/app/shared
echo "done: website/zola/public/play/ and website/zola/public/sandbox/app/"
