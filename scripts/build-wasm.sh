#!/usr/bin/env bash
# Build the site's wasm tools from the one crate `paros-wasm` (#540), one bundle
# per cargo feature.
#
#   scripts/build-wasm.sh              both bundles, then stage paros-core (paxos)
#                                      into website/site/public/paxos/wasm/ and
#                                      the playground into
#                                      website/site/public/playground/app/
#                                      (run it before `npm run build` in
#                                      website/site, #810 (website redo 1/4))
#   scripts/build-wasm.sh --wasm-only  only the wasm-bindgen output of both tools
#
# The `playground` feature is the paros playground (website/sandbox/, bundle
# paros_playground), served at /playground/. The `paxos` feature (bundle
# paros_paxos, paros-core only) is built for the Paxos game's code in
# website/play/ (not on the site) and for the step-through diagrams of the
# Paxos pages, which record their runs with it in the browser (#811). Each is built alone, so a page never
# downloads the simulator by accident.
#
# Run inside `nix develop` (wasm-bindgen-cli, wasm-opt, node and npm come from
# the flake). The wasm-bindgen CLI version must equal the `wasm-bindgen` crate
# pin in crates/support/paros-wasm/Cargo.toml; the flake and the pin are bumped
# together.
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

PAXOS=website/site/public/paxos/wasm
echo "staging paros-core (paxos) into $PAXOS/ for the Paxos pages (#811)…"
rm -rf "$PAXOS"
mkdir -p "$PAXOS"
cp website/play/src/wasm/paros_paxos.js website/play/src/wasm/paros_paxos_bg.wasm "$PAXOS/"

APP=website/site/public/playground/app
echo "staging the playground into $APP/…"
rm -rf "$APP"
mkdir -p "$APP"
cp website/sandbox/main.js website/sandbox/stage.js website/sandbox/worker.js \
  website/sandbox/style.css "$APP/"
cp -r website/sandbox/wasm "$APP/wasm"
echo "done: $APP"
