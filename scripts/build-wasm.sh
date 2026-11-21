#!/usr/bin/env bash
# Build the site's wasm tools from the crate `paros-wasm` (#540), one bundle per
# cargo feature: `paxos` (bundle paros_paxos, paros-consensus only), the engine
# of the step-through diagrams of the Paxos, Consensus and Engineering pages, and
# `journal` (bundle paros_journal_lab, paros-journal only), the fold lab of the
# Journal pages (#1363).
#
#   scripts/build-wasm.sh              build, then stage it into
#                                      website/site/public/paxos/wasm/ and
#                                      website/site/public/journal/wasm/
#                                      (run it before `npm run build` in
#                                      website/site)
#   scripts/build-wasm.sh --wasm-only  only the wasm-bindgen output
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

build paxos target/wasm-tools/paxos paros_paxos --no-typescript
build journal target/wasm-tools/journal paros_journal_lab --no-typescript

if [ "${1:-}" = "--wasm-only" ]; then
  exit 0
fi

PAXOS=website/site/public/paxos/wasm
echo "staging paros-consensus (paxos) into $PAXOS/ for the Paxos pages (#811)…"
rm -rf "$PAXOS"
mkdir -p "$PAXOS"
cp target/wasm-tools/paxos/paros_paxos.js target/wasm-tools/paxos/paros_paxos_bg.wasm "$PAXOS/"

JOURNAL=website/site/public/journal/wasm
echo "staging paros-journal (journal) into $JOURNAL/ for the Journal pages (#1363)…"
rm -rf "$JOURNAL"
mkdir -p "$JOURNAL"
cp target/wasm-tools/journal/paros_journal_lab.js target/wasm-tools/journal/paros_journal_lab_bg.wasm "$JOURNAL/"
