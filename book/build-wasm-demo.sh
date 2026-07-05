#!/usr/bin/env bash
# Build the wasm paros demo and stage it as a book asset so the wasm chapter can
# embed it live. Run before `mdbook build` (locally and in CI). The staged
# directory book/src/wasm-demo/ is generated and gitignored.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "building paros-wasm-demo for wasm32…"
cargo build --release --target wasm32-unknown-unknown -p paros-wasm-demo --lib
wasm-bindgen --target web --out-dir paros-wasm-demo/web/pkg \
  target/wasm32-unknown-unknown/release/paros_wasm_demo.wasm

# Best-effort: stage the IBM Plex Sans/Mono woff2 the design system asks for, so
# the widget renders in Plex rather than the system fallback. Never fatal — if the
# fonts can't be found, tokens.js falls back to system-ui / ui-monospace and the
# demo still builds and works. Set PLEX_WOFF2_DIR to a directory of *.woff2 to use
# your own; otherwise we try nixpkgs#ibm-plex (which ships woff2 under a
# fonts/ibm-plex/*/woff2/ tree). See paros-wasm-demo/README.md.
stage_fonts() {
  local out="paros-wasm-demo/web/app/fonts"
  mkdir -p "$out"
  local src="${PLEX_WOFF2_DIR:-}"
  if [ -z "$src" ] && command -v nix >/dev/null 2>&1; then
    local plex
    plex="$(nix build --no-link --print-out-paths nixpkgs#ibm-plex 2>/dev/null || true)"
    [ -n "$plex" ] && src="$plex"
  fi
  local want=(IBMPlexSans-Regular IBMPlexSans-Bold IBMPlexMono-Regular IBMPlexMono-Bold)
  local ok=1
  for f in "${want[@]}"; do
    if [ -n "$src" ]; then
      local found
      found="$(find -L "$src" -name "${f}.woff2" 2>/dev/null | head -n1 || true)"
      if [ -n "$found" ]; then cp "$found" "$out/${f}.woff2"; continue; fi
    fi
    ok=0
  done
  if [ "$ok" = 1 ]; then echo "staged IBM Plex woff2."; else
    echo "note: IBM Plex woff2 not staged — using system-ui / ui-monospace fallback."; fi
}
stage_fonts

echo "staging assets in book/src/wasm-demo/…"
rm -rf book/src/wasm-demo
mkdir -p book/src/wasm-demo
cp paros-wasm-demo/web/index.html book/src/wasm-demo/index.html
cp -r paros-wasm-demo/web/app book/src/wasm-demo/app
cp -r paros-wasm-demo/web/pkg book/src/wasm-demo/pkg

echo "done. book/src/wasm-demo/ ready for mdbook build."
