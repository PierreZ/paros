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
# fonts can't be staged, the @font-face block is stripped from the *staged*
# index.html (the plex-fonts markers) so the book never 404s, and the CSS stacks
# fall back to system-ui / ui-monospace. Set PLEX_WOFF2_DIR to a directory of
# *.woff2 to use your own; otherwise we use nixpkgs#ibm-plex, which ships OTF
# only, so each face is converted with woff2_compress (nixpkgs#woff2).
FONTS_OK=1
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
  for f in "${want[@]}"; do
    [ -s "$out/${f}.woff2" ] && continue # already staged by an earlier build
    if [ -n "$src" ]; then
      local found
      found="$(find -L "$src" -name "${f}.woff2" 2>/dev/null | head -n1 || true)"
      if [ -n "$found" ]; then cp "$found" "$out/${f}.woff2"; continue; fi
      # OTF-only source (nixpkgs#ibm-plex): copy out of the read-only store and convert
      found="$(find -L "$src" -name "${f}.otf" 2>/dev/null | head -n1 || true)"
      if [ -n "$found" ] && command -v nix >/dev/null 2>&1 \
        && cp "$found" "$out/${f}.otf" 2>/dev/null \
        && nix shell nixpkgs#woff2 -c woff2_compress "$out/${f}.otf" >/dev/null 2>&1 \
        && [ -s "$out/${f}.woff2" ]; then
        rm -f "$out/${f}.otf"
        continue
      fi
      rm -f "$out/${f}.otf"
    fi
    FONTS_OK=0
  done
  if [ "$FONTS_OK" = 1 ]; then echo "staged IBM Plex woff2."; else
    echo "note: IBM Plex woff2 not staged — using system-ui / ui-monospace fallback."; fi
}
stage_fonts

echo "staging assets in book/src/wasm-demo/…"
rm -rf book/src/wasm-demo
mkdir -p book/src/wasm-demo
cp paros-wasm-demo/web/index.html book/src/wasm-demo/index.html
cp -r paros-wasm-demo/web/app book/src/wasm-demo/app
cp -r paros-wasm-demo/web/pkg book/src/wasm-demo/pkg
if [ "$FONTS_OK" != 1 ]; then
  # no fonts staged: drop the @font-face block so the book page loads without 404s
  sed -i '/plex-fonts-begin/,/plex-fonts-end/d' book/src/wasm-demo/index.html
fi

echo "done. book/src/wasm-demo/ ready for mdbook build."
