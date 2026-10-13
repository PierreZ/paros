#!/usr/bin/env bash
# Build the paros site (website/zola/, Zola + Goyo, #254) into website/zola/public/.
#
#   website/zola/build.sh          zola build
#   website/zola/build.sh serve    zola serve (live preview)
#
# Run inside `nix develop`: the flake provides `zola` and exports PAROS_GOYO, the store
# path of the Goyo theme pinned by rev in flake.nix. The theme is copied into
# website/zola/themes/goyo here (no git submodule), so the build is reproducible through Nix.
set -euo pipefail
cd "$(dirname "$0")/../.."

if [ -z "${PAROS_GOYO:-}" ] || [ ! -f "$PAROS_GOYO/theme.toml" ]; then
  echo "PAROS_GOYO is unset or is not a Goyo checkout: run inside \`nix develop\`" >&2
  exit 1
fi
rm -rf website/zola/themes/goyo
mkdir -p website/zola/themes
cp -r "$PAROS_GOYO" website/zola/themes/goyo
chmod -R u+w website/zola/themes/goyo

cd website/zola
if [ "${1:-}" = "serve" ]; then
  exec zola serve
fi
zola build
