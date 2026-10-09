#!/usr/bin/env bash
# Build the paros site (web/site/, Zola + Goyo, #254) into web/site/public/.
#
#   web/site/build.sh          zola build
#   web/site/build.sh serve    zola serve (live preview)
#
# Run inside `nix develop`: the flake provides `zola` and exports PAROS_GOYO, the store
# path of the Goyo theme pinned by rev in flake.nix. The theme is copied into
# web/site/themes/goyo here (no git submodule), so the build is reproducible through Nix.
set -euo pipefail
cd "$(dirname "$0")/../.."

if [ -z "${PAROS_GOYO:-}" ] || [ ! -f "$PAROS_GOYO/theme.toml" ]; then
  echo "PAROS_GOYO is unset or is not a Goyo checkout: run inside \`nix develop\`" >&2
  exit 1
fi
rm -rf web/site/themes/goyo
mkdir -p web/site/themes
cp -r "$PAROS_GOYO" web/site/themes/goyo
chmod -R u+w web/site/themes/goyo

cd web/site
if [ "${1:-}" = "serve" ]; then
  exec zola serve
fi
zola build
