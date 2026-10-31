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

# The "Updated" date of each page: the day of the last commit that touched it
# (#804 (flat engineering redesign)). The templates read it with load_data. A
# shallow clone gives one day for every page, so the Pages workflow fetches
# the whole history.
{
  echo "# Written by website/zola/build.sh. Do not edit."
  find content -name '*.md' | sort | while read -r f; do
    day=$(git log -1 --format=%cs -- "$f" 2>/dev/null || true)
    if [ -n "$day" ]; then
      echo "\"${f#content/}\" = \"$day\""
    fi
  done
} > updated.toml

if [ "${1:-}" = "serve" ]; then
  exec zola serve
fi
zola build
