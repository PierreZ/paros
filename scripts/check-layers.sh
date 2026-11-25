#!/usr/bin/env bash
# Fails when a crate of paros breaks the layering of the service
# (docs/architecture/04-roles.md §4.4, #1411 (CI checks that hold the layering)).
#
# The table of the layer of each crate is `scripts/layers.sh`. With no argument,
# the script reads the manifests through `cargo metadata --no-deps` (no build,
# a second or two) and the sources, and checks:
# 1. Every crate of the workspace has a row in the table, and every row names a
#    crate of the workspace.
# 2. A crate depends only on crates of a strictly lower layer: no dependency
#    points up or sideways. Normal, build and dev dependencies all count.
# 3. A crate of layers 0 to 6 never names an item of a higher crate in its
#    `src/`: the crate's identifier (`paros_wasm`, as in a `use`, a path
#    or a doc link) does not appear. The facade `paros` is not checked: it
#    defines no item of its own, so every `paros::` path is a re-export of a
#    lower layer. The prose name of a crate (`paros-wasm`) is not an
#    item, so a doc comment may cite it.
# 4. No fault switch in a pure crate (layers 0 to 5): no `buggify!` and no
#    fault `hint!` outside a comment. Faults come from the walk, from moonpool
#    or from an adapter.
#
# Layers 0 to 5 are also the pure crates of `scripts/check-core-deps.sh`, which
# reads the same table, so a new crate of those layers is checked there too.
#
# With `--wasm`, the script runs `cargo check --target wasm32-unknown-unknown`
# on each crate of layers 0 to 5 (the `checks` job of
# `.github/workflows/rust.yml`, in `nix develop`).
set -euo pipefail

cd "$(dirname "$0")/.."
# shellcheck source=scripts/layers.sh
. scripts/layers.sh

if [ "${1:-}" = --wasm ]; then
  while IFS= read -r crate; do
    echo "check-layers: cargo check --target wasm32-unknown-unknown -p $crate" >&2
    cargo check --target wasm32-unknown-unknown -p "$crate"
  done < <(crates_in_layers 0 "$PURE_TOP")
  exit 0
fi

status=0

# `cargo metadata` reads the manifests as Cargo does, so every way to write a
# dependency counts: a dotted key, a `[dependencies.<name>]` table, a target
# section, a comment after a header, and a rename with `package =` (the line
# carries the real package name). Prints "crate <name> <dir>" for each member
# and "dep <crate> <dependency>" for each of its dependencies, of every kind.
metadata="$(cargo metadata --no-deps --format-version 1 | python3 -c '
import json, os, sys
meta = json.load(sys.stdin)
root = meta["workspace_root"]
for pkg in meta["packages"]:
    print("crate", pkg["name"], os.path.relpath(os.path.dirname(pkg["manifest_path"]), root))
    for dep in pkg["dependencies"]:
        print("dep", pkg["name"], dep["name"])
')"

declare -A dir_of=()
while read -r _ name dir; do
  dir_of["$name"]="$dir"
done < <(grep '^crate ' <<<"$metadata")
if [ "${#dir_of[@]}" -eq 0 ]; then
  echo "check-layers: cargo metadata lists no crate" >&2
  exit 1
fi

# 1. The table and the workspace hold the same crates.
for crate in $(printf '%s\n' "${!dir_of[@]}" | sort); do
  if [ -z "$(layer_of "$crate")" ]; then
    echo "check-layers: ${dir_of[$crate]} ($crate) has no layer; add its row to scripts/layers.sh" >&2
    status=1
  fi
done
while IFS= read -r crate; do
  if [ -z "${dir_of[$crate]:-}" ]; then
    echo "check-layers: scripts/layers.sh has a row for $crate, which is not a crate of the workspace" >&2
    status=1
  fi
done < <(table_crates)

# 2. Every dependency on a workspace crate points strictly down.
while read -r _ crate dep; do
  [ -z "${dir_of[$dep]:-}" ] && continue
  own="$(layer_of "$crate")"
  target="$(layer_of "$dep")"
  { [ -z "$own" ] || [ -z "$target" ]; } && continue
  if [ "$target" -ge "$own" ]; then
    echo "check-layers: $crate (layer $own) depends on $dep (layer $target); a crate depends only on lower layers" >&2
    status=1
  fi
done < <(grep '^dep ' <<<"$metadata" | sort -u)

# Sets `src` to the source folder of a crate of the table. A crate with no
# `src/` fails (no subshell, so `status` keeps the failure).
src_of() {
  src=""
  [ -n "${dir_of[$1]:-}" ] || return 1
  if [ ! -d "${dir_of[$1]}/src" ]; then
    echo "check-layers: $1 has no src/ to check" >&2
    status=1
    return 1
  fi
  src="${dir_of[$1]}/src"
}

# 3. A crate of layers 0 to 6 names no item of a higher crate.
while IFS= read -r crate; do
  own="$(layer_of "$crate")"
  src_of "$crate" || continue
  while IFS= read -r higher; do
    [ "$higher" = paros ] && continue
    [ "$(layer_of "$higher")" -gt "$own" ] || continue
    ident="${higher//-/_}"
    while IFS= read -r hit; do
      echo "check-layers: $hit: $crate (layer $own) names $ident (layer $(layer_of "$higher"))" >&2
      status=1
    done < <(grep -rnwE --include='*.rs' "$ident" "$src" | cut -d: -f1,2 || true)
  done < <(table_crates)
done < <(crates_in_layers 0 6)

# 4. No fault switch in a pure crate.
while IFS= read -r crate; do
  src_of "$crate" || continue
  while IFS= read -r file; do
    while IFS=: read -r line _; do
      echo "check-layers: $file:$line: a fault switch in $crate, a pure crate (layer $(layer_of "$crate"))" >&2
      status=1
    done < <(sed 's#//.*$##' "$file" | grep -nE 'buggify!|hint!\(' || true)
  done < <(find "$src" -name '*.rs' | sort)
done < <(crates_in_layers 0 "$PURE_TOP")

if [ "$status" -ne 0 ]; then
  echo "check-layers: the layering of docs/architecture/04-roles.md §4.4 is broken (table: scripts/layers.sh)" >&2
fi
exit "$status"
