#!/usr/bin/env bash
# Fails when a crate of paros breaks the layering of the service
# (docs/architecture/04-roles.md §4.4, #1411 (CI checks that hold the layering)).
#
# The table of the layer of each crate is `scripts/layers.sh`. With no argument,
# the script reads only files (no cargo, a second or less), and checks:
# 1. Every crate of the workspace has a row in the table, and every row names a
#    crate of the workspace.
# 2. A crate depends only on crates of a strictly lower layer: no dependency
#    points up or sideways. Normal, build and dev dependencies all count.
# 3. A crate of layers 0 to 6 never names an item of a higher crate in its
#    `src/`: the crate's identifier (`paros_journal_io`, as in a `use`, a path
#    or a doc link) does not appear. The facade `paros` is not checked: it
#    defines no item of its own, so every `paros::` path is a re-export of a
#    lower layer. The prose name of a crate (`paros-journal-io`) is not an
#    item, so a doc comment may cite it.
# 4. No fault switch in a pure crate (layers 0 to 5): no `buggify!` and no
#    fault `hint!` outside a comment. Faults come from the walk, from moonpool
#    or from an adapter.
#
# Layers 0 to 5 are also the pure crates of `scripts/check-core-deps.sh`, which
# reads the same table, so a new crate of those layers is checked there too.
#
# With `--wasm`, the script runs `cargo check --target wasm32-unknown-unknown`
# on each crate of layers 0 to 5 (the portability job of
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

# Prints the package name of a manifest.
package_name() {
  awk '
    /^\[/ { inside = ($0 == "[package]"); next }
    inside && /^name[[:space:]]*=/ { gsub(/.*=[[:space:]]*"|".*/, ""); print; exit }
  ' "$1"
}

# Prints the dependency names of a manifest: [dependencies], [dev-dependencies],
# [build-dependencies], their `target.<cfg>` forms and the `[dependencies.<name>]`
# table form.
all_deps() {
  awk '
    /^\[/ {
      header = $0
      inside = 0
      if (header ~ /^\[(target\.[^]]*\.)?(dev-|build-)?dependencies\]$/) { inside = 1; next }
      if (header ~ /^\[(target\.[^]]*\.)?(dev-|build-)?dependencies\.[A-Za-z0-9_-]+\]$/) {
        sub(/^.*dependencies\./, "", header); sub(/\]$/, "", header); print header
      }
      next
    }
    inside && /^[A-Za-z0-9_-]+[[:space:]]*=/ { sub(/[[:space:]]*=.*/, ""); print }
  ' "$1"
}

declare -A dir_of=()
while IFS= read -r manifest; do
  dir="$(dirname "$manifest")"
  dir_of["$(package_name "$manifest")"]="$dir"
done < <(find crates -mindepth 3 -maxdepth 3 -name Cargo.toml | sort)

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
for crate in $(printf '%s\n' "${!dir_of[@]}" | sort); do
  own="$(layer_of "$crate")"
  [ -z "$own" ] && continue
  while IFS= read -r dep; do
    [ -z "${dir_of[$dep]:-}" ] && continue
    target="$(layer_of "$dep")"
    [ -z "$target" ] && continue
    if [ "$target" -ge "$own" ]; then
      echo "check-layers: $crate (layer $own) depends on $dep (layer $target); a crate depends only on lower layers" >&2
      status=1
    fi
  done < <(all_deps "${dir_of[$crate]}/Cargo.toml" | sort -u)
done

# 3. A crate of layers 0 to 6 names no item of a higher crate.
while IFS= read -r crate; do
  own="$(layer_of "$crate")"
  src="${dir_of[$crate]:-}/src"
  [ -d "$src" ] || continue
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
  src="${dir_of[$crate]:-}/src"
  [ -d "$src" ] || continue
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
