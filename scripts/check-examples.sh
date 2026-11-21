#!/usr/bin/env bash
# Fails when a composing crate of paros has no example (Pierre, 2026-11-21).
#
# A composing crate is a library crate of `crates/lib/` that depends on
# another library crate of the workspace: `paros` (the facade),
# `paros-journal` and `paros-journal-io` today, and later the moonpool adapter
# and the journal client. A user programs against these crates, so each one
# shows its abstractions and its developer experience in a runnable example.
# The examples are how a reviewer judges the API.
#
# The check: such a crate has at least one example, `examples/<name>.rs`, and
# the `examples` job of `.github/workflows/rust.yml` runs each example of it.
# A crate with no workspace library dependency (`paros-consensus`) is not
# composing; its examples are welcome but not required here.
set -euo pipefail

cd "$(dirname "$0")/.."

workflow=.github/workflows/rust.yml
status=0

libs=()
for manifest in crates/lib/*/Cargo.toml; do
  libs+=("$(basename "$(dirname "$manifest")")")
done

# Prints the names of the [dependencies] of a manifest (not dev or build).
normal_deps() {
  awk '
    /^\[/ { inside = ($0 == "[dependencies]") ; next }
    inside && /^[A-Za-z0-9_-]+[[:space:]]*=/ { sub(/[[:space:]]*=.*/, ""); print }
  ' "$1"
}

for crate in "${libs[@]}"; do
  manifest="crates/lib/$crate/Cargo.toml"
  composing=false
  for dep in $(normal_deps "$manifest"); do
    for lib in "${libs[@]}"; do
      [ "$dep" = "$lib" ] && composing=true
    done
  done
  "$composing" || continue

  shopt -s nullglob
  examples=("crates/lib/$crate/examples/"*.rs)
  shopt -u nullglob
  if [ "${#examples[@]}" -eq 0 ]; then
    echo "check-examples: $crate composes other paros crates and has no example (crates/lib/$crate/examples/<name>.rs)" >&2
    status=1
    continue
  fi
  for example in "${examples[@]}"; do
    name="$(basename "$example" .rs)"
    if ! grep -q -- "cargo run -p $crate --example $name\$" "$workflow"; then
      echo "check-examples: the example $name of $crate is not run by the examples job of $workflow" >&2
      status=1
    fi
  done
done

if [ "$status" -ne 0 ]; then
  echo "check-examples: every composing crate has examples, and CI runs each one (AGENTS.md, \"Composing crates have examples\")" >&2
fi
exit "$status"
