#!/usr/bin/env bash
# Fails when a library crate of paros gains a runtime, I/O or storage dependency
# (Pierre, 2026-11-21; docs/architecture/04-roles.md §4.4).
#
# paros is a library: a program uses it by implementing its traits (storage,
# transport, clock) and driving its pure state machines. The pure crates below
# therefore depend on nothing that does I/O or schedules work: no async runtime
# (tokio, compio, smol, ...), no storage engine, no network or file crate.
#
# Three checks (build dependencies are free):
# - With no feature on, a pure crate depends on workspace crates only.
# - With every feature on, it depends on workspace crates and on the crates of
#   its observation features only (`serde`, `tracing`, `assertions`). These
#   features only observe: they decide nothing and do no I/O.
# - The examples of a pure crate build with its dev-dependencies, and they are
#   the proof that a program uses the crate with no runtime (#1370 (journal
#   examples), whose crates were removed on 2026-11-24). So with no feature on, its normal and dev dependencies
#   together are workspace crates only.
#
# The facade `paros` is checked with its default features: an adapter (a
# moonpool host) is an optional feature of the
# facade, off by default, and lives in its own crate.
#
# The pure crates are the crates of layers 0 to 5 of the layer table,
# `scripts/layers.sh` (docs/architecture/04-roles.md §4.4): a new crate of
# those layers is checked here as soon as it has its row. A crate that does
# I/O (an adapter, such as a moonpool host) is in layer 6 or above.
set -euo pipefail

cd "$(dirname "$0")/.."
root="$(pwd)"

# shellcheck source=scripts/layers.sh
. scripts/layers.sh
mapfile -t PURE < <(crates_in_layers 0 "$PURE_TOP")
FACADE=paros

# The crates that the observation features of the pure crates pull in.
OBSERVATION=(
  moonpool-assertions
  once_cell
  pin-project-lite
  proc-macro2
  quote
  serde
  serde_core
  serde_derive
  syn
  tracing
  tracing-attributes
  tracing-core
  unicode-ident
)

# Prints one line per crate outside the workspace: "name version".
# The edge kinds are `normal` unless the caller adds `-e dev`.
external_deps() {
  cargo tree -q -e normal --prefix none -f '{p}' "$@" |
    sed 's/ (\*)$//' |
    grep -v " ($root/" |
    sort -u || true
}

is_observation() {
  local name="$1" o
  for o in "${OBSERVATION[@]}"; do
    [ "$name" = "$o" ] && return 0
  done
  return 1
}

status=0

for crate in "${PURE[@]}"; do
  bare="$(external_deps -p "$crate" --no-default-features)"
  if [ -n "$bare" ]; then
    echo "check-core-deps: $crate with no feature depends on a crate outside the workspace:" >&2
    echo "$bare" | sed 's/^/  /' >&2
    status=1
  fi
  examples="$(external_deps -e dev -p "$crate" --no-default-features)"
  if [ -n "$examples" ]; then
    echo "check-core-deps: the examples of $crate build with a crate outside the workspace:" >&2
    echo "$examples" | sed 's/^/  /' >&2
    status=1
  fi
done

check_observation_only() {
  local label="$1"
  shift
  local line name
  while IFS= read -r line; do
    [ -z "$line" ] && continue
    name="${line%% *}"
    if ! is_observation "$name"; then
      echo "check-core-deps: $label depends on '$line', which is not an observation crate" >&2
      status=1
    fi
  done <<< "$(external_deps "$@")"
}

for crate in "${PURE[@]}"; do
  check_observation_only "$crate (every feature on)" -p "$crate" --all-features
done
check_observation_only "$FACADE (default features)" -p "$FACADE"

if [ "$status" -ne 0 ]; then
  echo "check-core-deps: a pure crate must not depend on a runtime, I/O or storage crate (docs/architecture/04-roles.md §4.4)" >&2
fi
exit "$status"
