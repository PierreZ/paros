#!/usr/bin/env bash
# Fails when a library crate of paros gains a runtime, I/O or storage dependency
# (Pierre, 2026-11-21; docs/journal/04-roles.md §J4.3).
#
# paros is a library: a program uses it by implementing its traits (storage,
# transport, clock) and driving its pure state machines. The pure crates below
# therefore depend on nothing that does I/O or schedules work: no async runtime
# (tokio, compio, smol, ...), no storage engine, no network or file crate.
#
# Two checks, on the normal dependencies only (dev and build dependencies are
# free):
# - With no feature on, a pure crate depends on workspace crates only.
# - With every feature on, it depends on workspace crates and on the crates of
#   its observation features only (`serde`, `tracing`, `assertions`). These
#   features only observe: they decide nothing and do no I/O.
#
# The facade `paros` is checked with its default features: an adapter (the
# moonpool host, #1330 (paros-journal-moonpool)) is an optional feature of the
# facade, off by default, and lives in its own crate.
#
# A new pure crate of the journal (paros-journal-io, paros-journal-control, the
# client) joins PURE in the pull request that creates it. A crate that does I/O
# (an adapter such as paros-journal-moonpool) never joins it.
set -euo pipefail

cd "$(dirname "$0")/.."
root="$(pwd)"

PURE=(paros-consensus paros-journal)
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
  echo "check-core-deps: a pure crate must not depend on a runtime, I/O or storage crate (docs/journal/04-roles.md §J4.3)" >&2
fi
exit "$status"
