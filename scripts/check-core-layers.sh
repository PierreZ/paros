#!/usr/bin/env bash
# Fails when a paros-consensus module reaches up a layer (#730 (paros-consensus rework)).
#
# The layers point down only: foundations <- roles <- deployments.
# - A foundation uses only foundations.
# - A role uses foundations and roles, never a deployment.
# - A deployment uses foundations and roles, never another deployment.
#
# The check reads every `crate::<name>` path outside a comment. A lowercase name is a module of
# the crate root. An uppercase name is a re-export of the crate root (`crate::ColocatedNode`):
# the table `reexport` below maps it to the module that defines it, and a re-export with no row
# fails. Two forms hide the module that a path reaches, so they fail outside test code: a grouped
# root import (`use crate::{..}`) and a path that climbs two levels (`super::super::`). Write
# each upward-looking import as `crate::<module>::..`. Test code (`tests.rs`, `tests/`,
# `models/`, `test_support.rs`) is exempt.
#
# `scripts/check-core-layers.sh <src>` checks another source tree. `--self-test` runs the check
# on the fixtures of `scripts/fixtures/core-layers/`: `good/` must pass and each case of `bad/`
# must fail (#907 (core unit tests, invariants, mutation scope, layer check)). CI runs both.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"

if [ "${1:-}" = "--self-test" ]; then
  fixtures="$here/fixtures/core-layers"
  status=0
  if ! "$0" "$fixtures/good" >/dev/null 2>&1; then
    echo "check-core-layers --self-test: the good fixture fails" >&2
    "$0" "$fixtures/good" || true
    status=1
  fi
  cases=0
  for case in "$fixtures"/bad/*/; do
    cases=$((cases + 1))
    if "$0" "${case%/}" >/dev/null 2>&1; then
      echo "check-core-layers --self-test: the bad fixture ${case#"$fixtures"/} passes" >&2
      status=1
    fi
  done
  if [ "$cases" -eq 0 ]; then
    echo "check-core-layers --self-test: no bad fixture found in $fixtures/bad" >&2
    status=1
  fi
  [ "$status" -eq 0 ] && echo "check-core-layers --self-test: the good fixture passes, $cases bad fixtures fail"
  exit "$status"
fi

src="${1:-$here/../crates/lib/paros-consensus/src}"

layer() {
  case "$1" in
    probe | types | membership | journal | contract | retained) echo 0 ;;
    acceptor | belief | proposer | replica | quorum_read | batcher | collector | decree | matchmaking | matchmaker | publisher) echo 1 ;;
    node | proxy_leader | replica_node) echo 2 ;;
    *) echo "unknown" ;;
  esac
}

# The module of each re-export of `lib.rs` (`pub use <module>::..`).
reexport() {
  case "$1" in
    Config | HardState | Limits | Message | Storage | WriteOp) echo contract ;;
    Matchmaker | MatchmakerReconfigurer) echo matchmaker ;;
    ColocatedNode | Ready) echo node ;;
    ProxyLeader) echo proxy_leader ;;
    ReplicaNode) echo replica_node ;;
    Ballot | FNV_OFFSET | NodeId | Slot | Value | fnv1a) echo types ;;
    *) echo "unknown" ;;
  esac
}

status=0
while IFS= read -r file; do
  rel="${file#"$src"/}"
  case "$rel" in
    lib.rs | models.rs | models/* | test_support.rs | */tests.rs | */tests/*) continue ;;
  esac
  own="${rel%%/*}"
  own="${own%.rs}"
  own_layer="$(layer "$own")"
  if [ "$own_layer" = unknown ]; then
    echo "check-core-layers: $rel: module '$own' has no layer; add it to this script" >&2
    status=1
    continue
  fi
  code="$(sed 's#//.*$##' "$file")"
  # The two forms that hide the module a path reaches.
  while IFS=: read -r line _; do
    [ -n "$line" ] || continue
    echo "check-core-layers: $rel:$line: a grouped 'use crate::{..}' hides its modules; import each as crate::<module>::.." >&2
    status=1
  done < <(grep -nE 'crate::\{' <<<"$code" || true)
  while IFS=: read -r line _; do
    [ -n "$line" ] || continue
    echo "check-core-layers: $rel:$line: 'super::super::' hides the module it reaches; write crate::<module>::.." >&2
    status=1
  done < <(grep -nE 'super::super::' <<<"$code" || true)
  # Every `crate::<name>`: a module, or a re-export of the root.
  while IFS=: read -r line name; do
    [ -n "$line" ] || continue
    target="$name"
    if [[ "$name" =~ ^[A-Z] ]] || [ "$name" = fnv1a ]; then
      target="$(reexport "$name")"
      if [ "$target" = unknown ]; then
        echo "check-core-layers: $rel:$line: crate::$name is a re-export with no module; add it to the table of this script" >&2
        status=1
        continue
      fi
    fi
    target_layer="$(layer "$target")"
    if [ "$target_layer" = unknown ]; then
      # A lowercase name that is no module is a re-export of a function or
      # a macro: it hides its module like an uppercase one.
      echo "check-core-layers: $rel:$line: crate::$name is neither a module with a layer nor a re-export of the table; add it to this script" >&2
      status=1
      continue
    fi
    [ "$target" = "$own" ] && continue
    if [ "$target_layer" -gt "$own_layer" ] || { [ "$own_layer" = 2 ] && [ "$target_layer" = 2 ]; }; then
      echo "check-core-layers: $rel:$line: '$own' (layer $own_layer) uses crate::$name (layer $target_layer)" >&2
      status=1
    fi
  done < <(grep -noE 'crate::[A-Za-z_][A-Za-z0-9_]*' <<<"$code" | sed 's/crate:://' | sort -u -t: -k2,2 || true)
done < <(find "$src" -name '*.rs' | sort)

if [ "$status" -ne 0 ]; then
  echo "check-core-layers: a module reaches up a layer (crates/lib/paros-consensus/AGENTS.md)" >&2
fi
exit "$status"
