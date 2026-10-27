#!/usr/bin/env bash
# Fails when a paros-core module reaches up a layer (#730 (paros-core rework)).
#
# The layers point down only: foundations <- roles <- deployments.
# - A foundation uses only foundations.
# - A role uses foundations and roles, never a deployment.
# - A deployment uses foundations and roles, never another deployment.
# The check reads every `crate::<module>` path outside a comment. Test code
# (`tests.rs`, `tests/`, `models/`, `test_support.rs`) is exempt.
set -euo pipefail

src="$(dirname "$0")/../crates/lib/paros-core/src"

layer() {
  case "$1" in
    probe | types | membership | journal | contract | retained) echo 0 ;;
    acceptor | proposer | replica | quorum_read | batcher | collector | decree | matchmaking | matchmaker) echo 1 ;;
    node | proxy_leader | replica_node) echo 2 ;;
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
  # Every `crate::<module>` outside a `//` comment.
  while IFS=: read -r line target; do
    target_layer="$(layer "$target")"
    [ "$target_layer" = unknown ] && continue
    [ "$target" = "$own" ] && continue
    if [ "$target_layer" -gt "$own_layer" ] || { [ "$own_layer" = 2 ] && [ "$target_layer" = 2 ]; }; then
      echo "check-core-layers: $rel:$line: '$own' (layer $own_layer) uses crate::$target (layer $target_layer)" >&2
      status=1
    fi
  done < <(sed 's#//.*$##' "$file" | grep -noE 'crate::[a-z_]+' | sed 's/crate:://' | sort -u -t: -k2,2)
done < <(find "$src" -name '*.rs' | sort)

if [ "$status" -ne 0 ]; then
  echo "check-core-layers: a module reaches up a layer (crates/lib/paros-core/AGENTS.md)" >&2
fi
exit "$status"
