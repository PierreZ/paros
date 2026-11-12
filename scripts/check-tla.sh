#!/usr/bin/env bash
# Run TLC, the TLA+ model checker, on the specs of specs/tla/ (#579 (zero-stall
# reconfiguration)). Each folder has a `checks.txt`: one line for each TLC
# configuration, with the result that it must give. A check that must find a
# violation is a control: it shows that the model can see the bug.
#
#   nix develop .#tla --command scripts/check-tla.sh          # the CI set
#   nix develop .#tla --command scripts/check-tla.sh --all    # the long ones too
set -euo pipefail
cd "$(dirname "$0")/.."
all=false
[ "${1:-}" = "--all" ] && all=true
workers="${TLC_WORKERS:-auto}"
status=0
for manifest in specs/tla/*/checks.txt; do
  dir=$(dirname "$manifest")
  while read -r name expected where; do
    case "$name" in '' | '#'*) continue ;; esac
    if [ "$where" != ci ] && [ "$all" = false ]; then
      continue
    fi
    meta=$(mktemp -d)
    log="$meta/tlc.log"
    (cd "$dir" && tlc -workers "$workers" -deadlock -metadir "$meta/states" \
      -config "$name.cfg" MC.tla) >"$log" 2>&1 || true
    if grep -q "Model checking completed. No error has been found." "$log"; then
      got=pass
    elif grep -q "Error: Invariant .* is violated." "$log"; then
      got=violation
    else
      got=error
    fi
    states=$(grep -E "distinct states found" "$log" | tail -1 || true)
    if [ "$got" = "$expected" ]; then
      echo "ok    $dir/$name: $got. $states"
    else
      echo "FAIL  $dir/$name: expected $expected, got $got" >&2
      tail -40 "$log" >&2
      status=1
    fi
    rm -rf "$meta"
  done <"$manifest"
done
exit "$status"
