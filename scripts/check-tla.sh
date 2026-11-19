#!/usr/bin/env bash
# Run TLC, the TLA+ model checker, on the specs of specs/tla/ (#579 (zero-stall
# reconfiguration)). Each folder has a `checks.txt`: one line for each TLC
# configuration, with the result that it must give. A check that must find a
# violation is a control: it shows that the model can see the bug.
#
#   nix develop .#tla --command scripts/check-tla.sh          # the CI set
#   nix develop .#tla --command scripts/check-tla.sh --all    # the long ones too
#   nix develop .#tla --command scripts/check-tla.sh reads    # one folder
#
# Each argument that is not `--all` names a folder of specs/tla/; with no
# folder, every folder runs. CI runs one job for each folder. A violation in an
# initial state counts as a violation too (`grid/` checks constants only).
set -euo pipefail
cd "$(dirname "$0")/.."
all=false
dirs=()
for arg in "$@"; do
  if [ "$arg" = "--all" ]; then
    all=true
  else
    dirs+=("specs/tla/$arg")
  fi
done
if [ "${#dirs[@]}" -eq 0 ]; then
  for manifest in specs/tla/*/checks.txt; do
    dirs+=("$(dirname "$manifest")")
  done
fi
workers="${TLC_WORKERS:-auto}"
status=0
for dir in "${dirs[@]}"; do
  manifest="$dir/checks.txt"
  if [ ! -f "$manifest" ]; then
    echo "FAIL  $dir: no checks.txt" >&2
    status=1
    continue
  fi
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
    elif grep -qE "Error: (Invariant .* is violated(\.| by the initial state)|Temporal properties were violated\.)" "$log"; then
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
