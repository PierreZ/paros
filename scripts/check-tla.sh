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
# A line of `checks.txt` can end with a time bound in minutes, for a long
# check that does not finish: `<name> pass long <minutes>`. TLC stops at the
# bound, and the line is `ok` when no violation was found until then
# (`bounded`). A line with no bound must finish.
#
# A line can also end with `seed=<n>`: TLC runs with `-seed <n>`. A check
# whose initial states are random draws (`RandomElement`, the inductive checks
# of multi-paxos/, #1307) gives the seed, so that each run draws the same
# states.
#
# Each argument that is not `--all` names a folder of specs/tla/; with no
# folder, every folder of a model of the plan runs. An experiment of
# specs/tla/experiments/ runs only when it is named, for example
# `scripts/check-tla.sh experiments/segments/a`. CI checks the folders that a
# change can break, the experiments too (`scripts/ci-plan.py`). A violation in an initial state counts as a
# violation too (`grid/` checks constants only), and so does a violation of an
# action property (`[][A]_vars`).
#
# After each folder, the script prints the minutes that its checks took. When
# they are far from the folder's line in `scripts/tla-minutes.txt` (more than
# 1.5 times the line plus one minute, or less than half the line minus one
# minute), it prints a GitHub `::warning::` that asks for a new measure: the
# plan packs the jobs from these lines, and a stale line can push a job past
# its 30 minutes. A warning, not a failure: the speed of a runner varies.
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
  started=$SECONDS
  while read -r name expected where rest; do
    case "$name" in '' | '#'*) continue ;; esac
    bound=""
    seed=()
    for field in $rest; do
      case "$field" in
        seed=*) seed=(-seed "${field#seed=}") ;;
        *) bound="$field" ;;
      esac
    done
    if [ "$where" != ci ] && [ "$all" = false ]; then
      continue
    fi
    meta=$(mktemp -d)
    log="$meta/tlc.log"
    limit=()
    if [ -n "${bound:-}" ]; then
      limit=(timeout "${bound}m")
    fi
    (cd "$dir" && "${limit[@]}" tlc -workers "$workers" -deadlock -metadir "$meta/states" \
      "${seed[@]}" -config "$name.cfg" MC.tla) >"$log" 2>&1 || true
    if grep -q "Model checking completed. No error has been found." "$log"; then
      got=pass
    elif grep -qE "Error: ((Invariant|Action property) .* is violated(\.| by the initial state)|Temporal properties were violated\.)" "$log"; then
      got=violation
    elif [ -n "${bound:-}" ] && [ "$expected" = pass ] && grep -q "distinct states found" "$log" \
      && ! grep -q "^Error:" "$log"; then
      got=pass
      echo "      $dir/$name: bounded, stopped after $bound min with no violation"
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
  folder=${dir#specs/tla/}
  took=$((SECONDS - started))
  line=$(awk -v f="$folder" '$1 == f { print $2 }' scripts/tla-minutes.txt)
  echo "time  $folder: $(awk -v s="$took" 'BEGIN { printf "%.1f", s / 60 }') min (line: ${line:-none})"
  if [ -n "$line" ] && awk -v s="$took" -v m="$line" \
    'BEGIN { t = s / 60; exit !(t > 1.5 * m + 1 || t < m / 2 - 1) }'; then
    echo "::warning::specs/tla/$folder took $((took / 60)) min, its line in scripts/tla-minutes.txt says $line: measure it again"
  fi
done
exit "$status"
