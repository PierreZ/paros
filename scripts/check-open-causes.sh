#!/usr/bin/env bash
# The list of open causes only gets shorter (rule 8, step 1.4 of the rework,
# #1093 (heal scorecard and open causes)):
#
#   scripts/check-open-causes.sh <base-list> <head-list> [changed-files]
#
# Fails when the head list names a cause that the base list does not, or moves
# a cause to a later phase: a pull request must not make itself green with one
# more line. Deleting a line, or moving a cause to an earlier phase, passes.
# CI's `open-causes` job runs this script as the base branch has it, so a
# pull request cannot edit the script that judges it (AGENTS.md, "A pull
# request does not change its own judge").
#
# With a third argument, a file of the paths the diff changes, one per line,
# it also prints each judge the diff touches (REVIEW.md lists them), so that a
# reviewer reads those lines first. That part only reports.
set -euo pipefail

base=${1:?usage: check-open-causes.sh <base-list> <head-list> [changed-files]}
head=${2:?usage: check-open-causes.sh <base-list> <head-list> [changed-files]}
changed=${3:-}

# `<message>\t<phase>` for each cause of a list; a missing list has none.
causes() {
  if [ -f "$1" ]; then
    grep -v '^#' "$1" | grep -P '^\s*\d+\t' | awk -F'\t' '{ gsub(/^[ \t]+|[ \t]+$/, "", $1); m = $2; sub(/[ \t]+$/, "", m); print m "\t" $1 }' | sort
  fi
}

if [ ! -f "$base" ]; then
  echo "the base has no list of open causes: this diff creates it"
  exit 0
fi

bad=0
while IFS=$'\t' read -r message phase; do
  [ -n "$message" ] || continue
  was=$(causes "$base" | awk -F'\t' -v m="$message" '$1 == m { print $2 }')
  if [ -z "$was" ]; then
    echo "ADDED: \"$message\" (phase $phase) is not on the base list: a pull request never adds an open cause"
    bad=1
  elif [ "$phase" -gt "$was" ]; then
    echo "LATER: \"$message\" moves from phase $was to phase $phase: a cause never moves to a later phase"
    bad=1
  fi
done < <(causes "$head")

closed=$(comm -23 <(causes "$base" | cut -f1) <(causes "$head" | cut -f1) | grep -c . || true)
echo "open causes: $(causes "$base" | grep -c . || true) on the base, $(causes "$head" | grep -c . || true) on the head, $closed closed by this diff"

if [ -n "$changed" ] && [ -f "$changed" ]; then
  # The judges of REVIEW.md: the oracles and assertions of the simulation,
  # paros-core's assertions, the TLA+ checks, the end-to-end check, the
  # workflows, the lints and the list of open causes.
  judges=$(grep -E '^(crates/sim/paros-sim/src/(audit|tail|workload)|crates/sim/paros-sim-runner/|crates/lib/paros-core/src/|specs/tla/|scripts/verify-cell\.sh|scripts/check-|\.github/workflows/|clippy\.toml|Cargo\.toml$)' "$changed" || true)
  if [ -n "$judges" ]; then
    echo "judges this diff touches (read these lines first):"
    printf '%s\n' "$judges" | sed 's/^/  - /'
  else
    echo "this diff touches no judge"
  fi
fi

exit "$bad"
