#!/usr/bin/env bash
# The jobs of the `mutants` job of `.github/workflows/rust.yml`: one job for
# each shard of each crate whose `src/` the pull request changes.
#
#   scripts/mutants-plan.sh <base>
#
# It writes `any=true|false` and `matrix=<json>` to `$GITHUB_OUTPUT`, or to
# stdout. A crate runs when `git diff <base> HEAD -- crates/lib/<crate>/src`
# is not empty: that diff is the input of `scripts/mutants.sh --in-diff`, so a
# crate whose diff is empty has no mutant to test.
#
# The shards: one for each `lines` lines that the diff adds outside the test
# files (`tests.rs`, `tests/`), at least `min`. cargo-mutants makes no mutant
# in a test module. The numbers come from two measures, both at `-j 2`:
#
# - paros-journal (PR #1470 (re-replication of a sealed segment's replica
#   set)): 536 such lines gave 133 mutants, and the first pass took 85
#   minutes, about 0.16 minutes for each line (0.64 for each mutant). With 250
#   lines, the first pass of a shard takes about 40 minutes. paros-journal-control
#   and paros-journal-io use the same number; their shards of that PR took 8
#   to 16 minutes.
# - paros-consensus (the comment of the old job): 70 mutants took 88 minutes,
#   about 1.26 minutes for each mutant, twice as slow. Its number is half of
#   the journal's, 125 lines, on the reading (not a measure) that its lines
#   give mutants as densely. It keeps the two shards that it had at least.
#
# Each shard then ends inside the 90 minutes of the job on a runner 1.5 times
# as slow, with the Nix setup, the build and the second pass. When a shard
# nears the 90 minutes, measure its crate again and change its number here.
set -euo pipefail

cd "$(dirname "$0")/.."

base=${1:?usage: scripts/mutants-plan.sh <base>}

entries=()
# crate:flag:lines:min
for row in paros-consensus::125:2 paros-journal:--journal:250:1 \
  paros-journal-control:--control:250:1 paros-journal-io:--journal-io:250:1; do
  IFS=: read -r crate flag per min <<< "$row"
  src=crates/lib/$crate/src
  if git diff --quiet "$base" HEAD -- "$src"; then
    continue
  fi
  lines=$(git diff "$base" HEAD -- "$src" ':(exclude)**/tests.rs' ':(exclude)**/tests/**' | grep -c '^+[^+]' || true)
  of=$(((lines + per - 1) / per))
  if [ "$of" -lt "$min" ]; then
    of=$min
  fi
  for ((shard = 0; shard < of; shard++)); do
    entries+=("{\"crate\":\"$crate\",\"flag\":\"$flag\",\"shard\":$shard,\"of\":$of}")
  done
done

any=false
if [ "${#entries[@]}" -gt 0 ]; then
  any=true
fi
matrix="{\"include\":[$(IFS=,; echo "${entries[*]}")]}"
text="any=$any
matrix=$matrix
"
printf '%s' "$text"
if [ -n "${GITHUB_OUTPUT:-}" ]; then
  printf '%s' "$text" >> "$GITHUB_OUTPUT"
fi
