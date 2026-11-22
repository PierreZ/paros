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
# The shards: one for each `LINES_PER_SHARD` lines that the diff adds outside
# the test files (`tests.rs`, `tests/`), at least one. cargo-mutants makes no
# mutant in a test module. The number comes from one measure: the run of
# PR #1470 (re-replication of a sealed segment's replica set) added 536 such
# lines to paros-journal, cargo-mutants found 133 mutants, and the first pass
# took 85 minutes at `-j 2`, about 0.16 minutes for each line. With 250 lines,
# the first pass of a shard takes about 40 minutes, so a runner 1.5 times as
# slow still ends inside the 90 minutes of the job, with the Nix setup, the
# build and the second pass. The measure is of paros-journal only: when a
# shard of another crate nears the 90 minutes, measure that crate and give it
# its own number here.
set -euo pipefail

cd "$(dirname "$0")/.."

base=${1:?usage: scripts/mutants-plan.sh <base>}
LINES_PER_SHARD=250

entries=()
for pair in paros-consensus: paros-journal:--journal paros-journal-control:--control paros-journal-io:--journal-io; do
  crate=${pair%%:*}
  flag=${pair#*:}
  src=crates/lib/$crate/src
  if git diff --quiet "$base" HEAD -- "$src"; then
    continue
  fi
  lines=$(git diff "$base" HEAD -- "$src" ':(exclude)**/tests.rs' ':(exclude)**/tests/**' | grep -c '^+[^+]' || true)
  of=$(((lines + LINES_PER_SHARD - 1) / LINES_PER_SHARD))
  if [ "$of" -lt 1 ]; then
    of=1
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
