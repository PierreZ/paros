#!/usr/bin/env bash
# Mutation testing of paros-consensus, in two passes (`.cargo/mutants.toml`).
#
#   scripts/mutants.sh [ARGS...]
#   scripts/mutants.sh --journal [ARGS...]
#   scripts/mutants.sh --journal-io [ARGS...]
#
# `--journal` runs the same two passes on paros-journal, judged by its unit
# tests and by journal-sim (`.cargo/mutants-journal.toml`, then
# `.cargo/mutants-journal-tlc.toml`).
#
# `--journal-io` runs one pass on paros-journal-io, judged by its unit tests
# and by the driver walk of journal-sim (`.cargo/mutants-journal-io.toml`).
# The driver has no trace check yet, so there is no second pass: a mutant
# that the first pass misses survives.
#
# ARGS go to the first pass and cut the set of mutants, for example
# `--shard 3/48 --sharding round-robin`, `--in-diff pr.diff` or `-f <file>`.
#
# 1. The first pass runs every mutant against the fast tests: the tests of
#    paros-consensus and of consensus-sim, without the coverage gate and the
#    trace checks (`.cargo/mutants.toml`). Its outcomes are kept in
#    `mutants.first/`.
# 2. The second pass runs only the mutants that the first pass missed, against
#    the trace checks with TLC (`.cargo/mutants-tlc.toml`).
#
# `mutants.out/` holds the outcomes of the last pass that ran. A survivor is a
# mutant that both passes missed: the script lists them in `survivors.txt`.
# The exit status is
# 0 when no mutant survives, 2 when one does, and the status of cargo-mutants
# for any other error. A timeout counts as caught: a mutant that makes a test
# loop for ever changed what the code does.
#
# Run it in the nix dev shell. `MUTANTS_JOBS` sets the parallel builds of the
# first pass (default 2); the second pass runs one mutant at a time, because
# each trace check starts TLC in a JVM of its own.
set -euo pipefail

cd "$(dirname "$0")/.."

crate=paros-consensus
first=.cargo/mutants.toml
second=.cargo/mutants-tlc.toml
# The trace checks of the second pass, run once on the unmutated tree: the
# filter of the second pass's config.
baseline_args=(--features paros-consensus/serde -p paros-consensus -p consensus-sim
  -E 'binary(traces) and not test(edited) and not test(no_record)')
if [ "${1:-}" = "--journal" ]; then
  shift
  crate=paros-journal
  first=.cargo/mutants-journal.toml
  second=.cargo/mutants-journal-tlc.toml
  baseline_args=(-p paros-journal -p journal-sim
    -E '(binary(traces) | binary(register_traces)) and not test(edited)')
elif [ "${1:-}" = "--journal-io" ]; then
  shift
  crate=paros-journal-io
  first=.cargo/mutants-journal-io.toml
  second=
fi

# cargo-mutants exits with 2 when a mutant is missed and 3 on a timeout; both
# are results, not errors.
run() {
  local rc=0
  cargo mutants -p "$crate" "$@" || rc=$?
  case "$rc" in
    0 | 2 | 3) ;;
    *) exit "$rc" ;;
  esac
}

rm -rf survivors.txt mutants.out
run --config "$first" -j "${MUTANTS_JOBS:-2}" "$@"
touch survivors.txt
# A cut with no mutant (`--in-diff` of a diff that changes only comments of
# the core) runs nothing, and cargo-mutants writes no `mutants.out/`.
if [ ! -d mutants.out ]; then
  echo "First pass: the cut holds no mutant. Nothing to run."
  exit 0
fi
rm -rf mutants.first
cp -r mutants.out mutants.first

if [ -z "$second" ]; then
  cp mutants.first/missed.txt survivors.txt 2>/dev/null || true
  echo "Survivors: $(wc -l < survivors.txt)."
  cat survivors.txt
  if [ -s survivors.txt ]; then
    exit 2
  fi
  exit 0
fi

if [ ! -s mutants.first/missed.txt ]; then
  echo "First pass: no mutant missed. The second pass has nothing to run."
  exit 0
fi

# One anchored regex for each missed mutant, matched on its whole name
# (`file:line:col: replace ...`), so only those mutants run again.
filters=()
while IFS= read -r name; do
  [ -n "$name" ] || continue
  escaped=$(printf '%s' "$name" | sed 's/[][\.*^$()+?{}|\\/]/\\&/g')
  filters+=(--re "^${escaped}\$")
done < mutants.first/missed.txt

echo "Second pass: $((${#filters[@]} / 2)) survivors of the first pass, against the trace checks."
# A timeout counts as caught, so the timeout of this pass comes from a run of
# the same trace checks on the unmutated tree (the filter of
# `.cargo/mutants-tlc.toml`): three times its time, and at
# least 60 s. With a fixed timeout below that time, every mutant timed out
# and was counted as caught. The run also proves that the trace checks pass
# without a mutant, so a mutant that fails them is caught by its own change.
start=$(date +%s)
cargo nextest run --cargo-profile mutants "${baseline_args[@]}" --test-threads 2
baseline=$(($(date +%s) - start))
timeout=$((baseline * 3 > 60 ? baseline * 3 : 60))
echo "Second pass: the unmutated trace checks took ${baseline} s; the timeout of a mutant is ${timeout} s."
run --config "$second" --baseline skip --timeout "$timeout" -j 1 "${filters[@]}"

# cargo-mutants 27.1 keeps the struct-field deletions of the package whatever
# `--re` says, so the second pass can test mutants that the first pass caught.
# A survivor is a mutant that both passes missed.
grep -Fxf mutants.first/missed.txt mutants.out/missed.txt > survivors.txt || true
echo "Survivors: $(wc -l < survivors.txt)."
cat survivors.txt
if [ -s survivors.txt ]; then
  exit 2
fi
