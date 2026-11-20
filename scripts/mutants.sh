#!/usr/bin/env bash
# Mutation testing of paros-consensus, in two passes (`.cargo/mutants.toml`).
#
#   scripts/mutants.sh [ARGS...]
#
# ARGS go to the first pass and cut the set of mutants, for example
# `--shard 3/16 --sharding round-robin`, `--in-diff pr.diff` or `-f <file>`.
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

# cargo-mutants exits with 2 when a mutant is missed and 3 on a timeout; both
# are results, not errors.
run() {
  local rc=0
  cargo mutants -p paros-consensus "$@" || rc=$?
  case "$rc" in
    0 | 2 | 3) ;;
    *) exit "$rc" ;;
  esac
}

rm -f survivors.txt
run -j "${MUTANTS_JOBS:-2}" "$@"
rm -rf mutants.first
cp -r mutants.out mutants.first
touch survivors.txt

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
# The baseline of cargo-mutants builds paros-consensus alone, which has no
# trace check to run, so this pass skips it (the first pass ran one) and sets
# its own timeout.
run --config .cargo/mutants-tlc.toml --baseline skip --timeout 400 -j 1 "${filters[@]}"

# cargo-mutants 27.1 keeps the struct-field deletions of the package whatever
# `--re` says, so the second pass can test mutants that the first pass caught.
# A survivor is a mutant that both passes missed.
grep -Fxf mutants.first/missed.txt mutants.out/missed.txt > survivors.txt || true
echo "Survivors: $(wc -l < survivors.txt)."
cat survivors.txt
if [ -s survivors.txt ]; then
  exit 2
fi
