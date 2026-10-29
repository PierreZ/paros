#!/usr/bin/env bash
# The rolling survivors issue's body (#269), from the `mutants.out` directories
# of a cargo-mutants run's shards: `scripts/mutants-report.sh <dir> [<negative>]`,
# where each subdirectory of <dir> is one shard's `mutants.out`, and each of
# <negative> one shard's `negative.out` (#742 (mutant per excuse)). Prints
# Markdown.
#
# Reads RUN_URL, SEEDS and GITHUB_SHA from the environment when set.
set -euo pipefail

dir=${1:?usage: mutants-report.sh <shards-dir> [<negative-dir>]}
negative=${2:-}
shards=$(find "$dir" -mindepth 1 -maxdepth 1 -type d | sort)

# Every line of one outcome file across the shards, sorted.
lines() {
  for shard in $shards; do
    if [ -f "$shard/$1.txt" ]; then cat "$shard/$1.txt"; fi
  done | sed '/^$/d' | sort -V
}
count() { lines "$1" | wc -l | tr -d ' '; }

missing=()
for shard in $shards; do
  [ -f "$shard/outcomes.json" ] || missing+=("$(basename "$shard")")
done

caught=$(count caught)
missed=$(count missed)
timeout=$(count timeout)
unviable=$(count unviable)

cat <<MD
Rolling report of the weekly mutation hunt over paros-core (#269), rewritten by
every run of \`.github/workflows/mutants.yml\`. **Do not edit by hand**: triage
lands in code (an oracle, a BUGGIFY site) or in \`.cargo/mutants.toml\` (an
equivalent mutant, with its reason), and the next run reflects it.

- Run: ${RUN_URL:-local}
- Commit: \`${GITHUB_SHA:-unknown}\`
- Hunt: seeds 1..=${SEEDS:-300} of the main campaign per mutant

| caught | survived | timeout | unviable |
|-------:|---------:|--------:|---------:|
| $caught | $missed | $timeout | $unviable |

A timeout is a mutant that livelocked the hunt: it counts as caught. An unviable
mutant does not compile.
MD

if [ "${#missing[@]}" -gt 0 ]; then
  echo
  echo "**Shards with no results** (failed or cancelled; their mutants are not counted): ${missing[*]}"
fi

echo
echo "## Survivors ($missed)"
echo
if [ "$missed" -eq 0 ]; then
  echo "None."
else
  cat <<'MD'
Triage each into one of #269's classes: an equivalent mutant (exclude it in
`.cargo/mutants.toml` with a reason), a missing oracle (`adding-an-audit-check`),
an unreachable state (`adding-a-buggify-site`), or a mechanism a unit test pins
once the sim proves it. A class-2 or class-3 survivor gets its own issue.

MD
  lines missed | sed 's/^/- [ ] `/; s/$/`/'
fi

if [ "$timeout" -gt 0 ]; then
  echo
  echo "<details><summary>Timeouts ($timeout)</summary>"
  echo
  lines timeout | sed 's/^/- `/; s/$/`/'
  echo
  echo "</details>"
fi

if [ -n "$negative" ] && [ -d "$negative" ]; then
  verdicts=$(find "$negative" -name verdicts.txt -exec cat {} + | sed '/^$/d' | sort)
  echo
  echo "## Negative hunt"
  echo
  cat <<'MD'
Hand-written mutants that each oracle excuse must catch (#742 (mutant per
excuse), `crates/sim/paros-sim-runner/negative`). A row marked UNEXPECTED is a
verdict that differs from the mutant's `expect`: an excuse wider than the rule
it copies, or a mutant the code moved away from (stale).

| mutant | excuse | verdict | expect | |
|---|---|---|---|---|
MD
  if [ -z "$verdicts" ]; then
    echo "| (no verdicts: every shard failed or was cancelled) | | | | |"
  else
    echo "$verdicts" | awk -F'\t' '{ printf "| `%s` | %s | %s | %s | %s |\n", $1, $2, $3, $4, $5 }'
  fi
fi
