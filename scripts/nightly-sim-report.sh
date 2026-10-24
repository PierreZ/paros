#!/usr/bin/env bash
# The nightly simulation hunt's report (`.github/workflows/nightly-sim.yml`),
# from the shards' output: `scripts/nightly-sim-report.sh <dir>`, where each
# subdirectory of <dir> is named `nightly-shard-<n>` and holds one shard's
# chunk logs (`scripts/nightly-sim-shard.sh`). Prints Markdown.
#
# Exits 1 when a chunk found a violation, a failed run, hung or crashed, or a
# shard wrote nothing; 0 when the night is clean.
#
# Reads RUN_URL, GITHUB_SHA, SHARDS and AXIS from the environment when set.
set -euo pipefail

dir=${1:?usage: nightly-sim-report.sh <shards-dir>}
axis=${AXIS:-main}
replay=replay-main
if [ "$axis" = canary ]; then replay=replay-canary; fi

seeds=0
chunks=0
red=0
findings=$(mktemp)
missing=()

for n in $(seq 1 "${SHARDS:-0}"); do
  [ -d "$dir/nightly-shard-$n" ] || missing+=("$n")
done

for shard in $(find "$dir" -mindepth 1 -maxdepth 1 -type d -name 'nightly-shard-*' | sort -V); do
  name=$(basename "$shard")
  job="hunt (shard ${name#nightly-shard-}/${SHARDS:-?})"
  for status in $(find "$shard" -name '*.status' | sort -V); do
    log=${status%.status}.log
    chunk=$(basename "${status%.status}")
    code=$(cat "$status")
    chunks=$((chunks + 1))
    ran=$(grep -m1 -oE '^[0-9]+ seeds:' "$log" | grep -oE '^[0-9]+' || echo 0)
    seeds=$((seeds + ran))
    case "$code" in
      0) continue ;;
      1)
        failing=$(grep -m1 '^FAILING SEEDS:' "$log" | sed 's/^FAILING SEEDS: //' || true)
        {
          echo "### $job, $chunk: failing seeds $failing"
          echo
          for seed in $(echo "$failing" | tr -d '[],'); do
            echo "- seed \`$seed\`: \`cargo run --release -p paros-sim-runner --bin sim-paros-hunt -- $replay $seed\`"
          done
          echo
          echo '<details><summary>Failed runs and violations</summary>'
          echo
          echo '```'
          grep '^failed run:' "$log" || true
          sed -n '/^VIOLATIONS:/,/^FAILING SEEDS:/p' "$log" | head -n 200
          echo '```'
          echo
          echo '</details>'
          echo
        } >> "$findings"
        ;;
      124)
        {
          echo "### $job, $chunk: a chunk hung"
          echo
          echo "The chunk ran past its timeout, so its seeds are unknown: a liveness bug, or a seed far slower than the rest."
          echo
        } >> "$findings"
        ;;
      *)
        {
          echo "### $job, $chunk: the hunt crashed (exit $code)"
          echo
          echo '```'
          tail -n 40 "$log"
          echo '```'
          echo
        } >> "$findings"
        ;;
    esac
    red=1
  done
done

if [ "${#missing[@]}" -gt 0 ]; then red=1; fi

cat <<MD
Nightly simulation hunt of \`.github/workflows/nightly-sim.yml\`: raw seed volume
through the \`$axis\` axis of \`sim-paros-hunt\`, coverage-blind.

- Run: ${RUN_URL:-local}
- Commit: \`${GITHUB_SHA:-unknown}\`
- Seeds: $seeds in $chunks chunks over ${SHARDS:-?} shards
MD
if [ "${#missing[@]}" -gt 0 ]; then
  echo "- Shards that wrote nothing (the job died before its upload): ${missing[*]}"
fi
echo
if [ "$red" -eq 0 ]; then
  echo "No violation, failed run, hang or crash."
else
  echo "## Findings"
  echo
  cat "$findings"
fi
rm -f "$findings"
exit "$red"
