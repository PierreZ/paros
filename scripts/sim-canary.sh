#!/usr/bin/env bash
# The determinism canary of a pull request (#460 (the canary covers two
# seeds)): `sim-paros-hunt canary` on fresh seeds, one hunt process per core.
#
#   scripts/sim-canary.sh <out-dir> [seeds] [workers]
#
# Every seed of the main campaign runs twice under moonpool's
# `check_determinism`, and the second run must reproduce the first run's
# draws, all of them. The seeds are fresh on every run: each hunt process
# takes its base seed from the wall clock in nanoseconds, and prints the seeds
# it ran. A red seed names the first diverging draw; the script prints its
# replay command.
#
# `seeds` (default 200) is split over `workers` processes (default: one per
# core). Reads HUNT (the binary, default target/release/sim-paros-hunt) and
# CHUNK_TIMEOUT (seconds, default 1800) from the environment.
# Exits 1 when a seed diverged, a run failed, a process hung or crashed.
set -euo pipefail

out=${1:?usage: sim-canary.sh <out-dir> [seeds] [workers]}
seeds=${2:-200}
workers=${3:-0}
hunt=${HUNT:-target/release/sim-paros-hunt}
chunk_timeout=${CHUNK_TIMEOUT:-1800}

if [ "$workers" -le 0 ]; then workers=$(nproc); fi
test -x "$hunt"
mkdir -p "$out"
per=$(((seeds + workers - 1) / workers))

echo "determinism canary: $workers processes of $per seeds"
pids=()
for w in $(seq 1 "$workers"); do
  (
    code=0
    timeout "$chunk_timeout" "$hunt" canary "$per" > "$out/w$w.log" 2>&1 || code=$?
    echo "$code" > "$out/w$w.status"
  ) &
  pids+=("$!")
done
wait "${pids[@]}"

red=0
ran=0
for w in $(seq 1 "$workers"); do
  log=$out/w$w.log
  code=$(cat "$out/w$w.status")
  n=$(grep -m1 -oE '^[0-9]+ seeds:' "$log" | grep -oE '^[0-9]+' || echo 0)
  ran=$((ran + n))
  echo "process $w: exit $code, $(grep -m1 '^canary seeds:' "$log" || echo 'no seed list')"
  case "$code" in
    0) ;;
    1)
      red=1
      failing=$(grep -m1 '^FAILING SEEDS:' "$log" | sed 's/^FAILING SEEDS: //' || true)
      echo "process $w: failing seeds $failing"
      for seed in $(echo "$failing" | tr -d '[],'); do
        echo "  replay: cargo run --release -p paros-sim-runner --bin sim-paros-hunt -- replay-canary $seed"
      done
      grep '^failed run:' "$log" || true
      sed -n '/^VIOLATIONS:/,/^FAILING SEEDS:/p' "$log" | head -n 200
      ;;
    124)
      red=1
      echo "process $w: hung past ${chunk_timeout}s, its seeds are unknown"
      ;;
    *)
      red=1
      echo "process $w: crashed (exit $code)"
      tail -n 40 "$log"
      ;;
  esac
done

echo "determinism canary: $ran seeds, each run twice"
if [ "$red" -eq 0 ]; then echo "every seed replayed draw by draw"; fi
exit "$red"
