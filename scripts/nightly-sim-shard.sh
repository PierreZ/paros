#!/usr/bin/env bash
# One shard of the nightly simulation hunt (`.github/workflows/nightly-sim.yml`).
#
#   scripts/nightly-sim-shard.sh <out-dir> <minutes> [workers] [chunk] [axis]
#
# Runs `workers` hunt processes side by side (default: one per core) until
# `minutes` have passed. Each process runs `sim-paros-hunt <axis> <chunk>` again
# and again, so a time budget, not a seed count, sets the length of the night,
# and a hung chunk costs only its own seeds. Every hunt draws its seeds from a
# wall-clock base seed, so two processes, or two shards, share a seed only by a
# 64-bit hash collision.
#
# Each chunk writes `<out-dir>/w<worker>-c<chunk>.log` and its exit status to
# `<out-dir>/w<worker>-c<chunk>.status` (0 clean, 1 violation, 124 hung past
# CHUNK_TIMEOUT, other: crash). `scripts/nightly-sim-report.sh` reads them.
#
# Reads HUNT (the binary, default target/release/sim-paros-hunt) and
# CHUNK_TIMEOUT (seconds, default 1800) from the environment.
# Exits 0: the verdict is the report's.
set -euo pipefail

out=${1:?usage: nightly-sim-shard.sh <out-dir> <minutes> [workers] [chunk] [axis]}
minutes=${2:?usage: nightly-sim-shard.sh <out-dir> <minutes> [workers] [chunk] [axis]}
workers=${3:-0}
chunk=${4:-100}
axis=${5:-main}
hunt=${HUNT:-target/release/sim-paros-hunt}
chunk_timeout=${CHUNK_TIMEOUT:-1800}

if [ "$workers" -le 0 ]; then workers=$(nproc); fi
test -x "$hunt"
mkdir -p "$out"
deadline=$(($(date +%s) + minutes * 60))

worker() {
  local w=$1 k=0 code
  while [ "$(date +%s)" -lt "$deadline" ]; do
    k=$((k + 1))
    code=0
    timeout "$chunk_timeout" "$hunt" "$axis" "$chunk" > "$out/w$w-c$k.log" 2>&1 || code=$?
    echo "$code" > "$out/w$w-c$k.status"
    echo "worker $w chunk $k: exit $code ($(grep -m1 -E '^[0-9]+ seeds:' "$out/w$w-c$k.log" || echo 'no seed count'))"
  done
}

echo "nightly shard: $workers workers, chunks of $chunk $axis seeds, $minutes minutes"
for w in $(seq 1 "$workers"); do worker "$w" & done
wait
