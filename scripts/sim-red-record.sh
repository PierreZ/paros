#!/usr/bin/env bash
# The red record of a simulation run (#920 (sweep budget), #921 (seed tools)):
# `scripts/sim-red-record.sh <dir>` adds one run to the table in a rolling
# issue: the sweep of a push to main (label `main-red`, the `main-reds` job of
# `rust.yml`) or a night of the nightly hunt (label `nightly-sim`,
# `nightly-sim.yml`). Every file named `red.txt` under <dir> holds one line per
# assertion that failed, `<message>\t<seeds>` (`sim-paros-chain`'s
# `PAROS_SIM_RED`, or `scripts/nightly-sim-report.sh`).
#
# The issue body keeps its data in one HTML comment (JSON) and shows a table
# made from it: per message, the red runs, the red rate over every recorded
# run on main, the first and the last commit, and the newest seeds with their
# replay command. The fix-main thread starts from the top line.
#
# Reads RESULT (`success` or `failure`: the run's verdict), LABEL (default
# `main-red`), TITLE, WHAT (what one run is, default "sweeps of main"),
# GITHUB_SHA, RUN_URL, GITHUB_REPOSITORY and GH_TOKEN from the environment. With DRY_RUN=1 it reads the body from
# BODY_IN (a file, empty for a new issue) and prints the new body instead of
# writing the issue.
set -euo pipefail

dir=${1:?usage: sim-red-record.sh <reds-dir>}
result=${RESULT:-unknown}
sha=${GITHUB_SHA:-unknown}
short=${sha:0:7}
run_url=${RUN_URL:-local}
today=$(date -u +%Y-%m-%d)
label=${LABEL:-main-red}
title=${TITLE:-sim: the red record of main}
what=${WHAT:-sweeps of main}

if [ "$result" = cancelled ] || [ "$result" = skipped ]; then
  echo "the sweep was $result: nothing to record"
  exit 0
fi

# This run's red lines, one JSON object per message.
lines=$(mktemp)
if [ -d "$dir" ]; then
  find "$dir" -name red.txt -print0 | xargs -0 -r cat >"$lines"
fi
if [ "$result" = failure ] && [ ! -s "$lines" ]; then
  # A shard failed but wrote no line: it timed out, crashed or failed to build.
  printf 'a shard failed with no red line (timeout, crash or build)\t\n' >"$lines"
fi
reds=$(jq -R -s -c '
  split("\n") | map(select(length > 0) | split("\t") | {msg: .[0], seeds: (.[1] // "")})
  | group_by(.msg) | map({msg: .[0].msg, seeds: (map(.seeds) | map(select(length > 0)) | join(" "))})
' "$lines")
rm -f "$lines"

if [ "${DRY_RUN:-0}" = 1 ]; then
  issue=""
  body=$(cat "${BODY_IN:-/dev/null}")
else
  issue=$(gh issue list -R "$GITHUB_REPOSITORY" --label "$label" --state open \
    --json number --jq '.[0].number // empty')
  body=""
  if [ -n "$issue" ]; then
    body=$(gh issue view "$issue" -R "$GITHUB_REPOSITORY" --json body --jq .body)
  fi
fi

data=$(printf '%s\n' "$body" | sed -n 's/^<!-- main-red-data \(.*\) -->$/\1/p' | head -n 1)
if [ -z "$data" ]; then data='{"runs":0,"red_runs":0,"since":"'"$today"'","rows":{}}'; fi

data=$(jq -c --argjson reds "$reds" --arg sha "$short" --arg date "$today" '
  .runs += 1
  | .red_runs += (if ($reds | length) > 0 then 1 else 0 end)
  | reduce $reds[] as $r (.;
      .rows[$r.msg] = ((.rows[$r.msg] // {reds: 0, first: $sha})
        + {last: $sha, date: $date, seeds: $r.seeds}
        | .reds += 1))
' <<<"$data")

table=$(jq -r '
  .runs as $runs
  | .rows | to_entries | sort_by(-.value.reds, .key)
  | map(
      "| \(.key | gsub("\\|"; "\\|")) | \(.value.reds) | \((.value.reds * 100 / $runs) | floor) % | `\(.value.first)` | `\(.value.last)` (\(.value.date)) | "
      + (.value.seeds | split(" ") | map(select(length > 0)) | .[0:3]
         | map("`sim-paros-hunt replay-main \(.)`") | join("<br>"))
      + " |")
  | .[]
' <<<"$data")
summary=$(jq -r --arg what "$what" '"\(.runs) \($what) recorded since \(.since), \(.red_runs) of them red."' <<<"$data")

new_body=$(cat <<MD
The red record of the $what (#920 (sweep budget), #921 (seed tools)), one line
per failed message, written by \`scripts/sim-red-record.sh\` after each run.
Do not edit by hand: the next run rewrites it.

$summary Last run: $run_url (\`$short\`, $result).

| Message | Red runs | Rate | First | Last | Newest seeds |
|---|---|---|---|---|---|
$table

A seed of an explored timeline may need its recipe (the shard log's \`bug recipe\`
line, \`sim-paros-hunt replay-recipe\`). A seed is stale at the first later
commit that changes a draw.

<!-- main-red-data $data -->
MD
)

if [ "${DRY_RUN:-0}" = 1 ]; then
  printf '%s\n' "$new_body"
  exit 0
fi

if [ -z "$issue" ]; then
  gh label create "$label" -R "$GITHUB_REPOSITORY" --color B60205 \
    --description "The red record of the sweep on main" 2>/dev/null || true
  gh issue create -R "$GITHUB_REPOSITORY" --label "$label" \
    --title "$title" --body "$new_body"
else
  gh issue edit "$issue" -R "$GITHUB_REPOSITORY" --body "$new_body"
fi
