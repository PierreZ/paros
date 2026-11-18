#!/usr/bin/env bash
# The red record and the heal scorecard of a simulation run (#920 (sweep
# budget), #921 (seed tools), rule 8 and step 1.4 of the rework, #1093 (heal
# scorecard and open causes)): `scripts/sim-red-record.sh <dir>` adds one run
# to the rolling issue: the sweep of a push to main (label `main-red`, the
# `main-reds` job of `rust.yml`) or a night of the nightly hunt (label
# `nightly-sim`, `nightly-sim.yml`).
#
# Every file named `red.txt` under <dir> is the record of one shard or one
# chunk (`sim-paros-chain` and `sim-paros-hunt` with `PAROS_SIM_RED`): one
# line per cause, `<cause>\t<seeds>`, the seeds whose own run had it, and one
# line `seeds run\t<count>`.
#
# The issue body keeps its data in one HTML comment (JSON) and shows two
# tables made from it:
# - the heal scorecard: for each cause, the seeds it was red on at each of the
#   last commits, over the seeds the commit ran, with its status from the list
#   of open causes (`crates/sim/paros-sim-runner/open-causes.txt`): open until
#   the gate of its phase, or blocking;
# - the red record: per cause, the runs it was red in, its rate over every
#   recorded run, the first and the last commit, and the newest seeds with
#   their replay command. The fix-main thread starts from the top blocking
#   line.
#
# Reads RESULT (`success` or `failure`: the run's verdict), LABEL (default
# `main-red`), TITLE, WHAT (what one run is, default "sweeps of main"),
# OPEN_CAUSES (default the list above), GITHUB_SHA, RUN_URL, GITHUB_REPOSITORY
# and GH_TOKEN from the environment. With DRY_RUN=1 it reads the body from
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
list=${OPEN_CAUSES:-crates/sim/paros-sim-runner/open-causes.txt}
# The commits the scorecard keeps, newest first, and the ones it shows.
keep=30
shown=6

if [ "$result" = cancelled ] || [ "$result" = skipped ]; then
  echo "the sweep was $result: nothing to record"
  exit 0
fi

# This run's lines, from every shard or chunk.
lines=$(mktemp)
if [ -d "$dir" ]; then
  find "$dir" -name red.txt -print0 | xargs -0 -r cat >"$lines"
fi
if [ "$result" = failure ] && ! grep -qv '^seeds run	' "$lines"; then
  # A shard failed but wrote no cause: it timed out, crashed or failed to build.
  printf 'a shard failed with no red line (timeout, crash or build)\t\n' >>"$lines"
fi
run=$(jq -R -s -c '
  split("\n") | map(select(length > 0) | split("\t") | {msg: .[0], seeds: (.[1] // "")})
  | {
      seeds: (map(select(.msg == "seeds run") | .seeds | tonumber? // 0) | add // 0),
      reds: (map(select(.msg != "seeds run"))
        | group_by(.msg)
        | map({msg: .[0].msg,
               seeds: (map(.seeds | split(" ") | map(select(length > 0))) | add | unique)}))
    }
' "$lines")
rm -f "$lines"

# The list of open causes: message -> phase.
open='{}'
if [ -f "$list" ]; then
  open=$(grep -v '^#' "$list" | grep -P '^\d+\t' \
    | jq -R -s -c 'split("\n") | map(select(length > 0) | split("\t") | {(.[1]): (.[0] | tonumber)}) | add // {}')
fi

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

data=$(jq -c --argjson run "$run" --arg sha "$short" --arg date "$today" \
  --arg result "$result" --argjson keep "$keep" '
  .runs += 1
  | .red_runs += (if $result == "failure" then 1 else 0 end)
  | reduce $run.reds[] as $r (.;
      .rows[$r.msg] = ((.rows[$r.msg] // {reds: 0, first: $sha})
        + {last: $sha, date: $date, seeds: ($r.seeds | .[0:3] | join(" "))}
        | .reds += 1))
  | .cards = ([{sha: $sha, date: $date, seeds: $run.seeds,
                causes: ($run.reds | map({(.msg): (.seeds | length)}) | add // {})}]
              + (.cards // []) | .[0:$keep])
' <<<"$data")

esc='gsub("\\|"; "\\|")'
scorecard=$(jq -r --argjson open "$open" --argjson shown "$shown" "
  (.cards // [] | .[0:\$shown]) as \$cards
  | (\$cards | map(.causes | keys) | add // [] | unique) as \$causes
  | \"| Cause | Status | \" + (\$cards | map(\"\`\(.sha)\` (\(.seeds) seeds)\") | join(\" | \")) + \" |\",
    \"|---|---|\" + (\$cards | map(\"---|\") | join(\"\")),
    (\$causes
      | map(. as \$m | {msg: \$m, total: ([\$cards[] | .causes[\$m] // 0] | add)})
      | sort_by(if \$open[.msg] then 1 else 0 end, -.total, .msg)
      | .[]
      | .msg as \$m
      | \"| \(\$m | $esc) | \"
        + (if \$open[\$m] then \"open, phase \(\$open[\$m])\" else \"**blocks**\" end)
        + \" | \" + ([\$cards[] | (.causes[\$m] // 0 | tostring)] | join(\" | \")) + \" |\")
" <<<"$data")

table=$(jq -r --argjson open "$open" "
  .runs as \$runs
  | .rows | to_entries | sort_by(-.value.reds, .key)
  | map(
      \"| \(.key | $esc) | \"
      + (if \$open[.key] then \"open, phase \(\$open[.key])\" else \"**blocks**\" end)
      + \" | \(.value.reds) | \((.value.reds * 100 / \$runs) | floor) % | \`\(.value.first)\` | \`\(.value.last)\` (\(.value.date)) | \"
      + (.value.seeds | split(\" \") | map(select(length > 0)) | .[0:3]
         | map(\"\`sim-paros-hunt replay-main \(.)\`\") | join(\"<br>\"))
      + \" |\")
  | .[]
" <<<"$data")
summary=$(jq -r --arg what "$what" '"\(.runs) \($what) recorded since \(.since), \(.red_runs) of them red on a blocking cause."' <<<"$data")

new_body=$(cat <<MD
The heal scorecard and the red record of the $what (#920 (sweep budget), #1093
(heal scorecard and open causes)), written by \`scripts/sim-red-record.sh\` after
each run. Do not edit by hand: the next run rewrites it.

$summary Last run: $run_url (\`$short\`, $result).

## Heal scorecard

The seeds each cause was red on, for each of the last commits. An open cause
waits for the gate of its phase (\`crates/sim/paros-sim-runner/open-causes.txt\`);
every other cause blocks a pull request.

$scorecard

## Red record

| Cause | Status | Red runs | Rate | First | Last | Newest seeds |
|---|---|---|---|---|---|---|
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
