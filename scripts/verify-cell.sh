#!/usr/bin/env bash
# The end-to-end check of the shipped binaries (the `verify` skill): three `parosd`
# processes on loopback, driven by `parosctl` as a user drives them. It forms a cell,
# creates a tenant and a journal, writes, reads, supersedes the writer, kills one machine
# with SIGKILL, writes and reads on the two that are left, starts the machine again and
# stops the cell.
#
# It is a local check, run by hand or by an agent before a pull request. It is not a CI
# job and not a test of the protocol: the simulation proves the protocol. It answers one
# question: "do the binaries a user runs still do the user's flow?"
#
# Each step prints one line. A step that fails prints its command, its exit code, its
# output and the last lines of each machine's log, and the script exits 1. The directory
# of the run is kept after a failure, and removed after a success unless --keep is given.
#
# Usage: scripts/verify-cell.sh [--keep] [--no-build] [--port-base N]
#   Run it in the dev shell: nix develop --command scripts/verify-cell.sh
#
# Environment: VERIFY_DIR (the directory of the run; default: a fresh one under TMPDIR),
# VERIFY_PATIENCE_S (how long a step waits for the cell; default 60), VERIFY_RUST_LOG (the
# log filter of the machines; default: parosd's own default). The RUST_LOG of the caller is
# not used: the dev shell sets one, and it would fill each failure with debug lines.
set -uo pipefail

keep=false
build=true
port_base=""
while [ $# -gt 0 ]; do
  case "$1" in
    --keep) keep=true ;;
    --no-build) build=false ;;
    --port-base) port_base="$2"; shift ;;
    -h|--help) sed -n '2,22p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "verify-cell: unknown argument $1" >&2; exit 2 ;;
  esac
  shift
done

root=$(cd "$(dirname "$0")/.." && pwd)
dir=${VERIFY_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/paros-verify.XXXXXX")}
patience=${VERIFY_PATIENCE_S:-60}
mkdir -p "$dir"
bin="$root/target/debug"
step_no=0
declare -A pids=()

say() { printf '%s\n' "$*"; }

stop_all() {
  for name in "${!pids[@]}"; do
    kill -KILL "${pids[$name]}" 2>/dev/null
  done
  wait 2>/dev/null
}

# Print what a person needs to see why a step failed, then exit 1.
fail() {
  say "FAIL  step $step_no: $1"
  shift
  for line in "$@"; do say "      $line"; done
  for log in "$dir"/node*.log; do
    [ -f "$log" ] || continue
    say "----- last lines of $log"
    tail -n 15 "$log" | cut -c1-240 | sed 's/^/      /'
  done
  say "The directory of the run is kept: $dir"
  stop_all
  exit 1
}

step() { step_no=$((step_no + 1)); step_name=$1; }
pass() { say "ok    step $step_no: $step_name${1:+ ($1)}"; }

# Run a parosctl command. Its output goes to $out, its exit code to $code.
ctl() {
  out=$(RUST_LOG=error "$bin/parosctl" "$@" 2>&1)
  code=$?
}

# Run a parosctl command that must end with exit code $1.
expect() {
  local want=$1
  shift
  ctl "$@"
  [ "$code" -eq "$want" ] || fail "$step_name" "command: parosctl $*" \
    "exit code: $code (expected $want)" "output: $out"
}

# Run a parosctl command again until it ends with 0, for at most $patience seconds. Use it
# only where the cell is allowed to need time: after a start, a kill or a restart.
eventually() {
  local deadline=$((SECONDS + patience))
  while :; do
    ctl "$@"
    [ "$code" -eq 0 ] && return 0
    [ "$SECONDS" -ge "$deadline" ] && fail "$step_name" "command: parosctl $*" \
      "exit code: $code after ${patience}s of retries" "output: $out"
    sleep 1
  done
}

contains() {
  case "$out" in
    *"$1"*) ;;
    *) fail "$step_name" "the output does not contain: $1" "output: $out" ;;
  esac
}

port_free() { ! (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

start_machine() {
  local i=$1
  PAROS_LISTEN="127.0.0.1:${ports[$i]}" PAROS_DATA_DIR="$dir/node$i" PAROS_NAME="node$i" NO_COLOR=1 RUST_LOG="${VERIFY_RUST_LOG:-warn,parosd=info}" \
    "$bin/parosd" >>"$dir/node$i.log" 2>&1 &
  pids[node$i]=$!
}

alive() { kill -0 "${pids[$1]}" 2>/dev/null; }

trap 'stop_all' EXIT

# ---------------------------------------------------------------------------------------
step "build parosd and parosctl"
if $build; then
  (cd "$root" && cargo build -p parosd -p parosctl) >"$dir/build.log" 2>&1 \
    || fail "$step_name" "cargo build failed; see $dir/build.log" "$(tail -n 5 "$dir/build.log")"
fi
[ -x "$bin/parosd" ] && [ -x "$bin/parosctl" ] \
  || fail "$step_name" "no binary in $bin; run without --no-build"
pass

step "start three machines"
if [ -z "$port_base" ]; then
  port_base=$((20000 + RANDOM % 20000))
  while ! { port_free "$port_base" && port_free $((port_base + 1)) && port_free $((port_base + 2)); }; do
    port_base=$((20000 + RANDOM % 20000))
  done
fi
declare -A ports=([1]=$port_base [2]=$((port_base + 1)) [3]=$((port_base + 2)))
servers="127.0.0.1:${ports[1]},127.0.0.1:${ports[2]},127.0.0.1:${ports[3]}"
export PAROSCTL_SERVERS=$servers
for i in 1 2 3; do start_machine "$i"; done
sleep 1
for i in 1 2 3; do
  alive "node$i" || fail "$step_name" "node$i ended at its start"
done
pass "$servers"

step "form the cell (parosctl init)"
eventually init --members "$servers"
contains "initialized"
pass

step "a second init is refused"
expect 3 init --members "$servers"
contains "already initialized"
pass

step "create a tenant and a journal"
eventually tenant create acme
eventually journal create acme orders
contains "created"
journal=acme/orders
pass "$journal"

step "write two records and read them back"
eventually write "$journal" first second --leader 7
expect 0 read "$journal"
contains "first"
contains "second"
pass

step "a superseded writer is refused"
expect 0 set-leader "$journal" --new 8
expect 3 write "$journal" stale --leader 7 --no-claim
expect 0 write "$journal" third --leader 8
expect 0 read "$journal"
contains "third"
case "$out" in *stale*) fail "$step_name" "the refused record is in the journal" "output: $out" ;; esac
pass

step "kill one machine (SIGKILL) and keep writing"
kill -KILL "${pids[node2]}"
wait "${pids[node2]}" 2>/dev/null
eventually write "$journal" fourth --leader 8
eventually read "$journal"
contains "first"
contains "fourth"
pass "node2 is down; two machines serve"

step "start the machine again"
start_machine 2
sleep 1
alive node2 || fail "$step_name" "node2 ended at its restart"
eventually --servers "127.0.0.1:${ports[2]}" inspect --journal "$journal"
eventually write "$journal" fifth --leader 8
eventually read "$journal"
for record in first second third fourth fifth; do contains "$record"; done
pass "node2 serves again; the five records are read in the journal"

step "the views answer"
expect 0 machine list
for i in 1 2 3; do contains "node$i"; done
eventually status
expect 0 journal list acme
contains "orders"
pass

step "stop the cell (SIGTERM)"
for i in 1 2 3; do kill -TERM "${pids[node$i]}"; done
for i in 1 2 3; do
  wait "${pids[node$i]}"
  code=$?
  [ "$code" -eq 0 ] || fail "$step_name" "node$i ended with exit code $code (expected 0)"
done
pids=()
pass

say "verify-cell: every step passed ($step_no steps)"
if $keep; then say "The directory of the run is kept: $dir"; else rm -rf "$dir"; fi
