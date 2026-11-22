#!/usr/bin/env bash
# The pin of the proved specs (#1459 (pin the proved specs)). TLAPS proves
# the models that a `*Proof.tla` module extends (scripts/check-proofs.sh).
# `specs/tla/PROVED.sha256` holds the SHA-256 of each proof module and of
# each module of its folder that a proof extends: the files that the proof
# covers. A change to one of them needs the proof to run again.
#
#   scripts/proof-hashes.sh check   # fail when a pinned file changed or a covered file is not pinned
#   scripts/proof-hashes.sh write   # pin the current files (check-proofs.sh does it after a full pass)
set -euo pipefail
cd "$(dirname "$0")/../specs/tla"
pin=PROVED.sha256

# Each file that a proof covers, as a path under specs/tla/, sorted: each
# *Proof.tla, and each module of its folder that it names after EXTENDS. The
# walk is one level deep: a module that such a model extends in turn is not
# followed, so a proved model that extends a sibling module needs this walk
# to grow first.
covered() {
  for proof in */*Proof.tla; do
    [ -f "$proof" ] || continue
    dir=$(dirname "$proof")
    echo "$proof"
    # The names after EXTENDS, over as many lines as the list takes.
    awk '/^EXTENDS/ { on = 1; sub(/^EXTENDS/, "") }
         on { print; if ($0 !~ /,[[:space:]]*$/) exit }' "$proof" \
      | tr ',' '\n' | tr -d ' \t' | while read -r name; do
          if [ -n "$name" ] && [ -f "$dir/$name.tla" ]; then echo "$dir/$name.tla"; fi
        done
  done | sort -u
}

case "${1:-}" in
  write)
    covered | xargs sha256sum >"$pin"
    echo "pinned $(wc -l <"$pin") files in specs/tla/$pin"
    ;;
  check)
    if [ ! -f "$pin" ]; then
      echo "FAIL  specs/tla/$pin is missing" >&2
      exit 1
    fi
    status=0
    if ! diff <(covered) <(awk '{ print $2 }' "$pin" | sort) >/dev/null; then
      echo "FAIL  the files that the proofs cover are not the files of specs/tla/$pin:" >&2
      diff <(covered) <(awk '{ print $2 }' "$pin" | sort) >&2 || true
      status=1
    fi
    if ! sha256sum --quiet -c "$pin"; then
      status=1
    fi
    if [ "$status" -ne 0 ]; then
      echo "Run the proofs again (nix develop .#proofs --command scripts/check-proofs.sh)," >&2
      echo "then scripts/proof-hashes.sh write, in the same pull request." >&2
    fi
    exit "$status"
    ;;
  *)
    echo "usage: $0 check|write" >&2
    exit 2
    ;;
esac
