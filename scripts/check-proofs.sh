#!/usr/bin/env bash
# Check the TLAPS proofs of specs/tla/ (#1307 (inductive invariants, then
# proofs)). A proof is a module `<Name>Proof.tla` beside the model it proves;
# a long proof is split into several such modules, and the module that
# extends them all is the map of the proof. tlapm (TLAPS) sends each proof
# obligation to its backends (Z3 and Zenon); the script fails when one
# obligation is not proved.
#
#   nix develop .#proofs --command scripts/check-proofs.sh                 # every proof
#   nix develop .#proofs --command scripts/check-proofs.sh decree          # one folder
#   nix develop .#proofs --command scripts/check-proofs.sh \
#     multi-paxos/MultiPaxosWinProof.tla                                     # one module
#
# The TLAPS of nixpkgs has no Isabelle and no LS4 (the PTL backend), so a
# step that needs one of them cannot be checked here; a proof marks such a
# step `PROOF OMITTED` and says why. The script prints the summary of each
# module, which counts the omitted steps.
#
# `--method smt,zenon` names the two backends that exist. With the default
# method, tlapm also tries Isabelle on an obligation that the others fail,
# and the missing Isabelle stops the run at the first failure, so the report
# names one failure only. `--cleanfp` proves every obligation again: a run
# that reads the fingerprints of an earlier run can report a pass that it
# did not check.
set -euo pipefail
cd "$(dirname "$0")/.."
modules=()
for arg in "$@"; do
  case "$arg" in
    *.tla) modules+=("specs/tla/$arg") ;;
    *)
      found=false
      for proof in "specs/tla/$arg"/*Proof.tla; do
        [ -f "$proof" ] && { modules+=("$proof"); found=true; }
      done
      if [ "$found" = false ]; then
        echo "FAIL  specs/tla/$arg: no *Proof.tla" >&2
        exit 1
      fi
      ;;
  esac
done
if [ "${#modules[@]}" -eq 0 ]; then
  for proof in specs/tla/*/*Proof.tla; do
    [ -f "$proof" ] && modules+=("$proof")
  done
fi
threads="${TLAPM_THREADS:-$(nproc)}"
status=0
for proof in "${modules[@]}"; do
  if [ ! -f "$proof" ]; then
    echo "FAIL  $proof: no such module" >&2
    status=1
    continue
  fi
  dir=$(dirname "$proof")
  name=$(basename "$proof")
  log=$(mktemp)
  if (cd "$dir" && tlapm --cleanfp --method smt,zenon --threads "$threads" --stretch 2 "$name") \
      >"$log" 2>&1 \
    && ! grep -q "Could not prove or check" "$log"; then
    echo "ok    $proof"
  else
    echo "FAIL  $proof" >&2
    grep -A40 -E "^File|Error" "$log" | grep -v "isabelle-process\|^/nix/store\|Executable .* not found\|PATH" >&2 || true
    status=1
  fi
  (cd "$dir" && tlapm --summary "$name") 2>/dev/null | grep -E "obligations_count|omitted|incomplete" || true
  rm -rf "$dir/${name%.tla}.tlaps" "$log"
done
exit "$status"
