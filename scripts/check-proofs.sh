#!/usr/bin/env bash
# Check the TLAPS proofs of specs/tla/ (#1307 (inductive invariants, then
# proofs)). A proof is a module `<Name>Proof.tla` beside the model it proves;
# a long proof is split into several such modules, and the module that
# extends them all is the map of the proof. tlapm (TLAPS) sends each proof
# obligation to its backends (Z3, Zenon, and LS4 for a temporal step); the
# script fails when one obligation is not proved or a step is omitted.
#
#   nix develop .#proofs --command scripts/check-proofs.sh                 # every proof, then the pin
#   nix develop .#proofs --command scripts/check-proofs.sh decree          # one folder
#   nix develop .#proofs --command scripts/check-proofs.sh \
#     multi-paxos/MultiPaxosWinProof.tla                                     # one module
#
# The TLAPS of nixpkgs has no Isabelle and no LS4 (the PTL backend). The
# proofs shell of the flake adds LS4 and `ptl_to_trp`, built from source, so
# a temporal step such as `Spec => []Inv` is checked too. A `PROOF OMITTED`
# step fails the check: an omitted step is a step that nothing checks.
#
# `--method smt,zenon,ls4` names the backends that exist. With the default
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
  if (cd "$dir" && tlapm --cleanfp --method smt,zenon,ls4 --threads "$threads" --stretch 2 "$name") \
      >"$log" 2>&1 \
    && ! grep -q "Could not prove or check" "$log"; then
    echo "ok    $proof"
  else
    echo "FAIL  $proof" >&2
    grep -A40 -E "^File|Error" "$log" | grep -v "isabelle-process\|^/nix/store\|Executable .* not found\|PATH" >&2 || true
    status=1
  fi
  summary=$( (cd "$dir" && tlapm --summary "$name") 2>/dev/null || true)
  echo "$summary" | grep -E "obligations_count|omitted|incomplete" || true
  if echo "$summary" | grep -q "omitted_proofs_count"; then
    echo "FAIL  $proof: a step is PROOF OMITTED" >&2
    status=1
  fi
  rm -rf "$dir/${name%.tla}.tlaps" "$log"
done
# A full pass pins the files that the proofs cover (specs/tla/PROVED.sha256).
if [ "$#" -eq 0 ] && [ "$status" -eq 0 ]; then
  scripts/proof-hashes.sh write
fi
exit "$status"
