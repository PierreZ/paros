#!/usr/bin/env bash
# Check the TLAPS proofs of specs/tla/ (#1307 (inductive invariants, then
# proofs)). A proof is a module `<Name>Proof.tla` beside the model it proves.
# tlapm (TLAPS) sends each proof obligation to its backends (Z3 and Zenon);
# the script fails when one obligation is not proved.
#
#   nix develop .#proofs --command scripts/check-proofs.sh           # every proof
#   nix develop .#proofs --command scripts/check-proofs.sh decree    # one folder
#
# The TLAPS of nixpkgs has no Isabelle and no LS4 (the PTL backend), so a
# step that needs one of them cannot be checked here; a proof marks such a
# step `PROOF OMITTED` and says why. The script prints the summary of each
# module, which counts the omitted steps.
set -euo pipefail
cd "$(dirname "$0")/.."
dirs=()
for arg in "$@"; do
  dirs+=("specs/tla/$arg")
done
if [ "${#dirs[@]}" -eq 0 ]; then
  for proof in specs/tla/*/*Proof.tla; do
    [ -f "$proof" ] && dirs+=("$(dirname "$proof")")
  done
fi
threads="${TLAPM_THREADS:-$(nproc)}"
status=0
for dir in $(printf '%s\n' "${dirs[@]}" | sort -u); do
  found=false
  for proof in "$dir"/*Proof.tla; do
    [ -f "$proof" ] || continue
    found=true
    name=$(basename "$proof")
    log=$(mktemp)
    # --cleanfp: prove every obligation again, with no fingerprint cache.
    if (cd "$dir" && tlapm --cleanfp --threads "$threads" --stretch 2 "$name") >"$log" 2>&1 \
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
  if [ "$found" = false ]; then
    echo "FAIL  $dir: no *Proof.tla" >&2
    status=1
  fi
done
exit "$status"
