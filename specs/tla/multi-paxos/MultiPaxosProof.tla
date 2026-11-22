--------------------------- MODULE MultiPaxosProof ---------------------------
(***************************************************************************)
(* The TLAPS proof that `Inv` of `MultiPaxos.tla` is an inductive          *)
(* invariant and that it implies `Agreement` (#1307 (inductive invariants, *)
(* then proofs), step 2). TLC checks `Inv` on drawn states only            *)
(* (`inductive*.cfg`); this proof holds for every number of acceptors,     *)
(* commands, ballots and slots, and every quorum system where each Phase-1 *)
(* quorum meets each Phase-2 quorum of each slot.                          *)
(*                                                                         *)
(* The proof covers the configurations of `InvApplies` (the disk is the    *)
(* memory, a wiped acceptor never takes a step again, no acceptor is       *)
(* down) with the rules of the code: `Pick = "highest"` and                *)
(* `Alloc = "frontier"`. `Gap` is free: "noop" and "skip" are both covered. *)
(*                                                                         *)
(* Run: `nix develop .#proofs --command scripts/check-proofs.sh`.          *)
(***************************************************************************)
EXTENDS MultiPaxosFactsProof, MultiPaxosPhase1Proof, MultiPaxosPhase2Proof,
        MultiPaxosLearnProof, MultiPaxosWinProof

USE NBType, NSType, NoneNotValue, NoopNotCommand, OwnerType, LearnersType
    DEF Ballots, Slots, Values, NoVote

THEOREM StepInv ==
  ASSUME Inv, Hyp, [Next]_vars
  PROVE  Inv'
<1>1. CASE UNCHANGED vars
  BY <1>1, UnchangedInv
<1> QED
  BY <1>1, PrepareInv, WinInv, AllocateInv, PromiseInv, AcceptInv, DecideInv, CatchUpInv,
     PersistInv, RestartInv, WipeInv DEF Next

-----------------------------------------------------------------------------
(* The theorem. `InitInv` and `StepInv` are the two premises of the TLA   *)
(* proof rule INV1 (Lamport, "The Temporal Logic of Actions", 1994):      *)
(*   Init => Inv,  Inv /\ [Next]_vars => Inv'  |-  Init /\ [][Next]_vars => []Inv. *)
(* TLAPS checks a use of that rule with its PTL backend, LS4, which the   *)
(* TLAPS of nixpkgs does not ship. So the last step is the one step that  *)
(* TLAPS does not check here: it is the rule itself, applied as written.  *)
(***************************************************************************)

THEOREM Safety == Hyp => (Spec => []Agreement)
<1>1. Hyp /\ Init => Inv
  BY InitInv
<1>2. Hyp /\ Inv /\ [Next]_vars => Inv'
  BY StepInv
<1>3. Inv /\ Hyp => Agreement
  BY InvAgreement
<1> QED
  PROOF OMITTED

=============================================================================
