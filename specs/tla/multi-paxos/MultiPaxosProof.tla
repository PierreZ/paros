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
<1>1. CASE UNCHANGED vars \/ \E a \in Acceptors : Persist(a)
  <2>a. UNCHANGED vars
    BY <1>1, PersistStutter
  <2>0. TypeOK /\ UNCHANGED <<prom, vote, dprom, dvote, learned, phase, next, front, wiped, restarts, msgs>>
    BY <2>a DEF Inv, vars
  <2>1. \A x, c, s, v : VotedFor(x, c, s, v)' = VotedFor(x, c, s, v)
    BY <2>0 DEF VotedFor
  <2>2. \A x \in Acceptors : prom'[x] \in Nat /\ prom'[x] >= prom[x]
    BY <2>0 DEF TypeOK
  <2>3. KeepsVW
    <3>1. \A x \in Acceptors, c \in Ballots, s \in Slots :
            WontVoteIn(x, c, s) => \A v \in Values : ~VotedFor(x, c, s, v)'
      BY <2>1 DEF WontVoteIn
    <3> QED
      BY <2>0, <2>2, <3>1, KeepsVWFrom
  <2>4. TypeOK' /\ restarts' <= Restarts /\ Cardinality(wiped') <= Wipes
        /\ dprom' = prom' /\ dvote' = vote'
    BY <2>0 DEF Inv, TypeOK
  <2>5. NodeInv'
    <3> SUFFICES ASSUME NEW x \in Acceptors PROVE NodeOK(x)'
      BY DEF NodeInv
    <3>1. IF x \in wiped THEN prom'[x] = prom[x] ELSE prom'[x] \in Nat /\ prom'[x] >= prom[x]
      BY <2>0 DEF TypeOK
    <3> QED
      BY <2>0, <2>1, <3>1, NodeKeep
  <2>6. BallotInv'
    <3> SUFFICES ASSUME NEW c \in Ballots PROVE BallotOK(c)'
      BY DEF BallotInv
    <3>1. phase[c] \in {"p1", "p2"} => Owner[c] \notin wiped'
      BY <2>0 DEF Inv, BallotInv, BallotOK
    <3> QED
      BY <2>0, <2>2, <2>3, <3>1, BallotKeep
  <2>7. MsgInv'
    <3> SUFFICES ASSUME NEW m \in msgs' PROVE MsgOK(m)'
      BY DEF MsgInv
    <3> QED
      BY <2>0, <2>1, <2>2, <2>3, OldMsg
  <2> QED
    BY <2>4, <2>5, <2>6, <2>7 DEF Inv
<1> QED
  BY <1>1, PrepareInv, WinInv, AllocateInv, PromiseInv, AcceptInv, DecideInv, CatchUpInv,
     RestartInv, WipeInv DEF Next

-----------------------------------------------------------------------------
(* The theorem. `InitInv` and `StepInv` are the two premises of the TLA   *)
(* proof rule INV1 (Lamport, "The Temporal Logic of Actions", 1994):      *)
(*   Init => Inv,  Inv /\ [Next]_vars => Inv'  |-  Init /\ [][Next]_vars => []Inv. *)
(* TLAPS checks the use of that rule with its PTL backend, LS4, which the *)
(* proofs shell of the flake builds from source.                          *)
(***************************************************************************)

THEOREM Safety == Hyp => (Spec => []Agreement)
<1> SUFFICES ASSUME Hyp PROVE Spec => []Agreement
  OBVIOUS
<1>1. Init => Inv
  BY InitInv
<1>2. Inv /\ [Next]_vars => Inv'
  BY StepInv
<1>3. Inv => Agreement
  BY InvAgreement
<1> QED
  BY <1>1, <1>2, <1>3, PTL DEF Spec

=============================================================================
