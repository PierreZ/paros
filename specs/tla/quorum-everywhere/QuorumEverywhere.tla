--------------------------- MODULE QuorumEverywhere ---------------------------
(***************************************************************************)
(* Quorum everywhere in the journal, #1257 (quorum everywhere, no step     *)
(* needs every acceptor). The rule (Pierre, 2026-11-20): every step of the *)
(* journal waits only for a quorum. No step waits for every member of a    *)
(* group, as a FoundationDB commit waits for every TLog.                   *)
(*                                                                         *)
(* The model is a liveness check. A set of members is down at the start    *)
(* and stays down (crash-stop, the worst case for liveness). The journal   *)
(* then runs its steps one after the other. Each step is a list of waits.  *)
(* A wait sends to a set of targets and completes when the answers hold    *)
(* one of its quorums. A live target answers some time; a down target      *)
(* never answers. A wait can have more than one alternative: a write that  *)
(* is stuck in one column moves to the next column with the same slot,     *)
(* ballot and value (#533). The move comes only when every live target of  *)
(* the alternative answered and the answers hold no quorum: the abstract   *)
(* form of the re-send timeout.                                            *)
(*                                                                         *)
(* The quorums come from `Grid.tla`, the same sets that the other models   *)
(* and `QuorumSystem::Grid { rows, cols, acks }` use:                       *)
(*   - a write (Phase 2) waits for `acks` acceptors of one column;          *)
(*   - Phase 1 waits for `rows - acks + 1` acceptors of every column, for  *)
(*     each configuration of `H_b`;                                         *)
(*   - a leaderless read sends to `GridReadTargets` and waits for a        *)
(*     Phase-1 quorum of the answers (`QuorumRead`, `has_phase1_quorum_in`). *)
(* Matchmaker quorums are majorities. A segment retires after `F + 1`      *)
(* replicas of its replica set hold it.                                    *)
(*                                                                         *)
(* The journal holds two configurations: `Old`, a sealed segment, and      *)
(* `New`, the configuration in force. `New` is `Old` with the members      *)
(* moved by `Shift` places, so a member that stays can change its column.  *)
(* There are two matchmaker sets, `MM` and its successor `MM2`, for the    *)
(* handover, and one replica set `R` for the sealed segment.               *)
(*                                                                         *)
(* Constants:                                                              *)
(*   Rows, Cols, Acks  the grid of both configurations.                    *)
(*   F                 the members that may be down: F in each column of   *)
(*                     each configuration, F of each matchmaker set, F of  *)
(*                     the replica set.                                    *)
(*   Shift             how far `New` moves the members of `Old`.           *)
(*   Mode   "quorum"   each wait completes on a quorum (the design).       *)
(*          "all"      each wait needs every target (a FoundationDB TLog   *)
(*                     commit): a control.                                 *)
(*   Extra  "none"     F down, as above.                                   *)
(*          "column"   F + 1 down in one column, F in the others.          *)
(*          "every_column" F + 1 down in every column of `New`.            *)
(*          "mm"       F + 1 down in one matchmaker set.                   *)
(*          "replica"  F + 1 down in the replica set.                      *)
(*   Only              the one step to run, or "all" for every step.       *)
(***************************************************************************)
EXTENDS Grid

CONSTANTS Rows, Cols, Acks, F, Shift, Mode, Extra, Only

N == Rows * Cols

ASSUME /\ Rows >= 1 /\ Cols >= 1 /\ Acks \in 1..Rows
       /\ F \in 0..Rows /\ Shift \in 0..N
       /\ Mode \in {"quorum", "all"}
       /\ Extra \in {"none", "column", "every_column", "mm", "replica"}

(***************************************************************************)
(* The members. Each is a pair <<kind, number>>, so the three kinds can    *)
(* share one set of answers.                                               *)
(***************************************************************************)
A(i) == <<"a", i>>
OldSeq == [i \in 1..N |-> A(i)]
NewSeq == [i \in 1..N |-> A(i + Shift)]
Acceptors == {A(i) : i \in 1..(N + Shift)}

MM  == {<<"m", i>> : i \in 1..(2 * F + 1)}
MM2 == {<<"m", i>> : i \in 2..(2 * F + 2)}
R   == {<<"r", i>> : i \in 1..(2 * F + 1)}

ColumnsOf(M) == {GridColumn(M, Cols, j) : j \in 0..(Cols - 1)}
AllColumns == ColumnsOf(OldSeq) \cup ColumnsOf(NewSeq)

(***************************************************************************)
(* The waits.                                                              *)
(***************************************************************************)
\* One alternative: its targets and its quorums. In mode "all" the only
\* quorum is every target.
Alt(T, Qs) == [to |-> T, q |-> IF Mode = "all" THEN {T} ELSE Qs]

\* A sequence that holds each element of a finite set once.
SeqOf(S) == CHOOSE s \in [1..Cardinality(S) -> S] : \A i, j \in DOMAIN s : i # j => s[i] # s[j]

MMWait(S) == <<Alt(S, MajorityQuorums(S))>>

\* Phase 2 in one column, with the move to the next column (#533).
Phase2(M) == [j \in 1..Cols |-> Alt(GridColumn(M, Cols, j - 1), GridQ2At(M, Cols, Acks, j - 1))]

\* Phase 1 in one configuration: `rows - acks + 1` of every column.
Phase1(M) == <<Alt(GridMembers(M), GridQ1(M, Cols, Acks, 0))>>

\* A leaderless read: each set of `GridReadTargets` is one alternative (one
\* row on the full grid, the whole membership else), and the read completes
\* on a Phase-1 quorum of its answers.
ReadAlts(M) ==
  LET T == SeqOf(GridReadTargets(M, Cols, Acks))
  IN [k \in DOMAIN T |-> Alt(T[k], GridQ1(M, Cols, Acks, 0))]

\* The replicas that hold a sealed segment before it retires.
HoldQ == {Q \in SUBSET R : Cardinality(Q) >= F + 1}

Steps == <<"write", "election", "read", "seal", "retire", "offload", "trim", "handover">>

\* The waits of each step, in order. A zero-arity definition: TLC computes
\* it once.
WaitsOf == [s \in {Steps[k] : k \in DOMAIN Steps} |->
  CASE s = "write"    -> <<Phase2(NewSeq)>>
    \* Matchmaking at a matchmaker quorum, then Phase 1 over every
    \* configuration of H_b (`Old` below its end slot, and `New`).
    [] s = "election" -> <<MMWait(MM), Phase1(OldSeq), Phase1(NewSeq)>>
    \* The read of the configuration in force, then the check of the
    \* segment record at a matchmaker quorum (#1254).
    [] s = "read"     -> <<ReadAlts(NewSeq), MMWait(MM)>>
    \* The sealing leader's Phase 1 in the old grid finds the highest vote;
    \* the end slot is written once by compare-and-set at the matchmakers.
    [] s = "seal"     -> <<Phase1(OldSeq), MMWait(MM)>>
    \* F + 1 replicas hold the sealed segment, then a matchmaker quorum
    \* acks its retirement.
    [] s = "retire"   -> <<<<Alt(R, HoldQ)>>, MMWait(MM)>>
    \* The offload mark in the segment record (#1256).
    [] s = "offload"  -> <<MMWait(MM)>>
    \* The decided `Truncate` (a Phase-2 round), then the record of the trim.
    [] s = "trim"     -> <<Phase2(NewSeq), MMWait(MM)>>
    \* The handover of the matchmaker set: a stop quorum of `MM`, then the
    \* successor decree in `MM2`, Phase 1 and Phase 2.
    [] s = "handover" -> <<MMWait(MM), MMWait(MM2), MMWait(MM2)>>]

Run == IF Only = "all" THEN Steps ELSE <<Only>>

ASSUME Only = "all" \/ Only \in {Steps[k] : k \in DOMAIN Steps}

(***************************************************************************)
(* The members that are down.                                              *)
(***************************************************************************)
AtMost(D, Gs, k) == \A g \in Gs : Cardinality(D \cap g) <= k

\* The down sets of the design. A wait that completes with a down set also
\* completes with each smaller one (its guard only grows with the answers),
\* so the check takes only the down sets that no member can join: the
\* largest ones. TLC still checks every way to place them.
AccOk(D, k) == AtMost(D, AllColumns, k)
MMOk(D, k) == AtMost(D, {MM, MM2}, k)

AccDown ==
  CASE Extra = "column" ->
         {D \in SUBSET Acceptors :
            AccOk(D, F + 1) /\ Cardinality({c \in AllColumns : Cardinality(D \cap c) > F}) = 1}
    [] Extra = "every_column" ->
         {D \in SUBSET Acceptors :
            AccOk(D, F + 1) /\ \A c \in ColumnsOf(NewSeq) : Cardinality(D \cap c) = F + 1}
    [] OTHER -> {D \in SUBSET Acceptors :
                   AccOk(D, F) /\ \A x \in Acceptors \ D : ~AccOk(D \cup {x}, F)}

MMDown ==
  IF Extra = "mm"
  THEN {D \in SUBSET (MM \cup MM2) :
          MMOk(D, F + 1) /\ (Cardinality(D \cap MM) = F + 1 \/ Cardinality(D \cap MM2) = F + 1)}
  ELSE {D \in SUBSET (MM \cup MM2) :
          MMOk(D, F) /\ \A x \in (MM \cup MM2) \ D : ~MMOk(D \cup {x}, F)}

RDown ==
  IF Extra = "replica"
  THEN {D \in SUBSET R : Cardinality(D) = F + 1}
  ELSE {D \in SUBSET R : Cardinality(D) = F}

\* The check covers the worst case of the issue: some down set has exactly F
\* members in every column of both configurations at once.
ASSUME Extra = "none" =>
  \E D \in AccDown : \A c \in AllColumns : Cardinality(D \cap c) = F

VARIABLES down, step, wait, alt, answered

vars == <<down, step, wait, alt, answered>>

Finished == step > Len(Run)
Waits == WaitsOf[Run[step]]
Cur == Waits[wait][alt]
HasQuorum == \E Q \in Cur.q : Q \subseteq answered

TypeOK ==
  /\ step \in 1..(Len(Run) + 1)
  /\ answered \subseteq Acceptors \cup MM \cup MM2 \cup R

Init ==
  /\ \E da \in AccDown, dm \in MMDown, dr \in RDown : down = da \cup dm \cup dr
  /\ step = 1
  /\ wait = 1
  /\ alt = 1
  /\ answered = {}

\* A live target answers. The order of the answers does not change what a
\* wait can complete (its guard only grows with the answers), so the targets
\* answer in one fixed order: this keeps the state space linear in the
\* number of answers.
Answer ==
  /\ ~Finished
  /\ Cur.to \ (down \cup answered) # {}
  /\ answered' = answered \cup {CHOOSE p \in Cur.to \ (down \cup answered) : TRUE}
  /\ UNCHANGED <<down, step, wait, alt>>

\* The answers hold a quorum: the wait completes, and the next one starts.
Complete ==
  /\ ~Finished
  /\ HasQuorum
  /\ answered' = {}
  /\ alt' = 1
  /\ IF wait < Len(Waits)
     THEN wait' = wait + 1 /\ UNCHANGED step
     ELSE wait' = 1 /\ step' = step + 1
  /\ UNCHANGED down

\* Every live target answered and the answers hold no quorum: the wait moves
\* to its next alternative, if it has one.
Move ==
  /\ ~Finished
  /\ Len(Waits[wait]) > 1
  /\ Cur.to \ down \subseteq answered
  /\ ~HasQuorum
  /\ alt' = (alt % Len(Waits[wait])) + 1
  /\ answered' = {}
  /\ UNCHANGED <<down, step, wait>>

Next ==
  \/ Answer
  \/ Complete
  \/ Move

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

\* Every step completes.
Done == <>Finished
=============================================================================
