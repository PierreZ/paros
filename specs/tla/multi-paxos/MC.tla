--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. The quorums of *)
(* a grid come from `grid/Grid.tla`, as in `config-register/` and `reads/`. *)
EXTENDS MultiPaxos, TLC, Grid

\* The acceptor names, model values that the configurations set.
CONSTANTS a1, a2, a3, a4, a5, a6

\* A majority of the acceptors, the same for every slot.
Majorities == MajorityQuorums(Acceptors)
MajQ2 == [s \in Slots |-> Majorities]

\* A slot's Phase-2 quorums on a grid: `acks` acceptors of column s % cols
\* (`column_of`).
SlotQ2(M, cols, acks) == [s \in Slots |-> GridQ2At(M, cols, acks, s % cols)]

\* A slot's Phase-2 quorums when the round may move: `acks` acceptors of
\* any one column (`move_round`, `propose_in`; the decision is judged in the
\* round's current column, `proposer/rounds.rs`). It holds the home column.
AnyQ2(M, cols, acks) == [s \in Slots |-> GridQ2(M, cols, acks)]

\* A control: a Phase-1 quorum is `rows - acks + 1` acceptors of one column
\* only, not of every column.
GridQ1OneColumn(M, cols, acks) ==
  {Q \in SUBSET GridMembers(M) :
     \E j \in 0..(cols - 1) :
       Cardinality(Q \cap GridColumn(M, cols, j)) >= GridPhase1Count(M, cols, acks, 0)}

\* The owners of ballots 1 and 2.
Own3 == [b \in Ballots |-> IF b % 3 = 1 THEN a1 ELSE IF b % 3 = 2 THEN a2 ELSE a3]

\* Four acceptors in a 2 x 2 grid: the full grid (acks = rows).
Layout4 == <<a1, a2, a3, a4>>
FullQ1 == GridQ1(Layout4, 2, 2, 0)
FullQ2 == SlotQ2(Layout4, 2, 2)
FullQ2Any == AnyQ2(Layout4, 2, 2)
FullQ1Bad == GridQ1OneColumn(Layout4, 2, 2)

\* Six acceptors in a 3 x 2 grid: two partitions with F = 1 (acks = F + 1).
Layout6 == <<a1, a2, a3, a4, a5, a6>>
PartQ1 == GridQ1(Layout6, 2, 2, 0)
PartQ2 == SlotQ2(Layout6, 2, 2)
PartQ2Any == AnyQ2(Layout6, 2, 2)

\* Every acceptor may restart (safety checks).
AllAcceptors == Acceptors

\* A liveness check: every acceptor but the owner of the last ballot.
NotLastOwner == Acceptors \ {Own3[NB]}

\* Node a1 owns ballots 1 and 3: one node leads two times, and its second
\* campaign reads its own durable log after a restart.
OwnTwice == [b \in Ballots |-> IF b % 2 = 1 THEN a1 ELSE a2]

Symmetry == Permutations(Commands)

-----------------------------------------------------------------------------
(* Random states where `Inv` holds (#1307 (inductive invariants, then      *)
(* proofs)).                                                               *)
(*                                                                         *)
(* The states of a Multi-Paxos instance where `Inv` holds are too many for *)
(* TLC to list: with three acceptors, two ballots and one slot, more than  *)
(* four million. So `IndInit` draws `Samples` random states where `Inv`    *)
(* holds, as Lamport's note "Using TLC to Check Inductive Invariance"      *)
(* proposes, and the check with the invariant `Inv` judges `Inv'` after    *)
(* every step from each of them. A violation is a state where `Inv` holds  *)
(* and a step that breaks it: `Inv` is not inductive. A pass says that no  *)
(* such state was drawn. TLC draws with its `-seed`, so a run is the same  *)
(* each time (`checks.txt` gives the seed).                                *)
(*                                                                         *)
(* Each part of a state is drawn from the values that `Inv` allows once    *)
(* the parts before it are known:                                          *)
(*   - the disk is the memory (`dprom = prom`, `dvote = vote`);            *)
(*   - a ballot proposes at most one value at a slot, and only below its   *)
(*     allocator, so the 2a messages are a function `P`;                   *)
(*   - a vote is for the proposal of its ballot and slot, so a 2b message  *)
(*     is an <<acceptor, ballot, slot>>;                                   *)
(*   - a record and each slot of a report is a proposal, so it is a ballot *)
(*     (0 for `NoVote`) and `P` gives its value;                           *)
(*   - a learned value is the value of the record;                         *)
(*   - one 1a message for each ballot that left `idle`, and at most one 1b *)
(*     message for each acceptor and ballot.                               *)
(* A draw where a proposal is not `SafeAt` or a free slot is not `FreeAt`  *)
(* is dropped. Two parts are fixed: `restarts = 0`, because a higher count *)
(* only disables `Restart`, and `front = 0`, because no step reads `front` *)
(* and `Inv` says nothing of it. The check's `INVARIANT Inv` judges every  *)
(* drawn state too, so a draw outside `Inv` fails the check.               *)
(***************************************************************************)

\* How many states `IndInit` draws.
Samples == 100000

\* The facts of `MultiPaxos` judged on chosen messages, promises and wipes.
Pre(M, pr, W) == INSTANCE MultiPaxos WITH msgs <- M, prom <- pr, wiped <- W

\* The 2a and 2b messages of the proposals P and the votes V.
M2(P, V) ==
  {[type |-> "2a", bal |-> y[1], slot |-> y[2], val |-> P[y]] : y \in {z \in Ballots \X Slots : P[z] # None}}
    \cup {[type |-> "2b", acc |-> x[1], bal |-> x[2], slot |-> x[3], val |-> P[x[2], x[3]]] : x \in V}

\* The record of slot s at ballot b (0 for `NoVote`).
Rec(P, s, b) == IF b = 0 THEN NoVote ELSE [bal |-> b, val |-> P[b, s]]

\* The ballots that can be a record of a at s, held below `below`.
RecBals(M, pr, W, P, a, s, below) ==
  {b \in 0..NB : (b = 0 \/ P[b, s] # None) /\ Pre(M, pr, W)!RecordOK(a, s, Rec(P, s, b), below)}

\* The phases that ballot b can have when the acceptors W are wiped.
PhaseOpts(W, b) == IF Owner[b] \in W THEN {"idle", "dead"} ELSE {"idle", "p1", "p2", "dead"}

\* The promises that acceptor a can hold: at or above each ballot it owns
\* that left `idle`.
PromOpts(W, ph, a) ==
  IF a \in W THEN {0} ELSE {p \in 0..NB : \A b \in Ballots : Owner[b] = a /\ ph[b] # "idle" => p >= b}

\* The votes that can be cast: at a proposal, and at or below the promise.
VoteCands(W, pr, P) ==
  {x \in Acceptors \X Ballots \X Slots : P[x[2], x[3]] # None /\ (x[1] \in W \/ x[2] <= pr[x[1]])}

\* The record ballots of acceptor x[1] at slot x[2].
RecordOpts(M, W, pr, P, x) ==
  IF x[1] \in W THEN {0}
  ELSE {b \in RecBals(M, pr, W, P, x[1], x[2], NB + 1) :
          /\ b <= pr[x[1]]
          /\ b > 0 /\ ~Pre(M, pr, W)!VotedFor(x[1], b, x[2], P[b, x[2]])
               => Pre(M, pr, W)!ChosenBy(x[2], b, P[b, x[2]])}

\* What node x[1] learned at slot x[2]: nothing, or the value of its record.
LearnOpts(M, W, pr, P, rb, x) ==
  {None} \cup
  (IF /\ x[1] \notin W
      /\ x[1] \in Learners \cup {Owner[b] : b \in Ballots}
      /\ rb[x] > 0
      /\ Pre(M, pr, W)!ChosenBy(x[2], rb[x], P[rb[x], x[2]])
   THEN {P[rb[x], x[2]]} ELSE {})

\* No promise: a report that no ballot can hold.
Absent == [s \in Slots |-> NB + 1]

\* The promise of acceptor x[1] at ballot x[2]: none, or the record ballot
\* that it reports at each slot. The owner of a ballot that left `idle`
\* promised it.
PromiseOpts(M, W, ph, pr, P, fm, x) ==
  IF ph[x[2]] = "idle" \/ (x[1] \notin W /\ x[2] > pr[x[1]]) THEN {Absent}
  ELSE (IF x[1] = Owner[x[2]] THEN {} ELSE {Absent})
       \cup {f \in [Slots -> 0..NB] :
               \A s \in Slots :
                 IF s >= fm[x[2]] THEN f[s] \in RecBals(M, pr, W, P, x[1], s, x[2]) ELSE f[s] = 0}

\* A random choice from each set F[k] of a function on K. `TLCEval` makes
\* TLC draw each value now and keep it: a lazy function would draw again at
\* each use.
Draw(K, F) == TLCEval([k \in K |-> RandomElement(F[k])])

\* Each part is bound by `\E x \in {...}` so that TLC draws it one time.
IndStates ==
  \E i \in 1..Samples :
  \E W \in {RandomElement({X \in SUBSET Acceptors : Cardinality(X) <= Wipes})} :
  \E ph \in {Draw(Ballots, [b \in Ballots |-> PhaseOpts(W, b)])} :
  \E pr \in {Draw(Acceptors, [a \in Acceptors |-> PromOpts(W, ph, a)])} :
  \E nx \in {Draw(Ballots, [b \in Ballots |-> IF ph[b] \in {"p2", "dead"} THEN 0..NS ELSE {0}])} :
  \E P \in {Draw(Ballots \X Slots,
                 [y \in Ballots \X Slots |->
                    IF ph[y[1]] \in {"p2", "dead"} /\ y[2] < nx[y[1]] THEN Values \cup {None} ELSE {None}])} :
  \E coin \in {Draw(VoteCands(W, pr, P), [x \in VoteCands(W, pr, P) |-> BOOLEAN])} :
  LET V == {x \in VoteCands(W, pr, P) : coin[x]} IN
  LET M == M2(P, V) IN
  \* `= TRUE` makes TLC judge the test as a value: an `\E` inside a test
  \* of an initial predicate would be enumerated as a choice.
  /\ (\A b \in Ballots, s \in Slots : P[b, s] # None => Pre(M, pr, W)!SafeAt(b, s, P[b, s])) = TRUE
  /\ (\A b \in Ballots : ph[b] = "p2" => \A s \in Slots : s >= nx[b] => Pre(M, pr, W)!FreeAt(b, s)) = TRUE
  /\ \E rb \in {Draw(Acceptors \X Slots, [x \in Acceptors \X Slots |-> RecordOpts(M, W, pr, P, x)])} :
     \E ln \in {Draw(Acceptors \X Slots, [x \in Acceptors \X Slots |-> LearnOpts(M, W, pr, P, rb, x)])} :
     \E fm \in {Draw(Ballots, [b \in Ballots |-> IF ph[b] = "idle" THEN {0} ELSE 0..NS])} :
     \E pb \in {Draw(Acceptors \X Ballots, [x \in Acceptors \X Ballots |-> PromiseOpts(M, W, ph, pr, P, fm, x)])} :
       /\ wiped = W
       /\ phase = ph
       /\ prom = pr
       /\ dprom = pr
       /\ next = nx
       /\ front = [b \in Ballots |-> 0]
       /\ vote = [a \in Acceptors |-> [s \in Slots |-> Rec(P, s, rb[a, s])]]
       /\ dvote = vote
       /\ learned = [a \in Acceptors |-> [s \in Slots |-> ln[a, s]]]
       /\ restarts = 0
       /\ msgs = M
            \cup {[type |-> "1a", bal |-> b, from |-> fm[b]] : b \in {c \in Ballots : ph[c] # "idle"}}
            \cup {[type |-> "1b", acc |-> x[1], bal |-> x[2], from |-> fm[x[2]],
                   rep |-> [s \in Slots |-> Rec(P, s, pb[x][s])]] :
                    x \in {y \in Acceptors \X Ballots : pb[y] # Absent}}

IndInit ==
  /\ Assert(InvApplies, "Inv does not cover this configuration")
  /\ IndStates

\* One step from each drawn state, and no more: the drawn states are level 1.
IndNext == TLCGet("level") = 1 /\ Next
=============================================================================
