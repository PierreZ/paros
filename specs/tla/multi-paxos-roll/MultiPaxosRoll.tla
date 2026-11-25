---------------------------- MODULE MultiPaxosRoll ----------------------------
(***************************************************************************)
(* The `Roll` on plain Multi-Paxos (step 3 of                              *)
(* docs/archive/journal-design-a/prompt.md; §4.7 of docs/architecture/).   *)
(*                                                                         *)
(* The module extends `MultiPaxos` and changes none of it: a `Roll` is a   *)
(* command, so Phase 1 recovers it, the gap fill skips it and a learner    *)
(* learns it as any other value. `RollTo[c]` is 0 for a client command     *)
(* and k >= 2 for a `Roll` to segment k. A node folds the segments from    *)
(* the prefix it learned (R3): a `Roll` to its segment plus one ends the   *)
(* segment at the next slot; any other `Roll` changes nothing (R2).        *)
(*                                                                         *)
(* There is no pause (R4a, dropped by Pierre on 2026-11-25): the leader    *)
(* pipelines across a `Roll`. So the spec is `Spec` of `MultiPaxos`, with  *)
(* `Roll`s among the commands, and the inductive invariant `Inv`, proved   *)
(* for any set of commands (`MultiPaxosProof.tla`), holds with no change.  *)
(* The checks judge `Inv` on every reachable state, and the folds of the   *)
(* learners.                                                               *)
(***************************************************************************)
EXTENDS MultiPaxos

CONSTANT RollTo  \* RollTo[c]: 0 for a client command, k >= 2 for a Roll to k

ASSUME RollType == RollTo \in [Commands -> Nat] /\ \A c \in Commands : RollTo[c] # 1

IsRoll(v) == v \in Commands /\ RollTo[v] >= 2

\* Node n learned every slot below s.
PrefixLearned(n, s) == \A t \in Slots : t < s => learned[n][t] # None

\* The segment of slot s as node n folds its learned prefix (R3); read it
\* only under PrefixLearned(n, s).
RECURSIVE SegFold(_, _)
SegFold(n, s) ==
  IF s = 0 THEN 1
  ELSE LET k == SegFold(n, s - 1)
           v == learned[n][s - 1]
       IN  IF IsRoll(v) /\ RollTo[v] = k + 1 THEN k + 1 ELSE k

-----------------------------------------------------------------------------
(* Safety *)

\* One end for each segment: two nodes that learned a prefix fold the same
\* segment at every slot of it, so they see each `Roll` at the same slot.
SegsAgree == \A n, m \in Acceptors, s \in 0..NS :
               PrefixLearned(n, s) /\ PrefixLearned(m, s) => SegFold(n, s) = SegFold(m, s)

\* A control of R3: the fold of the votes that a node holds, not of what it
\* learned. A vote is not a choice, so two nodes can fold two segments.
PrefixVoted(n, s) == \A t \in Slots : t < s => vote[n][t].val # None

RECURSIVE VoteFold(_, _)
VoteFold(n, s) ==
  IF s = 0 THEN 1
  ELSE LET k == VoteFold(n, s - 1)
           v == vote[n][s - 1].val
       IN  IF IsRoll(v) /\ RollTo[v] = k + 1 THEN k + 1 ELSE k

VoteSegsAgree == \A n, m \in Acceptors, s \in 0..NS :
                   PrefixVoted(n, s) /\ PrefixVoted(m, s) => VoteFold(n, s) = VoteFold(m, s)

-----------------------------------------------------------------------------
(* Reach: each must be violated, so that the safe checks see the case.     *)

\* A node folds segment 2.
NoSeg2 == ~\E n \in Acceptors, s \in 0..NS : PrefixLearned(n, s) /\ SegFold(n, s) = 2

\* A `Roll` that ballot b1 proposed is proposed again by a later ballot,
\* from its Phase 1, and is learned.
NoRollRecovered ==
  ~\E s \in Slots, b1, b2 \in Ballots, v \in Commands :
     /\ IsRoll(v) /\ b1 < b2
     /\ [type |-> "2a", bal |-> b1, slot |-> s, val |-> v] \in msgs
     /\ [type |-> "2a", bal |-> b2, slot |-> s, val |-> v] \in msgs
     /\ \E n \in Acceptors : learned[n][s] = v

\* A `Roll` that a ballot proposed is lost: the slot is learned as another value.
NoRollLost ==
  ~\E s \in Slots, v \in Commands, n \in Acceptors :
     /\ IsRoll(v)
     /\ \E m \in msgs : m.type = "2a" /\ m.slot = s /\ m.val = v
     /\ learned[n][s] \notin {v, None}

=============================================================================
