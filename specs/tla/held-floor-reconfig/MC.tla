--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write.               *)
EXTENDS HeldFloorReconfig, TLC

\* The acceptor and replica names and the `Roll` value: model values that
\* the configurations set.
CONSTANTS a1, a2, a3, a4, r1, r2, roll2

\* The owners of the ballots: a1 leads under C1, a3 opens the
\* reconfiguration.
Own == [b \in Ballots |-> IF b = 1 THEN a1 ELSE a3]

\* One `Roll`, `roll2`, which opens segment 2.
Roll2 == [v \in {roll2} |-> 2]
\* Segment 1 has the replicas r1 and r2, or r1 alone; segment 2 has none,
\* so it is never held.
Reps12 == [k \in Segs |-> IF k = 1 THEN {r1, r2} ELSE {}]
Reps1 == [k \in Segs |-> IF k = 1 THEN {r1} ELSE {}]

\* The script of the checks, a state constraint: a leader allocates the
\* command at slot 0 and the `Roll` at slot 1 (`RollPlan`), or, with one
\* slot, the `Roll` at slot 0 (`RollPlan1`: segment 1 is the `Roll` alone).
\* The gap fill still writes a `Noop` at a hole.
RollPlan == \A m \in msgs : m.type = "2a" /\ m.val \in Proposals =>
              m.val \in (IF m.slot = 0 THEN Commands ELSE Rolls)
RollPlan1 == \A m \in msgs : m.type = "2a" /\ m.val \in Proposals => m.val \in Rolls

\* The two replicas of segment 1 are alike.
ReplicaPerms == Permutations({r1, r2})
=============================================================================
