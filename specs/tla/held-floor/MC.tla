--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write.               *)
EXTENDS HeldFloor, TLC

\* The acceptor and replica names, the truncation value and the `Roll`
\* value: model values that the configurations set.
CONSTANTS a1, a2, a3, r1, r2, roll2

\* A majority of the acceptors, the same for every slot.
Majorities == {Q \in SUBSET Acceptors : 2 * Cardinality(Q) > Cardinality(Acceptors)}
MajQ2 == [s \in Slots |-> Majorities]

\* The owners of the ballots: a1, a2, a3, a1, ...
Own3 == [b \in Ballots |-> IF b % 3 = 1 THEN a1 ELSE IF b % 3 = 2 THEN a2 ELSE a3]
\* The owners for the retry of a laggard: a1 leads first, a3 leads last.
OwnLag == [b \in Ballots |-> IF b = 1 THEN a1 ELSE a3]
\* One leader, a1.
OwnOne == [b \in Ballots |-> a1]

\* No truncation value.
NoCut == [c \in {} |-> 0]

\* One `Roll`, `roll2`, which opens segment 2.
Roll2 == [v \in {roll2} |-> 2]
\* Segment 1 has the replicas r1 and r2, or r1 alone; segment 2 has none,
\* so it is never held.
Reps12 == [k \in Segs |-> IF k = 1 THEN {r1, r2} ELSE {}]
Reps1 == [k \in Segs |-> IF k = 1 THEN {r1} ELSE {}]

\* The script of every check, a state constraint: a leader allocates the
\* command at slot 0 and the `Roll` at slot 1. The gap fill of a new leader
\* still writes a `Noop` at a hole.
RollPlan == \A m \in msgs : m.type = "2a" /\ m.val \in Proposals =>
              m.val \in (IF m.slot = 0 THEN Commands ELSE Rolls)

\* The two replicas of segment 1 are alike.
ReplicaPerms == Permutations({r1, r2})
=============================================================================
