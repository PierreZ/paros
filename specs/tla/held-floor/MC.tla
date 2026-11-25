--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write.               *)
EXTENDS HeldFloor, TLC

\* The acceptor and replica names, the truncation value and the `Roll`
\* value: model values that the configurations set.
CONSTANTS a1, a2, a3, r1, r2, roll2, roll3

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

\* One `Roll`, `roll2`, which opens segment 2; or two, `roll2` and `roll3`.
Roll2 == [v \in {roll2} |-> 2]
Roll23 == [v \in {roll2, roll3} |-> IF v = roll2 THEN 2 ELSE 3]
\* Segment 1 has the replicas r1 and r2, or r1 alone; the last segment has
\* none, so it is never held. With two `Roll`s, segment 2 keeps the replica
\* of segment 1 (D4: a `Roll` with no replicas keeps the same machines).
Reps12 == [k \in Segs |-> IF k = 1 THEN {r1, r2} ELSE {}]
Reps1 == [k \in Segs |-> IF k < Cardinality(Segs) THEN {r1} ELSE {}]

\* The script of a check, a state constraint on the values that a leader
\* allocates. The gap fill of a new leader still writes a `Noop` at a hole.
\* `RollPlan`: the command at slot 0, the `Roll` at slot 1.
\* `RollPlan3`: the command, `roll2`, then `roll3` (segment 2 is `roll3`).
\* `CmdPlan3`: the command, `roll2`, then the command again, in segment 2.
Planned(script) == \A m \in msgs : m.type = "2a" /\ m.val \in Proposals => m.val \in script[m.slot]
RollPlan == Planned([s \in Slots |-> IF s = 0 THEN Commands ELSE Rolls])
RollPlan3 == Planned([s \in Slots |-> IF s = 0 THEN Commands ELSE IF s = 1 THEN {roll2} ELSE {roll3}])
CmdPlan3 == Planned([s \in Slots |-> IF s = 1 THEN Rolls ELSE Commands])

\* The two replicas of segment 1 are alike.
ReplicaPerms == Permutations({r1, r2})
=============================================================================
