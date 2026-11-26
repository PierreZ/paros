--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write.               *)
EXTENDS HeldFloor, TLC

\* The acceptor and replica names, the truncation value and the `Roll`
\* value, the `Hold` value, the retry value and the commands: model values that the
\* configurations set.
CONSTANTS a1, a2, a3, r1, r2, r3, roll2, roll3, hold1, w1, c1, c2

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

\* Each truncation value releases the first write (`up_to` = 1).
Cut1 == [c \in Cuts |-> 1]

\* No `Hold` value, or one, `hold1`, the floor at the held segment 1.
NoHold == [h \in {} |-> 1]
Hold1 == [h \in {hold1} |-> 1]

\* Each retry, `w1`, retries the one command of the check; or `c2`, the
\* second command.
RetryOne == [v \in Retries |-> CHOOSE c \in Commands : TRUE]
RetryC2 == [v \in Retries |-> c2]
RetryC1 == [v \in Retries |-> c1]

\* No `Roll`: one segment.
NoRoll == [v \in {} |-> 2]
\* One `Roll`, `roll2`, which opens segment 2; or two, `roll2` and `roll3`.
Roll2 == [v \in {roll2} |-> 2]
Roll23 == [v \in {roll2, roll3} |-> IF v = roll2 THEN 2 ELSE 3]
\* Segment 1 has the replicas r1 and r2, or r1 alone; the last segment has
\* none, so it is never held. With two `Roll`s, segment 2 keeps the replica
\* of segment 1 (D4: a `Roll` with no replicas keeps the same machines).
Reps12 == [k \in Segs |-> IF k = 1 THEN {r1, r2} ELSE {}]
Reps1 == [k \in Segs |-> IF k < Cardinality(Segs) THEN {r1} ELSE {}]
\* Segment 1 has three replicas, so a replica that reported it and lost its
\* disk can still be counted while two others hold it.
Reps123 == [k \in Segs |-> IF k = 1 THEN {r1, r2, r3} ELSE {}]

\* The script of a check, a state constraint on the values that a leader
\* allocates. The gap fill of a new leader still writes a `Noop` at a hole.
\* `RollPlan`: the command at slot 0, the `Roll` at slot 1.
\* `RollPlan3`: the command, `roll2`, then `roll3` (segment 2 is `roll3`).
\* `CmdPlan3`: the command, `roll2`, then the command again, in segment 2.
Planned(script) == \A m \in msgs : m.type = "2a" /\ m.val \in Proposals => m.val \in script[m.slot]
RollPlan == Planned([s \in Slots |-> IF s = 0 THEN Commands ELSE Rolls])
RollPlan3 == Planned([s \in Slots |-> IF s = 0 THEN Commands ELSE IF s = 1 THEN {roll2} ELSE {roll3}])
\* `HoldPlan`: the command, `roll2`, then `hold1`.
HoldPlan == Planned([s \in Slots |-> IF s = 0 THEN Commands ELSE IF s = 1 THEN Rolls ELSE HoldCmds])
\* `RetryPlan`: the command, `roll2`, a retry of the command in segment 2,
\* then `hold1`.
RetryPlan == Planned([s \in Slots |-> IF s = 0 THEN Commands ELSE IF s = 1 THEN Rolls
                                       ELSE IF s = 2 THEN Retries ELSE HoldCmds])
\* `InnerPlan`: the command, a retry of it, `roll2`, then `hold1`: the
\* retry is inside segment 1.
InnerPlan == Planned([s \in Slots |-> IF s = 0 THEN Commands ELSE IF s = 1 THEN Retries
                                       ELSE IF s = 2 THEN Rolls ELSE HoldCmds])
\* `KeepPlan`: `c1`, `c2`, `roll2`, a retry of `c2` in segment 2, then
\* `hold1`: the drop keeps slot 1 and drops slot 0.
KeepPlan == Planned([s \in Slots |-> IF s = 0 THEN {c1} ELSE IF s = 1 THEN {c2}
                                      ELSE IF s = 2 THEN Rolls ELSE IF s = 3 THEN Retries
                                      ELSE HoldCmds])
\* `CompactPlan`: `c1`, `c2`, a retry of `c1`, then a `Truncate` that
\* releases `c1`: the target of the compaction is slot 1, below the
\* write that the retry at slot 2 reads.
CompactPlan == Planned([s \in Slots |-> IF s = 0 THEN {c1} ELSE IF s = 1 THEN {c2}
                                         ELSE IF s = 2 THEN Retries ELSE Cuts])
\* `a3` votes for no slot: the votes of `a1` and `a2` choose every slot.
\* A scenario constraint that keeps the five slots of `KeepPlan` small.
QuietA3 == ~\E m \in msgs : m.type = "2b" /\ m.acc = a3
CmdPlan3 == Planned([s \in Slots |-> IF s = 1 THEN Rolls ELSE Commands])

\* The two replicas of segment 1 are alike.
ReplicaPerms == Permutations({r1, r2})
\* The three replicas of segment 1 are alike.
ReplicaPerms3 == Permutations({r1, r2, r3})
=============================================================================
