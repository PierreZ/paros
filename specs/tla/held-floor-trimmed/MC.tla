--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write, over          *)
(* `HeldTrim.tla`: the definitions of `held-floor/MC.tla` that the checks   *)
(* of this folder use.                                                     *)
EXTENDS HeldTrim, TLC

CONSTANTS a1, a2, a3, r1, r2, r3, roll2, roll3, hold1, w1, c1, c2

\* A majority of the acceptors, the same for every slot.
Majorities == {Q \in SUBSET Acceptors : 2 * Cardinality(Q) > Cardinality(Acceptors)}
MajQ2 == [s \in Slots |-> Majorities]

\* One leader, a1.
OwnOne == [b \in Ballots |-> a1]

\* No truncation value, or one that releases the first write.
NoCut == [c \in {} |-> 0]
Cut1 == [c \in Cuts |-> 1]

\* No `Hold` value, or one, `hold1`, the floor at the held segment 1.
NoHold == [h \in {} |-> 1]
Hold1 == [h \in {hold1} |-> 1]

\* Each retry retries the one command of the check.
RetryOne == [v \in Retries |-> CHOOSE c \in Commands : TRUE]

\* One `Roll`, `roll2`, which opens segment 2.
Roll2 == [v \in {roll2} |-> 2]

\* Segment 1 has the replicas r1 and r2; segment 2 has none.
Reps12 == [k \in Segs |-> IF k = 1 THEN {r1, r2} ELSE {}]

\* The script of a check, a state constraint on the values that a leader
\* allocates.
Planned(script) == \A m \in msgs : m.type = "2a" /\ m.val \in Proposals => m.val \in script[m.slot]
\* `CutHoldPlan`: the command, a `Truncate` of it, `roll2`, then `hold1`:
\* segment 1 is released, and its `Hold` follows.
CutHoldPlan == Planned([s \in Slots |-> IF s = 0 THEN Commands ELSE IF s = 1 THEN Cuts
                                         ELSE IF s = 2 THEN Rolls ELSE HoldCmds])
\* `HoldPlan`: the command, `roll2`, then `hold1`: segment 1 is held by
\* its replicas, and no `Truncate` released it.
HoldPlan == Planned([s \in Slots |-> IF s = 0 THEN Commands ELSE IF s = 1 THEN Rolls ELSE HoldCmds])
\* `LatePlan`: `c1`, `roll2`, `c2` in segment 2, then a `Truncate` of the
\* first write: the floor is the slot of `c2`, and the state sealed there
\* predates the `Truncate` (only the head `first_seq` proves segment 1
\* released).
LatePlan == Planned([s \in Slots |-> IF s = 0 THEN {c1} ELSE IF s = 1 THEN Rolls
                                      ELSE IF s = 2 THEN {c2} ELSE Cuts])
\* `a3` votes for no slot: the votes of `a1` and `a2` choose every slot.
QuietA3 == ~\E m \in msgs : m.type = "2b" /\ m.acc = a3

\* The two replicas of segment 1 are alike.
ReplicaPerms == Permutations({r1, r2})
=============================================================================
