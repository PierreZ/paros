--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write.               *)
EXTENDS SegmentTruncate, TLC

CONSTANTS r1, r2, r3, r4, r5, r6

W == [op |-> "w"]
T(n) == [op |-> "t", up |-> n]

\* A write of one record, and a truncate to `seq` 1 or 2.
Values == {W, T(1), T(2)}
\* A truncate to `seq` 1 only: the floor can land inside S1.
Inside == {W, T(1)}

\* A cut of `Values` for the long checks (#1321): at most two writes, one
\* `Truncate(1)` and one `Truncate(2)`. The budget is NS = 4 values, so a run
\* still fills every slot, and a state with no enabled step is a true end.
Uses(v) == Cardinality({s \in Slots : chosen[s] = v})
Budget == Uses(W) <= 2 /\ Uses(T(1)) <= 1 /\ Uses(T(2)) <= 1
ASSUME NS <= 4

\* The replicas of each set are alike (safety checks only).
Sym == Permutations(R1) \cup Permutations(R2)
=============================================================================
