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

\* The replicas of each set are alike (safety checks only).
Sym == Permutations(R1) \cup Permutations(R2)
=============================================================================
