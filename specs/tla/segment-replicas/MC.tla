--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write.               *)
EXTENDS SegmentReplicas, TLC

CONSTANTS r1, r2, r3, r4, r5, r6, u1, u2

\* A write of one record under each uuid, and the compare-and-set that
\* moves the leader from u1 to u2.
Values == {[op |-> "w", u |-> u1, n |-> 1],
           [op |-> "w", u |-> u2, n |-> 1],
           [op |-> "sl", new |-> u2, old |-> u1]}

\* The liveness checks need no fold: one value keeps the state space small.
OneValue == {[op |-> "w", u |-> u1, n |-> 1]}
=============================================================================
