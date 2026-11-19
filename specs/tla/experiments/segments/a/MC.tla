--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write.               *)
EXTENDS SegmentsA, TLC

CONSTANTS a1, a2, a3, a4

\* One acceptor for each configuration: a vote is then a choice, and the
\* state space stays small enough for CI.
Cfg1 == [g \in 1..2 |-> IF g = 1 THEN {a1} ELSE {a2}]
\* Three acceptors for each configuration, two of them shared.
Cfg3 == [g \in 1..2 |-> IF g = 1 THEN {a1, a2, a3} ELSE {a2, a3, a4}]

Symmetry == Permutations(Values) \cup Permutations(Replicas)
=============================================================================
