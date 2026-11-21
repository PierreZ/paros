--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write.               *)
EXTENDS SegmentsB, TLC

CONSTANTS a1, a2, a3, a4

\* The same configurations as design A.
Cfg1 == [g \in 1..2 |-> IF g = 1 THEN {a1} ELSE {a2}]
Cfg3 == [g \in 1..2 |-> IF g = 1 THEN {a1, a2, a3} ELSE {a2, a3, a4}]
\* Three configurations of one acceptor each, as in design A.
CfgChain == [g \in 1..3 |-> CASE g = 1 -> {a1} [] g = 2 -> {a2} [] OTHER -> {a3}]

Symmetry == Permutations(Values) \cup Permutations(Replicas)
=============================================================================
