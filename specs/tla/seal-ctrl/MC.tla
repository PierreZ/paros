--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write.               *)
EXTENDS SealCtrl, TLC

CONSTANTS a1, a2, a3, c1, c2, c3, v1, v2

\* The old grid: one column of three acceptors (the default grid, F = 1).
\* The next configuration: one acceptor, so that a vote there is a choice
\* and the state space stays small enough for CI.
Cfg1 == [c \in {1, 2} |-> IF c = 1 THEN {a1, a2, a3} ELSE {c1}]
\* The next configuration with three acceptors too.
Cfg3 == [c \in {1, 2} |-> IF c = 1 THEN {a1, a2, a3} ELSE {c1, c2, c3}]
\* Ballot 1 is the old grid's; every later ballot is the next one's.
GenOf == [b \in 1..NB |-> IF b = 1 THEN 1 ELSE 2]
\* The old leader writes v1; every leader of the next configuration writes v2.
CmdOf == [b \in 1..NB |-> IF b = 1 THEN v1 ELSE v2]
\* The acceptors of each configuration play the same part.
Sym == Permutations({a1, a2, a3}) \cup Permutations({c1, c2, c3})
=============================================================================
