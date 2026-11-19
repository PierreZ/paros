--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS Matchmaker, TLC

CONSTANTS a1, a2, a3, a4, a5

\* A move from A to B keeps a3 and replaces a1 and a2. A majority of the
\* union, {a3, a4, a5}, holds no Phase-1 quorum of A.
A == {a1, a2, a3}
B == {a3, a4, a5}
Pool == A \cup B
Move == {A, B}

\* The smallest move: one acceptor in each configuration. The GC checks
\* with two matchmakers and three ballots finish on it.
A1 == {a1}
B1 == {a2}
Pool1 == A1 \cup B1
Move1 == {A1, B1}

\* a1 and a2 have the same place in the move, and so have a4 and a5.
Symmetry ==
  Permutations(Values) \cup Permutations(Matchmakers)
    \cup Permutations({a1, a2}) \cup Permutations({a4, a5})
=============================================================================
