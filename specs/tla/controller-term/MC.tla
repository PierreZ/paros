--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS ControllerTerm, TLC

CONSTANTS a1, a2, a3, a4, a5

\* The move of `../matchmaker/MC.tla`: A to B keeps a3.
A == {a1, a2, a3}
B == {a3, a4, a5}
Pool == A \cup B
Move == {A, B}

\* The smallest move: one acceptor in each configuration.
A1 == {a1}
B1 == {a2}
Pool1 == A1 \cup B1
Move1 == {A1, B1}

Symmetry ==
  Permutations(Values) \cup Permutations(Matchmakers)
    \cup Permutations({a1, a2}) \cup Permutations({a4, a5})
SymmetrySmall == Permutations(Values) \cup Permutations(Matchmakers)
=============================================================================
