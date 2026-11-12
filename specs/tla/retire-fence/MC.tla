--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS Retire, TLC

\* A shrink from three members to two. With q1 = n, the old configuration
\* needs each of its members, thus it needs the node that leaves.
Shrink == {{"n0", "n1", "n2"}, {"n0", "n1"}}
Boot == {"n0", "n1", "n2"}

Symmetry == Permutations(Matchmakers)
=============================================================================
