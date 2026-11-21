--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS Generations, TLC

CONSTANTS m1, m2, m3

\* Generation 0 is {m1, m2}; a handover proposes any set of the pool.
Pair == {m1, m2}
Pool == {m1, m2, m3}
AnySet == SUBSET Pool \ {{}}
\* The sets of a smaller check: the bootstrap pair, a pair with m3, and m3.
Few == {{m1, m2}, {m2, m3}, {m3}}
\* Generation 0 is the whole pool, so a majority leaves one member out.
Three == {{m1, m2, m3}, {m2, m3}, {m3}}

\* The sets of the pass check: the pair and m3, so m1 and m2 skip
\* generation 1 when they come back in generation 2.
Two == {{m1, m2}, {m3}}

\* m1 and m2 play the same part in `Pair` and `Two`.
Symmetry == Permutations({m1, m2})
=============================================================================
