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

Symmetry == Permutations({m1, m2, m3})
=============================================================================
