--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS Decree, TLC

Maj(S) == {Q \in SUBSET S : 2 * Cardinality(Q) > Cardinality(S)}
Majorities == Maj(Acceptors)

Symmetry == Permutations(Values) \cup Permutations(Acceptors)
=============================================================================
