--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS StaleStopped, TLC

Majorities == {Q \in SUBSET Acceptors : 2 * Cardinality(Q) > Cardinality(Acceptors)}

Symmetry == Permutations(Values) \cup Permutations(Acceptors)
=============================================================================
