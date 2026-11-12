--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS Move, TLC

Symmetry == Permutations(Journals)
=============================================================================
