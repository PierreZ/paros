--------------------------------- MODULE MC ---------------------------------
(* The model checked by the configurations of this folder. The actors are *)
(* symmetric in the safety checks.                                         *)
EXTENDS Election, TLC

Perms == Permutations(Actors)
=============================================================================
