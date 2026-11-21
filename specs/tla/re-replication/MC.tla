--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write.               *)
EXTENDS ReReplication, TLC

\* The members of the first set are alike, the spares are alike, and the
\* controllers are alike (safety checks and `Ends`, an invariant).
Sym == Permutations(Init0) \cup Permutations(Pool \ Init0) \cup Permutations(Ctls)
=============================================================================
