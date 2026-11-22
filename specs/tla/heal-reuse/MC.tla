--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write.               *)
EXTENDS HealReuse, TLC

\* The members of the first set are alike, and the spares are alike. The
\* controllers are not: a copy id carries the order of the terms.
Sym == Permutations(Init0) \cup Permutations(Pool \ Init0)
=============================================================================
