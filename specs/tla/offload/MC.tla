--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write.               *)
EXTENDS Offload, TLC

CONSTANTS r1, r2, r3, o1, o2

\* A rolling upgrade: the two offloaders run two releases, so their bytes
\* differ in the header.
TwoReleases == (o1 :> 1) @@ (o2 :> 2)
=============================================================================
