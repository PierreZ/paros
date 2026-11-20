---- MODULE MC ----
\* The constants that a TLC configuration file cannot write.
EXTENDS FlightRule, TLC

MCValueOf == (1 :> "v1") @@ (2 :> "v2") @@ (3 :> "v1")
MCQuorums == {Q \in SUBSET Acceptors : 2 * Cardinality(Q) > Cardinality(Acceptors)}
Symmetry == Permutations(Acceptors)
====
