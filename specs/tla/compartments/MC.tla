--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. The quorums    *)
(* come from `grid/Grid.tla`, as in `multi-paxos/` and `reads/`.            *)
EXTENDS Compartments, TLC, Grid

\* The model values that the configurations set.
CONSTANTS w1, w2, a1, a2, a3, r1, r2

\* Write w1 has one record and write w2 two, so a merged batch has three.
Count12 == [w \in Writes |-> IF w = w1 THEN 1 ELSE 2]

\* A majority of the acceptors: the Phase-2 quorums and the read quorums.
Majorities == MajorityQuorums(Acceptors)

\* A control: a read replica counts as a voter. A read closes on as many
\* answers as a majority of the acceptors, and the replica's answer is its
\* own folded prefix.
WithReplicas == {Q \in SUBSET (Acceptors \cup Replicas) :
                   2 * Cardinality(Q) > Cardinality(Acceptors)}
\* Every replica keeps the first segment.
AllFirst == [r \in Replicas |-> 0]
\* Replica r2 keeps the second segment, the others the first.
R2Second == [r \in Replicas |-> IF r = r2 THEN 1 ELSE 0]
=============================================================================
