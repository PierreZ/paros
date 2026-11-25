--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS SegmentRoll, TLC

\* Two replicas in segment 1, one in each later segment: OneEnd needs two
\* replicas of the segment that the first Roll ends.
Reps2 == [k \in Seg |-> IF k = 1 THEN 2 ELSE 1]

\* No replica: the routing checks read the votes and the proposals alone.
Reps0 == [k \in Seg |-> 0]

\* One replica of segment 1, to publish the chain.
Reps1 == [k \in Seg |-> IF k = 1 THEN 1 ELSE 0]

Symmetry == Permutations(Acceptors)
=============================================================================
