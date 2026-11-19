--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. The quorums    *)
(* come from `grid/Grid.tla`, as in `multi-paxos/`.                         *)
EXTENDS LeaderHandoff, TLC, Grid

\* The acceptor names, model values that the configurations set.
CONSTANTS a1, a2, a3

\* A majority of the acceptors, the same for every slot.
Majorities == MajorityQuorums(Acceptors)
MajQ2 == [s \in Slots |-> Majorities]

\* The owners of the ballots: a1 owns ballot 1, a2 ballot 2, a3 ballot 3.
Own3 == [b \in Ballots |-> IF b % 3 = 1 THEN a1 ELSE IF b % 3 = 2 THEN a2 ELSE a3]

Symmetry == Permutations(Commands)
=============================================================================
