--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. The quorums    *)
(* come from `grid/Grid.tla`, as in `multi-paxos/`.                         *)
EXTENDS LeaderHandoff, TLC, Grid

\* The acceptor names, model values that the configurations set.
CONSTANTS a1, a2, a3, a4

\* A majority of the acceptors, the same for every slot.
Majorities == MajorityQuorums(Acceptors)
MajQ2 == [s \in Slots |-> Majorities]

\* The owners of the ballots: a1 owns ballot 1, a2 ballot 2, a3 ballot 3.
Own3 == [b \in Ballots |-> IF b % 3 = 1 THEN a1 ELSE IF b % 3 = 2 THEN a2 ELSE a3]

\* A 2 x 2 grid with two acks per column: slot s uses column s % 2 for
\* Phase 2, as `membership/quorum_system.rs` stripes the slots.
Layout4 == <<a1, a2, a3, a4>>
FullQ1 == GridQ1(Layout4, 2, 2, 0)
FullQ2 == [s \in Slots |-> GridQ2At(Layout4, 2, 2, s % 2)]
ASSUME GridAdmits(Layout4, 2, 2)

\* The owners on four acceptors: a1 owns ballot 1, a2 ballot 2, and so on.
Own4 == [b \in Ballots |-> Layout4[((b - 1) % 4) + 1]]

\* One node owns every ballot: the predecessor campaigns again after it
\* hands its ballot on.
OneOwner == [b \in Ballots |-> a1]

Symmetry == Permutations(Commands)
=============================================================================
