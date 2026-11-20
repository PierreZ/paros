--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS Node, Grid, TLC

CONSTANTS a1, a2, a3, a4, a5, m1, m2, m3, c1, c2, r1, x1

\* Three acceptors, the configuration of plain Multi-Paxos.
Three == {a1, a2, a3}

\* A move from A to B keeps a3 and replaces a1 and a2: a majority of the
\* union holds no Phase-1 quorum of A.
A == {a1, a2, a3}
B == {a3, a4, a5}
Pool == A \cup B
Move == {A, B}

\* The smallest move: one acceptor in each configuration.
A1 == {a1}
B1 == {a2}
Pool1 == A1 \cup B1
Move1 == {A1, B1}

\* The quorums of a configuration: its majorities.
MajQ(c) == Maj(c)

\* Four acceptors in a 2 x 2 grid (the full grid, acks = rows).
Layout4 == <<a1, a2, a3, a4>>
Four == GridMembers(Layout4)
GridP1(c) == GridQ1(Layout4, 2, 2, 0)
GridP2(c) == GridQ2(Layout4, 2, 2)
ASSUME GridAdmits(Layout4, 2, 2)

\* The owners of the ballots: they take turns.
Own3 == [b \in Ballots |-> IF b % 3 = 1 THEN a1 ELSE IF b % 3 = 2 THEN a2 ELSE a3]
Own2 == [b \in Ballots |-> IF b % 2 = 1 THEN a1 ELSE a2]
\* Node a3 owns every ballot, on the move from A to B.
OwnMove == [b \in Ballots |-> IF b = 1 THEN a1 ELSE a3]

Symmetry == Permutations(Commands)
=============================================================================
