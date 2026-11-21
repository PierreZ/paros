--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. The quorums of *)
(* a grid come from `grid/Grid.tla`, as in `multi-paxos/`.                  *)
EXTENDS ProxyLeader, TLC, Grid

\* The acceptor names, model values that the configurations set.
CONSTANTS a1, a2, a3, a4

\* Ballot b is owned by l1 when b is odd, by l2 when b is even.
CONSTANTS l1, l2
OwnTwo == [b \in Ballots |-> IF b % 2 = 1 THEN l1 ELSE l2]

\* The colocated deployment: ballot b is owned by acceptor a1 when b is odd,
\* by a2 when b is even.
OwnColocated == [b \in Ballots |-> IF b % 2 = 1 THEN a1 ELSE a2]

\* Every slot goes first to proxy p1.
CONSTANTS p1, p2
ToP1 == [s \in Slots |-> p1]

\* Majority quorums for every slot.
MajQ1(M) == MajorityQuorums(M)
MajQ2(M) == [s \in Slots |-> MajorityQuorums(M)]

\* Shrink: configuration 0 is {a1, a2, a3}, configuration 1 is {a1}. One
\* vote of a1 is a Phase-2 quorum of configuration 1 and not of 0, and a
\* Phase-1 quorum of 0 can miss it: what a proxy that counts over the wrong
\* configuration needs to commit a value that was not chosen.
ShrinkMembers == [k \in 0..1 |-> IF k = 0 THEN {a1, a2, a3} ELSE {a1}]
ShrinkQ1 == [k \in 0..1 |-> MajQ1(ShrinkMembers[k])]
ShrinkQ2 == [k \in 0..1 |-> MajQ2(ShrinkMembers[k])]
\* Ballot 2 runs configuration 1; ballots 1 and 3 run configuration 0.
ShrinkCfg == [b \in 0..NB |-> IF b = 2 THEN 1 ELSE 0]

\* Grid: configuration 0 is a majority of {a1, a2, a3}, configuration 1 is
\* the full 2 x 2 grid of a1..a4 (acks = rows: a whole column writes slot
\* s, column s % 2, and one acceptor of each column elects).
Layout4 == <<a1, a2, a3, a4>>
GridMembers2 == [k \in 0..1 |-> IF k = 0 THEN {a1, a2, a3} ELSE GridMembers(Layout4)]
GridQ1s == [k \in 0..1 |-> IF k = 0 THEN MajQ1({a1, a2, a3}) ELSE GridQ1(Layout4, 2, 2, 0)]
GridQ2s == [k \in 0..1 |-> IF k = 0 THEN MajQ2({a1, a2, a3})
                           ELSE [s \in Slots |-> GridQ2At(Layout4, 2, 2, s % 2)]]

Symmetry == Permutations(Commands)
\* The grid layout tiles its members (`QuorumSystem::admits`).
ASSUME GridAdmits(Layout4, 2, 2)

=============================================================================
