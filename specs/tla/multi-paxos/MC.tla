--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. The quorums of *)
(* a grid come from `grid/Grid.tla`, as in `config-register/` and `reads/`. *)
EXTENDS MultiPaxos, TLC, Grid

\* The acceptor names, model values that the configurations set.
CONSTANTS a1, a2, a3, a4, a5, a6

\* A majority of the acceptors, the same for every slot.
Majorities == MajorityQuorums(Acceptors)
MajQ2 == [s \in Slots |-> Majorities]

\* A slot's Phase-2 quorums on a grid: `acks` acceptors of column s % cols
\* (`column_of`).
SlotQ2(M, cols, acks) == [s \in Slots |-> GridQ2At(M, cols, acks, s % cols)]

\* A slot's Phase-2 quorums when the round may move: `acks` acceptors of
\* any one column (`move_round`, `propose_in`; the decision is judged in the
\* round's current column, `proposer/rounds.rs`). It holds the home column.
AnyQ2(M, cols, acks) == [s \in Slots |-> GridQ2(M, cols, acks)]

\* A control: a Phase-1 quorum is `rows - acks + 1` acceptors of one column
\* only, not of every column.
GridQ1OneColumn(M, cols, acks) ==
  {Q \in SUBSET GridMembers(M) :
     \E j \in 0..(cols - 1) :
       Cardinality(Q \cap GridColumn(M, cols, j)) >= GridPhase1Count(M, cols, acks, 0)}

\* The owners of ballots 1 and 2.
Own3 == [b \in Ballots |-> IF b % 3 = 1 THEN a1 ELSE IF b % 3 = 2 THEN a2 ELSE a3]

\* Four acceptors in a 2 x 2 grid: the full grid (acks = rows).
Layout4 == <<a1, a2, a3, a4>>
FullQ1 == GridQ1(Layout4, 2, 2, 0)
FullQ2 == SlotQ2(Layout4, 2, 2)
FullQ2Any == AnyQ2(Layout4, 2, 2)
FullQ1Bad == GridQ1OneColumn(Layout4, 2, 2)

\* Six acceptors in a 3 x 2 grid: two partitions with F = 1 (acks = F + 1).
Layout6 == <<a1, a2, a3, a4, a5, a6>>
PartQ1 == GridQ1(Layout6, 2, 2, 0)
PartQ2 == SlotQ2(Layout6, 2, 2)
PartQ2Any == AnyQ2(Layout6, 2, 2)

\* Every acceptor may restart (safety checks).
AllAcceptors == Acceptors

\* A liveness check: every acceptor but the owner of the last ballot.
NotLastOwner == Acceptors \ {Own3[NB]}

\* Node a1 owns ballots 1 and 3: one node leads two times, and its second
\* campaign reads its own durable log after a restart.
OwnTwice == [b \in Ballots |-> IF b % 2 = 1 THEN a1 ELSE a2]

Symmetry == Permutations(Commands)
\* Each grid layout tiles its members (`QuorumSystem::admits`).
ASSUME GridAdmits(Layout4, 2, 2) /\ GridAdmits(Layout6, 2, 2)

=============================================================================
