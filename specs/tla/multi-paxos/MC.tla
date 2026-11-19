--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS MultiPaxos, TLC, Sequences

\* The acceptor names, model values that the configurations set.
CONSTANTS a1, a2, a3, a4, a5, a6

\* Every slot under one quorum set.
AllSlots(Qs) == [s \in Slots |-> Qs]

\* A majority of the acceptors.
Majorities == {Q \in SUBSET Acceptors : 2 * Cardinality(Q) > Cardinality(Acceptors)}
MajQ2 == AllSlots(Majorities)

\* An acceptor grid (`QuorumSystem::Grid`) over the acceptors laid out in
\* the order `Layout`, row-major in `rows x cols`: member i sits in column
\* i % cols. A Phase-2 quorum of slot s is `acks` acceptors of column
\* s % cols; a Phase-1 quorum is `rows - acks + 1` acceptors of every column.
Column(Layout, cols, c) == {Layout[i] : i \in {j \in 1..Len(Layout) : (j - 1) % cols = c}}
GridQ2(Layout, cols, acks) ==
  [s \in Slots |-> {Q \in SUBSET Column(Layout, cols, s % cols) : Cardinality(Q) >= acks}]
GridQ1(Layout, rows, cols, acks) ==
  {Q \in SUBSET Acceptors : \A c \in 0..(cols - 1) : Cardinality(Q \cap Column(Layout, cols, c)) >= rows - acks + 1}
\* A control: a Phase-1 quorum in one column only.
GridQ1OneColumn(Layout, rows, cols, acks) ==
  {Q \in SUBSET Acceptors : \E c \in 0..(cols - 1) : Cardinality(Q \cap Column(Layout, cols, c)) >= rows - acks + 1}

\* Three acceptors: the majority checks.
Own3 == [b \in Ballots |-> IF b % 3 = 1 THEN a1 ELSE IF b % 3 = 2 THEN a2 ELSE a3]

\* Four acceptors in a 2 x 2 grid: the full grid (acks = rows).
Layout4 == <<a1, a2, a3, a4>>
FullQ1 == GridQ1(Layout4, 2, 2, 2)
FullQ2 == GridQ2(Layout4, 2, 2)
FullQ1Bad == GridQ1OneColumn(Layout4, 2, 2, 2)

\* Six acceptors in a 3 x 2 grid: two partitions with F = 1 (acks = F + 1).
Layout6 == <<a1, a2, a3, a4, a5, a6>>
PartQ1 == GridQ1(Layout6, 3, 2, 2)
PartQ2 == GridQ2(Layout6, 2, 2)

Own2 == [b \in Ballots |-> IF b % 2 = 1 THEN a1 ELSE a2]

Symmetry == Permutations(Commands)
=============================================================================
