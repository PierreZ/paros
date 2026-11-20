--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS ConfigRegister, Grid, TLC

Maj(S) == {Q \in SUBSET S : 2 * Cardinality(Q) > Cardinality(S)}

\* A over a1..a3 and B over a2..a4: a move that replaces a1 by a4.
AB == {"A", "B"}
MemAB == [c \in AB |-> IF c = "A" THEN {"a1", "a2", "a3"} ELSE {"a2", "a3", "a4"}]
QAB == [c \in AB |-> Maj(MemAB[c])]

\* A resize: A over a1..a3 and R over a1..a5 (a successor of another size).
AR == {"A", "R"}
MemAR == [c \in AR |-> IF c = "A" THEN {"a1", "a2", "a3"} ELSE {"a1", "a2", "a3", "a4", "a5"}]
QAR == [c \in AR |-> Maj(MemAR[c])]

\* A partitioned layout (decision 15): two partitions of three. Phase 1
\* takes a majority of every partition; the one slot's Phase 2 goes to the
\* first partition. The move replaces a3 by a7 in the first partition.
PA == {"P", "Q"}
ColsPQ == [c \in PA |-> IF c = "P" THEN <<{"a1", "a2", "a3"}, {"a4", "a5", "a6"}>>
                                    ELSE <<{"a1", "a2", "a7"}, {"a4", "a5", "a6"}>>]
MemPQ == [c \in PA |-> ColsPQ[c][1] \cup ColsPQ[c][2]]
Q1PQ == [c \in PA |-> {Q \in SUBSET MemPQ[c] :
                         \A i \in 1..2 : 2 * Cardinality(Q \cap ColsPQ[c][i]) > 3}]
Q2PQ == [c \in PA |-> Maj(ColsPQ[c][1])]

\* A full grid of 2 x 2 (`QuorumSystem::grid(2, 2)`, the full grid of
\* Compartmentalized Paxos): G over a1..a4, then H, where a5 replaces a1.
\* The layout is row-major over the sorted members, so the move also moves
\* a2, a3 and a4 to other cells: G has the columns {a1, a3} and {a2, a4}, H
\* has {a2, a4} and {a3, a5}. Phase 2 takes a whole column, any column;
\* Phase 1 one acceptor of every column.
GH == {"G", "H"}
SeqGH == [c \in GH |-> IF c = "G" THEN <<"a1", "a2", "a3", "a4">> ELSE <<"a2", "a3", "a4", "a5">>]
MemGH == [c \in GH |-> GridMembers(SeqGH[c])]
Q1GH == [c \in GH |-> GridQ1(SeqGH[c], 2, 2, 0)]
Q2GH == [c \in GH |-> GridQ2(SeqGH[c], 2, 2)]

\* The control of the grid: Phase 2 takes one acceptor of a column
\* (acks = 1), and Phase 1 still one acceptor of each column, where the
\* grid needs two (`grid_slack` of `specs/tla/grid/`).
Q1GHSlack == [c \in GH |-> GridQ1(SeqGH[c], 2, 1, 1)]
Q2GHOne == [c \in GH |-> GridQ2(SeqGH[c], 2, 1)]

\* The journal grid, rows 2F + 1 = 3, cols 2, acks F + 1 = 2 (#1258 (reshape
\* with a column below quorum), T5): J over a1..a6, then K, a heal where a7
\* replaces a1. The layout is row-major over the sorted members, so every
\* member that stays changes its column: J has the columns {a1, a3, a5} and
\* {a2, a4, a6}, K has {a2, a4, a6} and {a3, a5, a7}. Phase 2 takes two of
\* one column, any column; Phase 1 two of every column.
JK == {"J", "K"}
SeqJK == [c \in JK |-> IF c = "J" THEN <<"a1", "a2", "a3", "a4", "a5", "a6">>
                                 ELSE <<"a2", "a3", "a4", "a5", "a6", "a7">>]
MemJK == [c \in JK |-> GridMembers(SeqJK[c])]
Q1JK == [c \in JK |-> GridQ1(SeqJK[c], 2, 2, 0)]
Q2JK == [c \in JK |-> GridQ2(SeqJK[c], 2, 2)]
\* The control: Phase 1 one acceptor short in every column.
Q1JKShort == [c \in JK |-> GridQ1(SeqJK[c], 2, 2, 1)]

\* Matchmaker hosts.
HostOne == [m \in {"m1"} |-> "a1"]
HostThree == [m \in {"m1", "m2", "m3"} |-> IF m = "m1" THEN "a1" ELSE IF m = "m2" THEN "a2" ELSE "a3"]

Symmetry == Permutations(Values)
\* Each grid layout tiles its members (`QuorumSystem::admits`).
ASSUME \A c \in GH : GridAdmits(SeqGH[c], 2, 2) /\ GridAdmits(SeqGH[c], 2, 1)
ASSUME \A c \in JK : GridAdmits(SeqJK[c], 2, 2)

=============================================================================
