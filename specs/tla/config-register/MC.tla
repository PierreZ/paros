--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS ConfigRegister, TLC

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

\* Matchmaker hosts.
HostOne == [m \in {"m1"} |-> "a1"]
HostThree == [m \in {"m1", "m2", "m3"} |-> IF m = "m1" THEN "a1" ELSE IF m = "m2" THEN "a2" ELSE "a3"]

Symmetry == Permutations(Values)
=============================================================================
