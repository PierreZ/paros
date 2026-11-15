--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS Abandon, TLC

\* G: a 2x2 grid over a1..a4. Its Phase-1 quorums are the rows, its Phase-2
\* quorums the columns. M: a majority of a2..a4.
GridMaj == {"G", "M"}
MemGM == [c \in GridMaj |-> IF c = "G" THEN {"a1", "a2", "a3", "a4"} ELSE {"a2", "a3", "a4"}]
Maj(S) == {Q \in SUBSET S : 2 * Cardinality(Q) > Cardinality(S)}
Q1GM == [c \in GridMaj |-> IF c = "G" THEN {{"a1", "a2"}, {"a3", "a4"}} ELSE Maj(MemGM["M"])]
Q2GM == [c \in GridMaj |-> IF c = "G" THEN {{"a1", "a3"}, {"a2", "a4"}} ELSE Maj(MemGM["M"])]

Owner3 == <<"a1", "a2", "a3">>

Symmetry == Permutations(Values) \cup Permutations(Matchmakers)
=============================================================================
