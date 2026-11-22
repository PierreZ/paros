--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS Register, TLC

\* A over a1..a3 and B over a2..a4, majorities.
AB == {"A", "B"}
MemAB == [c \in AB |-> IF c = "A" THEN {"a1", "a2", "a3"} ELSE {"a2", "a3", "a4"}]
Maj(S) == {Q \in SUBSET S : 2 * Cardinality(Q) > Cardinality(S)}
QAB == [c \in AB |-> Maj(MemAB[c])]

\* A alone: the register never changes the configuration.
OnlyA == {"A"}

Symmetry == Permutations(Values) \cup Permutations(Matchmakers)
=============================================================================
