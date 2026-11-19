--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write.               *)
EXTENDS Truncation, TLC

\* The acceptor names and the truncation values, model values that the
\* configurations set.
CONSTANTS a1, a2, a3, trunc, cut0

\* A majority of the acceptors, the same for every slot.
Majorities == {Q \in SUBSET Acceptors : 2 * Cardinality(Q) > Cardinality(Acceptors)}
MajQ2 == [s \in Slots |-> Majorities]

\* The owners of the ballots: a1, a2, a3, a1, ...
Own3 == [b \in Ballots |-> IF b % 3 = 1 THEN a1 ELSE IF b % 3 = 2 THEN a2 ELSE a3]

\* The owners for the retry of a laggard: a1 leads first, a3 leads last.
OwnLag == [b \in Ballots |-> IF b = 1 THEN a1 ELSE a3]

\* The positions of the truncations: `trunc` releases every record before
\* its slot, `cut0` truncates up to position 0 and so keeps every record.
CutAll == [c \in {trunc} |-> NS]
CutZero == [c \in {cut0} |-> 0]
CutBoth == [c \in {trunc, cut0} |-> IF c = trunc THEN NS ELSE 0]
=============================================================================
