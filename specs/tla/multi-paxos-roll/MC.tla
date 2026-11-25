--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write.               *)
EXTENDS MultiPaxosRoll, TLC, Grid

CONSTANTS a1, a2, a3, c1, r2, r3

Majorities == MajorityQuorums(Acceptors)
MajQ2 == [s \in Slots |-> Majorities]

\* The owners of the ballots: a1, a2, a3 in turn.
Own3 == [b \in Ballots |-> IF b % 3 = 1 THEN a1 ELSE IF b % 3 = 2 THEN a2 ELSE a3]

AllAcceptors == Acceptors

\* c1 is a client command, r2 a Roll to segment 2, r3 a Roll to segment 3.
Rolls == [c \in Commands |-> IF c = r2 THEN 2 ELSE IF c = r3 THEN 3 ELSE 0]
=============================================================================
