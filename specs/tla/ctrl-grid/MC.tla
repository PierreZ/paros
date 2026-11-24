--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write.               *)
EXTENDS CtrlGrid, Grid, TLC

\* The acceptor names and the configurations, model values that the
\* configurations set.
CONSTANTS a1, a2, a3, a4, a5, a6, c1, c2

\* The partitioned grid of the journal (§J5.1 of docs/journal/, at the git
\* tag journal-design-a) with
\* F = 1: two columns of 2F + 1 = 3 acceptors, a write on F + 1 = 2 of one
\* column, Phase 1 on 3 - 2 + 1 = 2 of every column. Row-major: column 0 is
\* a1, a3, a5 and column 1 is a2, a4, a6.
G6 == <<a1, a2, a3, a4, a5, a6>>
GridMembersOf == [c \in {c1} |-> GridMembers(G6)]
GridQ1Of == [c \in {c1} |-> GridQ1(G6, 2, 2, 0)]
GridQ2Of == [c \in {c1} |-> GridQ2(G6, 2, 2)]

\* A smaller grid for CI: two columns of two, a write on one acceptor,
\* Phase 1 on both acceptors of each column. Column 0 is a1, a3; column 1
\* is a2, a4. A majority of the four is three.
G4 == <<a1, a2, a3, a4>>
Grid4MembersOf == [c \in {c1} |-> GridMembers(G4)]
Grid4Q1Of == [c \in {c1} |-> GridQ1(G4, 2, 1, 0)]
Grid4Q2Of == [c \in {c1} |-> GridQ2(G4, 2, 1)]

\* A majority of three, for the checks that need three ballots.
M3 == {a1, a2, a3}
MajMembersOf == [c \in {c1} |-> M3]
MajQOf == [c \in {c1} |-> MajorityQuorums(M3)]

\* Two configurations: c1 is a majority of a1, a2, a3 (the sealed segment);
\* c2 is a majority of a3, a4 (both), the configuration in force. Ballot 1
\* is in c1, the later ballots in c2.
TwoMembersOf == [c \in {c1, c2} |-> IF c = c1 THEN M3 ELSE {a3, a4}]
TwoQOf == [c \in {c1, c2} |-> MajorityQuorums(TwoMembersOf[c])]

OneCfg == [b \in Ballots |-> c1]
TwoCfg == [b \in Ballots |-> IF b = 1 THEN c1 ELSE c2]

\* The owners of the ballots.
Own123 == [b \in Ballots |-> IF b % 3 = 1 THEN a1 ELSE IF b % 3 = 2 THEN a2 ELSE a3]
OwnTwo == [b \in Ballots |-> IF b = 1 THEN a1 ELSE IF b = 2 THEN a3 ELSE a4]
Own21 == [b \in Ballots |-> IF b = 1 THEN a2 ELSE a1]
=============================================================================
