--------------------------------- MODULE MC ---------------------------------
(***************************************************************************)
(* The constants of the CTRL checks of `JournalCtrl.tla` (section J5.3).   *)
(***************************************************************************)
EXTENDS JournalCtrl, TLC

CONSTANTS a1, a2, a3, r1, r2, r3, u1

\* One acceptor in each grid: a damaged vote is the only report.
Cfg1 == [g \in 1..2 |-> IF g = 1 THEN {a1} ELSE {a2}]
Q1 == [g \in 1..2 |-> {Cfg1[g]}]
\* Three acceptors, majorities: a damaged vote is one report of a quorum,
\* and a quorum of the others can still decide.
Cfg3 == [g \in 1..2 |-> {a1, a2, a3}]
Q3 == [g \in 1..2 |-> {{a1, a2}, {a1, a3}, {a2, a3}}]
\* One replica in each set.
Rep1 == [g \in 1..2 |-> IF g = 1 THEN {r1} ELSE {r2}]

W(u, e, id) == [op |-> "w", u |-> u, e |-> e, id |-> id]
Script(seq) == [s \in 0..(Len(seq) - 1) |-> seq[s + 1]]

\* Multi-writer: two writes that race for slot 0, then one more.
SRace == Script(<<{W(None, 0, 1), W(None, 0, 2)}, {W(None, 1, 3)}>>)
\* Multi-writer: two writes that race for the one slot.
SRace1 == Script(<<{W(None, 0, 1), W(None, 0, 2)}>>)
\* Multi-writer: one write at each slot.
SOne == Script(<<{W(None, 0, 1)}, {W(None, 1, 2)}>>)

\* Ballot b registers configuration b only; or any ballot the first.
OneEach == {<<b, b>> : b \in 1..2}
AllFirst == {<<b, 1>> : b \in 1..3}
=============================================================================
