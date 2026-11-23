--------------------------------- MODULE MC ---------------------------------
(***************************************************************************)
(* The constants of the compaction checks of `JournalCompact.tla` (section *)
(* J3.2): two replicas in a set, so a laggard has a peer to jump from.     *)
(***************************************************************************)
EXTENDS JournalCompact, TLC

CONSTANTS a1, a2, a3, r1, r2, r3, u1

\* One acceptor in each grid: a compaction drops the only vote.
Cfg1 == [g \in 1..2 |-> IF g = 1 THEN {a1} ELSE {a2}]
Q1 == [g \in 1..2 |-> {Cfg1[g]}]
\* Three acceptors in both grids, majorities: one acceptor can compact while
\* a promise quorum of the others still holds the vote.
Cfg3 == [g \in 1..2 |-> {a1, a2, a3}]
Q3 == [g \in 1..2 |-> {{a1, a2}, {a1, a3}, {a2, a3}}]
\* Two replicas in the set of the first segment, one in the second.
Rep2 == [g \in 1..2 |-> IF g = 1 THEN {r1, r2} ELSE {r3}]
\* One replica in each set.
Rep1 == [g \in 1..2 |-> IF g = 1 THEN {r1} ELSE {r2}]
\* One replica in the first set, two in the second: a replica of the second
\* set can be without a base while its peer compacts.
Rep12 == [g \in 1..2 |-> IF g = 1 THEN {r1} ELSE {r2, r3}]

W(u, e, id) == [op |-> "w", u |-> u, e |-> e, id |-> id]
SL(new, old) == [op |-> "sl", new |-> new, old |-> old]
TR(u, up) == [op |-> "tr", u |-> u, up |-> up]
Script(seq) == [s \in 0..(Len(seq) - 1) |-> seq[s + 1]]

\* The claim of u1, a write, a Truncate over it and a second write: the
\* floor moves to position 1, so a replica compacts to the slot of the
\* second write (or to its applied prefix before it).
SCompact == Script(<<{SL(u1, Unset)}, {W(u1, 0, 1)}, {TR(u1, 1)}, {W(u1, 1, 2)}>>)
\* The claim, a write and a Truncate over it: no record is kept, so a replica
\* compacts to its applied prefix.
STrunc == Script(<<{SL(u1, Unset)}, {W(u1, 0, 1)}, {TR(u1, 1)}>>)

\* The claim and one write: a replica compacts to the slot of the write.
SClaim == Script(<<{SL(u1, Unset)}, {W(u1, 0, 1)}>>)

\* The claim, two writes and a Truncate over the first: the compaction point
\* is the slot of the second write, below the Truncate, so a fold that jumps
\* there is blind until it applies the Truncate.
SBlind == Script(<<{SL(u1, Unset)}, {W(u1, 0, 1)}, {W(u1, 1, 2)}, {TR(u1, 1)}>>)

\* Multi-writer: one write and a Truncate over it, with no claim.
SMultiTrunc == Script(<<{W(None, 0, 1)}, {TR(None, 1)}>>)
\* Multi-writer: two writes and a Truncate over the first: the compaction
\* point is the slot of the second write, below the Truncate, so a fold
\* that jumps there is blind.
SMultiBlind == Script(<<{W(None, 0, 1)}, {W(None, 1, 2)}, {TR(None, 1)}>>)

\* Ballot b registers configuration b only; or any ballot either one.
OneEach == {<<b, b>> : b \in 1..2}
AnyGen == (1..2) \X (1..2)
\* Two leaders of the first configuration.
TwoInFirst == {<<1, 1>>, <<2, 1>>}
=============================================================================
