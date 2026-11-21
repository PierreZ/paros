--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write.               *)
EXTENDS Journal, TLC

CONSTANTS a1, a2, a3, r1, r2, r3, u1, u2

\* One acceptor in each grid, one replica in each set: a reshape moves the
\* journal to new acceptors and new replicas.
Cfg1 == [g \in 1..3 |-> CASE g = 1 -> {a1} [] g = 2 -> {a2} [] g = 3 -> {a3}]
Q1 == [g \in 1..3 |-> {Cfg1[g]}]
Rep1 == [g \in 1..3 |-> CASE g = 1 -> {r1} [] g = 2 -> {r2} [] g = 3 -> {r3}]

\* The first grid has three acceptors (majorities), the next ones one: an
\* old leader can still win a vote outside the promise quorum of a sealer,
\* so two sealers can compute two ends, and an Accept can raise a promise.
CfgReg == [g \in 1..3 |-> CASE g = 1 -> {a1, a2, a3} [] g = 2 -> {a3} [] g = 3 -> {a2}]
QReg == [g \in 1..3 |-> IF g = 1 THEN {{a1, a2}, {a1, a3}, {a2, a3}} ELSE {CfgReg[g]}]

\* Three acceptors in every grid, majorities.
Cfg3 == [g \in 1..3 |-> {a1, a2, a3}]
Q3 == [g \in 1..3 |-> {{a1, a2}, {a1, a3}, {a2, a3}}]

W(u, e, id) == [op |-> "w", u |-> u, e |-> e, id |-> id]
SL(new, old) == [op |-> "sl", new |-> new, old |-> old]
TR(u, up) == [op |-> "tr", u |-> u, up |-> up]

\* A script gives the commands that a leader may propose at each slot (a
\* `Noop` may come too, from the gap fill).
\* The claim of u1 on a fresh journal, its write at position 0, and a second
\* copy of the write (a resend).
Resend == <<{SL(u1, Unset)}, {W(u1, 0, 1)}, {W(u1, 0, 1)}>>
\* The claim, the write, and SetLeader(None, u1): multi-writer.
ToNone == <<{SL(u1, Unset)}, {W(u1, 0, 1)}, {SL(None, u1)}>>
\* The same on a fresh journal that is multi-writer.
ToNoneFresh == <<{SL(u1, None)}, {W(u1, 0, 1)}, {SL(None, u1)}>>
\* The claim, the write and a Truncate of u1 over it.
Trunc == <<{SL(u1, Unset)}, {W(u1, 0, 1)}, {TR(u1, 1)}>>
\* The claim, the SetLeader that deposes u1, and a write of u1.
Move == <<{SL(u1, Unset)}, {SL(u2, u1)}, {W(u1, 0, 1)}>>
\* Two unfenced writes that differ at each slot, on a fresh journal that is
\* multi-writer (L0 = None): Paxos and the seals.
Two == <<{W(None, 0, 1), W(None, 0, 2)}, {W(None, 0, 1), W(None, 0, 2)}>>
Script(seq) == [s \in 0..(Len(seq) - 1) |-> seq[s + 1]]
SResend == Script(Resend)
SToNone == Script(ToNone)
SToNoneFresh == Script(ToNoneFresh)
STrunc == Script(Trunc)
SMove == Script(Move)
STwo == Script(Two)
\* The claim of u1, its write at position 0, and at the third slot a resend
\* of it or another write of u1 at position 0 (not a resend).
SResendOther == Script(<<{SL(u1, Unset)}, {W(u1, 0, 1)}, {W(u1, 0, 1), W(u1, 0, 2)}>>)
\* One unfenced write at each of two slots (L0 = None).
\* The claim of u1 and its write.
SClaimWrite == Script(<<{SL(u1, Unset)}, {W(u1, 0, 1)}>>)
\* One unfenced write, one slot (L0 = None).
SSingle == Script(<<{W(None, 0, 1), W(None, 0, 2)}>>)
SOne == Script(<<{W(None, 0, 1)}, {W(None, 1, 2)}>>)

\* Ballot b registers configuration b only: no leader change inside a
\* configuration (the checks of the chain).
OneEach == {<<b, b>> : b \in 1..3}
\* Any ballot may register the newest configuration: leader changes, and two
\* first leaders of one configuration that race to seal.
AnyGen == (1..3) \X (1..3)
\* Ballot 1 leads the first grid; ballots 2 and 3 are two first leaders of
\* the second grid that race to seal it.
SealRace == {<<1, 1>>, <<2, 2>>, <<3, 2>>}
=============================================================================
