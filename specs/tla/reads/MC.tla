--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS Reads, Grid, TLC

\* paros: ballot (round, node). Ballot 2 of proposer 2 is between ballots 1 and 3.
OwnerParos3 == <<1, 2, 1>>

\* Two ballots of two proposers, and two ballots of one proposer (a bypass).
OwnerTwo  == <<1, 2>>
OwnerSame == <<1, 1>>
OwnerSame3 == <<1, 1, 1>>

\* A configuration of three members, then one of two with one common member.
\* A majority of the first can choose a slot without the common member.
ConfigsRead == {{"a1", "a2", "a3"}, {"a3", "a4"}}

\* One reader that is only in the old configuration, and the common member.
ReadersBoth == {"a1", "a3"}

\* The quorums of a majority: a reader asks the whole membership.
Q1Maj == [c \in ConfigsRead |-> MajorityQuorums(c)]
TargetsMaj == [c \in ConfigsRead |-> {c}]

\* The full grid of 2 x 2 (`QuorumSystem::grid(2, 2)`): G over a1..a4, then
\* H, where a5 replaces a1. The layout is row-major over the sorted members:
\* G has the rows {a1, a2} and {a3, a4} and the columns {a1, a3} and
\* {a2, a4}; H has the rows {a2, a3} and {a4, a5}. Phase 2 takes a whole
\* column, any column; Phase 1 one acceptor of every column; a reader asks
\* one row.
SeqG == <<"a1", "a2", "a3", "a4">>
SeqH == <<"a2", "a3", "a4", "a5">>
CfgG == GridMembers(SeqG)
CfgH == GridMembers(SeqH)
ConfigsFull == {CfgG, CfgH}
ConfigsFullStatic == {CfgG}
SeqOf(c) == IF c = CfgG THEN SeqG ELSE SeqH
Q1Full == [c \in ConfigsFull |-> GridQ1(SeqOf(c), 2, 2, 0)]
Q2Full == [c \in ConfigsFull |-> GridQ2(SeqOf(c), 2, 2)]
TargetsFull == [c \in ConfigsFull |-> GridReadTargets(SeqOf(c), 2, 2)]

\* The partitions of #532 (acceptor partitions), F = 1 and two partitions
\* (`QuorumSystem::partitions(1, 2)`, a grid of 3 x 2 with acks = 2): P over
\* a1..a6, then R, where a7 replaces a1. Phase 2 takes two acceptors of one
\* column; Phase 1 two of every column; a reader asks every member.
SeqP == <<"a1", "a2", "a3", "a4", "a5", "a6">>
SeqR == <<"a2", "a3", "a4", "a5", "a6", "a7">>
CfgP == GridMembers(SeqP)
CfgR == GridMembers(SeqR)
ConfigsPart == {CfgP, CfgR}
SeqPart(c) == IF c = CfgP THEN SeqP ELSE SeqR
Q1Part == [c \in ConfigsPart |-> GridQ1(SeqPart(c), 2, 2, 0)]
Q2Part == [c \in ConfigsPart |-> GridQ2(SeqPart(c), 2, 2)]
TargetsPart == [c \in ConfigsPart |-> GridReadTargets(SeqPart(c), 2, 2)]

\* A grid whose Phase-1 quorums need not meet (#1483): four rows, one column,
\* acks = 3 (`QuorumSystem::grid(4, 1, 3)`). Phase 2 takes three acceptors,
\* Phase 1 two, and two Phase-1 quorums can be disjoint. It is not a full
\* grid, so a reader asks every member and the seal is a Phase-1 quorum.
\* F over a1..a4, then K, where a5 replaces a1.
SeqF == <<"a1", "a2", "a3", "a4">>
SeqK == <<"a2", "a3", "a4", "a5">>
CfgF == GridMembers(SeqF)
CfgK == GridMembers(SeqK)
ConfigsRows4 == {CfgF, CfgK}
SeqRows4(c) == IF c = CfgF THEN SeqF ELSE SeqK
Q1Rows4 == [c \in ConfigsRows4 |-> GridQ1(SeqRows4(c), 1, 3, 0)]
Q2Rows4 == [c \in ConfigsRows4 |-> GridQ2(SeqRows4(c), 1, 3)]
TargetsRows4 == [c \in ConfigsRows4 |-> GridReadTargets(SeqRows4(c), 1, 3)]
\* The same layout with acks = 2: Phase 1 takes three, so two Phase-1 quorums
\* meet (the admission rule that #1483 proposes).
Q1Rows4Meet == [c \in ConfigsRows4 |-> GridQ1(SeqRows4(c), 1, 2, 0)]
Q2Rows4Meet == [c \in ConfigsRows4 |-> GridQ2(SeqRows4(c), 1, 2)]
TargetsRows4Meet == [c \in ConfigsRows4 |-> GridReadTargets(SeqRows4(c), 1, 2)]

\* The reader of the grids: a1, the member that leaves. One reader keeps the
\* checks small; the stale read of `grid_full` needs only this one.
ReadersGrid == {"a1"}

\* A lagging reader (#1228 (stale read by a lagging reader)): a1 is in the
\* old configuration only, and its pool lags, so it ignores the new one while
\* it promises its ballot. A majority of the new configuration chooses a
\* slot without a1 and a2.
ConfigsLag == {{"a1", "a2", "a3"}, {"a3", "a4", "a5"}}
ReadersLag == {"a1"}
Q1Lag == [c \in ConfigsLag |-> MajorityQuorums(c)]
TargetsLag == [c \in ConfigsLag |-> {c}]

\* No read replica.
NoReplicas == {}

\* A read replica r1, the one reader (#1230 (rules no check tests)): it is in
\* no configuration, and its basis can go stale (`ReplicaRestart`, a late
\* beat).
ReadersReplica == {"r1"}

\* The learner checks: ballot 3 is only the abandoned campaign of a reader.
OnlyOwn3 == status[3] \in {"matching", "abandoned"} /\ replied[3] = {}

Symmetry == Permutations(Values) \cup Permutations(Matchmakers)
\* Each grid layout tiles its members (`QuorumSystem::admits`).
ASSUME \A c \in ConfigsFull : GridAdmits(SeqOf(c), 2, 2)
ASSUME \A c \in ConfigsPart : GridAdmits(SeqPart(c), 2, 2)
ASSUME \A c \in ConfigsRows4 : GridAdmits(SeqRows4(c), 1, 3)
ASSUME \A c \in ConfigsRows4 : GridAdmits(SeqRows4(c), 1, 2)

=============================================================================
