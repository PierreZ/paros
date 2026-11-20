------------------------------ MODULE GridCheck ------------------------------
(***************************************************************************)
(* The arithmetic of `QuorumSystem::Grid` checked on every layout up to    *)
(* MaxN members (#1102 (rework 1.13)). The state is one layout; it never   *)
(* changes, so each invariant is checked once on each layout.              *)
(*                                                                         *)
(* Constants:                                                              *)
(*   MaxN   the largest membership.                                        *)
(*   Slack  0: the Phase-1 count of the code. 1: a control, Phase 1 takes  *)
(*          one acceptor less of each column.                              *)
(*   Full   TRUE: only the full grids (`acks = rows`) with two columns or  *)
(*          more. FALSE: every layout.                                     *)
(***************************************************************************)
EXTENDS Grid

CONSTANTS MaxN, Slack, Full

VARIABLES n, cols, acks

vars == <<n, cols, acks>>

M == [i \in 1..n |-> i]
Rows == GridRows(M, cols)
Q1 == GridQ1(M, cols, acks, Slack)
Q2 == GridQ2(M, cols, acks)

Init ==
  /\ n \in 1..MaxN
  /\ cols \in {c \in 1..n : n % c = 0}
  /\ acks \in 1..(n \div cols)
  /\ Full => acks = n \div cols /\ cols >= 2

Next == UNCHANGED vars

Spec == Init /\ [][Next]_vars

\* Paxos safety: every Phase-1 quorum meets every Phase-2 quorum
\* (`cross_intersects`).
CrossIntersect == \A P \in Q1, C \in Q2 : P \cap C # {}

\* Each read target holds a Phase-1 quorum, so a read meets every Phase-2
\* quorum. On the full grid a row is a Phase-1 quorum.
ReadsArePhase1 == \A T \in GridReadTargets(M, cols, acks) : \E P \in Q1 : P \subseteq T

\* `tolerated_failures`: min(acks - 1, cols * (rows - acks + 1) - 1) is the
\* largest f such that any f failures leave a Phase-1 and a Phase-2 quorum.
Survives(f) ==
  \A F \in {S \in SUBSET GridMembers(M) : Cardinality(S) = f} :
    /\ \E P \in Q1 : P \cap F = {}
    /\ \E C \in Q2 : C \cap F = {}
Tolerated ==
  LET a == acks - 1
      b == cols * (Rows - acks + 1) - 1
  IN IF a <= b THEN a ELSE b
ToleratedIsExact == Survives(Tolerated) /\ ~Survives(Tolerated + 1)

\* #1257 (quorum everywhere): f acceptors down in every column at once leave
\* a Phase-1 quorum and a Phase-2 quorum of some column exactly when
\* f + 1 <= acks <= rows - f. The partitions `rows = 2F + 1, acks = F + 1`
\* survive F down in every column; the full grid (`acks = rows`) survives no
\* member down in every column, because a write needs a whole column.
SurvivesPerColumn(f) ==
  \A D \in {S \in SUBSET GridMembers(M) :
            \A j \in 0..(cols - 1) : Cardinality(S \cap GridColumn(M, cols, j)) = f} :
    /\ \E P \in Q1 : P \cap D = {}
    /\ \E C \in Q2 : C \cap D = {}
PerColumnTolerance ==
  \A f \in 0..Rows : SurvivesPerColumn(f) <=> (f + 1 <= acks /\ acks <= Rows - f)

\* Not true of every grid. Two Phase-1 quorums meet only when
\* 2 * (rows - acks + 1) > rows: on the partitions, not on the full grid.
\* With Full = TRUE, TLC must find two rows that do not meet. This is why a
\* row that did not hear of a campaign can still answer a read (`reads/`).
Phase1Meets == \A P, R \in Q1 : P \cap R # {}
=============================================================================
