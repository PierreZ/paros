-------------------------------- MODULE Health --------------------------------
(***************************************************************************)
(* The health measures of a journal's acceptor grid, checked against the   *)
(* quorums of `grid/Grid.tla`, for #1260 (journal health measures).        *)
(* `docs/architecture/05-failure-model-and-zones.md` section 5.1 has the   *)
(* catalogue.                                                              *)
(*                                                                         *)
(* A health measure is a count: the live acceptors of a column against a   *)
(* threshold. The grid controller acts on the counts. The protocol acts on *)
(* the quorums. This model checks that each count says what the quorums    *)
(* say, on every grid that the fenced reshape accepts                      *)
(* (`F + 1 <= acks <= rows - F` with F >= 1, section 2.11), for every set  *)
(* of live acceptors. It also checks that the journal states are a         *)
(* partition, and that the fixed action of each state can run.             *)
(*                                                                         *)
(* The state is one layout, one live set and two flags. It never changes,  *)
(* so each invariant is checked once on each state.                        *)
(*                                                                         *)
(* Constants:                                                              *)
(*   MinN, MaxN  the sizes of the membership.                              *)
(*   Measure     "code": the counts of section 5.1.                        *)
(*               "p2_only": a control, electable when every column has     *)
(*               `acks` live (the Phase-2 count read as the quorum).       *)
(*               "majority": a control, electable on a majority of all     *)
(*               the members (a count of the whole grid).                  *)
(*               "p1_only": a control, a column takes writes when it has   *)
(*               `rows - acks + 1` live.                                   *)
(*   States      "code": the seven states of section 5.1.                  *)
(*               "draft": a control, the states of the first draft:        *)
(*               WritesDegraded and ElectionBlocked by the leader alone,   *)
(*               and no state for a grid with no writable column.          *)
(*   HoldSlack   0: the bypass hold counts `acks` promises of one column.  *)
(*               1: a control, one promise less.                           *)
(*   Zones       0: the grid checks, every live set. Z > 0: the zone       *)
(*               checks, every member live and every map of the members to *)
(*               Z zones.                                                  *)
(*   V1          TRUE: the v1 scope of #1315. Only the partitioned grid    *)
(*               (`rows = 2F + 1`, `acks = F + 1`; the majority is one     *)
(*               column), and no bypass of section J5.5, so WritesDegraded *)
(*               holds and alerts. FALSE: every grid of the bound and the  *)
(*               bypass hold, the design for after v1.                     *)
(***************************************************************************)
EXTENDS Grid

CONSTANTS MinN, MaxN, Measure, States, HoldSlack, Zones, V1

VARIABLES n, cols, acks, live, leader, placeable, zone

vars == <<n, cols, acks, live, leader, placeable, zone>>

M == [i \in 1..n |-> i]
Members == GridMembers(M)
Rows == GridRows(M, cols)
Cols == 0..(cols - 1)
Col(j) == GridColumn(M, cols, j)
Q1 == GridQ1(M, cols, acks, 0)

Init ==
  /\ n \in MinN..MaxN
  /\ cols \in {c \in 1..n : n % c = 0 /\ n \div c >= 3}
  \* The bound of the fenced reshape for some F >= 1: 2 <= acks <= rows - 1.
  /\ acks \in 2..(n \div cols - 1)
  \* The v1 grids: the partitioned grid only, rows = 2F + 1 and acks = F + 1.
  /\ V1 => n \div cols = 2 * acks - 1
  /\ IF Zones = 0
     THEN /\ live \in SUBSET Members
          /\ leader \in BOOLEAN
          /\ placeable \in BOOLEAN
          /\ zone = [i \in 1..n |-> 0]
     \* The zone checks: every member live, every map of members to zones.
     ELSE /\ live = Members
          /\ leader = TRUE
          /\ placeable = TRUE
          /\ zone \in [1..n -> 1..Zones]

Next == UNCHANGED vars

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* The protocol facts, as sets: what `membership/` answers.                *)
(***************************************************************************)

\* A new leader can run Phase 1 with the acceptors of L only.
ElectableP(L) == \E P \in Q1 : P \subseteq L

\* Column j can choose a slot with the acceptors of L only.
ColWritableP(L, j) == \E C \in GridQ2At(M, cols, acks, j) : C \subseteq L

\* Some column can choose a slot (a stuck round moves, #533).
WritableP(L) == \E j \in Cols : ColWritableP(L, j)

\* The bypass hold of section 2.13: the promises of L meet every read
\* quorum of the grid. A read completes on a Phase-1 quorum, so L must meet
\* every Phase-1 quorum. The hold is monotone in L, so it can complete with
\* the live acceptors exactly when the whole live set meets every quorum.
HoldP(L) == \A P \in Q1 : P \cap L # {}

(***************************************************************************)
(* The health measures, as counts: what the controller computes.           *)
(***************************************************************************)

LiveIn(L, j) == Cardinality(L \cap Col(j))

P1Need == Rows - acks + 1
P2Need == acks

\* p1_spare and p2_spare of section 5.1.
P1Spare(L, j) == LiveIn(L, j) - P1Need
P2Spare(L, j) == LiveIn(L, j) - P2Need
Spare(L, j) == IF P1Spare(L, j) < P2Spare(L, j) THEN P1Spare(L, j) ELSE P2Spare(L, j)

ElectableH(L) ==
  CASE Measure = "p2_only"  -> \A j \in Cols : P2Spare(L, j) >= 0
    [] Measure = "majority" -> 2 * Cardinality(L) > n
    [] OTHER                -> \A j \in Cols : P1Spare(L, j) >= 0

ColWritableH(L, j) ==
  IF Measure = "p1_only" THEN P1Spare(L, j) >= 0 ELSE P2Spare(L, j) >= 0

WritableH(L) == \E j \in Cols : ColWritableH(L, j)

\* Every column has a spare of both kinds.
SpareEverywhere(L) ==
  \A j \in Cols :
    CASE Measure = "p2_only" -> P2Spare(L, j) >= 1
      [] Measure = "p1_only" -> P1Spare(L, j) >= 1
      [] OTHER               -> Spare(L, j) >= 1

\* The hold of the bypass, as a count: `acks` promises of one column.
HoldH(L) == \E j \in Cols : LiveIn(L, j) >= acks - HoldSlack

(***************************************************************************)
(* The journal states of section 5.1 (the grid part; the segment, the      *)
(* matchmaker and the controller states are separate flags).               *)
(***************************************************************************)

Healthy           == SpareEverywhere(live)
AtRisk            == ~Healthy /\ ElectableH(live) /\ WritableH(live) /\ placeable
AtRiskUnplaceable == ~Healthy /\ ElectableH(live) /\ WritableH(live) /\ ~placeable
\* The first draft named a state by "a column below quorum" and the leader
\* alone, with no test of a writable column.
DraftWritable == States = "draft" \/ WritableH(live)
WritesDegraded    == ~ElectableH(live) /\ DraftWritable /\ leader
ElectionBlocked   == ~ElectableH(live) /\ DraftWritable /\ ~leader
WritesBlocked     == States = "code" /\ ElectableH(live) /\ ~WritableH(live)
Stopped           == States = "code" /\ ~ElectableH(live) /\ ~WritableH(live)

StateSet ==
  {s \in {"Healthy", "AtRisk", "AtRiskUnplaceable", "WritesDegraded",
          "ElectionBlocked", "WritesBlocked", "Stopped"} :
     CASE s = "Healthy"           -> Healthy
       [] s = "AtRisk"            -> AtRisk
       [] s = "AtRiskUnplaceable" -> AtRiskUnplaceable
       [] s = "WritesDegraded"    -> WritesDegraded
       [] s = "ElectionBlocked"   -> ElectionBlocked
       [] s = "WritesBlocked"     -> WritesBlocked
       [] s = "Stopped"           -> Stopped}

(***************************************************************************)
(* Invariants.                                                             *)
(***************************************************************************)

\* Each count says what the quorums say.
ElectableMeaning == ElectableH(live) <=> ElectableP(live)
WritableMeaning  == \A j \in Cols : ColWritableH(live, j) <=> ColWritableP(live, j)
HoldMeaning      == HoldH(live) <=> HoldP(live)

\* The hold can complete exactly when a column can write: a column with
\* `acks` live acceptors leaves the bypass of section 2.13 its blocking set.
HoldIsWritable == HoldP(live) <=> WritableP(live)

\* A spare everywhere means: any one more loss leaves every column writable
\* and an election possible. With no spare somewhere, one loss breaks one.
SpareMeaning ==
  Healthy <=>
    \A a \in Members :
      /\ ElectableP(live \ {a})
      /\ \A j \in Cols : ColWritableP(live \ {a}, j)

\* The states are a partition: every live set is in exactly one state.
OneState == Cardinality(StateSet) = 1

\* The fixed action of each state can run, or nothing can run.
\*   AtRisk, AtRiskUnplaceable: the data plane serves, and a heal reshape
\*     can run its full Phase 1 over the old grid.
\*   WritesDegraded: no election can run. The live leader writes on in the
\*     columns that can write. In v1 it holds and alerts, so a column that
\*     can write is all it needs. After v1 a heal by the bypass of section
\*     J5.5 runs, and its hold can complete.
\*   ElectionBlocked: no election can run, so no leader and no reshape: hold.
\*   WritesBlocked: no column can write, but an election and so a heal
\*     reshape can run.
\*   Stopped: no election, no write (and after v1 no bypass hold): hold and
\*     page.
ActionCanRun ==
  /\ Healthy => ElectableP(live) /\ WritableP(live)
  /\ (AtRisk \/ AtRiskUnplaceable) => ElectableP(live) /\ WritableP(live)
  /\ WritesDegraded => ~ElectableP(live) /\ IF V1 THEN WritableP(live) ELSE HoldP(live)
  /\ ElectionBlocked => ~ElectableP(live)
  /\ WritesBlocked => ElectableP(live) /\ ~WritableP(live)
  /\ Stopped => ~ElectableP(live) /\ ~WritableP(live) /\ (V1 \/ ~HoldP(live))

\* On the partitioned grid (rows = 2F + 1, acks = F + 1) the two spares are
\* one number, so the catalogue can show one spare per column.
PartitionedOneSpare ==
  (Rows = 2 * acks - 1) => \A j \in Cols : P1Spare(live, j) = P2Spare(live, j)

(***************************************************************************)
(* The zone checks (Zones > 0).                                            *)
(***************************************************************************)

InZone(d) == {i \in Members : zone[i] = d}
ZoneIds == 1..Zones

\* Rule 1 of the zone rule (section 5): both quorums hold on M minus any
\* one zone.
ZoneRule == \A d \in ZoneIds : ElectableP(Members \ InZone(d)) /\ WritableP(Members \ InZone(d))

\* No placement over two zones survives the loss of one of them, on any grid
\* (a Phase-1 quorum inside one zone misses a Phase-2 quorum inside the
\* other). So after a zone loss on three zones no heal can meet the zone
\* rule: the controller holds and alerts. With Zones = 3 TLC must find a
\* placement that meets it (`zones3_placeable`, a control).
NoPlacement == ~ZoneRule

\* zone_margin of section 5.1: the worst p1_spare and p2_spare after the
\* loss of any one zone, as counts, against the quorums.
ZoneMarginMeaning ==
  /\ (\A d \in ZoneIds, j \in Cols : P1Spare(Members \ InZone(d), j) >= 0)
       <=> (\A d \in ZoneIds : ElectableP(Members \ InZone(d)))
  /\ (\A d \in ZoneIds : \E j \in Cols : P2Spare(Members \ InZone(d), j) >= 0)
       <=> (\A d \in ZoneIds : WritableP(Members \ InZone(d)))

\* Row = zone on the partitioned grid (rows = 2F + 1 zones, acks = F + 1):
\* a zone loss takes one acceptor from every column at once, so every
\* column drops to the spare F - 1 together (0 for F = 1).
RowIsZone == \A i \in Members : zone[i] = (i - 1) \div cols + 1
ZoneLossEveryColumn ==
  (Zones = Rows /\ Rows = 2 * acks - 1 /\ RowIsZone) =>
    \A d \in ZoneIds, j \in Cols : Spare(Members \ InZone(d), j) = acks - 2

\* No grid of the check is electable and writes nowhere: WritesBlocked is
\* empty. It holds on the v1 grids (`v1`), where a Phase-1 quorum holds
\* `acks` acceptors of every column. Without V1 it is a reached state
\* (`reach_writes_blocked`): WritesBlocked is not empty on the wider grids.
NoWritesBlocked == ~(ElectableP(live) /\ ~WritableP(live))
=============================================================================
