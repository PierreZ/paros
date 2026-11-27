----------------------------- MODULE HeldReseat -------------------------------
(***************************************************************************)
(* The held tally across a change of the replica set of a segment (R7 and  *)
(* the first Open of R8 of `docs/architecture/04-7-segments.md`). A        *)
(* segment is `Held` once F + 1 of its replicas reported it whole, and     *)
(* then the acceptors drop it: its slots live only at the replicas. The    *)
(* set of the segment can change two ways:                                 *)
(*   - while the segment is open, by a decided `SetReplicas` at slot      *)
(*     `SetAt` (D10): each `Commit` above that slot goes to the new set;   *)
(*   - once it is sealed, by a heal, a `Decree` at the matchmakers that    *)
(*     replaces `HealOut` by `HealIn` (R8), which the fold does not see.  *)
(* A removed replica keeps its copy until the tally marks the segment      *)
(* held over the set in force (it is released then), and a released one   *)
(* may lose its copy at any time: it is out of the set that F protects.    *)
(* Each change of the set is a new version of the set; the held mark       *)
(* names the version that it judged.                                       *)
(*                                                                         *)
(* The log is abstract: slots 1..N of the segment, chosen in order, the   *)
(* `Roll` at N (`held-floor/` checks the log and the floor).              *)
(*                                                                         *)
(* Faults: at most F replicas that F protects (the set in force and the    *)
(* removed ones not yet released) are lost and not yet whole again at one  *)
(* time (the fault rule that `held-floor-retry/` checks the tally under).  *)
(* A `Commit` to a replica may be lost; a replica catches up from the      *)
(* acceptors until they drop the segment, and copies from any replica.     *)
(*                                                                         *)
(* Constants (each rule with a control):                                   *)
(*   Count  "inforce": the tally counts a report only from a member of     *)
(*          the set in force, the heal included (the rule).                *)
(*          "ever": a control, it counts a report from any replica.        *)
(*          "fold": a control, it knows only the set of the fold, so it   *)
(*          neither sees a heal nor judges the segment again after it.    *)
(*   Report "whole": a replica reports once it holds every slot of its     *)
(*          segment (the rule: a new member counts as down until then).   *)
(*          "routed": a control, a replica reports once it holds the      *)
(*          slots that the leader routed to it.                            *)
(*   Stamp  "version": the tally stamps a report with the version of the   *)
(*          set that it knows, and counts a report only at its version     *)
(*          (the rule). "none": a control, a report counts at every        *)
(*          version, so a replica that left the set, dropped its copy and  *)
(*          came back counts with its old report.                          *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS
  Reps,     \* every replica of the model
  Old,      \* the set of the segment at its open
  New,      \* the set that `SetReplicas` names (when SetAt > 0)
  SetAt,    \* the slot of the `SetReplicas`, 0 for none; below N
  N,        \* the slots of the segment, the `Roll` at N
  HealOut,  \* the replica a heal removes (Heal = TRUE)
  HealIn,   \* the replica a heal adds
  Heal,     \* whether the sealed segment is healed
  F,        \* the tolerated failures: `Held` at F + 1 reports
  Losses,   \* how many copies may be lost in a behaviour
  Count,
  Report,
  Stamp

ASSUME Count \in {"inforce", "ever", "fold"}
ASSUME Report \in {"whole", "routed"}
ASSUME Stamp \in {"version", "none"}
ASSUME Old \subseteq Reps /\ New \subseteq Reps /\ {HealOut, HealIn} \subseteq Reps
ASSUME N \in Nat \ {0} /\ SetAt \in 0..(N - 1)
ASSUME F \in Nat

Slots == 1..N

VARIABLES
  chosen,   \* the slots chosen so far: 1..chosen
  acc,      \* the slots that the acceptors hold, until they drop them
  holds,    \* holds[r]: the slots that replica r holds
  reports,  \* the reports that the leader holds, <<replica, version>>
  heldAt,   \* the version of the set that the tally judged held, or -1
  healed,   \* the heal of the sealed segment is decided
  lost,     \* the replicas that lost their copy at least once
  losses

vars == <<chosen, acc, holds, reports, heldAt, healed, lost, losses>>

\* The set of the fold, and its version.
FoldSet == IF SetAt > 0 /\ chosen >= SetAt THEN New ELSE Old
FoldVersion == IF SetAt > 0 /\ chosen >= SetAt THEN 1 ELSE 0

\* The set in force, and its version: a heal counts too.
InForce == IF healed THEN (FoldSet \ {HealOut}) \cup {HealIn} ELSE FoldSet
Version == FoldVersion + (IF healed THEN 1 ELSE 0)

\* What the tally knows of the set and its version.
TallySet == CASE Count = "inforce" -> InForce
              [] Count = "fold" -> FoldSet
              [] OTHER -> Reps
TallyVersion == IF Count = "fold" THEN FoldVersion ELSE Version

\* The set that the leader routes the `Commit` of slot s to.
Route(s) == IF SetAt > 0 /\ s > SetAt THEN New ELSE Old

\* Every replica that a set of the segment named so far.
WasIn == Old \cup (IF SetAt > 0 /\ chosen >= SetAt THEN New ELSE {})
             \cup (IF healed THEN {HealIn} ELSE {})

Sealed == chosen = N
Dropped == heldAt >= 0
Whole(r) == holds[r] = Slots

\* A removed replica is released once the tally marks the segment held at
\* the version that it knows as current.
Released(r) == r \in WasIn \ InForce /\ Dropped /\ heldAt = TallyVersion

\* The replicas that F protects.
Protected == {r \in WasIn : ~Released(r)}

TypeOK ==
  /\ chosen \in 0..N
  /\ acc \subseteq Slots
  /\ holds \in [Reps -> SUBSET Slots]
  /\ reports \subseteq Reps \X (0..2)
  /\ heldAt \in -1..2
  /\ healed \in BOOLEAN
  /\ lost \subseteq Reps
  /\ losses \in 0..Losses

Init ==
  /\ chosen = 0
  /\ acc = {}
  /\ holds = [r \in Reps |-> {}]
  /\ reports = {}
  /\ heldAt = -1
  /\ healed = FALSE
  /\ lost = {}
  /\ losses = 0

\* The next slot of the segment is chosen.
Choose ==
  /\ chosen < N
  /\ chosen' = chosen + 1
  /\ acc' = acc \cup {chosen + 1}
  /\ UNCHANGED <<holds, reports, heldAt, healed, lost, losses>>

\* The leader's `Commit` of slot s reaches replica r of the routed set.
Commit(s, r) ==
  /\ s <= chosen
  /\ r \in Route(s)
  /\ holds' = [holds EXCEPT ![r] = @ \cup {s}]
  /\ UNCHANGED <<chosen, acc, reports, heldAt, healed, lost, losses>>

\* Replica r catches slot s up from the acceptors, which hold it until
\* they drop the segment.
CatchUp(s, r) ==
  /\ s \in acc
  /\ r \in WasIn
  /\ holds' = [holds EXCEPT ![r] = @ \cup {s}]
  /\ UNCHANGED <<chosen, acc, reports, heldAt, healed, lost, losses>>

\* Replica r copies slot s from replica p, a peer that holds it (R8).
Copy(s, r, p) ==
  /\ s \in holds[p]
  /\ r \in WasIn
  /\ holds' = [holds EXCEPT ![r] = @ \cup {s}]
  /\ UNCHANGED <<chosen, acc, reports, heldAt, healed, lost, losses>>

\* Replica r reports the sealed segment. Any replica that a set named may
\* report: a removed one does not know it is out after a heal, so the
\* tally must sort the reports. A whole replica holds the `Roll` at N, so
\* the segment is sealed, and a set named it (`Sound`).
ReportHeld(r) ==
  /\ IF Report = "whole" THEN Whole(r)
     ELSE Sealed /\ r \in WasIn /\ {s \in Slots : r \in Route(s)} \subseteq holds[r]
  /\ reports' = reports \cup {<<r, TallyVersion>>}
  /\ UNCHANGED <<chosen, acc, holds, heldAt, healed, lost, losses>>

\* The leader judges the tally: F + 1 reports of the set it counts, at the
\* version it knows. A report comes only from a sealed segment. Then the
\* acceptors drop the segment.
Counted == {q \in TallySet :
              IF Stamp = "version" THEN <<q, TallyVersion>> \in reports
              ELSE \E v \in 0..2 : <<q, v>> \in reports}

Judge ==
  /\ Cardinality(Counted) >= F + 1
  /\ heldAt' = TallyVersion
  /\ acc' = {}
  /\ UNCHANGED <<chosen, holds, reports, healed, lost, losses>>

\* The heal of the sealed segment is decided at the matchmakers.
HealStep ==
  /\ Heal
  /\ Sealed
  /\ healed' = TRUE
  /\ UNCHANGED <<chosen, acc, holds, reports, heldAt, lost, losses>>

\* Replica r loses its copy: a released replica at any time, a protected
\* one while fewer than F protected replicas are lost and not yet whole.
Lose(r) ==
  /\ losses < Losses
  /\ \/ r \notin Protected
     \/ Cardinality({q \in lost \cap Protected : ~Whole(q)}) < F
  /\ holds' = [holds EXCEPT ![r] = {}]
  /\ lost' = lost \cup {r}
  /\ losses' = losses + 1
  /\ UNCHANGED <<chosen, acc, reports, heldAt, healed>>

Next ==
  \/ Choose
  \/ \E s \in Slots, r \in Reps : Commit(s, r) \/ CatchUp(s, r)
  \/ \E s \in Slots, r, p \in Reps : Copy(s, r, p)
  \/ \E r \in Reps : ReportHeld(r) \/ Lose(r)
  \/ Judge
  \/ HealStep

Spec == Init /\ [][Next]_vars

\* Sound: a replica holds only chosen slots, and only a replica that a set
\* named holds any; the acceptors hold nothing once they dropped the
\* segment; a heal comes only when asked, on a sealed segment.
Sound ==
  /\ \A r \in Reps : holds[r] \subseteq 1..chosen
  /\ \A r \in Reps \ WasIn : holds[r] = {}
  /\ acc \subseteq 1..chosen
  /\ Dropped => acc = {}
  /\ healed => Heal /\ Sealed

\* Kept: once the acceptors dropped the segment, each of its slots is held
\* by some replica.
Kept == Dropped => \A s \in Slots : \E r \in Reps : s \in holds[r]

\* Reaches: the tally marks the segment held after a heal, and a removed
\* replica is released.
NoHeldAfterHeal == ~(healed /\ heldAt = Version)
NoRelease == ~\E r \in Reps : Released(r)
=============================================================================
