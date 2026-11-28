----------------------------- MODULE ReseatMany ------------------------------
(***************************************************************************)
(* Several `SetReplicas` of one open segment (D10 of                       *)
(* `docs/architecture/04-7-segments.md`), as the core folds them: the     *)
(* fold keeps the members that a valid `SetReplicas` removed              *)
(* (`JournalState::removed`), a removed member stops where it was removed, *)
(* and a `SetReplicas` that names a removed member again is refused        *)
(* (`Outcome::ReseatRemoved`). `segment-reseat/` checks one `SetReplicas`, *)
(* its route and its reads; this model checks what the fold of several     *)
(* adds: who stops, and that every member of the set in force walks the    *)
(* whole segment.                                                          *)
(*                                                                         *)
(* The log is slots 1..N of the segment, chosen in any order. A slot of    *)
(* `SetSlots` holds its `SetReplicas` `Prop[s]` or, after a change of      *)
(* leader, a `Noop`. The set at the open of the segment is `Old`. The      *)
(* leader routes the `Commit` of slot s to the set that the fold gives     *)
(* before s (`Route = "fold"` of `segment-reseat/`). A replica pulls any  *)
(* chosen slot, and walks its contiguous prefix while it is not out.       *)
(*                                                                         *)
(* Constants (each rule with a control):                                   *)
(*   Refuse  TRUE: a `SetReplicas` that names a removed member is refused  *)
(*           (the rule). FALSE: a control, it is valid, and the member it  *)
(*           names again stopped already, so it never walks the segment.   *)
(*   OutRule "removed": a replica is out once the fold at its walk names   *)
(*           it removed (the rule). "state": a control, it is out once    *)
(*           the set at its walk leaves it out, so a member that a later   *)
(*           `SetReplicas` adds is out from the start and never walks.     *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
  Reps,     \* every replica of the model
  Old,      \* the set of the segment at its open
  N,        \* the slots of the segment
  SetSlots, \* the slots that hold a proposed `SetReplicas`
  Prop,     \* Prop[s]: the set that the `SetReplicas` of slot s names
  Refuse,
  OutRule

ASSUME Old \subseteq Reps /\ Old # {}
ASSUME N \in Nat \ {0} /\ SetSlots \subseteq 1..N
ASSUME \A s \in SetSlots : Prop[s] \subseteq Reps /\ Prop[s] # {}
ASSUME Refuse \in BOOLEAN /\ OutRule \in {"removed", "state"}

Slots == 1..N

VARIABLES
  chosen,   \* the slots chosen so far
  isSet,    \* isSet[s]: slot s of SetSlots holds its `SetReplicas`
  holds,    \* holds[r]: the slots that replica r holds
  walk      \* walk[r]: the end of the contiguous prefix that r walked

vars == <<chosen, isSet, holds, walk>>

\* Whether the `SetReplicas` of slot k names a member that fold f removed.
NamesRemoved(k, f) == Prop[k] \cap f[2] # {}

\* The fold after slot k: <<the set of the segment, the removed members>>.
\* A slot that is not chosen, or holds a `Noop`, moves nothing.
RECURSIVE Fold(_)
Fold(k) ==
  IF k = 0 THEN <<Old, {}>>
  ELSE LET f == Fold(k - 1) IN
       IF k \in SetSlots /\ k \in chosen /\ isSet[k]
       THEN IF Refuse /\ NamesRemoved(k, f) THEN f
            ELSE <<Prop[k], f[2] \cup (f[1] \ Prop[k])>>
       ELSE f

\* The set in force at the head of the chosen log, and every replica that
\* a set of the segment named so far.
InForce == Fold(N)[1]
WasIn == UNION {Fold(k)[1] : k \in 0..N}

\* Replica r is out: its segment ended at its walk.
Out(r) ==
  IF OutRule = "removed" THEN r \in Fold(walk[r])[2]
  ELSE r \notin Fold(walk[r])[1]

TypeOK ==
  /\ chosen \subseteq Slots
  /\ isSet \in [SetSlots -> BOOLEAN]
  /\ holds \in [Reps -> SUBSET Slots]
  /\ walk \in [Reps -> 0..N]

Init ==
  /\ chosen = {}
  /\ isSet = [s \in SetSlots |-> FALSE]
  /\ holds = [r \in Reps |-> {}]
  /\ walk = [r \in Reps |-> 0]

\* Slot s is chosen; a slot of SetSlots holds its `SetReplicas` (v) or a
\* `Noop` of a new leader.
Choose(s, v) ==
  /\ s \notin chosen
  /\ chosen' = chosen \cup {s}
  /\ isSet' = IF s \in SetSlots THEN [isSet EXCEPT ![s] = v] ELSE isSet
  /\ UNCHANGED <<holds, walk>>

\* The leader's `Commit` of slot s, routed to the set that the fold gives
\* before s, reaches replica r. The route over a prefix that is not all
\* chosen yet is `segment-reseat/`'s.
Commit(s, r) ==
  /\ s \in chosen
  /\ r \in Fold(s - 1)[1]
  /\ holds' = [holds EXCEPT ![r] = @ \cup {s}]
  /\ UNCHANGED <<chosen, isSet, walk>>

\* Replica r, which a set named, pulls the chosen slot s from the
\* acceptors or a peer. A replica that is out may still hold a slot past
\* its stop; it never walks it (the core drops it, which the model leaves
\* out).
CatchUp(s, r) ==
  /\ s \in chosen
  /\ r \in WasIn
  /\ holds' = [holds EXCEPT ![r] = @ \cup {s}]
  /\ UNCHANGED <<chosen, isSet, walk>>

\* Replica r walks the next slot of its prefix, while it is not out.
Walk(r) ==
  /\ ~Out(r)
  /\ walk[r] + 1 \in holds[r]
  /\ walk' = [walk EXCEPT ![r] = @ + 1]
  /\ UNCHANGED <<chosen, isSet, holds>>

Next ==
  \/ \E s \in Slots, v \in BOOLEAN : Choose(s, v)
  \/ \E s \in Slots, r \in Reps : Commit(s, r) \/ CatchUp(s, r)
  \/ \E r \in Reps : Walk(r)

Fairness ==
  /\ \A s \in Slots : WF_vars(\E v \in BOOLEAN : Choose(s, v))
  /\ \A s \in Slots, r \in Reps : WF_vars(Commit(s, r)) /\ WF_vars(CatchUp(s, r))
  /\ \A r \in Reps : WF_vars(Walk(r))

Spec == Init /\ [][Next]_vars /\ Fairness

\* A removed member stops at the `SetReplicas` that removed it: its walk
\* reaches that slot and no slot past it.
Stops ==
  OutRule = "removed" =>
    \A r \in Reps : Out(r) => walk[r] > 0 /\ r \notin Fold(walk[r] - 1)[2]

\* Sound: a replica holds only chosen slots and walks only slots it holds;
\* only a replica that a set named holds any; with the refusal, no removed
\* member is in the set, and a member stays removed for the rest of the
\* segment.
Sound ==
  /\ \A r \in Reps : holds[r] \subseteq chosen
  /\ \A r \in Reps : 1..walk[r] \subseteq holds[r]
  /\ \A r \in Reps \ WasIn : holds[r] = {}
  /\ Refuse => \A k \in 0..N : Fold(k)[1] \cap Fold(k)[2] = {}
  /\ \A k \in 1..N : Fold(k - 1)[2] \subseteq Fold(k)[2]
  /\ Stops

\* Heals: every member of the set in force walks the whole segment.
Whole == <>[](\A r \in InForce : walk[r] = N)

\* Reaches: a `SetReplicas` that names a removed member is chosen.
NoNamedAgain ==
  ~\E k \in SetSlots : k \in chosen /\ isSet[k] /\ NamesRemoved(k, Fold(k - 1))

\* Reaches: two `SetReplicas` remove a member each.
NoTwoRemoved == Cardinality(Fold(N)[2]) < 2
=============================================================================
