---------------------------- MODULE SegmentReseat -----------------------------
(***************************************************************************)
(* A `SetReplicas` of the open segment (D10 of                             *)
(* `docs/architecture/04-7-segments.md`): the decided command at slot      *)
(* `SetAt` replaces the set `Old` of the segment by `New`. The model       *)
(* checks the route of each `Commit` across the change, the join of a new  *)
(* member in the middle of the segment, and the end of the segment for a   *)
(* removed member.                                                         *)
(*                                                                         *)
(* The log is abstract: slots 1..N of the segment, chosen in any order (a  *)
(* leader pipelines). Slot `SetAt` holds the `SetReplicas` or, after a     *)
(* change of leader, a `Noop` of the gap fill: then the set stays `Old`.   *)
(* A replica holds a slot from a `Commit`, or from a catch-up from the     *)
(* acceptors or a peer (one step: `held-floor/` and `peer-pull/` check     *)
(* the sources). It walks its contiguous prefix, and serves a read of a    *)
(* slot only from that prefix (the read rule of `compartments/`). A        *)
(* removed member that walked the `SetReplicas` knows that its segment     *)
(* ends there: it pulls no more, and answers a read above it with          *)
(* "moved", as a replica past its `Roll` answers "sealed".                 *)
(* The held tally across the change, and the drop of the copy of a         *)
(* removed member, are `held-reseat/`.                                     *)
(*                                                                         *)
(* Constants (each rule with a control):                                   *)
(*   Route  "fold": the leader routes the `Commit` of slot s only once its *)
(*          fold applied the slots below s (R4d), to the set that the     *)
(*          fold names there (the rule). "proposed": a control, it routes  *)
(*          by the `SetReplicas` that it proposed, before it is chosen.    *)
(*   Pull   TRUE: a replica that a set named and that does not know it is  *)
(*          out pulls the slots it lacks (the rule: a new member joins at  *)
(*          the start of the segment). FALSE: a control, a replica gets    *)
(*          only the `Commit`s routed to it.                               *)
(*   Moved  TRUE: a removed member answers a read above the `SetReplicas`  *)
(*          with "moved" (the rule). FALSE: a control, it serves only.     *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS
  Reps,     \* every replica of the model
  Old,      \* the set of the segment at its open
  New,      \* the set that the `SetReplicas` names
  SetAt,    \* the slot of the `SetReplicas`, below N
  N,        \* the slots of the segment
  Asks,     \* the reads a reader may ask: <<replica, slot>>
  Route,
  Pull,
  Moved

ASSUME Old \subseteq Reps /\ New \subseteq Reps /\ Old # New
ASSUME N \in Nat \ {0} /\ SetAt \in 1..(N - 1)
ASSUME Asks \subseteq Reps \X (1..N)
ASSUME Route \in {"fold", "proposed"} /\ Pull \in BOOLEAN /\ Moved \in BOOLEAN

Slots == 1..N

VARIABLES
  chosen,   \* the slots chosen so far
  setVal,   \* slot SetAt holds the `SetReplicas` (once it is chosen)
  holds,    \* holds[r]: the slots that replica r holds
  walk,     \* walk[r]: the end of the contiguous prefix that r walked
  read,     \* read[q]: the state of the read q of Asks
  misroute  \* a `Commit` went to a replica that the fold does not name there

vars == <<chosen, setVal, holds, walk, read, misroute>>

\* The `SetReplicas` is chosen.
Reseated == SetAt \in chosen /\ setVal

\* The set in force, and every replica that a set named so far.
InForce == IF Reseated THEN New ELSE Old
WasIn == Old \cup (IF Reseated THEN New ELSE {})

\* The set that the fold names at slot s: `New` above a chosen `SetReplicas`.
FoldRoute(s) == IF s > SetAt /\ Reseated THEN New ELSE Old

\* A removed member that walked the `SetReplicas`: its segment ends there.
Out(r) == Reseated /\ r \in Old \ New /\ walk[r] >= SetAt

TypeOK ==
  /\ chosen \subseteq Slots
  /\ setVal \in BOOLEAN
  /\ holds \in [Reps -> SUBSET Slots]
  /\ walk \in [Reps -> 0..N]
  /\ read \in [Asks -> {"none", "asked", "served", "moved"}]
  /\ misroute \in BOOLEAN

Init ==
  /\ chosen = {}
  /\ setVal = FALSE
  /\ holds = [r \in Reps |-> {}]
  /\ walk = [r \in Reps |-> 0]
  /\ read = [q \in Asks |-> "none"]
  /\ misroute = FALSE

\* Slot s is chosen; at SetAt, the `SetReplicas` (v) or a `Noop` of a new
\* leader.
Choose(s, v) ==
  /\ s \notin chosen
  /\ chosen' = chosen \cup {s}
  /\ setVal' = IF s = SetAt THEN v ELSE setVal
  /\ UNCHANGED <<holds, walk, read, misroute>>

\* The set that the leader routes slot s to, and whether it may route it.
RouteTo(s) == IF Route = "fold" THEN FoldRoute(s)
              ELSE IF s > SetAt THEN New ELSE Old
RouteReady(s) == Route = "proposed" \/ 1..(s - 1) \subseteq chosen

\* The leader's `Commit` of the chosen slot s reaches replica r.
Commit(s, r) ==
  /\ s \in chosen
  /\ RouteReady(s)
  /\ r \in RouteTo(s)
  /\ holds' = [holds EXCEPT ![r] = @ \cup {s}]
  /\ misroute' = (misroute \/ r \notin FoldRoute(s) \/ ~(1..(s - 1) \subseteq chosen))
  /\ UNCHANGED <<chosen, setVal, walk, read>>

\* Replica r pulls the chosen slot s, from the acceptors or a peer.
CatchUp(s, r) ==
  /\ Pull
  /\ s \in chosen
  /\ r \in WasIn
  /\ ~Out(r)
  /\ holds' = [holds EXCEPT ![r] = @ \cup {s}]
  /\ UNCHANGED <<chosen, setVal, walk, read, misroute>>

\* Replica r walks the next slot of its prefix.
Walk(r) ==
  /\ walk[r] + 1 \in holds[r]
  /\ walk' = [walk EXCEPT ![r] = @ + 1]
  /\ UNCHANGED <<chosen, setVal, holds, read, misroute>>

\* A reader asks replica r, which a set named, for slot s.
Ask(q) ==
  /\ read[q] = "none"
  /\ q[1] \in WasIn
  /\ read' = [read EXCEPT ![q] = "asked"]
  /\ UNCHANGED <<chosen, setVal, holds, walk, misroute>>

\* Replica r serves the read from its walked prefix.
Serve(q) ==
  /\ read[q] = "asked"
  /\ q[2] <= walk[q[1]]
  /\ read' = [read EXCEPT ![q] = "served"]
  /\ UNCHANGED <<chosen, setVal, holds, walk, misroute>>

\* A removed member answers a read above the `SetReplicas` with "moved".
Move(q) ==
  /\ Moved
  /\ read[q] = "asked"
  /\ Out(q[1])
  /\ q[2] > SetAt
  /\ read' = [read EXCEPT ![q] = "moved"]
  /\ UNCHANGED <<chosen, setVal, holds, walk, misroute>>

Next ==
  \/ \E s \in Slots, v \in BOOLEAN : Choose(s, v)
  \/ \E s \in Slots, r \in Reps : Commit(s, r) \/ CatchUp(s, r)
  \/ \E r \in Reps : Walk(r)
  \/ \E q \in Asks : Ask(q) \/ Serve(q) \/ Move(q)

Fairness ==
  /\ \A s \in Slots : WF_vars(\E v \in BOOLEAN : Choose(s, v))
  /\ \A s \in Slots, r \in Reps : WF_vars(Commit(s, r)) /\ WF_vars(CatchUp(s, r))
  /\ \A r \in Reps : WF_vars(Walk(r))
  /\ \A q \in Asks : WF_vars(Serve(q)) /\ WF_vars(Move(q))

Spec == Init /\ [][Next]_vars /\ Fairness

\* Sound: a replica holds only chosen slots, walks only slots it holds,
\* and only a replica that a set named holds any; a read is served from
\* the walked prefix, and only a removed member answers "moved", above
\* the `SetReplicas`.
Sound ==
  /\ \A r \in Reps : holds[r] \subseteq chosen
  /\ \A r \in Reps : 1..walk[r] \subseteq holds[r]
  /\ \A r \in Reps \ WasIn : holds[r] = {}
  /\ \A q \in Asks : read[q] = "served" => q[2] <= walk[q[1]]
  /\ \A q \in Asks : read[q] = "moved" => Out(q[1]) /\ q[2] > SetAt

\* ReadOnce: an answered read keeps its answer.
ReadOnce == [][\A q \in Asks : read[q] \in {"served", "moved"} => read'[q] = read[q]]_vars

\* Routed: each `Commit` went to the set that the fold names at its slot,
\* once the slots below it were chosen (R4d).
Routed == ~misroute

\* Heals: every member of the set in force walks the whole segment, and
\* every read that a reader asked gets an answer.
Whole == <>[](\A r \in InForce : walk[r] = N)
Answered == \A q \in Asks : read[q] = "asked" ~> read[q] \in {"served", "moved"}

\* Reaches: the `SetReplicas` is chosen and a removed member answers "moved".
NoMoved == ~\E q \in Asks : read[q] = "moved"
=============================================================================
