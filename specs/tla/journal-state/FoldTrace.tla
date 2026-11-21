------------------------------ MODULE FoldTrace ------------------------------
(***************************************************************************)
(* The trace spec of the journal fold: the fold of `Journal.tla` (the      *)
(* operators `Res`, `Step`, `Bound`, `CoversOn` and `ResolveOn`), forced   *)
(* to follow the events that one fold walk of `journal-sim` recorded       *)
(* (#1324 (paros-journal), `crates/sim/journal-sim/src/fold.rs`). `l` is   *)
(* the index of the next event.                                            *)
(*                                                                         *)
(* In `Journal.tla` the fold is a function of the chosen log: `Apply`      *)
(* folds the next chosen slot of a replica with `Step`, answers it with    *)
(* `Res`, and a replica of the next segment starts from `Bound` of the     *)
(* fold at the seal. This spec applies those same operators, one event at  *)
(* a time, and defines no rule of its own (#1306 (trace specs extend the   *)
(* checked model)). `st` is the fold of the segment in force, `base` the   *)
(* boundary state it started from, `segs` the sealed segments (each with   *)
(* its base and its last fold), `belows` the writes answered `Below`.      *)
(*   - "apply": the fold applied the command of the event (`op` "noop",    *)
(*     "w", "tr" or "sl"), answered `res` and moved to the state           *)
(*     (`next`, `first`, `leader`) of the event (`Apply`).                 *)
(*   - "seal": the segment in force is sealed; the next segment folds from *)
(*     its boundary state (`BoundWrite` then `TakeBase`).                  *)
(*   - "resolve": a replica of sealed segment `seg` answered `res` to the  *)
(*     `Resolve` of a write answered `Below` (`ResolveBelow`).             *)
(* A write of the code holds one record here, as in the model.             *)
(***************************************************************************)
EXTENDS Journal, Trace, TLC

VARIABLES l, st, base, segs, belows

tvars == <<l, st, base, segs, belows>>

e == Trace[l]
Has(f) == f \in DOMAIN e

\* The names of the trace: "none" is multi-writer, "unset" no leader.
Lv(x) == IF x = "none" THEN None ELSE IF x = "unset" THEN Unset ELSE x

Cmd(ev) ==
  CASE ev.op = "noop" -> Noop
    [] ev.op = "w" -> [op |-> "w", u |-> Lv(ev.u), e |-> ev.e, id |-> ev.id]
    [] ev.op = "tr" -> [op |-> "tr", u |-> Lv(ev.u), up |-> ev.up]
    [] ev.op = "sl" -> [op |-> "sl", new |-> Lv(ev.new), old |-> Lv(ev.old)]

\* The state the event observed.
Observed(s) ==
  /\ Has("next") /\ Has("first") /\ Has("leader")
  /\ s.next = e.next /\ s.first = e.first /\ s.leader = Lv(e.leader)

\* The bounds of the model: one grid, one replica, no slot. The fold uses
\* none of them.
TraceCfg == [g \in {1} |-> {"a"}]
TraceQ == [g \in {1} |-> {{"a"}}]
TraceRep == [g \in {1} |-> {"r"}]
TraceValues == [s \in {0} |-> {}]
TraceAcceptors == {"a"}
TraceGenOK == {}
\* The leader of the fresh journal: the trace defines `TraceFresh`.
TraceL0 == Lv(TraceFresh)

TraceInit ==
  /\ Init
  /\ l = 1 /\ st = Empty /\ base = Empty /\ segs = <<>> /\ belows = {}

TraceStep ==
  \/ /\ e.event = "apply" /\ Has("op") /\ Has("res")
     /\ Res(base, st, Cmd(e)) = e.res
     /\ st' = Step(base, st, Cmd(e))
     /\ Observed(st')
     /\ belows' = IF e.res = "below" THEN belows \cup {Cmd(e)} ELSE belows
     /\ UNCHANGED <<base, segs>>
  \/ /\ e.event = "seal"
     /\ segs' = Append(segs, [base |-> base, st |-> st])
     /\ base' = Bound(st)
     /\ st' = Bound(st)
     /\ Observed(st')
     /\ UNCHANGED belows
  \/ /\ e.event = "resolve" /\ Has("seg") /\ Has("res") /\ e.op = "w"
     /\ e.seg \in 1..Len(segs)
     /\ Cmd(e) \in belows
     /\ CoversOn(segs[e.seg].base, segs[e.seg].st, e.e)
     /\ ResolveOn(segs[e.seg].st, Cmd(e)) = e.res
     /\ UNCHANGED <<st, base, segs, belows>>

TraceNext == l <= Len(Trace) /\ TraceStep /\ l' = l + 1 /\ UNCHANGED vars

TraceSpec == TraceInit /\ [][TraceNext]_<<tvars, vars>>

\* Violated exactly when a behaviour matched every event: the trace is
\* accepted.
TraceUnfinished == l <= Len(Trace)
=============================================================================
