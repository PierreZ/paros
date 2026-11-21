---------------------------- MODULE RegisterTrace ----------------------------
(***************************************************************************)
(* The trace spec of the journal's matchmaker register: the actions of     *)
(* `SegmentRecords.tla`, forced to follow the events that one register     *)
(* walk of `journal-sim` recorded (#1325 (the matchmaker register as an    *)
(* optional role), `crates/sim/journal-sim/src/register.rs`). `l` is the   *)
(* index of the next event. Each event applies one action of the model and *)
(* binds what the code saw; this spec defines no step of its own (#1306    *)
(* (trace specs extend the checked model)).                                *)
(*   - "read": operation `i` read at the serving majority `q` of           *)
(*     generation `g`, saw the value `seen` and went on ("read") or was    *)
(*     refused ("refused") (`Read`).                                       *)
(*   - "write": member `m` took the write of operation `i`, the value      *)
(*     `val` (`Write`).                                                    *)
(*   - "retire": the old acceptors retired (`Retire`).                     *)
(*   - "drop": a replica dropped its copy (`Drop`).                        *)
(*   - "stop": member `m` froze (`Stop`).                                  *)
(*   - "close": the handover built the successor state from the frozen     *)
(*     majority `q` (`Close`).                                             *)
(*   - "bootstrap": member `m` of the next set stored it (`Bootstrap`).    *)
(*   - "decide": the decree chose it (`Decide`).                           *)
(*   - "activate": member `m` serves the next generation (`Activate`).     *)
(*   - "crash": machine `m` is down for good (`Crash`).                    *)
(* A successor state is the write generation `wop` and the value `val` of  *)
(* the highest write, and the read generation `rg`.                        *)
(***************************************************************************)
EXTENDS SegmentRecords, Trace, TLC

VARIABLES l

tvars == <<l>>

e == Trace[l]
Has(f) == f \in DOMAIN e

\* The machines of the walk: X goes, W comes.
TraceM0 == {"X", "Y", "Z"}
TraceT == {"Y", "Z", "W"}

Succ(ev) == <<<<ev.wop, ev.val>>, ev.rg>>

TraceInit == Init /\ l = 1

TraceStep ==
  \/ /\ e.event = "read" /\ Has("i") /\ Has("g") /\ Has("q") /\ Has("res")
     /\ e.i \in DOMAIN Ops
     /\ Read(e.i, e.g, e.q)
     /\ ost'[e.i] = e.res
     /\ oseen'[e.i] = e.seen
  \/ /\ e.event = "write" /\ Has("i") /\ Has("m") /\ Has("val")
     /\ e.i \in DOMAIN Ops
     /\ Write(e.i, e.m)
     /\ reg'[e.m] = <<e.i, e.val>>
  \/ e.event = "retire" /\ Retire
  \/ e.event = "drop" /\ Drop
  \/ e.event = "stop" /\ Has("m") /\ Stop(e.m)
  \/ /\ e.event = "close" /\ Has("q")
     /\ Close(e.q)
     /\ Succ(e) \in props'
  \/ e.event = "bootstrap" /\ Has("m") /\ Bootstrap(e.m, Succ(e))
  \/ e.event = "decide" /\ Decide(Succ(e))
  \/ e.event = "activate" /\ Has("m") /\ Activate(e.m)
  \/ e.event = "crash" /\ Has("m") /\ Crash(e.m)

TraceNext == l <= Len(Trace) /\ TraceStep /\ l' = l + 1

TraceSpec == TraceInit /\ [][TraceNext]_<<tvars, vars>>

\* Violated exactly when a behaviour matched every event: the trace is
\* accepted.
TraceUnfinished == l <= Len(Trace)
=============================================================================
