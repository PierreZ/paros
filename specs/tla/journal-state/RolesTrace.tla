------------------------------ MODULE RolesTrace ------------------------------
(***************************************************************************)
(* The trace spec of the journal roles: the actions of `Journal.tla`,      *)
(* forced to follow the events that one roles walk of `journal-sim`        *)
(* recorded (#1326 (the journal roles over the core),                      *)
(* `crates/sim/journal-sim/src/roles.rs`). `l` is the index of the next    *)
(* event. Each event is one action of the model, with the parameters that  *)
(* the code chose, and its outputs must be the model's. The spec defines   *)
(* no rule of its own (#1306 (trace specs extend the checked model)).      *)
(*                                                                         *)
(*   "reshape"  `Reshape`: the controller opened segment g.               *)
(*   "register" `Register(b)`: a campaign; its leader writes from `from`. *)
(*   "promise"  `Promise(a, b)`: an acceptor queued a raised promise.      *)
(*   "accept"   `Accept(a, b, s)`: an acceptor queued a vote.              *)
(*   "flush"    `Flush(a)`: the acceptor's first queued batch is durable,  *)
(*              and its reply goes.                                        *)
(*   "crash"    `Crash(a)`: the queue and the memory are lost.             *)
(*   "phase1"   `Phase1On(b, Qs)`: the leader read the complete reports of *)
(*              the acceptors q; a first leader computed `end`.            *)
(*   "seal"     `SealWrite(b)`; "open" `Open(b)` from `start`.             *)
(*   "propose"  `Propose(b, s, v)`.                                        *)
(*   "base"     `TakeBase(r)`; "apply" `Apply(r)` at slot s, answered res; *)
(*   "boundary" `BoundWrite(r)`; "retire" `Retire(p)`;                     *)
(*   "trim"     `Trim(p, r)`: the register took the trim of replica r.     *)
(*   "resolve"  `ResolveBelow(s, r)`, answered res.                        *)
(*                                                                         *)
(* The trace defines the sizes and the grids: `TraceGrids` (the acceptors  *)
(* of each segment, from 1), `TraceSlots`, `TraceBallots`, `TraceCrashes`, *)
(* `TraceFresh` (the leader of the fresh journal) and `TraceValues` (the   *)
(* commands proposed at each slot, from slot 0).                           *)
(***************************************************************************)
EXTENDS Journal, Trace, TLC

VARIABLES l

e == Trace[l]
Has(f) == f \in DOMAIN e

\* The names of the trace: "none" is no uuid or multi-writer, "unset" no
\* leader.
Lv(x) == IF x = "none" THEN None ELSE IF x = "unset" THEN Unset ELSE x

Cmd(ev) ==
  CASE ev.op = "noop" -> Noop
    [] ev.op = "w" -> [op |-> "w", u |-> Lv(ev.u), e |-> ev.e, id |-> ev.id]
    [] ev.op = "tr" -> [op |-> "tr", u |-> Lv(ev.u), up |-> ev.up]
    [] ev.op = "sl" -> [op |-> "sl", new |-> Lv(ev.new), old |-> Lv(ev.old)]

\* The constants of the model, from the definitions of the trace.
TraceAcceptors == UNION {TraceGrids[g] : g \in 1..Len(TraceGrids)}
TraceCfg == [g \in 1..Len(TraceGrids) |-> TraceGrids[g]]
\* A grid of v1 is one column of 2F + 1: its quorums are the majorities.
TraceQ == [g \in 1..Len(TraceGrids) |->
             {Q \in SUBSET TraceGrids[g] : 2 * Cardinality(Q) > Cardinality(TraceGrids[g])}]
\* The replica set of segment g: 10g + 1 to 10g + 3.
TraceRep == [g \in 1..Len(TraceGrids) |-> {10 * g + 1, 10 * g + 2, 10 * g + 3}]
TraceVals == [s \in 0..(TraceSlots - 1) |-> {Cmd(x) : x \in TraceValues[s + 1]}]
TraceGenOK == (1..TraceBallots) \X (1..Len(TraceGrids))
TraceL0 == Lv(TraceFresh)
TraceG == Len(TraceGrids)
\* A run with no crash still queues its writes: the bound is one above the
\* crashes, so the model never takes the `Direct` shortcut.
TraceMaxCrash == TraceCrashes + 1

\* The quorum of each configuration that the leader of b read.
QsOf(b, q) == [h \in bH[b] |-> q \cap CfgOf[h]]

TraceInit == Init /\ l = 1

TraceStep ==
  \/ /\ e.event = "reshape"
     /\ Reshape
     /\ top' = e.g
     /\ CfgOf[e.g] = e.grid
  \/ /\ e.event = "register"
     /\ Register(e.b)
     /\ blo'[e.b] = e.from
  \/ e.event = "promise" /\ Promise(e.a, e.b)
  \/ e.event = "accept" /\ Accept(e.a, e.b, e.s)
  \/ e.event = "flush" /\ Flush(e.a)
  \/ e.event = "crash" /\ Crash(e.a)
  \/ /\ e.event = "phase1"
     /\ Phase1On(e.b, QsOf(e.b, e.q))
     /\ bend'[e.b] = IF Has("end") THEN e.end ELSE None
  \/ e.event = "seal" /\ SealWrite(e.b)
  \/ /\ e.event = "open"
     /\ Open(e.b)
     /\ bstart'[e.b] = e.start
  \/ e.event = "propose" /\ Propose(e.b, e.s, Cmd(e))
  \/ e.event = "base" /\ TakeBase(e.r)
  \/ /\ e.event = "apply"
     /\ Apply(e.r)
     /\ Front(e.r) = e.s
     /\ ChosenVal(e.s) = Cmd(e)
     /\ acked'[e.s] = e.res
  \/ e.event = "boundary" /\ BoundWrite(e.r)
  \/ e.event = "retire" /\ Retire(e.p)
  \/ e.event = "trim" /\ Trim(e.p, e.r)
  \/ /\ e.event = "resolve"
     /\ ResolveBelow(e.s, e.r)
     /\ resolved'[e.s] = e.res

TraceNext == l <= Len(Trace) /\ TraceStep /\ l' = l + 1

TraceSpec == TraceInit /\ [][TraceNext]_<<l, vars>>

\* Violated exactly when a behaviour matched every event: the trace is
\* accepted.
TraceUnfinished == l <= Len(Trace)
=============================================================================
