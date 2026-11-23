------------------------------ MODULE RolesTrace ------------------------------
(***************************************************************************)
(* The trace spec of the journal roles: the actions of `Journal.tla`, of   *)
(* the compaction in `JournalCompact.tla` and of CTRL in `JournalCtrl.tla`,*)
(* forced to follow the events that one roles walk of `journal-sim`        *)
(* recorded (#1326 (the journal roles over the core),                      *)
(* `crates/sim/journal-sim/src/roles.rs`). `l` is the index of the next    *)
(* event. Each event is one action of the model, with the parameters that  *)
(* the code chose, and its outputs must be the model's. The spec defines   *)
(* no rule of its own (#1306 (trace specs extend the checked model)).      *)
(*                                                                         *)
(*   "reshape"  `Reshape`: the controller opened segment g.               *)
(*   "register" `RegisterFrom(b, from)`: a campaign from `from`.           *)
(*   "promise"  `Promise(a, b)`: an acceptor queued a raised promise.      *)
(*   "accept"   `Accept(a, b, s)`: an acceptor queued a vote.              *)
(*   "flush"    `Flush(a)`: the acceptor's first queued batch is durable,  *)
(*              and its reply goes.                                        *)
(*   "crash"    `Crash(a)`: the queue and the memory are lost.             *)
(*   "phase1"   `Phase1On(b, Qs)`: the leader read the complete reports of *)
(*              the acceptors q; a first leader computed `end`.            *)
(*   "seal"     `SealWrite(b)`; "open" `Open(b)` from `start`.             *)
(*   "propose"  `Propose(b, s, v)`.                                        *)
(*   "base"     `TakeBase(r)`; "apply" `Apply(r)` at slot s, answered      *)
(*              res, with no res when the fold was blind (a `Commit` of    *)
(*              the leader is a source too once an acceptor dropped s);    *)
(*   "boundary" `BoundWrite(r)`; "retire" `Retire(p)`;                     *)
(*   "trim"     `Trim(p, r)`: the register took the trim of replica r.     *)
(*   "drop"     `Drop(r)`: replica r dropped its copy of a trimmed segment.*)
(*   "resolve"  `ResolveBelow(s, r)`, answered res.                        *)
(*   "compact"  `AccCompact(a, p, x)`: an acceptor queued its compaction.  *)
(*   "compact_replica" `RepCompact(r)`: replica r compacted to slot.       *)
(*   "jump"     `JumpFrom(r, m)`: replica r took the point slot, floor.    *)
(*   "rot"      `Rot(a, s)`: acceptor a booted with its vote at s damaged. *)
(*   "probe"    `ProbeDecide(b, s, Qs)`: a late answer decided the blocked *)
(*              slot s on the qualifying answers q.                        *)
(*   "skip"     `ProbeSkip(b, s, a)`: the late answer of acceptor a      *)
(*              dropped the blocked slot s, so the leader skips it.        *)
(*   "learn"    `LearnChosen(b, s)`: a replica gave the blocked slot s its *)
(*              chosen command.                                            *)
(*   "repair"   `AccRepair(a, s, v, c)`: acceptor a queued the repair of   *)
(*              its faulty vote at s with v, chosen at ballot c.           *)
(*                                                                         *)
(* The trace defines the sizes and the grids: `TraceGrids` (the acceptors  *)
(* of each segment, from 1), `TraceSlots`, `TraceBallots`, `TraceCrashes`, *)
(* `TraceFresh` (the leader of the fresh journal), `TraceRots` (the rots)  *)
(* and `TraceValues` (the commands proposed at each slot, from slot 0).    *)
(***************************************************************************)
EXTENDS JournalCtrl, Trace, TLC

VARIABLES l

e == Trace[l]
Has(f) == f \in DOMAIN e

\* The names of the trace: "none" is no uuid, "unset" no leader, "u<n>" a
\* uuid and "m<n>" the tag of a multi-writer mode.
Lv(x) == IF x = "none" THEN None ELSE IF x = "unset" THEN Unset ELSE x
TraceTags == {x \in STRING : SubSeq(x, 1, 1) = "m"}

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
TraceMaxRot == TraceRots

\* The quorum of each configuration that the leader of b read.
QsOf(b, q) == [h \in bH[b] |-> q \cap CfgOf[h]]

TraceInit == Init /\ l = 1

TraceStep ==
  \/ /\ e.event = "reshape"
     /\ Reshape
     /\ top' = e.g
     /\ CfgOf[e.g] = e.grid
  \/ /\ e.event = "register"
     /\ e.from \in LowFrom..HighFrom
     /\ RegisterFrom(e.b, e.from)
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
     \* A `Commit` in flight is a source only once its sender may no
     \* longer be one: a leader's once an acceptor dropped the slot, a
     \* peer's once that peer compacted past it. Before, the slot has a
     \* source of `Apply`.
     /\ \/ Apply(e.r)
        \/ /\ \/ \E a \in Acceptors : e.s \in adrop[a] \cup dadrop[a]
              \/ \E q \in RepOf[GenOf(e.r)] \ {e.r} : rcomp[q] > e.s
           /\ ApplyIf(e.r, TRUE)
     /\ Front(e.r) = e.s
     /\ ChosenVal(e.s) = Cmd(e)
     /\ IF Has("res") THEN ~Blind(e.r) /\ acked'[e.s] = e.res ELSE Blind(e.r)
  \/ e.event = "boundary" /\ BoundWrite(e.r)
  \/ e.event = "retire" /\ Retire(e.p)
  \/ e.event = "trim" /\ Trim(e.p, e.r)
  \/ e.event = "drop" /\ Drop(e.r)
  \/ e.event = "compact" /\ AccCompact(e.a, e.p, e.x)
  \/ /\ e.event = "compact_replica"
     /\ RepCompact(e.r)
     /\ rcomp'[e.r] = e.slot
  \/ /\ e.event = "jump"
     /\ \E m \in trims : m[2] = e.slot /\ m[3] = e.floor /\ JumpFrom(e.r, m)
  \* The walk damages a readable vote, and repairs a faulty one.
  \/ e.event = "rot" /\ dvote[e.a][e.s][2] # None /\ Rot(e.a, e.s)
  \/ e.event = "probe" /\ ProbeDecide(e.b, e.s, QsOf(e.b, e.q))
  \/ e.event = "skip" /\ ProbeSkip(e.b, e.s, e.a)
  \/ /\ e.event = "learn"
     /\ LearnChosen(e.b, e.s)
     /\ IsChosen(e.s)
     /\ ChosenVal(e.s) = Cmd(e)
  \/ /\ e.event = "repair"
     /\ e.c \in Ballots
     /\ AccRepair(e.a, e.s, Cmd(e), e.c)
  \/ /\ e.event = "resolve"
     /\ ResolveBelow(e.s, e.r)
     /\ resolved'[e.s] = e.res

TraceNext == l <= Len(Trace) /\ TraceStep /\ l' = l + 1

TraceSpec == TraceInit /\ [][TraceNext]_<<l, vars>>

\* Violated exactly when a behaviour matched every event: the trace is
\* accepted.
TraceUnfinished == l <= Len(Trace)
=============================================================================
