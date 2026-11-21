------------------------------ MODULE NodeTrace ------------------------------
(***************************************************************************)
(* The binding of the walks of the `consensus-sim` cluster to the actions  *)
(* of `Node` (#1198 (trace checking), #1306 (trace specs extend the        *)
(* checked model)): `Node`, forced to follow the events that one walk      *)
(* recorded (`crates/sim/consensus-sim/src/cluster/trace.rs`). `l` is the  *)
(* index of the next event.                                                *)
(*                                                                         *)
(* Each event is one action of `Node` with its arguments, so TLC has       *)
(* nothing to guess. The other fields of an event are what the code        *)
(* observed, and the binding asks that they are the model's: the report of *)
(* a promise, the frontier of a win, the read fence of a basis, the index  *)
(* of a served read. Three events are no step when the model already holds *)
(* what they say: a `propose` of a slot whose `Accept` a win or a probe    *)
(* sent, a `learn` of a slot the node learned (the prefix reaches a slot   *)
(* that a handoff payload named), and a `rejoin` of a node that never      *)
(* retired (it lingered). This module defines no step of its own.          *)
(*                                                                         *)
(* Each trace spec of this folder extends this module, and its             *)
(* configuration names the invariants its walk must keep. The walk writes  *)
(* the constants of the run (`TraceAcceptors`, `TraceOwner`, ...), and the *)
(* bounds come from the events. Ballots are the ranks of the code's        *)
(* `(round, node)` ballots in the run, from 1. Nodes, matchmakers and      *)
(* commands are strings; `Noop` is "noop". A watermark, a fence or an      *)
(* index that is none is -1.                                               *)
(***************************************************************************)
EXTENDS Node, Trace, Grid, TLC

VARIABLE l

e == Trace[l]

-----------------------------------------------------------------------------
(* The constants of the run *)

\* The events of one kind.
Events(kind) == {i \in 1..Len(Trace) : Trace[i].event = kind}

RunRestarts == Cardinality(Events("restart"))
RunFaults == Cardinality(Events("rot"))
RunPaged == \E i \in Events("promise") : "upto" \in DOMAIN Trace[i]
RunFloors == Events("compact") \cup Events("trim") \cup Events("fold") # {}
RunProxies == Events("commit") # {}
RunMaxPos ==
  LET P == {Trace[i].high : i \in Events("fold")} \cup {Trace[i].first : i \in Events("fold")}
  IN IF P = {} THEN 0 ELSE Max(P)
RunMatchmaking == TraceMatchmakers # {}
RunReads == Events("basis") \cup Events("applied") # {}

\* The configuration of plain Multi-Paxos: every acceptor.
RunStatic == TraceAcceptors

\* The quorums of configuration c: its majorities, or the grid of
\* `TraceCols` columns and `TraceAcks` acks over its members in row-major
\* order (`TraceLayouts`). A driver may name the column of a round, so a
\* Phase-2 quorum is `acks` acceptors of any column. A ballot that selected
\* no configuration yet has no quorum.
Layout(c) == CHOOSE M \in TraceLayouts : GridMembers(M) = c
RunP1(c) ==
  IF c = {} THEN {} ELSE IF TraceCols = 0 THEN Maj(c) ELSE GridQ1(Layout(c), TraceCols, TraceAcks, 0)
RunP2(c) ==
  IF c = {} THEN {} ELSE IF TraceCols = 0 THEN Maj(c) ELSE GridQ2(Layout(c), TraceCols, TraceAcks)

\* Each grid configuration of the run has one layout, and it tiles its grid
\* (`QuorumSystem::admits`).
ASSUME TraceCols = 0 \/
         \A c \in TraceConfigs \cup {TraceAcceptors} :
           c \in {GridMembers(M) : M \in TraceLayouts} =>
             /\ Cardinality({M \in TraceLayouts : GridMembers(M) = c}) = 1
             /\ GridAdmits(Layout(c), TraceCols, TraceAcks)

-----------------------------------------------------------------------------
(* The events *)

TraceInit == Init /\ l = 1

\* A malformed event is a rejection, not a TLC error.
Has(f) == f \in DOMAIN e

\* Whether an event before the current one is a `name` event that agrees
\* with the current one on each field of `fs`. The walk records some steps
\* once only (the first `propose` of a (ballot, slot)): a second record of such a step is no step of the run.
TracedBefore(name, fs) ==
  \E j \in 1..(l - 1) :
    /\ Trace[j].event = name
    /\ \A f \in fs : f \in DOMAIN Trace[j] /\ f \in DOMAIN e /\ Trace[j][f] = e[f]

IsNode(f) == Has(f) /\ e[f] \in Acceptors
IsMatchmaker == Has("mm") /\ e.mm \in Matchmakers
IsBallot == Has("bal") /\ e.bal \in Ballots
IsSlot == Has("slot") /\ e.slot \in Slots
IsValue == Has("val") /\ e.val \in Values
IsRep == Has("rep") /\ e.rep \in Replicas
IsCtx == Has("ctx") /\ e.ctx \in Ctxs
IsFloor == Has("floor") /\ e.floor \in 1..NS

\* The vote at slot s that a promise reports.
Reported(s) ==
  IF \E r \in e.rep : r.slot = s
  THEN LET r == CHOOSE r \in e.rep : r.slot = s IN [bal |-> r.bal, val |-> r.val]
  ELSE NoVote

\* The report of a promise from `lo` up to `hi` is node a's records there.
ReportHeld(a, lo, hi) ==
  /\ \A r \in e.rep : r.slot \in Slots /\ lo <= r.slot /\ r.slot < hi
  /\ \A s \in Slots : lo <= s /\ s < hi => vote[a][s] = Reported(s)

\* A promise: one page from `from` to the end, that raised the acceptor; or
\* a page up to `upto` (-1: the last page) with paged promises.
TPromise ==
  /\ e.event = "promise" /\ IsNode("acc") /\ IsBallot /\ Has("from") /\ Has("rep")
  /\ e.from \in 0..NS
  /\ IF Has("upto")
     THEN LET hi == IF e.upto < 0 THEN NS ELSE e.upto
          IN /\ hi \in 0..NS
             /\ ReportHeld(e.acc, e.from, hi)
             /\ Page(e.acc, e.bal, e.from, hi)
     ELSE /\ prom[e.acc] < e.bal
          /\ [type |-> "1a", bal |-> e.bal, from |-> e.from] \in msgs
          /\ ReportHeld(e.acc, e.from, NS)
          /\ Page(e.acc, e.bal, e.from, NS)

\* A prepare: `hist` is the code's `H_b`, the model's history less the
\* configurations that only abandoned ballots registered.
TPrepare ==
  /\ e.event = "prepare" /\ IsBallot /\ IsNode("owner") /\ Has("from") /\ Has("hist")
  /\ Owner[e.bal] = e.owner
  /\ e.from = From(e.owner)
  /\ Prepare(e.bal)
  /\ Matchmaking =>
       /\ e.hist \subseteq hist'[e.bal]
       /\ \A c \in hist'[e.bal] \ e.hist : AbandonedIn(e.bal, c, msgs')

TPropose ==
  /\ e.event = "propose" /\ IsBallot /\ IsSlot /\ IsValue
  /\ ~TracedBefore("propose", {"bal", "slot"})
  /\ \/ Allocate(e.bal, e.slot, e.val)
     \/ Probe(e.bal, e.slot, e.val)
     \/ /\ [type |-> "2a", bal |-> e.bal, slot |-> e.slot, val |-> e.val] \in msgs
        /\ UNCHANGED vars

TLearn ==
  /\ e.event = "learn" /\ IsNode("node") /\ IsSlot /\ IsValue
  /\ IF learned[e.node][e.slot] = e.val THEN UNCHANGED vars ELSE Learn(e.node, e.slot, e.val)

TRejoin ==
  /\ e.event = "rejoin" /\ IsNode("node")
  /\ IF e.node \in retired THEN Rejoin(e.node) ELSE UNCHANGED vars

\* The read basis of a node, with the fence the code holds.
TBasis ==
  /\ e.event = "basis" /\ IsNode("node") /\ IsBallot /\ Has("fence")
  /\ Basis(e.node, e.bal)
  /\ rbasis'[e.node].fence = e.fence

\* A read of a node on its basis, with the basis and the own answer the
\* code holds.
TReadStart ==
  /\ e.event = "readstart" /\ IsNode("node") /\ IsBallot /\ IsCtx /\ Has("fence")
  /\ rbasis[e.node] = [bal |-> e.bal, fence |-> e.fence]
  /\ ReadStart(e.node, e.ctx)
  /\ IF Has("own")
     THEN rds'[e.ctx].answers = {[acc |-> e.node, wm |-> e.own]}
     ELSE rds'[e.ctx].answers = {}

TReadServe ==
  /\ e.event = "readserve" /\ IsNode("node") /\ IsCtx /\ Has("index")
  /\ ReadServe(e.node, e.ctx)
  /\ rds'[e.ctx].index = e.index

\* An answer to a read, with the watermark the code sent.
TReadAck ==
  /\ e.event = "readack" /\ IsNode("acc") /\ Has("ctx") /\ e.ctx \in Ctxs /\ Has("wm")
  /\ Has("fence") /\ e.fence \in -1..NB
  /\ e.wm = VoteWatermark(e.acc)
  /\ ReadAck(e.acc, e.ctx, e.fence)

Step ==
  \/ TPrepare
  \/ TPromise
  \/ e.event = "win" /\ IsBallot /\ Has("front") /\ Win(e.bal) /\ front'[e.bal] = e.front
  \/ TPropose
  \/ e.event = "accept" /\ IsNode("acc") /\ IsBallot /\ IsSlot /\ IsValue
       /\ Accept(e.acc, e.bal, e.slot, e.val)
  \/ e.event = "record" /\ IsNode("node") /\ IsBallot /\ IsSlot /\ IsValue
       /\ Record(e.node, e.bal, e.slot, e.val)
  \/ TLearn
  \/ e.event = "restart" /\ IsNode("node") /\ Has("upto") /\ Has("led")
       /\ e.upto \in 0..NS /\ e.led \in 0..NB
       /\ Restart(e.node, e.upto, e.led)
  \/ e.event = "select" /\ IsBallot /\ Has("cfg") /\ e.cfg \in Configs /\ Select(e.bal, e.cfg)
  \/ e.event = "register" /\ IsMatchmaker /\ IsBallot /\ Register(e.mm, e.bal)
  \/ e.event = "gcask" /\ IsBallot /\ GcAsk(e.bal)
  \/ e.event = "gcack" /\ IsMatchmaker /\ IsBallot /\ GcAck(e.mm, e.bal)
  \/ e.event = "retire" /\ IsNode("node") /\ Shutdown(e.node)
  \/ TRejoin
  \/ e.event = "commit" /\ IsBallot /\ IsSlot /\ IsValue /\ ProxyCommit(e.bal, e.slot, e.val)
  \/ e.event = "fold" /\ IsSlot /\ Has("high") /\ Has("first")
       /\ e.high \in 0..MaxPos /\ e.first \in 0..MaxPos
       /\ Fold(e.slot, e.high, e.first)
  \/ e.event = "compact" /\ IsNode("node") /\ IsFloor /\ Compact(e.node, e.floor)
  \/ e.event = "trim" /\ IsNode("node") /\ IsFloor /\ Trim(e.node, e.floor)
  \/ e.event = "rot" /\ IsNode("node") /\ IsSlot /\ IsBallot /\ Rot(e.node, e.slot, e.bal)
  \/ e.event = "hole" /\ IsNode("node") /\ Has("slot") /\ e.slot \in -1..(NS - 1)
       /\ Hole(e.node, e.slot)
  \/ e.event = "rlearn" /\ IsRep /\ IsBallot /\ IsSlot /\ IsValue
       /\ RLearn(e.rep, e.bal, e.slot, e.val)
  \/ e.event = "rapply" /\ IsRep /\ IsSlot /\ RApply(e.rep, e.slot)
  \/ e.event = "rcompact" /\ IsRep /\ IsFloor /\ RCompact(e.rep, e.floor)
  \/ e.event = "rtrim" /\ IsRep /\ IsFloor /\ RTrim(e.rep, e.floor)
  \/ e.event = "rboot" /\ IsRep /\ Has("from") /\ e.from \in 0..NS /\ RBoot(e.rep, e.from)
  \/ e.event = "rread" /\ IsRep /\ IsCtx /\ RRead(e.rep, e.ctx)
  \/ e.event = "rserve" /\ IsRep /\ IsCtx /\ Has("index") /\ e.index \in -1..(NS - 1)
       /\ RServe(e.rep, e.ctx, e.index)
  \/ TReadAck
  \/ TBasis
  \/ TReadStart
  \/ e.event = "readfold" /\ IsNode("node") /\ IsNode("acc") /\ IsCtx
       /\ ReadFold(e.node, e.acc, e.ctx)
  \/ TReadServe
  \/ e.event = "applied" /\ IsSlot /\ Applied(e.slot)

TraceNext == l <= Len(Trace) /\ Step /\ l' = l + 1

TraceSpec == TraceInit /\ [][TraceNext]_<<l, vars>>

\* Violated exactly when a behaviour matched every event: the trace is
\* accepted.
TraceUnfinished == l <= Len(Trace)
=============================================================================
