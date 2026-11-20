------------------------- MODULE LeaderHandoffTrace -------------------------
(***************************************************************************)
(* The trace spec of `LeaderHandoff`: the model, forced to follow the      *)
(* events that one walk of the `consensus-sim` cluster recorded (#1198     *)
(* (trace checking),                                                       *)
(* `crates/sim/consensus-sim/src/cluster/trace/leader_handoff.rs`). `l` is *)
(* the index of the next event.                                            *)
(*                                                                         *)
(* The walk records a step when a batch of a node reached its disk, and    *)
(* only when the step changes the model's state:                           *)
(*   - "prepare", "promise", "win", "accept", "restart": as in             *)
(*     `multi-paxos/MultiPaxosTrace.tla`;                                  *)
(*   - "propose": node `by` sent the first `Accept` of a (ballot, slot): a *)
(*     fresh command at its frontier (`Allocate`), or a slot that `Win` or *)
(*     `Install` already sent;                                             *)
(*   - "relinquish": the leader handed its ballot to a successor, with the *)
(*     payload it sent (`Relinquish`); the payload must be the model's;    *)
(*   - "install": the successor installed the ballot (`Install`).          *)
(*                                                                         *)
(* The model opens the rounds of a recovery and records them as the        *)
(* leader's votes in one step (`Win`, `Install`); the code sends them in   *)
(* pages (`advance_recovery`). So an "accept" of the leader's own round    *)
(* that the model already holds is no step.                                *)
(*                                                                         *)
(* The other events are steps of the model too: "win" is `Win`, which      *)
(* skips the slots the owner learned (the event's `known` must be those    *)
(* slots); "stepdown" is `StepDown`; "learn" is `Learn` (a slot learned    *)
(* again, by `Install` or by the prefix that reaches a slot of a payload,  *)
(* is no step); "record" is `Record`; "restart" is `Restart` with the      *)
(* prefix `upto` that the chosen index kept. The trace spec defines no     *)
(* step of its own (#1306 (trace specs extend the checked model)).         *)
(*                                                                         *)
(* No message is lost (`Drops` is 0): the walk drops messages, and a       *)
(* message that the model holds but the code never delivers only takes    *)
(* behaviours away. Ballots are the ranks of the code's `(round, node)`    *)
(* ballots in the run, from 1. Nodes and commands are strings; `Noop` is   *)
(* "noop".                                                                 *)
(***************************************************************************)
EXTENDS LeaderHandoff, Trace, Grid, Naturals, Sequences, FiniteSets, TLC

VARIABLE l

e == Trace[l]

TraceQ1 == MajorityQuorums(TraceAcceptors)
TraceQ2 == [s \in 0..(TraceNS - 1) |-> MajorityQuorums(TraceAcceptors)]
TraceBallots == 1..TraceNB

TraceInit == Init /\ l = 1

\* A malformed event is a rejection, not a TLC error.
Has(f) == f \in DOMAIN e
IsNode(f) == Has(f) /\ e[f] \in Acceptors
IsBallot == Has("bal") /\ e.bal \in Ballots
IsSlot == Has("slot") /\ e.slot \in Slots
IsValue == Has("val") /\ e.val \in Values

\* The vote at slot s that a promise reports.
Reported(s) ==
  IF \E r \in e.rep : r.slot = s
  THEN LET r == CHOOSE r \in e.rep : r.slot = s IN [bal |-> r.bal, val |-> r.val]
  ELSE NoVote

\* A map of the payload, as rounds: the value at each slot it names.
AsRounds(m) ==
  [s \in Slots |-> IF \E r \in m : r.slot = s THEN (CHOOSE r \in m : r.slot = s).val ELSE None]

\* The values that a "win" names as chosen, by slot.
Known == AsRounds(e.known)

Step ==
  \/ /\ e.event = "prepare" /\ IsBallot /\ IsNode("owner") /\ Has("from")
     /\ Owner[e.bal] = e.owner
     /\ FirstUnlearned(e.owner) = e.from
     /\ Prepare(e.bal)
  \/ /\ e.event = "promise" /\ IsNode("acc") /\ IsBallot /\ Has("from") /\ Has("rep")
     /\ Promise(e.acc, e.bal)
     /\ [type |-> "1a", bal |-> e.bal, from |-> e.from] \in Delivered
     /\ \A s \in Slots : s >= e.from => vote[e.acc][s] = Reported(s)
     /\ \A r \in e.rep : r.slot \in Slots /\ r.slot >= e.from
  \/ /\ e.event = "win" /\ IsBallot /\ Has("front") /\ Has("known")
     /\ \A k \in e.known : k.slot \in Slots /\ k.val \in Values
     /\ LET o == Owner[e.bal]
            from == (CHOOSE m \in msgs : m.type = "1a" /\ m.bal = e.bal).from
        IN \A s \in Slots :
             s >= from /\ s < e.front => Known[s] = learned[o][s]
     /\ Win(e.bal)
     /\ nxt'[Owner[e.bal]] = e.front
  \/ /\ e.event = "propose" /\ IsNode("by") /\ IsBallot /\ IsSlot /\ IsValue
     /\ \/ /\ bal[e.by] = e.bal
           /\ nxt[e.by] = e.slot
           /\ e.val \in Commands
           /\ Allocate(e.by, e.val)
        \/ /\ [type |-> "2a", bal |-> e.bal, slot |-> e.slot, val |-> e.val] \in msgs
           /\ UNCHANGED vars
  \/ /\ e.event = "accept" /\ IsNode("acc") /\ IsBallot /\ IsSlot /\ IsValue /\ Has("own")
     /\ \/ /\ Accept(e.acc, e.bal, e.slot)
           /\ vote'[e.acc][e.slot] = [bal |-> e.bal, val |-> e.val]
        \/ /\ e.own
           /\ vote[e.acc][e.slot] = [bal |-> e.bal, val |-> e.val]
           /\ UNCHANGED vars
  \/ /\ e.event = "relinquish" /\ IsNode("from") /\ IsNode("to") /\ IsBallot
     /\ Has("lo") /\ Has("nxt") /\ Has("dec") /\ Has("pen")
     /\ bal[e.from] = e.bal
     /\ Relinquish(e.from, e.to)
     /\ [type |-> "rel", from |-> e.from, to |-> e.to, bal |-> e.bal, lo |-> e.lo,
         nxt |-> e.nxt, dec |-> AsRounds(e.dec), pen |-> AsRounds(e.pen)] \in msgs'
  \/ /\ e.event = "install" /\ IsNode("node") /\ IsBallot
     /\ Install(e.node)
     /\ bal'[e.node] = e.bal
  \/ /\ e.event = "stepdown" /\ IsNode("node")
     /\ StepDown(e.node)
  \/ /\ e.event = "record" /\ IsNode("node") /\ IsBallot /\ IsSlot /\ IsValue
     /\ Record(e.node, e.bal, e.slot, e.val)
  \/ /\ e.event = "learn" /\ IsNode("node") /\ IsSlot /\ IsValue
     /\ IF learned[e.node][e.slot] = e.val THEN UNCHANGED vars ELSE Learn(e.node, e.slot, e.val)
  \/ /\ e.event = "restart" /\ IsNode("node") /\ Has("upto")
     /\ Restart(e.node, e.upto)

TraceNext == l <= Len(Trace) /\ Step /\ l' = l + 1

TraceSpec == TraceInit /\ [][TraceNext]_<<l, vars>>

\* Violated exactly when a behaviour matched every event: the trace is
\* accepted.
TraceUnfinished == l <= Len(Trace)
=============================================================================
