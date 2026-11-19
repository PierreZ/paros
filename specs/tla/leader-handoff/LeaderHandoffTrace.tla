------------------------- MODULE LeaderHandoffTrace -------------------------
(***************************************************************************)
(* The trace spec of `LeaderHandoff`: the model, forced to follow the      *)
(* events that one walk of the `consensus-sim` cell recorded (#1198 (trace *)
(* checking), `crates/sim/consensus-sim/src/cell/trace/leader_handoff.rs`).*)
(* `l` is the index of the next event.                                     *)
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
(* Four steps of the code are not steps of the model, and the trace spec   *)
(* writes each as its own step:                                            *)
(*   - "win" with `known`: a new leader that already knows a slot of its   *)
(*     recovery chosen (it decided the slot as an earlier leader, above a  *)
(*     gap of its chosen prefix) does not propose it again                 *)
(*     (`close_phase1`). The step is the model's `Win` with no round at    *)
(*     those slots, and asks that each of their values is chosen. A "win"  *)
(*     that knows no such slot is the model's `Win`;                       *)
(*   - "stepdown": a leader stops leading. The code steps down on a beat   *)
(*     at or above its promise (`OnBeat`), on a fence timeout              *)
(*     (`FenceResign`), on a higher promise, on `CheckQuorum` and when a   *)
(*     driver asks (`step_down`). The model has the first two; the walk    *)
(*     records the outcome only. A step down removes behaviours: the node  *)
(*     opens no round, hands nothing on, and drops its open rounds;        *)
(*   - "learn": the code learns one slot from one `Commit`; the model's    *)
(*     `CatchUp` copies a peer's log at once, and its `Decide` is the      *)
(*     leader's learn only. The step asks what the model asks of every     *)
(*     learn: the value is chosen (`LearnedChosen`). A leader learns each  *)
(*     slot that its payload names as decided just before the payload     *)
(*     (the model's `Decide`). A slot learned again (by `Install`, or the  *)
(*     prefix that reaches a slot of the payload) is no step;              *)
(*   - "record": a node keeps a chosen value as its record, below its      *)
(*     promise (`mark_chosen`).                                            *)
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

\* The owner of ballot b wins Phase 1, and its recovery skips each slot
\* that it already knows chosen (`close_phase1` with `is_chosen`): the
\* trace spec's own step, see above. It is the model's `Win` with no round
\* at the slots of K, and asks that each value of K is chosen.
WinKnowing(b, K) ==
  LET o == Owner[b] IN
  /\ \A s \in Slots : K[s] # None => Chosen(s, K[s])
  /\ phase[b] = "p1"
  /\ prom[o] = b
  /\ \E S \in SUBSET {m \in Delivered : m.type = "1b" /\ m.bal = b} :
       /\ \A m, n \in S : m.acc = n.acc => m = n
       /\ o \in {m.acc : m \in S}
       /\ \E Q \in Q1 : Q \subseteq {m.acc : m \in S}
       /\ LET from == (CHOOSE m \in msgs : m.type = "1a" /\ m.bal = b).from
              f == Frontier(S, from)
              R == [s \in Slots |->
                      IF s >= from /\ s < f /\ K[s] = None
                      THEN (IF Top(S, s).bal > 0 THEN Top(S, s).val ELSE Noop)
                      ELSE None]
          IN /\ nxt' = [nxt EXCEPT ![o] = f]
             /\ pend' = [pend EXCEPT ![o] = R]
             /\ vote' = [vote EXCEPT ![o] = OwnVotes(o, b, R)]
             /\ SendAll(RoundMsgs(o, b, R))
  /\ phase' = [phase EXCEPT ![b] = "won"]
  /\ role' = [role EXCEPT ![o] = "leader"]
  /\ bal' = [bal EXCEPT ![o] = b]
  /\ origin' = [origin EXCEPT ![o] = "elected"]
  /\ UNCHANGED <<prom, learned, restarts, drops, lost>>

\* Node n stops leading: the trace spec's own step, see above.
StepDown(n) ==
  /\ role[n] = "leader"
  /\ role' = [role EXCEPT ![n] = "follower"]
  /\ origin' = [origin EXCEPT ![n] = "elected"]
  /\ pend' = [pend EXCEPT ![n] = NoRounds]
  /\ UNCHANGED <<prom, vote, learned, phase, bal, nxt, restarts, drops, lost, msgs>>

\* A node learns one slot: the trace spec's own step, see above.
Learn(n, s, v) ==
  IF learned[n][s] = v THEN UNCHANGED vars
  ELSE /\ learned[n][s] = None
       /\ Chosen(s, v)
       /\ learned' = [learned EXCEPT ![n][s] = v]
       /\ UNCHANGED <<prom, vote, phase, role, bal, origin, nxt, pend, restarts, drops, lost, msgs>>

\* A node keeps the value chosen at ballot b as its record of slot s: the
\* trace spec's own step, see above. It is no vote: it sends no 2b.
Record(n, b, s, v) ==
  /\ ChosenAt(b, s, v)
  /\ vote' = [vote EXCEPT ![n][s] = [bal |-> b, val |-> v]]
  /\ UNCHANGED <<prom, learned, phase, role, bal, origin, nxt, pend, restarts, drops, lost, msgs>>

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
     /\ IF LET from == (CHOOSE m \in msgs : m.type = "1a" /\ m.bal = e.bal).from
           IN \E s \in Slots : Known[s] # None /\ s >= from /\ s < e.front
        THEN WinKnowing(e.bal, Known)
        ELSE Win(e.bal)
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
     /\ Learn(e.node, e.slot, e.val)
  \/ /\ e.event = "restart" /\ IsNode("node")
     /\ Restart(e.node)

TraceNext == l <= Len(Trace) /\ Step /\ l' = l + 1

TraceSpec == TraceInit /\ [][TraceNext]_<<l, vars>>

\* Violated exactly when a behaviour matched every event: the trace is
\* accepted.
TraceUnfinished == l <= Len(Trace)
=============================================================================
