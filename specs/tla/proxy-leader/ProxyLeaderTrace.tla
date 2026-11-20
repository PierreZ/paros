-------------------------- MODULE ProxyLeaderTrace --------------------------
(***************************************************************************)
(* The trace spec of `ProxyLeader`: the model, forced to follow the events *)
(* that one walk of the `consensus-sim` cluster recorded (#1198 (trace     *)
(* checking),                                                              *)
(* `crates/sim/consensus-sim/src/cluster/trace/proxy_leader.rs`). `l` is   *)
(* the index of the next event.                                            *)
(*                                                                         *)
(* The walk records a step when a batch of a node reached its disk, or     *)
(* when a proxy sends a batch, and only when the step changes the model's  *)
(* state:                                                                  *)
(*   - "prepare": the owner of a ballot opened Phase 1. The owner is its   *)
(*     own first acceptor, so this is `Prepare` and the owner's `Promise`  *)
(*     in one step;                                                        *)
(*   - "promise": an acceptor raised its promise and reported its votes at *)
(*     or above `from` (`Promise`); the report must be the model's votes;  *)
(*   - "win": the owner won Phase 1 (`Win`);                               *)
(*   - "propose": the leader picked the value of a slot (`Propose`);       *)
(*   - "delegate": the leader handed the round of a slot to a proxy        *)
(*     (`Delegate`);                                                       *)
(*   - "fanout": a proxy opened the round and fanned it out (`Delegated`); *)
(*   - "accept": a new durable vote (`Accept`);                            *)
(*   - "commit": a proxy decided its round and sent the `Commit`           *)
(*     (`Decide`);                                                         *)
(*   - "crash": a proxy crashed and lost its rounds (`Crash`).             *)
(*                                                                         *)
(* The cluster colocates a leader with an acceptor, and a leader may run  *)
(* a round itself (`Delegation::Colocated`, the leader's take-back): the   *)
(* steps of the model's colocated deployment (`Colocated = TRUE`):         *)
(*   - "direct": the leader sends the `Accept` of its proposed value to    *)
(*     the acceptors itself (`Direct`);                                    *)
(*   - an "accept" with `own`: the leader records the round it opens as    *)
(*     its own vote (`OwnVote`);                                           *)
(*   - "record": a node keeps a value chosen at a ballot as its record,    *)
(*     below its promise (`Record`);                                       *)
(*   - "learn": a node learned a slot. The model has no learner state, so  *)
(*     the event changes nothing, and the trace spec asks that the value   *)
(*     is chosen.                                                          *)
(* The trace spec defines no step of its own (#1306 (trace specs extend    *)
(* the checked model)).                                                    *)
(* A node restart changes nothing here (the promise and the votes are      *)
(* durable), so the walk's restarts are not in the trace.                  *)
(*                                                                         *)
(* The code closes a proxy round on a `Nack` and on an eviction            *)
(* (`expire_stale`), and forgets the oldest rounds it decided. The model   *)
(* leaves these out: a round that stays open decides nothing more. So the  *)
(* walk keeps such a round open in its mirror, and the code's next         *)
(* delegation of it is no step.                                            *)
(*                                                                         *)
(* Ballot 1 is the bootstrap: no node runs it. The model's Phase 1 of a    *)
(* ballot asks the configurations of the lower ballots, and the code's     *)
(* first ballot asks the bootstrap configuration; so the code's ballots    *)
(* are ranked from 2. Each ballot runs the one configuration of the        *)
(* cluster (configuration 0): a cluster without matchmakers never          *)
(* reconfigures.                                                           *)
(***************************************************************************)
EXTENDS ProxyLeader, Trace, Grid, Naturals, Sequences, FiniteSets, TLC

VARIABLE l

e == Trace[l]

\* The quorums of the run: a majority, or the grid of `TraceCols` columns
\* and `TraceAcks` acks over the acceptors in row-major order. A driver may
\* name the column of a round (`propose_in`), so a slot's Phase-2 quorum is
\* `acks` acceptors of any column.
TraceLayout == [i \in 1..Cardinality(TraceAcceptors) |-> "n" \o ToString(i - 1)]
TraceMembers == [k \in {0} |-> TraceAcceptors]
TraceQ1 ==
  [k \in {0} |->
     IF TraceCols = 0 THEN MajorityQuorums(TraceAcceptors)
     ELSE GridQ1(TraceLayout, TraceCols, TraceAcks, 0)]
TraceQ2 ==
  [k \in {0} |->
     [s \in 0..(TraceNS - 1) |->
        IF TraceCols = 0 THEN MajorityQuorums(TraceAcceptors)
        ELSE GridQ2(TraceLayout, TraceCols, TraceAcks)]]
TraceCfg == [b \in 0..TraceNB |-> 0]
\* `Move` is on, so the first proxy of a slot is any proxy.
TraceProxyOf == [s \in 0..(TraceNS - 1) |-> CHOOSE p \in TraceProxies : TRUE]

\* A grid of the run tiles its acceptors (`QuorumSystem::admits`).
ASSUME TraceCols = 0 \/ GridAdmits(TraceLayout, TraceCols, TraceAcks)

TraceInit == Init /\ l = 1

\* A malformed event is a rejection, not a TLC error.
Has(f) == f \in DOMAIN e
IsNode(f) == Has(f) /\ e[f] \in Acceptors
IsProxy == Has("proxy") /\ e.proxy \in Proxies
IsBallot == Has("bal") /\ e.bal \in Ballots
IsSlot == Has("slot") /\ e.slot \in Slots
IsValue == Has("val") /\ e.val \in Commands

\* The vote at slot s that a promise reports.
Reported(s) ==
  IF \E r \in e.rep : r.slot = s
  THEN LET r == CHOOSE r \in e.rep : r.slot = s IN [bal |-> r.bal, val |-> r.val]
  ELSE NoVote

Step ==
  \/ /\ e.event = "prepare" /\ IsBallot /\ IsNode("owner")
     /\ Owner[e.bal] = e.owner
     /\ PrepareOwn(e.bal)
  \/ /\ e.event = "promise" /\ IsNode("acc") /\ IsBallot /\ Has("from") /\ Has("rep")
     /\ Promise(e.acc, e.bal)
     /\ \A s \in Slots : s >= e.from => vote[e.acc][s] = Reported(s)
     /\ \A r \in e.rep : r.slot \in Slots /\ r.slot >= e.from
  \/ /\ e.event = "win" /\ IsBallot
     /\ Win(e.bal)
  \/ /\ e.event = "propose" /\ IsBallot /\ IsSlot /\ IsValue
     /\ Propose(e.bal, e.slot, e.val)
  \/ /\ e.event = "direct" /\ IsBallot /\ IsSlot /\ IsValue
     /\ prop[e.bal][e.slot] = e.val
     /\ Direct(e.bal, e.slot)
  \/ /\ e.event = "delegate" /\ IsBallot /\ IsSlot /\ IsValue /\ IsProxy
     /\ prop[e.bal][e.slot] = e.val
     /\ Delegate(e.bal, e.slot, e.proxy)
  \/ /\ e.event = "fanout" /\ IsBallot /\ IsSlot /\ IsValue /\ IsProxy
     /\ Delegated(e.proxy, [type |-> "del", bal |-> e.bal, slot |-> e.slot,
                            val |-> e.val, proxy |-> e.proxy])
  \/ /\ e.event = "accept" /\ IsNode("acc") /\ IsBallot /\ IsSlot /\ IsValue /\ Has("own")
     /\ IF e.own
        THEN /\ prop[e.bal][e.slot] = e.val
             /\ OwnVote(e.acc, e.bal, e.slot)
        ELSE /\ Accept(e.acc, e.bal, e.slot)
             /\ vote'[e.acc][e.slot] = [bal |-> e.bal, val |-> e.val]
  \/ /\ e.event = "commit" /\ IsBallot /\ IsSlot /\ IsValue /\ IsProxy
     /\ round[e.proxy][e.slot] = [bal |-> e.bal, val |-> e.val]
     /\ Decide(e.proxy, e.slot)
  \/ /\ e.event = "crash" /\ IsProxy
     /\ Crash(e.proxy)
  \/ /\ e.event = "record" /\ IsNode("node") /\ IsBallot /\ IsSlot /\ IsValue
     /\ Record(e.node, e.bal, e.slot, e.val)
  \/ /\ e.event = "learn" /\ IsNode("node") /\ IsSlot /\ IsValue
     /\ Chosen(e.slot, e.val)
     /\ UNCHANGED vars

TraceNext == l <= Len(Trace) /\ Step /\ l' = l + 1

TraceSpec == TraceInit /\ [][TraceNext]_<<l, vars>>

\* Violated exactly when a behaviour matched every event: the trace is
\* accepted.
TraceUnfinished == l <= Len(Trace)
=============================================================================
