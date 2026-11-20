--------------------------- MODULE MultiPaxosTrace ---------------------------
(***************************************************************************)
(* The trace spec of `MultiPaxos`: the model, forced to follow the events  *)
(* that one walk of the `consensus-sim` cluster recorded (#1198 (trace     *)
(* checking), `crates/sim/consensus-sim/src/cluster/trace.rs`). `l` is the *)
(* index of the next event.                                                *)
(*                                                                         *)
(* The walk records a step when a batch of a node reached its disk, and    *)
(* only when the step changes the model's state:                           *)
(*   - "prepare": the owner of a ballot opened Phase 1 (`Prepare`);        *)
(*   - "promise": an acceptor raised its promise and reported its votes at *)
(*     or above `from` (`Promise`); the report must be the model's votes;  *)
(*   - "win": the owner won Phase 1, with the frontier of its allocator    *)
(*     (`Win`);                                                            *)
(*   - "propose": the first `Accept` of a (ballot, slot): a fresh command  *)
(*     at the allocator's next slot (`Allocate`), or a slot of the         *)
(*     recovery that `Win` already sent. A fresh command may be a `Noop`:  *)
(*     a settled leader fills its log up to a read's vote watermark with   *)
(*     `Noop`s (`fill_to_watermark`, `node/quorum_reads.rs`). The model's  *)
(*     `Next` allocates only `Commands`; any value is safe at a fresh slot, *)
(*     so the trace spec takes `Allocate` with a `Noop` too;               *)
(*   - "accept": a new durable vote (`Accept`);                            *)
(*   - "record": a node wrote a chosen value as its accepted record, at   *)
(*     the ballot that chose it and below its promise (`mark_chosen`);     *)
(*   - "learn": a slot entered the chosen prefix on a node's disk, or a    *)
(*     leader names a slot it decided in the payload of its handoff;       *)
(*   - "restart": a node booted again from its disk, with the first slot  *)
(*     past the chosen prefix that its disk holds (`upto`).                *)
(*                                                                         *)
(* "learn", "record" and "restart" are the three steps that are not steps *)
(* of the model. The model's `Restart` keeps every learned slot. The code *)
(* writes its chosen index relaxed (`MustSync::Relaxed`): a crash may lose *)
(* the writes that no fsync covered, so a node may boot with a shorter     *)
(* prefix than it learned. The restart step is the model's `Restart` that  *)
(* also forgets every learned slot at or above `upto`, and asks that the   *)
(* node learned every slot below it. Forgetting a chosen value only takes  *)
(* knowledge away: the node learns the slot again, chosen, later.          *)
(*                                                                         *)
(* The code learns a slot from one `Commit`; the model's `CatchUp`        *)
(* copies every slot of a peer at once, and its `Decide` is the leader's   *)
(* learn only. So the trace spec learns one slot, and asks only what the   *)
(* model asks of every learn: the value is chosen (`LearnedChosen`). A     *)
(* slot learned again (the prefix reaches a slot of the payload) is no     *)
(* step. The code also keeps a learned value as the slot's accepted        *)
(* record (`mark_chosen`), which the model does not do. The record's      *)
(* ballot is the ballot of the `Commit` or of the catch-up that taught it, *)
(* which can be above the ballot that chose the value: a later leader     *)
(* proposes the chosen value again at its own ballot, and a node learns   *)
(* it from that leader before a quorum votes at that ballot. So the record *)
(* step asks that a quorum chose the value at the record's ballot or      *)
(* below. That is safe: once v is chosen at c, every ballot above c that  *)
(* has a value in slot s has v (P2c), so the record reports to a later    *)
(* Phase 1 only a value that no other value can replace. The later        *)
(* promises of the node report it.                                        *)
(*                                                                         *)
(* Ballots are the ranks of the code's `(round, node)` ballots in the run, *)
(* from 1. Nodes and commands are strings; `Noop` is "noop".               *)
(***************************************************************************)
EXTENDS MultiPaxos, Trace, Grid, Naturals, Sequences, FiniteSets, TLC

VARIABLE l

e == Trace[l]

\* The quorums of the run: a majority, or the grid of `TraceCols` columns
\* and `TraceAcks` acks over the acceptors in row-major order. A driver may
\* name the column of a round (`propose_in`), so a slot's Phase-2 quorum is
\* `acks` acceptors of any column.
TraceLayout == [i \in 1..Cardinality(TraceAcceptors) |-> "n" \o ToString(i - 1)]
TraceQ1 ==
  IF TraceCols = 0 THEN MajorityQuorums(TraceAcceptors)
  ELSE GridQ1(TraceLayout, TraceCols, TraceAcks, 0)
TraceQ2 ==
  [s \in 0..(TraceNS - 1) |->
     IF TraceCols = 0 THEN MajorityQuorums(TraceAcceptors)
     ELSE GridQ2(TraceLayout, TraceCols, TraceAcks)]

\* A grid of the run tiles its acceptors (`QuorumSystem::admits`).
ASSUME TraceCols = 0 \/ GridAdmits(TraceLayout, TraceCols, TraceAcks)

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

\* A node learns one slot: the trace spec's own step, see above.
\* A slot that a leader learned before its handoff (the payload's decided
\* slots) is learned again when the prefix reaches it: no step.
Learn(n, s, v) ==
  IF learned[n][s] = v THEN UNCHANGED vars
  ELSE /\ learned[n][s] = None
       /\ Chosen(s, v)
       /\ learned' = [learned EXCEPT ![n][s] = v]
       /\ UNCHANGED <<prom, vote, dprom, dvote, phase, next, front, wiped, restarts, msgs>>

\* A node keeps a value chosen at ballot b or below as its record of slot
\* s, at ballot b: the trace spec's own step, see above. It is no vote: it
\* sends no 2b.
Record(n, b, s, v) ==
  /\ \E c \in 1..b : ChosenAt(c, s, v)
  /\ vote' = [vote EXCEPT ![n][s] = [bal |-> b, val |-> v]]
  /\ dvote' = [dvote EXCEPT ![n][s] = [bal |-> b, val |-> v]]
  /\ UNCHANGED <<prom, dprom, learned, phase, next, front, wiped, restarts, msgs>>

\* Node n boots again from its disk, with the chosen prefix below upto: the
\* trace spec's own step, see above. It is `Restart`, and it forgets what a
\* crash lost of the learned prefix. The campaigns of n die, and so does
\* the ballot n led (`led`, 0 for none). The model's `Restart` kills every
\* ballot n owns, but a leader may have handed its ballot on before the
\* crash (the walk's handoff, which this model does not have): that ballot
\* lives on at its successor, and the successor's restart ends it.
\* `Reboot` is the boot; `TraceRestart` adds that n learned the prefix.
Reboot(n, upto, led) ==
  /\ Live(n)
  /\ restarts < Restarts
  /\ restarts' = restarts + 1
  /\ prom' = [prom EXCEPT ![n] = dprom[n]]
  /\ vote' = [vote EXCEPT ![n] = dvote[n]]
  /\ learned' = [learned EXCEPT ![n] = [s \in Slots |-> IF s >= upto THEN None ELSE @[s]]]
  /\ phase' = [b \in Ballots |->
                IF \/ Owner[b] = n /\ phase[b] = "p1"
                   \/ b = led /\ phase[b] = "p2"
                THEN "dead" ELSE phase[b]]
  /\ UNCHANGED <<dprom, dvote, next, front, wiped, msgs>>

TraceRestart(n, upto, led) ==
  /\ \A s \in Slots : s < upto => learned[n][s] # None
  /\ Reboot(n, upto, led)

Step ==
  \/ /\ e.event = "prepare" /\ IsBallot /\ IsNode("owner") /\ Has("from")
     /\ Owner[e.bal] = e.owner
     /\ Prepare(e.bal)
     /\ [type |-> "1a", bal |-> e.bal, from |-> e.from] \in msgs'
  \/ /\ e.event = "promise" /\ IsNode("acc") /\ IsBallot /\ Has("from") /\ Has("rep")
     /\ Promise(e.acc, e.bal)
     /\ [type |-> "1a", bal |-> e.bal, from |-> e.from] \in msgs
     /\ \A s \in Slots : s >= e.from => vote[e.acc][s] = Reported(s)
     /\ \A r \in e.rep : r.slot \in Slots /\ r.slot >= e.from
  \/ /\ e.event = "win" /\ IsBallot /\ Has("front")
     /\ Win(e.bal)
     /\ front'[e.bal] = e.front
  \/ /\ e.event = "propose" /\ IsBallot /\ IsSlot /\ IsValue
     /\ \/ /\ next[e.bal] = e.slot
           /\ Allocate(e.bal, e.val)
        \/ /\ [type |-> "2a", bal |-> e.bal, slot |-> e.slot, val |-> e.val] \in msgs
           /\ UNCHANGED vars
  \/ /\ e.event = "accept" /\ IsNode("acc") /\ IsBallot /\ IsSlot /\ IsValue
     /\ Accept(e.acc, e.bal, e.slot)
     /\ vote'[e.acc][e.slot] = [bal |-> e.bal, val |-> e.val]
  \/ /\ e.event = "record" /\ IsNode("node") /\ IsBallot /\ IsSlot /\ IsValue
     /\ Record(e.node, e.bal, e.slot, e.val)
  \/ /\ e.event = "learn" /\ IsNode("node") /\ IsSlot /\ IsValue
     /\ Learn(e.node, e.slot, e.val)
  \/ /\ e.event = "restart" /\ IsNode("node") /\ Has("upto") /\ Has("led")
     /\ TraceRestart(e.node, e.upto, e.led)

TraceNext == l <= Len(Trace) /\ Step /\ l' = l + 1

TraceSpec == TraceInit /\ [][TraceNext]_<<l, vars>>

\* Violated exactly when a behaviour matched every event: the trace is
\* accepted.
TraceUnfinished == l <= Len(Trace)
=============================================================================
