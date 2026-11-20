---------------------------- MODULE HandoverTrace ----------------------------
(***************************************************************************)
(* The trace spec of `Generations`: the model, forced to follow the events *)
(* that one `handover` walk of the `consensus-sim` cluster recorded (#1231 *)
(* (traced walks reach the rules),                                         *)
(* `crates/sim/consensus-sim/src/cluster/trace/handover.rs`). `l` is the   *)
(* index of the next event. The trace spec defines no step of its own      *)
(* (#1306 (trace specs extend the checked model)):                         *)
(*   - "stop": a member of generation `gen` that serves it freezes         *)
(*     (`Stop`);                                                           *)
(*   - "boot": a member of a set proposed for generation `gen` holds its   *)
(*     bootstrap, the floor `wm` and the registrations `regs` (`Boot`);    *)
(*   - "vote": a member of `gen` votes at decree ballot `bal` for `set`    *)
(*     (`Vote`);                                                           *)
(*   - "activate": a member of the set chosen for `gen` serves it, with    *)
(*     the state of its bootstrap (`Activate`);                            *)
(*   - "register" and "gcack": a matchmaker that serves its generation     *)
(*     registers a ballot or raises its floor (`Register`, `GcAck`).       *)
(* The walk's sibling journal is its own: the trace holds one journal.     *)
(***************************************************************************)
EXTENDS Generations, Trace, Sequences, TLC

VARIABLE l

e == Trace[l]
Has(f) == f \in DOMAIN e

\* Every set of the pool may be a target.
TraceTargets == SUBSET TraceMatchmakers \ {{}}

TraceInit == Init /\ l = 1

IsMatchmaker == Has("mm") /\ e.mm \in Matchmakers
IsGen == Has("gen") /\ e.gen \in Gens
IsSet == Has("set") /\ e.set \in Targets
IsBallot == Has("bal") /\ e.bal \in Ballots
IsState == Has("wm") /\ e.wm \in Ballots /\ Has("regs") /\ e.regs \subseteq Ballots

Step ==
  \/ /\ e.event = "stop" /\ IsMatchmaker /\ IsGen
     /\ gen[e.mm] = e.gen
     /\ Stop(e.mm)
  \/ /\ e.event = "boot" /\ IsMatchmaker /\ IsGen /\ IsSet /\ IsState
     /\ Boot(e.mm, e.gen, e.set, e.wm, e.regs)
  \/ /\ e.event = "vote" /\ IsMatchmaker /\ IsGen /\ IsSet /\ Has("bal") /\ e.bal \in Decrees
     /\ Vote(e.mm, e.gen, e.bal, e.set)
  \/ /\ e.event = "activate" /\ IsMatchmaker /\ IsGen /\ IsSet /\ IsState
     /\ Activate(e.mm, e.gen, e.set, e.wm, e.regs)
  \/ /\ e.event = "register" /\ IsMatchmaker /\ IsBallot
     /\ Register(e.mm, e.bal)
  \/ /\ e.event = "gcack" /\ IsMatchmaker /\ IsBallot
     /\ GcAck(e.mm, e.bal)

TraceNext == l <= Len(Trace) /\ Step /\ l' = l + 1

TraceSpec == TraceInit /\ [][TraceNext]_<<l, vars>>

\* Violated exactly when a behaviour matched every event: the trace is
\* accepted.
TraceUnfinished == l <= Len(Trace)
=============================================================================
