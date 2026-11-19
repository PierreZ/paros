----------------------------- MODULE DecreeTrace -----------------------------
(***************************************************************************)
(* The trace spec of `Decree`: the model, forced to follow the events that *)
(* one walk of `consensus-sim` recorded (#1198 (trace checking)). `l` is   *)
(* the index of the next event. Each event is one step of `Decree` with    *)
(* its arguments, so TLC has nothing to guess and no hidden step.          *)
(*                                                                         *)
(* The walk records a step only when it changes the state of the model:   *)
(*   - "prepare": a proposer opens a ballot (a re-send is not a step);     *)
(*   - "promise": an acceptor raised its promise and reported its vote;    *)
(*   - "propose": the driver sent the `Accept` of a ballot that won        *)
(*     Phase 1, with the value it sent;                                    *)
(*   - "accept": an acceptor durably voted for a new (ballot, value);      *)
(*   - "chosen": a proposer reported a value chosen, with its ballot.      *)
(* A crash and a restart are not steps: an acceptor's state is its disk,   *)
(* and a proposer's decree dies with it (its ballot takes no more steps).  *)
(*                                                                         *)
(* Ballots are the ranks of the code's `(round, node)` ballots in the run, *)
(* from 1, so the order is the same. Acceptors and values are strings.     *)
(***************************************************************************)
EXTENDS Decree, Trace, Naturals, Sequences, FiniteSets

VARIABLE l

e == Trace[l]

\* The quorums of the run: the majorities of its acceptors.
TraceQuorums == {Q \in SUBSET TraceAcceptors : 2 * Cardinality(Q) > Cardinality(TraceAcceptors)}

TraceInit == Init /\ l = 1

\* A malformed event is a rejection, not a TLC error.
IsAcceptor == "acc" \in DOMAIN e /\ e.acc \in Acceptors
IsBallot == "bal" \in DOMAIN e /\ e.bal \in Ballots
IsValue == "val" \in DOMAIN e /\ e.val \in Values

Step ==
  \/ /\ e.event = "prepare" /\ IsBallot
     /\ Prepare(e.bal)
  \* The promise carries what the acceptor reports: it must be the vote
  \* that the model's acceptor holds.
  \/ /\ e.event = "promise" /\ IsAcceptor /\ IsBallot
     /\ Promise(e.acc, e.bal)
     /\ e.vbal = vbal[e.acc] /\ e.vval = vval[e.acc]
  \/ /\ e.event = "propose" /\ IsBallot /\ IsValue
     /\ Propose(e.bal)
     /\ [type |-> "2a", bal |-> e.bal, val |-> e.val] \in msgs'
  \/ /\ e.event = "accept" /\ IsAcceptor /\ IsBallot /\ IsValue
     /\ Accept(e.acc, e.bal)
     /\ vval'[e.acc] = e.val
  \/ /\ e.event = "chosen" /\ IsBallot /\ IsValue
     /\ ChosenAt(e.bal, e.val)
     /\ UNCHANGED vars

TraceNext == l <= Len(Trace) /\ Step /\ l' = l + 1

TraceSpec == TraceInit /\ [][TraceNext]_<<l, vars>>

\* Violated exactly when a behaviour matched every event: the trace is
\* accepted.
TraceUnfinished == l <= Len(Trace)
=============================================================================
