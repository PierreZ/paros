----------------------------- MODULE FaultsTrace -----------------------------
(***************************************************************************)
(* The trace spec of the faulty entries of CTRL of `Truncation`, on a walk *)
(* of the `consensus-sim` cell with disk faults (#1231 (traced walks reach *)
(* the rules), `crates/sim/consensus-sim/src/cell/trace/faults.rs`).       *)
(*                                                                         *)
(* This module extends `TruncationTrace` (the log, the floor and the fold) *)
(* and adds the rules of `Truncation.tla` for a faulty entry:              *)
(*   - "rot": a boot read the record of a slot faulty (`Rot`). The record  *)
(*     keeps its ballot and loses its value (`Lost`), on disk and in       *)
(*     memory. Each boot reads it again until a vote heals it. A record    *)
(*     that a scrub wrote again before the boot is no step. `Rot` of       *)
(*     `Truncation` also drops the learned value of the slot; the code     *)
(*     keeps its durable chosen index over the slot, and only its fold     *)
(*     stops at the hole until a peer brings the value back                *)
(*     (`Replica::fold_hole`). So the node still holds the slot as         *)
(*     chosen, and it may compact below it;                                *)
(*   - "hole": the fold of a node stops at a slot of its prefix that a     *)
(*     boot read faulty, or runs to the end (`Replica::fold_hole`). A      *)
(*     campaign starts at the hole (`open_campaign`): the promises of its  *)
(*     Phase 1 bring the value back. The model has no fold: this is the    *)
(*     trace spec's own step;                                              *)
(*     The trace spec keeps the learned value, which no step sends;        *)
(*   - "promise": a page of a promise (`Limits::promise_page`). It reports *)
(*     the votes from `from` up to `upto` (-1: the last page), a faulty    *)
(*     record as a vote of `Lost`. The first page answers a `1a` and       *)
(*     raises the promise; a continuation goes on from the end of a page   *)
(*     before it, at the same promise. The promise of `Truncation` is one  *)
(*     message. Here each page sends the answer so far: a `1b` with the    *)
(*     votes of each page as that page saw them, up to `upto` (`NS` once   *)
(*     the last page is sent). This is the trace spec's own step. An       *)
(*     acceptor may answer again at the ballot it promised                 *)
(*     (`Acceptor::prepare` re-affirms it): the repair probe asks the      *)
(*     acceptors that did not answer, and a dropped answer is asked again. *)
(*     Each answer is a promise of its own. A page from past the first     *)
(*     slot of the campaign that goes on from no page is the answer to a   *)
(*     keep-alive (`Election::keep_alive_from`): the tally never counts    *)
(*     it, so it sends nothing, and it raises the promise;                 *)
(*   - "win": `Win` of `Truncation`, with the tri-state tally and the     *)
(*     pages: the tally reads one answer so far of each acceptor, and the  *)
(*     acceptors whose answer is complete hold a Phase-1 quorum. The code  *)
(*     merges every page it takes (`Proposer::fold_promise`), and counts   *)
(*     an acceptor only after its last page: a vote that a page reports is *)
(*     a vote, whoever sent it. A slot whose complete answers do not       *)
(*     decide it (`Decidable`) is not proposed again; it goes to the       *)
(*     repair probe;                                                       *)
(*   - "propose" of a slot below the frontier that `Win` did not propose:  *)
(*     `Probe` of `Truncation`. More promises of the same ballot decide    *)
(*     the slot, and the leader proposes the highest readable report or a  *)
(*     `Noop`. A probe that ends because the leader learned the slot sends *)
(*     nothing, and is no step.                                            *)
(* A vote over a faulty record heals it: `Accept` and `record` write the   *)
(* slot again.                                                             *)
(*                                                                         *)
(* `NoFillChosen` is the property of the tri-state tally: a leader never   *)
(* proposes a `Noop` at a slot where a lower ballot chose a command.       *)
(***************************************************************************)
EXTENDS TruncationTrace

VARIABLES rotted, \* node: the slots of its prefix that a boot read faulty since it booted
          hole    \* node: the slot its fold stops at, -1 for none

faultVars == <<rotted, hole>>

Lost == "lost"
Readable(v) == v.bal > 0 /\ v.val # Lost
Faulty(v) == v.bal > 0 /\ v.val = Lost

\* The end of the page: `NS` for the last page.
Upto == IF e.upto < 0 THEN NS ELSE e.upto

\* The end of the answer so far that a `1b` m holds: the owner's own
\* promise (`PromiseMsg`) is complete.
Up(m) == IF "upto" \in DOMAIN m THEN m.upto ELSE NS

\* The first slot of the campaign of ballot b.
From(b) == (CHOOSE m \in msgs : m.type = "1a" /\ m.bal = b).from

FPromise ==
  /\ e.event = "promise" /\ IsNode("acc") /\ IsBallot /\ Has("from") /\ Has("upto") /\ Has("rep")
  /\ LET a == e.acc
         b == e.bal
         lo == e.from
         hi == Upto
         first == IF [type |-> "1a", bal |-> b, from |-> lo] \in msgs /\ prom[a] <= b
                  THEN {[type |-> "1b", acc |-> a, bal |-> b, from |-> lo, upto |-> lo,
                         rep |-> [s \in Slots |-> NoVote]]}
                  ELSE {}
         cont == {m \in msgs : m.type = "1b" /\ m.acc = a /\ m.bal = b /\ Up(m) = lo /\ prom[a] = b}
     IN /\ Live(a)
        /\ lo >= floor[a] /\ lo <= hi
        /\ \A r \in e.rep : r.slot \in Slots /\ lo <= r.slot /\ r.slot < hi
        /\ \A s \in Slots : lo <= s /\ s < hi => vote[a][s] = Reported(s)
        /\ \/ \E m \in first \cup cont :
              Send([m EXCEPT !.upto = hi,
                             !.rep = [s \in Slots |-> IF lo <= s /\ s < hi THEN vote[a][s] ELSE @[s]]])
           \/ /\ \E m \in msgs : m.type = "1a" /\ m.bal = b /\ m.from < lo
              /\ prom[a] <= b
              /\ UNCHANGED msgs
        /\ prom' = [prom EXCEPT ![a] = b]
        /\ dprom' = [dprom EXCEPT ![a] = Persisted(dprom[a], b)]
  /\ UNCHANGED <<vote, dvote, learned, phase, next, front, wiped, restarts, floorVars>>

\* The answers of ballot b that a tally may read: one for each acceptor.
PromiseSets(b) ==
  {S \in SUBSET {m \in msgs : m.type = "1b" /\ m.bal = b} :
     \A m, n \in S : m.acc = n.acc => m = n}

\* The acceptors whose answer in S is complete.
Complete(S) == {m.acc : m \in {n \in S : Up(n) = NS}}

\* The highest readable vote that the promises S report at slot s.
RTop(S, s) ==
  LET rep == {m \in S : Readable(m.rep[s])}
  IN IF rep = {} THEN NoVote
     ELSE (CHOOSE m \in rep : \A n \in rep : n.rep[s].bal <= m.rep[s].bal).rep[s]

\* The tri-state tally at slot s: a complete answer qualifies unless it is
\* a faulty report above the highest readable one; the qualifying answers
\* decide the slot when they hold a Phase-1 quorum.
Decidable(S, s) ==
  \/ ~\E m \in S : Faulty(m.rep[s])
  \/ \E Q \in TraceQ1 :
       Q \subseteq Complete({n \in S : ~(Faulty(n.rep[s]) /\ n.rep[s].bal > RTop(S, s).bal)})

\* The value ballot b proposes again at slot s: the highest readable report,
\* or a `Noop` (the gap fill).
Refill(S, b, s) ==
  [type |-> "2a", bal |-> b, slot |-> s,
   val |-> IF RTop(S, s).bal > 0 THEN RTop(S, s).val ELSE Noop]

FWin ==
  /\ e.event = "win" /\ IsBallot /\ Has("front")
  /\ LET b == e.bal
         lo == From(b)
     IN /\ phase[b] = "p1"
        /\ \E S \in PromiseSets(b) :
             /\ Owner[b] \in Complete(S)
             /\ \E Q \in TraceQ1 : Q \subseteq Complete(S)
             /\ LET f == Frontier(S, lo)
                    open == {s \in lo..(f - 1) : ~Decidable(S, s)}
                IN /\ f = e.front
                   /\ front' = [front EXCEPT ![b] = f]
                   /\ next' = [next EXCEPT ![b] = f]
                   /\ SendAll({Refill(S, b, s) : s \in (lo..(f - 1)) \ open})
        /\ phase' = [phase EXCEPT ![b] = "p2"]
  /\ UNCHANGED <<prom, vote, dprom, dvote, learned, wiped, restarts, floorVars>>

FProbe ==
  /\ e.event = "propose" /\ IsBallot /\ IsSlot /\ IsValue
  /\ LET b == e.bal
         s == e.slot
     IN /\ phase[b] = "p2"
        /\ From(b) <= s /\ s < front[b]
        /\ ~\E m \in msgs : m.type = "2a" /\ m.bal = b /\ m.slot = s
        /\ \E S \in PromiseSets(b) :
             /\ \E Q \in TraceQ1 : Q \subseteq Complete(S)
             /\ Decidable(S, s)
             /\ Refill(S, b, s).val = e.val
             /\ Send(Refill(S, b, s))
  /\ UNCHANGED <<prom, vote, dprom, dvote, learned, phase, next, front, wiped, restarts, floorVars>>

FRot ==
  /\ e.event = "rot" /\ IsNode("node") /\ IsSlot /\ IsBallot
  /\ LET n == e.node
         s == e.slot
     IN /\ dvote[n][s].bal = e.bal /\ e.bal > 0
        /\ vote' = [vote EXCEPT ![n][s] = [bal |-> e.bal, val |-> Lost]]
        /\ dvote' = [dvote EXCEPT ![n][s] = [bal |-> e.bal, val |-> Lost]]
  /\ UNCHANGED <<prom, dprom, learned, phase, next, front, wiped, restarts, msgs, floorVars>>

\* The fold of node n stops at e.slot (-1: none): a slot of its prefix
\* whose value a boot lost, until a catch-up brings the value back.
FHole ==
  /\ e.event = "hole" /\ IsNode("node") /\ Has("slot")
  /\ \/ e.slot = -1
     \/ e.slot \in rotted[e.node] /\ e.slot < FirstUnheld(e.node)
  /\ hole' = [hole EXCEPT ![e.node] = e.slot]
  /\ UNCHANGED <<vars, floorVars, rotted>>

\* `Prepare`, from the hole of the owner's fold, or from the first slot it
\* does not hold when its fold has no hole (`Replica::fold_hole`).
FPrepare ==
  /\ OpenPhase1
  /\ e.from = IF hole[e.owner] >= 0 THEN hole[e.owner] ELSE FirstUnheld(e.owner)

FaultsInit ==
  /\ TruncInit
  /\ rotted = [a \in Acceptors |-> {}]
  /\ hole = [a \in Acceptors |-> -1]

\* The steps that leave the fold alone.
Unfolded ==
  \/ FPrepare \/ TFold \/ TCompact \/ TTrim
  \/ e.event \notin {"promise", "win"} /\ LogStep
  \/ FPromise \/ FWin \/ FProbe

FaultsNext ==
  /\ l <= Len(Trace)
  /\ \/ Unfolded /\ UNCHANGED faultVars
     \/ /\ TRestart
        /\ rotted' = [rotted EXCEPT ![e.node] = {}]
        /\ hole' = [hole EXCEPT ![e.node] = -1]
     \/ /\ FRot
        /\ rotted' = [rotted EXCEPT ![e.node] =
                        IF learned[e.node][e.slot] # None THEN @ \cup {e.slot} ELSE @]
        /\ UNCHANGED hole
     \/ FHole
  /\ l' = l + 1

FaultsSpec == FaultsInit /\ [][FaultsNext]_<<l, vars, floorVars, faultVars>>

-----------------------------------------------------------------------------
(* Safety *)

\* No fill of a chosen slot: a leader never proposes a `Noop` at a slot
\* where a lower ballot chose a command.
NoFillChosen ==
  \A m \in msgs :
    (m.type = "2a" /\ m.val = Noop) =>
      ~\E b \in 1..(m.bal - 1), v \in Commands : ChosenAt(b, m.slot, v)
=============================================================================
