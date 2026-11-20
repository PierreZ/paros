--------------------------- MODULE TruncationTrace ---------------------------
(***************************************************************************)
(* The trace spec of the compaction floor of `Truncation`, on a walk of    *)
(* the `consensus-sim` cluster with truncation (#1231 (traced walks reach  *)
(* the rules),                                                             *)
(* `crates/sim/consensus-sim/src/cluster/trace/truncation.rs`).            *)
(*                                                                         *)
(* The log, Phase 1, Phase 2, the learns and the restarts are              *)
(* `MultiPaxosTrace` (linked from `../multi-paxos/`): this module extends  *)
(* it and adds the floor of each acceptor and the journal fold. The steps  *)
(* are the rules of `Truncation.tla` for the floor:                        *)
(*   - "fold": the first party that applied a slot names the end of the    *)
(*     records of the slot (`high`, 0 when it holds none) and `first_seq`  *)
(*     after the slot (`first`). The slots come in order. The fold is a    *)
(*     function of the chosen log, so one fold serves every node;          *)
(*   - "compact": a node moved its floor to `floor` (`Compact`). It holds  *)
(*     every slot below the floor (`CompactWhen` = "prefix"), the floor    *)
(*     goes up, and each record below the floor is below `first_seq` once  *)
(*     the node folded its prefix (`FloorReleased`). A retained retry may  *)
(*     hold the floor lower than the fold allows; it is never higher;      *)
(*   - "trim": a node jumped to the floor of a peer (`TrimTo`). The floor  *)
(*     goes up, some peer holds a floor at least as high, and the jump     *)
(*     moves no promise (`TrimPromise` = "keep"): it is no step of the     *)
(*     log.                                                                *)
(* A floor drops the votes and the learned values below it. The steps of   *)
(* the log take the floor guards: a promise asks from at or above the      *)
(* floor (`FloorGuard` = "nack"), and a vote, a record and a learn are at  *)
(* or above it (`AcceptOutcome::BelowFloor`, `mark_chosen`). A restart     *)
(* proves its prefix with the slots the node holds. A campaign             *)
(* asks from the first slot its owner does not hold (`FirstUnchosen`).     *)
(*                                                                         *)
(* `FloorChosen` is the property of `Truncation`: each slot below a floor  *)
(* is chosen. The faulty entries of CTRL are not traced: the walk of this  *)
(* spec rots no disk.                                                      *)
(***************************************************************************)
EXTENDS MultiPaxosTrace, Integers

VARIABLES floor, \* acceptor: the first slot it holds
          fold   \* the fold of the slots 0..Len(fold)-1, in order

floorVars == <<floor, fold>>

\* The fold of slot s.
F(s) == fold[s + 1]

\* Node n holds slot s: it learned it, or the slot is below its floor.
Holds(n, s) == s < floor[n] \/ learned[n][s] # None

\* Each record below floor f is below `first_seq` once node n folded every
\* slot it holds from 0, the slots below f among them.
Released(n, f) ==
  /\ f <= Len(fold)
  /\ \E t \in Slots :
       /\ t < Len(fold)
       /\ \A u \in 0..t : Holds(n, u)
       /\ \A s \in 0..(f - 1) : F(s).high <= F(t).first

\* Node n's votes and learned values at and above floor f.
Above(f, m) == [s \in Slots |-> IF s < f THEN NoVote ELSE m[s]]
Learned(f, m) == [s \in Slots |-> IF s < f THEN None ELSE m[s]]

\* Node n moves its floor to f.
Raise(n, f) ==
  /\ floor' = [floor EXCEPT ![n] = f]
  /\ vote' = [vote EXCEPT ![n] = Above(f, @)]
  /\ dvote' = [dvote EXCEPT ![n] = Above(f, @)]
  /\ learned' = [learned EXCEPT ![n] = Learned(f, @)]
  /\ UNCHANGED <<prom, dprom, phase, next, front, wiped, restarts, msgs, fold>>

TruncInit == TraceInit /\ floor = [a \in Acceptors |-> 0] /\ fold = <<>>

\* The floor guards of the steps of the log.
Guarded ==
  /\ e.event = "promise" => e.from >= floor[e.acc]
  /\ e.event \in {"accept"} => e.slot >= floor[e.acc]
  /\ e.event \in {"record", "learn"} => e.slot >= floor[e.node]

\* The first slot node n does not hold: where its campaign starts
\* (`first_unchosen`), never below its floor.
FirstUnheld(n) ==
  IF \A s \in Slots : Holds(n, s) THEN NS
  ELSE CHOOSE s \in Slots : ~Holds(n, s) /\ \A t \in Slots : t < s => Holds(n, t)

\* `Prepare` of `MultiPaxos`, from the slot `e.from`.
OpenPhase1 ==
  /\ e.event = "prepare" /\ IsBallot /\ IsNode("owner") /\ Has("from")
  /\ Owner[e.bal] = e.owner
  /\ LET b == e.bal
         o == e.owner
     IN /\ phase[b] = "idle"
        /\ Live(o)
        /\ prom[o] < b
        /\ phase' = [phase EXCEPT ![b] = "p1"]
        /\ prom' = [prom EXCEPT ![o] = b]
        /\ dprom' = [dprom EXCEPT ![o] = Persisted(dprom[o], b)]
        /\ SendAll({[type |-> "1a", bal |-> b, from |-> e.from], PromiseMsg(o, b, e.from)})
        /\ UNCHANGED <<vote, dvote, learned, next, front, wiped, restarts>>
  /\ UNCHANGED floorVars

\* `Prepare` of `MultiPaxos`, from the first slot the owner does not hold.
TPrepare == OpenPhase1 /\ e.from = FirstUnheld(e.owner)

LogStep == e.event \notin {"prepare", "restart"} /\ Guarded /\ Step /\ UNCHANGED floorVars

\* `TraceRestart` over the slots the node holds: its durable chosen index
\* covers its floor, and a floor drops the learned values below it.
TRestart ==
  /\ e.event = "restart" /\ IsNode("node") /\ Has("upto") /\ Has("led")
  /\ \A s \in Slots : s < e.upto => Holds(e.node, s)
  /\ Reboot(e.node, e.upto, e.led)
  /\ UNCHANGED floorVars

TFold ==
  /\ e.event = "fold" /\ IsSlot /\ Has("high") /\ Has("first")
  /\ e.slot = Len(fold)
  /\ e.first >= IF fold = <<>> THEN 0 ELSE F(Len(fold) - 1).first
  /\ fold' = Append(fold, [high |-> e.high, first |-> e.first])
  /\ UNCHANGED <<vars, floor>>

TCompact ==
  /\ e.event = "compact" /\ IsNode("node") /\ Has("floor")
  /\ LET n == e.node
         f == e.floor
     IN /\ f > floor[n] /\ f <= NS
        /\ \A s \in Slots : s < f => Holds(n, s)
        /\ Released(n, f)
        /\ Raise(n, f)

TTrim ==
  /\ e.event = "trim" /\ IsNode("node") /\ Has("floor")
  /\ LET n == e.node
         f == e.floor
     IN /\ f > floor[n] /\ f <= NS
        /\ \E p \in Acceptors : floor[p] >= f
        /\ Raise(n, f)

TruncNext == l <= Len(Trace) /\ (TPrepare \/ TRestart \/ LogStep \/ TFold \/ TCompact \/ TTrim) /\ l' = l + 1

TruncSpec == TruncInit /\ [][TruncNext]_<<l, vars, floorVars>>

-----------------------------------------------------------------------------
(* Safety *)

FloorType == floor \in [Acceptors -> 0..NS]

\* Each slot below a floor is chosen.
FloorChosen == \A a \in Acceptors, s \in Slots : s < floor[a] => \E v \in Values : Chosen(s, v)
=============================================================================
