-------------------------- MODULE CompartmentsTrace --------------------------
(***************************************************************************)
(* The trace spec of the read replica of `Compartments`, on a walk of the  *)
(* `consensus-sim` cluster with replicas (#1231 (traced walks reach the    *)
(* rules), `crates/sim/consensus-sim/src/cluster/trace/compartments.rs`).  *)
(*                                                                         *)
(* The log is `MultiPaxosTrace` (linked from `../multi-paxos/`), not the   *)
(* abstract log of `Compartments`: this module extends it and adds the     *)
(* replicas. The steps are the rules of `Compartments.tla` for a replica:  *)
(*   - "rlearn": a replica holds a value chosen at a slot, at the ballot   *)
(*     that chose it (`WriteOp::Learned`). A replica is not an acceptor:   *)
(*     it never votes and counts in no quorum;                             *)
(*   - "rapply": a replica folded a slot. It walks its chosen prefix in    *)
(*     slot order (`Walk` = "prefix"): the slot is the next one, and the   *)
(*     replica holds it;                                                   *)
(*   - "fold": the first party that applied a slot names the end of the    *)
(*     records of the slot (`high`) and `first_seq` after it (`first`), as *)
(*     in `truncation/TruncationTrace.tla`;                                *)
(*   - "rcompact": a replica compacted below a slot its walk folded, and   *)
(*     each record below the floor is below `first_seq` at a slot its walk *)
(*     folded;                                                             *)
(*   - "rtrim": a replica jumped to a trim point, and its walk moves to    *)
(*     it. Each record below the trim point is below `first_seq` at some   *)
(*     folded slot: the trim point is the floor of a peer, which compacted *)
(*     by that rule. After either, each slot below the floor is chosen;    *)
(*   - "rboot": a replica booted again and folded its prefix again from    *)
(*     its disk (`Boot` = "refold"): its walk is at `from`, and it holds   *)
(*     every slot below `from`;                                            *)
(*   - "rread": a replica opened a read (`ReplicaNode::quorum_read_in`).   *)
(*     It remembers the writes that completed before it (`snap`);          *)
(*   - "readack": an acceptor answered with a watermark at or above its    *)
(*     highest vote. A compaction may raise the watermark to the floor     *)
(*     minus 1, never lower it;                                            *)
(*   - "rserve": a replica served a read at `index` (`Read` = "quorum"):   *)
(*     the answers of a Phase-1 quorum are at or below the index, and the  *)
(*     replica folded every slot up to it;                                 *)
(*   - "applied": a party applied every slot up to `slot`: those writes    *)
(*     completed, so each slot up to it is chosen.                         *)
(* `ReadSeesAcked` is the property: a served read covers each write that   *)
(* completed before it started. A watermark or an index that is none is -1.*)
(***************************************************************************)
EXTENDS MultiPaxosTrace, Integers

VARIABLES rlearned, \* replica: the value it holds at each slot
          rnext,    \* replica: the next slot its walk folds
          rfloor,   \* replica: the first slot it holds
          racks,    \* the answers the acceptors sent
          rds,      \* read token: the read
          acked,    \* the highest slot whose write completed (-1: none)
          rfold     \* the fold of the slots 0..Len(rfold)-1, in order

repVars == <<rlearned, rnext, rfloor, racks, rds, acked>>

Replicas == TraceReplicas
Ctxs == TraceCtxs
NoRead == [state |-> "none", rep |-> None, snap |-> -1, index |-> -1]

MaxOf(S) == CHOOSE x \in S : \A y \in S : y <= x

\* The highest slot at which acceptor a voted (-1: none).
TopVote(a) == MaxOf({s \in Slots : vote[a][s].bal > 0} \cup {-1})

\* Replica r holds slot s: it learned it, or the slot is below its floor.
RHolds(r, s) == s < rfloor[r] \/ rlearned[r][s] # None

\* The fold of slot s.
RF(s) == rfold[s + 1]

\* The end of the records of the slots folded so far: `next_seq`.
RNextSeq == MaxOf({0} \cup {RF(s).high : s \in 0..(Len(rfold) - 1)})

\* Each record below floor f is below `first_seq` at a folded slot below u.
RReleased(f, u) ==
  /\ f <= Len(rfold)
  /\ \E t \in 0..(u - 1) : t < Len(rfold) /\ \A s \in 0..(f - 1) : RF(s).high <= RF(t).first

CompInit ==
  /\ TraceInit
  /\ rlearned = [r \in Replicas |-> [s \in Slots |-> None]]
  /\ rnext = [r \in Replicas |-> 0]
  /\ rfloor = [r \in Replicas |-> 0]
  /\ racks = {}
  /\ rds = [c \in Ctxs |-> NoRead]
  /\ acked = -1
  /\ rfold = <<>>

IsRep == Has("rep") /\ e.rep \in Replicas
IsCtx == Has("ctx") /\ e.ctx \in Ctxs

RLearn ==
  /\ e.event = "rlearn" /\ IsRep /\ IsBallot /\ IsSlot /\ IsValue
  /\ ChosenAt(e.bal, e.slot, e.val)
  /\ e.slot >= rfloor[e.rep]
  /\ rlearned' = [rlearned EXCEPT ![e.rep][e.slot] = e.val]
  /\ UNCHANGED <<rnext, rfloor, racks, rds, acked>>

RApply ==
  /\ e.event = "rapply" /\ IsRep /\ IsSlot
  /\ e.slot = rnext[e.rep]
  /\ RHolds(e.rep, e.slot)
  /\ rnext' = [rnext EXCEPT ![e.rep] = e.slot + 1]
  /\ UNCHANGED <<rlearned, rfloor, racks, rds, acked>>

\* Replica r moves its floor to f, and its walk to f when it was below.
Raise(r, f) ==
  /\ f > rfloor[r] /\ f <= NS
  /\ \A s \in Slots : s < f => \E v \in Values : Chosen(s, v)
  /\ rfloor' = [rfloor EXCEPT ![r] = f]
  /\ rlearned' = [rlearned EXCEPT ![r] = [s \in Slots |-> IF s < f THEN None ELSE @[s]]]
  /\ rnext' = [rnext EXCEPT ![r] = IF @ < f THEN f ELSE @]
  /\ UNCHANGED <<racks, rds, acked>>

\* A compaction follows the walk: the replica folded every slot below it.
RCompact ==
  /\ e.event = "rcompact" /\ IsRep /\ Has("floor")
  /\ e.floor <= rnext[e.rep]
  /\ RReleased(e.floor, rnext[e.rep])
  /\ Raise(e.rep, e.floor)

\* A jump moves the walk to the trim point of a peer.
RTrim ==
  /\ e.event = "rtrim" /\ IsRep /\ Has("floor")
  /\ RReleased(e.floor, Len(rfold))
  /\ Raise(e.rep, e.floor)

RBoot ==
  /\ e.event = "rboot" /\ IsRep /\ Has("from")
  /\ e.from <= NS
  /\ \A s \in Slots : s < e.from => RHolds(e.rep, s)
  /\ rnext' = [rnext EXCEPT ![e.rep] = e.from]
  /\ rds' = [c \in Ctxs |-> IF rds[c].state = "open" /\ rds[c].rep = e.rep
                           THEN [rds[c] EXCEPT !.state = "ended"] ELSE rds[c]]
  /\ UNCHANGED <<rlearned, rfloor, racks, acked>>

RRead ==
  /\ e.event = "rread" /\ IsRep /\ IsCtx
  /\ rds[e.ctx].state = "none"
  /\ rds' = [rds EXCEPT ![e.ctx] = [state |-> "open", rep |-> e.rep, snap |-> acked, index |-> -1]]
  /\ UNCHANGED <<rlearned, rnext, rfloor, racks, acked>>

RReadAck ==
  /\ e.event = "readack" /\ IsNode("acc") /\ Has("ctx") /\ Has("wm")
  /\ e.wm >= TopVote(e.acc)
  /\ racks' = racks \cup {[acc |-> e.acc, ctx |-> e.ctx, wm |-> e.wm]}
  /\ UNCHANGED <<rlearned, rnext, rfloor, rds, acked>>

RServe ==
  /\ e.event = "rserve" /\ IsRep /\ IsCtx /\ Has("index")
  /\ LET r == rds[e.ctx]
         i == e.index
     IN /\ r.state = "open"
        /\ r.rep = e.rep
        /\ \E Q \in TraceQ1 : \A a \in Q : \E k \in racks : k.acc = a /\ k.ctx = e.ctx /\ k.wm <= i
        /\ i < rnext[e.rep]
        /\ rds' = [rds EXCEPT ![e.ctx].state = "done", ![e.ctx].index = i]
  /\ UNCHANGED <<rlearned, rnext, rfloor, racks, acked>>

RApplied ==
  /\ e.event = "applied" /\ IsSlot
  /\ e.slot > acked
  /\ \A s \in Slots : s <= e.slot => \E v \in Values : Chosen(s, v)
  /\ acked' = e.slot
  /\ UNCHANGED <<rlearned, rnext, rfloor, racks, rds>>

RFold ==
  /\ e.event = "fold" /\ IsSlot /\ Has("high") /\ Has("first")
  /\ e.slot = Len(rfold)
  /\ \E v \in Values : Chosen(e.slot, v)
  /\ e.high = 0 \/ e.high >= RNextSeq
  /\ e.first >= IF rfold = <<>> THEN 0 ELSE RF(Len(rfold) - 1).first
  /\ e.first <= MaxOf({RNextSeq, e.high})
  /\ rfold' = Append(rfold, [high |-> e.high, first |-> e.first])
  /\ UNCHANGED <<vars, repVars>>

RepStep ==
  /\ \/ RLearn \/ RApply \/ RCompact \/ RTrim \/ RBoot \/ RRead \/ RReadAck \/ RServe \/ RApplied
  /\ UNCHANGED <<vars, rfold>>

CompNext ==
  /\ l <= Len(Trace)
  /\ (Step /\ UNCHANGED <<repVars, rfold>>) \/ RepStep \/ RFold
  /\ l' = l + 1

CompSpec == CompInit /\ [][CompNext]_<<l, vars, repVars, rfold>>

-----------------------------------------------------------------------------
(* Safety *)

\* A served read covers each write that completed before it started.
ReadSeesAcked == \A c \in Ctxs : rds[c].state = "done" => rds[c].snap <= rds[c].index
=============================================================================
