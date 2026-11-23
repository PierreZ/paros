---------------------------- MODULE JournalCompact ----------------------------
(***************************************************************************)
(* The compaction of the journal v1, over `Journal.tla` (section J3.2,     *)
(* #1326 (the journal roles over the core)). `segment-truncate/` checks    *)
(* each rule of the compaction alone, with replicas that stop; this module *)
(* puts the rules into the composed model, so that TLC checks their seams  *)
(* with Paxos in each segment, the seal, the boundary state, retire and    *)
(* trim:                                                                   *)
(*                                                                         *)
(*   - A replica drops the slots of its log below its compaction point:    *)
(*     the slot that holds the record at its `first_seq`, or its applied   *)
(*     prefix when it keeps no record (`RepCompact`). The same write keeps *)
(*     its fold at that point, the trim point that a laggard takes.        *)
(*   - The acceptors drop the votes of a segment only below a slot that    *)
(*     CompactAt (F + 1) replicas of its set compacted to, in whichever    *)
(*     grid the votes live (#1318, `AccCompact`): the replicas report      *)
(*     their compaction point to the leader side, which counts them and    *)
(*     sends the floor. `Journal.tla` has the rest: a promise reports the  *)
(*     dropped slots, the leader proposes nothing there and its seal end   *)
(*     counts them as votes, and an acceptor refuses an `Accept` there.    *)
(*   - A laggard takes the compaction point of a peer of its set and its   *)
(*     fold there (`TrimmedTo`, `JumpFrom`), whenever a peer compacted     *)
(*     past it (`trims`: the points on a disk, each with the floor that    *)
(*     chose it). The fold that jumped holds no write below the trim floor *)
(*     of the point (`rfloor`): it gives no answer until its own first_seq *)
(*     reaches the floor (`Blind` in `Journal.tla`).                       *)
(*                                                                         *)
(* Compact "replicas" is the design. "floor" is a control: the acceptors   *)
(* drop the votes below the slot of the decided floor alone, before any    *)
(* replica applied them, and `Learnable` breaks.                           *)
(***************************************************************************)
EXTENDS Journal

CONSTANTS Compact, CompactAt

ASSUME Compact \in {"replicas", "floor"}
ASSUME CompactAt \in Nat /\ CompactAt >= 1

\* The compaction point of a replica: the slot that holds the record at its
\* `first_seq`, or its applied prefix when it keeps no record.
CompactTo(r) ==
  LET keep == {s \in rstart[r]..(Front(r) - 1) :
                 Record(s) /\ Truth(s).next >= StateOf(r).first}
  IN IF keep = {} THEN Front(r) ELSE Min(keep)

\* CompactAt replicas of the set of p compacted to x or above.
CompactedTo(p, x) ==
  Cardinality({r \in RepOf[p] : rbase[r] # None /\ rcomp[r] >= x}) >= CompactAt

\* The control "floor": the slot that holds the record at the decided floor
\* of the whole log, or the chosen prefix.
FloorSlot ==
  LET keep == {s \in 0..(Frontier - 1) : Record(s) /\ Truth(s).next >= Truth(Frontier).first}
  IN IF keep = {} THEN Frontier ELSE Min(keep)

\* A replica drops the slots of its log below its compaction point.
RepCompact(r) ==
  /\ rbase[r] # None
  /\ CompactTo(r) > rcomp[r]
  /\ rcomp' = [rcomp EXCEPT ![r] = CompactTo(r)]
  /\ rfloor' = [rfloor EXCEPT ![r] = StateOf(r).first]
  /\ trims' = trims \cup {<<GenOf(r), CompactTo(r), StateOf(r).first>>}
  /\ UNCHANGED <<reg, ldr, acc, rapp, rbase, rstart, acked, resolved, rdrop>>

\* Acceptor a drops the votes of segment p below x: the slots Start(p) ..
\* x - 1 belong to p and its replica set. The step drops them in memory and
\* queues the write, after the writes queued before it, so the flush of a
\* vote queued earlier does not bring the vote back. With `Direct` the
\* write is durable in the step.
AccCompact(a, p, x) ==
  LET drop == Start(p)..(x - 1)
      clear(v) == [s \in Slots |-> IF s \in drop THEN NoVote ELSE v[s]]
  IN
  /\ StartKnown(p)
  \* A range that the acceptor dropped already is no new write: the leader
  \* sends the point on every tick.
  /\ ~(drop \subseteq adrop[a])
  /\ IF Compact = "replicas" THEN CompactedTo(p, x) ELSE x <= FloorSlot
  /\ adrop' = [adrop EXCEPT ![a] = @ \cup drop]
  /\ mvote' = [mvote EXCEPT ![a] = clear(@)]
  /\ IF Direct
     THEN /\ dadrop' = [dadrop EXCEPT ![a] = @ \cup drop]
          /\ dvote' = [dvote EXCEPT ![a] = clear(@)]
          /\ UNCHANGED pend
     ELSE /\ pend' = [pend EXCEPT ![a] = Append(@, <<"d", Start(p), x, None, FALSE>>)]
          /\ UNCHANGED <<dadrop, dvote>>
  /\ UNCHANGED <<reg, ldr, mprom, dprom, crashes, rots, p1b, sent2b, rep>>

\* Replica r takes a compaction point m of its set, on the disk of a peer:
\* the peer's answer to a `Pull` below it (`TrimmedTo`) carries the point,
\* the fold there and its trim floor. No bytes, no ballot.
JumpFrom(r, m) ==
  /\ rbase[r] # None
  /\ m[1] = GenOf(r)
  /\ m[2] > Front(r)
  /\ rapp' = [rapp EXCEPT ![r] = m[2] - rstart[r]]
  /\ rcomp' = [rcomp EXCEPT ![r] = m[2]]
  /\ rfloor' = [rfloor EXCEPT ![r] = m[3]]
  /\ UNCHANGED <<reg, ldr, acc, rbase, rstart, acked, resolved, rdrop, trims>>

NextC ==
  \/ Next
  \* A leader's `Commit` of a chosen slot can arrive after the acceptors
  \* dropped the slot and the peers compacted it.
  \/ \E r \in Replicas : ApplyIf(r, TRUE)
  \* A campaign that writes from the start of its oldest configuration, so
  \* its Phase 1 reads the slots the acceptors dropped.
  \/ \E b \in Ballots : RegisterFrom(b, LowFrom)
  \/ \E r \in Replicas : RepCompact(r)
  \/ \E r \in Replicas, m \in trims : JumpFrom(r, m)
  \/ \E a \in Acceptors, p \in Gens, x \in Slots \cup {NS} : AccCompact(a, p, x)

SpecC == Init /\ [][NextC]_vars

(***************************************************************************)
(* Safety.                                                                 *)
(***************************************************************************)
\* No acceptor and no peer holds the next slot of r.
NoSource(r) ==
  LET s == Front(r) IN ~OnDisk(s) /\ ~\E q \in RepOf[GenOf(r)] \ {r} : Holds(q, s)

\* A replica that has not applied its whole segment can learn its next
\* chosen slot: a quorum of a live grid holds it, a peer holds it in its log,
\* or a peer compacted past it. A replica of a trimmed segment needs nothing.
Learnable == \A r \in Replicas :
  LET g == GenOf(r)
      s == Front(r)
  IN
  (/\ rbase[r] # None /\ ~rdrop[r] /\ ~rtrim[g] /\ s < NS /\ IsChosen(s)
   /\ IF EndKnown(g) THEN s < rend[g].v ELSE TRUE)
  => \/ ~NoSource(r)
     \/ \E q \in RepOf[g] \ {r} : rbase[q] # None /\ rcomp[q] > s

\* A replica applies, compacts and jumps only once it holds a base: a
\* `TrimmedTo` answers a `Pull`, which only a replica with a base sends.
BaseFirst == \A r \in Replicas : rbase[r] = None => rapp[r] = 0 /\ rcomp[r] = 0

\* An acceptor drops chosen slots only, and keeps no vote where it dropped.
DropChosen == \A a \in Acceptors : \A s \in adrop[a] \cup dadrop[a] : IsChosen(s)
DroppedEmpty == \A a \in Acceptors :
  /\ \A s \in adrop[a] : mvote[a][s] = NoVote
  /\ \A s \in dadrop[a] : dvote[a][s] = NoVote

\* Every record below a compaction point lies below its trim floor: a fold
\* that starts at the point and has reached the floor judges every position
\* as the whole log does.
TrimTrue == \A m \in trims : \A s \in Slots :
  Start(m[1]) <= s /\ s < m[2] /\ Record(s) => Truth(s).next < m[3]

\* A replica never gets back the slots it dropped, and never moves back.
CompactStays == [][\A r \in Replicas : rcomp'[r] >= rcomp[r] /\ rapp'[r] >= rapp[r]]_vars

(***************************************************************************)
(* Reach: each must be violated, so that a passing check is not empty.     *)
(***************************************************************************)
\* An acceptor dropped votes, and a leader whose Phase 1 saw it writes.
NoSkipWrite == ~\E b \in Ballots : bskip[b] # {} /\ bst[b] = "w"
\* A fold is blind: it jumped to a point below its trim floor.
NoBlind == \A r \in Replicas : ~Blind(r)
\* A replica jumped over a slot that it did not apply.
NoJump == [][\A r \in Replicas : rapp'[r] <= rapp[r] + 1]_vars
=============================================================================
