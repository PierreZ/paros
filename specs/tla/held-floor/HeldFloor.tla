------------------------------ MODULE HeldFloor -------------------------------
(***************************************************************************)
(* Held segments and the floor (R7 of `docs/architecture/04-7-segments.md`,*)
(* step 6 of the segments in the core), on the model of truncation of      *)
(* `truncation/` (`Truncation.tla`, a link). That model is the log, the    *)
(* compaction floor, the floor guard of Phase 1, the trim-point jump and   *)
(* the retry of a refused candidate; this module changes none of its      *)
(* steps and adds:                                                         *)
(*   - A `Roll` is a value in a slot (`Control::Roll { next }`). It is     *)
(*     valid when `next` is the tail segment plus one; a valid `Roll` at   *)
(*     slot s ends the tail segment at s + 1 (R2). The segment of a slot   *)
(*     is a fold of the chosen prefix (R3).                                *)
(*   - Each segment k has its replicas `RepOf[k]`, nodes that are not      *)
(*     acceptors (`ReplicaNode`). A replica copies the chosen slots of its *)
(*     segment from a node that learned them (a `Commit`, a catch-up from  *)
(*     the acceptors) or from a peer replica. Up to `F` replicas lose      *)
(*     every slot that they hold (a lost disk).                            *)
(*   - A node that applied the `Roll` publishes the end of its segment in  *)
(*     the chain (R5). The chain of the matchmakers is one variable here.  *)
(*   - The leader marks a segment `Held` when `F + 1` of its replicas hold *)
(*     every slot of it and its end is in the chain (the tally of          *)
(*     `collector.rs`, Scenario 3 of Matchmaker Paxos §3.5 and §4.5).      *)
(*   - An acceptor raises its floor to the end of a held segment, and drops*)
(*     the records below it in the same write: to the acceptor, a held     *)
(*     segment is a `Truncate` whose records are kept elsewhere. It needs  *)
(*     to have learned nothing, since every slot below the end is chosen.  *)
(*     The floor guard of Phase 1 refuses a campaign from below it, and a  *)
(*     laggard below it jumps with `TrimmedTo` (the steps of               *)
(*     `Truncation.tla`, which read the floor and not its reason).         *)
(*                                                                         *)
(* Constants (each rule has a control):                                    *)
(*   HeldAt  "f_plus_one": `Held` needs `F + 1` replicas. "f": a control,  *)
(*           `F` replicas, so one lost disk loses a record.                *)
(*   HeldEnd "chain": the end of a held segment is the end that the chain  *)
(*           publishes. "log": a control, the end in the chosen log, so a  *)
(*           floor can pass an end that is not published.                  *)
(***************************************************************************)
EXTENDS Truncation

CONSTANTS RollTo, Replicas, RepOf, F, HeldAt, HeldEnd

ASSUME RollTo \in [Rolls -> 2..(Cardinality(Rolls) + 1)]
\* The segments: segment 1 starts at slot 0, each `Roll` names the next.
Segs == 1..(Cardinality(Rolls) + 1)
ASSUME RepOf \in [Segs -> SUBSET Replicas] /\ Replicas \cap Acceptors = {}
ASSUME F \in Nat /\ HeldAt \in {"f_plus_one", "f"} /\ HeldEnd \in {"chain", "log"}

VARIABLES rhold,  \* replica: the slots it holds
          chain,  \* segment: the end that the chain publishes, 0 for none
          held,   \* the segments marked `Held`
          rlost   \* how many replicas lost their disk

seg == <<rhold, chain, held, rlost>>
hvars == <<vars, seg>>

HeldTypeOK ==
  /\ TypeOK
  /\ rhold \in [Replicas -> SUBSET Slots]
  /\ chain \in [Segs -> 0..NS]
  /\ held \subseteq Segs
  /\ rlost \in 0..F

HeldInit ==
  /\ Init
  /\ rhold = [r \in Replicas |-> {}]
  /\ chain = [k \in Segs |-> 0]
  /\ held = {}
  /\ rlost = 0

-----------------------------------------------------------------------------
(* The segments of the chosen log *)

\* Every slot below s is chosen.
PrefixChosen(s) == \A t \in Slots : t < s => IsChosen(t)

\* The segment of slot s: the fold of the valid `Roll`s below it (R2, R3).
\* Evaluate it only under PrefixChosen(s).
RECURSIVE SegAt(_)
SegAt(s) ==
  IF s = 0 THEN 1
  ELSE LET k == SegAt(s - 1)
           v == ChosenVal(s - 1)
       IN IF v \in Rolls /\ RollTo[v] = k + 1 THEN k + 1 ELSE k

\* The end of segment k in the chosen log: one past the slot of the valid
\* `Roll` that ends it, or 0 while none is chosen with its prefix.
LogEnd(k) ==
  LET e == {s \in Slots : PrefixChosen(s + 1) /\ SegAt(s) = k /\ SegAt(s + 1) = k + 1}
  IN IF e = {} THEN 0 ELSE Max(e) + 1

\* The end of segment k that `Held` and the floor follow.
End(k) == IF HeldEnd = "chain" THEN chain[k] ELSE LogEnd(k)

\* The slots of segment k, which ends at e > 0.
SegSlots(k, e) == {s \in 0..(e - 1) : SegAt(s) = k}

-----------------------------------------------------------------------------
(* The steps of segments *)

\* Replica r may copy slot s: a chosen slot of its segment that a node
\* learned or that a peer replica holds.
CanCopy(r, s) ==
  /\ PrefixChosen(s + 1)
  /\ r \in RepOf[SegAt(s)]
  /\ \/ \E n \in Acceptors : learned[n][s] # None
     \/ \E p \in Replicas : s \in rhold[p]

\* Replica r copies every slot that it may copy, in one step. A replica
\* that holds a part of its segment is still reached: a node learns one
\* slot at a time. (`specs/tla/segment-roll/` checks that a replica applies
\* only the chosen prefix of its own segment.)
Copy(r) ==
  /\ rhold' = [rhold EXCEPT ![r] = @ \cup {s \in Slots : CanCopy(r, s)}]
  /\ UNCHANGED <<vars, chain, held, rlost>>

\* A node applied the `Roll` that ends segment k and publishes the end in
\* the chain (R3, R5). Every writer writes the same value, a function of
\* the chosen log, so the model writes the end of the log: a step that
\* changes nothing while no `Roll` ends k. `specs/tla/segment-roll/` checks
\* that a node publishes only an end that it applied (its control
\* `act_vote`).
Publish(k) ==
  /\ chain' = [chain EXCEPT ![k] = LogEnd(k)]
  /\ UNCHANGED <<vars, rhold, held, rlost>>

\* The replicas that `Held` needs.
Need == IF HeldAt = "f_plus_one" THEN F + 1 ELSE F

\* The leader marks segment k `Held`: enough of its replicas hold every slot
\* of it, up to its end.
MarkHeld(k) ==
  /\ End(k) > 0
  /\ Cardinality({r \in RepOf[k] : SegSlots(k, End(k)) \subseteq rhold[r]}) >= Need
  /\ held' = held \cup {k}
  /\ UNCHANGED <<vars, rhold, chain, rlost>>

\* Acceptor a raises its floor to the end of the held segment k, and drops
\* the records below it in the same write (`WriteOp::Truncate`). A floor
\* never goes down.
HeldFloor(a, k) ==
  LET f == IF End(k) > floor[a] THEN End(k) ELSE floor[a]
  IN /\ k \in held
     /\ floor' = [floor EXCEPT ![a] = f]
     /\ dfloor' = [dfloor EXCEPT ![a] = f]
     /\ vote' = [vote EXCEPT ![a] = DropVotes(a, f)]
     /\ learned' = [learned EXCEPT ![a] = DropLearned(a, f)]
     /\ UNCHANGED <<prom, phase, from, next, blocked, restarts, faults, msgs, seg>>

\* Replica r loses its disk. At most `F` replicas do.
LoseReplica(r) ==
  /\ rlost < F
  /\ rlost' = rlost + 1
  /\ rhold' = [rhold EXCEPT ![r] = {}]
  /\ UNCHANGED <<vars, chain, held>>

HeldNext ==
  \/ Next /\ UNCHANGED seg
  \/ \E r \in Replicas : Copy(r) \/ LoseReplica(r)
  \/ \E k \in Segs : MarkHeld(k) \/ Publish(k) \/ \E a \in Acceptors : HeldFloor(a, k)

HeldSpec == HeldInit /\ [][HeldNext]_hvars

\* The liveness of `Truncation.tla`: the fairness of the last leader, whose
\* steps keep `seg`.
HeldLiveSpec ==
  /\ HeldSpec
  /\ LET o == Owner[NB]
     IN /\ WF_hvars(Prepare(NB) /\ UNCHANGED seg)
        /\ WF_hvars(AskAgain(NB) /\ UNCHANGED seg)
        /\ WF_hvars(Win(NB) /\ UNCHANGED seg)
        /\ \A s \in Slots : WF_hvars(Decide(NB, s) /\ UNCHANGED seg) /\ WF_hvars(Probe(NB, s) /\ UNCHANGED seg)
        /\ \A a \in Acceptors :
             /\ WF_hvars(Promise(a, NB) /\ UNCHANGED seg)
             /\ \A s \in Slots : WF_hvars(Accept(a, NB, s) /\ UNCHANGED seg)
        /\ \A p \in Acceptors : WF_hvars(CatchUp(o, p) /\ UNCHANGED seg) /\ WF_hvars(TrimTo(o, p) /\ UNCHANGED seg)

-----------------------------------------------------------------------------
(* Safety *)

\* Slot s is in a held segment whose end the chain publishes.
InHeld(s) == \E k \in held : s < chain[k]

\* A floor is truncated or held: each slot below the floor of an acceptor
\* is at or below a chosen truncation, or in a held segment whose end the
\* chain publishes. With `HeldEnd = "log"` a floor passes an end that is
\* not in the chain, and a reader of the chain cannot find the replicas of
\* a slot that the acceptors dropped. (`FloorTruncated` of
\* `Truncation.tla`, with the held segments.)
FloorTruncatedOrHeld ==
  \A a \in Acceptors, s \in Slots :
    s < floor[a] => (\E t \in Slots, v \in Truncations : t >= s /\ Chosen(t, v)) \/ InHeld(s)

\* The record of the chosen slot s is released: it is below `first_seq`.
Released(s) == \E t \in Slots : t >= s /\ (\A u \in 0..t : IsChosen(u)) /\ FirstSeq(t) > Pos(s)

\* A floor keeps the records: a record below a floor is below `first_seq`,
\* or in a held segment. (`FloorReleased` of `Truncation.tla`, with the
\* held segments.)
FloorReleasedOrHeld ==
  \A a \in Acceptors, s \in Slots :
    (s < floor[a] /\ \A u \in 0..s : IsChosen(u)) /\ ChosenVal(s) \in Commands =>
      Released(s) \/ InHeld(s)

\* A record is kept: each chosen command whose record is not released is
\* readable at an acceptor (a vote or a learned value) or held by a
\* replica, after up to `F` replicas lost their disk. With `HeldAt = "f"`
\* the acceptors drop a slot that `F` replicas hold, and they can lose it.
RecordKept ==
  \A s \in Slots :
    (\A u \in 0..s : IsChosen(u)) /\ ChosenVal(s) \in Commands /\ ~Released(s) =>
      \/ \E a \in Acceptors : Readable(vote[a][s]) \/ learned[a][s] # None
      \/ \E r \in Replicas : s \in rhold[r]

\* The chain is true: a published end is the end in the chosen log.
ChainTrue == \A k \in Segs : chain[k] # 0 => chain[k] = LogEnd(k)

\* Reachability controls: each must be violated.
\* A segment is held.
NoHeld == held = {}
\* Every acceptor dropped a held segment: its floor is at the end.
NoHeldDrop == ~\E k \in held : chain[k] > 0 /\ \A a \in Acceptors : floor[a] >= chain[k]
\* A node jumped to a held floor: its floor covers a slot of a held segment
\* that it never voted.
NoHeldJump == ~\E a \in Acceptors, s \in Slots :
                /\ s < floor[a] /\ InHeld(s)
                /\ ~\E m \in msgs : m.type = "2b" /\ m.acc = a /\ m.slot = s
\* A leader wins Phase 1 from at or above a held floor.
NoWinAboveHeld == ~\E b \in Ballots, k \in held : phase[b] = "p2" /\ chain[k] > 0 /\ from[b] >= chain[k]
\* A replica lost its disk after a segment was held.
NoLossAfterHeld == ~(held # {} /\ rlost > 0)
=============================================================================
