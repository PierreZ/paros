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
(*   FloorTo "prefix": the floor rises to the end of segment k only when   *)
(*           every segment up to k is held. "any": a control, to the end   *)
(*           of any held segment, over a segment before it that is not.   *)
(*   HoldBy  "decided": the floor at a held segment is a decided command, *)
(*           `Hold` (a value of `HoldCmds`, `HoldOf` names its segment):  *)
(*           the leader proposes it once its tally marked the segment     *)
(*           `Held`, and an acceptor raises its floor when its fold       *)
(*           reaches the `Hold`. So every node judges a retry of a write  *)
(*           in a held segment from the log alone (`Judge`), the same     *)
(*           way. "tally": a control, an acceptor raises its floor as     *)
(*           soon as the tally marks the segment `Held`; a retry is       *)
(*           judged from the floor of the node, and two nodes answer it   *)
(*           two ways.                                                    *)
(*   HoldWhen "held": the leader proposes `Hold` for segment k only when  *)
(*           its tally marked k (and, with `FloorTo = "prefix"`, every    *)
(*           segment before k) `Held`. "any": a control, it proposes a    *)
(*           `Hold` at any time, so a floor drops records no replica      *)
(*           holds.                                                       *)
(*   HoldDrop "keep": the drop at a decided `Hold` keeps every record     *)
(*           that a retry folded as a duplicate above the new floor reads *)
(*           (the fixed point of `Replica::compaction_target`), so a node *)
(*           that folds the retry again (after a jump or a boot) finds    *)
(*           it. "end": a control, the drop goes to the end of the        *)
(*           segment, over a retry chosen between the end and the `Hold`. *)
(*   JumpTo  "closed": a compaction or a jump of `Truncation.tla` lands a *)
(*           floor on a closed point (`Closed`): a `TrimmedTo` carries a  *)
(*           floor that the peer held. "any": a control, a jump lands on  *)
(*           any point up to the floor of the peer.                      *)
(*   Retries the retries of a write in the log (`RetryOf` names the write  *)
(*           that each one retries); none in the checks that do not look  *)
(*           at them.                                                     *)
(*   Judges  the nodes that judge a retry of a write (`Judge`); none in    *)
(*           the checks that do not look at retries.                       *)
(*   HeldFloorNodes  the acceptors that raise their floor at `Held`; the   *)
(*           others reach it only with a jump (`TrimTo`). Every acceptor   *)
(*           but in the checks that must see a jump.                       *)
(***************************************************************************)
EXTENDS Truncation

CONSTANTS RollTo, Replicas, RepOf, F, HeldAt, HeldEnd, FloorTo, HeldFloorNodes,
          HoldOf, HoldBy, HoldWhen, Judges, HoldDrop, RetryOf, JumpTo

ASSUME RollTo \in [Rolls -> 2..(Cardinality(Rolls) + 1)]
\* The segments: segment 1 starts at slot 0, each `Roll` names the next.
Segs == 1..(Cardinality(Rolls) + 1)
ASSUME RepOf \in [Segs -> SUBSET Replicas] /\ Replicas \cap Acceptors = {}
ASSUME F \in Nat /\ HeldAt \in {"f_plus_one", "f"} /\ HeldEnd \in {"chain", "log"}
ASSUME FloorTo \in {"prefix", "any"} /\ HeldFloorNodes \subseteq Acceptors
ASSUME HoldOf \in [HoldCmds -> Segs] /\ HoldBy \in {"decided", "tally"}
ASSUME HoldWhen \in {"held", "any"} /\ Judges \subseteq Acceptors
ASSUME HoldDrop \in {"keep", "end"} /\ RetryOf \in [Retries -> Commands]
ASSUME JumpTo \in {"closed", "any"}

VARIABLES rhold,  \* replica: the slots it holds
          chain,  \* segment: the end that the chain publishes, 0 for none
          held,   \* the segments marked `Held`
          rlost,  \* how many replicas lost their disk
          answers \* the retries judged: a node, a fold point, a position

seg == <<rhold, chain, held, rlost, answers>>
hvars == <<vars, seg>>

HeldTypeOK ==
  /\ TypeOK
  /\ rhold \in [Replicas -> SUBSET Slots]
  /\ chain \in [Segs -> 0..NS]
  /\ held \subseteq Segs
  /\ rlost \in 0..F
  /\ answers \subseteq [node : Acceptors, at : Slots, pos : 0..NS,
                        ans : {"truncated", "held", "dup"}, has : BOOLEAN]

HeldInit ==
  /\ Init
  /\ rhold = [r \in Replicas |-> {}]
  /\ chain = [k \in Segs |-> 0]
  /\ held = {}
  /\ rlost = 0
  /\ answers = {}

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
  /\ UNCHANGED <<vars, chain, held, rlost, answers>>

\* A node applied the `Roll` that ends segment k and publishes the end in
\* the chain (R3, R5). Every writer writes the same value, a function of
\* the chosen log, so the model writes the end of the log: a step that
\* changes nothing while no `Roll` ends k. `specs/tla/segment-roll/` checks
\* that a node publishes only an end that it applied (its control
\* `act_vote`).
Publish(k) ==
  /\ chain' = [chain EXCEPT ![k] = LogEnd(k)]
  /\ UNCHANGED <<vars, rhold, held, rlost, answers>>

\* The replicas that `Held` needs.
Need == IF HeldAt = "f_plus_one" THEN F + 1 ELSE F

\* The leader marks segment k `Held`: enough of its replicas hold every slot
\* of it, up to its end.
MarkHeld(k) ==
  /\ End(k) > 0
  /\ Cardinality({r \in RepOf[k] : SegSlots(k, End(k)) \subseteq rhold[r]}) >= Need
  /\ held' = held \cup {k}
  /\ UNCHANGED <<vars, rhold, chain, rlost, answers>>

\* The floor that the held segment k gives: its end, when every segment up
\* to k is held (with `FloorTo = "any"`, whatever the segments before it).
\* It is 0 otherwise, and then HeldFloor changes nothing.
HeldEndOf(k) ==
  IF HoldBy = "decided" THEN End(k)
  ELSE IF k \in held /\ (FloorTo = "any" \/ \A j \in 1..k : j \in held) THEN End(k) ELSE 0

\* Acceptor a folded a `Hold` of segment k: it learned the `Hold` at some
\* slot t and holds every slot up to t.
FoldedHold(a, k) ==
  \E t \in Slots :
    /\ learned[a][t] \in HoldCmds
    /\ HoldOf[learned[a][t]] = k
    /\ \A s \in 0..t : Holds(a, s)

\* The slots below the retry at slot u that hold the write that it
\* retries. Evaluate it only under PrefixChosen(u).
Original(u) == {s \in 0..(u - 1) : ChosenVal(s) = RetryOf[ChosenVal(u)]}

\* The outcome of the fold of the retry at slot u (`JournalState::apply`):
\* refused when the log holds no such write, truncated below `first_seq`,
\* held in a segment whose `Hold` the fold applied before u, and else a
\* duplicate, which reads the record of the write. A function of the
\* chosen log, so every node folds it the same way. Evaluate it only under
\* PrefixChosen(u + 1) and ChosenVal(u) \in Retries.
RetryAnswer(u) ==
  IF Original(u) = {} THEN "refused"
  ELSE LET s == Min(Original(u))
       IN IF Pos(s) < FirstSeq(u - 1) THEN "truncated"
          ELSE IF HoldBy = "decided"
                  /\ \E w \in 0..(u - 1) : ChosenVal(w) \in HoldCmds /\ s < End(HoldOf[ChosenVal(w)])
               THEN "held"
          ELSE "dup"

\* The retries at a slot in f..(t - 1) that fold as a duplicate.
DupRetries(f, t) == {u \in f..(t - 1) : ChosenVal(u) \in Retries /\ RetryAnswer(u) = "dup"}

\* The floor of a drop to f, with every slot below t chosen: f, lowered
\* to the slot of the write of each retry above it that folds as a
\* duplicate, until no such retry reads below it (the fixed point of
\* `Replica::compaction_target`).
RECURSIVE KeepFrom(_, _)
KeepFrom(f, t) ==
  LET g == Min({f} \cup {Min(Original(u)) : u \in DupRetries(f, t)})
  IN IF g = f THEN f ELSE KeepFrom(g, t)

\* Every `Hold` that a leader proposed names a segment that its tally
\* marked `Held` (with `FloorTo = "prefix"`, every segment up to it). A
\* re-proposal of a `Hold` by a later leader keeps it: `held` only grows.
HoldProposedHeld ==
  \A m \in msgs :
    m.type = "2a" /\ m.val \in HoldCmds =>
      /\ HoldOf[m.val] \in held
      /\ FloorTo = "prefix" => \A j \in 1..HoldOf[m.val] : j \in held

\* Acceptor a raises its floor to the end of the held segment k, and drops
\* the records below it in the same write (`WriteOp::Truncate`). A floor
\* never goes down.
HeldFloor(a, k) ==
  LET e == IF HoldBy = "decided" /\ HoldDrop = "keep"
             THEN KeepFrom(HeldEndOf(k), FirstUnchosen(a))
             ELSE HeldEndOf(k)
      f == IF e > floor[a] THEN e ELSE floor[a]
  IN /\ HoldBy = "decided" => FoldedHold(a, k)
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
  /\ UNCHANGED <<vars, chain, held, answers>>

\* The slot of the write at position p, a position below `next_seq` after
\* slot t.
SlotOf(p, t) == CHOOSE s \in 0..t : ChosenVal(s) \in Commands /\ Pos(s) = p

\* The answer to a retry of the write at position p, judged at the fold of
\* slot t. Below `first_seq` it is truncated. In a held segment it is held:
\* the reader asks the replicas of the segment. Else the log holds the write
\* there, and the retry is a duplicate.
Answer(n, t, p) ==
  IF p < FirstSeq(t) THEN "truncated"
  ELSE IF HoldBy = "decided"
          /\ \E u \in 0..t : ChosenVal(u) \in HoldCmds /\ SlotOf(p, t) < End(HoldOf[ChosenVal(u)])
       THEN "held"
  ELSE IF HoldBy = "tally" /\ SlotOf(p, t) < floor[n] THEN "held"
  ELSE "dup"

\* Node n judges a retry of the write at position p when its fold reaches
\* slot t: it learned every slot up to t and none above it. The retry is
\* chosen at slot t + 1, so that slot is not chosen yet. A retry that is
\* already chosen is a value of the log (`Retries`): `RetryAnswer` folds
\* it, and `RetryKept` checks that the drop keeps the record that it
\* reads. The answer of a duplicate needs the record of the write (`has`).
\* With `HoldBy = "decided"` the answer is a function of the chosen log,
\* so `OneAnswer` holds by the fold; its control is `hold_tally`.
Judge(n, t, p) ==
  /\ PrefixChosen(t + 1)
  /\ t + 1 \in Slots => ~IsChosen(t + 1)
  /\ FirstUnchosen(n) = t + 1
  /\ p < Pos(t + 1)
  /\ LET s == SlotOf(p, t)
     IN answers' = answers \cup {[node |-> n, at |-> t, pos |-> p, ans |-> Answer(n, t, p),
                                   has |-> learned[n][s] # None \/ Readable(vote[n][s])]}
  /\ UNCHANGED <<vars, rhold, chain, held, rlost>>

\* Floor f is closed: each retry at or above f that folds as a duplicate
\* reads a write at or above f. A node folds again every slot at or above
\* its floor (after a jump or a boot), so a floor must be closed.
Closed(f) ==
  \A u \in Slots :
    /\ u >= f /\ PrefixChosen(u + 1) /\ ChosenVal(u) \in Retries
    /\ RetryAnswer(u) = "dup"
    => Min(Original(u)) >= f

\* A step of `Truncation.tla` that moves a floor (a compaction, a jump)
\* lands it on a closed point. A `TrimmedTo` carries a floor that the peer
\* held, and each floor that a node holds is closed (`RetryKept`); the
\* compaction of the code clamps its floor to a closed point. With
\* `JumpTo = "any"` (a control), a jump may land on any point up to the
\* floor of the peer.
FloorsClosed ==
  JumpTo = "closed" => \A a \in Acceptors : floor'[a] # floor[a] => Closed(floor'[a])

HeldNext ==
  \/ Next /\ UNCHANGED seg /\ (HoldWhen = "held" => HoldProposedHeld') /\ FloorsClosed
  \/ \E n \in Judges, t \in Slots, p \in 0..NS : Judge(n, t, p)
  \/ \E r \in Replicas : Copy(r) \/ LoseReplica(r)
  \/ \E k \in Segs : MarkHeld(k) \/ Publish(k) \/ \E a \in HeldFloorNodes : HeldFloor(a, k)

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

\* Slot s is in a held segment whose end the chain publishes: the chain
\* says which replicas hold it.
InHeld(s) ==
  \E k \in held : chain[k] > 0 /\ s < chain[k] /\ PrefixChosen(s + 1) /\ SegAt(s) = k

\* A floor is truncated or held: each slot below the floor of an acceptor
\* is at or below a chosen truncation, or in a held segment whose end the
\* chain publishes. The chain is how a reader finds the replicas of a slot
\* that the acceptors dropped, so this is the rule "the floor follows the
\* chain" written as a state. `HeldEnd = "log"` breaks it by definition;
\* `FloorTo = "any"` breaks it at a slot of a segment that is not held.
\* (`FloorTruncated` of `Truncation.tla`, with the held segments.)
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

\* A replica holds only chosen slots of its own segments.
ReplicaTrue == \A r \in Replicas, s \in Slots : s \in rhold[r] => PrefixChosen(s + 1) /\ r \in RepOf[SegAt(s)]

\* One answer to a retry: every node that judged a retry of position p at
\* the fold of slot t gave the same answer. The journal fold is the same on
\* every node, so a retry gets one outcome.
OneAnswer == \A x, y \in answers : x.at = y.at /\ x.pos = y.pos => x.ans = y.ans

\* A duplicate is judged on the record: the node that answers it holds the
\* write.
DupHasRecord == \A x \in answers : x.ans = "dup" => x.has

\* A retry finds its write: the floor of each acceptor is closed. It holds
\* across the steps that choose a retry, and across the drop at a `Hold`,
\* which `FloorsClosed` does not constrain.
RetryKept == \A a \in Acceptors : Closed(floor[a])

\* A node drops a segment only on a `Hold` of a held segment (with
\* `FloorTo = "prefix"`, every segment up to it is held): each step of the
\* decided floor is a step of the tally floor, which the checks with
\* `HoldBy = "tally"` explore.
FoldedHeld ==
  \A a \in Acceptors, k \in Segs :
    FoldedHold(a, k) =>
      /\ k \in held
      /\ FloorTo = "prefix" => \A j \in 1..k : j \in held

\* Reachability controls: each must be violated.
\* A segment is held.
NoHeld == held = {}
\* Every acceptor dropped a held segment: its floor is at the end.
NoHeldDrop == ~\E k \in held : chain[k] > 0 /\ \A a \in Acceptors : floor[a] >= chain[k]
\* A node jumped to a held floor with `TrimTo`: it does not raise its floor
\* at `Held` itself, and its floor covers a slot of a held segment that it
\* never voted.
NoHeldJump == ~\E a \in Acceptors \ HeldFloorNodes, s \in Slots :
                /\ s < floor[a] /\ InHeld(s)
                /\ ~\E m \in msgs : m.type = "2b" /\ m.acc = a /\ m.slot = s
\* The laggard of the last ballot: its campaign asked from below a held
\* floor, it jumped with `TrimTo` (it does not raise its floor at `Held`),
\* and it won Phase 1 from at or above that floor.
NoLaggardWin == ~\E k \in held :
                  /\ chain[k] > 0
                  /\ Owner[NB] \notin HeldFloorNodes
                  /\ \E m \in msgs : m.type = "1a" /\ m.bal = NB /\ m.from < chain[k]
                  /\ phase[NB] = "p2" /\ from[NB] >= chain[k]
                  /\ floor[Owner[NB]] >= chain[k]
\* A leader wins Phase 1 from at or above a held floor.
NoWinAboveHeld == ~\E b \in Ballots, k \in held : phase[b] = "p2" /\ chain[k] > 0 /\ from[b] >= chain[k]
\* A retry is answered held.
\* A node folded a `Hold` and dropped the held segment: its floor is at the
\* end (with `HoldBy = "decided"`, only a learner folds a `Hold`).
NoHoldDrop == ~\E k \in held, a \in Acceptors : chain[k] > 0 /\ floor[a] >= chain[k]
\* A retry chosen between the end of a held segment and its `Hold` reads
\* a write of that segment, and holds a floor below the end.
NoRetryKeep == ~\E a \in Acceptors, k \in held :
                 /\ chain[k] > 0 /\ FoldedHold(a, k) /\ floor[a] < chain[k]
                 /\ \E u \in Slots : u >= chain[k] /\ PrefixChosen(u + 1) /\ ChosenVal(u) \in Retries
\* The drop at a `Hold` keeps a part of a held segment: a floor strictly
\* between 0 and the end.
NoKeepMid == ~\E a \in Acceptors, k \in held : 0 < floor[a] /\ floor[a] < chain[k]
NoJudgeHeld == ~\E x \in answers : x.ans = "held"
\* A replica lost its disk after a segment was held.
NoLossAfterHeld == ~(held # {} /\ rlost > 0)
=============================================================================
