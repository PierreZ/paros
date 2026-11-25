-------------------------- MODULE HeldFloorReconfig ---------------------------
(***************************************************************************)
(* Held segments and the floor across a reconfiguration onto empty stores  *)
(* (R7 of `docs/architecture/04-7-segments.md`, step 6 of the segments in  *)
(* the core), on the model of `truncation-reconfig/`                       *)
(* (`TruncationReconfig.tla`, a link). That model is the log on a first    *)
(* configuration `C1` and a second `C2` whose new member starts empty,     *)
(* matchmaking, the floor guard, the trim-point jump, the GC that forgets  *)
(* `C1` once a Phase-2 quorum of `C2` holds the prefix up to the election  *)
(* fence, and the wipe of an acceptor. This module changes none of its     *)
(* steps, and adds the steps of `held-floor/` (`HeldFloor.tla`): a `Roll`  *)
(* ends segment 1, its replicas copy its chosen slots, a node that applied *)
(* the `Roll` publishes the end in the chain, the leader marks the segment *)
(* `Held` at `F + 1` replicas, and an acceptor raises its floor to the end *)
(* and drops the records below it. Up to `F` replicas lose their disk.     *)
(*                                                                         *)
(* The question: once the acceptors of `C1` dropped a held segment, can a  *)
(* reconfiguration onto a new, empty member lose a slot or recover a wrong *)
(* value? The new member holds nothing below the held floor: it jumps with *)
(* `TrimmedTo`, or it never holds it, and the GC may forget `C1`.          *)
(*                                                                         *)
(* Constants (each rule has a control):                                    *)
(*   HeldAt  "f_plus_one": `Held` needs `F + 1` replicas. "f": a control,  *)
(*           `F` replicas.                                                 *)
(*   HeldEnd "chain": the floor follows the end in the chain. "log": a     *)
(*           control, the end in the chosen log.                           *)
(***************************************************************************)
EXTENDS TruncationReconfig

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

ChosenVal(s) == CHOOSE v \in Values : Chosen(s, v)

-----------------------------------------------------------------------------
(* The segments of the chosen log, as in `HeldFloor.tla` *)

PrefixChosen(s) == \A t \in Slots : t < s => IsChosen(t)

\* The segment of slot s. Evaluate it only under PrefixChosen(s).
RECURSIVE SegAt(_)
SegAt(s) ==
  IF s = 0 THEN 1
  ELSE LET k == SegAt(s - 1)
           v == ChosenVal(s - 1)
       IN IF v \in Rolls /\ RollTo[v] = k + 1 THEN k + 1 ELSE k

\* The end of segment k in the chosen log, or 0.
LogEnd(k) ==
  LET e == {s \in Slots : PrefixChosen(s + 1) /\ SegAt(s) = k /\ SegAt(s + 1) = k + 1}
  IN IF e = {} THEN 0 ELSE Max(e) + 1

End(k) == IF HeldEnd = "chain" THEN chain[k] ELSE LogEnd(k)

SegSlots(k, e) == {s \in 0..(e - 1) : SegAt(s) = k}

-----------------------------------------------------------------------------
(* The steps of segments, as in `HeldFloor.tla` *)

\* A wiped acceptor learned nothing, so it gives no slot.
CanCopy(r, s) ==
  /\ PrefixChosen(s + 1)
  /\ r \in RepOf[SegAt(s)]
  /\ \/ \E n \in Acceptors : learned[n][s] # None
     \/ \E p \in Replicas : s \in rhold[p]

Copy(r) ==
  /\ rhold' = [rhold EXCEPT ![r] = @ \cup {s \in Slots : CanCopy(r, s)}]
  /\ UNCHANGED <<vars, chain, held, rlost>>

Publish(k) ==
  /\ chain' = [chain EXCEPT ![k] = LogEnd(k)]
  /\ UNCHANGED <<vars, rhold, held, rlost>>

Need == IF HeldAt = "f_plus_one" THEN F + 1 ELSE F

MarkHeld(k) ==
  /\ End(k) > 0
  /\ Cardinality({r \in RepOf[k] : SegSlots(k, End(k)) \subseteq rhold[r]}) >= Need
  /\ held' = held \cup {k}
  /\ UNCHANGED <<vars, rhold, chain, rlost>>

\* Acceptor a raises its floor to the end of the held segment k. A wiped
\* acceptor that does so changes nothing that a step reads: with Identity
\* it takes no step again.
HeldFloor(a, k) ==
  LET f == IF End(k) > floor[a] THEN End(k) ELSE floor[a]
  IN /\ k \in held
     /\ floor' = [floor EXCEPT ![a] = f]
     /\ vote' = [vote EXCEPT ![a] = DropVotes(a, f)]
     /\ learned' = [learned EXCEPT ![a] = DropLearned(a, f)]
     /\ UNCHANGED <<wiped, prom, phase, from, next, fence, hist, wm, msgs, seg>>

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

-----------------------------------------------------------------------------
(* Safety *)

InHeld(s) == \E k \in held : s < chain[k]

\* A floor is truncated or held: each slot below the floor of an acceptor
\* is at or below a chosen truncation, or in a held segment whose end the
\* chain publishes.
FloorTruncatedOrHeld ==
  \A a \in Acceptors, s \in Slots :
    s < floor[a] => (\E t \in Slots, v \in Cuts : t >= s /\ Chosen(t, v)) \/ InHeld(s)

\* A record is kept: each chosen command that no truncation released is
\* recorded at an acceptor that is up, or held by a replica, after up to
\* `F` replicas lost their disk. Here a truncation releases every record
\* before its slot.
RecordKept ==
  \A s \in Slots :
    (IsChosen(s) /\ ChosenVal(s) \in Commands /\ ~\E t \in Slots, v \in Cuts : t > s /\ Chosen(t, v)) =>
      \/ \E a \in Acceptors : Up(a) /\ (vote[a][s].val = ChosenVal(s) \/ learned[a][s] # None)
      \/ \E r \in Replicas : s \in rhold[r]

ChainTrue == \A k \in Segs : chain[k] # 0 => chain[k] = LogEnd(k)

\* Reachability controls: each must be violated.
\* The GC forgets the first configuration while a new member holds nothing
\* of a held segment that it jumped over.
NoHeldJoin == ~(wm >= R /\ \E k \in held, j \in Joiners :
                  /\ chain[k] > 0 /\ floor[j] >= chain[k]
                  /\ \A s \in 0..(chain[k] - 1) : ~\E m \in msgs : m.type = "2b" /\ m.acc = j /\ m.slot = s)
\* A ballot of the second configuration wins Phase 1 from at or above a
\* held floor. (With two ballots its `H_b` still holds the first
\* configuration; `NoHeldJoin` reaches the GC that forgets it.)
NoHeldWin == ~\E b \in Ballots, k \in held :
               b >= R /\ phase[b] = "p2" /\ chain[k] > 0 /\ from[b] >= chain[k]
=============================================================================
