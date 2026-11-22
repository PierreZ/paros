---------------------------- MODULE SegmentTruncate ----------------------------
(***************************************************************************)
(* A user truncate across segments, #1276 (user truncate across segments,  *)
(* retired and offloaded).                                                 *)
(*                                                                         *)
(* Truncation is driven by the user only: a `Truncate` decided by Paxos    *)
(* and judged at apply raises `first_seq` (docs/architecture/              *)
(* 02-data-plane.md, section 2.6). `truncation/` checks it on plain        *)
(* Multi-Paxos and `truncation-reconfig/` across one reconfiguration. This *)
(* model checks it with segments (section 2.10, design A of #1213          *)
(* (segments in the protocol or in storage)): a floor inside a sealed      *)
(* segment, a floor over a whole segment in each of its states (sealed,    *)
(* retired, offloaded), a `Truncate` decided at the seal, a lagging        *)
(* replica, and the reads of an offloaded segment.                         *)
(*                                                                         *)
(* The journal has two segments. S1 holds the slots 0 .. K - 1 and is      *)
(* sealed at K; S2 holds the slots K .. NS - 1 and is the configuration in *)
(* force. R1 is the replica set of S1 and R2 the replica set of S2, as in  *)
(* `segment-replicas/`, which checks the writer fence of the boundary      *)
(* state. This model has one writer, so it has no leader uuid. The log is  *)
(* abstract: the slots are chosen in order. A value is a `Write` of one    *)
(* record or a `Truncate(up)`.                                             *)
(*                                                                         *)
(* What the model has:                                                     *)
(*   - Learn and Apply as in `segment-replicas/`. A replica learns a slot  *)
(*     of its segment from the acceptors of the segment (until S1 retires, *)
(*     and only at or above the acceptors' floor) or from a live peer of   *)
(*     its set that holds it. No replica learns a slot of a trimmed        *)
(*     segment.                                                            *)
(*   - Compaction, lazily at the walk: a replica drops the slots below the *)
(*     slot that holds the record at its `first_seq`, or below its applied *)
(*     prefix when no record of it is kept (`truncation/`, FloorRule       *)
(*     "fold"). The acceptors of a segment compact only to a slot that     *)
(*     F + 1 replicas of its set compacted to (rule AccRule).              *)
(*   - Jump: a replica whose next slot has no source takes the trim point  *)
(*     of a live peer of its set (`TrimmedTo`, `truncation/`).             *)
(*   - The boundary state: the first replica of R1 that applied every slot *)
(*     of S1 writes the journal state at the seal into the segment record, *)
(*     once, by compare-and-set. A replica of R2 starts its fold there.    *)
(*   - The segment record of each segment has a state: open, sealed,       *)
(*     retired, offloaded, trimmed. S1 retires at RetireAt holders once    *)
(*     its boundary state is written; its acceptors are then gone. The     *)
(*     offloader puts the whole segment into the object store and marks it *)
(*     offloaded, only from retired (the compare-and-set mark of #1256     *)
(*     (offload races), which checks the races of the upload). After the   *)
(*     mark, the replicas of R1 drop their copy.                           *)
(*   - Trim: a replica whose fold has `first_seq` at or above the last     *)
(*     `seq` of S1 writes "trimmed" into the record of S1, from sealed,    *)
(*     retired or offloaded. The trim then deletes the object by its key.  *)
(*   - Read of a `seq` q: the reader takes the floor from a replica of the *)
(*     segment in force, at the basis of its leaderless read (`reads/`).   *)
(*     Below that floor the answer is "trunc". Above it the reader routes  *)
(*     by the segment records: a replica of R2 for a slot of S2; a replica *)
(*     of R1 that holds the slot for a sealed or retired S1; the object,   *)
(*     read by the replica of R2, for an offloaded S1; "trunc" for a       *)
(*     trimmed S1. A slot that is gone gives "lost".                       *)
(*   - Crash: a replica stops for good, at most F in each set.             *)
(*                                                                         *)
(* Constants select the rules. The first value of each is the design; each *)
(* other value is a control.                                               *)
(*   AccRule   "replicas": the acceptors of a segment compact only to a    *)
(*             slot that F + 1 replicas of its set compacted to. "floor":  *)
(*             they compact to the slot of the decided floor alone.        *)
(*   Boundary  "full": the boundary state holds `next_seq` and `first_seq`.*)
(*             "nofirst": it holds `next_seq` only; R2 starts with         *)
(*             `first_seq` 0.                                              *)
(*             "trimstate": it holds `next_seq` only; R2 takes `first_seq` *)
(*             from the trimmed segment records (the end of S1 if S1 is    *)
(*             trimmed when the replica starts, else 0).                   *)
(*   FloorFrom "tail": a read takes the floor from a replica of the        *)
(*             segment in force. "holder": it takes the floor from the     *)
(*             replica that holds the slot (its fold stops at the seal).   *)
(*   CacheCheck "floor": a read of the object checks the floor first.      *)
(*             "none": the replica of R2 serves the object (its cache)     *)
(*             with no check against the floor.                            *)
(*   TrimWhen  "decided": the trim of S1 waits for a replica whose fold    *)
(*             has the floor at or above the end of S1. "intent": a        *)
(*             control, it trims once a `Truncate` that covers S1 exists,  *)
(*             before it is decided.                                       *)
(*   TrimOpen  FALSE: only a sealed segment can be trimmed. TRUE: a        *)
(*             control, the open segment S2 is trimmed when its `seq`      *)
(*             range so far is below the floor.                            *)
(*   Jump      TRUE: a laggard jumps to the trim point of a peer. FALSE: a *)
(*             control, it waits for the slot.                             *)
(*   Reprop    the number of the top slots of S1 that the sealing leader   *)
(*             proposed again in its own grid (docs/journal §J3.1): their  *)
(*             votes live at the acceptors of the grid of S2, but they     *)
(*             belong to S1 and its replica set R1.                        *)
(*   AccKey    "segment": the acceptors keep a floor for each segment, and *)
(*             a slot follows the floor and the retire of its own segment, *)
(*             in whichever grid its vote lives (#1318). "grid": a control,*)
(*             the acceptors of a grid keep one floor, counted on the      *)
(*             replica set of the segment in force; it covers the slots of *)
(*             S1 that live in the grid of S2.                             *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS R1, R2, K, NS, F, Vals, None, RetireAt, ReadSeqs,
          AccRule, Boundary, FloorFrom, CacheCheck, TrimWhen, TrimOpen, Jump,
          Reprop, AccKey

ASSUME R1 # {} /\ R2 # {} /\ R1 \cap R2 = {}
ASSUME K \in 1..NS /\ NS \in Nat
ASSUME F \in Nat /\ RetireAt \in Nat
ASSUME AccRule \in {"replicas", "floor"}
ASSUME Boundary \in {"full", "nofirst", "trimstate"}
ASSUME FloorFrom \in {"tail", "holder"}
ASSUME CacheCheck \in {"floor", "none"}
ASSUME TrimWhen \in {"decided", "intent"}
ASSUME TrimOpen \in BOOLEAN /\ Jump \in BOOLEAN
ASSUME Reprop \in 0..K /\ AccKey \in {"segment", "grid"}


Replicas == R1 \cup R2
Slots == 0..(NS - 1)
SlotsOf(g) == IF g = 1 THEN 0..(K - 1) ELSE K..(NS - 1)
Start(g) == IF g = 1 THEN 0 ELSE K
End(g) == IF g = 1 THEN K ELSE NS
Seg(s) == IF s < K THEN 1 ELSE 2
SetOf(r) == IF r \in R1 THEN 1 ELSE 2
Set(g) == IF g = 1 THEN R1 ELSE R2
\* The `seq` values that a reader asks for (at most one record per slot).
ASSUME ReadSeqs \subseteq 0..(NS - 1)
Seqs == ReadSeqs

(***************************************************************************)
(* The journal state and its fold (02-data-plane.md, sections 2.1 and      *)
(* 2.6). A `Truncate` never lowers `first_seq` and is clamped to           *)
(* `next_seq`.                                                             *)
(***************************************************************************)
Empty == [next |-> 0, first |-> 0]
Max(a, b) == IF a > b THEN a ELSE b
Min(a, b) == IF a < b THEN a ELSE b

Step(st, v) ==
  IF v.op = "w"
  THEN [st EXCEPT !.next = st.next + 1]
  ELSE [st EXCEPT !.first = Max(st.first, Min(v.up, st.next))]

VARIABLES
  chosen,   \* the chosen value of each slot, or None
  held,     \* the slots that each replica holds
  applied,  \* how many slots of its segment each replica applied
  base,     \* each replica's boundary state (R2), or None
  bnd,      \* the boundary state in the record of S1, or None
  state,    \* the state in the record of each segment
  accFloor, \* the floor of the acceptors of each segment
  obj,      \* the object of S1 is in the object store
  down,     \* the replicas that stopped
  rd        \* the read of each `seq`, or None

vars == <<chosen, held, applied, base, bnd, state, accFloor, obj, down, rd>>

RECURSIVE FoldTo(_, _, _)
\* The state after the chosen values of the slots lo .. hi - 1, from st.
FoldTo(st, lo, hi) == IF lo >= hi THEN st ELSE FoldTo(Step(st, chosen[lo]), lo + 1, hi)

Frontier == CHOOSE n \in 0..NS : (\A s \in 0..(n - 1) : chosen[s] # None) /\
                                 (n = NS \/ chosen[n] = None)
\* The true journal state before slot s, and now.
Truth(s) == FoldTo(Empty, 0, s)
Floor == Truth(Frontier).first
S1Full == Frontier >= K

\* The slot that holds the record at `seq` q, or None.
SlotOf(q) ==
  LET hits == {s \in 0..(Frontier - 1) : chosen[s].op = "w" /\ Truth(s).next = q}
  IN IF hits = {} THEN None ELSE CHOOSE s \in hits : TRUE

BaseOf(r) == IF SetOf(r) = 1 THEN Empty ELSE base[r]
Ready(r) == BaseOf(r) # None
Front(r) == Start(SetOf(r)) + applied[r]
StateOf(r) == FoldTo(BaseOf(r), Start(SetOf(r)), Front(r))
SeqAt(r, s) == FoldTo(BaseOf(r), Start(SetOf(r)), s).next

\* The compaction target of a replica: the slot that holds the record at its
\* `first_seq`, or its applied prefix when its segment keeps no such record.
CompactTo(r) ==
  LET keep == {s \in Start(SetOf(r))..(Front(r) - 1) :
                 chosen[s].op = "w" /\ SeqAt(r, s) >= StateOf(r).first}
  IN IF keep = {} THEN Front(r) ELSE CHOOSE s \in keep : \A t \in keep : s <= t

\* The acceptors of segment g compact only to a slot that F + 1 replicas of
\* its set compacted to: each of them applied and folded the slots below it,
\* so a live one can give a laggard its trim point. With AccRule "floor" (a
\* control) they compact to the decided floor alone.
FloorSlot(g) ==
  LET keep == {s \in 0..(Frontier - 1) : chosen[s].op = "w" /\ Truth(s).next >= Floor}
      slot == IF keep = {} THEN Frontier ELSE CHOOSE s \in keep : \A t \in keep : s <= t
  IN Max(Start(g), Min(slot, End(g)))
Compacted(g, x) == Cardinality({r \in Set(g) : Ready(r) /\ CompactTo(r) >= x}) >= F + 1
\* The lowest slot of the floor of g: with AccKey "grid", the floor of the
\* grid of S2 also covers the slots of S1 proposed again there.
AccStart(g) == IF g = 2 /\ AccKey = "grid" THEN K - Reprop ELSE Start(g)
AccTarget(g) ==
  IF AccRule = "floor" THEN FloorSlot(g)
  ELSE CHOOSE x \in AccStart(g)..End(g) :
         /\ x = AccStart(g) \/ Compacted(g, x)
         /\ \A y \in AccStart(g)..End(g) : y > x => ~Compacted(g, y)

Live(g) == Set(g) \ down
Holders == {r \in R1 : Front(r) = K}
Count2 == Max(0, Frontier - K)
\* The acceptors of a segment still answer.
AccLive(g) == IF g = 1 THEN state[1] = "sealed" ELSE state[2] = "open"
\* The slot s is at the acceptors. A slot of S1 proposed again lives in the
\* grid of S2: with AccKey "segment" it follows the floor and the retire of
\* S1, with AccKey "grid" those of the grid of S2.
Moved(s) == Seg(s) = 1 /\ s >= K - Reprop
AccHolds(s) ==
  IF AccKey = "grid" /\ Moved(s)
  THEN AccLive(2) /\ s >= accFloor[2]
  ELSE AccLive(Seg(s)) /\ s >= accFloor[Seg(s)]

TypeOK ==
  /\ \A s \in Slots : chosen[s] \in Vals \cup {None}
  /\ held \in [Replicas -> SUBSET Slots]
  /\ applied \in [Replicas -> 0..NS]
  /\ state \in [{1, 2} -> {"open", "sealed", "retired", "offloaded", "trimmed"}]
  /\ accFloor \in [{1, 2} -> 0..NS]
  /\ obj \in BOOLEAN
  /\ down \subseteq Replicas

Init ==
  /\ chosen = [s \in Slots |-> None]
  /\ held = [r \in Replicas |-> {}]
  /\ applied = [r \in Replicas |-> 0]
  /\ base = [r \in Replicas |-> None]
  /\ bnd = None
  /\ state = [g \in {1, 2} |-> IF g = 1 THEN "sealed" ELSE "open"]
  /\ accFloor = [g \in {1, 2} |-> AccStart(g)]
  /\ obj = FALSE
  /\ down = {}
  /\ rd = [q \in Seqs |-> None]

\* The leader chooses the next slot. Only the control TrimOpen trims S2, and
\* that run must break TrimTrue: no check needs a rule for a trimmed S2 here.
Choose(v) ==
  /\ Frontier < NS
  /\ chosen' = [chosen EXCEPT ![Frontier] = v]
  /\ UNCHANGED <<held, applied, base, bnd, state, accFloor, obj, down, rd>>

\* A replica gets a chosen slot of its segment from the acceptors or a peer.
\* A slot can also reach a replica that is down: it counts nowhere, because
\* a down replica applies and serves nothing, and Findable reads the live
\* replicas only.
Learn(r, s) ==
  /\ chosen[s] # None
  /\ Seg(s) = SetOf(r)
  /\ s >= Front(r)
  /\ s \notin held[r]
  /\ state[Seg(s)] # "trimmed"
  /\ \/ AccHolds(s)
     \/ \E p \in Live(Seg(s)) : s \in held[p]
  /\ held' = [held EXCEPT ![r] = @ \cup {s}]
  /\ UNCHANGED <<chosen, applied, base, bnd, state, accFloor, obj, down, rd>>

Apply(r) ==
  /\ r \notin down
  /\ Ready(r)
  /\ Front(r) < End(SetOf(r))
  /\ Front(r) \in held[r]
  /\ applied' = [applied EXCEPT ![r] = @ + 1]
  /\ UNCHANGED <<chosen, held, base, bnd, state, accFloor, obj, down, rd>>

\* The replica drops the slots below its compaction target.
Compact(r) ==
  /\ r \notin down
  /\ Ready(r)
  /\ \E s \in held[r] : s < CompactTo(r)
  /\ held' = [held EXCEPT ![r] = {s \in @ : s >= CompactTo(r)}]
  /\ UNCHANGED <<chosen, applied, base, bnd, state, accFloor, obj, down, rd>>

AccCompact(g) ==
  /\ AccLive(g)
  /\ AccTarget(g) > accFloor[g]
  /\ accFloor' = [accFloor EXCEPT ![g] = AccTarget(g)]
  /\ UNCHANGED <<chosen, held, applied, base, bnd, state, obj, down, rd>>

\* A laggard whose next slot has no source takes the trim point of a live
\* peer: the peer's compaction target and its fold there. No bytes move.
NoSource(r) ==
  LET s == Front(r) IN
  /\ ~AccHolds(s)
  /\ ~\E p \in Live(SetOf(r)) : s \in held[p]
JumpTo(r) ==
  /\ Jump
  /\ r \notin down
  /\ Ready(r)
  /\ Front(r) < End(SetOf(r))
  /\ Front(r) \notin held[r]
  /\ chosen[Front(r)] # None
  /\ state[SetOf(r)] # "trimmed"
  /\ NoSource(r)
  /\ \E p \in Live(SetOf(r)) :
       /\ Ready(p)
       /\ CompactTo(p) > Front(r)
       /\ applied' = [applied EXCEPT ![r] = CompactTo(p) - Start(SetOf(r))]
       /\ held' = [held EXCEPT ![r] = {s \in @ : s >= CompactTo(p)}]
  /\ UNCHANGED <<chosen, base, bnd, state, accFloor, obj, down, rd>>

\* A replica of R1 that applied every slot of S1 writes the boundary state.
WriteBoundary(p) ==
  /\ p \in R1 \ down
  /\ bnd = None
  /\ Front(p) = K
  /\ bnd' = StateOf(p)
  /\ UNCHANGED <<chosen, held, applied, base, state, accFloor, obj, down, rd>>

\* A replica of R2 takes its boundary state from the record.
TakeBase(r) ==
  /\ r \in R2 \ down
  /\ base[r] = None
  /\ bnd # None
  /\ base' = [base EXCEPT ![r] =
        CASE Boundary = "full" -> bnd
          [] Boundary = "nofirst" -> [bnd EXCEPT !.first = 0]
          [] Boundary = "trimstate" ->
               [bnd EXCEPT !.first = IF state[1] = "trimmed" THEN bnd.next ELSE 0]]
  /\ UNCHANGED <<chosen, held, applied, bnd, state, accFloor, obj, down, rd>>

Retire ==
  /\ state[1] = "sealed"
  /\ S1Full
  /\ bnd # None
  /\ Cardinality(Holders \ down) >= RetireAt
  /\ state' = [state EXCEPT ![1] = "retired"]
  /\ UNCHANGED <<chosen, held, applied, base, bnd, accFloor, obj, down, rd>>

\* The offloader puts the whole segment and marks it, from retired only.
Offload ==
  /\ state[1] = "retired"
  /\ state' = [state EXCEPT ![1] = "offloaded"]
  /\ obj' = TRUE
  /\ UNCHANGED <<chosen, held, applied, base, bnd, accFloor, down, rd>>

\* After the mark (or the trim), a replica of R1 drops its copy.
Drop(r) ==
  /\ r \in R1
  /\ state[1] \in {"offloaded", "trimmed"}
  /\ held[r] # {}
  /\ held' = [held EXCEPT ![r] = {}]
  /\ UNCHANGED <<chosen, applied, base, bnd, state, accFloor, obj, down, rd>>

\* The floor that a replica's fold knows covers every `seq` of S1.
FloorCovers(e) ==
  \E r \in Replicas \ down : Ready(r) /\ StateOf(r).first >= e

\* The record of S1 becomes trimmed.
Trim ==
  /\ state[1] \in {"sealed", "retired", "offloaded"}
  /\ bnd # None
  /\ CASE TrimWhen = "decided" -> FloorCovers(bnd.next)
       [] TrimWhen = "intent" -> \E v \in Vals : v.op = "t" /\ v.up >= bnd.next
  /\ state' = [state EXCEPT ![1] = "trimmed"]
  /\ UNCHANGED <<chosen, held, applied, base, bnd, accFloor, obj, down, rd>>

\* A control: the open segment is trimmed when its `seq` range so far is
\* below the floor that a replica of R2 knows.
TrimS2 ==
  /\ TrimOpen
  /\ state[2] = "open"
  /\ \E r \in R2 \ down : Ready(r) /\ StateOf(r).first >= StateOf(r).next
  /\ state' = [state EXCEPT ![2] = "trimmed"]
  /\ UNCHANGED <<chosen, held, applied, base, bnd, accFloor, obj, down, rd>>

\* The trim deletes the object by its key.
DeleteObject ==
  /\ state[1] = "trimmed"
  /\ obj
  /\ obj' = FALSE
  /\ UNCHANGED <<chosen, held, applied, base, bnd, state, accFloor, down, rd>>

\* A reader asks for `seq` q, which was written. The replica t of R2 gives
\* the floor at the basis of its leaderless read.
StartRead(q) ==
  /\ rd[q] = None
  /\ q < Truth(Frontier).next
  /\ \E t \in Live(2) :
       /\ Ready(t)
       /\ rd' = [rd EXCEPT ![q] = [t |-> t, basis |-> Count2, fl0 |-> Floor, ans |-> None]]
  /\ UNCHANGED <<chosen, held, applied, base, bnd, state, accFloor, obj, down>>

Reroute(q) ==
  /\ rd[q] # None
  /\ rd[q].ans = None
  /\ rd[q].t \in down
  /\ \E t \in Live(2) :
       /\ Ready(t)
       /\ rd' = [rd EXCEPT ![q] = [t |-> t, basis |-> Count2, fl0 |-> @.fl0, ans |-> None]]
  /\ UNCHANGED <<chosen, held, applied, base, bnd, state, accFloor, obj, down>>

Answer(q, a) ==
  /\ rd' = [rd EXCEPT ![q].ans = a]
  /\ UNCHANGED <<chosen, held, applied, base, bnd, state, accFloor, obj, down>>

Serve(q) ==
  /\ rd[q] # None
  /\ rd[q].ans = None
  /\ LET t == rd[q].t
         s == SlotOf(q)
         fl == StateOf(t).first
     IN
     /\ t \notin down
     /\ applied[t] >= rd[q].basis
     /\ IF Seg(s) = 2
        THEN IF q < fl THEN Answer(q, "trunc")
             ELSE IF s \in held[t] THEN Answer(q, "data") ELSE Answer(q, "lost")
        ELSE CASE state[1] \in {"sealed", "retired"} ->
                    \E d \in Live(1) :
                      /\ s \in held[d]
                      /\ IF q < (IF FloorFrom = "tail" THEN fl ELSE StateOf(d).first)
                         THEN Answer(q, "trunc") ELSE Answer(q, "data")
               [] state[1] = "offloaded" ->
                    IF q < fl /\ CacheCheck = "floor" THEN Answer(q, "trunc")
                    ELSE IF obj THEN Answer(q, "data") ELSE Answer(q, "lost")
               [] state[1] = "trimmed" -> Answer(q, "trunc")

Crash(r) ==
  /\ r \notin down
  /\ Cardinality(down \cap Set(SetOf(r))) < F
  /\ down' = down \cup {r}
  /\ UNCHANGED <<chosen, held, applied, base, bnd, state, accFloor, obj, rd>>

Next ==
  \/ \E v \in Vals : Choose(v)
  \/ \E r \in Replicas, s \in Slots : Learn(r, s)
  \/ \E r \in Replicas : Apply(r) \/ Compact(r) \/ JumpTo(r) \/ WriteBoundary(r)
                         \/ TakeBase(r) \/ Drop(r) \/ Crash(r)
  \/ \E g \in {1, 2} : AccCompact(g)
  \/ Retire \/ Offload \/ Trim \/ TrimS2 \/ DeleteObject
  \/ \E q \in Seqs : StartRead(q) \/ Reroute(q) \/ Serve(q)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Safety.                                                                 *)
(***************************************************************************)
\* Every replica's fold is the fold of the whole log.
FoldTrue == \A r \in Replicas : Ready(r) => StateOf(r) = Truth(Front(r))

\* The record holds the journal state at the seal.
BoundaryTrue == bnd # None => S1Full /\ bnd = Truth(K)

\* No read returns a record below the floor that was decided when it started.
ReadFloor == \A q \in Seqs : rd[q] # None /\ rd[q].ans = "data" => q >= rd[q].fl0

\* No record at or above the floor is lost: a read never finds it gone, and
\* "trunc" is the answer only below the floor.
ReadKeep == \A q \in Seqs : rd[q] # None =>
  /\ rd[q].ans # "lost"
  /\ rd[q].ans = "trunc" => q < Floor

\* A trimmed record covers only a sealed segment wholly below the floor.
TrimTrue ==
  /\ state[2] # "trimmed"
  /\ state[1] = "trimmed" => bnd # None /\ bnd.next <= Floor

\* Each record at or above the floor is somewhere: at a live replica of its
\* set, at the acceptors of its segment, or in the object.
Findable == \A s \in 0..(Frontier - 1) :
  chosen[s].op = "w" /\ Truth(s).next >= Floor =>
    \/ \E r \in Live(Seg(s)) : s \in held[r]
    \/ AccHolds(s)
    \/ Seg(s) = 1 /\ state[1] = "offloaded" /\ obj

\* A trimmed record never becomes live again; the object of a trimmed
\* segment never comes back.
TrimStays == [][/\ \A g \in {1, 2} : state[g] = "trimmed" => state'[g] = "trimmed"
                /\ state[1] = "trimmed" /\ ~obj => ~obj']_vars

(***************************************************************************)
(* Liveness. Every step is bounded, so a run ends in a state where no step *)
(* is enabled. In that state every read was served, every live replica of  *)
(* R2 applied the whole log, S1 is offloaded or trimmed (trimmed when the  *)
(* floor covers it), and the object of a trimmed S1 is deleted.            *)
(***************************************************************************)
Goal ==
  /\ \A q \in Seqs : rd[q] # None => rd[q].ans # None
  /\ \A r \in Live(2) : Front(r) = NS
  /\ state[1] \in {"offloaded", "trimmed"}
  /\ (bnd # None /\ bnd.next <= Floor) => state[1] = "trimmed"
  /\ state[1] = "trimmed" => ~obj

Ends == (~ENABLED Next) => Goal

(***************************************************************************)
(* Reach: a check that must be violated, so that a passing check is not    *)
(* empty.                                                                  *)
(***************************************************************************)
\* The floor lands inside S1 and a read of S1 above it is served.
NoReadInside == ~\E q \in Seqs :
  /\ bnd # None /\ Floor > 0 /\ Floor < bnd.next
  /\ rd[q] # None /\ rd[q].ans = "data" /\ SlotOf(q) < K
\* A read of the offloaded S1 is served from the object above the floor.
NoObjectRead == ~(state[1] = "offloaded" /\ Floor > 0 /\
                  \E q \in Seqs : rd[q] # None /\ rd[q].ans = "data" /\ SlotOf(q) < K)
\* S1 is trimmed from each of its states.
NoTrimFromOffloaded == [][~(state[1] = "offloaded" /\ state'[1] = "trimmed")]_vars
NoTrimFromRetired == [][~(state[1] = "retired" /\ state'[1] = "trimmed")]_vars
NoTrimFromSealed == [][~(state[1] = "sealed" /\ state'[1] = "trimmed")]_vars
\* A `Truncate` at the last slot of S1 lands in the boundary state.
NoSealTruncate == ~(bnd # None /\ bnd.first > 0)
\* A laggard jumps.
NoJump == [][\A r \in Replicas : applied'[r] <= applied[r] + 1]_vars
================================================================================
