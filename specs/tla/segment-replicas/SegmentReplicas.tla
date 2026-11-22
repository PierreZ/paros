---------------------------- MODULE SegmentReplicas ----------------------------
(***************************************************************************)
(* A write reaches only the replica set of its segment, #1271 (a write     *)
(* reaches only the replica set of its segment).                           *)
(*                                                                         *)
(* Compartmentalized Paxos (docs/references/papers/                        *)
(* scaling-rsm-compartmentalization, sections 3.3 and 8.5) says that every *)
(* replica executes every write, so write throughput has a bound however   *)
(* many replicas there are. In design A (#1213 (segments in the protocol   *)
(* or in storage), docs/architecture/02-data-plane.md section 2.10) each   *)
(* segment has its own replica set. This model checks that the journal     *)
(* stays correct and live when a write of a segment goes only to the       *)
(* replicas of that segment.                                               *)
(*                                                                         *)
(* The journal has two segments. S1 holds the slots 0 .. K - 1 and is      *)
(* sealed at K; S2 holds the slots K .. NS - 1 and is the configuration in *)
(* force. R1 is the replica set of S1, R2 the replica set of S2, and the   *)
(* two sets are disjoint. The log is abstract: the leader chooses the      *)
(* slots in order, one value for each slot (`multi-paxos/` checks the      *)
(* agreement, `experiments/segments/a/` the seal end and the gap fill).    *)
(*                                                                         *)
(* A replica holds opaque bytes, but it also folds the journal state, and  *)
(* the fold is judged at apply (02-data-plane.md, sections 2.1 and 2.2): a *)
(* `Write` is accepted only under the current leader uuid, and it gets the *)
(* `seq` range `[next_seq, next_seq + n)`. So a replica of S2 that never   *)
(* sees the slots of S1 must still start its fold from the journal state   *)
(* at the seal: `next_seq` and the leader uuid. The model calls it the     *)
(* boundary state. Where the replicas of S2 get it is the rule `Boundary`. *)
(*                                                                         *)
(* What the model has:                                                     *)
(*   - Learn: a replica gets a chosen slot from the `Commit` of the leader *)
(*     or from the acceptors of the slot's grid (until the segment         *)
(*     retires), or from a live peer of the same replica set.              *)
(*   - Apply: a replica walks the slots of its segment in order, from the  *)
(*     boundary state (S2) or from the empty state (S1).                   *)
(*   - Ack: a live replica of the segment that applied a slot answers the  *)
(*     writer with the result of its fold.                                 *)
(*   - Read: a reader reads the `seq` of an acked write, at one replica of *)
(*     the segment that holds it. The replica first does the leaderless    *)
(*     read of its own segment (the number of chosen slots of the segment  *)
(*     when the read starts, the basis that `reads/` checks) and answers   *)
(*     when its applied prefix covers that basis: the slot that holds the  *)
(*     `seq`, "tail" when the `seq` is at or above its `next_seq`, or      *)
(*     "truncated" when the `seq` is below the first `seq` that it knows.  *)
(*   - Retire: S1 retires once RetireAt replicas of R1 hold every slot of  *)
(*     S1 and, with the boundary in the record, once the record holds it.  *)
(*     After that its acceptors are gone.                                  *)
(*   - Crash: a replica stops for good. At most F replicas of each set     *)
(*     crash, at any time.                                                 *)
(*   - Vanish (with `Gone`): once S1 retired, every replica of R1 stops,   *)
(*     as when the segment is offloaded and the replicas drop their copy.  *)
(*                                                                         *)
(* Constants select the rules. The first value of each is the design; each *)
(* other value is a control.                                               *)
(*   Deliver   "segment": a replica gets only the slots of its segment.    *)
(*             "all": every replica gets every slot (the paper's replica). *)
(*   Boundary  "record": the first replica of R1 that applied every slot   *)
(*             of S1 writes the boundary state once into the segment       *)
(*             record (a compare-and-set at the matchmakers), and the      *)
(*             replicas of S2 read it there.                               *)
(*             "peer": a replica of S2 asks a live replica of R1 that      *)
(*             applied every slot of S1.                                   *)
(*             "zero": a replica of S2 starts from the empty state.        *)
(*             "early": as "record", but the record may be written before  *)
(*             the writer applied every slot of S1.                        *)
(*             "seq": as "record", but the record holds `next_seq` only;   *)
(*             a replica of S2 starts with the leader uuid of the empty    *)
(*             state.                                                      *)
(*             "meta": the segment record holds `next_seq` only, and the   *)
(*             leader uuid goes to the journal's metadata at the           *)
(*             matchmakers, in the same compare-and-set. The first leader  *)
(*             of S2 reads it there and proposes it as the first slot of   *)
(*             S2, a `Fence(u)` that sets the leader uuid. A replica of S2 *)
(*             takes `next_seq` from the record and the leader uuid from   *)
(*             that slot, so a later change of the metadata cannot reach   *)
(*             its fold.                                                   *)
(*             "meta_stale": as "meta", but the first leader of S2 may     *)
(*             read the metadata before the seal wrote it.                 *)
(*   Route     "segment": a read goes to a replica of the set of the       *)
(*             segment that holds its `seq`.                               *)
(*             "other": a read goes to a replica of the other set.         *)
(*   RetireAt  how many replicas of R1 must hold S1 before it retires.     *)
(*   RetireRecord TRUE: with the boundary in the record, S1 retires only   *)
(*             once the record holds it. FALSE: a control, it may retire   *)
(*             before.                                                     *)
(*   Gone      TRUE: the replicas of R1 may all stop after the retire.     *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS R1, R2, K, NS, F, Vals, U0, None, Deliver, Boundary, Route, RetireAt,
          RetireRecord, Gone

ASSUME R1 # {} /\ R2 # {} /\ R1 \cap R2 = {}
ASSUME K \in 1..NS /\ NS \in Nat
ASSUME F \in Nat /\ RetireAt \in Nat
ASSUME Deliver \in {"segment", "all"}
ASSUME Boundary \in {"record", "peer", "zero", "early", "seq", "meta", "meta_stale"}

\* The modes where the record holds `next_seq` and the metadata the uuid.
MetaMode == Boundary \in {"meta", "meta_stale"}
\* The modes where a replica of R1 writes the record.
RecordMode == Boundary \in {"record", "early", "seq", "meta", "meta_stale"}
ASSUME Route \in {"segment", "other"}
ASSUME Gone \in BOOLEAN /\ RetireRecord \in BOOLEAN

Replicas == R1 \cup R2
Slots == 0..(NS - 1)
SlotsOf(g) == IF g = 1 THEN 0..(K - 1) ELSE K..(NS - 1)
Start(g) == IF g = 1 THEN 0 ELSE K
Seg(s) == IF s < K THEN 1 ELSE 2
SetOf(r) == IF r \in R1 THEN 1 ELSE 2
Set(g) == IF g = 1 THEN R1 ELSE R2
Other(g) == 3 - g
\* The answers of a read, beside a slot: still open, at or above the
\* replica's `next_seq`, below the first `seq` that the replica knows.
Wait == -1
Tail == -2
Truncated == -3

(***************************************************************************)
(* The journal state and its fold (02-data-plane.md, sections 2.1-2.3).    *)
(* A value is a `Write` of `n` records under leader uuid `u`, or a         *)
(* `SetLeader(new, old)`, a compare-and-set of the leader uuid, or (in the *)
(* "meta" modes, at the first slot of S2 only) a `Fence(u)` that sets it.  *)
(***************************************************************************)
Empty == [next |-> 0, leader |-> U0]

Step(st, v) ==
  IF v.op = "w"
  THEN IF v.u = st.leader THEN [st EXCEPT !.next = st.next + v.n] ELSE st
  ELSE IF v.op = "fence" THEN [st EXCEPT !.leader = v.u]
  ELSE IF v.old = st.leader THEN [st EXCEPT !.leader = v.new] ELSE st

\* The answer of a slot: the first `seq` of an accepted write, or -1 (a
\* refused write, a `SetLeader`).
Res(st, v) == IF v.op = "w" /\ v.u = st.leader THEN st.next ELSE -1

VARIABLES
  chosen,   \* the chosen value of each slot, or None
  held,     \* the chosen slots that each replica holds
  applied,  \* how many slots of its segment each replica applied
  base,     \* each replica's boundary state, or None
  rec,      \* the boundary state in the record of S1, or None
  meta,     \* the leader uuid in the journal's metadata, or None
  retired,  \* S1 retired: its acceptors are gone
  down,     \* the replicas that stopped
  acked,    \* the answer that the writer of each slot got, or None
  rd        \* the read of each acked write, or None

vars == <<chosen, held, applied, base, rec, meta, retired, down, acked, rd>>

RECURSIVE FoldTo(_, _, _)
\* The state after the chosen values of the slots lo .. hi - 1, from st.
FoldTo(st, lo, hi) == IF lo >= hi THEN st ELSE FoldTo(Step(st, chosen[lo]), lo + 1, hi)

\* The fold of the journal: a `Fence` only repeats what the log already
\* decided, so the true state ignores it. A `Fence` with the wrong uuid
\* makes a replica's fold differ from this one.
Step0(st, v) == IF v.op = "fence" THEN st ELSE Step(st, v)
RECURSIVE Fold0(_, _, _)
Fold0(st, lo, hi) == IF lo >= hi THEN st ELSE Fold0(Step0(st, chosen[lo]), lo + 1, hi)

\* The true journal state before slot s: the fold of the whole log.
Truth(s) == Fold0(Empty, 0, s)

\* A replica's starting state: empty for S1, its boundary state for S2.
BaseOf(r) == IF SetOf(r) = 1 THEN Empty ELSE base[r]
Ready(r) == BaseOf(r) # None
Front(r) == Start(SetOf(r)) + applied[r]
StateOf(r) == FoldTo(BaseOf(r), Start(SetOf(r)), Front(r))
ResAt(r, s) == Res(FoldTo(BaseOf(r), Start(SetOf(r)), s), chosen[s])

\* The answer of a replica to a read of `seq` q.
Lookup(r, q) ==
  LET hits == {s \in Start(SetOf(r))..(Front(r) - 1) :
                 chosen[s].op = "w" /\ ResAt(r, s) >= 0 /\
                 ResAt(r, s) <= q /\ q < ResAt(r, s) + chosen[s].n}
  IN IF hits # {} THEN CHOOSE s \in hits : TRUE
     ELSE IF q >= StateOf(r).next THEN Tail ELSE Truncated

Live(g) == Set(g) \ down
Holders == {r \in R1 : SlotsOf(1) \subseteq held[r]}
Count(g) == Cardinality({s \in SlotsOf(g) : chosen[s] # None})
Frontier == CHOOSE n \in 0..NS : (\A s \in 0..(n - 1) : chosen[s] # None) /\
                                 (n = NS \/ chosen[n] = None)
S1Full == \A s \in SlotsOf(1) : chosen[s] # None

TypeOK ==
  /\ \A s \in Slots : chosen[s] \in Vals \cup {None} \/ (MetaMode /\ s = K /\ chosen[s].op = "fence")
  /\ held \in [Replicas -> SUBSET Slots]
  /\ applied \in [Replicas -> 0..NS]
  /\ retired \in BOOLEAN
  /\ down \subseteq Replicas

Init ==
  /\ chosen = [s \in Slots |-> None]
  /\ held = [r \in Replicas |-> {}]
  /\ applied = [r \in Replicas |-> 0]
  /\ base = [r \in Replicas |-> None]
  /\ rec = None
  /\ meta = None
  /\ retired = FALSE
  /\ down = {}
  /\ acked = [s \in Slots |-> None]
  /\ rd = [s \in Slots |-> None]

\* The leader chooses the next slot. Once every slot is chosen, Frontier is
\* NS, outside the log, and the step changes nothing.
Choose(v) ==
  /\ ~(MetaMode /\ Frontier = K)
  /\ chosen' = [chosen EXCEPT ![Frontier] = v]
  /\ UNCHANGED <<held, applied, base, rec, meta, retired, down, acked, rd>>

\* The first leader of S2 proposes the leader uuid that it read in the
\* journal's metadata as the first slot of S2. With "meta_stale" it may read
\* the metadata before the seal wrote it, and finds the first uuid.
\* The metadata holds a uuid only once a replica of R1 applied all of S1
\* (WriteRecord), so with Boundary = "meta" the slots below K are chosen;
\* once K is chosen, the step writes the same fence again.
ChooseFence ==
  /\ MetaMode
  /\ K < NS
  /\ Boundary = "meta" => meta # None
  /\ chosen' = [chosen EXCEPT ![K] = [op |-> "fence", u |-> IF meta = None THEN U0 ELSE meta]]
  /\ UNCHANGED <<held, applied, base, rec, meta, retired, down, acked, rd>>

\* A replica gets a chosen slot. With "segment", only a slot of its own
\* segment. The source is the leader's `Commit` or the slot's acceptors
\* (gone once S1 retired), or a live peer of the slot's replica set.
Learn(r, s) ==
  /\ r \notin down
  /\ chosen[s] # None
  /\ s \notin held[r]
  /\ Deliver = "all" \/ Seg(s) = SetOf(r)
  /\ \/ Seg(s) = 2 \/ ~retired
     \/ \E p \in Live(Seg(s)) : s \in held[p]
  /\ held' = [held EXCEPT ![r] = @ \cup {s}]
  /\ UNCHANGED <<chosen, applied, base, rec, meta, retired, down, acked, rd>>

\* A replica applies the next slot of its segment.
Apply(r) ==
  /\ r \notin down
  /\ Ready(r)
  /\ Front(r) \in SlotsOf(SetOf(r))
  /\ Front(r) \in held[r]
  /\ applied' = [applied EXCEPT ![r] = @ + 1]
  /\ UNCHANGED <<chosen, held, base, rec, meta, retired, down, acked, rd>>

\* A replica of R1 writes the boundary state into the record of S1. The
\* code writes it once (a compare-and-set); a second write writes the same
\* state, since each replica of R1 folds the same slots up to K.
WriteRecord(p) ==
  /\ RecordMode
  /\ p \in R1 \ down
  /\ Boundary # "early" => Front(p) = K
  /\ rec' = StateOf(p)
  /\ meta' = IF MetaMode THEN StateOf(p).leader ELSE meta
  /\ UNCHANGED <<chosen, held, applied, base, retired, down, acked, rd>>

\* A replica of R2 takes its boundary state.
TakeBase(r) ==
  /\ r \in R2 \ down
  /\ base[r] = None
  /\ CASE Boundary \in {"record", "early"} ->
            rec # None /\ base' = [base EXCEPT ![r] = rec]
       [] Boundary = "peer" ->
            \E p \in Live(1) : Front(p) = K /\ base' = [base EXCEPT ![r] = StateOf(p)]
       [] Boundary = "zero" -> base' = [base EXCEPT ![r] = Empty]
       [] Boundary \in {"seq", "meta", "meta_stale"} ->
            rec # None /\ base' = [base EXCEPT ![r] = [Empty EXCEPT !.next = rec.next]]
  /\ UNCHANGED <<chosen, held, applied, rec, meta, retired, down, acked, rd>>

\* A live replica of the slot's segment that applied it answers the writer.
Ack(s) ==
  /\ chosen[s] # None
  /\ acked[s] = None
  /\ \E r \in Live(Seg(s)) :
       /\ Ready(r)
       /\ s < Front(r)
       /\ acked' = [acked EXCEPT ![s] = ResAt(r, s)]
  /\ UNCHANGED <<chosen, held, applied, base, rec, meta, retired, down, rd>>

\* The set that a read of the write at slot s goes to.
Target(s) == IF Route = "segment" THEN Seg(s) ELSE Other(Seg(s))

\* A reader reads the first `seq` of an accepted write that was acked. The
\* replica's leaderless read fixes the basis: the chosen slots of its own
\* segment when the read starts.
StartRead(s) ==
  /\ acked[s] # None
  /\ acked[s] >= 0
  /\ rd[s] = None
  /\ \E r \in Live(Target(s)) :
       rd' = [rd EXCEPT ![s] = [rep |-> r, basis |-> Count(SetOf(r)), ans |-> Wait]]
  /\ UNCHANGED <<chosen, held, applied, base, rec, meta, retired, down, acked>>

\* The replica of an open read stopped: the reader asks another replica of
\* the same set.
Reroute(s) ==
  /\ rd[s] # None
  /\ rd[s].ans = Wait
  /\ rd[s].rep \in down
  /\ \E r \in Live(SetOf(rd[s].rep)) :
       rd' = [rd EXCEPT ![s] = [rep |-> r, basis |-> Count(SetOf(r)), ans |-> Wait]]
  /\ UNCHANGED <<chosen, held, applied, base, rec, meta, retired, down, acked>>

\* The replica's applied prefix covers the basis: it answers. A replica that
\* stopped may answer too: its answer left before it stopped.
Serve(s) ==
  /\ rd[s] # None
  /\ rd[s].ans = Wait
  /\ Ready(rd[s].rep)
  /\ applied[rd[s].rep] >= rd[s].basis
  /\ rd' = [rd EXCEPT ![s].ans = Lookup(rd[s].rep, acked[s])]
  /\ UNCHANGED <<chosen, held, applied, base, rec, meta, retired, down, acked>>

\* S1 retires: RetireAt replicas of R1 hold it, and with the boundary in the
\* record, the record holds it.
Retire ==
  /\ S1Full
  /\ Cardinality(Holders) >= RetireAt
  /\ RetireRecord /\ RecordMode => rec # None
  /\ retired' = TRUE
  /\ UNCHANGED <<chosen, held, applied, base, rec, meta, down, acked, rd>>

\* A replica stops for good; at most F of each set.
Crash(r) ==
  /\ r \notin down
  /\ Cardinality(down \cap Set(SetOf(r))) < F
  /\ down' = down \cup {r}
  /\ UNCHANGED <<chosen, held, applied, base, rec, meta, retired, acked, rd>>

\* After the retire, every replica of R1 stops (offloaded, copies dropped).
Vanish ==
  /\ Gone
  /\ retired
  /\ down' = down \cup R1
  /\ UNCHANGED <<chosen, held, applied, base, rec, meta, retired, acked, rd>>

Next ==
  \/ \E v \in Vals : Choose(v)
  \/ ChooseFence
  \/ \E r \in Replicas, s \in Slots : Learn(r, s)
  \/ \E r \in Replicas : Apply(r) \/ WriteRecord(r) \/ TakeBase(r) \/ Crash(r)
  \/ \E s \in Slots : Ack(s) \/ StartRead(s) \/ Reroute(s) \/ Serve(s)
  \/ Retire
  \/ Vanish

Spec == Init /\ [][Next]_vars

\* Every step is bounded, so weak fairness on Next runs the journal until no
\* step is enabled.
LiveSpec == Spec /\ WF_vars(Next)

(***************************************************************************)
(* Safety.                                                                 *)
(***************************************************************************)
\* Delivery: no replica holds a slot of a segment outside its own.
NoForeign == \A r \in Replicas : \A s \in held[r] : Seg(s) = SetOf(r)

\* Every replica's fold is the fold of the whole log, although a replica of
\* S2 never applied a slot of S1. (A replica of S2 that applied nothing is
\* judged once S1 is full. In the "meta" modes it has no leader uuid until
\* it applies the `Fence` of slot K, so only its `next_seq` is judged then.)
OneFold == \A r \in Replicas : Ready(r) /\ Frontier >= Front(r) =>
  IF MetaMode /\ SetOf(r) = 2 /\ applied[r] = 0
  THEN StateOf(r).next = Truth(Front(r)).next
  ELSE StateOf(r) = Truth(Front(r))

\* The record holds the journal state at the seal.
RecordTrue == rec # None => S1Full /\ rec = Truth(K)

\* Each writer got the answer of the whole log.
AckTrue == \A s \in Slots : acked[s] # None => acked[s] = Res(Truth(s), chosen[s])

\* A writer gets its answer from a replica of the slot's set that applied the
\* slot.
AckApplied == \A s \in Slots : acked[s] # None => \E r \in Set(Seg(s)) : s < Front(r)

\* Each served read of an acked write returns that write: never "tail" (a
\* stale read) and never `Truncated` (nothing was trimmed).
ReadTrue == \A s \in Slots : rd[s] # None /\ rd[s].ans # Wait => rd[s].ans = s

(***************************************************************************)
(* Liveness: the journal runs until no step is enabled. Then every chosen  *)
(* slot is acked, every read is served, and S1 retired once it is full.    *)
(***************************************************************************)
AllAcked(g) == \A s \in SlotsOf(g) : chosen[s] # None => acked[s] # None
AllServed(g) == \A s \in SlotsOf(g) : rd[s] # None => rd[s].ans # Wait

Ends == <>[](/\ AllAcked(1) /\ AllAcked(2)
             /\ AllServed(1) /\ AllServed(2)
             /\ (S1Full => retired))

\* With `Gone`, R1 may stop after the retire, so only S2 must end well: the
\* replicas of S2 never need a slot of S1.
EndsS2 == <>[](AllAcked(2) /\ AllServed(2))

(***************************************************************************)
(* Reach: a check that must be violated, so that a passing check is not    *)
(* empty.                                                                  *)
(***************************************************************************)
\* Two reads across the seal are served, one in each segment.
NoCrossRead ==
  ~(\E s1 \in SlotsOf(1), s2 \in SlotsOf(2) :
      rd[s1] # None /\ rd[s1].ans # Wait /\ rd[s2] # None /\ rd[s2].ans # Wait)
\* S1 retires, then a write of S2 is acked.
NoAckAfterRetire == ~(retired /\ \E s \in SlotsOf(2) : acked[s] # None /\ acked[s] >= 0)
================================================================================
