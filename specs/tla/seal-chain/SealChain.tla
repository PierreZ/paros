------------------------------ MODULE SealChain ------------------------------
(***************************************************************************)
(* Two seals in a row, an empty segment, and where the boundary uuid       *)
(* lives, #1303 (two seals in a row).                                      *)
(*                                                                         *)
(* The journal state is folded at apply (docs/architecture/02-data-plane.md*)
(* sections 2.1 and 2.2), and each segment has its own replica set         *)
(* (section 2.10, `segment-replicas/`). So a replica of a segment starts   *)
(* its fold from the boundary state, the journal state at the seal of the  *)
(* segment before it: `next_seq`, the trim floor `first_seq` and the       *)
(* leader uuid. `segment-replicas/` checks one seal. This model checks a   *)
(* chain of G segments, with a seal at any time: before the first slot of  *)
(* a segment (an empty segment), before its `Fence`, and before its first  *)
(* leader read the uuid.                                                   *)
(*                                                                         *)
(* The log is abstract: the leader chooses the slots in order, one value   *)
(* for each slot (`multi-paxos/` checks the agreement and                  *)
(* `experiments/segments/a/` the seal end). A seal closes the open segment *)
(* at the frontier, so the next segment starts at the first slot that is   *)
(* not chosen. A segment's replica set is one fold here (`segment-replicas/`*)
(* checks the crashes and the reads inside one set): it applies the chosen *)
(* slots of its segment in order, from its boundary state, and answers the *)
(* writer of each slot.                                                    *)
(*                                                                         *)
(* A value is a `Write` of one record under a uuid, a `SetLeader(new, old)`*)
(* (a compare-and-set of the leader uuid), a `Truncate` to the tail under  *)
(* a uuid (it raises `first_seq` to `next_seq`), a `Noop` (the election    *)
(* gap fill) or a `Fence(u)` (it sets the leader uuid).                    *)
(*                                                                         *)
(* Constants select the rules. The first value of each is the design that  *)
(* passes; each other value is a control.                                  *)
(*   Boundary  "record": the first replica of a segment that applied every *)
(*             slot writes the whole boundary state (`next_seq`,           *)
(*             `first_seq`, leader uuid) into the record of that segment,  *)
(*             once, by compare-and-set. The replicas of the next segment  *)
(*             start from that record. No `Fence` is needed.               *)
(*             "meta": the record holds `next_seq` and `first_seq`; the    *)
(*             leader uuid goes into the journal's matchmaker record (one  *)
(*             field, the journal metadata) in the same compare-and-set.   *)
(*             The first leader of the next segment reads it and proposes  *)
(*             `Fence(uuid)`. A replica of that segment starts with no     *)
(*             uuid and takes it from the `Fence`.                         *)
(*   BaseFrom  ("record") "pred": a replica of segment g starts from the   *)
(*             record of segment g - 1. "latest": a control, it starts     *)
(*             from the highest record that is written.                    *)
(*   FenceRead ("meta") "rec": the leader of segment g reads the uuid once *)
(*             the record of g - 1 is written, in the same read of the     *)
(*             register. "any": a control, it reads the uuid once the     *)
(*             metadata holds one (from an earlier seal). "stale": a       *)
(*             control, it may read the metadata at any time.              *)
(*   EmptyMeta ("meta") what the boundary write of a segment whose fold    *)
(*             has no uuid (no `Fence` applied: an empty segment, or a     *)
(*             `Noop` in place of the `Fence`) does to the metadata.       *)
(*             "keep": it leaves the metadata as it is. "blank": a         *)
(*             control, it writes the uuid of the empty state. "wait": a   *)
(*             control, it waits for a uuid (the rule "the replica writes  *)
(*             the uuid of its fold").                                     *)
(*   SealGuard TRUE: in "meta", a seal waits until the `Fence` of the      *)
(*             segment is chosen (candidate 1 of #1303). FALSE: a seal at  *)
(*             any time.                                                   *)
(*   GapFill   FALSE: in "meta", the first slot of a segment is its        *)
(*             `Fence`. TRUE: a `Noop` (the gap fill of a lost `Fence`), a *)
(*             command pipelined behind it, or a `Noop` anywhere may come  *)
(*             first, and the `Fence` comes later; a replica refuses every *)
(*             `Write`, `Truncate` and `SetLeader` of its segment until it *)
(*             applied a `Fence` (#1302 (Fence slot against the gap        *)
(*             fill), candidate 1). In "record", a `Noop` may take any     *)
(*             slot.                                                       *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS G, NS, Vals, U0, Unset, None,
          Boundary, BaseFrom, FenceRead, EmptyMeta, SealGuard, GapFill

ASSUME G \in Nat /\ G >= 1 /\ NS \in Nat /\ NS >= 1
ASSUME Boundary \in {"record", "meta"}
ASSUME BaseFrom \in {"pred", "latest"}
ASSUME FenceRead \in {"rec", "any", "stale"}
ASSUME EmptyMeta \in {"keep", "blank", "wait"}
ASSUME SealGuard \in BOOLEAN /\ GapFill \in BOOLEAN

Meta == Boundary = "meta"
Segs == 1..G
Slots == 0..(NS - 1)
Noop == [op |-> "noop"]
Fence(u) == [op |-> "fence", u |-> u]

(***************************************************************************)
(* The fold. A state is `next_seq`, `first_seq` and the leader uuid. In    *)
(* "meta" the uuid of a replica is `Unset` until it applies a `Fence`, and *)
(* a command under an `Unset` uuid is refused.                             *)
(***************************************************************************)
Empty == [next |-> 0, first |-> 0, leader |-> U0]

Cmd(v) == v.op \in {"w", "tr", "sl"}

\* A command is taken: the fence uuid of its fold is set and matches.
Taken(st, v) ==
  /\ Cmd(v)
  /\ st.leader # Unset
  /\ IF v.op = "sl" THEN v.old = st.leader ELSE v.u = st.leader

\* The answer of a slot: the first `seq` of a taken write, 1 for a taken
\* `Truncate` or `SetLeader`, -1 for a refused command, a `Noop` or a
\* `Fence`.
Res(st, v) == IF Taken(st, v) THEN (IF v.op = "w" THEN st.next ELSE 1) ELSE -1

Step(st, v) ==
  CASE v.op = "fence" -> [st EXCEPT !.leader = v.u]
    [] ~Taken(st, v) -> st
    [] v.op = "w" -> [st EXCEPT !.next = st.next + 1]
    [] v.op = "tr" -> [st EXCEPT !.first = st.next]
    [] v.op = "sl" -> [st EXCEPT !.leader = v.new]

VARIABLES
  chosen,   \* the chosen value of each slot, or None
  cur,      \* the open segment: segments 1 .. cur - 1 are sealed
  st,       \* the start slot of each segment that opened, or None
  en,       \* the end slot of each sealed segment, or None
  applied,  \* how many slots of its segment the replica set of each segment applied
  base,     \* the boundary state that each replica set started from, or None
  rec,      \* the record of each segment: its boundary state, or None
  meta,     \* ("meta") the leader uuid in the journal metadata, or None
  fread,    \* ("meta") the uuid that the leader of each segment read, or None
  acked     \* the answer that the writer of each slot got, or None

vars == <<chosen, cur, st, en, applied, base, rec, meta, fread, acked>>

\* The first slot that is not chosen.
Frontier == CHOOSE n \in 0..NS : (\A s \in 0..(n - 1) : chosen[s] # None) /\
                                 (n = NS \/ chosen[n] = None)

\* The slots of a segment that are chosen: up to its end, or the frontier.
Top(g) == IF en[g] # None THEN en[g] ELSE Frontier
SlotsOf(g) == st[g]..(Top(g) - 1)

\* A `Fence` is chosen in segment g.
Fenced(g) == \E s \in SlotsOf(g) : chosen[s].op = "fence"

(***************************************************************************)
(* The truth: the fold of the whole log, one walk from slot 0. A `Fence`   *)
(* only repeats what the log already holds, so the truth keeps its uuid.  *)
(* In "meta" a command of a segment before the first `Fence` of that       *)
(* segment is refused, as the replicas refuse it.                          *)
(***************************************************************************)
Starts == {st[g] : g \in 2..cur}

RECURSIVE TruthFrom(_, _, _, _)
\* t is the state, ok tells whether commands are taken, from slot lo to hi.
TruthFrom(t, ok, lo, hi) ==
  IF lo >= hi THEN t
  ELSE LET open == IF Meta /\ lo \in Starts THEN FALSE ELSE ok
           v == chosen[lo]
           t2 == IF v.op = "fence" THEN t
                 ELSE IF Cmd(v) /\ ~open THEN t
                 ELSE Step(t, v)
           ok2 == open \/ v.op = "fence"
       IN TruthFrom(t2, ok2, lo + 1, hi)

Truth(s) == TruthFrom(Empty, TRUE, 0, s)

\* Whether the truth takes commands at slot s (a `Fence` of its segment is
\* before s, in "meta").
RECURSIVE OpenFrom(_, _, _)
OpenFrom(ok, lo, hi) ==
  IF lo >= hi THEN ok
  ELSE LET open == IF Meta /\ lo \in Starts THEN FALSE ELSE ok
       IN OpenFrom(open \/ chosen[lo].op = "fence", lo + 1, hi)
OpenAt(s) == IF Meta /\ s \in Starts THEN FALSE ELSE OpenFrom(TRUE, 0, s)

\* The true answer of slot s.
TrueRes(s) ==
  IF Cmd(chosen[s]) /\ ~OpenAt(s) THEN -1 ELSE Res(Truth(s), chosen[s])

(***************************************************************************)
(* The replica set of segment g.                                           *)
(***************************************************************************)
RECURSIVE FoldTo(_, _, _)
FoldTo(t, lo, hi) == IF lo >= hi THEN t ELSE FoldTo(Step(t, chosen[lo]), lo + 1, hi)

Ready(g) == base[g] # None
Front(g) == st[g] + applied[g]
StateOf(g) == FoldTo(base[g], st[g], Front(g))

TypeOK ==
  /\ cur \in Segs
  /\ \A g \in Segs : applied[g] \in 0..NS
  /\ \A s \in Slots : chosen[s] = None \/ chosen[s] \in Vals \cup {Noop} \/ chosen[s].op = "fence"

Init ==
  /\ chosen = [s \in Slots |-> None]
  /\ cur = 1
  /\ st = [g \in Segs |-> IF g = 1 THEN 0 ELSE None]
  /\ en = [g \in Segs |-> None]
  /\ applied = [g \in Segs |-> 0]
  /\ base = [g \in Segs |-> IF g = 1 THEN Empty ELSE None]
  /\ rec = [g \in Segs |-> None]
  /\ meta = None
  /\ fread = [g \in Segs |-> None]
  /\ acked = [s \in Slots |-> None]

\* In "meta" with no gap fill, the first slot of a segment above the first
\* is its `Fence`.
FenceFirst == Meta /\ ~GapFill /\ cur > 1 /\ Frontier = st[cur]

\* The leader of the open segment chooses the next slot: a command, or a
\* `Noop` of the gap fill.
Choose(v) ==
  /\ Frontier < NS
  /\ ~FenceFirst
  /\ v = Noop => GapFill
  /\ chosen' = [chosen EXCEPT ![Frontier] = v]
  /\ UNCHANGED <<cur, st, en, applied, base, rec, meta, fread, acked>>

\* ("meta") The first leader of a segment above the first reads the uuid in
\* the journal metadata, once.
ReadMeta ==
  /\ Meta
  /\ cur > 1
  /\ fread[cur] = None
  /\ CASE FenceRead = "rec" -> rec[cur - 1] # None
       [] FenceRead = "any" -> meta # None
       [] FenceRead = "stale" -> TRUE
  /\ fread' = [fread EXCEPT ![cur] = IF meta = None THEN U0 ELSE meta]
  /\ UNCHANGED <<chosen, cur, st, en, applied, base, rec, meta, acked>>

\* ("meta") It proposes `Fence(uuid)`: at the first slot, or later with the
\* gap fill, while its segment has no `Fence`.
ChooseFence ==
  /\ Meta
  /\ cur > 1
  /\ Frontier < NS
  /\ fread[cur] # None
  /\ ~Fenced(cur)
  /\ GapFill \/ Frontier = st[cur]
  /\ chosen' = [chosen EXCEPT ![Frontier] = Fence(fread[cur])]
  /\ UNCHANGED <<cur, st, en, applied, base, rec, meta, fread, acked>>

\* The next configuration seals the open segment at the frontier: its first
\* leader's Phase 1 saw no vote above it. With `SealGuard` in "meta", the
\* segment's `Fence` must be chosen first.
Seal ==
  /\ cur < G
  /\ SealGuard /\ Meta /\ cur > 1 => Fenced(cur)
  /\ en' = [en EXCEPT ![cur] = Frontier]
  /\ st' = [st EXCEPT ![cur + 1] = Frontier]
  /\ cur' = cur + 1
  /\ UNCHANGED <<chosen, applied, base, rec, meta, fread, acked>>

\* The replica set of a segment applies its next slot and answers its writer.
Apply(g) ==
  /\ g <= cur
  /\ Ready(g)
  /\ Front(g) < Top(g)
  /\ applied' = [applied EXCEPT ![g] = @ + 1]
  /\ acked' = [acked EXCEPT ![Front(g)] = Res(StateOf(g), chosen[Front(g)])]
  /\ UNCHANGED <<chosen, cur, st, en, base, rec, meta, fread>>

\* The first replica of a sealed segment that applied every slot writes the
\* boundary state, once, by compare-and-set at the matchmakers.
WriteRecord(g) ==
  /\ g < cur
  /\ rec[g] = None
  /\ Ready(g)
  /\ Front(g) = en[g]
  /\ IF ~Meta
     THEN /\ rec' = [rec EXCEPT ![g] = StateOf(g)]
          /\ UNCHANGED meta
     ELSE LET u == StateOf(g).leader IN
          /\ EmptyMeta = "wait" => u # Unset
          /\ rec' = [rec EXCEPT ![g] = [StateOf(g) EXCEPT !.leader = Unset]]
          /\ meta' = IF u # Unset THEN u
                     ELSE IF EmptyMeta = "blank" THEN U0
                     ELSE meta
  /\ UNCHANGED <<chosen, cur, st, en, applied, base, fread, acked>>

\* The highest segment below g whose record is written.
Latest(g) == CHOOSE h \in 1..(g - 1) : rec[h] # None /\ \A k \in (h + 1)..(g - 1) : rec[k] = None

\* The replica set of a segment above the first takes its boundary state.
TakeBase(g) ==
  /\ 1 < g /\ g <= cur
  /\ base[g] = None
  /\ IF ~Meta /\ BaseFrom = "latest"
     THEN /\ \E h \in 1..(g - 1) : rec[h] # None
          /\ base' = [base EXCEPT ![g] = rec[Latest(g)]]
     ELSE /\ rec[g - 1] # None
          /\ base' = [base EXCEPT ![g] = rec[g - 1]]
  /\ UNCHANGED <<chosen, cur, st, en, applied, rec, meta, fread, acked>>

Next ==
  \/ \E v \in Vals \cup {Noop} : Choose(v)
  \/ ReadMeta
  \/ ChooseFence
  \/ Seal
  \/ \E g \in Segs : Apply(g) \/ WriteRecord(g) \/ TakeBase(g)

Spec == Init /\ [][Next]_vars

\* Every step is bounded, so weak fairness on Next runs the journal until no
\* step is enabled.
LiveSpec == Spec /\ WF_vars(Next)

(***************************************************************************)
(* Safety.                                                                 *)
(***************************************************************************)
\* Each writer got the answer of the whole log: no deposed writer writes, no
\* `seq` is given twice, no command is applied twice.
AckTrue == \A s \in Slots : acked[s] # None => acked[s] = TrueRes(s)

\* Each replica set's fold is the fold of the whole log at its front. In
\* "meta" its uuid is judged only once it applied a `Fence`.
OneFold == \A g \in 1..cur : Ready(g) =>
  LET t == StateOf(g) IN
  /\ t.next = Truth(Front(g)).next
  /\ t.first = Truth(Front(g)).first
  /\ t.leader # Unset => t.leader = Truth(Front(g)).leader

\* Each record holds the journal state at the end of its segment.
RecordTrue == \A g \in Segs : rec[g] # None =>
  /\ en[g] # None
  /\ rec[g].next = Truth(en[g]).next
  /\ rec[g].first = Truth(en[g]).first
  /\ ~Meta => rec[g].leader = Truth(en[g]).leader

\* ("meta") Each `Fence` carries the true uuid at its slot.
FenceTrue == \A s \in Slots : chosen[s] # None /\ chosen[s].op = "fence" =>
  chosen[s].u = Truth(s).leader

\* ("meta") The metadata holds the true uuid at the end of the last segment
\* whose record is written.
MetaTrue == Meta /\ meta # None =>
  \E g \in Segs : /\ rec[g] # None
                  /\ (g = G \/ rec[g + 1] = None)
                  /\ meta = Truth(en[g]).leader

\* `next_seq` and `first_seq` never move back across the seals.
NoMoveBack == \A g \in 2..G : rec[g] # None /\ rec[g - 1] # None =>
  /\ rec[g].next >= rec[g - 1].next
  /\ rec[g].first >= rec[g - 1].first

(***************************************************************************)
(* Liveness: the journal runs until no step is enabled. Then every slot is *)
(* chosen (so the first write of the open segment was not stuck behind its *)
(* boundary state), every sealed segment's record is written, and every    *)
(* slot is acked.                                                          *)
(***************************************************************************)
Ends == <>[](/\ Frontier = NS
             /\ \A g \in 1..(cur - 1) : rec[g] # None
             /\ \A s \in Slots : acked[s] # None)

(***************************************************************************)
(* Reach: a check that must be violated, so that a passing check is not    *)
(* empty.                                                                  *)
(***************************************************************************)
\* Two seals in a row with an empty middle segment, and a write accepted in
\* the third segment.
NoEmptyChain ==
  ~(/\ cur = 3
    /\ en[2] = st[2]
    /\ rec[2] # None
    /\ \E s \in Slots : acked[s] # None /\ s >= st[3] /\ chosen[s].op = "w" /\ acked[s] >= 0)

\* A `SetLeader` in the middle segment is taken, and the deposed writer is
\* refused in the third.
NoMovedChain ==
  ~(/\ cur = 3
    /\ \E s1, s2 \in Slots :
         /\ acked[s1] # None /\ acked[s1] >= 0 /\ chosen[s1].op = "sl"
         /\ st[2] <= s1 /\ s1 < en[2]
         /\ acked[s2] = -1 /\ chosen[s2].op = "w"
         /\ s2 >= st[3])
================================================================================
