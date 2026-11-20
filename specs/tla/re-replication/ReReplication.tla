---------------------------- MODULE ReReplication ----------------------------
(***************************************************************************)
(* The re-replication of a sealed segment, #1314 (re-replication of a      *)
(* sealed segment's replica set).                                          *)
(*                                                                         *)
(* Offload is prepared, not built, in v1 (docs/architecture/               *)
(* 02-data-plane.md, section 2.10). So a sealed and retired segment lives  *)
(* only on its replica set (#1271, `segment-replicas/`) until a user       *)
(* `Truncate` trims it (#1276, `segment-truncate/`). Its acceptors are     *)
(* gone. When a replica of the set fails, the grid controller heals: it    *)
(* copies the segment to a new replica and changes the replica set in the  *)
(* segment record. This model checks that step.                            *)
(*                                                                         *)
(* The segment has the slots 0 .. K - 1, all chosen; the bytes of a slot   *)
(* never change, so a replica's copy is the range of slots [lo, hi) that   *)
(* it holds. The boundary state of the segment has the trim floor FL0 (a   *)
(* `Truncate` inside the segment, `segment-truncate/`): a replica that     *)
(* compacted holds [FL0, hi). The floor that the journal decided later is  *)
(* `fl`, from FL0 up to FloorTo (at most K); at K the whole segment is     *)
(* below the floor, and the record of the segment can become trimmed.     *)
(*                                                                         *)
(* The record. The segment record is one value of the journal's            *)
(* compare-and-set register at the matchmakers (`segment-records/` checks  *)
(* the register across a matchmaker handover). It holds the state          *)
(* (retired or trimmed), the replica set, the controller term of the       *)
(* matchmaker record (#1259, `grid-controller/`) and `ver`, the version of *)
(* the register: every write raises it. Each step on the record is atomic  *)
(* here, so a step acts on a write that is done (#1275).                   *)
(*                                                                         *)
(* What the model has:                                                     *)
(*   - Crash and restart: a replica stops and can come back with its disk. *)
(*     At most F members of the set in the record are down at one time     *)
(*     (the promise of section 2.11). A replica that is not a member can   *)
(*     stop at any time.                                                   *)
(*   - Catch-up: a member that lags copies the next slot from a live       *)
(*     replica that holds it, or jumps to the trim point of a peer         *)
(*     (`TrimmedTo`).                                                      *)
(*   - Elect: a controller writes term + 1 into the record.                *)
(*   - Plan: a controller reads the record and, for a member that is down, *)
(*     picks a new replica and a source. The new replica copies the        *)
(*     segment from the source, one slot at a time, and reports "done" to  *)
(*     the controller when its copy ends.                                  *)
(*   - Commit: the controller writes the new set (the down member out, the *)
(*     new replica in) into the record.                                    *)
(*   - Abort: a plan whose replica or source is down, whose term is gone,  *)
(*     or whose read is old, ends with no write.                           *)
(*   - Drop: a replica that is not a member and not a new replica of a     *)
(*     plan drops its copy; so does every replica once the record is       *)
(*     trimmed. A removed replica that comes back drops its copy too.      *)
(*   - Floor and Trim: the decided floor rises; at K the record becomes    *)
(*     trimmed by compare-and-set.                                         *)
(*   - Read of a slot q: the reader takes the floor from the segment in    *)
(*     force and the replica set from the record. A member that holds q    *)
(*     answers the record; a member that does not hold it refuses, and the *)
(*     reader reads the record and the floor again.                        *)
(*                                                                         *)
(* Constants select the rules. The first value of each is the design; each *)
(* other value is a control.                                               *)
(*   CountWhen "done": the commit needs the new replica's report that its  *)
(*             copy is complete. "start": the controller counts the new    *)
(*             replica when its copy starts.                               *)
(*   CopyFrom  "full": the source is a member of the set that holds every *)
(*             slot up to the segment end (the `next_seq` of the record),  *)
(*             as the offloader of #1256. "any": any member that holds a   *)
(*             slot; the new replica takes the end of the source's copy as *)
(*             the end of the segment. "outside": a live replica that     *)
(*             holds every slot, a member or not; a replica that is not a  *)
(*             member can drop its copy while the new replica copies.      *)
(*   Cas       "cas": the commit is a compare-and-set on the version of    *)
(*             the register that the plan read, and on the controller's    *)
(*             term. "term": the term check only; the commit writes the    *)
(*             record value of its read with the new set. "blind": no      *)
(*             check; the same write.                                      *)
(*   TermFence TRUE: the "cas" commit also checks the term. FALSE: the     *)
(*             version alone.                                              *)
(*   Report    "copy": a "done" report belongs to the copy of one plan; a  *)
(*             new plan starts a new copy. "sticky": the controller keeps  *)
(*             a "done" report from an earlier copy of the same replica.   *)
(*   Refuse    TRUE: a member that does not hold the slot refuses the      *)
(*             read. FALSE: a member whose copy ends below the slot        *)
(*             answers "no record" (as a tail read of an open segment).    *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS Pool, Init0, Ctls, F, K, FL0, FloorTo, ReadSlots, MaxCrash, MaxRestart, MaxTerm,
          None, CountWhen, CopyFrom, Cas, TermFence, Report, Refuse

ASSUME Init0 \subseteq Pool /\ Cardinality(Init0) = 2 * F + 1
ASSUME K \in Nat \ {0} /\ FL0 \in 0..(K - 1) /\ FloorTo \in FL0..K
ASSUME ReadSlots \subseteq 0..(K - 1)
ASSUME CountWhen \in {"done", "start"}
ASSUME CopyFrom \in {"full", "any", "outside"}
ASSUME Cas \in {"cas", "term", "blind"}
ASSUME TermFence \in BOOLEAN /\ Refuse \in BOOLEAN
ASSUME Report \in {"copy", "sticky"}

Max(a, b) == IF a > b THEN a ELSE b

VARIABLES
  lo, hi,    \* the range of slots [lo, hi) that each replica holds
  down,      \* the replicas that are down
  rec,       \* the segment record: [st, set, term, ver]
  fl,        \* the floor that the journal decided
  term,      \* the term of each controller (0: none)
  plan,      \* the plan of each controller, or None
  cp,        \* each replica's copy: "none", "copying" or "done"
  rd,        \* the read of each slot, or None
  crashes,   \* the crashes so far
  restarts   \* the restarts so far

vars == <<lo, hi, down, rec, fl, term, plan, cp, rd, crashes, restarts>>

Holds(r, s) == lo[r] <= s /\ s < hi[r]
Live == Pool \ down
\* A good holder: live, and it holds every slot at or above the floor.
Good(r) == r \in Live /\ lo[r] <= fl /\ hi[r] = K
GoodMembers == {r \in rec.set : Good(r)}
Joiners == {plan[c].n : c \in {d \in Ctls : plan[d] # None}}

TypeOK ==
  /\ lo \in [Pool -> 0..K] /\ hi \in [Pool -> 0..K]
  /\ \A r \in Pool : lo[r] <= hi[r]
  /\ down \subseteq Pool
  /\ rec.st \in {"retired", "trimmed"} /\ rec.set \subseteq Pool
  /\ fl \in FL0..K
  /\ term \in [Ctls -> 0..MaxTerm]
  /\ cp \in [Pool -> {"none", "copying", "done"}]

\* Each member of the set holds the slots from the floor (or from 0) up to
\* some slot; F + 1 of them hold the whole segment, as at the retire.
Init ==
  /\ \E l \in [Init0 -> {0, FL0}], h \in [Init0 -> 0..K] :
       /\ \A r \in Init0 : l[r] <= h[r]
       /\ Cardinality({r \in Init0 : h[r] = K}) >= F + 1
       /\ lo = [r \in Pool |-> IF r \in Init0 THEN l[r] ELSE 0]
       /\ hi = [r \in Pool |-> IF r \in Init0 THEN h[r] ELSE 0]
  /\ down = {}
  /\ rec = [st |-> "retired", set |-> Init0, term |-> 0, ver |-> 0]
  /\ fl = FL0
  /\ term = [c \in Ctls |-> 0]
  /\ plan = [c \in Ctls |-> None]
  /\ cp = [r \in Pool |-> "none"]
  /\ rd = [q \in ReadSlots |-> None]
  /\ crashes = 0
  /\ restarts = 0

Crash(r) ==
  /\ r \in Live
  /\ crashes < MaxCrash
  /\ r \in rec.set => Cardinality(rec.set \cap down) < F
  /\ down' = down \cup {r}
  /\ crashes' = crashes + 1
  /\ UNCHANGED <<lo, hi, rec, fl, term, plan, cp, rd, restarts>>

Restart(r) ==
  /\ r \in down
  /\ restarts < MaxRestart
  /\ down' = down \ {r}
  /\ restarts' = restarts + 1
  /\ UNCHANGED <<lo, hi, rec, fl, term, plan, cp, rd, crashes>>

\* The slot range that replica r takes from source p in one step: the next
\* slot, or the trim point of p when r is below it (`TrimmedTo`).
Fetch(r, p) ==
  /\ p \in Live /\ p # r
  /\ hi[r] < hi[p]
  /\ IF hi[r] < lo[p]
     THEN lo' = [lo EXCEPT ![r] = lo[p]] /\ hi' = [hi EXCEPT ![r] = lo[p] + 1]
     ELSE hi' = [hi EXCEPT ![r] = @ + 1] /\ UNCHANGED lo

\* A member that lags copies the next slot from a live peer.
CatchUp(r) ==
  /\ r \in Live
  /\ rec.st = "retired"
  /\ r \in rec.set
  /\ r \notin Joiners
  /\ hi[r] < K
  /\ \E p \in Live : Fetch(r, p)
  /\ UNCHANGED <<down, rec, fl, term, plan, cp, rd, crashes, restarts>>

Elect(c) ==
  /\ rec.term < MaxTerm
  /\ term' = [term EXCEPT ![c] = rec.term + 1]
  /\ rec' = [rec EXCEPT !.term = @ + 1, !.ver = @ + 1]
  /\ UNCHANGED <<lo, hi, down, fl, plan, cp, rd, crashes, restarts>>

\* The controller reads the record and plans to replace the down member d
\* by the new replica n, which copies from the source p.
Plan(c) ==
  /\ plan[c] = None
  /\ term[c] > 0 /\ term[c] = rec.term
  /\ rec.st = "retired"
  /\ \E d \in rec.set \cap down, n \in Live \ (rec.set \cup Joiners), p \in Live :
       /\ p # n
       /\ CopyFrom # "outside" => p \in rec.set
       /\ IF CopyFrom = "any" THEN hi[p] > lo[p] ELSE hi[p] = K
       /\ plan' = [plan EXCEPT ![c] = [rd |-> rec, d |-> d, n |-> n, src |-> p, end |-> hi[p]]]
       /\ cp' = [cp EXCEPT ![n] = IF Report = "sticky" /\ @ = "done" THEN "done" ELSE "copying"]
       /\ lo' = [lo EXCEPT ![n] = IF cp'[n] = "done" THEN @ ELSE 0]
       /\ hi' = [hi EXCEPT ![n] = IF cp'[n] = "done" THEN @ ELSE 0]
  /\ UNCHANGED <<down, rec, fl, term, rd, crashes, restarts>>

\* The new replica of a plan copies the next slot from the plan's source.
\* Its copy is done when it reaches the end that it knows: the end of the
\* source's copy, which is the segment end when the source is full.
Copy(c) ==
  /\ plan[c] # None
  /\ LET n == plan[c].n
         p == plan[c].src
     IN /\ n \in Live
        /\ cp[n] = "copying"
        /\ hi[n] < plan[c].end
        /\ Fetch(n, p)
        /\ cp' = [cp EXCEPT ![n] = IF hi'[n] = plan[c].end THEN "done" ELSE "copying"]
  /\ UNCHANGED <<down, rec, fl, term, plan, rd, crashes, restarts>>

\* The record value that a commit writes.
NewSet(c) == (plan[c].rd.set \ {plan[c].d}) \cup {plan[c].n}

CasOk(c) ==
  CASE Cas = "cas"   -> /\ rec.ver = plan[c].rd.ver
                        /\ TermFence => rec.term = term[c]
    [] Cas = "term"  -> rec.term = term[c]
    [] Cas = "blind" -> TRUE

Commit(c) ==
  /\ plan[c] # None
  /\ CountWhen = "done" => cp[plan[c].n] = "done"
  /\ CasOk(c)
  /\ rec' = [plan[c].rd EXCEPT !.set = NewSet(c), !.ver = rec.ver + 1]
  /\ plan' = [plan EXCEPT ![c] = None]
  /\ UNCHANGED <<lo, hi, down, fl, term, cp, rd, crashes, restarts>>

\* A plan that cannot commit ends with no write: its new replica or its
\* source is down, its term is gone, its read is old, or the record is
\* trimmed. Its new replica stops copying.
Abort(c) ==
  /\ plan[c] # None
  /\ \/ plan[c].n \in down
     \/ plan[c].src \in down /\ cp[plan[c].n] = "copying"
     \/ ~CasOk(c)
     \/ rec.st = "trimmed"
  /\ plan' = [plan EXCEPT ![c] = None]
  /\ cp' = [cp EXCEPT ![plan[c].n] = IF @ = "copying" THEN "none" ELSE @]
  /\ UNCHANGED <<lo, hi, down, rec, fl, term, rd, crashes, restarts>>

\* A live replica that is not a member and is no plan's new replica drops
\* its copy; after the trim, every replica drops it.
Drop(r) ==
  /\ r \in Live
  /\ hi[r] > 0
  /\ \/ rec.st = "trimmed"
     \/ r \notin rec.set \cup Joiners
  /\ lo' = [lo EXCEPT ![r] = 0]
  /\ hi' = [hi EXCEPT ![r] = 0]
  /\ cp' = [cp EXCEPT ![r] = IF Report = "sticky" THEN @ ELSE "none"]
  /\ UNCHANGED <<down, rec, fl, term, plan, rd, crashes, restarts>>

\* A `Truncate` of the segment in force raises the decided floor.
RaiseFloor ==
  /\ fl < FloorTo
  /\ fl' = fl + 1
  /\ UNCHANGED <<lo, hi, down, rec, term, plan, cp, rd, crashes, restarts>>

\* The floor covers the whole segment: the record becomes trimmed.
Trim ==
  /\ rec.st = "retired"
  /\ fl = K
  /\ rec' = [rec EXCEPT !.st = "trimmed", !.ver = @ + 1]
  /\ UNCHANGED <<lo, hi, down, fl, term, plan, cp, rd, crashes, restarts>>

\* A read of slot q takes the floor from the segment in force and the set
\* from the record.
StartRead(q) ==
  /\ rd[q] = None
  /\ rd' = [rd EXCEPT ![q] = [set |-> rec.set, fl |-> fl, ans |-> None]]
  /\ UNCHANGED <<lo, hi, down, rec, fl, term, plan, cp, crashes, restarts>>

Serve(q) ==
  /\ rd[q] # None
  /\ rd[q].ans = None
  /\ IF q < rd[q].fl
     THEN rd' = [rd EXCEPT ![q].ans = "trunc"]
     ELSE \E m \in rd[q].set \cap Live :
            IF Holds(m, q) THEN rd' = [rd EXCEPT ![q].ans = "data"]
            ELSE IF ~Refuse /\ hi[m] <= q THEN rd' = [rd EXCEPT ![q].ans = "none"]
            \* A refusal: the reader reads the record and the floor again,
            \* and only when one of them changed (the abstract form of its
            \* back-off), so a run ends.
            ELSE /\ rd[q].set # rec.set \/ rd[q].fl # fl
                 /\ rd' = [rd EXCEPT ![q] = [set |-> rec.set, fl |-> fl, ans |-> None]]
  /\ UNCHANGED <<lo, hi, down, rec, fl, term, plan, cp, crashes, restarts>>

Next ==
  \/ \E r \in Pool : Crash(r) \/ Restart(r) \/ CatchUp(r) \/ Drop(r)
  \/ \E c \in Ctls : Elect(c) \/ Plan(c) \/ Copy(c) \/ Commit(c) \/ Abort(c)
  \/ RaiseFloor \/ Trim
  \/ \E q \in ReadSlots : StartRead(q) \/ Serve(q)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Safety.                                                                 *)
(***************************************************************************)
\* Every slot at or above the floor of a segment that is not trimmed is on a
\* live replica.
Findable == rec.st = "retired" =>
  \A s \in fl..(K - 1) : \E r \in Live : Holds(r, s)

\* The segment survives every failure that the promise still allows: with
\* `D` members of the set down, `F - D` more may fail, so at least
\* `F - D + 1` live members are good holders. With no member down, that is
\* `F + 1` good holders.
Margin == rec.st = "retired" =>
  Cardinality(GoodMembers) >= F + 1 - Cardinality(rec.set \cap down)

\* The set in the record keeps its size.
SetSize == Cardinality(rec.set) = 2 * F + 1

\* A read answers the record of the slot, or "trunc" below the floor.
ReadTrue == \A q \in ReadSlots : rd[q] # None =>
  /\ rd[q].ans \in {None, "data", "trunc"}
  /\ rd[q].ans = "trunc" => q < fl

\* A trimmed record stays trimmed.
TrimStays == [][rec.st = "trimmed" => rec'.st = "trimmed"]_vars

(***************************************************************************)
(* Liveness. Every step is bounded, so a run ends in a state where no step *)
(* is enabled. In that state every read is answered and no plan is open.   *)
(* If the segment is not trimmed, each member of the set is a good holder, *)
(* unless it is down and no live replica is left to replace it. If it is   *)
(* trimmed, no live replica holds a slot.                                  *)
(***************************************************************************)
Goal ==
  /\ \A q \in ReadSlots : rd[q] # None => rd[q].ans # None
  /\ \A c \in Ctls : plan[c] = None
  /\ rec.st = "retired" =>
       \A r \in rec.set : Good(r) \/ (r \in down /\ Live \subseteq rec.set)
  /\ rec.st = "trimmed" => \A r \in Live : hi[r] = 0

Ends == (~ENABLED Next) => Goal

(***************************************************************************)
(* Reach: a check that must be violated, so that a passing check is not    *)
(* empty.                                                                  *)
(***************************************************************************)
\* A new replica takes the place of a down member.
NoHeal == rec.set = Init0
\* Two heals in a row.
NoTwoHeals == Cardinality(rec.set \cap Init0) > 2 * F - 1
\* A read is served by a replica that a heal added.
NoReadNew == ~\E q \in ReadSlots :
  rd[q] # None /\ rd[q].ans = "data" /\ rd[q].set # Init0
\* A heal is aborted by the trim.
NoTrimAbort == [][~(\E c \in Ctls : plan[c] # None /\ plan'[c] = None /\ rec.st = "trimmed")]_vars
\* A removed replica comes back and drops its copy.
NoBackDrop == [][~\E r \in Pool :
  r \in down /\ r \notin down' /\ r \notin rec.set /\ hi[r] > 0]_vars
================================================================================
