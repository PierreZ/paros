------------------------------- MODULE Offload -------------------------------
(***************************************************************************)
(* The offload of sealed segments to an object store, and the races of     *)
(* its offloaders, #1256 (offload races and the offloader role).           *)
(*                                                                         *)
(* In design A (docs/architecture/02-data-plane.md section 2.10) each      *)
(* segment has a record in the journal's matchmaker record. The record of  *)
(* a sealed segment says "retired" once the matchmakers forgot its         *)
(* acceptor configuration, "offloaded" once its object is in the store,    *)
(* and "trimmed" once a user truncate removed it. The offloader is a       *)
(* stateless role (decided on 2026-11-20). For each retired segment it:    *)
(*   1. reads the segment from a replica of the segment that holds it;     *)
(*   2. builds the object, with deterministic bytes;                       *)
(*   3. writes the object with a conditional put (put if absent) at the    *)
(*      key journal id + segment number;                                   *)
(*   4. marks "offloaded" in the record by a compare-and-set.              *)
(* A replica drops its copy only after the mark, and then serves reads of  *)
(* the segment from the object.                                            *)
(*                                                                         *)
(* The races of the issue:                                                 *)
(*   - Late upload after trim: an offloader read "retired", trim then      *)
(*     removed the record and deleted the object, and the put of the slow  *)
(*     offloader creates the object again.                                 *)
(*   - Lost record write: the put is done, but the compare-and-set is lost *)
(*     (the write or its reply). The object has no mark.                   *)
(*   - Duplicate upload: two offloaders upload the same segment.           *)
(*   - Drop before mark: a replica drops its copy before the record says   *)
(*     offloaded.                                                          *)
(*   - An offloader that stops between the put and the mark.               *)
(*                                                                         *)
(* What the model has. A segment has L slots; its chosen content is fixed  *)
(* (it is sealed), so an object is right when it holds all L slots. Each   *)
(* replica of the segment's replica set applies the slots in order and     *)
(* holds its copy until it drops it. The segment retires once F + 1        *)
(* replicas applied every slot (`segment-replicas/`). Two offloaders run;  *)
(* each has a release, and the release is in the header of the bytes that *)
(* it builds (old formats stay readable for all time). An offloader can    *)
(* stop for good (at most NCrash stops, and one offloader stays up): the   *)
(* controller starts a new one, maybe of the next release, which is the    *)
(* other offloader of the model. The controller trims the segments in      *)
(* order when a user truncate asks (no fairness: a user may never ask),    *)
(* then deletes each object, and scrubs the store. The controller can stop *)
(* between the record write of a trim and the delete (one of the NCrash    *)
(* stops). The store is atomic: a put, a delete and the listing of one key *)
(* are single steps.                                                       *)
(*                                                                         *)
(* Constants select the rules. The first value of each is the design; each *)
(* other value is a control.                                               *)
(*   Build     "full": the offloader reads only a replica that applied     *)
(*             every slot of the segment, up to `next_seq` of the record.  *)
(*             "partial": it reads what the replica holds.                 *)
(*   Put       "cond": put if absent. "blind": the put overwrites.         *)
(*   Conflict  what an offloader does when its put finds an object.        *)
(*             "content": it reads the object and marks when the object    *)
(*             holds the whole segment, whatever its release. "bytes": it  *)
(*             marks only when the bytes equal its own. "abandon": it      *)
(*             stops this attempt.                                         *)
(*   Recheck   what an offloader does when its compare-and-set fails.      *)
(*             "trimmed": it deletes the key when the record says trimmed. *)
(*             "none": nothing. "anyfail": it deletes the key on any       *)
(*             failure.                                                    *)
(*   Cleanup   how the controller removes the objects of trimmed segments. *)
(*             "scrub": it deletes the key after the trim, and it lists    *)
(*             the store again and again and deletes each object of a      *)
(*             segment below the trim floor. "unmarked": the scrub also    *)
(*             deletes each object whose record is not offloaded.          *)
(*             "tomb": the trim writes a tombstone at the key (no scrub).  *)
(*             "trimonly": only the delete after the trim (no scrub).      *)
(*   TrimKey   "key": the delete after the trim uses the key of the        *)
(*             segment number. "record": it deletes only an object that    *)
(*             the record names (marked offloaded).                        *)
(*   Drop      "offloaded": a replica drops its copy once the record says  *)
(*             offloaded. "uploaded": once the object is in the store.     *)
(*   Gate      "retired": the offloader takes a retired segment. "sealed": *)
(*             it takes a sealed segment that did not retire yet.          *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Replicas, Offloaders, Release, NP, L, F, NCrash,
          Build, Put, Conflict, Recheck, Cleanup, TrimKey, Drop, Gate

ASSUME Build \in {"full", "partial"}
ASSUME Put \in {"cond", "blind"}
ASSUME Conflict \in {"content", "bytes", "abandon"}
ASSUME Recheck \in {"trimmed", "none", "anyfail"}
ASSUME Cleanup \in {"scrub", "unmarked", "tomb", "trimonly"}
ASSUME TrimKey \in {"key", "record"}
ASSUME Drop \in {"offloaded", "uploaded"}
ASSUME Gate \in {"retired", "sealed"}
ASSUME Release \in [Offloaders -> Nat]
ASSUME F + 1 <= Cardinality(Replicas) /\ L >= 1 /\ NP >= 1
ASSUME NCrash < Cardinality(Offloaders) + 1

Segs == 1..NP
\* What the store holds at a key: nothing, a tombstone, or an object: the
\* release of its header and the number of slots it holds.
Absent == [k |-> "none"]
Tomb == [k |-> "tomb"]
Obj(rel, n) == [k |-> "obj", rel |-> rel, n |-> n]
Objects == {Obj(rel, n) : rel \in {Release[o] : o \in Offloaders}, n \in 0..L}

VARIABLES
  \* The segment records in the journal's matchmaker record.
  ret,      \* segment -> TRUE once retired
  off,      \* segment -> TRUE once marked offloaded
  trim,     \* segment -> TRUE once trimmed
  \* The replicas of the segment's replica set.
  applied,  \* replica -> segment -> the slots that it applied
  held,     \* replica -> segment -> TRUE while it keeps its copy
  \* The object store, one key for each segment.
  store,    \* segment -> Absent, Tomb or an object
  \* The offloaders.
  ost,      \* offloader -> "idle", "built", "mark", "down"
  oseg,     \* offloader -> the segment of its attempt (0 when idle)
  olen,     \* offloader -> the number of slots of the object it built
  \* The controller.
  pend,     \* segment -> TRUE while the delete after its trim is to do
  crashes   \* the stops so far

rec == <<ret, off, trim>>
rep == <<applied, held>>
offl == <<ost, oseg, olen>>
vars == <<rec, rep, store, offl, pend, crashes>>

TypeOK ==
  /\ ret \in [Segs -> BOOLEAN] /\ off \in [Segs -> BOOLEAN] /\ trim \in [Segs -> BOOLEAN]
  /\ applied \in [Replicas -> [Segs -> 0..L]]
  /\ held \in [Replicas -> [Segs -> BOOLEAN]]
  /\ store \in [Segs -> {Absent, Tomb} \cup Objects]
  /\ ost \in [Offloaders -> {"idle", "built", "mark", "down"}]
  /\ oseg \in [Offloaders -> 0..NP]
  /\ olen \in [Offloaders -> 0..L]
  /\ pend \in [Segs -> BOOLEAN]
  /\ crashes \in 0..NCrash

Init ==
  /\ ret = [p \in Segs |-> FALSE]
  /\ off = [p \in Segs |-> FALSE]
  /\ trim = [p \in Segs |-> FALSE]
  /\ applied = [r \in Replicas |-> [p \in Segs |-> 0]]
  /\ held = [r \in Replicas |-> [p \in Segs |-> TRUE]]
  /\ store = [p \in Segs |-> Absent]
  /\ ost = [o \in Offloaders |-> "idle"]
  /\ oseg = [o \in Offloaders |-> 0]
  /\ olen = [o \in Offloaders |-> 0]
  /\ pend = [p \in Segs |-> FALSE]
  /\ crashes = 0

\* A replica that holds every slot of the segment.
Complete(r, p) == held[r][p] /\ applied[r][p] = L
Holders(p) == {r \in Replicas : Complete(r, p)}
IsObj(p) == store[p].k = "obj"
\* An object that holds the whole segment, whatever its release.
Whole(p) == IsObj(p) /\ store[p].n = L

(***************************************************************************)
(* Replicas and retire                                                      *)
(***************************************************************************)

Apply(r, p) ==
  /\ held[r][p] /\ ~trim[p]
  /\ applied[r][p] < L
  /\ applied' = [applied EXCEPT ![r][p] = @ + 1]
  /\ UNCHANGED <<rec, held, store, offl, pend, crashes>>

\* Matchmaker garbage collection: F + 1 replicas hold the whole segment, or
\* its object holds it and the record says so.
Retire(p) ==
  /\ ~ret[p] /\ ~trim[p]
  /\ Cardinality(Holders(p)) >= F + 1 \/ off[p]
  /\ ret' = [ret EXCEPT ![p] = TRUE]
  /\ UNCHANGED <<off, trim, rep, store, offl, pend, crashes>>

DropOk(p) ==
  \/ trim[p]
  \/ Drop = "offloaded" /\ off[p]
  \/ Drop = "uploaded" /\ IsObj(p)

DropCopy(r, p) ==
  /\ held[r][p]
  /\ DropOk(p)
  /\ held' = [held EXCEPT ![r][p] = FALSE]
  /\ UNCHANGED <<rec, applied, store, offl, pend, crashes>>

(***************************************************************************)
(* Offloaders                                                               *)
(***************************************************************************)

Takes(p) ==
  /\ ~off[p] /\ ~trim[p]
  /\ Gate = "retired" => ret[p]

\* Steps 1 and 2: read the record, read the segment from replica r and
\* build the object.
BuildObj(o, p, r) ==
  /\ ost[o] = "idle"
  /\ Takes(p)
  /\ held[r][p]
  /\ Build = "full" => applied[r][p] = L
  /\ ost' = [ost EXCEPT ![o] = "built"]
  /\ oseg' = [oseg EXCEPT ![o] = p]
  /\ olen' = [olen EXCEPT ![o] = applied[r][p]]
  /\ UNCHANGED <<rec, rep, store, pend, crashes>>

Idle(o) ==
  /\ ost' = [ost EXCEPT ![o] = "idle"]
  /\ oseg' = [oseg EXCEPT ![o] = 0]
  /\ olen' = [olen EXCEPT ![o] = 0]

\* Step 3: the put. A conditional put that finds a key fails.
PutObj(o) ==
  LET p == oseg[o]
      mine == Obj(Release[o], olen[o])
  IN /\ ost[o] = "built"
     /\ IF Put = "blind" \/ store[p] = Absent
          THEN /\ store' = [store EXCEPT ![p] = mine]
               /\ ost' = [ost EXCEPT ![o] = "mark"]
               /\ UNCHANGED <<oseg, olen>>
          ELSE /\ UNCHANGED store
               /\ IF \/ Conflict = "content" /\ Whole(p)
                     \/ Conflict = "bytes" /\ store[p] = mine
                    THEN /\ ost' = [ost EXCEPT ![o] = "mark"]
                         /\ UNCHANGED <<oseg, olen>>
                    ELSE Idle(o)
     /\ UNCHANGED <<rec, rep, pend, crashes>>

\* Step 4: the compare-and-set of the mark, from "not offloaded and not
\* trimmed". It can be lost: the record does not change, and the offloader
\* starts again from step 1, as after a timeout. After it, the offloader is
\* idle: it reads the record again before its next attempt.
MarkOk(o) ==
  LET p == oseg[o]
  IN /\ ost[o] = "mark"
     /\ Takes(p)
     /\ off' = [off EXCEPT ![p] = TRUE]
     /\ Idle(o)
     /\ UNCHANGED <<ret, trim, rep, store, pend, crashes>>

MarkLost(o) ==
  /\ ost[o] = "mark"
  /\ Takes(oseg[o])
  /\ Idle(o)
  /\ UNCHANGED <<rec, rep, store, pend, crashes>>

MarkFail(o) ==
  LET p == oseg[o]
  IN /\ ost[o] = "mark"
     /\ ~Takes(p)
     /\ store' = IF \/ Recheck = "trimmed" /\ trim[p] /\ IsObj(p)
                    \/ Recheck = "anyfail" /\ IsObj(p)
                   THEN [store EXCEPT ![p] = Absent]
                   ELSE store
     /\ Idle(o)
     /\ UNCHANGED <<rec, rep, pend, crashes>>

\* An offloader stops for good; one offloader stays up.
Stop(o) ==
  /\ ost[o] # "down"
  /\ crashes < NCrash
  /\ \E o2 \in Offloaders \ {o} : ost[o2] # "down"
  /\ ost' = [ost EXCEPT ![o] = "down"]
  /\ crashes' = crashes + 1
  /\ UNCHANGED <<rec, rep, store, oseg, olen, pend>>

(***************************************************************************)
(* The controller: trim, the delete after it, the scrub                    *)
(***************************************************************************)

\* A user truncate trims the next segment. The record write comes first.
Trim(p) ==
  /\ ~trim[p]
  /\ \A q \in Segs : q < p => trim[q]
  /\ trim' = [trim EXCEPT ![p] = TRUE]
  /\ pend' = [pend EXCEPT ![p] = TRUE]
  /\ UNCHANGED <<ret, off, rep, store, offl, crashes>>

\* The controller removes the object of the trimmed segment.
TrimDelete(p) ==
  /\ pend[p]
  /\ pend' = [pend EXCEPT ![p] = FALSE]
  /\ store' = CASE Cleanup = "tomb" -> [store EXCEPT ![p] = Tomb]
                [] TrimKey = "record" /\ ~off[p] -> store
                [] OTHER -> IF IsObj(p) THEN [store EXCEPT ![p] = Absent] ELSE store
  /\ UNCHANGED <<rec, rep, offl, crashes>>

\* The controller stops between the record write of a trim and the delete.
\* The controller that takes over does not know that a delete is to do.
CtlStop(p) ==
  /\ pend[p]
  /\ crashes < NCrash
  /\ pend' = [pend EXCEPT ![p] = FALSE]
  /\ crashes' = crashes + 1
  /\ UNCHANGED <<rec, rep, store, offl>>

\* The scrub lists the store and deletes an object by its key.
ScrubDel(p) ==
  /\ Cleanup \in {"scrub", "unmarked"}
  /\ IsObj(p)
  /\ trim[p] \/ (Cleanup = "unmarked" /\ ~off[p])
  /\ store' = [store EXCEPT ![p] = Absent]
  /\ UNCHANGED <<rec, rep, offl, pend, crashes>>

Next ==
  \/ \E r \in Replicas, p \in Segs : Apply(r, p) \/ DropCopy(r, p)
  \/ \E p \in Segs : Retire(p) \/ Trim(p) \/ TrimDelete(p) \/ CtlStop(p) \/ ScrubDel(p)
  \/ \E o \in Offloaders :
       \/ \E p \in Segs, r \in Replicas : BuildObj(o, p, r)
       \/ PutObj(o) \/ MarkOk(o) \/ MarkLost(o) \/ MarkFail(o) \/ Stop(o)

Spec == Init /\ [][Next]_vars

\* Every step but a user truncate, a lost mark and a stop is fair. A lost
\* mark is not fair: a write that is lost again and again is a network that
\* never heals.
Fair ==
  /\ \A r \in Replicas, p \in Segs : WF_vars(Apply(r, p)) /\ WF_vars(DropCopy(r, p))
  /\ \A p \in Segs : WF_vars(Retire(p)) /\ WF_vars(TrimDelete(p)) /\ WF_vars(ScrubDel(p))
  /\ \A o \in Offloaders :
       /\ WF_vars(\E p \in Segs, r \in Replicas : BuildObj(o, p, r))
       /\ WF_vars(PutObj(o)) /\ SF_vars(MarkOk(o)) /\ WF_vars(MarkFail(o))

LiveSpec == Spec /\ Fair

(***************************************************************************)
(* Properties                                                               *)
(***************************************************************************)

\* Every object of a segment holds the whole segment: the offloaders built
\* the same content, whatever their release.
ObjectTrue == \A p \in Segs : IsObj(p) => store[p].n = L

\* A reader finds each slot that is not trimmed. While the segment is not
\* retired, its acceptors hold it. Once it is retired and not offloaded,
\* F + 1 replicas hold it. Once the record says offloaded, its object holds it.
Findable ==
  \A p \in Segs : ~trim[p] =>
    /\ off[p] => Whole(p)
    /\ (ret[p] /\ ~off[p]) => Cardinality(Holders(p)) >= F + 1

\* Every segment ends offloaded or trimmed.
AllOffloaded == <>(\A p \in Segs : off[p] \/ trim[p])

\* No object of a trimmed segment stays in the store for good.
NoOrphan == <>[](\A p \in Segs : trim[p] => ~IsObj(p))

(***************************************************************************)
(* Reach: each of these must be false in some state, so that the passing    *)
(* checks are not empty.                                                    *)
(***************************************************************************)

\* An offloader's put finds the object of the other offloader.
NoDuplicate == ~\E o \in Offloaders : ost[o] = "built" /\ IsObj(oseg[o])
\* A put of a trimmed segment (the late upload).
NoLateUpload == ~\E p \in Segs : trim[p] /\ IsObj(p) /\ ~pend[p]
\* An offloader stopped with its object in the store and no mark.
NoStopBeforeMark == ~\E o \in Offloaders : ost[o] = "down" /\ oseg[o] # 0 /\ IsObj(oseg[o]) /\ ~off[oseg[o]]
\* Every replica dropped the copy of an offloaded segment.
NoAllDropped == ~\E p \in Segs : off[p] /\ ~trim[p] /\ \A r \in Replicas : ~held[r][p]
=============================================================================
