----------------------------- MODULE CtrlSegments -----------------------------
(***************************************************************************)
(* CTRL recovery in the journal, part 2: the copies of a sealed segment    *)
(* across retire and offload, and the repair of each copy that a scrub     *)
(* finds damaged (#1263 (CTRL recovery in the journal), items 2 and 4).    *)
(*                                                                         *)
(* A sealed segment S1 holds the slots 0 .. K - 1. Every slot of S1 is     *)
(* chosen (the seal, `experiments/segments/a/`). Its copies are:           *)
(*   - the records of the acceptors of its grid, until S1 retires;         *)
(*   - the logs of the replicas of its replica set (`segment-replicas/`);  *)
(*   - one object in the object store, once the offloader put it.          *)
(* The model is abstract: a copy of a slot is "good" (it reads back with   *)
(* its checksum), "bad" (it rotted), "none" (never held, or dropped) or    *)
(* "gone" (its machine is lost). The grid serves a slot while one of its   *)
(* acceptors holds a good record: `ctrl-grid/` checks that the tri-state   *)
(* tally decides a chosen slot right from such a record, or blocks.        *)
(*                                                                         *)
(* The controller scrubs every copy: a scrub reads a copy and finds its    *)
(* damage. A found damaged copy is repaired from a good copy: an acceptor  *)
(* record from a good record or a good replica (the repair of a chosen     *)
(* value, `ctrl-grid/`), a replica log from a live peer, from the grid     *)
(* until the retire, or from the object once the segment is offloaded      *)
(* (replicas serve offloaded reads), an object by a replace from a good    *)
(* replica log. When no good copy is left, the controller reports data     *)
(* loss (no unsafe recover in v1; Pierre, 2026-11-20).                     *)
(*                                                                         *)
(* The steps of the segment: `Retire` (the acceptors drop S1, after        *)
(* RetireAt replicas hold it), `Upload` (the offloader reads a replica log *)
(* and puts the object, a conditional put), `Mark` (the compare-and-set    *)
(* "offloaded" in the segment record) and `Drop` (a replica drops its      *)
(* copy).                                                                  *)
(*                                                                         *)
(* The faults. The survive promise of the journal is F: F copies may fail  *)
(* at once. A machine copy that rotted is a failed copy until its repair;  *)
(* a lost machine is a failed copy for ever (the model has no heal). So at *)
(* most F machine copies are bad or gone at one time, and a repair gives   *)
(* the budget back. The object store has its own durability and is not in *)
(* the promise: `ObjFaults` damaged objects may happen in the whole run.   *)
(*                                                                         *)
(* Constants (the first value of each is the design; each other value is   *)
(* a control):                                                             *)
(*   RetireAt    how many replicas must hold S1 before it retires (F + 1). *)
(*   RetireCount "clean": a replica whose copy a scrub found damaged and   *)
(*               not repaired does not count. "any": it counts.            *)
(*   UploadFrom  "good": the offloader checks each record's checksum and   *)
(*               puts only a good log. "any": it puts what it read.        *)
(*   DropWhen    "mark": a replica drops its copy only after the record    *)
(*               says offloaded. "retired": once S1 retired.               *)
(*   F           the survive number.                                       *)
(*   ObjFaults   how many objects may rot (in the whole run).              *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS Acceptors, Replicas, K, F, RetireAt, RetireCount, UploadFrom, DropWhen,
          ObjFaults

ASSUME K \in Nat /\ K >= 1 /\ F \in Nat /\ RetireAt \in Nat /\ ObjFaults \in Nat
ASSUME Acceptors \cap Replicas = {}
ASSUME RetireCount \in {"clean", "any"}
ASSUME UploadFrom \in {"good", "any"}
ASSUME DropWhen \in {"mark", "retired"}

Slots == 0..(K - 1)
Obj == "obj"
Holders == Acceptors \cup Replicas \cup {Obj}
Copy == {"none", "good", "bad", "gone"}

VARIABLES acc,      \* acceptor: its record of each slot
          rep,      \* replica: its log copy of each slot
          obj,      \* the object: its copy of each slot
          seg,      \* the segment record: "sealed", "retired" or "offloaded"
          found,    \* the damage that a scrub found and no repair fixed:
                    \* pairs <<holder, slot>>
          lost,     \* the slots that the controller reported as data loss
          objFaults \* how many objects rotted

vars == <<acc, rep, obj, seg, found, lost, objFaults>>

TypeOK ==
  /\ acc \in [Acceptors -> [Slots -> Copy]]
  /\ rep \in [Replicas -> [Slots -> Copy]]
  /\ obj \in [Slots -> Copy \ {"gone"}]
  /\ seg \in {"sealed", "retired", "offloaded"}
  /\ found \subseteq (Holders \X Slots)
  /\ lost \subseteq Slots
  /\ objFaults \in Nat

\* Every acceptor of the grid holds every slot (the seal); the replicas
\* learn the slots.
Init ==
  /\ acc = [a \in Acceptors |-> [s \in Slots |-> "good"]]
  /\ rep = [r \in Replicas |-> [s \in Slots |-> "none"]]
  /\ obj = [s \in Slots |-> "none"]
  /\ seg = "sealed"
  /\ found = {}
  /\ lost = {}
  /\ objFaults = 0

\* A machine is lost once one of its copies is gone.
Alive(h) ==
  IF h \in Acceptors THEN \A s \in Slots : acc[h][s] # "gone"
  ELSE \A s \in Slots : rep[h][s] # "gone"

\* The machine copies that failed now: bad or gone. A lost machine counts
\* once.
Failed ==
  {a \in Acceptors : ~Alive(a) \/ \E s \in Slots : acc[a][s] = "bad"}
    \cup {r \in Replicas : ~Alive(r) \/ \E s \in Slots : rep[r][s] = "bad"}

\* A good copy of slot s that a repair or a learn may read.
GridGood(s) == seg = "sealed" /\ \E a \in Acceptors : acc[a][s] = "good"
ReplicaGood(s) == \E r \in Replicas : rep[r][s] = "good"
ObjectGood(s) == seg = "offloaded" /\ obj[s] = "good"
GoodCopy(s) ==
  \/ \E a \in Acceptors : acc[a][s] = "good"
  \/ ReplicaGood(s)
  \/ obj[s] = "good"

-----------------------------------------------------------------------------
(* The replicas learn, the controller retires, the offloader puts, the     *)
(* replicas drop.                                                          *)

\* Replica r learns slot s from the grid, from a live peer or from the
\* object. A lost replica holds "gone" at every slot, never "none", so the
\* step needs no `Alive` guard.
Learn(r, s) ==
  /\ rep[r][s] = "none"
  /\ GridGood(s) \/ ReplicaGood(s) \/ ObjectGood(s)
  /\ rep' = [rep EXCEPT ![r][s] = "good"]
  /\ UNCHANGED <<acc, obj, seg, found, lost, objFaults>>

\* The replicas that the retire counts: they hold every slot of S1.
Counted ==
  {r \in Replicas :
     /\ \A s \in Slots : rep[r][s] \in {"good", "bad"}
     /\ RetireCount = "any" \/ \A s \in Slots : <<r, s>> \notin found}

\* S1 retires: its acceptors drop their records.
Retire ==
  /\ seg = "sealed"
  /\ Cardinality(Counted) >= RetireAt
  /\ seg' = "retired"
  /\ acc' = [a \in Acceptors |-> [s \in Slots |-> IF acc[a][s] = "gone" THEN "gone" ELSE "none"]]
  /\ found' = {p \in found : p[1] \notin Acceptors}
  /\ UNCHANGED <<rep, obj, lost, objFaults>>

\* The offloader reads the log of replica r and puts the object of slot s.
Upload(r, s) ==
  /\ seg = "retired"
  /\ obj[s] = "none"
  /\ rep[r][s] = "good" \/ (UploadFrom = "any" /\ rep[r][s] = "bad")
  /\ obj' = [obj EXCEPT ![s] = rep[r][s]]
  /\ UNCHANGED <<acc, rep, seg, found, lost, objFaults>>

\* The compare-and-set "offloaded", once every put returned. An object exists
\* only once S1 retired (`ObjAfterRetire`), and a second mark changes
\* nothing, so the step needs no guard on the record.
Mark ==
  /\ \A s \in Slots : obj[s] # "none"
  /\ seg' = "offloaded"
  /\ UNCHANGED <<acc, rep, obj, found, lost, objFaults>>

\* Replica r drops its copy of S1. A drop of an empty copy changes nothing.
Drop(r) ==
  /\ IF DropWhen = "mark" THEN seg = "offloaded" ELSE seg # "sealed"
  /\ Alive(r)
  /\ rep' = [rep EXCEPT ![r] = [s \in Slots |-> "none"]]
  /\ found' = {p \in found : p[1] # r}
  /\ UNCHANGED <<acc, obj, seg, lost, objFaults>>

-----------------------------------------------------------------------------
(* Faults, scrub and repair *)

\* A good machine copy rots, inside the budget of F failed copies.
RotAcc(a, s) ==
  /\ acc[a][s] = "good"
  /\ Cardinality(Failed \cup {a}) <= F
  /\ acc' = [acc EXCEPT ![a][s] = "bad"]
  /\ UNCHANGED <<rep, obj, seg, found, lost, objFaults>>

RotRep(r, s) ==
  /\ rep[r][s] = "good"
  /\ Cardinality(Failed \cup {r}) <= F
  /\ rep' = [rep EXCEPT ![r][s] = "bad"]
  /\ UNCHANGED <<acc, obj, seg, found, lost, objFaults>>

\* A machine is lost for ever, inside the same budget. A machine that is
\* lost already holds "gone" at every slot and has no found damage, so a
\* second loss changes nothing and the step needs no `Alive` guard.
LoseAcc(a) ==
  /\ Cardinality(Failed \cup {a}) <= F
  /\ acc' = [acc EXCEPT ![a] = [s \in Slots |-> "gone"]]
  /\ found' = {p \in found : p[1] # a}
  /\ UNCHANGED <<rep, obj, seg, lost, objFaults>>

LoseRep(r) ==
  /\ Cardinality(Failed \cup {r}) <= F
  /\ rep' = [rep EXCEPT ![r] = [s \in Slots |-> "gone"]]
  /\ found' = {p \in found : p[1] # r}
  /\ UNCHANGED <<acc, obj, seg, lost, objFaults>>

\* An object rots: outside the survive promise.
RotObj(s) ==
  /\ obj[s] = "good"
  /\ objFaults < ObjFaults
  /\ obj' = [obj EXCEPT ![s] = "bad"]
  /\ objFaults' = objFaults + 1
  /\ UNCHANGED <<acc, rep, seg, found, lost>>

\* The state of the copy of holder h at slot s.
CopyOf(h, s) == IF h \in Acceptors THEN acc[h][s] ELSE IF h \in Replicas THEN rep[h][s] ELSE obj[s]

\* A scrub of the controller reads the copy of holder h at slot s and finds
\* its damage (a second find adds nothing).
Scrub(h, s) ==
  /\ CopyOf(h, s) = "bad"
  /\ found' = found \cup {<<h, s>>}
  /\ UNCHANGED <<acc, rep, obj, seg, lost, objFaults>>

\* The repair of a found damaged copy, from a good copy (`NoResurrection`).
\* A found copy is bad (`FoundBad`), and the retire leaves no acceptor
\* record, so a found acceptor copy is one of a sealed S1.
RepairAcc(a, s) ==
  /\ <<a, s>> \in found
  /\ GridGood(s) \/ ReplicaGood(s)
  /\ acc' = [acc EXCEPT ![a][s] = "good"]
  /\ found' = found \ {<<a, s>>}
  /\ UNCHANGED <<rep, obj, seg, lost, objFaults>>

RepairRep(r, s) ==
  /\ <<r, s>> \in found
  /\ GridGood(s) \/ ReplicaGood(s) \/ ObjectGood(s)
  /\ rep' = [rep EXCEPT ![r][s] = "good"]
  /\ found' = found \ {<<r, s>>}
  /\ UNCHANGED <<acc, obj, seg, lost, objFaults>>

\* The replace of a damaged object, from a good replica log, in one step.
\* The conditional put of the offloader never replaces an object, so how a
\* replace works (a delete that the mark fences) is Open for the offload
\* design (docs/journal/03-segments-and-records.md §J3.1).
RepairObj(s) ==
  /\ <<Obj, s>> \in found
  /\ ReplicaGood(s)
  /\ obj' = [obj EXCEPT ![s] = "good"]
  /\ found' = found \ {<<Obj, s>>}
  /\ UNCHANGED <<acc, rep, seg, lost, objFaults>>

\* The controller reports data loss: no good copy of slot s is left. It
\* reads its view, not only its scrubs: a slot whose copies are all on lost
\* machines has no damaged copy to find (docs/journal/06-controller.md
\* §J6.2, `data_lost`). A second report adds nothing.
ReportLoss(s) ==
  /\ ~GoodCopy(s)
  /\ lost' = lost \cup {s}
  /\ UNCHANGED <<acc, rep, obj, seg, found, objFaults>>

Next ==
  \/ Retire \/ Mark
  \/ \E r \in Replicas : Drop(r) \/ LoseRep(r)
  \/ \E a \in Acceptors : LoseAcc(a)
  \/ \E s \in Slots : RotObj(s) \/ RepairObj(s) \/ ReportLoss(s)
  \/ \E r \in Replicas, s \in Slots :
       Learn(r, s) \/ Upload(r, s) \/ RotRep(r, s) \/ RepairRep(r, s)
  \/ \E a \in Acceptors, s \in Slots : RotAcc(a, s) \/ RepairAcc(a, s)
  \/ \E h \in Holders, s \in Slots : Scrub(h, s)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties *)

\* The survive promise: with at most F failed machine copies at once, every
\* slot keeps a good copy. A damaged object is outside the promise.
Survive == objFaults = 0 => \A s \in Slots : GoodCopy(s)

\* A loss report is true: no good copy of the slot is left.
TrueLoss == \A s \in lost : ~GoodCopy(s)

\* Every repair finds a source: a found damaged copy of a live holder has a
\* good copy to repair from, unless a damaged object left none.
Repairable ==
  objFaults = 0 =>
    \A p \in found :
      LET h == p[1]
          s == p[2]
      IN \/ h \in Acceptors /\ (GridGood(s) \/ ReplicaGood(s))
         \/ h \in Replicas /\ (GridGood(s) \/ ReplicaGood(s) \/ ObjectGood(s))
         \/ h = Obj /\ ReplicaGood(s)

\* Liveness. The controller keeps scrubbing, repairing and reporting, and
\* the segment moves on. No fault is forced.
Fairness ==
  /\ WF_vars(Retire) /\ WF_vars(Mark)
  /\ \A r \in Replicas : WF_vars(Drop(r))
  /\ \A s \in Slots :
       /\ WF_vars(RepairObj(s)) /\ WF_vars(ReportLoss(s))
       /\ \A r \in Replicas : WF_vars(Learn(r, s)) /\ WF_vars(Upload(r, s)) /\ WF_vars(RepairRep(r, s))
       /\ \A a \in Acceptors : WF_vars(RepairAcc(a, s))
       /\ \A h \in Holders : WF_vars(Scrub(h, s))

LiveSpec == Spec /\ Fairness

\* Each slot ends with a good copy or with a loss report.
Ends == <>[](\A s \in Slots : GoodCopy(s) \/ s \in lost)

\* With no damaged object, each slot ends with a good copy.
Kept == <>[](\A s \in Slots : GoodCopy(s))

\* Every found damage ends repaired, dropped with its copy, or reported.
Heals ==
  \A h \in Holders, s \in Slots :
    [](<<h, s>> \in found => <>(<<h, s>> \notin found \/ s \in lost))

\* The damage that the controller found is still there: only a repair, a
\* drop, a loss or the retire takes a copy out of `found`.
FoundBad == \A p \in found : CopyOf(p[1], p[2]) = "bad"

\* A lost machine stays lost: no step brings a "gone" copy back.
LostForEver ==
  [][/\ \A a \in Acceptors, s \in Slots : acc[a][s] = "gone" => acc'[a][s] = "gone"
     /\ \A r \in Replicas, s \in Slots : rep[r][s] = "gone" => rep'[r][s] = "gone"]_vars

\* The segment record moves forward only, each step a compare-and-set on
\* the state before it.
SegForward ==
  [][seg' # seg => <<seg, seg'>> \in {<<"sealed", "retired">>, <<"retired", "offloaded">>}]_vars

\* The offloader puts the object of S1 only once S1 retired.
ObjAfterRetire == seg = "sealed" => \A s \in Slots : obj[s] = "none"

\* Every copy comes from a good copy: a slot with no good copy never gets
\* one back (no unsafe recover in v1).
NoResurrection == [][\A s \in Slots : ~GoodCopy(s) => ~GoodCopy(s)']_vars

\* Reachability controls: each must be violated.
NoOffload == seg # "offloaded" \/ \E r \in Replicas, s \in Slots : rep[r][s] \notin {"none", "gone"}
NoLossReport == lost = {}
NoRepairAfterRetire ==
  ~(seg # "sealed" /\ \E r \in Replicas, s \in Slots : <<r, s>> \in found /\ rep[r][s] = "bad"
      /\ ENABLED RepairRep(r, s))
=============================================================================
