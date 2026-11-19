------------------------------ MODULE SegmentsA ------------------------------
(***************************************************************************)
(* Experiment #1213 (segments in the protocol or in storage), design A:    *)
(* the matchmakers of a journal hold its segment table. This is an         *)
(* experiment, not a model of the plan: it changes no model of             *)
(* specs/tla/.                                                             *)
(*                                                                         *)
(* A segment is the run of slots that one configuration (one grid) holds.  *)
(* The table has one record for each configuration g: a start slot, an    *)
(* end slot and a state (open, sealed, retired, offloaded, trimmed). The   *)
(* end slot is written once, at seal, by the first leader of g + 1 after   *)
(* its Phase 1. A leader change in g opens no segment.                     *)
(*                                                                         *)
(* The matchmakers are one atomic register here: the matchmaker model      *)
(* (specs/tla/matchmaker/) and the handover model check how a quorum of    *)
(* matchmakers keeps that register. The configurations are registered in   *)
(* ballot order, and a ballot selects the configuration in force or its    *)
(* one successor (plan rule 1).                                            *)
(*                                                                         *)
(* The model has a reshape (a ballot of the successor), a leader change    *)
(* (a later ballot of the same configuration), a leader crash (a leader    *)
(* that stops taking steps; a later ballot takes over), retire             *)
(* (Matchmaker garbage collection), offload to an object store, replicas   *)
(* that drop their copy after offload, trim, a consumer cursor and a tail  *)
(* reader that caches the table and fetches it again on a "sealed" error.  *)
(* Acceptors and replicas keep their state on disk, so a restart changes   *)
(* nothing that the model sees.                                            *)
(*                                                                         *)
(* Constants select the rules. The first value of each is the rule that    *)
(* passes; each other value is a control.                                  *)
(*   EndRule   "voted": the end is one above the highest slot that Phase 1 *)
(*             saw a vote at in the old configuration. "uncommitted": the  *)
(*             end is the first slot that the replicas did not apply (the  *)
(*             design doc's first text).                                   *)
(*   ReadOld   "all": Phase 1 reads every slot of a sealed configuration.  *)
(*             "belowEnd": it reads a sealed configuration below its end.  *)
(*   SealCas   TRUE: the end is a compare-and-set from "none".             *)
(*             FALSE: a leader writes the end over an end that is there.   *)
(*   Fence     "config": a leader writes while its promise quorum holds.   *)
(*             "table": a write also needs the table version that the      *)
(*             leader read, so any change of the table stops the tail.     *)
(*   RetireRule "paper": retire a sealed configuration when f+1 replicas   *)
(*             hold its slots and each slot with a vote in it at or above  *)
(*             its end is chosen in a later configuration. "early": retire *)
(*             at seal.                                                    *)
(*   DropRule  "offloaded": a replica drops a segment once the record says *)
(*             offloaded. "uploaded": once the object is in the store.     *)
(*   TrimCheck TRUE: trim stops at the consumer cursor. FALSE: it does not.*)
(*   ReadFence TRUE: an acceptor that promised a ballot of a later         *)
(*             configuration, or that retired, answers a tail read with    *)
(*             "sealed". FALSE: it answers with its votes.                 *)
(*   Reader    TRUE: the tail reader runs. FALSE: it does not (fewer       *)
(*             states for the checks that do not need it).                 *)
(*   Storage   TRUE: offload, drop, trim and the consumer run. FALSE: they *)
(*             do not (fewer states for the checks of the seal and of the  *)
(*             tail reader, which do not need them).                       *)
(*   GapFill   TRUE: a leader also proposes a Noop at each slot above the  *)
(*             applied prefix and below the start of its segment where     *)
(*             Phase 1 saw no vote (the election gap fill of the core).    *)
(*             FALSE: below the start it proposes only where Phase 1 saw a *)
(*             vote, so a slot with no vote below the end stays a hole.    *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Acceptors, Replicas, Values, None, Noop,
          CfgOf,        \* configuration -> its acceptors
          NS, NB, NG, F,
          EndRule, ReadOld, SealCas, Fence, RetireRule, DropRule,
          TrimCheck, ReadFence, Reader, Storage, GapFill

ASSUME EndRule \in {"voted", "uncommitted"}
ASSUME ReadOld \in {"all", "belowEnd"}
ASSUME SealCas \in BOOLEAN /\ TrimCheck \in BOOLEAN /\ ReadFence \in BOOLEAN /\ Reader \in BOOLEAN
ASSUME Storage \in BOOLEAN /\ GapFill \in BOOLEAN
ASSUME Fence \in {"config", "table"}
ASSUME RetireRule \in {"paper", "early"}
ASSUME DropRule \in {"offloaded", "uploaded"}
ASSUME None \notin Values /\ Noop \notin Values /\ Noop # None
ASSUME F + 1 <= Cardinality(Replicas)

Slots == 1..NS
Ballots == 1..NB
Gens == 1..NG
\* What a slot can hold: a client value, or the Noop of the gap fill.
Cmds == Values \cup {Noop}
ASSUME \A g \in Gens : CfgOf[g] # {} /\ CfgOf[g] \subseteq Acceptors

Maj(S) == {Q \in SUBSET S : 2 * Cardinality(Q) > Cardinality(S)}
Max(S) == CHOOSE x \in S : \A y \in S : y <= x

VARIABLES
  \* Matchmakers: the registrations and the segment table.
  bgen,     \* ballot -> its configuration, 0 before it registers
  sst,      \* configuration -> "none", "open", "sealed", "retired", "offloaded", "trimmed"
  sstart,   \* configuration -> its start slot, 0 while unknown
  send,     \* configuration -> its end slot, 0 while open
  ver,      \* the table version (only Fence = "table" counts it)
  \* Leaders.
  bst,      \* ballot -> "new", "p1" (Phase 1 done), "w" (writes)
  bsafe,    \* ballot -> slot -> the value that Phase 1 makes it propose, or None
  bmax,     \* ballot -> the highest slot with a vote that Phase 1 saw
  bver,     \* ballot -> the table version that it read
  prop,     \* ballot -> slot -> its proposal, or None
  \* Acceptors.
  prom,     \* acceptor -> the highest ballot it promised or voted at
  vote,     \* acceptor -> slot -> <<ballot, value>>, <<0, None>> when none
  sent2b,   \* every vote ever sent: <<slot, ballot, value, acceptor>>
  \* Replicas, the object store, trim.
  applied,  \* replica -> its applied prefix
  hold,     \* replica -> the slots it keeps on its disk
  store,    \* configuration -> slot -> the bytes of its object, all None when no object
  floor,    \* the trim floor: the slots below it are trimmed
  cursor,   \* the next slot that the consumer reads
  \* The tail reader.
  rview,    \* the configuration of its cached table
  rneed,    \* the highest chosen slot when its read started
  rgot,     \* the tail that its read returned
  rst       \* "idle", "wait", "done"

mm == <<bgen, sst, sstart, send, ver>>
ldr == <<bst, bsafe, bmax, bver, prop>>
acc == <<prom, vote, sent2b>>
dat == <<applied, hold, store, floor, cursor>>
rdr == <<rview, rneed, rgot, rst>>
vars == <<mm, ldr, acc, dat, rdr>>

NoProp == [s \in Slots |-> None]

TypeOK ==
  /\ bgen \in [Ballots -> 0..NG]
  /\ sst \in [Gens -> {"none", "open", "sealed", "retired", "offloaded", "trimmed"}]
  /\ sstart \in [Gens -> 0..NS + 1]
  /\ send \in [Gens -> 0..NS + 1]
  /\ bst \in [Ballots -> {"new", "p1", "w"}]
  /\ bsafe \in [Ballots -> [Slots -> Cmds \cup {None}]]
  /\ prop \in [Ballots -> [Slots -> Cmds \cup {None}]]
  /\ prom \in [Acceptors -> 0..NB]
  /\ applied \in [Replicas -> 0..NS]
  /\ hold \in [Replicas -> SUBSET Slots]
  /\ store \in [Gens -> [Slots -> Cmds \cup {None}]]
  /\ floor \in 1..NS + 1
  /\ cursor \in 1..NS + 1

Init ==
  /\ bgen = [b \in Ballots |-> 0]
  /\ sst = [g \in Gens |-> IF g = 1 THEN "open" ELSE "none"]
  /\ sstart = [g \in Gens |-> IF g = 1 THEN 1 ELSE 0]
  /\ send = [g \in Gens |-> 0]
  /\ ver = 0
  /\ bst = [b \in Ballots |-> "new"]
  /\ bsafe = [b \in Ballots |-> NoProp]
  /\ bmax = [b \in Ballots |-> 0]
  /\ bver = [b \in Ballots |-> 0]
  /\ prop = [b \in Ballots |-> NoProp]
  /\ prom = [a \in Acceptors |-> 0]
  /\ vote = [a \in Acceptors |-> [s \in Slots |-> <<0, None>>]]
  /\ sent2b = {}
  /\ applied = [r \in Replicas |-> 0]
  /\ hold = [r \in Replicas |-> {}]
  /\ store = [g \in Gens |-> NoProp]
  /\ floor = 1
  /\ cursor = 1
  /\ rview = 1
  /\ rneed = 0
  /\ rgot = 0
  /\ rst = "idle"

Bump == ver' = IF Fence = "table" THEN ver + 1 ELSE ver

(***************************************************************************)
(* Helpers                                                                 *)
(***************************************************************************)

\* The configurations that still run: registered, not retired.
Live == {g \in Gens : sst[g] \in {"open", "sealed"}}
\* The acceptors that still run.
Active == UNION {CfgOf[g] : g \in Live}
\* The newest registered configuration.
Top == Max({g \in Gens : sst[g] # "none"})
\* The configuration in force: the newest one that opened.
InForce == Max({g \in Gens : sst[g] # "none" /\ sstart[g] # 0})
Range(g) == IF sstart[g] = 0 \/ send[g] = 0 THEN {} ELSE sstart[g]..(send[g] - 1)

ChosenAtBal(s, v, b) ==
  bgen[b] # 0 /\ \E Q \in Maj(CfgOf[bgen[b]]) : \A a \in Q : <<s, b, v, a>> \in sent2b
Chosen(s, v) == \E b \in Ballots : ChosenAtBal(s, v, b)
IsChosen(s) == \E v \in Cmds : Chosen(s, v)
ChosenVal(s) == CHOOSE v \in Cmds : Chosen(s, v)
ChosenInGen(s, g) == \E b \in Ballots, v \in Cmds : bgen[b] = g /\ ChosenAtBal(s, v, b)

MaxApplied == Max({applied[r] : r \in Replicas})
\* f+1 replicas applied slot s.
Replicated(s) == Cardinality({r \in Replicas : applied[r] >= s}) >= F + 1

(***************************************************************************)
(* Matchmaking and Phase 1                                                 *)
(***************************************************************************)

\* Ballot b registers configuration g: the one in force or its successor.
\* A successor opens a record with no start yet.
Register(b, g) ==
  /\ bgen[b] = 0
  /\ \A c \in Ballots : bgen[c] # 0 => c < b
  /\ g \in {Top, Top + 1}
  /\ g <= NG
  /\ g = Top + 1 => sstart[Top] # 0
  /\ bgen' = [bgen EXCEPT ![b] = g]
  /\ IF g = Top + 1
       THEN /\ sst' = [sst EXCEPT ![g] = "open"]
            /\ Bump
       ELSE UNCHANGED <<sst, ver>>
  /\ UNCHANGED <<sstart, send, ldr, acc, dat, rdr>>

\* The slots that Phase 1 reads in configuration h.
ReadSet(h) == IF ReadOld = "belowEnd" /\ send[h] # 0 THEN 1..(send[h] - 1) ELSE Slots

\* Phase 1 of ballot b: a promise quorum of every configuration that still
\* runs, up to its own. The leader keeps, for each slot, the value of the
\* highest vote that it saw.
Phase1(b) ==
  LET g == bgen[b]
      H == {h \in Live : h <= g}
  IN /\ g # 0
     /\ bst[b] = "new"
     /\ \E Qs \in [H -> SUBSET Acceptors] :
          /\ \A h \in H : Qs[h] \in Maj(CfgOf[h]) /\ \A a \in Qs[h] : prom[a] < b
          /\ LET seen == UNION {{<<s, vote[a][s]>> : a \in UNION {Qs[h] : h \in {k \in H : s \in ReadSet(k)}}} : s \in Slots}
                 voted == {x \in seen : x[2][1] > 0}
                 top(s) == CHOOSE x \in voted : x[1] = s /\ \A y \in voted : y[1] = s => y[2][1] <= x[2][1]
             IN /\ bsafe' = [bsafe EXCEPT ![b] =
                    [s \in Slots |-> IF \E x \in voted : x[1] = s THEN top(s)[2][2] ELSE None]]
                /\ bmax' = [bmax EXCEPT ![b] = Max({x[1] : x \in voted} \cup {0})]
          /\ prom' = [a \in Acceptors |-> IF \E h \in H : a \in Qs[h] THEN b ELSE prom[a]]
     /\ bst' = [bst EXCEPT ![b] = "p1"]
     /\ bver' = [bver EXCEPT ![b] = ver]
     /\ UNCHANGED <<mm, prop, vote, sent2b, dat, rdr>>

\* The first leader of configuration g seals g - 1: it writes the end of
\* g - 1 and the start of g.
Seal(b) ==
  LET g == bgen[b]
      p == g - 1
      e == IF EndRule = "voted"
             THEN Max({bmax[b], MaxApplied, sstart[p] - 1}) + 1
             ELSE Max({MaxApplied, sstart[p] - 1}) + 1
  IN /\ bst[b] = "p1"
     /\ g >= 2
     /\ sstart[p] # 0
     /\ IF SealCas THEN send[p] = 0 ELSE sst[p] \in {"open", "sealed"}
     /\ send' = [send EXCEPT ![p] = e]
     /\ sstart' = [sstart EXCEPT ![g] = e]
     /\ sst' = [sst EXCEPT ![p] = "sealed"]
     /\ Bump
     /\ bst' = [bst EXCEPT ![b] = "w"]
     /\ UNCHANGED <<bgen, bsafe, bmax, bver, prop, acc, dat, rdr>>

\* A leader whose configuration already opened starts to write.
Open(b) ==
  /\ bst[b] = "p1"
  /\ sstart[bgen[b]] # 0
  /\ bst' = [bst EXCEPT ![b] = "w"]
  /\ UNCHANGED <<mm, bsafe, bmax, bver, prop, acc, dat, rdr>>

\* With Fence = "table", a leader reads the table again after a change.
Refresh(b) ==
  /\ Fence = "table"
  /\ bst[b] = "w"
  /\ bver[b] # ver
  /\ bver' = [bver EXCEPT ![b] = ver]
  /\ UNCHANGED <<mm, bst, bsafe, bmax, prop, acc, dat, rdr>>

(***************************************************************************)
(* Phase 2                                                                 *)
(***************************************************************************)

\* The leader of b may write at the tail.
CanWrite(b) ==
  /\ bst[b] = "w"
  /\ Fence = "table" => bver[b] = ver
  /\ \E Q \in Maj(CfgOf[bgen[b]]) : Q \subseteq Active /\ \A a \in Q : prom[a] <= b

\* The slots that the leader of b proposes at: every slot of its own segment,
\* and below it each slot that the replicas did not apply. Where Phase 1
\* saw a vote below the start, the leader proposes that vote again in its
\* own grid; where it saw none, it proposes a Noop (rule 2b of the report).
\* Without the Noop, a slot below the end with no vote is a hole that no
\* ballot can write: the old grid is sealed, and no later ballot registers
\* it again.
Writable(b) ==
  {s \in Slots : /\ s >= floor
                 /\ \/ s >= sstart[bgen[b]]
                    \/ s > MaxApplied /\ (bsafe[b][s] # None \/ GapFill)}

Propose(b, s, v) ==
  /\ CanWrite(b)
  /\ s \in Writable(b)
  /\ prop[b][s] = None
  \* A leader proposes in slot order (the allocator frontier).
  /\ \A t \in Writable(b) : t < s => prop[b][t] # None
  /\ v \in IF bsafe[b][s] # None THEN {bsafe[b][s]}
           ELSE IF s < sstart[bgen[b]] THEN {Noop}
           ELSE Values
  /\ prop' = [prop EXCEPT ![b][s] = v]
  /\ UNCHANGED <<mm, bst, bsafe, bmax, bver, acc, dat, rdr>>

Accept(a, b, s) ==
  /\ bgen[b] # 0
  /\ a \in CfgOf[bgen[b]]
  /\ a \in Active
  /\ prop[b][s] # None
  /\ prom[a] <= b
  /\ vote[a][s][1] < b
  /\ prom' = [prom EXCEPT ![a] = b]
  /\ vote' = [vote EXCEPT ![a][s] = <<b, prop[b][s]>>]
  /\ sent2b' = sent2b \cup {<<s, b, prop[b][s], a>>}
  /\ UNCHANGED <<mm, ldr, dat, rdr>>

(***************************************************************************)
(* Replicas, retire, offload, trim                                         *)
(***************************************************************************)

Apply(r) ==
  /\ applied[r] < NS
  /\ IsChosen(applied[r] + 1)
  /\ applied' = [applied EXCEPT ![r] = @ + 1]
  /\ hold' = [hold EXCEPT ![r] = IF applied[r] + 1 >= floor THEN @ \cup {applied[r] + 1} ELSE @]
  /\ UNCHANGED <<mm, ldr, acc, store, floor, cursor, rdr>>

\* Matchmaker garbage collection of a sealed configuration.
Retire(p) ==
  /\ sst[p] = "sealed"
  /\ RetireRule = "paper" =>
       /\ \A s \in Range(p) : Replicated(s)
       /\ \A a \in CfgOf[p], s \in Slots :
            vote[a][s][1] > 0 /\ s >= send[p] => \E g \in Gens : g > p /\ ChosenInGen(s, g)
  /\ sst' = [sst EXCEPT ![p] = "retired"]
  /\ Bump
  /\ UNCHANGED <<bgen, sstart, send, ldr, acc, dat, rdr>>

\* The offloader copies a retired segment to its object. The key is the
\* journal and the configuration, so no location is written.
Upload(p) ==
  /\ sst[p] = "retired"
  /\ store[p] = NoProp
  /\ \A s \in Range(p) : IsChosen(s)
  /\ store' = [store EXCEPT ![p] = [s \in Slots |-> IF s \in Range(p) THEN ChosenVal(s) ELSE None]]
  /\ UNCHANGED <<mm, ldr, acc, applied, hold, floor, cursor, rdr>>

\* The record says offloaded once the object is there.
Mark(p) ==
  /\ sst[p] = "retired"
  /\ store[p] # NoProp
  /\ sst' = [sst EXCEPT ![p] = "offloaded"]
  /\ Bump
  /\ UNCHANGED <<bgen, sstart, send, ldr, acc, dat, rdr>>

Drop(r, p) ==
  /\ IF DropRule = "offloaded" THEN sst[p] = "offloaded" ELSE store[p] # NoProp
  /\ hold[r] \cap Range(p) # {}
  /\ hold' = [hold EXCEPT ![r] = @ \ Range(p)]
  /\ UNCHANGED <<mm, ldr, acc, applied, store, floor, cursor, rdr>>

\* Trim every segment up to the end of p. The records leave the table
\* (state "trimmed") and the objects are deleted by their keys.
Trim(p) ==
  LET f == send[p]
  IN /\ sst[p] \in {"retired", "offloaded"}
     /\ f > floor
     /\ TrimCheck => f <= cursor
     /\ sst' = [g \in Gens |-> IF send[g] # 0 /\ send[g] <= f THEN "trimmed" ELSE sst[g]]
     /\ store' = [g \in Gens |-> IF send[g] # 0 /\ send[g] <= f THEN NoProp ELSE store[g]]
     /\ hold' = [r \in Replicas |-> {s \in hold[r] : s >= f}]
     /\ floor' = f
     /\ Bump
     /\ UNCHANGED <<bgen, sstart, send, ldr, acc, applied, cursor, rdr>>

\* The consumer reads its next slot.
Consume ==
  /\ cursor <= NS
  /\ cursor >= floor
  /\ IsChosen(cursor)
  /\ cursor' = cursor + 1
  /\ UNCHANGED <<mm, ldr, acc, applied, hold, store, floor, rdr>>

(***************************************************************************)
(* The tail reader (check 5)                                               *)
(***************************************************************************)

ReadStart ==
  /\ Reader
  /\ rst = "idle"
  /\ rneed' = Max({s \in Slots : IsChosen(s)} \cup {0})
  /\ rst' = "wait"
  /\ UNCHANGED <<mm, ldr, acc, dat, rview, rgot>>

\* An acceptor answers "sealed" when it retired or promised a ballot of a
\* later configuration than the reader's view.
SaysSealed(a) == a \notin Active \/ (prom[a] # 0 /\ bgen[prom[a]] > rview)

\* A leaderless read of the tail on a quorum of the cached configuration.
ReadAsk(Q) ==
  /\ Reader
  /\ rst = "wait"
  /\ Q \in Maj(CfgOf[rview])
  /\ IF ReadFence /\ \E a \in Q : SaysSealed(a)
       THEN /\ rview' = InForce
            /\ UNCHANGED <<rgot, rst>>
       \* The slots below the start of the cached configuration are in sealed
       \* segments, so the tail is at least the start minus one.
       ELSE /\ rgot' = Max({s \in Slots : \E b \in Ballots, v \in Cmds :
                              bgen[b] = rview /\ \A a \in Q : <<s, b, v, a>> \in sent2b}
                           \cup {sstart[rview] - 1})
            /\ rst' = "done"
            /\ UNCHANGED rview
  /\ UNCHANGED <<mm, ldr, acc, dat, rneed>>

ReadAgain ==
  /\ rst = "done"
  /\ rst' = "idle"
  /\ UNCHANGED <<mm, ldr, acc, dat, rview, rneed, rgot>>

Next ==
  \/ \E b \in Ballots, g \in Gens : Register(b, g)
  \/ \E b \in Ballots : Phase1(b) \/ Seal(b) \/ Open(b) \/ Refresh(b)
  \/ \E b \in Ballots, s \in Slots, v \in Cmds : Propose(b, s, v)
  \/ \E a \in Acceptors, b \in Ballots, s \in Slots : Accept(a, b, s)
  \/ \E r \in Replicas : Apply(r)
  \/ \E p \in Gens : Retire(p)
  \/ Storage /\ \E p \in Gens : Upload(p) \/ Mark(p) \/ Trim(p)
  \/ Storage /\ \E r \in Replicas, p \in Gens : Drop(r, p)
  \/ Storage /\ Consume
  \/ ReadStart \/ ReadAgain
  \/ \E Q \in UNION {Maj(CfgOf[g]) : g \in Gens} : ReadAsk(Q)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Checks                                                                  *)
(***************************************************************************)

\* Check 1: at most one value is chosen for each slot.
Agreement == \A s \in Slots, v, w \in Cmds : Chosen(s, v) /\ Chosen(s, w) => v = w

\* Nothing is chosen in a sealed configuration at or above its end.
SealedFrozen == \A p \in Gens, s \in Slots : send[p] # 0 /\ s >= send[p] => ~ChosenInGen(s, p)

\* A chosen slot is on a live write row: chosen at a ballot whose
\* configuration still runs.
OnLiveRow(s) == \E b \in Ballots, v \in Cmds : bgen[b] \in Live /\ ChosenAtBal(s, v, b)
InStore(s) == \E p \in Gens : store[p][s] # None

\* Check 2: every chosen slot at or above the floor can be read from a
\* live write row, f+1 replicas, or the object store, with the chosen bytes.
Readable ==
  \A s \in Slots : s >= floor /\ IsChosen(s) =>
    \/ OnLiveRow(s)
    \/ Cardinality({r \in Replicas : s \in hold[r]}) >= F + 1
    \/ \E p \in Gens : store[p][s] = ChosenVal(s)

\* Check 3, the state half: at most one object holds a slot. (A key holds
\* one object by construction; the action half is StoreImmutable.)
OneHome == \A s \in Slots, p, q \in Gens : store[p][s] # None /\ store[q][s] # None => p = q

\* Check 3, the action half: an object never changes; trim may delete it.
StoreImmutable == [][\A p \in Gens : store[p] # NoProp => store'[p] \in {store[p], NoProp}]_vars

\* The end slot is written once.
EndOnce == [][\A p \in Gens : send[p] # 0 => send'[p] = send[p]]_vars

\* Check 4 is Agreement under ReadOld = "belowEnd": Phase 1 reads a sealed
\* configuration only below its end, so a recovery reads at most the open
\* segment, its successor and the slots of each sealed segment that did not
\* retire. The bound is safe when Agreement holds with it.

\* Check 5: a tail read returns at least the tail chosen before it started.
ReadFresh == rst = "done" => rgot >= rneed

\* Check 6: a chosen slot whose only copy is in the object store is declared
\* in the table: its record says offloaded and names it.
Declared(s) ==
  \E p \in Gens : sst[p] = "offloaded" /\ s \in Range(p) /\ store[p][s] = ChosenVal(s)
Visible ==
  \A s \in Slots :
    (s >= floor /\ IsChosen(s) /\ ~OnLiveRow(s) /\ \A r \in Replicas : s \notin hold[r])
      => Declared(s)

\* The table is one consistent view: the sealed and later records cover
\* the slots below the start of the configuration in force with no overlap.
TableSound ==
  \A p, q \in Gens : p < q /\ sstart[q] # 0 /\ send[p] # 0 => send[p] <= sstart[q]

\* Check 7: offload, retire and trim of a sealed segment never stop a leader
\* that could write at the tail.
OffloadStep == \E p \in Gens : Upload(p) \/ Mark(p) \/ Trim(p) \/ Retire(p)
                 \/ \E r \in Replicas : Drop(r, p)
TailNeverBlocked == [][OffloadStep => \A b \in Ballots : CanWrite(b) => CanWrite(b)']_vars

\* The consumer cursor is never below the floor.
CursorSafe == cursor >= floor

\* No stranded hole: a hole is a slot at or above the floor that is not
\* chosen. Each leader of the configuration in force that finished Phase 1
\* may propose at each hole, so the chosen prefix can always grow past it.
\* A hole that no such leader may propose stays a hole for ever: a later
\* leader change in the same configuration sees what this one saw, and the
\* replicas stop at it, so retire, offload and trim never come.
NoStrandedHole ==
  \A b \in Ballots, s \in Slots :
    bst[b] = "w" /\ bgen[b] = InForce /\ s >= floor /\ ~IsChosen(s) => s \in Writable(b)

\* Reachability controls: each must be violated.
NoOffload == \A p \in Gens : sst[p] # "offloaded"
NoDropAfterOffload == ~\E s \in Slots : IsChosen(s) /\ s >= floor /\ ~OnLiveRow(s)
                         /\ \A r \in Replicas : s \notin hold[r]
NoTrim == floor = 1
NoReshapeWrite == ~\E s \in Slots : ChosenInGen(s, 2)
\* A slot below the end of the sealed configuration 1 is chosen in
\* configuration 2: the sealing leader proposed again a vote that was not
\* chosen. It needs more than one acceptor in a configuration.
NoBoundaryReproposal == ~\E s \in Slots : send[1] # 0 /\ s < send[1] /\ ChosenInGen(s, 2)
=============================================================================
