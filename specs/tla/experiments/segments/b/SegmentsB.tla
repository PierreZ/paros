------------------------------ MODULE SegmentsB ------------------------------
(***************************************************************************)
(* Experiment #1213 (segments in the protocol or in storage), design B:    *)
(* plain Matchmaker Paxos. This is an experiment, not a model of the plan: *)
(* it changes no model of specs/tla/.                                      *)
(*                                                                         *)
(* The matchmakers know the configurations and nothing about segments.     *)
(* Each replica cuts its applied prefix into chunks on its own, uploads a  *)
(* chunk to the object store under the key (journal, first slot, last      *)
(* slot), and drops its copy after the upload. A new leader learns the     *)
(* chosen prefix from the replicas and runs Phase 1 above it on every      *)
(* configuration that still runs.                                          *)
(*                                                                         *)
(* The variant Claims = "log" records each offload as an entry of the      *)
(* journal's own log, so that the log gives a decided view of what is      *)
(* offloaded (the LogBackupEngine idea of Delos, SOSP 2021, as the design  *)
(* doc reads it). A replica asks the leader to append a claim for a chunk; *)
(* the first chosen claim that covers a slot wins; a replica uploads only  *)
(* a winning claim and drops its copy only once it applied that claim and  *)
(* the object is in the store.                                             *)
(*                                                                         *)
(* The rest is the same as design A (SegmentsA.tla): the matchmakers are   *)
(* one atomic register, configurations register in ballot order (plan     *)
(* rule 1), a leader that stops is a crash, a later ballot is a leader     *)
(* change, acceptors and replicas keep their state on disk.                *)
(*                                                                         *)
(* Constants select the rules. The first value of each is the rule that    *)
(* passes; each other value is a control or the variant.                   *)
(*   Claims    "log": offloads are claims in the log. "none": plain B.     *)
(*   RetireRule "paper": retire a configuration when each slot with a vote *)
(*             in it is applied by f+1 replicas or chosen in a later       *)
(*             configuration. "early": retire once a later configuration   *)
(*             is in force.                                                *)
(*   ReadScope "live": a tail read reads every configuration that still    *)
(*             runs. "fence": it reads its cached configuration only, on   *)
(*             the basis of a leadership of that configuration that won,   *)
(*             and takes the bound below from that leadership's fence (the *)
(*             read rule of paros, specs/tla/reads/). "view": it reads its *)
(*             cached configuration only, with no fence (control).         *)
(*   Cuts      "fixed": a chunk is one slot, so its key is a function of   *)
(*             the slot (deterministic keys). "free": a replica cuts its   *)
(*             chunks where it likes.                                      *)
(*   TrimCheck, ReadFence, Reader, Storage: as in design A.                *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Acceptors, Replicas, Values, None,
          CfgOf, NS, NB, NG, F,
          Claims, RetireRule, TrimCheck, ReadFence, Reader, ReadScope, Storage,
          Cuts

ASSUME Claims \in {"log", "none"}
ASSUME ReadScope \in {"live", "fence", "view"}
ASSUME Cuts \in {"fixed", "free"}
ASSUME Storage \in BOOLEAN
ASSUME RetireRule \in {"paper", "early"}
ASSUME TrimCheck \in BOOLEAN /\ ReadFence \in BOOLEAN /\ Reader \in BOOLEAN
ASSUME None \notin Values
ASSUME F + 1 <= Cardinality(Replicas)

Slots == 1..NS
Ballots == 1..NB
Gens == 1..NG
ASSUME \A g \in Gens : CfgOf[g] # {} /\ CfgOf[g] \subseteq Acceptors

\* An object key: the first and the last slot of a chunk.
Keys == {k \in Slots \X Slots : k[1] <= k[2]}
KeyRange(k) == k[1]..k[2]
\* A log entry is a client value or, in the variant, a claim (a key).
Entries == Values \cup (IF Claims = "log" THEN Keys ELSE {})

Maj(S) == {Q \in SUBSET S : 2 * Cardinality(Q) > Cardinality(S)}
Max(S) == CHOOSE x \in S : \A y \in S : y <= x

VARIABLES
  \* Matchmakers: the registrations.
  bgen,     \* ballot -> its configuration, 0 before it registers
  gst,      \* configuration -> "none", "live", "retired"
  \* Leaders.
  bst,      \* ballot -> "new", "w" (Phase 1 done, writes)
  bsafe,    \* ballot -> slot -> the entry that Phase 1 makes it propose, or None
  bbase,    \* ballot -> the first slot above the chosen prefix it learned
  prop,     \* ballot -> slot -> its proposal, or None
  \* Acceptors.
  prom, vote, sent2b,
  \* Replicas, the object store, trim.
  applied,  \* replica -> its applied prefix
  hold,     \* replica -> the slots it keeps on its disk
  cut,      \* replica -> the last slot of its last chunk
  want,     \* the claims that replicas asked the leader to append
  store,    \* key -> slot -> the bytes of its object, all None when no object
  floor, cursor,
  \* The tail reader.
  rview, rneed, rgot, rst

mm == <<bgen, gst>>
ldr == <<bst, bsafe, bbase, prop>>
acc == <<prom, vote, sent2b>>
dat == <<applied, hold, cut, want, store, floor, cursor>>
rdr == <<rview, rneed, rgot, rst>>
vars == <<mm, ldr, acc, dat, rdr>>

NoProp == [s \in Slots |-> None]

TypeOK ==
  /\ bgen \in [Ballots -> 0..NG]
  /\ gst \in [Gens -> {"none", "live", "retired"}]
  /\ bst \in [Ballots -> {"new", "w"}]
  /\ bsafe \in [Ballots -> [Slots -> Entries \cup {None}]]
  /\ prop \in [Ballots -> [Slots -> Entries \cup {None}]]
  /\ prom \in [Acceptors -> 0..NB]
  /\ applied \in [Replicas -> 0..NS]
  /\ hold \in [Replicas -> SUBSET Slots]
  /\ cut \in [Replicas -> 0..NS]
  /\ want \subseteq Keys
  /\ store \in [Keys -> [Slots -> Entries \cup {None}]]
  /\ floor \in 1..NS + 1
  /\ cursor \in 1..NS + 1

Init ==
  /\ bgen = [b \in Ballots |-> 0]
  /\ gst = [g \in Gens |-> IF g = 1 THEN "live" ELSE "none"]
  /\ bst = [b \in Ballots |-> "new"]
  /\ bsafe = [b \in Ballots |-> NoProp]
  /\ bbase = [b \in Ballots |-> 1]
  /\ prop = [b \in Ballots |-> NoProp]
  /\ prom = [a \in Acceptors |-> 0]
  /\ vote = [a \in Acceptors |-> [s \in Slots |-> <<0, None>>]]
  /\ sent2b = {}
  /\ applied = [r \in Replicas |-> 0]
  /\ hold = [r \in Replicas |-> {}]
  /\ cut = [r \in Replicas |-> 0]
  /\ want = {}
  /\ store = [k \in Keys |-> NoProp]
  /\ floor = 1
  /\ cursor = 1
  /\ rview = 1
  /\ rneed = 0
  /\ rgot = 0
  /\ rst = "idle"

(***************************************************************************)
(* Helpers                                                                 *)
(***************************************************************************)

Live == {g \in Gens : gst[g] = "live"}
Active == UNION {CfgOf[g] : g \in Live}
Top == Max({g \in Gens : gst[g] # "none"})
\* The configuration in force: the newest one whose leader finished Phase 1.
InForce == Max({1} \cup {bgen[b] : b \in {c \in Ballots : bst[c] = "w"}})

ChosenAtBal(s, v, b) ==
  bgen[b] # 0 /\ \E Q \in Maj(CfgOf[bgen[b]]) : \A a \in Q : <<s, b, v, a>> \in sent2b
Chosen(s, v) == \E b \in Ballots : ChosenAtBal(s, v, b)
IsChosen(s) == \E v \in Entries : Chosen(s, v)
ChosenVal(s) == CHOOSE v \in Entries : Chosen(s, v)
ChosenInGen(s, g) == \E b \in Ballots, v \in Entries : bgen[b] = g /\ ChosenAtBal(s, v, b)

MaxApplied == Max({applied[r] : r \in Replicas})
Replicated(s) == Cardinality({r \in Replicas : applied[r] >= s}) >= F + 1

Overlap(k, j) == k[1] <= j[2] /\ j[1] <= k[2]
\* The claim k is chosen at slot t.
ClaimAt(k, t) == Claims = "log" /\ Chosen(t, k)
\* The claim k wins: it is chosen, and no claim chosen at an earlier slot
\* overlaps it. Every node that applied the log up to t agrees on it.
Winner(k, t) ==
  /\ ClaimAt(k, t)
  /\ ~\E j \in Keys, u \in Slots : u < t /\ ClaimAt(j, u) /\ Overlap(j, k)

(***************************************************************************)
(* Matchmaking and Phase 1                                                 *)
(***************************************************************************)

\* Ballot b registers configuration g: the one in force or its successor.
\* A successor waits until the newest configuration has a leader.
Register(b, g) ==
  /\ bgen[b] = 0
  /\ \A c \in Ballots : bgen[c] # 0 => c < b
  /\ g \in {Top, Top + 1}
  /\ g <= NG
  /\ g = Top + 1 => Top = 1 \/ \E c \in Ballots : bgen[c] = Top /\ bst[c] = "w"
  /\ bgen' = [bgen EXCEPT ![b] = g]
  /\ gst' = IF g = Top + 1 THEN [gst EXCEPT ![g] = "live"] ELSE gst
  /\ UNCHANGED <<ldr, acc, dat, rdr>>

\* Phase 1 of ballot b: a promise quorum of every configuration that still
\* runs, up to its own, read above the chosen prefix that the replicas
\* report.
Phase1(b) ==
  LET g == bgen[b]
      H == {h \in Live : h <= g}
      base == MaxApplied + 1
  IN /\ g # 0
     /\ bst[b] = "new"
     /\ \E Qs \in [H -> SUBSET Acceptors] :
          /\ \A h \in H : Qs[h] \in Maj(CfgOf[h]) /\ \A a \in Qs[h] : prom[a] < b
          /\ LET seen == {<<s, vote[a][s]>> : s \in base..NS, a \in UNION {Qs[h] : h \in H}}
                 voted == {x \in seen : x[2][1] > 0}
                 top(s) == CHOOSE x \in voted : x[1] = s /\ \A y \in voted : y[1] = s => y[2][1] <= x[2][1]
             IN bsafe' = [bsafe EXCEPT ![b] =
                    [s \in Slots |-> IF \E x \in voted : x[1] = s THEN top(s)[2][2] ELSE None]]
          /\ prom' = [a \in Acceptors |-> IF \E h \in H : a \in Qs[h] THEN b ELSE prom[a]]
     /\ bbase' = [bbase EXCEPT ![b] = base]
     /\ bst' = [bst EXCEPT ![b] = "w"]
     /\ UNCHANGED <<mm, prop, vote, sent2b, dat, rdr>>

(***************************************************************************)
(* Phase 2                                                                 *)
(***************************************************************************)

CanWrite(b) ==
  /\ bst[b] = "w"
  /\ \E Q \in Maj(CfgOf[bgen[b]]) : Q \subseteq Active /\ \A a \in Q : prom[a] <= b

Writable(b) == {s \in Slots : s >= floor /\ s >= bbase[b]}

Propose(b, s, v) ==
  /\ CanWrite(b)
  /\ s \in Writable(b)
  /\ prop[b][s] = None
  \* A leader proposes in slot order (the allocator frontier).
  /\ \A t \in Writable(b) : t < s => prop[b][t] # None
  /\ v \in IF bsafe[b][s] # None THEN {bsafe[b][s]} ELSE Values \cup want
  /\ prop' = [prop EXCEPT ![b][s] = v]
  /\ UNCHANGED <<mm, bst, bsafe, bbase, acc, dat, rdr>>

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
  /\ UNCHANGED <<mm, ldr, acc, cut, want, store, floor, cursor, rdr>>

\* Matchmaker garbage collection of a configuration below the one in force.
Retire(h) ==
  /\ gst[h] = "live"
  /\ \E c \in Ballots : bgen[c] > h /\ bst[c] = "w"
  /\ RetireRule = "paper" =>
       \A a \in CfgOf[h], s \in Slots :
         vote[a][s][1] > 0 => Replicated(s) \/ \E g \in Gens : g > h /\ ChosenInGen(s, g)
  /\ gst' = [gst EXCEPT ![h] = "retired"]
  /\ UNCHANGED <<bgen, ldr, acc, dat, rdr>>

Data(k) == [s \in Slots |-> IF s \in KeyRange(k) THEN ChosenVal(s) ELSE None]

\* Plain B: replica r cuts its next chunk and uploads it.
CutUpload(r, hi) ==
  LET k == <<cut[r] + 1, hi>>
  IN /\ Claims = "none"
     /\ cut[r] < hi
     /\ Cuts = "fixed" => hi = cut[r] + 1
     /\ hi <= applied[r]
     /\ k[1] >= floor
     /\ store' = [store EXCEPT ![k] = Data(k)]
     /\ cut' = [cut EXCEPT ![r] = hi]
     /\ UNCHANGED <<mm, ldr, acc, applied, hold, want, floor, cursor, rdr>>

\* Variant: replica r asks the leader to append a claim for its next chunk.
Ask(r, hi) ==
  LET k == <<cut[r] + 1, hi>>
  IN /\ Claims = "log"
     /\ cut[r] < hi
     /\ Cuts = "fixed" => hi = cut[r] + 1
     /\ hi <= applied[r]
     /\ k[1] >= floor
     /\ want' = want \cup {k}
     /\ cut' = [cut EXCEPT ![r] = hi]
     /\ UNCHANGED <<mm, ldr, acc, applied, hold, store, floor, cursor, rdr>>

\* Variant: a replica that applied a winning claim uploads its chunk.
UploadClaim(r, k) ==
  /\ Claims = "log"
  /\ store[k] = NoProp
  /\ k[1] >= floor
  /\ \E t \in Slots : Winner(k, t) /\ applied[r] >= t
  /\ store' = [store EXCEPT ![k] = Data(k)]
  /\ UNCHANGED <<mm, ldr, acc, applied, hold, cut, want, floor, cursor, rdr>>

\* A replica drops the slots of an object: in plain B once the object is
\* there; in the variant once it also applied the winning claim.
Drop(r, k) ==
  /\ store[k] # NoProp
  /\ Claims = "log" => \E t \in Slots : Winner(k, t) /\ applied[r] >= t
  /\ hold[r] \cap KeyRange(k) # {}
  /\ hold' = [hold EXCEPT ![r] = @ \ KeyRange(k)]
  /\ UNCHANGED <<mm, ldr, acc, applied, cut, want, store, floor, cursor, rdr>>

\* Trim up to f: a decided Truncate. Objects that end below f are deleted
\* by their keys.
Trim(f) ==
  /\ f > floor
  /\ f <= NS + 1
  /\ \A s \in 1..(f - 1) : Replicated(s)
  /\ TrimCheck => f <= cursor
  /\ store' = [k \in Keys |-> IF k[2] < f THEN NoProp ELSE store[k]]
  /\ hold' = [r \in Replicas |-> {s \in hold[r] : s >= f}]
  /\ floor' = f
  /\ UNCHANGED <<mm, ldr, acc, applied, cut, want, cursor, rdr>>

Consume ==
  /\ cursor <= NS
  /\ cursor >= floor
  /\ IsChosen(cursor)
  /\ cursor' = cursor + 1
  /\ UNCHANGED <<mm, ldr, acc, applied, hold, cut, want, store, floor, rdr>>

(***************************************************************************)
(* The tail reader (check 5)                                               *)
(***************************************************************************)

ReadStart ==
  /\ Reader
  /\ rst = "idle"
  /\ rneed' = Max({s \in Slots : IsChosen(s)} \cup {0})
  /\ rst' = "wait"
  /\ UNCHANGED <<mm, ldr, acc, dat, rview, rgot>>

SaysSealed(a) == a \notin Active \/ (prom[a] # 0 /\ bgen[prom[a]] > rview)

\* A leaderless read of the tail. B has no segment start, so a slot chosen
\* in an older configuration that still runs is not below any start that
\* the reader knows. With ReadScope = "live" the reader reads a quorum of
\* every configuration that still runs, up to its cached one, as Phase 1
\* does, and asks f+1 replicas (here: all, F + 1 = 2) for their applied
\* prefix. With ReadScope = "fence" it reads its cached configuration only,
\* on the basis of a ballot b of that configuration that finished Phase 1
\* and that no member of the quorum promised past: the tail is at least
\* b's fence, the highest slot that b's Phase 1 learned of (a vote it
\* proposes again, or the chosen prefix it learned). b proposes each slot
\* below its fence, so the fence gives the bound below that the start of
\* the cached segment gives in design A. With ReadScope = "view" (the
\* control) it reads its cached configuration only, with no fence.
Fence(b) == Max({s \in Slots : bsafe[b][s] # None} \cup {bbase[b] - 1})
QuorumTop(Q, H) ==
  {s \in Slots : \E b \in Ballots, v \in Entries :
     bgen[b] \in H /\ \A a \in Q : <<s, b, v, a>> \in sent2b}
ReadAsk(Q) ==
  LET H == IF ReadScope = "live" THEN {h \in Live : h <= rview} \cup {rview} ELSE {rview}
  IN /\ Reader
     /\ rst = "wait"
     /\ Q \in Maj(CfgOf[rview])
     /\ IF ReadFence /\ \E a \in Q : SaysSealed(a)
          THEN /\ rview' = InForce
               /\ UNCHANGED <<rgot, rst>>
          ELSE IF ReadScope = "fence"
          THEN /\ \E b \in Ballots :
                    /\ bst[b] = "w"
                    /\ bgen[b] = rview
                    /\ \A a \in Q : prom[a] <= b
                    /\ rgot' = Max(QuorumTop(Q, {rview}) \cup {Fence(b)})
               /\ rst' = "done"
               /\ UNCHANGED rview
          ELSE /\ \E Qs \in [H -> SUBSET Acceptors] :
                    /\ \A h \in H : Qs[h] \in Maj(CfgOf[h])
                    /\ Qs[rview] = Q
                    /\ rgot' = Max({s \in Slots : \E h \in H, b \in Ballots, v \in Entries :
                                       bgen[b] = h /\ \A a \in Qs[h] : <<s, b, v, a>> \in sent2b}
                                    \cup {MaxApplied})
               /\ rst' = "done"
               /\ UNCHANGED rview
     /\ UNCHANGED <<mm, ldr, acc, dat, rneed>>

ReadAgain ==
  /\ rst = "done"
  /\ rst' = "idle"
  /\ UNCHANGED <<mm, ldr, acc, dat, rview, rneed, rgot>>

Next ==
  \/ \E b \in Ballots, g \in Gens : Register(b, g)
  \/ \E b \in Ballots : Phase1(b)
  \/ \E b \in Ballots, s \in Slots, v \in Entries : Propose(b, s, v)
  \/ \E a \in Acceptors, b \in Ballots, s \in Slots : Accept(a, b, s)
  \/ \E r \in Replicas : Apply(r)
  \/ \E h \in Gens : Retire(h)
  \/ Storage /\ \E r \in Replicas, hi \in Slots : CutUpload(r, hi) \/ Ask(r, hi)
  \/ Storage /\ \E r \in Replicas, k \in Keys : UploadClaim(r, k) \/ Drop(r, k)
  \/ Storage /\ \E f \in Slots \cup {NS + 1} : Trim(f)
  \/ Storage /\ Consume
  \/ ReadStart \/ ReadAgain
  \/ \E Q \in UNION {Maj(CfgOf[g]) : g \in Gens} : ReadAsk(Q)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Checks: the same as design A where they apply                           *)
(***************************************************************************)

\* Check 1.
Agreement == \A s \in Slots, v, w \in Entries : Chosen(s, v) /\ Chosen(s, w) => v = w

OnLiveRow(s) == \E b \in Ballots, v \in Entries : bgen[b] \in Live /\ ChosenAtBal(s, v, b)

\* Check 2.
Readable ==
  \A s \in Slots : s >= floor /\ IsChosen(s) =>
    \/ OnLiveRow(s)
    \/ Cardinality({r \in Replicas : s \in hold[r]}) >= F + 1
    \/ \E k \in Keys : store[k][s] = ChosenVal(s)

\* Check 3: a key holds one object by construction (`store` is a function
\* of the key); the action half is StoreImmutable.
StoreImmutable == [][\A k \in Keys : store[k] # NoProp => store'[k] \in {store[k], NoProp}]_vars

\* Not a check of the issue, but the same state invariant as design A's:
\* at most one object holds a slot, so a reader has one place to go.
OneHome == \A s \in Slots, k, j \in Keys : store[k][s] # None /\ store[j][s] # None => k = j

\* Check 5.
ReadFresh == rst = "done" => rgot >= rneed

\* Check 6: a chosen slot whose only copy is in the object store is
\* declared in decided state. Plain B has no decided record of an offload:
\* the matchmakers know no chunk, and a listing of the object store is not
\* decided. In the variant, the winning claim in the log declares it.
Declared(s) ==
  \E k \in Keys, t \in Slots :
    Winner(k, t) /\ s \in KeyRange(k) /\ store[k][s] = ChosenVal(s)
Visible ==
  \A s \in Slots :
    (s >= floor /\ IsChosen(s) /\ ~OnLiveRow(s) /\ \A r \in Replicas : s \notin hold[r])
      => Declared(s)

\* Check 7.
OffloadStep == \/ \E h \in Gens : Retire(h)
               \/ \E r \in Replicas, hi \in Slots : CutUpload(r, hi) \/ Ask(r, hi)
               \/ \E r \in Replicas, k \in Keys : UploadClaim(r, k) \/ Drop(r, k)
               \/ \E f \in Slots \cup {NS + 1} : Trim(f)
TailNeverBlocked == [][OffloadStep => \A b \in Ballots : CanWrite(b) => CanWrite(b)']_vars

CursorSafe == cursor >= floor

\* No stranded hole, the same invariant as design A's: each leader of the
\* configuration in force that finished Phase 1 may propose at each slot at
\* or above the floor that is not chosen. It holds here by construction: a
\* leader writes every slot above the chosen prefix that it learned, and
\* a slot that is not chosen is above every applied prefix.
NoStrandedHole ==
  \A b \in Ballots, s \in Slots :
    bst[b] = "w" /\ bgen[b] = InForce /\ s >= floor /\ ~IsChosen(s) => s \in Writable(b)

\* Reachability controls: each must be violated.
NoDropAfterOffload == ~\E s \in Slots : IsChosen(s) /\ s >= floor /\ ~OnLiveRow(s)
                         /\ \A r \in Replicas : s \notin hold[r]
NoTrim == floor = 1
NoReshapeWrite == ~\E s \in Slots : ChosenInGen(s, 2)
NoClaim == ~\E k \in Keys, t \in Slots : Winner(k, t)
\* A read of the cached configuration 2 ends while the tail it must see is
\* a slot that configuration 1 chose and that no replica applied: the case
\* that breaks ReadScope = "view".
NoOldTailRead ==
  ~(rst = "done" /\ rview = 2 /\ \E s \in Slots : s = rneed /\ ChosenInGen(s, 1) /\ s > MaxApplied)
=============================================================================
