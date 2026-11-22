------------------------------ MODULE HealReuse ------------------------------
(***************************************************************************)
(* The heal of a retired segment's replica set when a later plan asks a    *)
(* node again, when the network delivers a request twice and late, and     *)
(* when a node acts on its own view of the register. #1314                 *)
(* (re-replication of a sealed segment's replica set).                     *)
(*                                                                         *)
(* `re-replication/` checks the heal with an atomic commit, and it never   *)
(* asks a node that runs a copy for a second copy. The code does both: the *)
(* controller's `decide` picks the first live candidate, so a deposed      *)
(* controller and the new one can ask the same node, and a removed member  *)
(* can be asked again; and a compare-and-set, a copy ask or an abandon can *)
(* arrive twice and late. This model has those steps. It checks five rules *)
(* of the code (section J3.1 of docs/journal/03-segments-and-records.md),  *)
(* each with a control:                                                    *)
(*                                                                         *)
(*   Order     "highest": a node takes a copy ask only above the copy      *)
(*             (term, number) it runs, and only if it is not a member.     *)
(*             "any": it takes any other copy (control `copy_any`).        *)
(*   TermFence TRUE: the register applies the compare-and-set only if its  *)
(*             term is the controller's term. FALSE: the record alone      *)
(*             (control `version_only`; it passes in `re-replication/`).   *)
(*   HealCount TRUE: the record counts its heals (`SegmentRecord::heals`), *)
(*             and the compare-and-set compares the count too. FALSE: it   *)
(*             compares the set only (control `no_heals`).                 *)
(*   SentEnds  TRUE: once its compare-and-set is sent, a heal ends only on *)
(*             an answer, or on a record that is neither its read nor its  *)
(*             result. FALSE: also when its new replica is down (control   *)
(*             `sent_abort`).                                              *)
(*   FreshRead TRUE: a node acts on a copy ask or an abandon (each can     *)
(*             wipe its copy) only on a register value that it saw done    *)
(*             after the controller sent that message. FALSE: on the last  *)
(*             value it saw (control `stale_view`).                        *)
(*                                                                         *)
(* The segment has K slots, all chosen; a node's copy is the slots         *)
(* [0, hi). A copy starts empty: an ask that a node takes wipes the copy   *)
(* that it runs. Each node keeps its disk across a crash. Each node knows  *)
(* the set from the last register value it read (`seen`); a value that     *)
(* names it ends its copy (it is a member, `SegmentReplica::observe`).     *)
(* Requests and answers stay in the network sets forever, so each can be   *)
(* delivered again, at any later time, or never. A controller that crashes *)
(* forgets its term and its heal (`role.rs`); it must win a new term. Any  *)
(* node can crash, and a heal that sent nothing can end at any time. The   *)
(* source of a copy is not modelled (`re-replication/` checks it).         *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS Pool, Init0, Ctls, F, K, MaxCrash, MaxRestart, MaxTerm, MaxPlans,
          None, Order, TermFence, HealCount, SentEnds, FreshRead

ASSUME Init0 \subseteq Pool /\ Cardinality(Init0) = 2 * F + 1
ASSUME K \in Nat \ {0}
ASSUME Order \in {"highest", "any"}
ASSUME TermFence \in BOOLEAN /\ HealCount \in BOOLEAN /\ SentEnds \in BOOLEAN
ASSUME FreshRead \in BOOLEAN

VARIABLES
  hi,       \* the slots [0, hi) that each node holds
  cid,      \* the copy that each node runs: <<term, number>>, or NoCopy
  seen,     \* each node's last register value: [set, at]
  down,     \* the nodes that are down
  rec,      \* the segment record: [set, heals]
  w,        \* the writes of the record so far (the index of its value)
  rterm,    \* the controller term of the register
  term,     \* the term of each controller (0: none)
  heal,     \* the heal of each controller, or None
  plans,    \* the plans so far (the number of the next copy)
  asks,     \* the copy asks sent: [n, id, at]
  reports,  \* the "copied" reports sent: [n, id]
  cas,      \* the compare-and-sets sent: [rd, term, d, n, id]
  ans,      \* the answers of the register: [m, ok]
  abandons, \* the abandon asks sent: [n, id, at]
  crashes, restarts

vars == <<hi, cid, seen, down, rec, w, rterm, term, heal, plans, asks, reports, cas,
          ans, abandons, crashes, restarts>>

Live == Pool \ down
\* No copy: below every copy id, whose term is at least 1.
NoCopy == <<0, 0>>
Above(a, b) == a[1] > b[1] \/ (a[1] = b[1] /\ a[2] > b[2])

TypeOK ==
  /\ hi \in [Pool -> 0..K]
  /\ down \subseteq Pool
  /\ rec.set \subseteq Pool /\ rec.heals \in Nat
  /\ \A r \in Pool : seen[r].at \in 0..w
  /\ rterm \in 0..MaxTerm /\ term \in [Ctls -> 0..MaxTerm]

Init ==
  /\ hi = [r \in Pool |-> IF r \in Init0 THEN K ELSE 0]
  /\ cid = [r \in Pool |-> NoCopy]
  /\ seen = [r \in Pool |-> [set |-> Init0, at |-> 0]]
  /\ down = {}
  /\ rec = [set |-> Init0, heals |-> 0]
  /\ w = 0
  /\ rterm = 0
  /\ term = [c \in Ctls |-> 0]
  /\ heal = [c \in Ctls |-> None]
  /\ plans = 0
  /\ asks = {} /\ reports = {} /\ cas = {} /\ ans = {} /\ abandons = {}
  /\ crashes = 0 /\ restarts = 0

Net == <<asks, reports, cas, ans, abandons>>
Ctl == <<rterm, term, heal, plans>>

(* Nodes ------------------------------------------------------------------ *)

\* Any node can stop, a member too: `Held` is about disks, which a crash
\* keeps, so the model needs no bound on the members that are down.
Crash(r) ==
  /\ r \in Live /\ crashes < MaxCrash
  /\ down' = down \cup {r}
  /\ crashes' = crashes + 1
  /\ UNCHANGED <<hi, cid, seen, rec, w, Ctl, Net, restarts>>

Restart(r) ==
  /\ r \in down /\ restarts < MaxRestart
  /\ down' = down \ {r}
  /\ restarts' = restarts + 1
  /\ UNCHANGED <<hi, cid, seen, rec, w, Ctl, Net, crashes>>

\* A node reads the register: a value done now. A value that names it ends
\* the copy it runs: it is a member.
Read(r) ==
  /\ r \in Live /\ seen[r].at < w
  /\ seen' = [seen EXCEPT ![r] = [set |-> rec.set, at |-> w]]
  /\ cid' = [cid EXCEPT ![r] = IF r \in rec.set THEN NoCopy ELSE @]
  /\ UNCHANGED <<hi, down, rec, w, Ctl, Net, crashes, restarts>>

\* The node's view is fresh enough for message a.
Fresh(r, a) == FreshRead => seen[r].at >= a.at

\* A node takes a copy ask: it starts that copy, empty.
Join(r) ==
  \E a \in asks :
    /\ a.n = r /\ r \in Live /\ Fresh(r, a)
    /\ r \notin seen[r].set
    /\ IF Order = "highest" THEN Above(a.id, cid[r]) ELSE a.id # cid[r]
    /\ cid' = [cid EXCEPT ![r] = a.id]
    /\ hi' = [hi EXCEPT ![r] = 0]
    /\ UNCHANGED <<seen, down, rec, w, Ctl, Net, crashes, restarts>>

\* A copy pulls the next slot. Its source is a full member of the set
\* (`re-replication/`, controls `copy_any` and `live_outside`); here the bytes
\* of a slot are not modelled, so the source is not either.
CopyStep(r) ==
  /\ r \in Live /\ cid[r] # NoCopy /\ hi[r] < K
  /\ hi' = [hi EXCEPT ![r] = @ + 1]
  /\ UNCHANGED <<cid, seen, down, rec, w, Ctl, Net, crashes, restarts>>

\* A complete copy reports to its controller.
Report(r) ==
  /\ r \in Live /\ cid[r] # NoCopy /\ hi[r] = K
  /\ reports' = reports \cup {[n |-> r, id |-> cid[r]]}
  /\ UNCHANGED <<hi, cid, seen, down, rec, w, Ctl, asks, cas, ans, abandons, crashes,
                 restarts>>

\* An abandon stops the copy that it names, and only that copy.
Abandon(r) ==
  \E a \in abandons :
    /\ a.n = r /\ r \in Live /\ Fresh(r, a)
    /\ cid[r] = a.id
    /\ cid' = [cid EXCEPT ![r] = NoCopy]
    /\ UNCHANGED <<hi, seen, down, rec, w, Ctl, Net, crashes, restarts>>

\* A node that its view leaves out of the set and that runs no copy drops
\* its copy.
Drop(r) ==
  /\ r \in Live /\ r \notin seen[r].set /\ cid[r] = NoCopy /\ hi[r] > 0
  /\ hi' = [hi EXCEPT ![r] = 0]
  /\ UNCHANGED <<cid, seen, down, rec, w, Ctl, Net, crashes, restarts>>

(* The register ------------------------------------------------------------ *)

Eq(a, b) == a.set = b.set /\ (HealCount => a.heals = b.heals)
After(m) == [set |-> (m.rd.set \ {m.d}) \cup {m.n}, heals |-> m.rd.heals + 1]

\* The register applies a compare-and-set, as `Change::Rereplicate`: the same
\* change again answers "ok"; then the term; then the record.
Apply ==
  /\ \E m \in cas :
       IF Eq(rec, After(m))
       THEN /\ ans' = ans \cup {[m |-> m, ok |-> TRUE]}
            /\ UNCHANGED <<rec, w>>
       ELSE IF (TermFence => m.term = rterm) /\ Eq(rec, m.rd)
       THEN /\ rec' = [set |-> After(m).set, heals |-> rec.heals + 1]
            /\ w' = w + 1
            /\ ans' = ans \cup {[m |-> m, ok |-> TRUE]}
       ELSE /\ ans' = ans \cup {[m |-> m, ok |-> FALSE]}
            /\ UNCHANGED <<rec, w>>
  /\ UNCHANGED <<hi, cid, seen, down, Ctl, asks, reports, cas, abandons, crashes,
                 restarts>>

(* Controllers ------------------------------------------------------------- *)

Elect(c) ==
  /\ rterm < MaxTerm /\ heal[c] = None /\ ~(term[c] > 0 /\ term[c] = rterm)
  /\ rterm' = rterm + 1
  /\ term' = [term EXCEPT ![c] = rterm + 1]
  /\ UNCHANGED <<hi, cid, seen, down, rec, w, heal, plans, Net, crashes, restarts>>

\* A controller crashes: it forgets its term and its heal. What it sent stays
\* in the network.
CtlCrash(c) ==
  /\ term' = [term EXCEPT ![c] = 0]
  /\ heal' = [heal EXCEPT ![c] = None]
  /\ UNCHANGED <<hi, cid, seen, down, rec, w, rterm, plans, Net, crashes, restarts>>

\* The holder plans a heal of a down member d by a live node n that is not a
\* member. n may run the copy of another controller's plan, or be a removed
\* member that came back.
Plan(c) ==
  /\ heal[c] = None /\ term[c] > 0 /\ term[c] = rterm
  /\ plans < MaxPlans
  /\ \E d \in rec.set \cap down, n \in Live \ rec.set :
       LET id == <<term[c], plans + 1>> IN
       /\ heal' = [heal EXCEPT ![c] = [rd |-> rec, d |-> d, n |-> n, id |-> id,
                                       sent |-> FALSE]]
       /\ asks' = asks \cup {[n |-> n, id |-> id, at |-> w]}
  /\ plans' = plans + 1
  /\ UNCHANGED <<hi, cid, seen, down, rec, w, rterm, term, reports, cas, ans, abandons,
                 crashes, restarts>>

Msg(c) == [rd |-> heal[c].rd, term |-> term[c], d |-> heal[c].d, n |-> heal[c].n,
           id |-> heal[c].id]

\* The report of the heal's own copy counts it; then the controller sends the
\* compare-and-set.
Send(c) ==
  /\ heal[c] # None /\ ~heal[c].sent
  /\ [n |-> heal[c].n, id |-> heal[c].id] \in reports
  /\ heal' = [heal EXCEPT ![c].sent = TRUE]
  /\ cas' = cas \cup {Msg(c)}
  /\ UNCHANGED <<hi, cid, seen, down, rec, w, rterm, term, plans, asks, reports, ans,
                 abandons, crashes, restarts>>

Ending(c) == abandons \cup {[n |-> heal[c].n, id |-> heal[c].id, at |-> w]}

\* An answer ends a sent heal; a refusal abandons its copy.
Answer(c) ==
  /\ heal[c] # None /\ heal[c].sent
  /\ \E a \in ans :
       /\ a.m = Msg(c)
       /\ abandons' = IF a.ok THEN abandons ELSE Ending(c)
  /\ heal' = [heal EXCEPT ![c] = None]
  /\ UNCHANGED <<hi, cid, seen, down, rec, w, rterm, term, plans, asks, reports, cas, ans,
                 crashes, restarts>>

\* A heal ends with no write and abandons its copy. A heal that sent
\* nothing ends at any time (the code ends it when its new replica is down,
\* its term is gone or its read is old). A sent heal ends on a record that
\* is neither its read nor its result (`role.rs`, "a heal on an old read
\* ends"); under ~SentEnds, also when its new replica is down.
Abort(c) ==
  /\ heal[c] # None
  /\ \/ ~heal[c].sent
     \/ ~Eq(rec, heal[c].rd) /\ ~Eq(rec, After(Msg(c)))
     \/ ~SentEnds /\ heal[c].n \in down
  /\ abandons' = Ending(c)
  /\ heal' = [heal EXCEPT ![c] = None]
  /\ UNCHANGED <<hi, cid, seen, down, rec, w, rterm, term, plans, asks, reports, cas, ans,
                 crashes, restarts>>

Next ==
  \/ \E r \in Pool : Crash(r) \/ Restart(r) \/ Read(r) \/ Join(r) \/ CopyStep(r)
                     \/ Report(r) \/ Abandon(r) \/ Drop(r)
  \/ Apply
  \/ \E c \in Ctls : Elect(c) \/ CtlCrash(c) \/ Plan(c) \/ Send(c) \/ Answer(c) \/ Abort(c)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Safety: every member of the set holds the whole segment (the walk's     *)
(* oracle `HELD`). A member that is down keeps its disk.                   *)
(***************************************************************************)
Held == \A r \in rec.set : hi[r] = K

SetSize == Cardinality(rec.set) = 2 * F + 1

(***************************************************************************)
(* Reach: a check that must be violated, so that a passing check is not    *)
(* empty.                                                                  *)
(***************************************************************************)
\* A heal commits.
NoHeal == rec.heals = 0
\* One node is asked for two copies of two terms.
NoReuse == ~\E a, b \in asks : a.n = b.n /\ a.id[1] # b.id[1]
\* The set comes back to an earlier value.
NoComeBack == ~(rec.heals > 0 /\ rec.set = Init0)
\* A compare-and-set is applied again: it is answered twice.
NoTwoAnswers == ~\E a, b \in ans : a.m = b.m /\ a # b
\* A late copy ask of a lower copy reaches a node that runs a higher one.
NoLowerAsk == ~\E r \in Pool, a \in asks :
  a.n = r /\ cid[r] # NoCopy /\ Above(cid[r], a.id) /\ r \in Live /\ r \notin rec.set
\* A member that a commit named has not read it yet, and an abandon of its
\* copy is sent.
NoLateAbandon == ~\E r \in rec.set, a \in abandons :
  a.n = r /\ r \notin seen[r].set /\ a.id = cid[r]
=============================================================================
