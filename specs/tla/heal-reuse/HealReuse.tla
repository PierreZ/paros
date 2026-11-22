------------------------------ MODULE HealReuse ------------------------------
(***************************************************************************)
(* The heal of a retired segment's replica set when a later plan asks a    *)
(* node again, and when the network delivers a request twice and late.    *)
(* #1314 (re-replication of a sealed segment's replica set).               *)
(*                                                                         *)
(* `re-replication/` checks the heal with an atomic commit, and it never   *)
(* asks a node that runs a copy for a second copy. The code does both: the *)
(* controller's `decide` picks the first live candidate, so a deposed      *)
(* controller and the new one can ask the same node, and a removed member  *)
(* can be asked again; and a compare-and-set or a copy ask can arrive      *)
(* twice and late. This model has those steps. It checks four rules of the *)
(* code (section J3.1 of docs/journal/03-segments-and-records.md), each    *)
(* with a control:                                                         *)
(*                                                                         *)
(*   Order     "highest": a node runs the highest copy (term, number) it   *)
(*             was asked; it refuses a lower one, and a node that is a     *)
(*             member refuses every copy. "any": the last ask wins (control *)
(*             `copy_any`).                                                *)
(*   TermFence TRUE: the register applies the compare-and-set only if its  *)
(*             term is the controller's term. FALSE: the record alone      *)
(*             (control `version_only`; it passes in `re-replication/`).   *)
(*   HealCount TRUE: the record counts its heals (`SegmentRecord::heals`), *)
(*             and the compare-and-set compares the count too. FALSE: it   *)
(*             compares the state and the set only (control `no_heals`).   *)
(*   SentEnds  TRUE: once its compare-and-set is sent, a heal ends only on *)
(*             an answer. FALSE: a heal whose new replica is down ends,    *)
(*             sent or not (control `sent_abort`).                         *)
(*                                                                         *)
(* The segment has K slots, all chosen; a node's copy is the slots         *)
(* [0, hi). A copy starts empty: an ask that a node takes wipes the copy   *)
(* that it runs. Each node keeps its disk across a crash. Requests and     *)
(* answers stay in the network sets forever, so each can be delivered      *)
(* again, at any later time. A controller that crashes forgets its term   *)
(* and its heal (`role.rs`); it must win a new term. Any node can crash,   *)
(* and a heal that sent nothing can end at any time.                      *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS Pool, Init0, Ctls, F, K, MaxCrash, MaxRestart, MaxTerm, MaxPlans,
          None, Order, TermFence, HealCount, SentEnds

ASSUME Init0 \subseteq Pool /\ Cardinality(Init0) = 2 * F + 1
ASSUME K \in Nat \ {0}
ASSUME Order \in {"highest", "any"}
ASSUME TermFence \in BOOLEAN /\ HealCount \in BOOLEAN /\ SentEnds \in BOOLEAN

VARIABLES
  hi,       \* the slots [0, hi) that each node holds
  cid,      \* the copy that each node runs: <<term, number>>, or NoCopy
  down,     \* the nodes that are down
  rec,      \* the segment record: [set, heals]
  rterm,    \* the controller term of the register
  term,     \* the term of each controller (0: none)
  heal,     \* the heal of each controller, or None
  plans,    \* the plans so far (the number of the next copy)
  asks,     \* the copy asks sent: [n, id]
  reports,  \* the "copied" reports sent: [n, id]
  cas,      \* the compare-and-sets sent: [rd, term, d, n, id]
  ans,      \* the answers of the register: [m, ok]
  abandons, \* the abandon asks sent: [n, id]
  crashes, restarts

vars == <<hi, cid, down, rec, rterm, term, heal, plans, asks, reports, cas, ans,
          abandons, crashes, restarts>>

Live == Pool \ down
\* No copy: below every copy id, whose term is at least 1.
NoCopy == <<0, 0>>
Above(a, b) == a[1] > b[1] \/ (a[1] = b[1] /\ a[2] > b[2])

TypeOK ==
  /\ hi \in [Pool -> 0..K]
  /\ down \subseteq Pool
  /\ rec.set \subseteq Pool /\ rec.heals \in Nat
  /\ rterm \in 0..MaxTerm /\ term \in [Ctls -> 0..MaxTerm]

Init ==
  /\ hi = [r \in Pool |-> IF r \in Init0 THEN K ELSE 0]
  /\ cid = [r \in Pool |-> NoCopy]
  /\ down = {}
  /\ rec = [set |-> Init0, heals |-> 0]
  /\ rterm = 0
  /\ term = [c \in Ctls |-> 0]
  /\ heal = [c \in Ctls |-> None]
  /\ plans = 0
  /\ asks = {} /\ reports = {} /\ cas = {} /\ ans = {} /\ abandons = {}
  /\ crashes = 0 /\ restarts = 0

Net == <<asks, reports, cas, ans, abandons>>

(* Nodes ------------------------------------------------------------------ *)

\* Any node can stop, a member too: `Held` is about disks, which a crash
\* keeps, so the model needs no bound on the members that are down.
Crash(r) ==
  /\ r \in Live /\ crashes < MaxCrash
  /\ down' = down \cup {r}
  /\ crashes' = crashes + 1
  /\ UNCHANGED <<hi, cid, rec, rterm, term, heal, plans, Net, restarts>>

Restart(r) ==
  /\ r \in down /\ restarts < MaxRestart
  /\ down' = down \ {r}
  /\ restarts' = restarts + 1
  /\ UNCHANGED <<hi, cid, rec, rterm, term, heal, plans, Net, crashes>>

\* A node takes a copy ask: it starts that copy, empty.
Join(r) ==
  \E a \in asks :
    /\ a.n = r /\ r \in Live
    /\ r \notin rec.set
    /\ Order = "highest" => Above(a.id, cid[r])
    /\ cid' = [cid EXCEPT ![r] = a.id]
    /\ hi' = [hi EXCEPT ![r] = 0]
    /\ UNCHANGED <<down, rec, rterm, term, heal, plans, Net, crashes, restarts>>

\* A copy pulls the next slot. Its source is a full member of the set
\* (`re-replication/`, controls `copy_any` and `live_outside`); here the bytes
\* of a slot are not modelled, so the source is not either.
CopyStep(r) ==
  /\ r \in Live /\ cid[r] # NoCopy /\ hi[r] < K
  /\ hi' = [hi EXCEPT ![r] = @ + 1]
  /\ UNCHANGED <<cid, down, rec, rterm, term, heal, plans, Net, crashes, restarts>>

\* A complete copy reports to its controller.
Report(r) ==
  /\ r \in Live /\ cid[r] # NoCopy /\ hi[r] = K
  /\ reports' = reports \cup {[n |-> r, id |-> cid[r]]}
  /\ UNCHANGED <<hi, cid, down, rec, rterm, term, heal, plans, asks, cas, ans, abandons,
                 crashes, restarts>>

\* An abandon stops the copy that it names, and only that copy.
Abandon(r) ==
  \E a \in abandons :
    /\ a.n = r /\ r \in Live /\ cid[r] = a.id
    /\ cid' = [cid EXCEPT ![r] = NoCopy]
    /\ UNCHANGED <<hi, down, rec, rterm, term, heal, plans, Net, crashes, restarts>>

\* A node that is not a member and runs no copy drops its copy.
Drop(r) ==
  /\ r \in Live /\ r \notin rec.set /\ cid[r] = NoCopy /\ hi[r] > 0
  /\ hi' = [hi EXCEPT ![r] = 0]
  /\ UNCHANGED <<cid, down, rec, rterm, term, heal, plans, Net, crashes, restarts>>

(* The register ------------------------------------------------------------ *)

Eq(a, b) == a.set = b.set /\ (HealCount => a.heals = b.heals)
After(m) == [set |-> (m.rd.set \ {m.d}) \cup {m.n}, heals |-> m.rd.heals + 1]

\* The register applies a compare-and-set, as `Change::Rereplicate`: the same
\* change again answers "ok"; then the term; then the record.
Apply ==
  /\ \E m \in cas :
       IF Eq(rec, After(m))
       THEN /\ ans' = ans \cup {[m |-> m, ok |-> TRUE]}
            /\ UNCHANGED rec
       ELSE IF (TermFence => m.term = rterm) /\ Eq(rec, m.rd)
       THEN /\ rec' = [set |-> After(m).set, heals |-> rec.heals + 1]
            /\ ans' = ans \cup {[m |-> m, ok |-> TRUE]}
       ELSE /\ ans' = ans \cup {[m |-> m, ok |-> FALSE]}
            /\ UNCHANGED rec
  /\ UNCHANGED <<hi, cid, down, rterm, term, heal, plans, asks, reports, cas, abandons,
                 crashes, restarts>>

(* Controllers ------------------------------------------------------------- *)

Elect(c) ==
  /\ rterm < MaxTerm /\ heal[c] = None /\ ~(term[c] > 0 /\ term[c] = rterm)
  /\ rterm' = rterm + 1
  /\ term' = [term EXCEPT ![c] = rterm + 1]
  /\ UNCHANGED <<hi, cid, down, rec, heal, plans, Net, crashes, restarts>>

\* A controller crashes: it forgets its term and its heal. What it sent stays
\* in the network.
CtlCrash(c) ==
  /\ term' = [term EXCEPT ![c] = 0]
  /\ heal' = [heal EXCEPT ![c] = None]
  /\ UNCHANGED <<hi, cid, down, rec, rterm, plans, Net, crashes, restarts>>

\* The holder plans a heal of a down member d by a live node n that is not a
\* member. n may run the copy of another controller's plan, or be a removed
\* member that came back.
Plan(c) ==
  /\ heal[c] = None /\ term[c] > 0 /\ term[c] = rterm
  /\ plans < MaxPlans
  /\ \E d \in rec.set \cap down, n \in Live \ rec.set :
       LET id == <<term[c], plans + 1>> IN
       /\ heal' = [heal EXCEPT ![c] = [rd |-> rec, d |-> d, n |-> n, id |-> id,
                                       copied |-> FALSE, sent |-> FALSE]]
       /\ asks' = asks \cup {[n |-> n, id |-> id]}
  /\ plans' = plans + 1
  /\ UNCHANGED <<hi, cid, down, rec, rterm, term, reports, cas, ans, abandons,
                 crashes, restarts>>

Msg(c) == [rd |-> heal[c].rd, term |-> term[c], d |-> heal[c].d, n |-> heal[c].n,
           id |-> heal[c].id]

\* The report of the heal's own copy counts it; then the controller sends the
\* compare-and-set.
Send(c) ==
  /\ heal[c] # None /\ ~heal[c].sent
  /\ [n |-> heal[c].n, id |-> heal[c].id] \in reports
  /\ heal' = [heal EXCEPT ![c].copied = TRUE, ![c].sent = TRUE]
  /\ cas' = cas \cup {Msg(c)}
  /\ UNCHANGED <<hi, cid, down, rec, rterm, term, plans, asks, reports, ans, abandons,
                 crashes, restarts>>

\* An answer ends a sent heal; a refusal abandons its copy.
Answer(c) ==
  /\ heal[c] # None /\ heal[c].sent
  /\ \E a \in ans :
       /\ a.m = Msg(c)
       /\ abandons' = IF a.ok THEN abandons
                      ELSE abandons \cup {[n |-> heal[c].n, id |-> heal[c].id]}
  /\ heal' = [heal EXCEPT ![c] = None]
  /\ UNCHANGED <<hi, cid, down, rec, rterm, term, plans, asks, reports, cas, ans,
                 crashes, restarts>>

\* A heal ends with no write and abandons its copy. The code ends it when its
\* new replica is down, its term is gone or its read is old; the model lets it
\* end at any time, so it checks more. Under SentEnds, only a heal that sent
\* nothing; else also a sent one, as when its new replica is down.
Abort(c) ==
  /\ heal[c] # None
  /\ SentEnds => ~heal[c].sent
  /\ abandons' = abandons \cup {[n |-> heal[c].n, id |-> heal[c].id]}
  /\ heal' = [heal EXCEPT ![c] = None]
  /\ UNCHANGED <<hi, cid, down, rec, rterm, term, plans, asks, reports, cas, ans,
                 crashes, restarts>>

Next ==
  \/ \E r \in Pool : Crash(r) \/ Restart(r) \/ Join(r) \/ CopyStep(r) \/ Report(r)
                     \/ Abandon(r) \/ Drop(r)
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
=============================================================================
