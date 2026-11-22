----------------------------- MODULE JournalIds -----------------------------
(***************************************************************************)
(* One id names at most one journal, forever (#1419 (the tenancy wrapper   *)
(* and paros-names), open point 5 of #1409 (build the layered parosd)).    *)
(*                                                                         *)
(* An id is an address, never an authority. A create call carries a name   *)
(* and a request id; the coordinator of the level draws the id, and the    *)
(* fold of the level checks it at apply and refuses an id in use (§3.8 of  *)
(* docs/architecture). The fold of the universe checks a TenantId; the     *)
(* fold of a tenant checks a JournalId, unique inside its tenant. A host   *)
(* keys the records of a journal by the pair (TenantId, JournalId).        *)
(*                                                                         *)
(* The model has:                                                          *)
(*   - cells, each with a cell tenant whose id is minted at `cell init`,   *)
(*     before any universe (the genesis record); `JoinFleet` records it in *)
(*     the fold of the universe, which refuses an id in use;              *)
(*   - user tenants, created through the universe once a cell joined;     *)
(*   - journals of the tenants of one cell, `Home`, all booked on one host *)
(*     of that cell;                                                       *)
(*   - draws that can collide (a small id space), a refusal at apply and a *)
(*     new draw, a restart of the coordinators that loses the draws not    *)
(*     yet recorded, a delete, and a host that drops the records of a      *)
(*     deleted journal later.                                              *)
(*                                                                         *)
(* A create resumes by its request id: the fold keeps the id it gave       *)
(* (jrec, utid), so a coordinator that restarts after the record reads the *)
(* id back and draws nothing. A draw lost before the record is drawn       *)
(* again. A draw never repeats an id that the fold refused for the same    *)
(* create (`jtried`, `utried`): it stands for a 64-bit random draw, which  *)
(* finds a free id in a few draws. Safety does not use it.                *)
(*                                                                         *)
(* Constants select the rule and its controls:                            *)
(*   Order     "fold_first": the host books slots only under an id that   *)
(*             the fold gave; "book_first": a control, slots are booked    *)
(*             under the id the coordinator drew, before the fold checks   *)
(*             it (the bug of the old resumed create).                     *)
(*   Tombs     TRUE: a deleted id stays used in the fold (a tombstone);    *)
(*             FALSE: a control, a delete frees the id.                    *)
(*   Key       "pair": the host key is (TenantId, JournalId); "journal":   *)
(*             a control, the host key is the JournalId alone.             *)
(*   CellMint  "draw": a cell tenant draws its id at `cell init`;          *)
(*             "counter": a control, a counter of the cell gives it.       *)
(*   Fresh     TRUE: a TenantId draw never collides with another TenantId *)
(*             (a 64-bit random draw); only the liveness checks set it.   *)
(*             A refused `JoinFleet` is final, so without it a universe   *)
(*             draw for a user tenant can take the id of a cell that has  *)
(*             not joined yet, and that cell never joins.                 *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Cells, Users, Journals, Owner, Home, TIds, JIds,
          Order, Tombs, Key, CellMint, Fresh, Restarts

Tenants == Cells \cup Users

ASSUME TIds \subseteq Nat \ {0} /\ JIds \subseteq Nat \ {0}
ASSUME Home \in Cells
\* Every journal of the model lives on the one host of Home: a tenant of
\* Home, the cell tenant or a user tenant placed there.
ASSUME \A j \in Journals : Owner[j] \in {Home} \cup Users
ASSUME Order \in {"fold_first", "book_first"} /\ Tombs \in BOOLEAN
ASSUME Key \in {"pair", "journal"} /\ CellMint \in {"draw", "counter"}
ASSUME Fresh \in BOOLEAN /\ Restarts \in Nat

VARIABLES
  ctid,      \* cell -> its tenant's id, written once at `cell init` (0: not yet)
  joined,    \* cell -> "out", "in" or "refused"
  uused,     \* the fold of the universe: every TenantId it recorded
  utid,      \* user tenant -> the id the universe fold gave (0: none yet)
  udraw,     \* user tenant -> the id the coordinator drew (0: none)
  utried,    \* user tenant -> the ids the universe fold refused it
  tused,     \* tenant -> the fold of the tenant: every JournalId it recorded
  jrec,      \* journal -> the id its tenant's fold gave (0: none yet)
  jdraw,     \* journal -> the id the coordinator drew (0: none)
  jtried,    \* journal -> the ids the tenant fold refused it
  jstate,    \* journal -> "new", "live" or "gone"
  disk,      \* the host: a set of records <<key, journal>>
  restarts,  \* the restarts of the coordinators so far
  given,     \* history: every <<tenant, JournalId, journal>> a tenant fold gave
  tgiven,    \* history: every <<TenantId, tenant>> the universe fold gave
  events     \* history: the rare steps that happened, for the reach checks

vars == <<ctid, joined, uused, utid, udraw, utried, tused, jrec, jdraw, jtried,
          jstate, disk, restarts, given, tgiven, events>>

TidOf(t) == IF t \in Cells THEN ctid[t] ELSE utid[t]

\* The key under which the host keeps the records of journal j at id i.
KeyOf(j, i) == IF Key = "pair" THEN <<TidOf(Owner[j]), i>> ELSE i

\* A tenant can create journals once its id is known and, for a user
\* tenant, once Home joined the universe and can host it.
Ready(t) == IF t \in Cells THEN ctid[t] # 0 ELSE utid[t] # 0 /\ joined[Home] = "in"

Init ==
  /\ ctid = [c \in Cells |-> 0]
  /\ joined = [c \in Cells |-> "out"]
  /\ uused = {}
  /\ utid = [u \in Users |-> 0]
  /\ udraw = [u \in Users |-> 0]
  /\ utried = [u \in Users |-> {}]
  /\ tused = [t \in Tenants |-> {}]
  /\ jrec = [j \in Journals |-> 0]
  /\ jdraw = [j \in Journals |-> 0]
  /\ jtried = [j \in Journals |-> {}]
  /\ jstate = [j \in Journals |-> "new"]
  /\ disk = {}
  /\ restarts = 0
  /\ given = {}
  /\ tgiven = {}
  /\ events = {}

-----------------------------------------------------------------------------
(* The universe level. *)

\* `cell init` mints the id of the cell tenant, before any universe. A fresh
\* draw avoids every TenantId drawn before (the cells' and the universe's).
Genesis(c) ==
  /\ ctid[c] = 0
  /\ \E i \in TIds :
       /\ IF CellMint = "counter" THEN i = 1
          ELSE Fresh => /\ i \notin uused
                         /\ \A d \in Cells : ctid[d] # i
                         /\ \A u \in Users : udraw[u] # i
       /\ ctid' = [ctid EXCEPT ![c] = i]
  /\ UNCHANGED <<joined, uused, utid, udraw, utried, tused, jrec, jdraw, jtried,
                 jstate, disk, restarts, given, tgiven, events>>

\* `JoinFleet` at apply in the fold of the universe: the cell tenant's id is
\* recorded, or refused when it is in use. A refused cell stays out: its id
\* is already on its own disks.
Join(c) ==
  /\ ctid[c] # 0
  /\ joined[c] = "out"
  /\ IF ctid[c] \notin uused
       THEN /\ joined' = [joined EXCEPT ![c] = "in"]
            /\ uused' = uused \cup {ctid[c]}
            /\ tgiven' = tgiven \cup {<<ctid[c], c>>}
            /\ UNCHANGED events
       ELSE /\ joined' = [joined EXCEPT ![c] = "refused"]
            /\ events' = events \cup {"join_refused"}
            /\ UNCHANGED <<uused, tgiven>>
  /\ UNCHANGED <<ctid, utid, udraw, utried, tused, jrec, jdraw, jtried, jstate,
                 disk, restarts, given>>

\* The universe coordinator draws an id for a user tenant that the fold has
\* not given one yet. A new draw replaces a draw not yet recorded (a retry).
DrawTenant(u) ==
  /\ utid[u] = 0
  /\ \E i \in TIds \ utried[u] :
       /\ Fresh => \A c \in Cells : ctid[c] # i
       /\ udraw' = [udraw EXCEPT ![u] = i]
  /\ UNCHANGED <<ctid, joined, uused, utid, utried, tused, jrec, jdraw, jtried,
                 jstate, disk, restarts, given, tgiven, events>>

\* The fold of the universe applies the create: it gives the drawn id, or
\* refuses an id in use and the coordinator draws again.
RecordTenant(u) ==
  /\ udraw[u] # 0
  /\ udraw' = [udraw EXCEPT ![u] = 0]
  /\ IF udraw[u] \notin uused
       THEN /\ utid' = [utid EXCEPT ![u] = udraw[u]]
            /\ uused' = uused \cup {udraw[u]}
            /\ tgiven' = tgiven \cup {<<udraw[u], u>>}
            /\ UNCHANGED <<utried, events>>
       ELSE /\ utried' = [utried EXCEPT ![u] = @ \cup {udraw[u]}]
            /\ events' = events \cup {"tenant_refused"}
            /\ UNCHANGED <<utid, uused, tgiven>>
  /\ UNCHANGED <<ctid, joined, tused, jrec, jdraw, jtried, jstate, disk,
                 restarts, given>>

-----------------------------------------------------------------------------
(* The tenant level and the host. *)

DrawJournal(j) ==
  /\ Ready(Owner[j])
  /\ jstate[j] = "new"
  /\ \E i \in JIds \ jtried[j] : jdraw' = [jdraw EXCEPT ![j] = i]
  /\ UNCHANGED <<ctid, joined, uused, utid, udraw, utried, tused, jrec, jtried,
                 jstate, disk, restarts, given, tgiven, events>>

\* The fold of the tenant applies the create. A used id, a tombstone too,
\* is refused, and the coordinator draws again.
RecordJournal(j) ==
  /\ jdraw[j] # 0
  /\ jstate[j] = "new"
  /\ jdraw' = [jdraw EXCEPT ![j] = 0]
  /\ LET t == Owner[j]
         i == jdraw[j]
     IN IF i \notin tused[t]
          THEN /\ jrec' = [jrec EXCEPT ![j] = i]
               /\ jstate' = [jstate EXCEPT ![j] = "live"]
               /\ tused' = [tused EXCEPT ![t] = @ \cup {i}]
               /\ given' = given \cup {<<t, i, j>>}
               /\ UNCHANGED <<jtried, events>>
          ELSE /\ jtried' = [jtried EXCEPT ![j] = @ \cup {i}]
               /\ events' = events \cup
                    {"journal_refused"} \cup
                    (IF \E g \in given : g[1] = t /\ g[2] = i /\ jstate[g[3]] = "gone"
                       THEN {"tomb_refused"} ELSE {})
               /\ UNCHANGED <<jrec, jstate, tused, given>>
  /\ UNCHANGED <<ctid, joined, uused, utid, udraw, utried, disk, restarts, tgiven>>

\* The placement books slots for a journal on the host: under the id the
\* fold gave (the rule), or under the id the coordinator drew (the control).
Book(j) ==
  /\ jstate[j] # "gone"
  /\ LET i == IF Order = "fold_first" THEN jrec[j] ELSE jdraw[j]
     IN /\ i # 0
        /\ disk' = disk \cup {<<KeyOf(j, i), j>>}
  /\ UNCHANGED <<ctid, joined, uused, utid, udraw, utried, tused, jrec, jdraw,
                 jtried, jstate, restarts, given, tgiven, events>>

\* A delete is final. A delete of a create not yet recorded cancels it.
Delete(j) ==
  /\ jstate' = [jstate EXCEPT ![j] = "gone"]
  /\ IF Tombs THEN UNCHANGED tused
     ELSE tused' = [tused EXCEPT ![Owner[j]] = @ \ {jrec[j]}]
  /\ UNCHANGED <<ctid, joined, uused, utid, udraw, utried, jrec, jdraw, jtried,
                 disk, restarts, given, tgiven, events>>

\* The host drops the records of a deleted journal, at some later time.
Drop(j) ==
  /\ jstate[j] = "gone"
  /\ disk' = {r \in disk : r[2] # j}
  /\ UNCHANGED <<ctid, joined, uused, utid, udraw, utried, tused, jrec, jdraw,
                 jtried, jstate, restarts, given, tgiven, events>>

\* The coordinators restart: every draw not yet recorded is lost. The folds
\* and the disks stay.
Restart ==
  /\ restarts < Restarts
  /\ restarts' = restarts + 1
  /\ udraw' = [u \in Users |-> 0]
  /\ jdraw' = [j \in Journals |-> 0]
  /\ events' = events \cup
       (IF \E j \in Journals : jdraw[j] # 0 THEN {"draw_lost"} ELSE {})
  /\ UNCHANGED <<ctid, joined, uused, utid, utried, tused, jrec, jtried, jstate,
                 disk, given, tgiven>>

Next ==
  \/ \E c \in Cells : Genesis(c) \/ Join(c)
  \/ \E u \in Users : DrawTenant(u) \/ RecordTenant(u)
  \/ \E j \in Journals : DrawJournal(j) \/ RecordJournal(j) \/ Book(j)
                         \/ Delete(j) \/ Drop(j)
  \/ Restart

Spec == Init /\ [][Next]_vars

\* The creates are retried until they are done; a delete, a drop and a
\* restart may never happen.
Fairness ==
  /\ \A c \in Cells : WF_vars(Genesis(c)) /\ WF_vars(Join(c))
  /\ \A u \in Users : WF_vars(DrawTenant(u)) /\ WF_vars(RecordTenant(u))
  /\ \A j \in Journals : WF_vars(DrawJournal(j)) /\ WF_vars(RecordJournal(j))
                         /\ WF_vars(Book(j)) /\ WF_vars(Drop(j))

LiveSpec == Spec /\ Fairness

-----------------------------------------------------------------------------
(* The rule, "one id names at most one journal, forever", in four parts. *)

\* (a) A tenant fold gives a JournalId to at most one journal, over the
\* whole history, deletes included.
JournalIdOnce ==
  \A g, h \in given : g[1] = h[1] /\ g[2] = h[2] => g[3] = h[3]

\* (b) The universe fold gives a TenantId to at most one tenant.
TenantIdOnce ==
  \A g, h \in tgiven : g[1] = h[1] => g[2] = h[2]

\* (c) On the host, one key holds the records of at most one journal.
DiskOneJournal ==
  \A r, s \in disk : r[1] = s[1] => r[2] = s[2]

\* (d) The host holds records under a key only after the fold gave that key
\* to the journal.
BookedAfterRecord ==
  \A r \in disk : jrec[r[2]] # 0 /\ r[1] = KeyOf(r[2], jrec[r[2]])

TypeOK ==
  /\ ctid \in [Cells -> TIds \cup {0}]
  /\ joined \in [Cells -> {"out", "in", "refused"}]
  /\ uused \subseteq TIds
  /\ utid \in [Users -> TIds \cup {0}]
  /\ jrec \in [Journals -> JIds \cup {0}]
  /\ jstate \in [Journals -> {"new", "live", "gone"}]

\* Every create ends: each cell joins the universe, each user tenant and
\* each journal gets its id.
AllCreated ==
  <>(/\ \A c \in Cells : joined[c] = "in"
     /\ \A u \in Users : utid[u] # 0
     /\ \A j \in Journals : jstate[j] # "new")

\* A delete is final: a deleted journal never comes back.
GoneStays == [][\A j \in Journals : jstate[j] = "gone" => jstate'[j] = "gone"]_vars

\* The host drops only the records of a deleted journal.
KeepLive == [][\A r \in disk : jstate[r[2]] # "gone" => r \in disk']_vars

\* The host drops the records of a deleted journal, and books none again.
Dropped == \A j \in Journals : <>[](jstate[j] = "gone" => \A r \in disk : r[2] # j)

\* The reach checks: each must be violated, so the checks above see the step.
NoJournalRefused == "journal_refused" \notin events
NoTombRefused == "tomb_refused" \notin events
NoTenantRefused == "tenant_refused" \notin events
NoJoinRefused == "join_refused" \notin events
NoDrawLost == "draw_lost" \notin events \/ \A j \in Journals : jrec[j] = 0
=============================================================================
