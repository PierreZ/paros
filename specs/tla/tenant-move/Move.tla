-------------------------------- MODULE Move --------------------------------
(* The move of one tenant between two cells, for #1015 (model of the tenant  *)
(* move with abort). The steps are those of the safety review of 2026-11-11  *)
(* (#1000 (tenant move safety review)): `MoveTenant`, `ReleaseTenant`,       *)
(* `AdoptTenant`, the arrival of each journal, `SettleTenant`, `FinishMove`  *)
(* and `MovedTenant`. Each step is one write to one journal.                 *)
(*                                                                           *)
(* A journal is the cell that holds its configuration in force and the cells *)
(* whose machines stopped it. The Paxos layer is not in the model.           *)
(*                                                                           *)
(* DeskRule : "cached" = the desk of a coordinator term claims and reads the *)
(*                 tenant control journal one time, and an arrival uses that *)
(*                 fold (`main` on 2026-11-12, #1004).                       *)
(*            "fresh"  = each arrival claims and reads the journal again.    *)
(* Pause : TRUE = one step of a coordinator can read its registry, stop, and *)
(*            send its `Reconfigure` later (finding F4 of the review).       *)
(* ReconfFence : TRUE = a `Reconfigure` towards a cell is refused when the   *)
(*            registry of that cell does not show the tenant as hosted.      *)
(* Abort : "none"   = an open move has no abort (`main`).                    *)
(*         "direct" = the operator clears the move in the directory. A       *)
(*                    target that sees no move for it leaves the tenant.     *)
(*         "acked"  = the operator marks the move as aborting. The target    *)
(*                    gives each journal back, then records the abort. The   *)
(*                    operator clears the move only on that record. A target *)
(*                    that settled first wins: the operator finishes the     *)
(*                    move.                                                  *)
(* AdoptFence : TRUE = the registry refuses an `AdoptTenant` for a move at   *)
(*            or below the last move that it adopted or aborted.             *)
(*                                                                           *)
(* Not in the model: the Paxos layer, the matchmaker set, `MovePeers` and    *)
(* the cell fence, the removal of a tenant, two operators, a stale read of   *)
(* the directory by a source coordinator, footprints.                        *)
EXTENDS Naturals, FiniteSets

CONSTANTS Cells, Home, Journals, MaxMoves, MaxTerms,
          DeskRule, Pause, ReconfFence, Abort, AdoptFence

None == "none"

VARIABLES dir,       \* the directory: [cell, move, epoch, aborting]
          reg,       \* cell -> the registry record of the tenant
          last,      \* cell -> the last move that its registry adopted or aborted
          desk,      \* cell -> [open, fold]: the desk of the coordinator term
          terms,     \* cell -> coordinator terms that ended
          writer,    \* the cell whose claim on the tenant control journal is in force
          live,      \* the live journals in the tenant control journal
          created,   \* each journal identifier that was used
          at,        \* journal -> the cell of its configuration in force
          stopped,   \* journal -> the cells whose machines stopped it
          adoptRead, \* cell -> the move that its coordinator read as open for it (0: none)
          pending,   \* the paused `Reconfigure` calls: <<cell, journal>>
          returned   \* history: an abort gave a journal back

vars == <<dir, reg, last, desk, terms, writer, live, created, at, stopped, adoptRead,
          pending, returned>>

Other(c) == CHOOSE d \in Cells : d # c
Closed   == [open |-> FALSE, fold |-> {}]

Init == /\ dir = [cell |-> Home, move |-> None, epoch |-> 0, aborting |-> FALSE]
        /\ reg = [c \in Cells |-> IF c = Home THEN "hosted" ELSE "none"]
        /\ last = [c \in Cells |-> 0]
        /\ desk = [c \in Cells |-> Closed]
        /\ terms = [c \in Cells |-> 0]
        /\ writer = None
        /\ live = {}
        /\ created = {}
        /\ at = [j \in Journals |-> Home]
        /\ stopped = [j \in Journals |-> {}]
        /\ adoptRead = [c \in Cells |-> 0]
        /\ pending = {}
        /\ returned = FALSE

\* ---------------------------------------------------------------------------
\* The tenant desk of a cell that hosts the tenant.

\* The mode step opens the desk: one claim, one read to the tail.
OpenDesk(c) ==
    /\ reg[c] = "hosted"
    /\ ~ desk[c].open
    /\ desk' = [desk EXCEPT ![c] = [open |-> TRUE, fold |-> live]]
    /\ writer' = c
    /\ UNCHANGED <<dir, reg, last, terms, live, created, at, stopped, adoptRead, pending,
                   returned>>

\* A write of a desk whose claim is not in force is refused. The desk is
\* superseded.
Refused(c) ==
    /\ desk' = [desk EXCEPT ![c] = Closed]
    /\ UNCHANGED <<live, created, at, stopped>>

Create(c, j) ==
    /\ reg[c] = "hosted"
    /\ desk[c].open
    /\ j \notin created
    /\ IF writer = c
       THEN /\ created' = created \cup {j}
            /\ live' = live \cup {j}
            /\ at' = [at EXCEPT ![j] = c]
            /\ stopped' = [stopped EXCEPT ![j] = {}]
            /\ desk' = [desk EXCEPT ![c].fold = @ \cup {j}]
       ELSE Refused(c)
    /\ UNCHANGED <<dir, reg, last, terms, writer, adoptRead, pending, returned>>

Delete(c, j) ==
    /\ reg[c] = "hosted"
    /\ desk[c].open
    /\ j \in desk[c].fold
    /\ IF writer = c
       THEN /\ live' = live \ {j}
            /\ desk' = [desk EXCEPT ![c].fold = @ \ {j}]
            /\ UNCHANGED <<created, at, stopped>>
       ELSE Refused(c)
    /\ UNCHANGED <<dir, reg, last, terms, writer, adoptRead, pending, returned>>

\* The coordinator term of c ends. The next term has a new desk.
NewTerm(c) ==
    /\ terms[c] < MaxTerms
    /\ desk[c].open
    /\ terms' = [terms EXCEPT ![c] = @ + 1]
    /\ desk' = [desk EXCEPT ![c] = Closed]
    /\ UNCHANGED <<dir, reg, last, writer, live, created, at, stopped, adoptRead, pending,
                   returned>>

\* ---------------------------------------------------------------------------
\* The move.

MoveTenant(c) ==
    /\ dir.move = None
    /\ dir.cell # c
    /\ dir.epoch < MaxMoves
    /\ dir' = [dir EXCEPT !.move = c, !.epoch = @ + 1]
    /\ UNCHANGED <<reg, last, desk, terms, writer, live, created, at, stopped, adoptRead,
                   pending, returned>>

Release(c) ==
    /\ dir.cell = c
    /\ dir.move # None
    /\ ~ dir.aborting
    /\ reg[c] = "hosted"
    /\ reg' = [reg EXCEPT ![c] = "leaving"]
    /\ UNCHANGED <<dir, last, desk, terms, writer, live, created, at, stopped, adoptRead,
                   pending, returned>>

\* The target coordinator reads the directory and the source registry.
ReadAdopt(c) ==
    /\ dir.move = c
    /\ ~ dir.aborting
    /\ reg[Other(c)] = "leaving"
    /\ adoptRead[c] = 0
    /\ adoptRead' = [adoptRead EXCEPT ![c] = dir.epoch]
    /\ UNCHANGED <<dir, reg, last, desk, terms, writer, live, created, at, stopped, pending,
                   returned>>

\* It writes `AdoptTenant` later. The registry judges the write at apply.
Adopt(c) ==
    /\ adoptRead[c] # 0
    /\ adoptRead' = [adoptRead EXCEPT ![c] = 0]
    /\ IF /\ reg[c] \in {"none", "moved", "aborted"}
          /\ AdoptFence => adoptRead[c] > last[c]
       THEN /\ reg' = [reg EXCEPT ![c] = "arriving"]
            /\ last' = [last EXCEPT ![c] = adoptRead[c]]
            /\ desk' = IF DeskRule = "fresh" THEN [desk EXCEPT ![c] = Closed] ELSE desk
       ELSE UNCHANGED <<reg, last, desk>>
    /\ UNCHANGED <<dir, terms, writer, live, created, at, stopped, pending, returned>>

\* The arrival opens the desk when it holds none.
ArriveOpen(c) ==
    /\ reg[c] = "arriving"
    /\ ~ desk[c].open
    /\ desk' = [desk EXCEPT ![c] = [open |-> TRUE, fold |-> live]]
    /\ writer' = c
    /\ UNCHANGED <<dir, reg, last, terms, live, created, at, stopped, adoptRead, pending,
                   returned>>

\* A journal has a leader while the machines of its configuration run it.
Served(j) == j \in live /\ at[j] \notin stopped[j]

\* One journal of the fold arrives: a reconfiguration onto machines of c.
ArriveJournal(c, j) ==
    /\ reg[c] = "arriving"
    /\ desk[c].open
    /\ j \in desk[c].fold
    /\ at[j] # c
    /\ Served(j)
    /\ at' = [at EXCEPT ![j] = c]
    /\ stopped' = [stopped EXCEPT ![j] = @ \ {c}]
    /\ UNCHANGED <<dir, reg, last, desk, terms, writer, live, created, adoptRead, pending,
                   returned>>

Settle(c) ==
    /\ reg[c] = "arriving"
    /\ desk[c].open
    /\ \A j \in desk[c].fold : at[j] = c
    /\ reg' = [reg EXCEPT ![c] = "hosted"]
    /\ UNCHANGED <<dir, last, desk, terms, writer, live, created, at, stopped, adoptRead,
                   pending, returned>>

\* The operator reads the target registry and flips the directory.
FinishMove ==
    /\ dir.move # None
    /\ reg[dir.move] = "hosted"
    /\ dir' = [dir EXCEPT !.cell = dir.move, !.move = None, !.aborting = FALSE]
    /\ UNCHANGED <<reg, last, desk, terms, writer, live, created, at, stopped, adoptRead,
                   pending, returned>>

\* The machines of c stop each journal of the tenant (`leave_tenant`). The
\* desk of the term keeps the tenant.
Leave(c, state) ==
    /\ reg' = [reg EXCEPT ![c] = state]
    /\ stopped' = [j \in Journals |-> stopped[j] \cup {c}]

Moved(c) ==
    /\ reg[c] = "leaving"
    /\ dir.cell # c
    /\ Leave(c, "moved")
    /\ UNCHANGED <<dir, last, desk, terms, writer, live, created, at, adoptRead, pending,
                   returned>>

\* ---------------------------------------------------------------------------
\* Finding F4: a `Reconfigure` that a coordinator of c sends late.

PauseReconf(c, j) ==
    /\ Pause
    /\ pending = {}
    /\ reg[c] = "hosted"
    /\ j \in live
    /\ at[j] = c
    /\ pending' = {<<c, j>>}
    /\ UNCHANGED <<dir, reg, last, desk, terms, writer, live, created, at, stopped,
                   adoptRead, returned>>

ResumeReconf(c, j) ==
    /\ <<c, j>> \in pending
    /\ pending' = {}
    /\ IF Served(j) /\ (ReconfFence => reg[c] = "hosted")
       THEN /\ at' = [at EXCEPT ![j] = c]
            /\ stopped' = [stopped EXCEPT ![j] = @ \ {c}]
       ELSE UNCHANGED <<at, stopped>>
    /\ UNCHANGED <<dir, reg, last, desk, terms, writer, live, created, adoptRead, returned>>

\* ---------------------------------------------------------------------------
\* The abort.

StartAbort ==
    /\ Abort # "none"
    /\ dir.move # None
    /\ ~ dir.aborting
    /\ dir' = IF Abort = "direct" THEN [dir EXCEPT !.move = None]
                                  ELSE [dir EXCEPT !.aborting = TRUE]
    /\ UNCHANGED <<reg, last, desk, terms, writer, live, created, at, stopped, adoptRead,
                   pending, returned>>

\* "direct": a cell that holds the tenant, and that the directory does not
\* name, leaves it.
Unadopt(c) ==
    /\ Abort = "direct"
    /\ reg[c] \in {"arriving", "hosted"}
    /\ dir.cell # c
    /\ dir.move # c
    /\ Leave(c, "moved")
    /\ UNCHANGED <<dir, last, desk, terms, writer, live, created, at, adoptRead, pending,
                   returned>>

\* "acked": the target gives one journal back to the source.
ReturnJournal(c, j) ==
    /\ Abort = "acked"
    /\ dir.aborting
    /\ dir.move = c
    /\ reg[c] = "arriving"
    /\ j \in live
    /\ at[j] = c
    /\ at' = [at EXCEPT ![j] = dir.cell]
    /\ returned' = TRUE
    /\ UNCHANGED <<dir, reg, last, desk, terms, writer, live, created, stopped, adoptRead,
                   pending>>

\* "acked": the target holds no journal. It records the abort of this move.
AckAbort(c) ==
    /\ Abort = "acked"
    /\ dir.aborting
    /\ dir.move = c
    /\ reg[c] \in {"none", "moved", "aborted", "arriving"}
    /\ \A j \in live : at[j] # c
    /\ last[c] <= dir.epoch
    /\ last' = [last EXCEPT ![c] = dir.epoch]
    /\ Leave(c, "aborted")
    /\ UNCHANGED <<dir, desk, terms, writer, live, created, at, adoptRead, pending,
                   returned>>

\* "acked": the operator reads the record of the target and clears the move.
FinishAbort ==
    /\ dir.aborting
    /\ reg[dir.move] = "aborted"
    /\ last[dir.move] = dir.epoch
    /\ dir' = [dir EXCEPT !.move = None, !.aborting = FALSE]
    /\ UNCHANGED <<reg, last, desk, terms, writer, live, created, at, stopped, adoptRead,
                   pending, returned>>

\* The source of a move that was aborted takes the tenant again. Its desk
\* claims and reads again.
Reclaim(c) ==
    /\ Abort # "none"
    /\ reg[c] = "leaving"
    /\ dir.cell = c
    /\ dir.move = None
    /\ reg' = [reg EXCEPT ![c] = "hosted"]
    /\ desk' = [desk EXCEPT ![c] = Closed]
    /\ UNCHANGED <<dir, last, terms, writer, live, created, at, stopped, adoptRead, pending,
                   returned>>

Next ==
    \/ \E c \in Cells :
          \/ OpenDesk(c) \/ NewTerm(c) \/ MoveTenant(c) \/ Release(c) \/ ReadAdopt(c)
          \/ Adopt(c) \/ ArriveOpen(c) \/ Settle(c) \/ Moved(c) \/ Unadopt(c)
          \/ AckAbort(c) \/ Reclaim(c)
          \/ \E j \in Journals :
                \/ Create(c, j) \/ Delete(c, j) \/ ArriveJournal(c, j)
                \/ PauseReconf(c, j) \/ ResumeReconf(c, j) \/ ReturnJournal(c, j)
    \/ FinishMove \/ StartAbort \/ FinishAbort

Spec == Init /\ [][Next]_vars

\* ---------------------------------------------------------------------------

\* I2 of the review: no live journal is stopped on the machines of the cell
\* that holds its configuration. Such a journal has no leader for good.
NoLostJournal == \A j \in live : at[j] \notin stopped[j]

\* At `SettleTenant`, and until the flip, the target holds each live journal.
SettleComplete ==
    \A c \in Cells : (reg[c] = "hosted" /\ dir.move = c) => \A j \in live : at[j] = c

\* With no open move, the cell of the directory holds each live journal.
NoStray == dir.move = None => \A j \in live : at[j] = dir.cell

\* An arrival never waits for a journal that is not live.
NotStuck ==
    \A c \in Cells : (reg[c] = "arriving" /\ desk[c].open) => desk[c].fold \subseteq live

\* Not real invariants. TLC must find a trace for each one: this shows that
\* the model reaches a round trip, and an abort that gave a journal back.
NoRoundTrip ==
    ~ (dir.epoch = 2 /\ dir.move = None /\ dir.cell = Home /\ reg[Home] = "hosted"
       /\ Cardinality(live) = Cardinality(Journals))
NoAbortEnd ==
    ~ (returned /\ dir.move = None /\ reg[dir.cell] = "hosted" /\ live # {}
       /\ \E c \in Cells : reg[c] = "aborted")
=============================================================================
