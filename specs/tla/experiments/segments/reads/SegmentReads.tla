---------------------------- MODULE SegmentReads ----------------------------
(***************************************************************************)
(* #1254 (leaderless reads across a reshape): a leaderless read of the     *)
(* tail on full grids, in design A of the segments experiment (#1213), with *)
(* a reader that holds an old segment record.                              *)
(*                                                                         *)
(* The base is `../a/SegmentsA.tla`: the matchmakers hold the segment      *)
(* table (start, end, state of each configuration), the first leader of a  *)
(* configuration seals the one before it at one above the highest vote     *)
(* that its Phase 1 saw, Phase 1 reads a sealed configuration only below   *)
(* its end, the leader proposes again below its start (the `Noop` gap      *)
(* fill), and retire stops Phase 1 and Phase 2 on a sealed configuration.  *)
(* This module puts full grids in place of majorities (`Grid.tla`): a      *)
(* column writes, a row elects, and a reader asks one row. Two rows of one *)
(* grid do not meet (`grid/grid_full_rows`).                               *)
(*                                                                         *)
(* The reader caches one record of the table: a configuration and its      *)
(* start. It asks the acceptors of that configuration, one at a time, for  *)
(* their vote watermark, and serves the read at the highest watermark, or  *)
(* at the start minus one when that is higher, once each slot up to there  *)
(* is chosen. Acceptors keep their promise and votes on disk, and an       *)
(* acceptor of a retired configuration still answers from its disk.        *)
(*                                                                         *)
(* Constants select the rules:                                             *)
(*   Fence     TRUE: an answer carries the promise of the acceptor. When   *)
(*             that promise is a ballot of a later configuration than the  *)
(*             cached record, the reader fetches the record in force and   *)
(*             starts its read again (the `PreReadAck` fence on the        *)
(*             promise, `quorum_read.rs`). FALSE: no promise in an answer. *)
(*   SealRows  TRUE: Phase 1 also takes one promise in each row of each    *)
(*             configuration it reads (decision 31, #1209 (seal each row   *)
(*             of a full grid)). FALSE: only a Phase-1 quorum of each.     *)
(*   ReadRule  "row": the reader asks one row and serves on a Phase-1      *)
(*             quorum of it (the code).                                    *)
(*             "record": rule 1 of #1254, the same row read, and before    *)
(*             it serves, the reader reads the segment table at a          *)
(*             matchmaker quorum. When a newer configuration is in force,  *)
(*             it fetches that record and starts again.                    *)
(*   Wipe      TRUE: an acceptor that is in no configuration that still    *)
(*             runs can lose its state of the journal (the GC of a retired *)
(*             configuration deletes it, or the node is booked again onto  *)
(*             an empty store), and it then answers a read as a member     *)
(*             with an empty store does: promise 0, watermark 0. FALSE:    *)
(*             every acceptor answers from its disk for ever.              *)
(*   Belief    TRUE: each acceptor also knows the newest configuration it  *)
(*             heard of (from a Prepare, an Accept, or the register), and  *)
(*             the fence of an answer is the higher of that configuration  *)
(*             and the one of its promise (`wire_read_fence`: the promise  *)
(*             and the belief ballot). An acceptor that lost its state     *)
(*             answers no read until it read the register again (`Refuse`  *)
(*             of `reads/`, the membership probe at boot). FALSE: the      *)
(*             fence is the promise alone.                                 *)
(*             "column": rule 2 of #1254, the reader asks one row and one  *)
(*             whole column (a write quorum), and serves only when both    *)
(*             answered. It needs Fence: a promise above the record in the *)
(*             column ends the read.                                       *)
(*                                                                         *)
(* The matchmakers are one atomic register, as in `../a/`: the matchmaker  *)
(* and handover models check how a quorum keeps it. A read of the table is *)
(* then linearizable by construction.                                      *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Acceptors, Values, None, Noop,
          NS, NB, NG,
          Fence, SealRows, ReadRule, Wipe, Belief,
          \* The quorum system of each configuration, which `MC.tla` builds
          \* from `Grid.tla` once (full grids: a column writes, a row elects
          \* and reads):
          CfgOf,        \* configuration -> its members
          Q1s,          \* configuration -> its Phase-1 quorums
          Q2s,          \* configuration -> its Phase-2 quorums (one column)
          RowsOf,       \* configuration -> its rows: the sets a reader asks
          ColsOf        \* configuration -> its columns: its write quorums

ASSUME Fence \in BOOLEAN /\ SealRows \in BOOLEAN /\ Wipe \in BOOLEAN /\ Belief \in BOOLEAN
ASSUME Belief => Fence
ASSUME ReadRule \in {"row", "record", "column"}
ASSUME ReadRule = "column" => Fence
ASSUME None \notin Values /\ Noop \notin Values /\ Noop # None

Slots == 1..NS
Ballots == 1..NB
Gens == 1..NG
Cmds == Values \cup {Noop}

Cfg(g) == CfgOf[g]
Q1(g) == Q1s[g]
Q2(g) == Q2s[g]
Rows(g) == RowsOf[g]
Columns(g) == ColsOf[g]

ASSUME \A g \in Gens : Cfg(g) \subseteq Acceptors
\* Paxos safety rests on this: every Phase-1 quorum meets every Phase-2 quorum.
ASSUME \A g \in Gens : \A P \in Q1(g), C \in Q2(g) : P \cap C # {}
\* A row holds a Phase-1 quorum, and a column is a Phase-2 quorum.
ASSUME \A g \in Gens : \A T \in Rows(g) : \E P \in Q1(g) : P \subseteq T
ASSUME \A g \in Gens : Columns(g) \subseteq Q2(g)

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : x <= y

VARIABLES
  \* The matchmakers: the registrations and the segment table.
  bgen,     \* ballot -> its configuration, 0 before it registers
  sst,      \* configuration -> "none", "open", "sealed", "retired"
  sstart,   \* configuration -> its start slot, 0 while unknown
  send,     \* configuration -> its end slot, 0 while open
  \* Leaders.
  bst,      \* ballot -> "new", "p1" (Phase 1 done), "w" (writes)
  bsafe,    \* ballot -> slot -> the value that Phase 1 makes it propose, or None
  bmax,     \* ballot -> the highest slot with a vote that Phase 1 saw
  prop,     \* ballot -> slot -> its proposal, or None
  \* Acceptors.
  prom,     \* acceptor -> the highest ballot it promised or voted at
  vote,     \* acceptor -> slot -> <<ballot, value>>, <<0, None>> when none
  sent2b,   \* every vote ever sent: <<slot, ballot, value, acceptor>>
  known,    \* acceptor -> the newest configuration it heard of, 0 after it lost its state
  \* The reader.
  rview,    \* the configuration of its cached record
  rstart,   \* the start slot of its cached record
  rT,       \* the acceptors that its open read asks
  rasked,   \* the acceptors that answered the open read
  rwm,      \* the highest watermark that they answered
  rok,      \* rule 1: TRUE once the record check of the open read passed
  rneed,    \* the highest chosen slot when its read started
  rhot,     \* TRUE when a later configuration than its record had chosen a slot then
  rindex,   \* the index that the read served
  rst       \* "idle", "wait", "done": a read starts in any state, and a
            \* new start drops the open read

mm == <<bgen, sst, sstart, send>>
ldr == <<bst, bsafe, bmax, prop>>
acc == <<prom, vote, sent2b, known>>
rdr == <<rview, rstart, rT, rasked, rwm, rok, rneed, rhot, rindex, rst>>
vars == <<mm, ldr, acc, rdr>>

NoProp == [s \in Slots |-> None]

TypeOK ==
  /\ bgen \in [Ballots -> 0..NG]
  /\ sst \in [Gens -> {"none", "open", "sealed", "retired"}]
  /\ sstart \in [Gens -> 0..NS + 1]
  /\ send \in [Gens -> 0..NS + 1]
  /\ bst \in [Ballots -> {"new", "p1", "w"}]
  /\ bsafe \in [Ballots -> [Slots -> Cmds \cup {None}]]
  /\ prop \in [Ballots -> [Slots -> Cmds \cup {None}]]
  /\ prom \in [Acceptors -> 0..NB]
  /\ rview \in Gens
  /\ rT \subseteq Acceptors
  /\ rst \in {"idle", "wait", "done"}

Init ==
  /\ bgen = [b \in Ballots |-> 0]
  /\ sst = [g \in Gens |-> IF g = 1 THEN "open" ELSE "none"]
  /\ sstart = [g \in Gens |-> IF g = 1 THEN 1 ELSE 0]
  /\ send = [g \in Gens |-> 0]
  /\ bst = [b \in Ballots |-> "new"]
  /\ bsafe = [b \in Ballots |-> NoProp]
  /\ bmax = [b \in Ballots |-> 0]
  /\ prop = [b \in Ballots |-> NoProp]
  /\ prom = [a \in Acceptors |-> 0]
  /\ vote = [a \in Acceptors |-> [s \in Slots |-> <<0, None>>]]
  /\ sent2b = {}
  /\ known = [a \in Acceptors |-> 1]
  /\ rview = 1
  /\ rstart = 1
  /\ rT = {}
  /\ rasked = {}
  /\ rwm = 0
  /\ rok = FALSE
  /\ rneed = 0
  /\ rhot = FALSE
  /\ rindex = 0
  /\ rst = "idle"

(***************************************************************************)
(* Helpers                                                                 *)
(***************************************************************************)

\* The configurations that still run: registered, not retired.
Live == {g \in Gens : sst[g] \in {"open", "sealed"}}
\* The acceptors that still run.
Active == UNION {Cfg(g) : g \in Live}
\* The newest registered configuration.
Top == Max({g \in Gens : sst[g] # "none"})
\* The configuration in force: the newest one that opened.
InForce == Max({g \in Gens : sst[g] # "none" /\ sstart[g] # 0})
Range(g) == IF sstart[g] = 0 \/ send[g] = 0 THEN {} ELSE sstart[g]..(send[g] - 1)

ChosenAtBal(s, v, b) ==
  bgen[b] # 0 /\ \E Q \in Q2(bgen[b]) : \A a \in Q : <<s, b, v, a>> \in sent2b
Chosen(s, v) == \E b \in Ballots : ChosenAtBal(s, v, b)
IsChosen(s) == \E v \in Cmds : Chosen(s, v)
ChosenInGen(s, g) == \E b \in Ballots, v \in Cmds : bgen[b] = g /\ ChosenAtBal(s, v, b)
\* The chosen prefix: what a replica that applied every chosen slot holds.
Prefix == Max({k \in 0..NS : \A s \in 1..k : IsChosen(s)})
HighChosen == Max({s \in Slots : IsChosen(s)} \cup {0})

(***************************************************************************)
(* Matchmaking, Phase 1, the seal                                          *)
(***************************************************************************)

\* Ballot b registers configuration g: the one in force or its successor
\* (plan rule 1; OneSuccessor). A successor opens a record with no start
\* yet. Ballots register in order, so a ballot registers once: a registered
\* b fails c < b for c = b.
Register(b, g) ==
  /\ \A c \in Ballots : bgen[c] # 0 => c < b
  /\ g \in {Top, Top + 1}
  /\ g = Top + 1 => sstart[Top] # 0
  /\ bgen' = [bgen EXCEPT ![b] = g]
  /\ sst' = IF g = Top + 1 THEN [sst EXCEPT ![g] = "open"] ELSE sst
  /\ UNCHANGED <<sstart, send, ldr, acc, rdr>>

\* The slots that Phase 1 reads in configuration h.
ReadSet(h) == IF send[h] # 0 THEN 1..(send[h] - 1) ELSE Slots

\* P covers the configurations H: it holds a Phase-1 quorum of each, and with
\* SealRows one acceptor of each row of each.
Covers(H, P) ==
  /\ \A h \in H : \E Q \in Q1(h) : Q \subseteq P
  /\ SealRows => \A h \in H : \A T \in Rows(h) : T \cap P # {}

\* The smallest sets that cover H, for each H. A larger set only adds
\* promises: these are the hardest cases for a read, which a promise can only
\* fence. A constant, so TLC computes it once.
MinCovers == [H \in SUBSET Gens |->
  {P \in SUBSET (UNION {Cfg(h) : h \in H}) :
     Covers(H, P) /\ ~\E x \in P : Covers(H, P \ {x})}]

\* Phase 1 of ballot b: a set P that covers every configuration that still
\* runs, up to the ballot's own, promises b. The leader keeps, for each slot, the
\* value of the highest vote that it saw.
Phase1(b) ==
  LET g == bgen[b]
      H == {h \in Live : h <= g}
  IN /\ g # 0
     /\ bst[b] = "new"
     /\ \E P \in MinCovers[H] :
          /\ \A a \in P : prom[a] < b
          /\ LET Seen(s) == {a \in P : \E h \in H : a \in Cfg(h) /\ s \in ReadSet(h)}
                 voted(s) == {vote[a][s] : a \in {x \in Seen(s) : vote[x][s][1] > 0}}
                 top(s) == CHOOSE x \in voted(s) : \A y \in voted(s) : y[1] <= x[1]
             IN /\ bsafe' = [bsafe EXCEPT ![b] =
                    [s \in Slots |-> IF voted(s) # {} THEN top(s)[2] ELSE None]]
                /\ bmax' = [bmax EXCEPT ![b] = Max({s \in Slots : voted(s) # {}} \cup {0})]
          /\ prom' = [a \in Acceptors |-> IF a \in P THEN b ELSE prom[a]]
          /\ known' = IF Belief
                       THEN [a \in Acceptors |-> IF a \in P THEN Max({known[a], g}) ELSE known[a]]
                       ELSE known
     /\ bst' = [bst EXCEPT ![b] = "p1"]
     /\ UNCHANGED <<mm, prop, vote, sent2b, rdr>>

\* The first leader of configuration g seals g - 1 by compare-and-set: the
\* end of g - 1 and the start of g are one above the highest vote that its
\* Phase 1 saw (rule 1 of design A). g - 1 has a start: g registered only
\* once the newest configuration, g - 1, had one (Register).
Seal(b) ==
  LET g == bgen[b]
      p == g - 1
      e == Max({bmax[b], sstart[p] - 1}) + 1
  IN /\ bst[b] = "p1"
     /\ g >= 2
     /\ send[p] = 0
     /\ send' = [send EXCEPT ![p] = e]
     /\ sstart' = [sstart EXCEPT ![g] = e]
     /\ sst' = [sst EXCEPT ![p] = "sealed"]
     /\ bst' = [bst EXCEPT ![b] = "w"]
     /\ UNCHANGED <<bgen, bsafe, bmax, prop, acc, rdr>>

\* A leader whose configuration already opened starts to write.
Open(b) ==
  /\ bst[b] = "p1"
  /\ sstart[bgen[b]] # 0
  /\ bst' = [bst EXCEPT ![b] = "w"]
  /\ UNCHANGED <<mm, bsafe, bmax, prop, acc, rdr>>

(***************************************************************************)
(* Phase 2                                                                 *)
(***************************************************************************)

\* The slots that the leader of b may propose at: each slot above the chosen
\* prefix, which it learns from the replicas. Below the start of its segment
\* it proposes the vote that Phase 1 saw, or a Noop where it saw none (rule
\* 2b of design A).
Writable(b) == {s \in Slots : s > Prefix}

\* The leader of b proposes at its next slot: the lowest slot that it may
\* write and did not propose yet (the allocator frontier of the core).
Propose(b, v) ==
  LET open == {t \in Writable(b) : prop[b][t] = None}
      s == Min(open)
  IN /\ bst[b] = "w"
     /\ open # {}
     /\ v \in IF bsafe[b][s] # None THEN {bsafe[b][s]}
              ELSE IF s < sstart[bgen[b]] THEN {Noop}
              ELSE Values
     /\ prop' = [prop EXCEPT ![b][s] = v]
     /\ UNCHANGED <<mm, bst, bsafe, bmax, acc, rdr>>

\* An Accept of a vote that the acceptor has changes nothing.
Accept(a, b, s) ==
  /\ bgen[b] # 0
  /\ a \in Cfg(bgen[b])
  /\ a \in Active
  /\ prop[b][s] # None
  /\ prom[a] <= b
  /\ prom' = [prom EXCEPT ![a] = b]
  /\ vote' = [vote EXCEPT ![a][s] = <<b, prop[b][s]>>]
  /\ sent2b' = sent2b \cup {<<s, b, prop[b][s], a>>}
  /\ known' = IF Belief THEN [known EXCEPT ![a] = Max({@, bgen[b]})] ELSE known
  /\ UNCHANGED <<mm, ldr, rdr>>

\* Matchmaker garbage collection of the oldest sealed configuration
\* (RetiredPrefix), at any time after its seal. The Phase 1 of the
\* successor's first leader holds a Phase-1 quorum of p, which meets every
\* Phase-2 quorum of p, so p chooses nothing after the seal. The condition
\* of the paper (each slot of the range chosen, each vote at or above the end
\* chosen later) lets a later leader learn from the replicas what p chose:
\* design A (`../a/`) checks it against the replicas (Readable,
\* `retire_early`). The reads of this folder need no part of it, and an
\* earlier retire only gives them more cases.
Retire(p) ==
  /\ sst[p] = "sealed"
  /\ p = Min(Live)
  /\ sst' = [sst EXCEPT ![p] = "retired"]
  /\ UNCHANGED <<bgen, sstart, send, ldr, acc, rdr>>

\* An acceptor that is in no configuration that still runs loses its state
\* of the journal. The history of the votes stays in sent2b: what was chosen
\* stays chosen. A wipe of an empty store loses only the belief, which the
\* probe reads again.
WipeStore(a) ==
  /\ Wipe
  /\ a \notin Active
  /\ prom' = [prom EXCEPT ![a] = 0]
  /\ vote' = [vote EXCEPT ![a] = [s \in Slots |-> <<0, None>>]]
  /\ known' = IF Belief THEN [known EXCEPT ![a] = 0] ELSE known
  /\ UNCHANGED <<mm, ldr, sent2b, rdr>>

\* An acceptor that lost its state reads the register (the membership probe):
\* it learns the configuration in force. Only Belief sets known to 0.
Probe(a) ==
  /\ known[a] = 0
  /\ known' = [known EXCEPT ![a] = InForce]
  /\ UNCHANGED <<mm, ldr, prom, vote, sent2b, rdr>>

(***************************************************************************)
(* The reader                                                              *)
(***************************************************************************)

\* The sets of acceptors that a read on configuration g asks.
Targets(g) ==
  IF ReadRule = "column"
    THEN {T \cup C : T \in Rows(g), C \in Columns(g)}
    ELSE Rows(g)

\* The reader fetches the record in force, at any time. The start of a
\* record never changes, so a fetch of the cached record changes nothing, and
\* a fetch during a read only raises the start that the read serves at.
Fetch ==
  /\ rview' = InForce
  /\ rstart' = sstart[InForce]
  /\ UNCHANGED <<mm, ldr, acc, rT, rasked, rwm, rok, rneed, rhot, rindex, rst>>

\* The reader starts a read at any time: it may drop an open read and start
\* again. ReadFresh judges the read against its last start.
ReadStart ==
  /\ rneed' = HighChosen
  /\ rhot' = \E s \in Slots, g \in Gens : g > rview /\ ChosenInGen(s, g)
  /\ \E T \in Targets(rview) : rT' = T
  /\ rasked' = {}
  /\ rwm' = 0
  /\ rok' = FALSE
  /\ rst' = "wait"
  /\ UNCHANGED <<mm, ldr, acc, rview, rstart, rindex>>

\* The read starts again on the record in force; it keeps what it must see.
Restart ==
  /\ rview' = InForce
  /\ rstart' = sstart[InForce]
  /\ \E T \in Targets(InForce) : rT' = T
  /\ rasked' = {}
  /\ rwm' = 0
  /\ rok' = FALSE

Watermark(a) == Max({s \in Slots : vote[a][s][1] > 0} \cup {0})

\* A fence above the record: a promise at a ballot of a later configuration,
\* or with Belief a later configuration that the acceptor heard of.
Above(a) ==
  \/ prom[a] # 0 /\ bgen[prom[a]] > rview
  \/ Belief /\ known[a] > rview

\* Acceptor a answers the read with its vote watermark, and with Fence its
\* promise. A promise above the record ends the read, and the reader starts
\* again on the record in force. A second answer of an acceptor is a later
\* answer, and an answer outside an open read (rst # "wait") changes nothing
\* that a served read keeps: ReadStart sets each part of a read again.
ReadAsk(a) ==
  /\ a \in rT
  \* With Belief, an acceptor that lost its state answers no read.
  /\ Belief => known[a] # 0
  /\ IF Fence /\ Above(a)
       THEN Restart
       ELSE /\ rasked' = rasked \cup {a}
            /\ rwm' = Max({rwm, Watermark(a)})
            /\ UNCHANGED <<rview, rstart, rT, rok>>
  /\ UNCHANGED <<mm, ldr, acc, rneed, rhot, rindex, rst>>

\* The answers that a read needs before it serves.
Complete ==
  /\ \E Q \in Q1(rview) : Q \subseteq rasked
  /\ ReadRule = "column" => \E C \in Columns(rview) : C \subseteq rasked

\* Rule 1: once the answers are complete, the reader reads the segment table
\* at a matchmaker quorum. The check passes when the cached record is still
\* in force. A newer record in force ends the read, and it starts again. The
\* reader serves in a later step: a reshape can come between the two.
\* A check outside an open read changes nothing that a served read keeps, and
\* a check again of a read that passed only finds a newer record, which
\* starts the read again. `Next` runs it with ReadRule = "record" only.
RecordCheck ==
  /\ Complete
  /\ IF InForce = rview
       THEN /\ rok' = TRUE
            /\ UNCHANGED <<rview, rstart, rT, rasked, rwm>>
       ELSE Restart
  /\ UNCHANGED <<mm, ldr, acc, rneed, rhot, rindex, rst>>

\* The reader serves at the highest watermark, or the start minus one, once
\* every slot up to there is chosen (a replica caught up to it; ServedChosen).
\* A serve again of a served read answers later, so no less fresh.
ReadServe ==
  LET i == Max({rwm, rstart - 1})
  IN /\ Complete
     /\ ReadRule = "record" => rok
     /\ \A s \in 1..i : IsChosen(s)
     /\ rindex' = i
     /\ rst' = "done"
     /\ UNCHANGED <<mm, ldr, acc, rview, rstart, rT, rasked, rwm, rok, rneed, rhot>>

Next ==
  \/ \E b \in Ballots, g \in Gens : Register(b, g)
  \/ \E b \in Ballots : Phase1(b) \/ Seal(b) \/ Open(b)
  \/ \E b \in Ballots, v \in Cmds : Propose(b, v)
  \/ \E a \in Acceptors, b \in Ballots, s \in Slots : Accept(a, b, s)
  \/ \E p \in Gens : Retire(p)
  \/ \E a \in Acceptors : WipeStore(a) \/ Probe(a)
  \/ Fetch \/ ReadStart \/ ReadServe
  \/ ReadRule = "record" /\ RecordCheck
  \/ \E a \in Acceptors : ReadAsk(a)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Checks                                                                  *)
(***************************************************************************)

\* At most one value is chosen for each slot.
Agreement == \A s \in Slots, v, w \in Cmds : Chosen(s, v) /\ Chosen(s, w) => v = w

\* Nothing is chosen in a sealed configuration at or above its end.
SealedFrozen == \A p \in Gens, s \in Slots : send[p] # 0 /\ s >= send[p] => ~ChosenInGen(s, p)

\* Plan rule 1: one configuration in force and at most one successor, the
\* newest, with no start yet.
OneSuccessor == \A g \in Gens : sst[g] # "none" /\ sstart[g] = 0 => g = Top

\* Matchmaker garbage collection forgets the oldest configuration first.
RetiredPrefix == \A p, q \in Gens : p < q /\ sst[q] = "retired" => sst[p] = "retired"

\* A served read serves chosen slots only.
ServedChosen == rst = "done" => \A s \in 1..rindex : IsChosen(s)

\* A served read covers the highest slot chosen before it started.
ReadFresh == rst = "done" => rindex >= rneed

\* Reachability control, must be violated: a read that started on a stale
\* record, after a later configuration chose a slot, is served.
NoStaleStart == ~(rst = "done" /\ rhot)

\* Reachability control, must be violated: a slot is chosen in configuration
\* 2 and configuration 1 retired.
NoRetire == ~(sst[1] = "retired" /\ \E s \in Slots : ChosenInGen(s, 2))
=============================================================================
