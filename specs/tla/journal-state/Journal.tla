------------------------------- MODULE Journal -------------------------------
(***************************************************************************)
(* The journal state model: the composed model of the journal v1, #1189    *)
(* (the journal state model) with #1309 (the state is the disk).           *)
(* docs/journal/07-verification.md, section J7.2.                          *)
(*                                                                         *)
(* Each rule of docs/journal/ has its own small model (section J7.1). This *)
(* model puts the rules together, so that TLC checks the seams between     *)
(* them: Paxos inside each segment, the seal of a segment by the first     *)
(* leader of the next grid, the matchmaker register of the journal, the    *)
(* boundary state, the replica set of each segment, the journal fold of   *)
(* `Write`, `Truncate` and `SetLeader` (with `None`) judged at apply,      *)
(* retire, trim and the fenced reshape of the grid controller, across a    *)
(* chain of G segments.                                                    *)
(*                                                                         *)
(* THE STATE IS THE DISK (#1309). The variables of an acceptor are what    *)
(* its disk holds (`dprom`, `dvote`); its memory (`mprom`, `mvote`) is the *)
(* state of its role, and a step of the role appends its writes to a       *)
(* queue (`pend`). The driver makes one write durable at a time (`Flush`), *)
(* so a batch is a sequence of atomic writes, and a crash between any two  *)
(* of them drops the rest of the queue and starts the role again from its  *)
(* disk. Each Phase 1b and Phase 2b reply is built from the disk: it is    *)
(* sent at the flush of the write that makes it true (the flight rule of   *)
(* section J4.2, transmitted at durability, as in `flight-rule/`). A       *)
(* leader keeps its state in memory and has no disk: a ballot belongs to   *)
(* one leader, and a leader that stops is a ballot that takes no more      *)
(* steps. The replicas and the matchmakers keep their state on disk, and   *)
(* each of their steps is one atomic write.                                *)
(*                                                                         *)
(* THE REGISTER. The matchmakers of the journal are one compare-and-set    *)
(* register (`cas-register/`, section J3.4). A write is first taken by a   *)
(* minority ("partial") and is done when a majority took it. A partial     *)
(* write can be lost: a later operation reads a majority that does not     *)
(* hold it and writes over it. `segment-records/` checks how a real        *)
(* register gives this behaviour across a handover; this model keeps the   *)
(* two outcomes for the two fields where "act on a write seen done"        *)
(* matters (section J3.4): the seal end and the boundary state. The other  *)
(* fields (the reshape, retired, trimmed) are one atomic write.            *)
(*                                                                         *)
(* THE LOG. A slot holds a `Noop` (the election gap fill) or a command:    *)
(*   [op "w", u, e, id]   a `Write` of one record. u is a uuid (a fenced   *)
(*   [op "w", u, e, id, n] a `Write` of n records, at the positions e to   *)
(*                        e + n - 1 (`RolesTrace.tla`, the wide walk).     *)
(*                        write at the position e) or None (unfenced).     *)
(*   [op "tr", u, up]     a `Truncate` to the position up, under u.        *)
(*   [op "sl", new, old]  `SetLeader(new, old)`; new may be None.          *)
(* A multi-writer mode is None or a tag of `Tags`: the code gives each     *)
(* mode a fresh tag (section J2.4.1, #1495), so a trace names it with its  *)
(* tag. `multi-writer/` checks the tag rule; this model folds a tag as     *)
(* it folds None.                                                          *)
(* The fold of a replica is (leader, next, first, recs): recs holds the    *)
(* positions written in this segment, with their write. Every command is   *)
(* judged at apply (section J2.2, J2.4.1).                                 *)
(*                                                                         *)
(* COMPACTION. `JournalCompact.tla` adds the compaction of section J3.2:   *)
(* a replica drops its log below a point, the acceptors drop the votes of  *)
(* a segment (`adrop`), a laggard jumps (`rcomp`). This module holds what  *)
(* the other steps do with it, so that its own checks keep the state space *)
(* they had: a promise reports the slots dropped, a leader proposes        *)
(* nothing there (`bskip`) and its seal end counts them as votes, an       *)
(* acceptor refuses an `Accept` there, and a replica serves a peer only    *)
(* the slots that it keeps.                                                *)
(*                                                                         *)
(* DAMAGE. `JournalCtrl.tla` adds CTRL (section J5.3): a vote that a disk  *)
(* lost keeps its ballot and loses its value, `<<b, None>>` (a faulty      *)
(* vote). This module holds what the other steps do with it: a Phase 1     *)
(* whose top report at a slot is a faulty vote blocks the slot (`bblock`): *)
(* the leader proposes nothing there, and its next slot is above it.       *)
(*                                                                         *)
(* Constants select the rules. The first value of each is the design; each *)
(* other value is a control that TLC must break.                          *)
(*   Reply1b  "disk": a promise is sent once it is durable. "mem": it is   *)
(*            sent from memory, before its write (the E1 bug of            *)
(*            etcd-io/raft#551): the record of a seal is written and the   *)
(*            promises that sealed the old grid are lost.                  *)
(*   Reply2b  "disk": a vote is sent once it is durable. "mem": it is sent *)
(*            from memory, before its write.                               *)
(*   WriteOrder  the writes of an `Accept` that raises the promise:        *)
(*            "atomic" one write; "prom_first" the promise, then the vote; *)
(*            "vote_first" the vote, then the promise.                     *)
(*   EndRule  "voted": the seal end is one above the highest slot where    *)
(*            the sealer's Phase 1 saw a vote in the old grid. "applied":  *)
(*            one above the highest slot that a replica applied.           *)
(*   EndAct   "done": a leader writes in its grid, and a replica stops at  *)
(*            the end, only once the write of the end is done. "read": on  *)
(*            a partial write.                                             *)
(*   BoundAct "done": a replica of the next segment starts from a          *)
(*            boundary write that is done. "read": from a partial one.     *)
(*   GapFill  TRUE: a leader writes a `Noop` at each slot below the start  *)
(*            of its segment where its Phase 1 saw no vote. FALSE: only    *)
(*            where it saw one.                                            *)
(*   RetireRule "full": retire once the boundary state is done and         *)
(*            RetireAt replicas of the set hold every slot. "early": at    *)
(*            the seal. "off": no retire, to check Phase 1 alone (a wrong  *)
(*            end breaks only the retire: `end_applied_safe` of            *)
(*            `journal-read/`).                                            *)
(*   ReshapeGuard TRUE: a reshape needs every older configuration retired  *)
(*            (`RetireGuard` of `grid-controller/`). FALSE: no guard.      *)
(*   Lookup   "below": the fold answers `Below` for a fenced write whose   *)
(*            position is in an older segment. "local": it answers from    *)
(*            the records of its own segment only (position taken).        *)
(*   Resolve  "write": a client that got `Below` sends                     *)
(*            Resolve(uuid, expected_seq, write) to a replica of the old   *)
(*            segment, whose fold covers the position; the replica judges  *)
(*            it with the compare of apply (#1320). "uuid": it compares    *)
(*            the uuid only. "early": a replica of the old segment answers *)
(*            before its fold covers the position.                         *)
(*   TrimRule "sealed": a segment is trimmed once its end is done and a    *)
(*            fold has `first_seq` at or above its last `seq`. "open": the *)
(*            open segment can be trimmed too, on its range so far.        *)
(*   ReadRule "all": Phase 1 reads every slot of every configuration that  *)
(*            is not retired, sealed or not (plain Matchmaker Paxos), so   *)
(*            one value for each slot does not rest on the seal end. "end":*)
(*            a sealed configuration is read only below its end, as it was *)
(*            done at the ballot's `Register` (the old rule: with a wrong  *)
(*            end, `end_applied_end` of journal-read/ chooses two values). *)
(*   Bind     "cas": a ballot binds to the newest configuration, above     *)
(*            every bound ballot, in one compare-and-set on the register   *)
(*            (`Change::Bind`). "stale": a ballot above every bound ballot *)
(*            binds to any configuration that is not retired, from a stale *)
(*            register value that showed it in force and not sealed (a     *)
(*            host that skips the compare-and-set).                        *)
(*                                                                         *)
(* Constants that size a check, with no rule in them:                      *)
(*   Values   slot -> the commands that a leader may propose there (a      *)
(*            script of the clients); a `Noop` of the gap fill may come    *)
(*            at any slot below the start of a segment.                    *)
(*   GenOK    the pairs <<ballot, configuration>> that may register: one   *)
(*            ballot for each configuration, or any (leader changes, and   *)
(*            two first leaders of one grid that race to seal).            *)
(*   Partial  TRUE: a write of the seal end or of the boundary state is    *)
(*            first partial, and can be lost. FALSE: it is done at once.   *)
(*   MaxCrash the crashes of acceptors in a run. With no crash and both    *)
(*            replies from the disk (`Direct`), a write is durable in its  *)
(*            step and Phase 1 takes its promise quorum in one step.       *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences

CONSTANTS
  Acceptors, CfgOf, QOf,  \* the acceptors, the grid of each configuration, its quorums
  RepOf,                  \* the replica set of each configuration
  Values,                 \* slot -> the commands that a leader may propose there
  L0,                     \* the leader of a fresh journal: Unset, None or a tag
  None, Unset, Noop,
  Tags,                   \* the tags of multi-writer modes other than None
  G, NS, NB, MaxCrash, RetireAt,
  Partial,                \* TRUE: a register write can be partial and lost
  GenOK,                  \* the pairs <<ballot, configuration>> that may register
  Reply1b, Reply2b, WriteOrder, EndRule, EndAct, BoundAct, GapFill,
  RetireRule, ReshapeGuard, Lookup, Resolve, TrimRule, ReadRule, Bind

ASSUME G \in Nat /\ G >= 1 /\ NS \in Nat /\ NS >= 1 /\ NB \in Nat /\ NB >= 1
ASSUME MaxCrash \in Nat /\ RetireAt \in Nat /\ RetireAt >= 1
ASSUME (L0 \in {Unset, None} \/ L0 \in Tags) /\ Partial \in BOOLEAN
ASSUME Reply1b \in {"disk", "mem"} /\ Reply2b \in {"disk", "mem"}
ASSUME WriteOrder \in {"atomic", "prom_first", "vote_first"}
ASSUME EndRule \in {"voted", "applied"}
ASSUME EndAct \in {"done", "read"} /\ BoundAct \in {"done", "read"}
ASSUME GapFill \in BOOLEAN /\ ReshapeGuard \in BOOLEAN
ASSUME RetireRule \in {"full", "early", "off"}
ASSUME Lookup \in {"below", "local"}
ASSUME Resolve \in {"write", "uuid", "early"}
ASSUME TrimRule \in {"sealed", "open"}
ASSUME ReadRule \in {"all", "end"} /\ Bind \in {"cas", "stale"}

Gens == 1..G
Slots == 0..(NS - 1)
Ballots == 1..NB
Replicas == UNION {RepOf[g] : g \in Gens}
ASSUME \A g \in Gens : CfgOf[g] \subseteq Acceptors /\ QOf[g] \subseteq SUBSET CfgOf[g]
ASSUME \A g \in Gens : \A Q1, Q2 \in QOf[g] : Q1 \cap Q2 # {}

\* The Phase 2 quorums of slot s in configuration g. A grid of one column
\* has one kind of quorum, `QOf[g]`; a grid of many columns chooses slot s
\* in its column `s % columns`, and its Phase 1 needs a quorum of every
\* column (section J5.1). `RolesTrace.tla` overrides this definition for the
\* grids of two columns of the wide roles walk; every quorum of Phase 1
\* meets every quorum of Phase 2 of each slot.
Q2Of(g, s) == QOf[g]
ASSUME \A g \in Gens : \A s \in 0..(NS - 1) :
         /\ Q2Of(g, s) \subseteq SUBSET CfgOf[g]
         /\ \A Q1 \in QOf[g] : \A Q2 \in Q2Of(g, s) : Q1 \cap Q2 # {}
ASSUME \A g, h \in Gens : g # h => RepOf[g] \cap RepOf[h] = {}
ASSUME \A g \in Gens : Cardinality(RepOf[g]) >= RetireAt
ASSUME \A s \in Slots : Noop \notin Values[s]

\* With no crash and replies from the disk, a step's writes are durable and
\* its reply is sent in the step: the queue only delays a send, which is a
\* slow network (the lesson of `flight-rule/`).
Direct == MaxCrash = 0 /\ Reply1b = "disk" /\ Reply2b = "disk"

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : x <= y
NoVote == <<0, None>>

VARIABLES
  \* The register of the journal (the matchmakers).
  top,     \* the newest configuration that a reshape opened
  rend,    \* configuration -> its seal end: None, or [v, done]
  rbound,  \* configuration -> its boundary state: None, or [v, done]
  rret,    \* configuration -> retired
  rtrim,   \* configuration -> trimmed
  bgen,    \* ballot -> the configuration it registered, 0 before
  \* Leaders, in memory.
  bst,     \* ballot -> "new", "p1" (Phase 1 done), "w" (writes)
  bH,      \* ballot -> the configurations its Phase 1 reads, fixed at Register
  bE,      \* ballot -> configuration -> its seal end at Register, or None
  bsafe,   \* ballot -> slot -> the vote that its Phase 1 saw, or None
  bend,    \* ballot -> the seal end it computed, or None
  blo,     \* ballot -> the first slot that it writes, fixed at Register
  bstart,  \* ballot -> the start of its segment when it opened, or None
  prop,    \* ballot -> slot -> its proposal, or None
  bskip,   \* ballot -> the slots that its Phase 1 saw dropped
  bblock,  \* ballot -> the slots whose top report was a faulty vote
  \* Acceptors: memory, the queue of writes, disk.
  mprom, mvote, pend, dprom, dvote,
  adrop,   \* acceptor -> the slots whose votes it dropped, in memory
  dadrop,  \* acceptor -> the slots whose votes it dropped, on disk
  crashes, \* the crashes so far
  rots,    \* the votes that a disk lost so far (`JournalCtrl.tla`)
  p1b,     \* the promises sent: [a, b, votes]
  sent2b,  \* the votes sent: <<s, b, v, a>>
  \* Replicas, on disk.
  rapp,    \* replica -> how many slots of its segment it applied
  rbase,   \* replica -> the boundary state it started from, or None
  rstart,  \* replica -> the start slot of its segment when it started, or None
  acked,   \* slot -> the answer that a replica gave, or None
  resolved, \* slot -> the answer to the Resolve of a `Below`, or None
  rdrop,   \* replica -> it dropped its copy (its segment is trimmed)
  rcomp,   \* replica -> the slot below which it dropped its log
  rfloor,  \* replica -> the trim floor of its compaction point
  trims    \* the compaction points on a disk: <<segment, slot, floor>>

reg == <<top, rend, rbound, rret, rtrim, bgen>>
ldr == <<bst, bH, bE, bsafe, bend, blo, bstart, prop, bskip, bblock>>
acc == <<mprom, mvote, pend, dprom, dvote, crashes, rots, p1b, sent2b, adrop, dadrop>>
rep == <<rapp, rbase, rstart, acked, resolved, rdrop, rcomp, rfloor, trims>>
vars == <<reg, ldr, acc, rep>>

(***************************************************************************)
(* The register.                                                           *)
(***************************************************************************)
Done(x) == x # None /\ x.done
\* The end of g as the actors see it: done, or also partial with EndAct "read".
EndKnown(g) == IF EndAct = "done" THEN Done(rend[g]) ELSE rend[g] # None
\* The start slot of g, once known.
StartKnown(g) == IF g = 1 THEN TRUE ELSE EndKnown(g - 1)
Start(g) == IF g = 1 THEN 0 ELSE rend[g - 1].v
\* The configuration in force: the newest whose predecessor's end is done.
InForce == Max({g \in 1..top : g = 1 \/ Done(rend[g - 1])})
\* The configurations whose acceptors still run.
Live == {g \in 1..top : ~rret[g]}
Active == UNION {CfgOf[g] : g \in Live}

(***************************************************************************)
(* Chosen values: a quorum of the ballot's grid sent its vote.             *)
(***************************************************************************)
ChosenAtBal(s, v, b) ==
  bgen[b] # 0 /\ \E Q \in Q2Of(bgen[b], s) : \A a \in Q : <<s, b, v, a>> \in sent2b
Cmds == UNION {Values[s] : s \in Slots} \cup {Noop}
Chosen(s, v) == \E b \in Ballots : ChosenAtBal(s, v, b)
IsChosen(s) == \E v \in Cmds : Chosen(s, v)
ChosenVal(s) == CHOOSE v \in Cmds : Chosen(s, v)
ChosenInGen(s, g) == \E b \in Ballots, v \in Cmds : bgen[b] = g /\ ChosenAtBal(s, v, b)

\* The chosen prefix: the first slot that is not chosen.
Frontier == IF \A s \in Slots : IsChosen(s) THEN NS
            ELSE Min({s \in Slots : ~IsChosen(s)})

(***************************************************************************)
(* The fold (sections J2.2, J2.4.1, J2.6). base.next is the first position *)
(* of the segment: a fenced write below it is in an older segment.         *)
(***************************************************************************)
Empty == [leader |-> L0, next |-> 0, first |-> 0, recs |-> {}]
Fenced(v) == v.u # None
\* The records of a write: n, or one when the write names no n.
Size(v) == IF "n" \in DOMAIN v THEN v.n ELSE 1
\* The journal is multi-writer: None, or a mode with its tag.
IsMulti(l) == l = None \/ l \in Tags

\* The answer of a command to the state st of a fold that started at base.
Res(base, st, v) ==
  CASE v = Noop -> "noop"
    [] v.op = "w" /\ Fenced(v) ->
         IF v.e < st.next
         THEN IF v.e < st.first THEN "trunc"
              ELSE IF v.e < base.next /\ Lookup = "below" THEN "below"
              ELSE IF <<v.e, v.u, v.id>> \in st.recs THEN "dup"
              ELSE "taken"
         ELSE IF v.u # st.leader THEN "fenced"
         ELSE IF v.e > st.next THEN "early"
         ELSE "ok"
    [] v.op = "w" -> IF IsMulti(st.leader) THEN "ok" ELSE "wrong"
    [] v.op = "tr" -> IF \/ v.u = None /\ IsMulti(st.leader)
                         \/ v.u # None /\ v.u = st.leader
                      THEN "ok" ELSE "fenced"
    [] v.op = "sl" -> IF v.old = st.leader /\ v.new # st.leader THEN "ok" ELSE "refused"

Step(base, st, v) ==
  LET r == Res(base, st, v) IN
  IF r # "ok" THEN st
  ELSE CASE v.op = "w" ->
              [st EXCEPT !.next = @ + Size(v),
                         !.recs = @ \cup {<<st.next, v.u, v.id>>}]
         [] v.op = "tr" ->
              [st EXCEPT !.first = Max({@, Min({v.up, st.next})})]
         [] v.op = "sl" -> [st EXCEPT !.leader = v.new]

RECURSIVE FoldFrom(_, _, _, _)
FoldFrom(base, st, lo, hi) ==
  IF lo >= hi THEN st ELSE FoldFrom(base, Step(base, st, ChosenVal(lo)), lo + 1, hi)

\* The truth: the fold of the whole log from slot 0, with every record.
Truth(s) == FoldFrom(Empty, Empty, 0, s)
TrueRes(s) == Res(Empty, Truth(s), ChosenVal(s))

\* The boundary state: the fold with the records left out.
Bound(st) == [st EXCEPT !.recs = {}]

\* Slot s holds a record of the journal: a `Write` that applied.
Record(s) == IsChosen(s) /\ ChosenVal(s) # Noop /\ ChosenVal(s).op = "w" /\ TrueRes(s) = "ok"

(***************************************************************************)
(* Replicas.                                                               *)
(***************************************************************************)
GenOf(r) == CHOOSE g \in Gens : r \in RepOf[g]
Front(r) == rstart[r] + rapp[r]
StateOf(r) == FoldFrom(rbase[r], rbase[r], rstart[r], Front(r))
\* The replica applied every slot of its sealed segment.
Full(r) == LET g == GenOf(r) IN EndKnown(g) /\ Front(r) = rend[g].v
\* Replica q holds slot s in its log: it applied it and did not compact it.
Holds(q, s) == rbase[q] # None /\ rstart[q] <= s /\ s < Front(q) /\ s >= rcomp[q]
\* A fold that starts at a compaction point holds no write below the trim
\* floor of the point: until its own first_seq reaches the floor, it gives
\* no answer (`SegmentReplica`, a blind fold).
Blind(r) == StateOf(r).first < rfloor[r]

TypeOK ==
  /\ top \in Gens
  /\ bgen \in [Ballots -> 0..G]
  /\ bst \in [Ballots -> {"new", "p1", "w"}]
  /\ mprom \in [Acceptors -> 0..NB] /\ dprom \in [Acceptors -> 0..NB]
  /\ crashes \in 0..MaxCrash
  /\ rapp \in [Replicas -> 0..NS]

Init ==
  /\ top = 1
  /\ rend = [g \in Gens |-> None]
  /\ rbound = [g \in Gens |-> None]
  /\ rret = [g \in Gens |-> FALSE]
  /\ rtrim = [g \in Gens |-> FALSE]
  /\ bgen = [b \in Ballots |-> 0]
  /\ bst = [b \in Ballots |-> "new"]
  /\ bH = [b \in Ballots |-> {}]
  /\ bE = [b \in Ballots |-> [h \in Gens |-> None]]
  /\ bsafe = [b \in Ballots |-> [s \in Slots |-> None]]
  /\ bend = [b \in Ballots |-> None]
  /\ blo = [b \in Ballots |-> 0]
  /\ bstart = [b \in Ballots |-> None]
  /\ prop = [b \in Ballots |-> [s \in Slots |-> None]]
  /\ mprom = [a \in Acceptors |-> 0]
  /\ mvote = [a \in Acceptors |-> [s \in Slots |-> NoVote]]
  /\ pend = [a \in Acceptors |-> <<>>]
  /\ dprom = [a \in Acceptors |-> 0]
  /\ dvote = [a \in Acceptors |-> [s \in Slots |-> NoVote]]
  /\ crashes = 0
  /\ rots = 0
  /\ p1b = {}
  /\ sent2b = {}
  /\ rapp = [r \in Replicas |-> 0]
  /\ rbase = [r \in Replicas |-> IF r \in RepOf[1] THEN Empty ELSE None]
  /\ rstart = [r \in Replicas |-> IF r \in RepOf[1] THEN 0 ELSE None]
  /\ acked = [s \in Slots |-> None]
  /\ rdrop = [r \in Replicas |-> FALSE]
  /\ rcomp = [r \in Replicas |-> 0]
  /\ rfloor = [r \in Replicas |-> 0]
  /\ trims = {}
  /\ bskip = [b \in Ballots |-> {}]
  /\ bblock = [b \in Ballots |-> {}]
  /\ adrop = [a \in Acceptors |-> {}]
  /\ dadrop = [a \in Acceptors |-> {}]
  /\ resolved = [s \in Slots |-> None]

(***************************************************************************)
(* The grid controller: the fenced reshape (section J6.1) opens the next   *)
(* configuration in state pending, with its grid and its replica set. Its  *)
(* guards: the newest configuration is in force, and every older one is    *)
(* retired (`RetireGuard`). `grid-controller/` checks the term and the     *)
(* expected configuration; one controller runs here.                       *)
(***************************************************************************)
Reshape ==
  /\ top < G
  \* With the guard the newest configuration is in force: `Retire` needs
  \* the end of its configuration done. The control keeps that check alone.
  /\ IF ReshapeGuard THEN \A h \in 1..(top - 1) : rret[h] ELSE top = InForce
  /\ top' = top + 1
  /\ UNCHANGED <<rend, rbound, rret, rtrim, bgen, ldr, acc, rep>>

(***************************************************************************)
(* Matchmaking and Phase 1.                                                *)
(***************************************************************************)
\* The configurations that the Phase 1 of a ballot of g reads.
H(g) == {h \in Live : h <= g}

\* The lowest slot that a campaign may write from: the start of the oldest
\* configuration of its history.
LowFrom == Start(Min(H(top)))
\* The highest: the front of a replica of that configuration with a base.
HighFrom == Max({LowFrom} \cup
                {Front(r) : r \in {r \in RepOf[Min(H(top))] : rbase[r] # None}})

\* Ballot b registers the newest configuration, in ballot order. The
\* leader takes what its Phase 1 reads from the register value of its
\* campaign, and keeps it (`JournalLeader::campaign`): the configurations
\* that are not retired, the end of each one that is sealed, and the first
\* slot it writes, the applied prefix of the oldest configuration it reads
\* (its replicas hold every slot below). A retire or a seal after the
\* campaign does not change them: the leader of a retired configuration's
\* acceptors waits, and a seal by another first leader stops this one.
\* A ballot registers once, above every registered ballot: the second guard
\* also refuses b itself once it registered. The code's register is
\* `Change::Bind`; its "ok" again for the bound ballot on the same newest
\* configuration changes nothing, so the model has no step for it.
\* The leader writes from `lo`, from `LowFrom` to `HighFrom`. Phase 1 reads
\* every slot from `lo`, so a lower `lo` reads more. `Register` takes the
\* highest; `JournalCompact.tla` also takes the lowest, so that a Phase 1
\* reads the slots the acceptors dropped.
\* Ballot b binds to configuration g and writes from lo. Above every bound
\* ballot. Bound to the newest configuration, it keeps the done end of each
\* configuration; bound to an older one, it saw no end.
RegisterIn(b, g, lo) ==
  /\ \A c \in Ballots : bgen[c] # 0 => c < b
  /\ <<b, g>> \in GenOK
  /\ bgen' = [bgen EXCEPT ![b] = g]
  /\ bH' = [bH EXCEPT ![b] = H(g)]
  /\ bE' = [bE EXCEPT ![b] = [h \in Gens |->
                IF g = top /\ Done(rend[h]) THEN rend[h].v ELSE None]]
  /\ blo' = [blo EXCEPT ![b] = lo]
  /\ UNCHANGED <<top, rend, rbound, rret, rtrim, bst, bsafe, bend, bstart, prop, bskip, bblock, acc, rep>>

RegisterFrom(b, lo) == RegisterIn(b, top, lo)

Register(b) == RegisterFrom(b, HighFrom)

\* The control of `Bind`: ballot b binds to g, a configuration that is not
\* retired, from a stale register value. That value showed g as the newest
\* configuration with no end, so the leader reads the configurations up to
\* g and writes from the start of the oldest one: a leader that takes its
\* configuration from a value it read, not from the value of its own done
\* `Change::Bind`.
StaleRegister(b, g) ==
  /\ Bind = "stale"
  /\ RegisterIn(b, g, Start(Min(H(g))))

\* Ballot b, bound and before its Phase 1, takes a later register value
\* than the one of its own done `Bind`: a value that another write gave
\* (a seal, a retire, a reshape) or the "ok" again of a `Bind` that it sent
\* again after a lost reply (`JournalLeader::observe` takes every value
\* that names its ballot as bound). It reads the configurations of its
\* binding that are not retired in that value, the end of each one that is
\* done there, and writes from the start of the oldest one or above. A value
\* where every configuration of the binding is retired gives a Phase 1 that
\* asks nobody and never closes, so the model has no step for it; an
\* unbound ballot reads nothing (`H(0)` is empty).
Reread(b) ==
  /\ bst[b] = "new"
  /\ H(bgen[b]) # {}
  /\ bH' = [bH EXCEPT ![b] = H(bgen[b])]
  /\ bE' = [bE EXCEPT ![b] = [h \in Gens |->
                IF Done(rend[h]) THEN rend[h].v ELSE None]]
  /\ blo' = [blo EXCEPT ![b] = Max({blo[b], Start(Min(H(bgen[b])))})]
  /\ UNCHANGED <<reg, bst, bsafe, bend, bstart, prop, bskip, bblock, acc, rep>>

\* The slots of h that the Phase 1 of b reads. With "all", every slot: the
\* end of a sealed configuration bounds nothing in Phase 1, and one value
\* for each slot rests on the promises alone. With "end", the slots below
\* the end, once the end was done at its Register.
ReadSet(b, h) == IF ReadRule = "end" /\ bE[b][h] # None THEN 0..(bE[b][h] - 1) ELSE Slots

\* The reports of Phase 1 of b from the acceptors of Q: <<a, votes>>. With
\* `Direct`, the promises are durable in the step, so Phase 1 takes them from
\* the acceptors in one step (as `experiments/segments/a/`); else from the
\* promises that the acceptors sent.
Reports(b, Q) ==
  IF Direct THEN {<<a, mvote[a], adrop[a]>> : a \in Q}
  ELSE {<<m.a, m.votes, m.drop>> : m \in {m \in p1b : m.b = b /\ m.a \in Q}}
Promised(b, Q) ==
  IF Direct THEN \A a \in Q : a \in Active /\ mprom[a] < b
  ELSE \A a \in Q : \E m \in p1b : m.a = a /\ m.b = b

\* The Phase 1 of b on the promise quorum Qs[h] of each configuration h
\* that it reads. `RolesTrace.tla` names the quorums that the code read.
Phase1On(b, Qs) ==
  LET g == bgen[b] IN
  \* A registered ballot: `Live` holds no 0.
  /\ g \in Live
  /\ bst[b] = "new"
  /\ \A h \in bH[b] : Qs[h] \in QOf[h] /\ Promised(b, Qs[h])
  /\ LET \* The votes seen at each slot, in the read set of each grid.
          seen(s) == {x[2][s] : x \in UNION {Reports(b, Qs[h]) : h \in {h \in bH[b] : s \in ReadSet(b, h)}}}
          voted(s) == {x \in seen(s) : x[1] > 0}
          \* A faulty vote has a ballot and no value.
          readable(s) == {x \in voted(s) : x[2] # None}
          top1(s) == CHOOSE x \in readable(s) : \A y \in readable(s) : y[1] <= x[1]
          \* The tri-state tally (section J5.3): the threshold is the
          \* highest readable report; an answer qualifies unless it is a
          \* faulty vote above it. The top report is a faulty vote and the
          \* qualifying answers hold no quorum of some grid that reads s:
          \* the faulty value may be the chosen one, so the slot waits for
          \* a late answer (`JournalCtrl.tla`, `ProbeDecide`) or a replica.
          thr(s) == IF readable(s) = {} THEN 0 ELSE top1(s)[1]
          qual(s, h) == {a \in Qs[h] :
                          \A x \in Reports(b, {a}) :
                            ~(x[2][s][1] > thr(s) /\ x[2][s][2] = None)}
          blocked(s) == /\ \E x \in voted(s) : x[2] = None /\
                               \A y \in readable(s) : y[1] < x[1]
                        /\ ~\A h \in {h \in bH[b] : s \in ReadSet(b, h)} :
                              \E Q \in QOf[h] : Q \subseteq qual(s, h)
          \* The slots with a vote in the old grid: a dropped vote counts.
          old == IF g >= 2 /\ g - 1 \in bH[b]
                 THEN {s \in Slots : \E x \in Reports(b, Qs[g - 1]) :
                         x[2][s][1] > 0 \/ s \in x[3]}
                 ELSE {}
          all == UNION {Qs[h] : h \in bH[b]}
          \* A slot that one acceptor dropped is chosen and applied at
          \* F + 1 replicas (`JournalCompact.tla`): the leader proposes
          \* nothing there.
          skip == UNION {x[3] : x \in UNION {Reports(b, Qs[h]) : h \in bH[b]}}
      IN /\ bsafe' = [bsafe EXCEPT ![b] =
              [s \in Slots |-> IF readable(s) # {} /\ ~blocked(s) /\ s \notin skip
                               THEN top1(s)[2] ELSE None]]
         /\ bskip' = [bskip EXCEPT ![b] = skip]
         /\ bblock' = [bblock EXCEPT ![b] = {s \in Slots : blocked(s) /\ s \notin skip}]
         \* A first leader of g: the end of g - 1 was not written at its
         \* Register. The end is at or above the first slot it writes.
         /\ bend' = [bend EXCEPT ![b] =
              IF g >= 2 /\ bE[b][g - 1] = None
              THEN IF EndRule = "voted"
                   THEN Max({Start(g - 1), blo[b]} \cup {s + 1 : s \in old})
                   ELSE Max({Start(g - 1)} \cup
                            {Front(r) : r \in {r \in RepOf[g - 1] : rbase[r] # None}})
              ELSE None]
         /\ IF Direct
            THEN /\ mprom' = [a \in Acceptors |-> IF a \in all THEN b ELSE mprom[a]]
                 /\ dprom' = [a \in Acceptors |-> IF a \in all THEN b ELSE dprom[a]]
            ELSE UNCHANGED <<mprom, dprom>>
  /\ bst' = [bst EXCEPT ![b] = "p1"]
  /\ UNCHANGED <<reg, bH, bE, blo, prop, bstart, mvote, pend, dvote, crashes, rots, p1b, sent2b, adrop, dadrop, rep>>

Phase1(b) == \E Qs \in [bH[b] -> UNION {QOf[h] : h \in Gens}] : Phase1On(b, Qs)

(***************************************************************************)
(* The seal (section J3.1): the first leader of g writes the end of g - 1  *)
(* by compare-and-set from "no end". Its read may miss a partial write of  *)
(* another sealer, and then it writes over it.                             *)
(***************************************************************************)
\* The code writes the seal in Phase 1; in the model, a leader that already
\* writes may also write the seal again. Only its Phase 1 sets `bend[b]`, so each
\* write is still the end that its Phase 1 read.
SealWrite(b) ==
  LET g == bgen[b] IN
  /\ bend[b] # None
  \* No guard on the start of g - 1: `Reshape` opens g only once the end of
  \* g - 2 is done (retired, or in force), and a done end stays.
  \* No end, or a partial end that its read missed.
  /\ IF rend[g - 1] = None THEN TRUE
     ELSE ~rend[g - 1].done /\ rend[g - 1].v # bend[b]
  /\ rend' = [rend EXCEPT ![g - 1] = [v |-> bend[b], done |-> ~Partial]]
  /\ UNCHANGED <<top, rbound, rret, rtrim, bgen, ldr, acc, rep>>

\* A partial write reaches a majority: the write is done.
EndDone(p) ==
  /\ rend[p] # None /\ ~rend[p].done
  /\ rend' = [rend EXCEPT ![p] = [@ EXCEPT !.done = TRUE]]
  /\ UNCHANGED <<top, rbound, rret, rtrim, bgen, ldr, acc, rep>>

\* A partial write is lost: the next read of a majority misses it.
EndLost(p) ==
  /\ rend[p] # None /\ ~rend[p].done
  /\ rend' = [rend EXCEPT ![p] = None]
  /\ UNCHANGED <<top, rbound, rret, rtrim, bgen, ldr, acc, rep>>

\* A leader starts to write: once the end of its predecessor is known. The
\* sealer writes from its own end; the start is the end that is in the
\* register.
Open(b) ==
  LET g == bgen[b] IN
  /\ bst[b] = "p1"
  /\ StartKnown(g)
  /\ IF g >= 2 /\ bend[b] # None THEN rend[g - 1].v = bend[b] ELSE TRUE
  /\ bst' = [bst EXCEPT ![b] = "w"]
  /\ bstart' = [bstart EXCEPT ![b] = Start(g)]
  /\ UNCHANGED <<reg, bH, bE, bsafe, bend, blo, prop, bskip, bblock, acc, rep>>

(***************************************************************************)
(* Phase 2. The leader of b writes every slot of its segment, and below    *)
(* its start each slot from the first slot that its Phase 1 read: the vote *)
(* that its Phase 1 saw, or a `Noop` (the gap fill, #1233).                *)
(***************************************************************************)
Writable(b) ==
  {s \in Slots : /\ s >= blo[b]
                 /\ s \notin bskip[b]
                 /\ s \notin bblock[b]
                 /\ \/ s >= bstart[b]
                    \/ bsafe[b][s] # None
                    \/ GapFill}

\* One above the highest slot where the Phase 1 of b saw a vote, a blocked
\* slot included: a new command never takes a blocked slot.
TopSeen(b) == Max({0} \cup {s + 1 : s \in {s \in Slots : bsafe[b][s] # None} \cup bblock[b]})

\* The gap fill writes a `Noop` at each slot with no vote below the start of
\* the segment and below the highest vote seen (`JournalLeader::open`).
Propose(b, s, v) ==
  /\ bst[b] = "w"
  /\ s \in Writable(b)
  /\ prop[b][s] = None
  /\ \A t \in Writable(b) : t < s => prop[b][t] # None
  /\ v \in IF bsafe[b][s] # None THEN {bsafe[b][s]}
           ELSE IF s < bstart[b] \/ s < TopSeen(b) THEN {Noop}
           ELSE Values[s]
  /\ prop' = [prop EXCEPT ![b][s] = v]
  /\ UNCHANGED <<reg, bst, bH, bE, bsafe, bend, blo, bstart, bskip, bblock, acc, rep>>

(***************************************************************************)
(* Acceptors: a step changes memory and queues its writes. A write is      *)
(* <<kind, s, b, v, send>>: kind "p" (promise b), "v" (vote <<b, v>> at s) *)
(* or "pv" (both). send tells whether the write's flush sends the reply.   *)
(* Kind "d" is the compaction of `JournalCompact.tla`: <<"d", s, x, ..>>   *)
(* drops the votes of the slots s .. x - 1.                                *)
(***************************************************************************)
\* `bH[b]` is empty until b registers, so only a registered ballot is promised.
Promise(a, b) ==
  /\ ~Direct
  /\ a \in Active
  /\ \E h \in bH[b] : a \in CfgOf[h]
  /\ mprom[a] < b
  /\ mprom' = [mprom EXCEPT ![a] = b]
  /\ CASE Reply1b = "mem" ->
          /\ p1b' = p1b \cup {[a |-> a, b |-> b, votes |-> mvote[a], drop |-> adrop[a]]}
          /\ pend' = [pend EXCEPT ![a] = Append(@, <<"p", 0, b, None, FALSE>>)]
          /\ UNCHANGED dprom
       [] OTHER ->
          /\ pend' = [pend EXCEPT ![a] = Append(@, <<"p", 0, b, None, TRUE>>)]
          /\ UNCHANGED <<p1b, dprom>>
  /\ UNCHANGED <<reg, ldr, mvote, dvote, crashes, rots, sent2b, adrop, dadrop, rep>>

\* An acceptor answers again a `Prepare` at the ballot it holds on disk,
\* with no write: its reply waits on its last batch, so it carries the
\* disk as that batch left it (the flight rule, section J4.2). It changes
\* no state of the acceptor. As `Flush`, it needs no grid that runs: a
\* reply that waited on a batch goes once the batch is durable. `SpecRe`
\* adds it to `Next`: each answer sent again is one more report, so the
\* checks of `Spec` leave it out to keep their state spaces, and the
\* checks `repromise` and `reach_repromise` judge it.
Repromise(a, b) ==
  /\ dprom[a] = b
  /\ p1b' = p1b \cup {[a |-> a, b |-> b, votes |-> dvote[a], drop |-> dadrop[a]]}
  /\ UNCHANGED <<reg, ldr, mprom, mvote, dprom, dvote, crashes, rots, sent2b, adrop, dadrop, pend, rep>>

Accept(a, b, s) ==
  LET v == prop[b][s]
      send == Reply2b = "disk"
      raise == mprom[a] < b
      ws == CASE ~raise -> <<<<"v", s, b, v, send>>>>
              [] WriteOrder = "atomic" -> <<<<"pv", s, b, v, send>>>>
              [] WriteOrder = "prom_first" -> <<<<"p", 0, b, None, FALSE>>, <<"v", s, b, v, send>>>>
              [] WriteOrder = "vote_first" -> <<<<"v", s, b, v, send>>, <<"p", 0, b, None, FALSE>>>>
  IN
  /\ bgen[b] # 0
  /\ a \in CfgOf[bgen[b]]
  /\ a \in Active
  /\ v # None
  \* A dropped slot is chosen: the acceptor refuses it.
  /\ s \notin adrop[a]
  /\ mprom[a] <= b
  /\ mvote[a][s] # <<b, v>>
  /\ mprom' = [mprom EXCEPT ![a] = b]
  /\ mvote' = [mvote EXCEPT ![a][s] = <<b, v>>]
  /\ IF Direct
     THEN /\ dprom' = [dprom EXCEPT ![a] = b]
          /\ dvote' = [dvote EXCEPT ![a][s] = <<b, v>>]
          /\ sent2b' = sent2b \cup {<<s, b, v, a>>}
          /\ UNCHANGED pend
     ELSE /\ pend' = [pend EXCEPT ![a] = @ \o ws]
          /\ sent2b' = IF Reply2b = "mem" THEN sent2b \cup {<<s, b, v, a>>} ELSE sent2b
          /\ UNCHANGED <<dprom, dvote>>
  /\ UNCHANGED <<reg, ldr, crashes, rots, p1b, adrop, dadrop, rep>>

\* The driver makes the first queued write durable, and sends its reply.
Flush(a) ==
  LET w == Head(pend[a])
      k == w[1]
      dp == IF k \in {"p", "pv"} THEN w[3] ELSE dprom[a]
      dv == CASE k \in {"v", "pv"} -> [dvote[a] EXCEPT ![w[2]] = <<w[3], w[4]>>]
              [] k = "d" -> [s \in Slots |-> IF s \in w[2]..(w[3] - 1) THEN NoVote ELSE dvote[a][s]]
              [] OTHER -> dvote[a]
      dd == IF k = "d" THEN dadrop[a] \cup w[2]..(w[3] - 1) ELSE dadrop[a]
  IN
  /\ pend[a] # <<>>
  /\ dprom' = [dprom EXCEPT ![a] = dp]
  /\ dvote' = [dvote EXCEPT ![a] = dv]
  /\ pend' = [pend EXCEPT ![a] = Tail(@)]
  /\ dadrop' = [dadrop EXCEPT ![a] = dd]
  /\ p1b' = IF k = "p" /\ w[5] THEN p1b \cup {[a |-> a, b |-> w[3], votes |-> dv, drop |-> dd]} ELSE p1b
  /\ sent2b' = IF k \in {"v", "pv"} /\ w[5] THEN sent2b \cup {<<w[2], w[3], w[4], a>>} ELSE sent2b
  /\ UNCHANGED <<reg, ldr, mprom, mvote, crashes, rots, adrop, rep>>

\* A crash between two writes: the queue is lost, and the role starts again
\* from its disk.
Crash(a) ==
  /\ crashes < MaxCrash
  /\ crashes' = crashes + 1
  /\ mprom' = [mprom EXCEPT ![a] = dprom[a]]
  /\ mvote' = [mvote EXCEPT ![a] = dvote[a]]
  /\ adrop' = [adrop EXCEPT ![a] = dadrop[a]]
  /\ pend' = [pend EXCEPT ![a] = <<>>]
  /\ UNCHANGED <<reg, ldr, dprom, dvote, rots, p1b, sent2b, dadrop, rep>>

\* A quorum of a grid that still runs holds a chosen vote of slot s on disk.
\* (A later ballot can only vote the chosen value again, so a quorum may hold
\* it at several ballots of its grid.)
OnDisk(s) == IsChosen(s) /\ \E g \in Live : \E Q \in Q2Of(g, s) : \A a \in Q :
  /\ dvote[a][s][1] > 0
  /\ bgen[dvote[a][s][1]] = g
  /\ dvote[a][s][2] = ChosenVal(s)

(***************************************************************************)
(* Replicas (sections J3.1, J3.4).                                         *)
(***************************************************************************)
\* A replica applies the next chosen slot of its segment and answers its
\* writer. It stops at the end of its segment, once the end is known.
\* With commit, the replica may also learn a chosen slot from a `Commit` of
\* a leader that saw it chosen: the message can arrive after every live
\* source dropped the slot (`JournalCompact.tla`).
ApplyIf(r, commit) ==
  LET g == GenOf(r)
      s == Front(r)
  IN
  \* A replica holds a base only once its segment is open (g <= top) and its
  \* start is known; with EndAct = "done" the start stays known (`EndOnce`).
  \* A slot past the last one is never chosen, so the sources below bound s.
  /\ rbase[r] # None
  /\ IF EndKnown(g) THEN s < rend[g].v ELSE TRUE
  \* A replica of a trimmed segment applies until it drops its copy: it
  \* learns the trim from the register later (`Drop`). Nothing reads it:
  \* `Covers` and `HeldBy` count no replica of a trimmed segment.
  /\ ~rdrop[r]
  \* It learns the slot from a live source: a quorum of a grid that still
  \* runs holds the vote on its disk, or a peer of its set applied it.
  \* Both sources hold a chosen slot only (`OnDisk` and a peer's `Apply`).
  /\ \/ OnDisk(s)
     \/ \E q \in RepOf[g] \ {r} : Holds(q, s)
     \/ commit /\ IsChosen(s)
  /\ rapp' = [rapp EXCEPT ![r] = @ + 1]
  /\ acked' = IF Blind(r) THEN acked
              ELSE [acked EXCEPT ![s] = Res(rbase[r], StateOf(r), ChosenVal(s))]
  /\ UNCHANGED <<reg, ldr, acc, rbase, rstart, resolved, rdrop, rcomp, rfloor, trims>>

Apply(r) == ApplyIf(r, FALSE)

\* A replica that applied every slot of its sealed segment writes the
\* boundary state, by compare-and-set from "no boundary".
BoundWrite(r) ==
  LET g == GenOf(r) IN
  /\ rbase[r] # None
  /\ StartKnown(g)
  /\ Full(r)
  /\ rbound[g] = None
  /\ rbound' = [rbound EXCEPT ![g] = [v |-> Bound(StateOf(r)), done |-> ~Partial]]
  /\ UNCHANGED <<top, rend, rret, rtrim, bgen, ldr, acc, rep>>

BoundDone(p) ==
  /\ rbound[p] # None /\ ~rbound[p].done
  /\ rbound' = [rbound EXCEPT ![p] = [@ EXCEPT !.done = TRUE]]
  /\ UNCHANGED <<top, rend, rret, rtrim, bgen, ldr, acc, rep>>

BoundLost(p) ==
  /\ rbound[p] # None /\ ~rbound[p].done
  /\ rbound' = [rbound EXCEPT ![p] = None]
  /\ UNCHANGED <<top, rend, rret, rtrim, bgen, ldr, acc, rep>>

\* A replica of g starts from the boundary state of g - 1, one time: so
\* `RolesTrace` refuses a base taken twice (`BaseOnce`).
TakeBase(r) ==
  LET g == GenOf(r) IN
  /\ g >= 2 /\ g <= top
  /\ rbase[r] = None
  /\ IF BoundAct = "done" THEN Done(rbound[g - 1]) ELSE rbound[g - 1] # None
  /\ StartKnown(g)
  /\ rbase' = [rbase EXCEPT ![r] = rbound[g - 1].v]
  /\ rstart' = [rstart EXCEPT ![r] = Start(g)]
  /\ rcomp' = [rcomp EXCEPT ![r] = Start(g)]
  /\ UNCHANGED <<reg, ldr, acc, rapp, acked, resolved, rdrop, rfloor, trims>>

(***************************************************************************)
(* Resolve (section J2.2, #1320). A client whose fenced write got `Below`  *)
(* sends Resolve(uuid, expected_seq, write) to a replica of the segment    *)
(* that holds the position. A sealed segment does not change, so the       *)
(* replica judges the resend with the compare of apply, once its fold      *)
(* covers the position: written ("dup") when the record there is this      *)
(* write of this uuid, else not written ("taken"). A replica whose fold    *)
(* has first_seq above the position answers "trunc" (`Truncated`).         *)
(***************************************************************************)
\* The answer of a fold st to the Resolve of the write v. `FoldTrace.tla`
\* checks the code against it.
ResolveOn(st, v) ==
  IF v.e < st.first THEN "trunc"
  ELSE IF Resolve = "uuid"
       THEN IF \E x \in st.recs : x[1] = v.e /\ x[2] = v.u THEN "dup" ELSE "taken"
       ELSE IF <<v.e, v.u, v.id>> \in st.recs THEN "dup" ELSE "taken"

ResolveAt(r, v) == ResolveOn(StateOf(r), v)

\* The replica can judge: its fold is below the position (truncated), or
\* it is a replica of the segment of the position and its fold covers it.
\* The fold-only part: a fold st that started at base can judge position e.
CoversOn(base, st, e) ==
  \/ e < st.first
  \/ /\ base.next <= e
     /\ IF Resolve = "early" THEN TRUE ELSE e < st.next

Covers(r, e) ==
  /\ rbase[r] # None
  /\ StartKnown(GenOf(r))
  /\ ~rtrim[GenOf(r)]
  /\ ~Blind(r)
  /\ CoversOn(rbase[r], StateOf(r), e)

ResolveBelow(s, r) ==
  /\ acked[s] = "below"
  /\ resolved[s] = None
  /\ Covers(r, ChosenVal(s).e)
  /\ resolved' = [resolved EXCEPT ![s] = ResolveAt(r, ChosenVal(s))]
  /\ UNCHANGED <<reg, ldr, acc, rapp, rbase, rstart, acked, rdrop, rcomp, rfloor, trims>>

(***************************************************************************)
(* Retire and trim (sections J3.1, J3.2, J3.4).                            *)
(***************************************************************************)
Holders(p) == {r \in RepOf[p] : rbase[r] # None /\ Done(rend[p]) /\ Front(r) = rend[p].v}

\* Only the first leader of p + 1 writes the end of p, so a done end has a
\* successor (p < top).
Retire(p) ==
  /\ ~rret[p]
  /\ Done(rend[p])
  /\ RetireRule # "off"
  /\ IF RetireRule = "full"
     THEN Done(rbound[p]) /\ Cardinality(Holders(p)) >= RetireAt
     ELSE TRUE
  /\ rret' = [rret EXCEPT ![p] = TRUE]
  /\ UNCHANGED <<top, rend, rbound, rtrim, bgen, ldr, acc, rep>>

\* A replica whose fold has first_seq at or above the last seq of segment p
\* writes "trimmed" into the record of p. The replicas of p then drop it.
Trim(p, r) ==
  LET last == IF Done(rbound[p]) THEN rbound[p].v.next
              ELSE StateOf(CHOOSE q \in RepOf[p] : rbase[q] # None).next
  IN
  /\ ~rtrim[p]
  /\ rbase[r] # None
  /\ StartKnown(GenOf(r))
  /\ IF TrimRule = "sealed" THEN Done(rend[p]) /\ Done(rbound[p])
     ELSE p = InForce /\ \E q \in RepOf[p] : rbase[q] # None
  /\ StateOf(r).first >= last
  /\ rtrim' = [rtrim EXCEPT ![p] = TRUE]
  /\ UNCHANGED <<top, rend, rbound, rret, bgen, ldr, acc, rep>>

\* A replica of a trimmed segment learns the trim and drops its copy
\* (`SegmentReplica::observe`): it applies nothing after it.
Drop(r) ==
  /\ rtrim[GenOf(r)]
  /\ ~rdrop[r]
  /\ rdrop' = [rdrop EXCEPT ![r] = TRUE]
  /\ UNCHANGED <<reg, ldr, acc, rapp, rbase, rstart, acked, resolved, rcomp, rfloor, trims>>

Next ==
  \/ Reshape
  \/ \E b \in Ballots : Register(b) \/ Phase1(b) \/ SealWrite(b) \/ Open(b)
  \/ \E b \in Ballots, g \in Live : StaleRegister(b, g)
  \/ \E b \in Ballots, s \in Slots, v \in Cmds : Propose(b, s, v)
  \/ \E a \in Acceptors :
       \/ Flush(a) \/ Crash(a)
       \/ \E b \in Ballots : Promise(a, b) \/ \E s \in Slots : Accept(a, b, s)
  \/ \E p \in Gens : EndDone(p) \/ EndLost(p) \/ BoundDone(p) \/ BoundLost(p) \/ Retire(p)
  \/ \E r \in Replicas : Apply(r) \/ BoundWrite(r) \/ TakeBase(r) \/ Drop(r)
  \/ \E p \in Gens, r \in Replicas : Trim(p, r)
  \/ \E s \in Slots, r \in Replicas : ResolveBelow(s, r)

Spec == Init /\ [][Next]_vars

\* `Next` with a leader that takes a later register value before its
\* Phase 1 (`Reread`).
NextLate == Next \/ \E b \in Ballots : Reread(b)
SpecLate == Init /\ [][NextLate]_vars
\* Reach: a ballot can take a later value that changes the ends it reads.
NoLateRead == \A b \in Ballots : ~ENABLED (Reread(b) /\ bE' # bE)

\* `Next` with the answers that acceptors send again (`Repromise`).
NextRe == Next \/ \E a \in Acceptors, b \in Ballots : Repromise(a, b)
SpecRe == Init /\ [][NextRe]_vars

(***************************************************************************)
(* Safety.                                                                 *)
(***************************************************************************)
\* Paxos: at most one value is chosen at each slot.
Agreement == \A s \in Slots, v, w \in Cmds : Chosen(s, v) /\ Chosen(s, w) => v = w

\* The grid of a sealed configuration chooses nothing at or above its end.
SealedFrozen == \A p \in Gens, s \in Slots :
  Done(rend[p]) /\ s >= rend[p].v => ~ChosenInGen(s, p)

\* Each answer is the answer of the fold of the whole log, or `Below` where
\* the whole log says `Duplicate` or position taken (a resend across a seal,
\* not final).
AckTrue == \A s \in Slots : acked[s] # None =>
  /\ s < Frontier
  /\ \/ acked[s] = TrueRes(s)
     \/ acked[s] = "below" /\ TrueRes(s) \in {"dup", "taken"}

\* The answer to a Resolve is the answer of the fold of the whole log at the
\* slot of the write ("dup": written, "taken": not written), or `Truncated`
\* once the floor of the journal is above the position.
ResolveTrue == \A s \in Slots : resolved[s] # None =>
  \/ resolved[s] = TrueRes(s)
  \/ resolved[s] = "trunc" /\ ChosenVal(s).e < Truth(Frontier).first

\* Each replica's fold is the fold of the whole log at its front.
OneFold == \A r \in Replicas : rbase[r] # None =>
  /\ Front(r) <= Frontier
  /\ LET t == StateOf(r) IN
     /\ t.next = Truth(Front(r)).next
     /\ t.first = Truth(Front(r)).first
     /\ t.leader = Truth(Front(r)).leader

\* A boundary write that is done holds the journal state at the end.
RecordTrue == \A g \in Gens : Done(rbound[g]) /\ Done(rend[g]) =>
  /\ rend[g].v <= Frontier
  /\ rbound[g].v = Bound(Truth(rend[g].v))

\* `next_seq` and `first_seq` never move back across the seals.
NoMoveBack == \A g \in 2..G : Done(rbound[g]) /\ Done(rbound[g - 1]) =>
  /\ rbound[g].v.next >= rbound[g - 1].v.next
  /\ rbound[g].v.first >= rbound[g - 1].v.first

\* At most one older configuration is not retired.
OneUnretired == Cardinality({g \in 1..top : ~rret[g]}) <= 2

\* Every record at or above the floor has a live copy: a quorum of a grid
\* that still runs holds its vote on disk, or a replica of its segment
\* applied it and the segment is not trimmed.
HeldBy(s) == \E r \in Replicas :
  /\ ~rtrim[GenOf(r)]
  /\ StartKnown(GenOf(r))
  /\ Holds(r, s)
Readable == \A s \in Slots :
  s < Frontier /\ Record(s) /\ Truth(s).next >= Truth(Frontier).first =>
    OnDisk(s) \/ HeldBy(s)

\* A written seal end never changes.
EndOnce == [][\A p \in Gens : Done(rend[p]) => rend'[p] = rend[p]]_vars
\* A replica that holds its base takes no other: a trace that takes one base
\* two times is refused at the copy.
BaseOnce == \A r \in Replicas : rbase[r] # None => ~ENABLED TakeBase(r)

\* An end is written only once the start of its segment is known.
EndAfterStart == \A p \in Gens : rend[p] # None => StartKnown(p)

\* A leader that sealed writes from its own end: the end that its Phase 1
\* chose (`Open`).
SealerStart == \A b \in Ballots : bst[b] = "w" /\ bend[b] # None => bstart[b] = bend[b]

\* A boundary state written at a majority never changes.
BoundOnce == [][\A p \in Gens : Done(rbound[p]) => rbound'[p] = rbound[p]]_vars

\* A `Resolve` only reads: a step that answers one changes nothing else, not
\* the register (the seal end, the boundary state), not a vote, and not the
\* log of a replica. The sealed segment stays as it was.
ResolveReadOnly == [][(\E s \in Slots : resolved'[s] # resolved[s]) =>
                        UNCHANGED <<reg, ldr, acc, rapp, rbase, rstart, acked, rdrop>>]_vars

\* A trimmed record stays trimmed.
TrimStays == [][\A p \in Gens : rtrim[p] => rtrim'[p]]_vars

\* An acceptor raises its promise to b only while its configuration runs
\* (the acceptors of a retired configuration stop) and only when b reads
\* that configuration: the leader of b sends its `Prepare` and its `Accept`
\* to the configurations of `H_b` alone.
PromiseRead == [][\A a \in Acceptors :
                    mprom'[a] > mprom[a] =>
                      /\ a \in Active
                      /\ \E h \in bH[mprom'[a]] : a \in CfgOf[h]]_vars

(***************************************************************************)
(* Liveness. Every step is bounded; with weak fairness the journal runs    *)
(* until no step is enabled. Then the chosen log has no hole, and every    *)
(* chosen slot is applied and answered.                                    *)
(***************************************************************************)
LiveSpec == Spec /\ WF_vars(Next)

NoHole == \A s \in Slots : IsChosen(s) => \A t \in 0..s : IsChosen(t)
Ends == <>[](NoHole /\ \A s \in Slots : IsChosen(s) => acked[s] # None)

(***************************************************************************)
(* Reach: each must be violated, so that a passing check is not empty.     *)
(***************************************************************************)
\* A crash between two writes of one batch, and a value chosen after it.
NoCrashMid == ~(crashes > 0 /\ \E s \in Slots : IsChosen(s))
\* No acceptor sent two different answers at one ballot: `SpecRe` breaks it
\* once an answer sent again carries a vote that the first answer did not.
OnePromise == \A m1, m2 \in p1b : m1.a = m2.a /\ m1.b = m2.b => m1 = m2
\* Three segments: two seals done, a write acked in the third.
NoThird == ~(G >= 3 /\ Done(rend[2]) /\ \E s \in Slots :
               s >= rend[2].v /\ acked[s] = "ok")
\* A resend across a seal answered `Below`.
NoBelow == \A s \in Slots : acked[s] # "below"
\* A resend across a seal answered `Below`, then resolved as written.
NoResolveDup == \A s \in Slots : resolved[s] # "dup"
\* A seal end lost, and a different end written.
NoLostEnd == ~\E b, c \in Ballots :
  /\ bend[b] # None /\ bend[c] # None /\ bend[b] # bend[c]
  /\ bgen[b] = bgen[c] /\ bst[b] = "p1" /\ Done(rend[bgen[c] - 1])
  /\ rend[bgen[c] - 1].v = bend[c]
\* A slot of an old segment chosen in the grid of the next configuration.
NoRepropose == ~\E s \in Slots, g \in 2..G :
  Done(rend[g - 1]) /\ s < rend[g - 1].v /\ ChosenInGen(s, g)
\* A segment retired and then trimmed.
NoTrimRetired == ~\E p \in Gens : rret[p] /\ rtrim[p]
\* A `SetLeader` in an earlier segment, and the deposed writer refused in a
\* later one.
NoMoved == ~\E s1, s2 \in Slots, g \in 2..G :
  /\ acked[s1] = "ok" /\ ChosenVal(s1) # Noop /\ ChosenVal(s1).op = "sl"
  /\ Done(rend[g - 1]) /\ s1 < rend[g - 1].v /\ s2 >= rend[g - 1].v
  /\ acked[s2] = "fenced"
=============================================================================
