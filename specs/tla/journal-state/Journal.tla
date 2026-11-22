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
(*                        write at the position e) or None (unfenced).     *)
(*   [op "tr", u, up]     a `Truncate` to the position up, under u.        *)
(*   [op "sl", new, old]  `SetLeader(new, old)`; new may be None.          *)
(* The fold of a replica is (leader, next, first, recs): recs holds the    *)
(* positions written in this segment, with their write. Every command is   *)
(* judged at apply (section J2.2, J2.4.1).                                 *)
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
(*            the seal.                                                    *)
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
  L0,                     \* the leader of a fresh journal: Unset or None
  None, Unset, Noop,
  G, NS, NB, MaxCrash, RetireAt,
  Partial,                \* TRUE: a register write can be partial and lost
  GenOK,                  \* the pairs <<ballot, configuration>> that may register
  Reply1b, Reply2b, WriteOrder, EndRule, EndAct, BoundAct, GapFill,
  RetireRule, ReshapeGuard, Lookup, Resolve, TrimRule

ASSUME G \in Nat /\ G >= 1 /\ NS \in Nat /\ NS >= 1 /\ NB \in Nat /\ NB >= 1
ASSUME MaxCrash \in Nat /\ RetireAt \in Nat /\ RetireAt >= 1
ASSUME L0 \in {Unset, None} /\ Partial \in BOOLEAN
ASSUME Reply1b \in {"disk", "mem"} /\ Reply2b \in {"disk", "mem"}
ASSUME WriteOrder \in {"atomic", "prom_first", "vote_first"}
ASSUME EndRule \in {"voted", "applied"}
ASSUME EndAct \in {"done", "read"} /\ BoundAct \in {"done", "read"}
ASSUME GapFill \in BOOLEAN /\ ReshapeGuard \in BOOLEAN
ASSUME RetireRule \in {"full", "early"}
ASSUME Lookup \in {"below", "local"}
ASSUME Resolve \in {"write", "uuid", "early"}
ASSUME TrimRule \in {"sealed", "open"}

Gens == 1..G
Slots == 0..(NS - 1)
Ballots == 1..NB
Replicas == UNION {RepOf[g] : g \in Gens}
ASSUME \A g \in Gens : CfgOf[g] \subseteq Acceptors /\ QOf[g] \subseteq SUBSET CfgOf[g]
ASSUME \A g \in Gens : \A Q1, Q2 \in QOf[g] : Q1 \cap Q2 # {}
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
  \* Acceptors: memory, the queue of writes, disk.
  mprom, mvote, pend, dprom, dvote,
  crashes, \* the crashes so far
  p1b,     \* the promises sent: [a, b, votes]
  sent2b,  \* the votes sent: <<s, b, v, a>>
  \* Replicas, on disk.
  rapp,    \* replica -> how many slots of its segment it applied
  rbase,   \* replica -> the boundary state it started from, or None
  rstart,  \* replica -> the start slot of its segment when it started, or None
  acked,   \* slot -> the answer that a replica gave, or None
  resolved \* slot -> the answer to the Resolve of a `Below`, or None

reg == <<top, rend, rbound, rret, rtrim, bgen>>
ldr == <<bst, bH, bE, bsafe, bend, blo, bstart, prop>>
acc == <<mprom, mvote, pend, dprom, dvote, crashes, p1b, sent2b>>
rep == <<rapp, rbase, rstart, acked, resolved>>
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
  bgen[b] # 0 /\ \E Q \in QOf[bgen[b]] : \A a \in Q : <<s, b, v, a>> \in sent2b
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
    [] v.op = "w" -> IF st.leader = None THEN "ok" ELSE "wrong"
    [] v.op = "tr" -> IF v.u = st.leader THEN "ok" ELSE "fenced"
    [] v.op = "sl" -> IF v.old = st.leader /\ v.new # st.leader THEN "ok" ELSE "refused"

Step(base, st, v) ==
  LET r == Res(base, st, v) IN
  IF r # "ok" THEN st
  ELSE CASE v.op = "w" ->
              [st EXCEPT !.next = @ + 1,
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

(***************************************************************************)
(* Replicas.                                                               *)
(***************************************************************************)
GenOf(r) == CHOOSE g \in Gens : r \in RepOf[g]
Front(r) == rstart[r] + rapp[r]
StateOf(r) == FoldFrom(rbase[r], rbase[r], rstart[r], Front(r))
\* The replica applied every slot of its sealed segment.
Full(r) == LET g == GenOf(r) IN EndKnown(g) /\ Front(r) = rend[g].v

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
  /\ p1b = {}
  /\ sent2b = {}
  /\ rapp = [r \in Replicas |-> 0]
  /\ rbase = [r \in Replicas |-> IF r \in RepOf[1] THEN Empty ELSE None]
  /\ rstart = [r \in Replicas |-> IF r \in RepOf[1] THEN 0 ELSE None]
  /\ acked = [s \in Slots |-> None]
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

\* Ballot b registers the newest configuration, in ballot order. The
\* leader takes what its Phase 1 reads from the register value of its
\* campaign, and keeps it (`JournalLeader::campaign`): the configurations
\* that are not retired, the end of each one that is sealed, and the first
\* slot it writes, the applied prefix of the oldest configuration it reads
\* (its replicas hold every slot below). A retire or a seal after the
\* campaign does not change them: the leader of a retired configuration's
\* acceptors waits, and a seal by another first leader stops this one.
\* A ballot registers once, above every registered ballot: the second guard
\* also refuses b itself once it registered.
Register(b) ==
  LET m == Min(H(top)) IN
  /\ \A c \in Ballots : bgen[c] # 0 => c < b
  /\ <<b, top>> \in GenOK
  /\ bgen' = [bgen EXCEPT ![b] = top]
  /\ bH' = [bH EXCEPT ![b] = H(top)]
  /\ bE' = [bE EXCEPT ![b] = [h \in Gens |-> IF Done(rend[h]) THEN rend[h].v ELSE None]]
  /\ blo' = [blo EXCEPT ![b] =
       Max({Start(m)} \cup {Front(r) : r \in {r \in RepOf[m] : rbase[r] # None}})]
  /\ UNCHANGED <<top, rend, rbound, rret, rtrim, bst, bsafe, bend, bstart, prop, acc, rep>>

\* The slots of h that the Phase 1 of b reads: below the end, once the end
\* was done at its Register.
ReadSet(b, h) == IF bE[b][h] # None THEN 0..(bE[b][h] - 1) ELSE Slots

\* The reports of Phase 1 of b from the acceptors of Q: <<a, votes>>. With
\* `Direct`, the promises are durable in the step, so Phase 1 takes them from
\* the acceptors in one step (as `experiments/segments/a/`); else from the
\* promises that the acceptors sent.
Reports(b, Q) ==
  IF Direct THEN {<<a, mvote[a]>> : a \in Q}
  ELSE {<<m.a, m.votes>> : m \in {m \in p1b : m.b = b /\ m.a \in Q}}
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
          top1(s) == CHOOSE x \in voted(s) : \A y \in voted(s) : y[1] <= x[1]
          \* The slots with a vote in the old grid.
          old == IF g >= 2 /\ g - 1 \in bH[b]
                 THEN {s \in Slots : \E x \in Reports(b, Qs[g - 1]) : x[2][s][1] > 0}
                 ELSE {}
          all == UNION {Qs[h] : h \in bH[b]}
      IN /\ bsafe' = [bsafe EXCEPT ![b] =
              [s \in Slots |-> IF voted(s) # {} THEN top1(s)[2] ELSE None]]
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
  /\ UNCHANGED <<reg, bH, bE, blo, prop, bstart, mvote, pend, dvote, crashes, p1b, sent2b, rep>>

Phase1(b) == \E Qs \in [bH[b] -> UNION {QOf[h] : h \in Gens}] : Phase1On(b, Qs)

(***************************************************************************)
(* The seal (section J3.1): the first leader of g writes the end of g - 1  *)
(* by compare-and-set from "no end". Its read may miss a partial write of  *)
(* another sealer, and then it writes over it.                             *)
(***************************************************************************)
\* The code writes the seal in Phase 1; the model also lets a leader that
\* already writes seal again. Only its Phase 1 sets `bend[b]`, so each
\* write is still the end that its Phase 1 read.
SealWrite(b) ==
  LET g == bgen[b] IN
  /\ bend[b] # None
  /\ StartKnown(g - 1)
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
  /\ UNCHANGED <<reg, bH, bE, bsafe, bend, blo, prop, acc, rep>>

(***************************************************************************)
(* Phase 2. The leader of b writes every slot of its segment, and below    *)
(* its start each slot from the first slot that its Phase 1 read: the vote *)
(* that its Phase 1 saw, or a `Noop` (the gap fill, #1233).                *)
(***************************************************************************)
Writable(b) ==
  {s \in Slots : /\ s >= blo[b]
                 /\ \/ s >= bstart[b]
                    \/ bsafe[b][s] # None
                    \/ GapFill}

\* One above the highest slot where the Phase 1 of b saw a vote.
TopSeen(b) == Max({0} \cup {s + 1 : s \in {s \in Slots : bsafe[b][s] # None}})

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
  /\ UNCHANGED <<reg, bst, bH, bE, bsafe, bend, blo, bstart, acc, rep>>

(***************************************************************************)
(* Acceptors: a step changes memory and queues its writes. A write is      *)
(* <<kind, s, b, v, send>>: kind "p" (promise b), "v" (vote <<b, v>> at s) *)
(* or "pv" (both). send tells whether the write's flush sends the reply.   *)
(***************************************************************************)
\* `bH[b]` is empty until b registers, so only a registered ballot is promised.
Promise(a, b) ==
  /\ ~Direct
  /\ a \in Active
  /\ \E h \in bH[b] : a \in CfgOf[h]
  /\ mprom[a] < b
  /\ mprom' = [mprom EXCEPT ![a] = b]
  /\ CASE Reply1b = "mem" ->
          /\ p1b' = p1b \cup {[a |-> a, b |-> b, votes |-> mvote[a]]}
          /\ pend' = [pend EXCEPT ![a] = Append(@, <<"p", 0, b, None, FALSE>>)]
          /\ UNCHANGED dprom
       [] OTHER ->
          /\ pend' = [pend EXCEPT ![a] = Append(@, <<"p", 0, b, None, TRUE>>)]
          /\ UNCHANGED <<p1b, dprom>>
  /\ UNCHANGED <<reg, ldr, mvote, dvote, crashes, sent2b, rep>>

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
  /\ UNCHANGED <<reg, ldr, crashes, p1b, rep>>

\* The driver makes the first queued write durable, and sends its reply.
Flush(a) ==
  LET w == Head(pend[a])
      k == w[1]
      dp == IF k \in {"p", "pv"} THEN w[3] ELSE dprom[a]
      dv == IF k \in {"v", "pv"} THEN [dvote[a] EXCEPT ![w[2]] = <<w[3], w[4]>>] ELSE dvote[a]
  IN
  /\ pend[a] # <<>>
  /\ dprom' = [dprom EXCEPT ![a] = dp]
  /\ dvote' = [dvote EXCEPT ![a] = dv]
  /\ pend' = [pend EXCEPT ![a] = Tail(@)]
  /\ p1b' = IF k = "p" /\ w[5] THEN p1b \cup {[a |-> a, b |-> w[3], votes |-> dv]} ELSE p1b
  /\ sent2b' = IF k \in {"v", "pv"} /\ w[5] THEN sent2b \cup {<<w[2], w[3], w[4], a>>} ELSE sent2b
  /\ UNCHANGED <<reg, ldr, mprom, mvote, crashes, rep>>

\* A crash between two writes: the queue is lost, and the role starts again
\* from its disk.
Crash(a) ==
  /\ crashes < MaxCrash
  /\ crashes' = crashes + 1
  /\ mprom' = [mprom EXCEPT ![a] = dprom[a]]
  /\ mvote' = [mvote EXCEPT ![a] = dvote[a]]
  /\ pend' = [pend EXCEPT ![a] = <<>>]
  /\ UNCHANGED <<reg, ldr, dprom, dvote, p1b, sent2b, rep>>

\* A quorum of a grid that still runs holds a chosen vote of slot s on disk.
\* (A later ballot can only vote the chosen value again, so a quorum may hold
\* it at several ballots of its grid.)
OnDisk(s) == IsChosen(s) /\ \E g \in Live : \E Q \in QOf[g] : \A a \in Q :
  /\ dvote[a][s][1] > 0
  /\ bgen[dvote[a][s][1]] = g
  /\ dvote[a][s][2] = ChosenVal(s)

(***************************************************************************)
(* Replicas (sections J3.1, J3.4).                                         *)
(***************************************************************************)
\* A replica applies the next chosen slot of its segment and answers its
\* writer. It stops at the end of its segment, once the end is known.
Apply(r) ==
  LET g == GenOf(r)
      s == Front(r)
  IN
  /\ g <= top
  /\ rbase[r] # None
  /\ StartKnown(g)
  /\ s < NS
  /\ IF EndKnown(g) THEN s < rend[g].v ELSE TRUE
  /\ ~rtrim[g]
  \* It learns the slot from a live source: a quorum of a grid that still
  \* runs holds the vote on its disk, or a peer of its set applied it.
  \* Both sources hold a chosen slot only (`OnDisk` and a peer's `Apply`).
  /\ OnDisk(s) \/ \E q \in RepOf[g] \ {r} : rbase[q] # None /\ Front(q) > s
  /\ rapp' = [rapp EXCEPT ![r] = @ + 1]
  /\ acked' = [acked EXCEPT ![s] = Res(rbase[r], StateOf(r), ChosenVal(s))]
  /\ UNCHANGED <<reg, ldr, acc, rbase, rstart, resolved>>

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

\* A replica of g starts from the boundary state of g - 1. A second take
\* changes nothing: every boundary write of g - 1 is the state of a full
\* replica, and the full replicas of a segment hold one state.
TakeBase(r) ==
  LET g == GenOf(r) IN
  /\ g >= 2 /\ g <= top
  /\ IF BoundAct = "done" THEN Done(rbound[g - 1]) ELSE rbound[g - 1] # None
  /\ StartKnown(g)
  /\ rbase' = [rbase EXCEPT ![r] = rbound[g - 1].v]
  /\ rstart' = [rstart EXCEPT ![r] = Start(g)]
  /\ UNCHANGED <<reg, ldr, acc, rapp, acked, resolved>>

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
  /\ CoversOn(rbase[r], StateOf(r), e)

ResolveBelow(s, r) ==
  /\ acked[s] = "below"
  /\ resolved[s] = None
  /\ Covers(r, ChosenVal(s).e)
  /\ resolved' = [resolved EXCEPT ![s] = ResolveAt(r, ChosenVal(s))]
  /\ UNCHANGED <<reg, ldr, acc, rapp, rbase, rstart, acked>>

(***************************************************************************)
(* Retire and trim (sections J3.1, J3.2, J3.4).                            *)
(***************************************************************************)
Holders(p) == {r \in RepOf[p] : rbase[r] # None /\ Done(rend[p]) /\ Front(r) = rend[p].v}

Retire(p) ==
  /\ p < top
  /\ ~rret[p]
  /\ Done(rend[p])
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

Next ==
  \/ Reshape
  \/ \E b \in Ballots : Register(b) \/ Phase1(b) \/ SealWrite(b) \/ Open(b)
  \/ \E b \in Ballots, s \in Slots, v \in Cmds : Propose(b, s, v)
  \/ \E a \in Acceptors :
       \/ Flush(a) \/ Crash(a)
       \/ \E b \in Ballots : Promise(a, b) \/ \E s \in Slots : Accept(a, b, s)
  \/ \E p \in Gens : EndDone(p) \/ EndLost(p) \/ BoundDone(p) \/ BoundLost(p) \/ Retire(p)
  \/ \E r \in Replicas : Apply(r) \/ BoundWrite(r) \/ TakeBase(r)
  \/ \E p \in Gens, r \in Replicas : Trim(p, r)
  \/ \E s \in Slots, r \in Replicas : ResolveBelow(s, r)

Spec == Init /\ [][Next]_vars

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
  /\ rbase[r] # None
  /\ ~rtrim[GenOf(r)]
  /\ StartKnown(GenOf(r))
  /\ rstart[r] <= s /\ s < Front(r)
Record(s) == IsChosen(s) /\ ChosenVal(s) # Noop /\ ChosenVal(s).op = "w" /\ TrueRes(s) = "ok"
Readable == \A s \in Slots :
  s < Frontier /\ Record(s) /\ Truth(s).next >= Truth(Frontier).first =>
    OnDisk(s) \/ HeldBy(s)

\* A written seal end never changes.
EndOnce == [][\A p \in Gens : Done(rend[p]) => rend'[p] = rend[p]]_vars
\* A boundary state written at a majority never changes.
BoundOnce == [][\A p \in Gens : Done(rbound[p]) => rbound'[p] = rbound[p]]_vars

\* A `Resolve` only reads: a step that answers one changes nothing else, not
\* the register (the seal end, the boundary state), not a vote, and not the
\* log of a replica. The sealed segment stays as it was.
ResolveReadOnly == [][(\E s \in Slots : resolved'[s] # resolved[s]) =>
                        UNCHANGED <<reg, ldr, acc, rapp, rbase, rstart, acked>>]_vars

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
