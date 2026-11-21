----------------------------- MODULE Compartments -----------------------------
(***************************************************************************)
(* The other roles of Compartmentalized Paxos as `paros-consensus` builds  *)
(* them: the batcher and the unbatcher (`batcher.rs`), the replica's walk  *)
(* and journal fold (`replica.rs`) and the read replica (`replica_node.rs`, *)
(* its leaderless read `quorum_read.rs`). Rework 6.10, #1190 (TLA+ model   *)
(* compartments).                                                          *)
(*                                                                         *)
(* The log under the roles is abstract. `multi-paxos/` checks it: one      *)
(* value for each slot (`Agreement`) and one proposal for each slot and    *)
(* ballot (`OneProposal`). Here a slot holds one proposal, a Phase-2       *)
(* quorum chooses it, and a leader change keeps each open slot at its slot *)
(* (the recovery re-proposes the highest reported vote), fills it with a   *)
(* `Noop` (no promise reported it), or frees the open slots above its      *)
(* frontier (no promise reported them, so the new leader allocates them    *)
(* again). These are the outcomes that `multi-paxos/` allows.              *)
(*                                                                         *)
(* What the model has, and the code it follows:                            *)
(*   - The batcher (`Batcher`): one lane of the journal. A write is pushed *)
(*     into the lane while the lane fits the batcher's record limit, and   *)
(*     the lane flushes as one batch at any time (`flush_records`, the     *)
(*     tick). A batch is one unfenced `Write` request to the leader. The   *)
(*     transport delivers a request at most one time, or, with `Copies`,   *)
(*     a few more times.                                                   *)
(*   - The leader refuses a merged batch over its record limit at the edge *)
(*     (`TooLarge`), before it allocates a slot. The batcher lowers its    *)
(*     limit (`lower_limits`) and sends each write of the batch again,     *)
(*     alone (docs/architecture/02-data-plane.md, section 2.4).            *)
(*   - Else the leader allocates the frontier slot and holds the answer.   *)
(*     It answers once the slot and every slot below it are chosen, with  *)
(*     the `seq` that its fold gives the batch, and only when the command *)
(*     chosen at the slot is the batch it proposed there. Only the leader *)
(*     that received the batch holds its answer: a new leader answers     *)
(*     nothing.                                                           *)
(*   - A deposed leader stays up and still holds its answers (#1232 (held *)
(*     writes)). It answers a held batch `Unknown` at any time, or `ok`   *)
(*     when the command chosen at the slot is the batch it proposed       *)
(*     there. It never answers a held batch `NotLeader`. The batcher      *)
(*     treats `Unknown` as an ambiguous batch. A single writer resolves   *)
(*     an `Unknown` with a resend at the same `expected_seq`; that half   *)
(*     is checked in `multi-writer/` (`AckTrue`, `RefusalFinal`).          *)
(*   - The unbatcher (`Unbatcher::scatter`) cuts the answer in batch       *)
(*     order: each write gets its own dense range.                         *)
(*   - A batch with no answer is ambiguous: the batcher answers each write *)
(*     unanswered and never sends the batch again (section 2.4).           *)
(*   - The leader sends the `Accept`s of an open slot again when they are  *)
(*     lost (`resend_pending`).                                            *)
(*   - A replica learns a chosen slot from a `Commit` or a catch-up, walks *)
(*     its chosen prefix in slot order and folds each slot into the        *)
(*     journal state (`Replica::advance`, `JournalState`): a write gets    *)
(*     `[next_seq, next_seq + n)` at apply.                                *)
(*   - A replica restart keeps what it learned (`WriteOp::Learned`)      *)
(*     and its chosen index as far as it reached the disk (the relaxed     *)
(*     `SetChosenIndex`). The boot folds the state again from that prefix  *)
(*     (`Replica::from_boot`, `refold`).                                   *)
(*   - A read replica is not an acceptor: it never votes and counts in no  *)
(*     quorum. Its read (`ReplicaNode::quorum_read_in`) asks acceptors for *)
(*     their vote watermarks, closes on a Phase-1 quorum, and is served    *)
(*     when its own prefix covers the maximum watermark.                   *)
(*                                                                         *)
(* Constants:                                                              *)
(*   Writes, Count  the writes of the writers and the records of each.    *)
(*   BatchMax  the batcher's record limit before it hears the leader's.    *)
(*   LeaderMax the leader's record limit (`max_batch_records`).            *)
(*   Acceptors, Replicas  the acceptors and the read replicas.             *)
(*   Readers   the replicas that serve reads.                              *)
(*   NS        the slots of the log.                                       *)
(*   MaxSends  the ids of the batches. The rule sends each write in one     *)
(*             batch, and again alone after a TooLarge, so a check of the  *)
(*             rule never sends more (`TypeOK`). A control that sends a    *)
(*             batch again bounds its search with `SendBound`.             *)
(*   Leaders   how many leader changes may happen.                         *)
(*   Restarts  how many replica restarts may happen.                       *)
(*   Q2        the Phase-2 quorums. RQ: the sets that close a read.        *)
(*   Resend    "never": an ambiguous batch is never sent again (the code). *)
(*             "ambiguous": a control, the batcher sends it again.         *)
(*   Accepts   TRUE: the leader sends lost `Accept`s again (the code).     *)
(*             FALSE: a control, it does not.                              *)
(*   Walk      "prefix": the walk folds the next slot only (the code).     *)
(*             "skip": a control, it folds any slot it learned.            *)
(*   Boot      "refold": the boot folds the state again (the code).        *)
(*             "keep": a control, the state survives the restart and the   *)
(*             walk starts again at the chosen index on disk.              *)
(*   Read      "quorum": the leaderless read (the code).                   *)
(*             "local": a control, the replica serves from its prefix.     *)
(*   Copies    how many times in a run the transport may deliver a batch   *)
(*             one more time (0: at most one time, as in the code's       *)
(*             contract with the batcher).                                 *)
(*             A copy lands again (multi-writer is at-least-once), so      *)
(*             `OneRange` and `AnswerAgrees` are not rules then: the       *)
(*             answer names one copy. `AckedChosen` and the rest hold.     *)
(*   Held      what a deposed leader answers for a held batch.             *)
(*             "unknown": `Unknown`, or `ok` when the chosen command at   *)
(*             the slot is the batch (the rule, #1232).                    *)
(*             "slot": a control, it answers from the outcome of the slot, *)
(*             whatever command the slot chose.                            *)
(*             "notleader": a control, it answers `NotLeader`, and the     *)
(*             batcher follows the hint and sends the batch again.         *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Writes, Count, BatchMax, LeaderMax, Acceptors, Replicas, Readers,
          NS, MaxSends, Leaders, Restarts, Q2, RQ,
          Resend, Accepts, Walk, Boot, Read, Held, Copies

Slots == 0..(NS - 1)
Ids == 1..MaxSends
Noop == 0
None == MaxSends + 1
\* No open read, no served read.
NoRead == [on |-> FALSE, acks |-> {}, max |-> 0, must |-> {}]
NoServe == [on |-> FALSE, must |-> {}, got |-> {}]
\* A read may ask an acceptor or, in a control, a replica.
Voters == UNION RQ

ASSUME Count \in [Writes -> 1..3]
ASSUME BatchMax \in Nat /\ LeaderMax \in Nat
ASSUME \A w \in Writes : Count[w] <= LeaderMax
ASSUME Readers \subseteq Replicas
ASSUME Acceptors \cap Replicas = {}
ASSUME Voters \subseteq Acceptors \cup Replicas
ASSUME Resend \in {"never", "ambiguous"}
ASSUME Accepts \in BOOLEAN
ASSUME Walk \in {"prefix", "skip"}
ASSUME Boot \in {"refold", "keep"}
ASSUME Read \in {"quorum", "local"}
ASSUME Copies \in Nat
ASSUME Held \in {"unknown", "slot", "notleader"}

VARIABLES
  \* The batcher.
  lane,     \* the writes of the lane, in push order
  pushed,   \* the writes that the writers pushed
  limit,    \* the batcher's record limit
  batches,  \* each batch sent: its writes and its state at the batcher
  \* The network between the batcher and the leader.
  reqs,     \* the batch ids on the way to the leader
  replies,  \* the answers on the way to the batcher
  \* The log, as the leader sees it.
  prop,     \* the proposal at each slot: a batch id, Noop, or None
  open,     \* the open slots whose Accepts are on the way
  chosen,   \* the chosen value at each slot, or None
  frontier, \* the leader's allocator frontier
  held,     \* the answers the leader holds: [s |-> slot, id |-> batch id]
  deposed,  \* the answers that deposed leaders still hold
  copies,   \* the extra deliveries of a batch so far
  changes,  \* the leader changes so far
  wm,       \* each acceptor's vote watermark: one past its highest vote
  \* The writes, as the writers see them.
  ranges,   \* the ranges each write was answered with
  ambiguous,\* the writes answered unanswered
  \* The replicas.
  learned,  \* the slots each replica learned (durable)
  walked,   \* the slots each replica folded, in fold order
  state,    \* each replica's journal state: next seq and the ranges
  disk,     \* each replica's chosen index on disk
  restarts, \* the replica restarts so far
  reading,  \* each replica's open read, or None
  served    \* each replica's last served read, or None

vars == <<lane, pushed, limit, batches, reqs, replies, prop, open, chosen,
          frontier, held, deposed, copies, changes, wm, ranges, ambiguous, learned, walked,
          state, disk, restarts, reading, served>>

Max(a, b) == IF a > b THEN a ELSE b

RECURSIVE SumOf(_)
\* The records of a sequence of writes.
SumOf(ws) == IF ws = <<>> THEN 0 ELSE Count[Head(ws)] + SumOf(Tail(ws))

Range(sq) == {sq[i] : i \in DOMAIN sq}

RECURSIVE SumAt(_)
\* The records that a set of ranges covers.
SumAt(at) == IF at = {} THEN 0
             ELSE LET x == CHOOSE y \in at : TRUE IN x[3] + SumAt(at \ {x})

EmptyState == [next |-> 0, at |-> {}]

\* The fold of one chosen value (`JournalState`): each write of a batch
\* gets its records at the next positions, in batch order.
Apply(st, v) ==
  IF v = Noop THEN st
  ELSE LET ws == batches[v].ws
       IN [next |-> st.next + SumOf(ws),
           at |-> st.at \cup
                  {<<ws[i], st.next + SumOf(SubSeq(ws, 1, i - 1)), Count[ws[i]]>> :
                     i \in DOMAIN ws}]

RECURSIVE FoldFrom(_, _)
\* The fold of the chosen values of a sequence of slots.
FoldFrom(st, sq) == IF sq = <<>> THEN st ELSE FoldFrom(Apply(st, chosen[Head(sq)]), Tail(sq))

\* The slots 0 .. n - 1, in order.
Prefix(n) == [i \in 1..n |-> i - 1]

\* What the unbatcher gives each write of batch id answered at `seq`.
Scatter(id, seq) ==
  LET ws == batches[id].ws
  IN {<<ws[i], seq + SumOf(SubSeq(ws, 1, i - 1)), Count[ws[i]]>> : i \in DOMAIN ws}

Init ==
  /\ lane = <<>>
  /\ pushed = {}
  /\ limit = BatchMax
  /\ batches = <<>>
  /\ reqs = {}
  /\ replies = {}
  /\ prop = [s \in Slots |-> None]
  /\ open = {}
  /\ chosen = [s \in Slots |-> None]
  /\ frontier = 0
  /\ held = {}
  /\ deposed = {}
  /\ copies = 0
  /\ changes = 0
  /\ wm = [a \in Acceptors |-> 0]
  /\ ranges = [w \in Writes |-> {}]
  /\ ambiguous = {}
  /\ learned = [r \in Replicas |-> {}]
  /\ walked = [r \in Replicas |-> <<>>]
  /\ state = [r \in Replicas |-> EmptyState]
  /\ disk = [r \in Replicas |-> 0]
  /\ restarts = 0
  /\ reading = [r \in Replicas |-> NoRead]
  /\ served = [r \in Replicas |-> NoServe]

Unchanged_log == UNCHANGED <<prop, open, chosen, frontier, held, deposed, copies, changes, wm>>
Unchanged_replicas == UNCHANGED <<learned, walked, state, disk, restarts, reading, served>>

\* Send the writes `ws` as one batch.
SendBatch(ws) ==
  /\ batches' = Append(batches, [ws |-> ws, st |-> "sent"])
  /\ reqs' = reqs \cup {Len(batches) + 1}


(***************************************************************************)
(* The batcher and the unbatcher. The batcher may stop for good at any     *)
(* time, with its lane: no step of the batcher is fair, so a behaviour in  *)
(* which it takes no more step is that stop.                               *)
(***************************************************************************)

\* A writer pushes write w into the lane (`Batcher::push`).
Push(w) ==
  /\ w \notin pushed
  /\ SumOf(lane) + Count[w] <= limit
  /\ lane' = Append(lane, w)
  /\ pushed' = pushed \cup {w}
  /\ UNCHANGED <<limit, batches, reqs, replies, ranges, ambiguous>>
  /\ Unchanged_log /\ Unchanged_replicas

\* The lane flushes as one batch (`flush_records` or the tick).
Flush ==
  /\ lane # <<>>
  /\ SendBatch(lane)
  /\ lane' = <<>>
  /\ UNCHANGED <<pushed, limit, replies, ranges, ambiguous>>
  /\ Unchanged_log /\ Unchanged_replicas

\* The batcher hears the leader's answer to batch id (`Next` takes rp from
\* `replies`).
Hear(rp) ==
  /\ replies' = replies \ {rp}
  /\ IF batches[rp.id].st # "sent"
       \* An ambiguous batch was answered already: the late answer is dropped.
       THEN /\ UNCHANGED <<batches, reqs, limit, ranges, ambiguous>>
       ELSE IF rp.kind = "ok"
         THEN /\ batches' = [batches EXCEPT ![rp.id].st = "done"]
              /\ ranges' = [w \in Writes |->
                              ranges[w] \cup {<<x[2], x[3]>> : x \in {y \in Scatter(rp.id, rp.seq) : y[1] = w}}]
              /\ UNCHANGED <<reqs, limit, ambiguous>>
       ELSE IF rp.kind = "unknown"
         \* The batch may have run: each of its writes is answered unanswered.
         THEN /\ batches' = [batches EXCEPT ![rp.id].st = "ambiguous"]
              /\ ambiguous' = ambiguous \cup Range(batches[rp.id].ws)
              /\ UNCHANGED <<reqs, limit, ranges>>
       ELSE IF rp.kind = "notleader"
         \* A control: the batcher follows the hint and sends the batch again.
         THEN /\ batches' = Append([batches EXCEPT ![rp.id].st = "redirected"],
                                   [ws |-> batches[rp.id].ws, st |-> "sent"])
              /\ reqs' = reqs \cup {Len(batches) + 1}
              /\ UNCHANGED <<limit, ranges, ambiguous>>
         \* TooLarge: lower the limit and send each write again, alone.
         ELSE /\ limit' = LeaderMax
              /\ reqs' = reqs \cup {Len(batches) + i : i \in DOMAIN batches[rp.id].ws}
              /\ batches' = [batches EXCEPT ![rp.id].st = "refused"]
                             \o [i \in DOMAIN batches[rp.id].ws |->
                                   [ws |-> <<batches[rp.id].ws[i]>>, st |-> "sent"]]
              /\ UNCHANGED <<ranges, ambiguous>>
  /\ UNCHANGED <<lane, pushed>>
  /\ Unchanged_log /\ Unchanged_replicas

\* No answer came in time: each write of batch id is answered unanswered.
Timeout(id) ==
  /\ id \in DOMAIN batches
  /\ batches[id].st = "sent"
  /\ ambiguous' = ambiguous \cup Range(batches[id].ws)
  /\ IF Resend = "ambiguous"
       THEN /\ batches' = Append([batches EXCEPT ![id].st = "ambiguous"],
                                 [ws |-> batches[id].ws, st |-> "sent"])
            /\ reqs' = reqs \cup {Len(batches) + 1}
       ELSE /\ batches' = [batches EXCEPT ![id].st = "ambiguous"]
            /\ UNCHANGED reqs
  /\ UNCHANGED <<lane, pushed, limit, replies, ranges>>
  /\ Unchanged_log /\ Unchanged_replicas

(***************************************************************************)
(* The leader and the log.                                                 *)
(***************************************************************************)

\* The leader receives batch id.
Receive(id) ==
  /\ id \in reqs
  \* With Copies, another copy of the request stays on the way.
  /\ \/ reqs' = reqs \ {id} /\ UNCHANGED copies
     \/ copies < Copies /\ copies' = copies + 1 /\ UNCHANGED reqs
  /\ IF Len(batches[id].ws) > 1 /\ SumOf(batches[id].ws) > LeaderMax
       \* At the edge: a merged batch over the limit takes no slot.
       THEN /\ replies' = replies \cup {[id |-> id, kind |-> "toolarge", seq |-> 0, from |-> "leader"]}
            /\ UNCHANGED <<prop, open, chosen, frontier, held, deposed, changes, wm>>
       \* NS covers every batch that a check sends (`TypeOK`).
       ELSE /\ prop' = [prop EXCEPT ![frontier] = id]
            /\ open' = open \cup {frontier}
            /\ held' = held \cup {[s |-> frontier, id |-> id]}
            /\ frontier' = frontier + 1
            /\ UNCHANGED <<replies, chosen, deposed, changes, wm>>
  /\ UNCHANGED <<lane, pushed, limit, batches, ranges, ambiguous>>
  /\ Unchanged_replicas

\* A Phase-2 quorum votes for the proposal at slot s: it is chosen. An open
\* slot is not chosen yet (`TypeOK`).
Choose(s) ==
  /\ s \in open
  \* The watermarks matter only to a read: a check with no reader keeps
  \* them at 0, so it does not pay for their states.
  /\ IF Readers = {} THEN UNCHANGED wm
     ELSE \E Q \in Q2 : wm' = [a \in Acceptors |-> IF a \in Q THEN Max(wm[a], s + 1) ELSE wm[a]]
  /\ chosen' = [chosen EXCEPT ![s] = prop[s]]
  /\ open' = open \ {s}
  /\ UNCHANGED <<prop, frontier, held, deposed, copies, changes>>
  /\ UNCHANGED <<lane, pushed, limit, batches, reqs, replies, ranges, ambiguous>>
  /\ Unchanged_replicas

\* The Accepts of slot s are lost (nothing changes when none is on the way).
LoseAccepts(s) ==
  /\ open' = open \ {s}
  /\ UNCHANGED <<prop, chosen, frontier, held, deposed, copies, changes, wm>>
  /\ UNCHANGED <<lane, pushed, limit, batches, reqs, replies, ranges, ambiguous>>
  /\ Unchanged_replicas

\* The leader sends the Accepts of slot s again (`resend_pending`), also when
\* some are still on the way.
ResendAccepts(s) ==
  /\ Accepts
  /\ s < frontier
  /\ chosen[s] = None
  /\ open' = open \cup {s}
  /\ UNCHANGED <<prop, chosen, frontier, held, deposed, copies, changes, wm>>
  /\ UNCHANGED <<lane, pushed, limit, batches, reqs, replies, ranges, ambiguous>>
  /\ Unchanged_replicas

\* A new leader wins Phase 1. Each open slot below its frontier keeps its
\* proposal or gets a Noop; each open slot above it is free again. Every
\* slot that the new leader recovers is sent again. The new leader holds
\* no answer. The old leader stays up and keeps the answers it held.
NewLeader ==
  /\ changes < Leaders
  /\ \E f \in 0..frontier :
       /\ \A s \in f..(frontier - 1) : chosen[s] = None
       /\ \E noop \in SUBSET {s \in 0..(f - 1) : chosen[s] = None} :
            /\ prop' = [s \in Slots |->
                          IF s >= f THEN None
                          ELSE IF s \in noop THEN Noop
                          ELSE prop[s]]
            /\ open' = {s \in 0..(f - 1) : chosen[s] = None}
            /\ frontier' = f
  /\ held' = {}
  /\ deposed' = deposed \cup held
  /\ changes' = changes + 1
  /\ UNCHANGED <<chosen, copies, wm>>
  /\ UNCHANGED <<lane, pushed, limit, batches, reqs, replies, ranges, ambiguous>>
  /\ Unchanged_replicas

\* The slot and every slot below it are chosen.
PrefixChosen(s) == \A k \in 0..s : chosen[k] # None

\* The answer `ok` to the batch of held answer h: the fold of the chosen
\* prefix below its slot gives the batch its seq.
OkReply(h, from) == [id |-> h.id, kind |-> "ok", seq |-> FoldFrom(EmptyState, Prefix(h.s)).next,
                     from |-> from]

\* The leader answers the batch of held answer h (`Next` takes h from `held`)
\* once its slot and every slot below it are chosen. The slot chose the batch
\* that the leader proposed there: the slots of a leader that still holds
\* its answers change only by its own proposals (`HeldOwn`).
Answer(h) ==
  /\ PrefixChosen(h.s)
  /\ held' = held \ {h}
  /\ replies' = replies \cup {OkReply(h, "leader")}
  /\ UNCHANGED <<prop, open, chosen, frontier, deposed, copies, changes, wm>>
  /\ UNCHANGED <<lane, pushed, limit, batches, reqs, ranges, ambiguous>>
  /\ Unchanged_replicas

\* A deposed leader answers the batch of held answer h (#1232).
AnswerDeposed(h) ==
  /\ deposed' = deposed \ {h}
  /\ CASE Held = "unknown" ->
            \/ replies' = replies \cup {[id |-> h.id, kind |-> "unknown", seq |-> 0, from |-> "deposed"]}
            \/ /\ PrefixChosen(h.s)
               /\ chosen[h.s] = h.id
               /\ replies' = replies \cup {OkReply(h, "deposed")}
       [] Held = "slot" ->
            /\ PrefixChosen(h.s)
            /\ replies' = replies \cup {OkReply(h, "deposed")}
       [] Held = "notleader" ->
            replies' = replies \cup {[id |-> h.id, kind |-> "notleader", seq |-> 0, from |-> "deposed"]}
  /\ UNCHANGED <<prop, open, chosen, frontier, held, copies, changes, wm>>
  /\ UNCHANGED <<lane, pushed, limit, batches, reqs, ranges, ambiguous>>
  /\ Unchanged_replicas

\* An answer is lost.
LoseReply(rp) ==
  /\ replies' = replies \ {rp}
  /\ UNCHANGED <<lane, pushed, limit, batches, reqs, ranges, ambiguous>>
  /\ Unchanged_log /\ Unchanged_replicas

(***************************************************************************)
(* The replicas.                                                           *)
(***************************************************************************)

Unchanged_rest == UNCHANGED <<lane, pushed, limit, batches, reqs, replies,
                              ranges, ambiguous>> /\ Unchanged_log

\* Replica r learns chosen slot s (a `Commit` or a catch-up).
Learn(r, s) ==
  /\ chosen[s] # None
  /\ learned' = [learned EXCEPT ![r] = @ \cup {s}]
  /\ UNCHANGED <<walked, state, disk, restarts, reading, served>>
  /\ Unchanged_rest

\* The slot that replica r may fold next.
NextSlots(r) ==
  IF Walk = "prefix"
    THEN {s \in learned[r] : s = Len(walked[r])}
    ELSE learned[r] \ Range(walked[r])

\* Replica r folds one slot (`Replica::advance`).
WalkStep(r) ==
  /\ \E s \in NextSlots(r) :
       /\ walked' = [walked EXCEPT ![r] = Append(@, s)]
       /\ state' = [state EXCEPT ![r] = Apply(@, chosen[s])]
  /\ UNCHANGED <<learned, disk, restarts, reading, served>>
  /\ Unchanged_rest

\* Replica r writes its chosen index (the relaxed `SetChosenIndex`). The index
\* matters only to a restart: a check with no restart keeps it at 0, so it
\* does not pay for its states (as `wm` in `Choose`).
Persist(r) ==
  /\ disk' = [disk EXCEPT ![r] = IF Restarts > 0 THEN Len(walked[r]) ELSE 0]
  /\ UNCHANGED <<learned, walked, state, restarts, reading, served>>
  /\ Unchanged_rest

\* Replica r restarts: it keeps what it learned and the index on disk.
Restart(r) ==
  /\ restarts < Restarts
  /\ restarts' = restarts + 1
  /\ walked' = [walked EXCEPT ![r] = SubSeq(@, 1, disk[r])]
  /\ IF Boot = "refold"
       THEN state' = [state EXCEPT ![r] = FoldFrom(EmptyState, SubSeq(walked[r], 1, disk[r]))]
       ELSE UNCHANGED state
  /\ reading' = [reading EXCEPT ![r] = NoRead]
  /\ UNCHANGED <<learned, disk, served>>
  /\ Unchanged_rest

\* The writes that are complete: answered with a range.
Complete == {w \in Writes : ranges[w] # {}}

\* Replica r opens a read. A read that is open is dropped, as a restart
\* drops it.
ReadStart(r) ==
  /\ r \in Readers
  /\ reading' = [reading EXCEPT ![r] = [on |-> TRUE, acks |-> {}, max |-> 0, must |-> Complete]]
  /\ UNCHANGED <<learned, walked, state, disk, restarts, served>>
  /\ Unchanged_rest

\* The watermark that v answers a read with.
WmOf(v) == IF v \in Acceptors THEN wm[v] ELSE Len(walked[v])

\* Voter v answers the read of replica r (`PreReadAck`). A second answer of v
\* can only raise the maximum.
ReadAck(r, v) ==
  /\ reading[r].on
  /\ reading' = [reading EXCEPT ![r] = [@ EXCEPT !.acks = @ \cup {v},
                                                 !.max = Max(@, WmOf(v))]]
  /\ UNCHANGED <<learned, walked, state, disk, restarts, served>>
  /\ Unchanged_rest

\* Replica r serves its read from its own folded prefix. With no read open,
\* `reading[r]` has no answer, so no quorum closes it, and a control
\* ("local") serves a read that must hold nothing.
ReadServe(r) ==
  /\ \/ Read = "local"
     \/ /\ \E Q \in RQ : Q \subseteq reading[r].acks
        /\ Len(walked[r]) >= reading[r].max
  /\ served' = [served EXCEPT ![r] = [on |-> TRUE, must |-> reading[r].must,
                                      got |-> {x[1] : x \in state[r].at}]]
  /\ reading' = [reading EXCEPT ![r] = NoRead]
  /\ UNCHANGED <<learned, walked, state, disk, restarts>>
  /\ Unchanged_rest

Next ==
  \/ \E w \in Writes : Push(w)
  \/ Flush
  \/ \E rp \in replies : Hear(rp) \/ LoseReply(rp)
  \/ \E id \in Ids : Timeout(id) \/ Receive(id)
  \/ \E s \in Slots : Choose(s) \/ LoseAccepts(s) \/ ResendAccepts(s)
  \/ \E h \in held : Answer(h)
  \/ \E h \in deposed : AnswerDeposed(h)
  \/ NewLeader
  \/ \E r \in Replicas :
       \/ \E s \in Slots : Learn(r, s)
       \/ WalkStep(r) \/ Persist(r) \/ Restart(r)
       \/ ReadStart(r) \/ ReadServe(r)
       \/ \E v \in Voters : ReadAck(r, v)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Liveness. A lost request or Accept is sent again, so it gets through in *)
(* the end (strong fairness on the steps that loss can disable).           *)
(***************************************************************************)
LiveSpec ==
  /\ Spec
  /\ \A id \in Ids : WF_vars(Receive(id))
  /\ \A s \in Slots : SF_vars(Choose(s)) /\ WF_vars(ResendAccepts(s))
  /\ \A r \in Replicas : WF_vars(WalkStep(r)) /\ \A s \in Slots : WF_vars(Learn(r, s))

\* Ends: every batch that the batcher sent reaches the log, and every slot
\* below the frontier is chosen and folded at every replica, also when the
\* batcher stopped after it sent the batch.
Ends ==
  <>[](/\ reqs = {}
       /\ \A s \in 0..(frontier - 1) : chosen[s] # None
       /\ \A r \in Replicas : Len(walked[r]) = frontier)

(***************************************************************************)
(* Safety.                                                                 *)
(***************************************************************************)

TypeOK ==
  /\ frontier \in 0..NS
  /\ changes \in 0..Leaders
  /\ restarts \in 0..Restarts
  /\ Len(batches) <= MaxSends
  /\ reqs \subseteq Ids
  /\ open \subseteq Slots
  /\ held \subseteq [s : Slots, id : Ids]
  /\ deposed \subseteq [s : Slots, id : Ids]
  /\ copies \in 0..Copies
  \* An open slot holds a proposal and is not chosen yet.
  /\ \A s \in open : prop[s] # None /\ chosen[s] = None

\* The slots of the leader that holds answers change only by its proposals.
HeldOwn == \A h \in held : prop[h.s] = h.id /\ chosen[h.s] \in {None, h.id}

\* A batcher whose limit is the leader's never hears TooLarge.
NoTooLarge == BatchMax <= LeaderMax => \A i \in DOMAIN batches : batches[i].st # "refused"

\* The search bound of a control that sends a batch again.
SendBound == Len(batches) <= MaxSends

\* OneRange: each write of a batch gets its own seq range one time, at each
\* replica, also when a batch is sent again (a TooLarge split, an Accept
\* sent again, a slot that a new leader recovers).
OneRange ==
  \A r \in Replicas, w \in Writes :
    Cardinality({x \in state[r].at : x[1] = w}) <= 1

\* OneAnswer: each write gets one answer: a range or unanswered.
OneAnswer ==
  \A w \in Writes :
    /\ Cardinality(ranges[w]) <= 1
    /\ ranges[w] # {} => w \notin ambiguous

\* AnswerAgrees: the range that the unbatcher gives a write is the range
\* that each replica folds for it.
AnswerAgrees ==
  \A r \in Replicas : \A x \in state[r].at :
    ranges[x[1]] \subseteq {<<x[2], x[3]>>}

\* AckedChosen: each range that the unbatcher gave a write is the range that
\* the fold of the chosen log gives it. An answered write is never lost,
\* also when no replica walked its slot yet (#1232).
AckedChosen ==
  LET n == CHOOSE k \in 0..NS : (\A s \in 0..(k - 1) : chosen[s] # None)
                                /\ (k = NS \/ chosen[k] = None)
      at == FoldFrom(EmptyState, Prefix(n)).at
  IN \A w \in Writes : \A rg \in ranges[w] :
       \E x \in at : x[1] = w /\ x[2] = rg[1] /\ x[3] = rg[2]

\* InOrder: a replica folds the chosen log in slot order with no gap, and
\* its state is the fold of that prefix.
InOrder ==
  \A r \in Replicas :
    /\ walked[r] = Prefix(Len(walked[r]))
    /\ \A s \in Range(walked[r]) : chosen[s] # None
    /\ state[r] = FoldFrom(EmptyState, walked[r])

\* Dense: the ranges of a replica's state are disjoint and cover
\* [0, next).
Dense ==
  \A r \in Replicas :
    LET at == state[r].at
    IN /\ \A x, y \in at : x # y => (x[2] + x[3] <= y[2] \/ y[2] + y[3] <= x[2])
       /\ SumAt(at) = state[r].next

\* ReadFresh: a read that a replica serves holds each write that was
\* complete before the read started.
ReadFresh ==
  \A r \in Replicas : served[r].on => served[r].must \subseteq served[r].got

\* Reachability controls: each must be violated.
NoMergedChosen == \A s \in Slots : chosen[s] \notin {id \in DOMAIN batches : Len(batches[id].ws) > 1}
NoSplit == \A id \in DOMAIN batches : batches[id].st # "refused" \/ \E w \in Writes : ranges[w] = {}
\* A deposed leader answers a held batch ok, and a write gets its range from it.
NoDeposedOk == \A rp \in replies : ~(rp.kind = "ok" /\ rp.from = "deposed")
NoReadAfterWrite == \A r \in Replicas : ~served[r].on \/ served[r].must = {}
=============================================================================
