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
(*     transport delivers a request at most one time.                      *)
(*   - The leader refuses a merged batch over its record limit at the edge *)
(*     (`TooLarge`), before it allocates a slot. The batcher lowers its    *)
(*     limit (`lower_limits`) and sends each write of the batch again,     *)
(*     alone (docs/architecture/02-data-plane.md, section 2.4).            *)
(*   - Else the leader allocates the frontier slot and holds the answer.   *)
(*     It answers once the slot and every slot below it are chosen, with  *)
(*     the `seq` that its fold gives the batch. Only the leader that       *)
(*     received the batch holds its answer: a new leader answers nothing.  *)
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
(*   MaxSends  how many batches the batcher may send (in the whole run).   *)
(*   Leaders   how many leader changes may happen.                         *)
(*   Restarts  how many replica restarts may happen.                       *)
(*   Crash     TRUE: the batcher may stop for good.                        *)
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
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Writes, Count, BatchMax, LeaderMax, Acceptors, Replicas, Readers,
          NS, MaxSends, Leaders, Restarts, Crash, Q2, RQ,
          Resend, Accepts, Walk, Boot, Read

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
ASSUME Crash \in BOOLEAN

VARIABLES
  \* The batcher.
  lane,     \* the writes of the lane, in push order
  pushed,   \* the writes that the writers pushed
  limit,    \* the batcher's record limit
  bup,      \* the batcher runs
  batches,  \* each batch sent: its writes and its state at the batcher
  \* The network between the batcher and the leader.
  reqs,     \* the batch ids on the way to the leader
  replies,  \* the answers on the way to the batcher
  \* The log, as the leader sees it.
  prop,     \* the proposal at each slot: a batch id, Noop, or None
  open,     \* the open slots whose Accepts are on the way
  chosen,   \* the chosen value at each slot, or None
  frontier, \* the leader's allocator frontier
  held,     \* the slots whose answer the leader holds
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

vars == <<lane, pushed, limit, bup, batches, reqs, replies, prop, open, chosen,
          frontier, held, changes, wm, ranges, ambiguous, learned, walked,
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
  /\ bup = TRUE
  /\ batches = <<>>
  /\ reqs = {}
  /\ replies = {}
  /\ prop = [s \in Slots |-> None]
  /\ open = {}
  /\ chosen = [s \in Slots |-> None]
  /\ frontier = 0
  /\ held = {}
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

Unchanged_log == UNCHANGED <<prop, open, chosen, frontier, held, changes, wm>>
Unchanged_replicas == UNCHANGED <<learned, walked, state, disk, restarts, reading, served>>

\* Send the writes `ws` as one batch.
SendBatch(ws) ==
  /\ Len(batches) < MaxSends
  /\ batches' = Append(batches, [ws |-> ws, st |-> "sent"])
  /\ reqs' = reqs \cup {Len(batches) + 1}


(***************************************************************************)
(* The batcher and the unbatcher.                                          *)
(***************************************************************************)

\* A writer pushes write w into the lane (`Batcher::push`).
Push(w) ==
  /\ bup
  /\ w \notin pushed
  /\ SumOf(lane) + Count[w] <= limit
  /\ lane' = Append(lane, w)
  /\ pushed' = pushed \cup {w}
  /\ UNCHANGED <<limit, bup, batches, reqs, replies, ranges, ambiguous>>
  /\ Unchanged_log /\ Unchanged_replicas

\* The lane flushes as one batch (`flush_records` or the tick).
Flush ==
  /\ bup
  /\ lane # <<>>
  /\ SendBatch(lane)
  /\ lane' = <<>>
  /\ UNCHANGED <<pushed, limit, bup, replies, ranges, ambiguous>>
  /\ Unchanged_log /\ Unchanged_replicas

\* The batcher hears the leader's answer to batch id.
Hear(rp) ==
  /\ bup
  /\ rp \in replies
  /\ replies' = replies \ {rp}
  /\ IF batches[rp.id].st # "sent"
       \* An ambiguous batch was answered already: the late answer is dropped.
       THEN /\ UNCHANGED <<batches, reqs, limit, ranges>>
       ELSE IF rp.kind = "ok"
         THEN /\ batches' = [batches EXCEPT ![rp.id].st = "done"]
              /\ ranges' = [w \in Writes |->
                              ranges[w] \cup {<<x[2], x[3]>> : x \in {y \in Scatter(rp.id, rp.seq) : y[1] = w}}]
              /\ UNCHANGED <<reqs, limit>>
         \* TooLarge: lower the limit and send each write again, alone.
         ELSE /\ limit' = LeaderMax
              /\ Len(batches) + Len(batches[rp.id].ws) <= MaxSends
              /\ reqs' = reqs \cup {Len(batches) + i : i \in DOMAIN batches[rp.id].ws}
              /\ batches' = [batches EXCEPT ![rp.id].st = "refused"]
                             \o [i \in DOMAIN batches[rp.id].ws |->
                                   [ws |-> <<batches[rp.id].ws[i]>>, st |-> "sent"]]
              /\ UNCHANGED ranges
  /\ UNCHANGED <<lane, pushed, bup, ambiguous>>
  /\ Unchanged_log /\ Unchanged_replicas

\* No answer came in time: each write of batch id is answered unanswered.
Timeout(id) ==
  /\ bup
  /\ id \in DOMAIN batches
  /\ batches[id].st = "sent"
  /\ ambiguous' = ambiguous \cup Range(batches[id].ws)
  /\ IF Resend = "ambiguous"
       THEN /\ Len(batches) < MaxSends
            /\ batches' = Append([batches EXCEPT ![id].st = "ambiguous"],
                                 [ws |-> batches[id].ws, st |-> "sent"])
            /\ reqs' = reqs \cup {Len(batches) + 1}
       ELSE /\ batches' = [batches EXCEPT ![id].st = "ambiguous"]
            /\ UNCHANGED reqs
  /\ UNCHANGED <<lane, pushed, limit, bup, replies, ranges>>
  /\ Unchanged_log /\ Unchanged_replicas

\* The batcher stops for good, with its lane.
CrashBatcher ==
  /\ Crash
  /\ bup
  /\ bup' = FALSE
  /\ lane' = <<>>
  /\ UNCHANGED <<pushed, limit, batches, reqs, replies, ranges, ambiguous>>
  /\ Unchanged_log /\ Unchanged_replicas

(***************************************************************************)
(* The leader and the log.                                                 *)
(***************************************************************************)

\* The leader receives batch id.
Receive(id) ==
  /\ id \in reqs
  /\ reqs' = reqs \ {id}
  /\ IF Len(batches[id].ws) > 1 /\ SumOf(batches[id].ws) > LeaderMax
       \* At the edge: a merged batch over the limit takes no slot.
       THEN /\ replies' = replies \cup {[id |-> id, kind |-> "toolarge", seq |-> 0]}
            /\ Unchanged_log
       ELSE /\ frontier < NS
            /\ prop' = [prop EXCEPT ![frontier] = id]
            /\ open' = open \cup {frontier}
            /\ held' = held \cup {frontier}
            /\ frontier' = frontier + 1
            /\ UNCHANGED <<replies, chosen, changes, wm>>
  /\ UNCHANGED <<lane, pushed, limit, bup, batches, ranges, ambiguous>>
  /\ Unchanged_replicas

\* A Phase-2 quorum votes for the proposal at slot s: it is chosen.
Choose(s) ==
  /\ s \in open
  /\ chosen[s] = None
  \* The watermarks matter only to a read: a check with no reader keeps
  \* them at 0, so it does not pay for their states.
  /\ IF Readers = {} THEN UNCHANGED wm
     ELSE \E Q \in Q2 : wm' = [a \in Acceptors |-> IF a \in Q THEN Max(wm[a], s + 1) ELSE wm[a]]
  /\ chosen' = [chosen EXCEPT ![s] = prop[s]]
  /\ open' = open \ {s}
  /\ UNCHANGED <<prop, frontier, held, changes>>
  /\ UNCHANGED <<lane, pushed, limit, bup, batches, reqs, replies, ranges, ambiguous>>
  /\ Unchanged_replicas

\* The Accepts of open slot s are lost.
LoseAccepts(s) ==
  /\ s \in open
  /\ open' = open \ {s}
  /\ UNCHANGED <<prop, chosen, frontier, held, changes, wm>>
  /\ UNCHANGED <<lane, pushed, limit, bup, batches, reqs, replies, ranges, ambiguous>>
  /\ Unchanged_replicas

\* The leader sends the Accepts of slot s again (`resend_pending`).
ResendAccepts(s) ==
  /\ Accepts
  /\ s < frontier
  /\ chosen[s] = None
  /\ s \notin open
  /\ open' = open \cup {s}
  /\ UNCHANGED <<prop, chosen, frontier, held, changes, wm>>
  /\ UNCHANGED <<lane, pushed, limit, bup, batches, reqs, replies, ranges, ambiguous>>
  /\ Unchanged_replicas

\* A new leader wins Phase 1. Each open slot below its frontier keeps its
\* proposal or gets a Noop; each open slot above it is free again. Every
\* slot that the new leader recovers is sent again. The held answers die
\* with the old leader.
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
  /\ changes' = changes + 1
  /\ UNCHANGED <<chosen, wm>>
  /\ UNCHANGED <<lane, pushed, limit, bup, batches, reqs, replies, ranges, ambiguous>>
  /\ Unchanged_replicas

\* The leader answers the batch of slot s: the slot and every slot below
\* it are chosen, so its fold gives the batch its seq.
Answer(s) ==
  /\ s \in held
  /\ \A k \in 0..s : chosen[k] # None
  /\ held' = held \ {s}
  /\ replies' = replies \cup
       {[id |-> chosen[s], kind |-> "ok", seq |-> FoldFrom(EmptyState, Prefix(s)).next]}
  /\ UNCHANGED <<prop, open, chosen, frontier, changes, wm>>
  /\ UNCHANGED <<lane, pushed, limit, bup, batches, reqs, ranges, ambiguous>>
  /\ Unchanged_replicas

\* An answer is lost.
LoseReply(rp) ==
  /\ rp \in replies
  /\ replies' = replies \ {rp}
  /\ UNCHANGED <<lane, pushed, limit, bup, batches, reqs, ranges, ambiguous>>
  /\ Unchanged_log /\ Unchanged_replicas

(***************************************************************************)
(* The replicas.                                                           *)
(***************************************************************************)

Unchanged_rest == UNCHANGED <<lane, pushed, limit, bup, batches, reqs, replies,
                              ranges, ambiguous>> /\ Unchanged_log

\* Replica r learns chosen slot s (a `Commit` or a catch-up).
Learn(r, s) ==
  /\ chosen[s] # None
  /\ s \notin learned[r]
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

\* Replica r writes its chosen index (the relaxed `SetChosenIndex`).
Persist(r) ==
  /\ Restarts > 0
  /\ disk[r] # Len(walked[r])
  /\ disk' = [disk EXCEPT ![r] = Len(walked[r])]
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

\* Replica r opens a read.
ReadStart(r) ==
  /\ r \in Readers
  /\ ~reading[r].on
  /\ reading' = [reading EXCEPT ![r] = [on |-> TRUE, acks |-> {}, max |-> 0, must |-> Complete]]
  /\ UNCHANGED <<learned, walked, state, disk, restarts, served>>
  /\ Unchanged_rest

\* The watermark that v answers a read with.
WmOf(v) == IF v \in Acceptors THEN wm[v] ELSE Len(walked[v])

\* Voter v answers the read of replica r (`PreReadAck`).
ReadAck(r, v) ==
  /\ reading[r].on
  /\ v \notin reading[r].acks
  /\ reading' = [reading EXCEPT ![r] = [@ EXCEPT !.acks = @ \cup {v},
                                                 !.max = Max(@, WmOf(v))]]
  /\ UNCHANGED <<learned, walked, state, disk, restarts, served>>
  /\ Unchanged_rest

\* Replica r serves its read from its own folded prefix.
ReadServe(r) ==
  /\ reading[r].on
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
  \/ CrashBatcher
  \/ \E s \in Slots : Choose(s) \/ LoseAccepts(s) \/ ResendAccepts(s) \/ Answer(s)
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
  /\ held \subseteq Slots

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
NoReadAfterWrite == \A r \in Replicas : ~served[r].on \/ served[r].must = {}
=============================================================================
