------------------------------- MODULE Batcher -------------------------------
(***************************************************************************)
(* The batcher and the unbatcher of Compartmentalized Paxos (section 3.5)  *)
(* as `paros-consensus` builds them (`Batcher`, `Unbatcher`), with the     *)
(* network of the batcher walk of `consensus-sim` (#1306 (trace specs      *)
(* extend the checked model)): `BatcherTrace` extends this module, and TLC *)
(* checks it here on bounded instances (`checks.txt`).                     *)
(*                                                                         *)
(* `compartments/` checks the batcher inside the log, with Paxos under it. *)
(* This model is the walk's shape: writers push writes of several          *)
(* multi-writer journals into one batcher, a batch lands whole at its      *)
(* journal's next position with no Paxos under it, and the network drops   *)
(* and duplicates batches and answers (a write may land twice, section 2.4 *)
(* of docs/architecture/02-data-plane.md). The steps:                      *)
(*   - `Write`: a writer makes write t of n records and `bytes` bytes for  *)
(*     journal j;                                                          *)
(*   - `Push`: the write joins the lane of its journal (`Batcher::push`).  *)
(*     It is not empty, it fits the limits alone, and the lane with it     *)
(*     stays within the limits: a lane that would not is flushed first;    *)
(*   - `Flush`: a batch leaves the batcher. It is the whole lane of its    *)
(*     journal, in push order, or one write alone: an empty write, or one  *)
(*     over the limits by itself, after its lane flushed;                  *)
(*   - `Crash`: the batcher stops and loses its lanes; a new one starts    *)
(*     empty;                                                              *)
(*   - `Land`: a batch lands whole at the next position of its journal,    *)
(*     once or more;                                                       *)
(*   - `Answer`: a writer hears the range of its write, cut from a landed  *)
(*     batch (`Unbatcher::scatter`).                                       *)
(*                                                                         *)
(* Constants: `Tokens`, the writes; `Journals`; `MaxRecords` and           *)
(* `MaxBytes`, the limits of a batch; `MaxN` and `MaxB`, the largest write *)
(* the writers make; `MaxBatches`, `MaxLog` and `MaxPushes`, the bounds of *)
(* a check (a writer may push a write again, after a crash).               *)
(* Rules, each with a control:                                             *)
(*   Order    "push": a flush sends its lane in push order (the rule).     *)
(*            "any": a control, it sends the lane in any order.            *)
(*   Scatter  "batch": a write's position is the batch's position plus the *)
(*            records of the writes before it in the batch (the rule).     *)
(*            "lane": a control, the records of the writes before it in    *)
(*            its lane now, as an unbatcher that reads the batcher's       *)
(*            state and not the batch.                                     *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Tokens, Journals, MaxRecords, MaxBytes, MaxN, MaxB, MaxBatches, MaxLog, MaxPushes,
          Order, Scatter

ASSUME Order \in {"push", "any"}
ASSUME Scatter \in {"batch", "lane"}

VARIABLES writes,  \* each write made: its journal, records and bytes
          pushed,  \* each write pushed, in push order
          lanes,   \* journal: the writes of its lane, in push order
          batches, \* each batch sent: its journal and its writes
          logs,    \* journal: the write of each record, in landing order
          landed,  \* the landings: a batch and its position
          answers  \* the answers heard: a write, a position and a count

vars == <<writes, pushed, lanes, batches, logs, landed, answers>>

Max(S) == CHOOSE x \in S : \A y \in S : y <= x

RECURSIVE Records(_), Bytes(_)
\* The records and the bytes of a sequence of writes.
Records(ws) == IF ws = <<>> THEN 0 ELSE writes[Head(ws)].n + Records(Tail(ws))
Bytes(ws) == IF ws = <<>> THEN 0 ELSE writes[Head(ws)].bytes + Bytes(Tail(ws))

Made(t) == t \in DOMAIN writes

\* A write that goes alone: empty, or over the limits by itself.
Alone(t) == writes[t].n = 0 \/ writes[t].n > MaxRecords \/ writes[t].bytes > MaxBytes

\* The orders of the sequence s: s itself, or with `Order = "any"` any
\* permutation of it.
Orders(s) ==
  IF Order = "push" THEN {s}
  ELSE {p \in [1..Len(s) -> {s[i] : i \in 1..Len(s)}] :
          \A i \in 1..Len(s) : Cardinality({k \in 1..Len(s) : p[k] = s[i]}) =
                               Cardinality({k \in 1..Len(s) : s[k] = s[i]})}

\* The records of the writes `ws`, in order: one entry for each record.
RECURSIVE Spread(_)
Spread(ws) == IF ws = <<>> THEN <<>>
              ELSE [i \in 1..writes[Head(ws)].n |-> Head(ws)] \o Spread(Tail(ws))

TypeOK ==
  /\ lanes \in [Journals -> Seq(Tokens)]
  /\ \A j \in Journals : Len(logs[j]) <= MaxLog

Init ==
  /\ writes = [t \in {} |-> 0]
  /\ pushed = <<>>
  /\ lanes = [j \in Journals |-> <<>>]
  /\ batches = <<>>
  /\ logs = [j \in Journals |-> <<>>]
  /\ landed = {}
  /\ answers = {}

-----------------------------------------------------------------------------

\* A writer makes write t of n records and b bytes for journal j (`Next`
\* takes j from `Journals`).
Write(t, j, n, b) ==
  /\ ~Made(t)
  /\ writes' = [x \in DOMAIN writes \cup {t} |->
                  IF x = t THEN [jr |-> j, n |-> n, bytes |-> b] ELSE writes[x]]
  /\ UNCHANGED <<pushed, lanes, batches, logs, landed, answers>>

\* Write t joins the lane of its journal.
Push(t) ==
  /\ Made(t)
  /\ Len(pushed) < MaxPushes
  /\ LET j == writes[t].jr
     IN /\ ~Alone(t)
        /\ Records(lanes[j]) + writes[t].n <= MaxRecords
        /\ Bytes(lanes[j]) + writes[t].bytes <= MaxBytes
        /\ lanes' = [lanes EXCEPT ![j] = Append(@, t)]
  /\ pushed' = Append(pushed, t)
  /\ UNCHANGED <<writes, batches, logs, landed, answers>>

\* Batch ws of journal j leaves the batcher (`Next` takes j from `Journals`):
\* the lane of j, never empty (`BatchFits`), or one write alone after its
\* lane flushed. The batcher gets the write that goes alone as it gets every
\* write, so it is pushed too (`SendOrder`).
Flush(j, ws) ==
  /\ Len(batches) < MaxBatches
  /\ \/ /\ lanes[j] # <<>>
        /\ ws \in Orders(lanes[j])
        /\ lanes' = [lanes EXCEPT ![j] = <<>>]
        /\ UNCHANGED pushed
     \/ /\ Len(ws) = 1
        /\ Made(ws[1]) /\ writes[ws[1]].jr = j /\ Alone(ws[1])
        /\ lanes[j] = <<>>
        /\ pushed' = Append(pushed, ws[1])
        /\ UNCHANGED lanes
  /\ batches' = Append(batches, [jr |-> j, ws |-> ws])
  /\ UNCHANGED <<writes, logs, landed, answers>>

\* The batcher stops and loses its lanes.
Crash ==
  /\ lanes' = [j \in Journals |-> <<>>]
  /\ UNCHANGED <<writes, pushed, batches, logs, landed, answers>>

\* Batch id lands whole at position seq, the next of its journal.
Land(id, seq) ==
  /\ id \in 1..Len(batches)
  /\ LET b == batches[id]
     IN /\ seq = Len(logs[b.jr])
        /\ seq + Records(b.ws) <= MaxLog
        /\ logs' = [logs EXCEPT ![b.jr] = @ \o Spread(b.ws)]
  /\ landed' = landed \cup {[id |-> id, seq |-> seq]}
  /\ UNCHANGED <<writes, pushed, lanes, batches, answers>>

\* The records before write t in the batch of landing x: of the writes
\* before it in the batch (the rule), or in its lane now (the control).
Before(x, i, t) ==
  IF Scatter = "batch" THEN Records(SubSeq(batches[x.id].ws, 1, i - 1))
  ELSE LET lane == lanes[writes[t].jr]
       IN Records(SelectSeq(lane, LAMBDA u : u # t))

\* A writer hears the range of write t: at position seq, n records. A copy
\* of an answer heard before adds nothing to `answers`: a stutter step.
Answer(t, seq, n) ==
  /\ Made(t)
  /\ n = writes[t].n
  /\ \E x \in landed : \E i \in 1..Len(batches[x.id].ws) :
       /\ batches[x.id].ws[i] = t
       /\ seq = x.seq + Before(x, i, t)
  /\ answers' = answers \cup {[tok |-> t, seq |-> seq, n |-> n]}
  /\ UNCHANGED <<writes, pushed, lanes, batches, logs, landed>>

\* The batches of one or two writes that a flush may name, as a recorded
\* event may.
Shorts == {<<t>> : t \in Tokens} \cup {<<t, u>> : t, u \in Tokens}

Next ==
  \/ \E t \in Tokens, j \in Journals, n \in 0..MaxN, b \in 0..MaxB : Write(t, j, n, b)
  \/ \E t \in Tokens : Push(t)
  \/ \E j \in Journals : \E ws \in {lanes[j]} \cup Orders(lanes[j]) \cup Shorts : Flush(j, ws)
  \/ Crash
  \/ \E id \in 1..MaxBatches, seq \in 0..MaxLog : Land(id, seq)
  \/ \E t \in Tokens, seq \in 0..MaxLog, n \in 0..MaxN : Answer(t, seq, n)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Safety *)

\* The range a write is answered with holds its own records.
AnswerHolds ==
  \A a \in answers :
    \A k \in 1..a.n :
      LET log == logs[writes[a.tok].jr] IN
      /\ a.seq + k \in DOMAIN log
      /\ log[a.seq + k] = a.tok

RECURSIVE Sub(_, _)
\* The sequence s is a subsequence of p.
Sub(s, p) ==
  \/ s = <<>>
  \/ /\ p # <<>>
     /\ IF Head(s) = Head(p) THEN Sub(Tail(s), Tail(p)) ELSE Sub(s, Tail(p))

\* A batch of a lane keeps the push order of its writes.
LaneOrder == \A x \in 1..Len(batches) : Len(batches[x].ws) = 1 \/ Sub(batches[x].ws, pushed)

\* A batch holds a write at least. A batch of more than one write is a lane:
\* no write in it goes alone, and it fits the limits (`Batcher::push`
\* flushes a lane that the next write would overfill).
BatchFits ==
  \A x \in 1..Len(batches) :
    LET ws == batches[x].ws
    IN /\ ws # <<>>
       /\ Len(ws) > 1 => /\ \A i \in 1..Len(ws) : ~Alone(ws[i])
                         /\ Records(ws) <= MaxRecords
                         /\ Bytes(ws) <= MaxBytes

\* The batches of a journal leave in the order that the batcher got their
\* writes: a write that goes alone flushes its lane first.
SendOrder ==
  \A j \in Journals :
    LET of == SelectSeq([x \in 1..Len(batches) |-> batches[x]], LAMBDA b : b.jr = j)
        Flat[k \in 0..Len(of)] == IF k = 0 THEN <<>> ELSE Flat[k - 1] \o of[k].ws
    IN Sub(Flat[Len(of)], pushed)

\* Reachability controls: each must be violated.
NoAnswer == answers = {}
NoTwoInBatch == \A x \in 1..Len(batches) : Len(batches[x].ws) <= 1
NoDuplicate == \A x \in 1..Len(batches) : Cardinality({l \in landed : l.id = x}) <= 1
=============================================================================
