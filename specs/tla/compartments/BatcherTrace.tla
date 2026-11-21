---------------------------- MODULE BatcherTrace ----------------------------
(***************************************************************************)
(* The trace spec of the batcher and the unbatcher of `Compartments`, on   *)
(* the batcher walk of `consensus-sim` (#1231 (traced walks reach the      *)
(* rules), `crates/sim/consensus-sim/src/batcher.rs`).                     *)
(*                                                                         *)
(* The walk is not the cluster: writers push writes of two multi-writer    *)
(* journals into one batcher, and a batch lands whole at its journal's     *)
(* next position, with no Paxos under it. So this module does not extend   *)
(* `Compartments`: it states the batcher rules of `Compartments.tla` for   *)
(* the lanes of the walk, one for each journal, and its network, which     *)
(* drops and duplicates batches and answers (a write may land twice,       *)
(* section 2.4 of docs/architecture/02-data-plane.md).                     *)
(*                                                                         *)
(* The steps:                                                              *)
(*   - "write": a writer made the write `tok` of `n` records and `bytes`   *)
(*     bytes for journal `jr`;                                             *)
(*   - "push": the write joins the lane of its journal (`Batcher::push`).  *)
(*     It is not empty, it fits the limits alone, and the lane with it     *)
(*     stays within the limits: a lane that would not is flushed first;    *)
(*   - "flush": a batch leaves the batcher. It is the whole lane of its    *)
(*     journal, in push order, or one write alone: an empty write, or one  *)
(*     over the limits by itself, after its lane flushed;                  *)
(*   - "crash": the batcher stops and loses its lanes; a new one starts    *)
(*     empty;                                                              *)
(*   - "land": batch `id` lands whole at the next position of its journal; *)
(*   - "answer": a writer hears the range of its write. The unbatcher cuts *)
(*     the range of a landed batch in batch order (`Unbatcher::scatter`):  *)
(*     the write's position is the batch's position plus the records of    *)
(*     the writes before it in the batch, and its count is its own. A     *)
(*     copy of an answer that the network duplicated is no step: the walk  *)
(*     records the first copy only.                                        *)
(* `AnswerHolds` is `OneRange` of `Compartments` for this walk: the range  *)
(* a write is answered with holds its own records.                         *)
(***************************************************************************)
EXTENDS Trace, Naturals, Sequences, FiniteSets

VARIABLES l,       \* the index of the next event
          writes,  \* each write made: its journal, records and bytes
          lanes,   \* journal: the writes of its lane, in push order
          batches, \* each batch sent: its journal and its writes
          logs,    \* journal: the write of each record, in landing order
          landed,  \* the landings: a batch and its position
          answers  \* the answers heard: a write, a position and a count

vars == <<l, writes, lanes, batches, logs, landed, answers>>

e == Trace[l]
Has(f) == f \in DOMAIN e

Journals == TraceJournals
MaxRecords == TraceMaxRecords
MaxBytes == TraceMaxBytes

RECURSIVE Records(_), Bytes(_)
\* The records and the bytes of a sequence of writes.
Records(ws) == IF ws = <<>> THEN 0 ELSE writes[Head(ws)].n + Records(Tail(ws))
Bytes(ws) == IF ws = <<>> THEN 0 ELSE writes[Head(ws)].bytes + Bytes(Tail(ws))

Made(t) == t \in DOMAIN writes

\* A write that goes alone: empty, or over the limits by itself.
Alone(t) == writes[t].n = 0 \/ writes[t].n > MaxRecords \/ writes[t].bytes > MaxBytes

Init ==
  /\ l = 1
  /\ writes = [t \in {} |-> 0]
  /\ lanes = [j \in Journals |-> <<>>]
  /\ batches = <<>>
  /\ logs = [j \in Journals |-> <<>>]
  /\ landed = {}
  /\ answers = {}

Write ==
  /\ e.event = "write" /\ Has("tok") /\ Has("jr") /\ Has("n") /\ Has("bytes")
  /\ e.jr \in Journals
  /\ ~Made(e.tok)
  /\ writes' = [t \in DOMAIN writes \cup {e.tok} |->
                  IF t = e.tok THEN [jr |-> e.jr, n |-> e.n, bytes |-> e.bytes] ELSE writes[t]]
  /\ UNCHANGED <<lanes, batches, logs, landed, answers>>

Push ==
  /\ e.event = "push" /\ Has("tok")
  /\ Made(e.tok)
  /\ LET t == e.tok
         j == writes[t].jr
     IN /\ ~Alone(t)
        /\ Records(lanes[j]) + writes[t].n <= MaxRecords
        /\ Bytes(lanes[j]) + writes[t].bytes <= MaxBytes
        /\ lanes' = [lanes EXCEPT ![j] = Append(@, t)]
  /\ UNCHANGED <<writes, batches, logs, landed, answers>>

Flush ==
  /\ e.event = "flush" /\ Has("jr") /\ Has("ws")
  /\ e.jr \in Journals
  /\ e.ws # <<>>
  /\ \/ /\ e.ws = lanes[e.jr]
        /\ lanes' = [lanes EXCEPT ![e.jr] = <<>>]
     \/ /\ Len(e.ws) = 1
        /\ Made(e.ws[1]) /\ writes[e.ws[1]].jr = e.jr /\ Alone(e.ws[1])
        /\ lanes[e.jr] = <<>>
        /\ UNCHANGED lanes
  /\ batches' = Append(batches, [jr |-> e.jr, ws |-> e.ws])
  /\ UNCHANGED <<writes, logs, landed, answers>>

Crash ==
  /\ e.event = "crash"
  /\ lanes' = [j \in Journals |-> <<>>]
  /\ UNCHANGED <<writes, batches, logs, landed, answers>>

\* The records of the writes `ws`, in order: one entry for each record.
RECURSIVE Spread(_)
Spread(ws) == IF ws = <<>> THEN <<>>
              ELSE [i \in 1..writes[Head(ws)].n |-> Head(ws)] \o Spread(Tail(ws))

Land ==
  /\ e.event = "land" /\ Has("id") /\ Has("seq")
  /\ e.id \in 1..Len(batches)
  /\ LET b == batches[e.id]
     IN /\ e.seq = Len(logs[b.jr])
        /\ logs' = [logs EXCEPT ![b.jr] = @ \o Spread(b.ws)]
  /\ landed' = landed \cup {[id |-> e.id, seq |-> e.seq]}
  /\ UNCHANGED <<writes, lanes, batches, answers>>

Answer ==
  /\ e.event = "answer" /\ Has("tok") /\ Has("seq") /\ Has("n")
  /\ Made(e.tok)
  /\ e.n = writes[e.tok].n
  /\ [tok |-> e.tok, seq |-> e.seq, n |-> e.n] \notin answers
  /\ \E x \in landed : \E i \in 1..Len(batches[x.id].ws) :
       /\ batches[x.id].ws[i] = e.tok
       /\ e.seq = x.seq + Records(SubSeq(batches[x.id].ws, 1, i - 1))
  /\ answers' = answers \cup {[tok |-> e.tok, seq |-> e.seq, n |-> e.n]}
  /\ UNCHANGED <<writes, lanes, batches, logs, landed>>

Next ==
  /\ l <= Len(Trace)
  /\ Write \/ Push \/ Flush \/ Crash \/ Land \/ Answer
  /\ l' = l + 1

TraceSpec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Safety *)

\* The range a write is answered with holds its own records.
AnswerHolds ==
  \A a \in answers :
    \A k \in 1..a.n : logs[writes[a.tok].jr][a.seq + k] = a.tok

\* Violated exactly when a behaviour matched every event: the trace is
\* accepted.
TraceUnfinished == l <= Len(Trace)
=============================================================================
