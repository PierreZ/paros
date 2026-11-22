---------------------------- MODULE BatcherTrace ----------------------------
(***************************************************************************)
(* The trace spec of `Batcher`: the model, forced to follow the events     *)
(* that one batcher walk of `consensus-sim` recorded (#1231 (traced walks  *)
(* reach the rules), `crates/sim/consensus-sim/src/batcher.rs`). `l` is    *)
(* the index of the next event. The trace spec defines no step of its own  *)
(* (#1306 (trace specs extend the checked model)):                         *)
(*   - "write": a writer made the write `tok` of `n` records and `bytes`   *)
(*     bytes for journal `jr` (`Write`);                                   *)
(*   - "push": the write joins the lane of its journal (`Push`);           *)
(*   - "flush": a batch `ws` of journal `jr` leaves the batcher (`Flush`); *)
(*   - "crash": the batcher stops and loses its lanes (`Crash`);           *)
(*   - "land": batch `id` lands whole at position `seq` (`Land`);          *)
(*   - "answer": a writer hears the range of its write (`Answer`).         *)
(***************************************************************************)
EXTENDS Batcher, Trace, TLC

VARIABLE l

e == Trace[l]
Has(f) == f \in DOMAIN e

\* The bounds of the model, from the events: every token the walk names,
\* and no bound on the batches, the log and the size of a write.
TraceTokens == {Trace[i].tok : i \in {k \in 1..Len(Trace) : "tok" \in DOMAIN Trace[k]}}
TraceMaxN == Max({0} \cup {Trace[i].n : i \in {k \in 1..Len(Trace) : "n" \in DOMAIN Trace[k]}})
TraceBound == Len(Trace) * (TraceMaxN + 1)

TraceInit == Init /\ l = 1

Step ==
  \/ /\ e.event = "write" /\ Has("tok") /\ Has("jr") /\ Has("n") /\ Has("bytes")
     /\ e.jr \in Journals /\ Write(e.tok, e.jr, e.n, e.bytes)
  \/ /\ e.event = "push" /\ Has("tok")
     /\ Push(e.tok)
  \/ /\ e.event = "flush" /\ Has("jr") /\ Has("ws")
     /\ e.jr \in Journals /\ Flush(e.jr, e.ws)
  \/ /\ e.event = "crash"
     /\ Crash
  \/ /\ e.event = "land" /\ Has("id") /\ Has("seq")
     /\ Land(e.id, e.seq)
  \/ /\ e.event = "answer" /\ Has("tok") /\ Has("seq") /\ Has("n")
     /\ Answer(e.tok, e.seq, e.n)

TraceNext == l <= Len(Trace) /\ Step /\ l' = l + 1

TraceSpec == TraceInit /\ [][TraceNext]_<<l, vars>>

\* Violated exactly when a behaviour matched every event: the trace is
\* accepted.
TraceUnfinished == l <= Len(Trace)
=============================================================================
