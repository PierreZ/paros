--------------------------- MODULE ControllerTrace ---------------------------
(***************************************************************************)
(* The trace spec of the grid controller: the actions of                   *)
(* `GridController.tla`, forced to follow the events that one controller   *)
(* walk of `journal-sim` recorded (#1261 (grid controller decision tests), *)
(* `crates/sim/journal-sim/src/controller.rs`). `l` is the index of the    *)
(* next event. Each event applies one action of the model and binds what   *)
(* the code saw; this spec defines no step of its own (#1306 (trace specs  *)
(* extend the checked model)).                                             *)
(*   - "read_term": controller `c` read the term (`ReadTerm`).             *)
(*   - "elect": its compare-and-set, which it `won` or lost (`Elect`).     *)
(*   - "plan": holder `c` planned the grid `mem` from configuration `exp`  *)
(*     at term `tm` (`Plan`).                                              *)
(*   - "call": it sent its call (`Call`).                                  *)
(*   - "stand": a deposed holder read the record and stopped (`Stand`).    *)
(*   - "process": the record took the request and answered `ans`          *)
(*     (`Process`).                                                        *)
(*   - "receive": the reply reached its controller (`Receive`).            *)
(*   - "lose_req", "lose_rep": the network lost it (`LoseReq`, `LoseRep`). *)
(*   - "seal", "retire": the journal's steps on configuration `g`          *)
(*     (`SealIn`, `Retire`).                                               *)
(*   - "crash", "restart": controller `c` (`Crash`, `Restart`).            *)
(*   - "fail": acceptor `a` (`Fail`).                                      *)
(* A grid of the walk is one column of three with a Phase-2 quorum of two. *)
(***************************************************************************)
EXTENDS MC, Trace, Sequences

VARIABLES l

tvars == <<l>>

e == Trace[l]

\* The controllers of the walk, as the trace names them.
TraceCtrls == {"c1", "c2"}
\* Every grid the walk can plan: three of the pool, two acks.
TraceGrids == {[mem |-> M, acks |-> 2] : M \in {S \in SUBSET AccPool : Cardinality(S) = 3}}

G(x) == [mem |-> x.mem, acks |-> 2]
ReqOf(x) == [c |-> x.c, tm |-> x.tm, exp |-> x.exp, grid |-> G(x)]

TraceInit == Init /\ l = 1

TraceStep ==
  \/ e.event = "read_term" /\ ReadTerm(e.c)
  \/ /\ e.event = "elect"
     /\ Elect(e.c)
     /\ ctl'[e.c].st = IF e.won THEN "lead" ELSE "idle"
  \/ /\ e.event = "plan"
     /\ ctl[e.c].tm = e.tm
     /\ Plan(e.c)
     /\ ctl'[e.c].plan = G(e)
     /\ ctl'[e.c].exp = e.exp
  \/ e.event = "call" /\ Call(e.c)
  \/ e.event = "stand" /\ Stand(e.c)
  \/ /\ e.event = "process"
     /\ ReqOf(e) \in reqs
     /\ Process(ReqOf(e))
     /\ \E r \in reps' : r.req = ReqOf(e) /\ r.ans = e.ans
  \/ /\ e.event = "receive"
     /\ \E m \in reps : m.req = ReqOf(e) /\ m.ans = e.ans /\ Receive(m)
  \/ e.event = "lose_req" /\ ReqOf(e) \in reqs /\ LoseReq(ReqOf(e))
  \/ /\ e.event = "lose_rep"
     /\ \E m \in reps : m.req = ReqOf(e) /\ m.ans = e.ans /\ LoseRep(m)
  \/ e.event = "seal" /\ e.g \in 2..NG /\ SealIn(e.g)
  \/ e.event = "retire" /\ e.g \in Gens /\ Retire(e.g)
  \/ e.event = "crash" /\ Crash(e.c)
  \/ e.event = "restart" /\ Restart(e.c)
  \/ e.event = "fail" /\ Fail(e.a)

TraceNext == l <= Len(Trace) /\ TraceStep /\ l' = l + 1

TraceSpec == TraceInit /\ [][TraceNext]_<<tvars, vars>>

\* Violated exactly when a behaviour matched every event: the trace is
\* accepted.
TraceUnfinished == l <= Len(Trace)
=============================================================================
