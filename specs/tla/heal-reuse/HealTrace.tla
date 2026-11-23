------------------------------ MODULE HealTrace ------------------------------
(***************************************************************************)
(* The trace spec of the heal of a retired segment's replica set: the      *)
(* actions of `HealReuse.tla`, forced to follow the events that one heal   *)
(* walk of `journal-sim` recorded (#1314 (re-replication of a sealed       *)
(* segment's replica set), `crates/sim/journal-sim/src/heal.rs`). `l` is   *)
(* the index of the next event. Each event applies one action of the model *)
(* and binds what the code saw; this spec defines no step of its own       *)
(* (#1306 (trace specs extend the checked model)).                         *)
(*   - "read": node `r` read the register (`Read`).                        *)
(*   - "join": node `r` started copy <<tm, num>> (`Join`).                 *)
(*   - "copy", "report", "abandon", "ends", "drop": node `r` (`CopyStep`,  *)
(*     `Report`, `Abandon`, `Ends`, `Drop`).                               *)
(*   - "crash", "restart": machine `r` (`Crash`, `Restart`).               *)
(*   - "elect", "ctl_crash": controller `c` (`Elect`, `CtlCrash`).         *)
(*   - "plan": controller `c` planned the heal of `d` by `n` on the set    *)
(*     `rd` with `heals` heals, copy <<tm, num>> (`Plan`).                 *)
(*   - "send", "done", "abort": controller `c` (`Send`, `Done`, `Abort`).  *)
(*   - "apply": the register took the compare-and-set of term `tm` on      *)
(*     `rd`, `heals`, `d`, `n`, answered `ok`, and the set is `set` after  *)
(*     (`ApplyOn`).                                                        *)
(*   - "answer": an answer `ok` ended the heal of controller `c`           *)
(*     (`AnswerOn`).                                                       *)
(* The walk numbers the copies of a term from 0; the trace numbers the     *)
(* plans of all terms in one count, as the model does (`num`). The walk    *)
(* has no trim, as the model.                                              *)
(***************************************************************************)
EXTENDS MC, Trace, Sequences

VARIABLES l

tvars == <<l>>

e == Trace[l]

\* The machines and the controllers of the walk, as the trace names them.
TracePool == {"n11", "n12", "n13", "n20", "n21", "n22", "n23", "n24", "n25"}
TraceInit0 == {"n11", "n12", "n13"}
TraceCtls == {"c1", "c2"}

Id(x) == <<x.tm, x.num>>
Rd(x) == [set |-> x.rd, heals |-> x.heals]

TraceInit == Init /\ l = 1

TraceStep ==
  \/ e.event = "read" /\ Read(e.r)
  \/ e.event = "join" /\ Join(e.r) /\ cid'[e.r] = Id(e)
  \/ e.event = "copy" /\ CopyStep(e.r)
  \/ e.event = "report" /\ Report(e.r)
  \/ e.event = "abandon" /\ Abandon(e.r)
  \/ e.event = "ends" /\ Ends(e.r)
  \/ e.event = "drop" /\ Drop(e.r)
  \/ e.event = "crash" /\ Crash(e.r)
  \/ e.event = "restart" /\ Restart(e.r)
  \/ e.event = "elect" /\ Elect(e.c)
  \/ e.event = "ctl_crash" /\ CtlCrash(e.c)
  \/ /\ e.event = "plan"
     /\ Plan(e.c)
     /\ heal'[e.c].rd = Rd(e)
     /\ heal'[e.c].d = e.d
     /\ heal'[e.c].n = e.n
     /\ heal'[e.c].id = Id(e)
  \/ e.event = "send" /\ Send(e.c)
  \/ /\ e.event = "apply"
     /\ \E m \in cas :
          /\ m.rd = Rd(e) /\ m.term = e.tm /\ m.d = e.d /\ m.n = e.n
          /\ Ok(m) = e.ok
          /\ ApplyOn(m)
     /\ rec'.set = e.set
  \/ e.event = "answer" /\ \E a \in ans : a.ok = e.ok /\ AnswerOn(e.c, a)
  \/ e.event = "done" /\ Done(e.c)
  \/ e.event = "abort" /\ Abort(e.c)

TraceNext == l <= Len(Trace) /\ TraceStep /\ l' = l + 1

TraceSpec == TraceInit /\ [][TraceNext]_<<tvars, vars>>

\* Violated exactly when a behaviour matched every event: the trace is
\* accepted.
TraceUnfinished == l <= Len(Trace)
=============================================================================
