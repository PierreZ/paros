---------------------------- MODULE ElectionTrace ----------------------------
(***************************************************************************)
(* The trace spec of the election: the actions of `Election.tla`, forced   *)
(* to follow the events that one election walk of `journal-sim` recorded   *)
(* (#1420 (paros-election), `crates/sim/journal-sim/src/election.rs`).     *)
(* `l` is the index of the next event. Each event applies one action of    *)
(* the model and binds what the code saw; this spec defines no step of its *)
(* own (#1306 (trace specs extend the checked model)).                     *)
(*   - "challenge": actor `a` sent `SetLeader(u, old)` (`Challenge`).      *)
(*   - "retry": it sent its `SetLeader` again after a loss (`Retry`).      *)
(*   - "campaign_answer": its claim was `won` with number `t`, or lost     *)
(*     (`CampaignAnswer`). The code steps it when the term record leaves,  *)
(*     once its fold reached the tail of its win.                          *)
(*   - "renew": a write of its term left (`Renew`): a renewal or a write   *)
(*     of the program.                                                     *)
(*   - "timeout": it gave up a lost write (`Timeout`).                     *)
(*   - "write_answer": the answer to its write, `ok` or not                *)
(*     (`WriteAnswer`).                                                    *)
(*   - "hand_on": it handed its term to `b` with the uuid `u` (`HandOn`).  *)
(*   - "hand_answer": the answer of its hand on (`HandAnswer`).            *)
(*   - "adopt": actor `b` was handed term `t` and took it or not           *)
(*     (`Adopt`).                                                          *)
(*   - "apply_sl", "apply_w": the journal applied a call under `u` of      *)
(*     incarnation `inc` of `a` (`ApplySetLeader`, `ApplyWrite`).          *)
(*   - "lose": the network lost a message of kind `kind` (`Lose`).         *)
(*   - "crash": actor `a` stopped and came back (`Crash`).                 *)
(*   - "user_multi": the user switched to multi-writer mode (`UserMulti`). *)
(* A read of the code is no step of the model: the model reads its log.    *)
(***************************************************************************)
EXTENDS MC, Trace, Sequences

VARIABLES l

tvars == <<l>>

e == Trace[l]

\* The actors of the walk, as the trace names them.
TraceActors == {"a1", "a2", "a3"}

\* The calls of incarnation `x.inc` of `x.a` under `x.u` of kind `k`, and
\* the answers to `x.a` of kind `k`.
Calls(k, x) == {m \in net : m.k = k /\ m.from = x.a /\ m.inc = x.inc /\ m.u = x.u}
Answers(k, x) == {m \in net : m.k = k /\ m.to = x.a}
Lost(x) == IF x.kind \in {"sl", "w"} THEN Calls(x.kind, x) ELSE Answers(x.kind, x)

TraceInit == Init /\ l = 1

TraceStep ==
  \/ /\ e.event = "challenge"
     /\ Challenge(e.a)
     /\ mine'[e.a] = e.u
     /\ base'[e.a][1] = e.old
  \/ e.event = "retry" /\ Retry(e.a)
  \/ /\ e.event = "campaign_answer"
     /\ CampaignAnswer(e.a)
     /\ st'[e.a] = IF e.won THEN "lead" ELSE "idle"
     /\ e.won => term'[e.a] = e.t
  \/ e.event = "renew" /\ Renew(e.a)
  \/ e.event = "timeout" /\ Timeout(e.a)
  \/ /\ e.event = "write_answer"
     /\ \E m \in Answers("wans", e) : m.ok = e.ok
     /\ WriteAnswer(e.a)
     /\ \A m \in Answers("wans", e) \ net' : m.ok = e.ok
  \/ /\ e.event = "hand_on"
     /\ HandOn(e.a, e.b)
     /\ mine'[e.a] = e.u
  \/ e.event = "hand_answer" /\ HandAnswer(e.a)
  \/ /\ e.event = "adopt"
     /\ \E m \in net : m.k = "lead" /\ m.to = e.b /\ m.t = e.t
     /\ e.took = (st[e.b] = "idle")
     /\ Adopt(e.b)
     /\ e.took => term'[e.b] = e.t
  \/ /\ e.event = "apply_sl"
     /\ Calls("sl", e) # {}
     /\ ApplySetLeader
     /\ Calls("sl", e) \cap net' = {}
     /\ e.ok = (jleader' = e.u /\ jleader # e.u)
  \/ /\ e.event = "apply_w"
     /\ Calls("w", e) # {}
     /\ ApplyWrite
     /\ Calls("w", e) \cap net' = {}
     /\ e.ok = (e.u = jleader \/ Calls("w", e) \cap done # {})
  \/ /\ e.event = "lose"
     /\ Lost(e) # {}
     /\ Lose
     /\ Lost(e) \cap net' = {}
  \/ e.event = "crash" /\ Crash(e.a)
  \/ e.event = "user_multi" /\ UserMulti

TraceNext == l <= Len(Trace) /\ TraceStep /\ l' = l + 1

TraceSpec == TraceInit /\ [][TraceNext]_<<tvars, vars>>

\* Violated exactly when a behaviour matched every event: the trace is
\* accepted.
TraceUnfinished == l <= Len(Trace)
=============================================================================
