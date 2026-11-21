---------------------------- MODULE GridController ----------------------------
(***************************************************************************)
(* The fenced reshape call of the grid controller, and the election of the *)
(* controller, #1259 (the fenced reshape call of the grid controller).     *)
(*                                                                         *)
(* Pierre's decisions (2026-11-19 and 2026-11-20): the controller is its   *)
(* own role in each journal, on stateless machines, and not the Paxos      *)
(* leader. It is elected by a term held in the journal's matchmaker        *)
(* record. Its only effect on the journal is                               *)
(*     reshape(expected configuration, new grid, controller term),         *)
(* a compare-and-set on that record. The controller decides when and       *)
(* where; the journal's guards decide whether it is safe. So the model     *)
(* lets a controller ask for any grid of `Grids`, from any configuration   *)
(* it read, at any time, and judges only what the record lets in.          *)
(*                                                                         *)
(* The matchmaker record is one atomic register here, as in the segments   *)
(* experiment of #1213 (`experiments/segments/a/`): `matchmaker/`,         *)
(* `handover/` and `cas-register/` check how a matchmaker quorum keeps a   *)
(* compare-and-set register, and `quorum-everywhere/` (#1257) checks that  *)
(* each write to it waits for a majority only. The record holds the term,  *)
(* the holder of the term and the segment table: configuration g has a     *)
(* grid, the configuration it was planned from, and a state.               *)
(*                                                                         *)
(*   pending   the reshape registered g; no leader runs it yet.            *)
(*   inforce   a leader of g sealed g - 1 (its Phase 1 in both grids) and  *)
(*             writes in g.                                                *)
(*   sealed    g + 1 is in force; g still holds slots that no replica set  *)
(*             of F + 1 has.                                               *)
(*   retired   Matchmaker garbage collection forgot g.                     *)
(*                                                                         *)
(* The seal and the retire are steps of the journal, abstract here: the    *)
(* seal needs a live Phase-1 quorum of the old grid and live Phase-1 and   *)
(* Phase-2 quorums of the new grid; `experiments/segments/a/` checks the   *)
(* seal end and the retire rule, and `quorum-everywhere/` their quorums.   *)
(*                                                                         *)
(* A grid here is one column: `mem`, its rows, and `acks`, its Phase-2     *)
(* quorum (`acks` of `mem`); its Phase-1 quorum is `rows - acks + 1` of    *)
(* `mem`. `GridCheck.tla` checks that the bound of #1257 is the same for   *)
(* every column count, so one column is enough for the bound.              *)
(*                                                                         *)
(* A controller is stateless: a crash forgets its term and its plan. It    *)
(* campaigns in two steps: it reads the term, then writes term + 1 by      *)
(* compare-and-set on the term it read. A candidate may depose a holder    *)
(* that is alive; the deposed holder does not know, and keeps calling      *)
(* reshape with its old term until a refusal tells it. A call goes over a  *)
(* network that can lose a request or a reply; a controller sends its      *)
(* request again while it has no answer. Acceptors crash and stay down.    *)
(* Crashes, losses and acceptor failures are bounded: after they stop,     *)
(* the liveness properties apply.                                          *)
(*                                                                         *)
(* Constants that select the rules. TRUE is the rule; FALSE is a control.  *)
(*   Raise        an election writes term + 1. FALSE: it keeps the term.   *)
(*   TermCheck    a reshape needs the term of the record.                  *)
(*   ExpCheck     a reshape needs the expected configuration to be the     *)
(*                newest one of the table.                                 *)
(*   RetireGuard  a reshape needs the newest configuration in force and    *)
(*                every older one retired.                                 *)
(*   BoundGuard   a reshape needs F + 1 <= acks <= rows - F (#1257).       *)
(*   Idempotent   a request that the record already took is answered ok    *)
(*                again (the record keeps the request's identity). The    *)
(*                model found this rule; Pierre decided it on 2026-11-20.  *)
(*   Reach        the name of an event that the invariant `NotReached`     *)
(*                says never happens, or "none": a reachability check.     *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Ctrls, Acc, Grids, G0, F, NG, MaxTerm, Crashes, Losses, Fails,
          Raise, TermCheck, ExpCheck, RetireGuard, BoundGuard, Idempotent,
          Reach

None == "none"
\* An empty grid, request and record: TLC compares a record only with a record.
NoGrid == [mem |-> {}, acks |-> 0]
Gens == 1..NG
Rows(G) == Cardinality(G.mem)
Bounded(G) == F + 1 <= G.acks /\ G.acks <= Rows(G) - F

ASSUME G0 \in Grids /\ Bounded(G0)
ASSUME NoGrid \notin Grids
ASSUME \A G \in Grids : G.mem \subseteq Acc /\ G.acks \in 1..Rows(G)
ASSUME NG >= 2 /\ MaxTerm >= 1
ASSUME {Raise, TermCheck, ExpCheck, RetireGuard, BoundGuard, Idempotent} \subseteq BOOLEAN
\* A failure stays inside the contract: at most F acceptors down in a grid
\* (bend, do not break).
ASSUME Fails <= F

VARIABLES
  term, holder,     \* the matchmaker record: the controller term and its holder
  top, rec, st,     \* the segment table: the newest configuration, each record, each state
  ctl,              \* each controller's local state, lost at a crash
  reqs, reps,       \* requests and replies in flight
  failed,           \* acceptors that are down
  crashes, losses,  \* faults used so far
  ev                \* ghost: the events that happened, for the reachability checks

vars == <<term, holder, top, rec, st, ctl, reqs, reps, failed, crashes, losses, ev>>

States == {"none", "pending", "inforce", "sealed", "retired"}
CtlStates == {"down", "idle", "cand", "lead"}
Answers == {"ok", "term", "exp", "unretired", "bound", "full"}

Live1(G) == Cardinality(G.mem \ failed) >= Rows(G) - G.acks + 1
Live2(G) == Cardinality(G.mem \ failed) >= G.acks

\* The request that controller c sends for its plan. A request is its own
\* identity: the record keeps it to answer a retry.
Req(c) == [c |-> c, tm |-> ctl[c].tm, exp |-> ctl[c].exp, grid |-> ctl[c].plan]

\* The ghost `ev` records only the event that `Reach` names (and the event
\* that it follows), so the checks that do not need it keep `ev` empty and
\* the state space small.
Note(e) ==
  IF \/ Reach = e
     \/ e = "takeover" /\ Reach = "open_after_takeover"
     \/ e = "crash_after_plan" /\ Reach = "open_after_crash"
  THEN {e} ELSE {}

Taken(m) == \E g \in Gens : st[g] # "none" /\ rec[g].rid = m

InFlight(c) == Req(c) \in reqs \/ \E m \in reps : m.req = Req(c)

Idle == [st |-> "idle", seen |-> 0, tm |-> 0, plan |-> NoGrid, exp |-> 0]

NoReq == [c |-> None, tm |-> 0, exp |-> 0, grid |-> NoGrid]
NoRec == [grid |-> NoGrid, from |-> 0, by |-> None, tm |-> 0, rid |-> NoReq]

TypeOK ==
  /\ term \in 0..MaxTerm
  /\ holder \in Ctrls \cup {None}
  /\ top \in Gens
  /\ st \in [Gens -> States]
  /\ \A g \in Gens : (rec[g] = NoRec) <=> (st[g] = "none")
  /\ \A c \in Ctrls : ctl[c].st \in CtlStates /\ ctl[c].plan \in Grids \cup {NoGrid}
  /\ failed \subseteq Acc
  /\ \A m \in reps : m.ans \in Answers

\* Only a holder (maybe deposed) has a plan.
PlanOnlyLead == \A c \in Ctrls : ctl[c].plan # NoGrid => ctl[c].st = "lead"

Init ==
  /\ term = 0
  /\ holder = None
  /\ top = 1
  /\ rec = [g \in Gens |-> IF g = 1
                           THEN [NoRec EXCEPT !.grid = G0]
                           ELSE NoRec]
  /\ st = [g \in Gens |-> IF g = 1 THEN "inforce" ELSE "none"]
  /\ ctl = [c \in Ctrls |-> Idle]
  /\ reqs = {}
  /\ reps = {}
  /\ failed = {}
  /\ crashes = 0
  /\ losses = 0
  /\ ev = {}

(***************************************************************************)
(* The election.                                                           *)
(***************************************************************************)
\* A candidate reads the term.
ReadTerm(c) ==
  /\ ctl[c].st = "idle"
  /\ term < MaxTerm
  /\ ctl' = [ctl EXCEPT ![c] = [Idle EXCEPT !.st = "cand", !.seen = term]]
  /\ UNCHANGED <<term, holder, top, rec, st, reqs, reps, failed, crashes, losses, ev>>

\* The compare-and-set on the term it read. Two candidates that read the
\* same term race here, and one loses.
Elect(c) ==
  /\ ctl[c].st = "cand"
  /\ IF term = ctl[c].seen
     THEN LET t == IF Raise THEN term + 1 ELSE term
          IN /\ term' = t
             /\ holder' = c
             /\ ctl' = [ctl EXCEPT ![c] = [Idle EXCEPT !.st = "lead", !.tm = t]]
             /\ ev' = ev \cup
                  (IF holder \in Ctrls /\ holder # c /\ ctl[holder].st = "down"
                   THEN Note("takeover") ELSE {})
                  \cup
                  (IF holder \in Ctrls /\ holder # c /\ ctl[holder].st = "lead"
                   THEN Note("deposed") ELSE {})
     ELSE /\ ctl' = [ctl EXCEPT ![c] = Idle]
          /\ ev' = ev \cup Note("lost_race")
          /\ UNCHANGED <<term, holder>>
  /\ UNCHANGED <<top, rec, st, reqs, reps, failed, crashes, losses>>

(***************************************************************************)
(* The controller's plan and its call.                                     *)
(***************************************************************************)
\* A holder (or a deposed holder that does not know it) reads the table and
\* plans any grid. Its view can be stale when the call arrives.
Plan(c) ==
  /\ ctl[c].st = "lead"
  /\ ctl[c].plan = NoGrid
  /\ top < NG
  /\ \E G \in Grids : ctl' = [ctl EXCEPT ![c].plan = G, ![c].exp = top]
  /\ UNCHANGED <<term, holder, top, rec, st, reqs, reps, failed, crashes, losses, ev>>

\* The call, and its retry while no request or reply of it is in flight.
\* Only a holder has a plan (`PlanOnlyLead`).
Call(c) ==
  /\ ctl[c].plan # NoGrid
  /\ ~InFlight(c)
  /\ reqs' = reqs \cup {Req(c)}
  /\ UNCHANGED <<term, holder, top, rec, st, ctl, reps, failed, crashes, losses, ev>>

(***************************************************************************)
(* The compare-and-set at the matchmaker record.                           *)
(***************************************************************************)
\* `Next` takes m from `reqs`, and a reply from `reps`.
Process(m) ==
  /\ LET termOk  == ~TermCheck \/ m.tm = term
         expOk   == ~ExpCheck \/ m.exp = top
         retOk   == ~RetireGuard \/ (st[top] = "inforce" /\ \A g \in 1..(top - 1) : st[g] = "retired")
         boundOk == ~BoundGuard \/ Bounded(m.grid)
         \* `full` is the bound of the model (NG configurations), not a rule.
         ans == CASE Idempotent /\ Taken(m) -> "ok"
                  [] ~termOk -> "term"
                  [] ~expOk -> "exp"
                  [] ~retOk -> "unretired"
                  [] ~boundOk -> "bound"
                  [] top = NG -> "full"
                  [] OTHER -> "ok"
         opens == ans = "ok" /\ ~(Idempotent /\ Taken(m))
         g == top + 1
     IN /\ IF opens
           THEN /\ top' = g
                /\ rec' = [rec EXCEPT ![g] = [grid |-> m.grid, from |-> m.exp, by |-> m.c,
                                              tm |-> m.tm, rid |-> m]]
                /\ st' = [st EXCEPT ![g] = "pending"]
           ELSE UNCHANGED <<top, rec, st>>
        /\ reqs' = reqs \ {m}
        /\ reps' = reps \cup {[c |-> m.c, req |-> m, ans |-> ans]}
        /\ ev' = ev
                 \cup (IF ans = "term" THEN Note("stale_refused") ELSE {})
                 \cup (IF ans = "unretired" /\ \E h \in Gens : st[h] = "sealed"
                       THEN Note("sealed_refused") ELSE {})
                 \cup (IF opens /\ "takeover" \in ev THEN Note("open_after_takeover") ELSE {})
                 \cup (IF opens /\ "crash_after_plan" \in ev THEN Note("open_after_crash") ELSE {})
  /\ UNCHANGED <<term, holder, ctl, failed, crashes, losses>>

\* A reply reaches its controller. A reply to an older request of the
\* controller (before a crash or a new term) is dropped. A refusal for the
\* term tells the controller it was deposed.
Receive(m) ==
  /\ reps' = reps \ {m}
  /\ IF ctl[m.c].plan # NoGrid /\ m.req = Req(m.c)
     THEN ctl' = [ctl EXCEPT ![m.c] = IF m.ans = "term" THEN Idle
                                     ELSE [@ EXCEPT !.plan = NoGrid, !.exp = 0]]
     ELSE UNCHANGED ctl
  /\ UNCHANGED <<term, holder, top, rec, st, reqs, failed, crashes, losses, ev>>

(***************************************************************************)
(* The journal's steps.                                                    *)
(***************************************************************************)
\* A leader of g seals g - 1: Phase 1 in both grids, then Phase 2 in g.
\* `Next` takes g in 2..NG. A pending g has g - 1 in force: a reshape opens
\* g only from g - 1 in force (`RetireGuard`), and only `SealIn(g)` moves
\* g - 1 then (`Chain`).
SealIn(g) ==
  /\ st[g] = "pending"
  /\ Live1(rec[g - 1].grid)
  /\ Live1(rec[g].grid)
  /\ Live2(rec[g].grid)
  /\ st' = [st EXCEPT ![g - 1] = "sealed", ![g] = "inforce"]
  /\ ev' = ev \cup (IF rec[g].grid.mem \cap failed # {} THEN Note("inforce_with_failed") ELSE {})
  /\ UNCHANGED <<term, holder, top, rec, ctl, reqs, reps, failed, crashes, losses>>

\* F + 1 replicas hold the sealed segment and a matchmaker quorum acks.
Retire(g) ==
  /\ st[g] = "sealed"
  /\ st' = [st EXCEPT ![g] = "retired"]
  /\ UNCHANGED <<term, holder, top, rec, ctl, reqs, reps, failed, crashes, losses, ev>>

(***************************************************************************)
(* Faults.                                                                 *)
(***************************************************************************)
\* A crash of a controller that is down changes nothing but the count.
Crash(c) ==
  /\ crashes < Crashes
  /\ crashes' = crashes + 1
  /\ ctl' = [ctl EXCEPT ![c] = [Idle EXCEPT !.st = "down"]]
  /\ ev' = ev \cup (IF ctl[c].plan # NoGrid /\ ~InFlight(c) THEN Note("crash_after_plan") ELSE {})
  /\ UNCHANGED <<term, holder, top, rec, st, reqs, reps, failed, losses>>

Restart(c) ==
  /\ ctl[c].st = "down"
  /\ ctl' = [ctl EXCEPT ![c] = Idle]
  /\ UNCHANGED <<term, holder, top, rec, st, reqs, reps, failed, crashes, losses, ev>>

LoseReq(m) ==
  /\ losses < Losses
  /\ reqs' = reqs \ {m}
  /\ losses' = losses + 1
  /\ UNCHANGED <<term, holder, top, rec, st, ctl, reps, failed, crashes, ev>>

LoseRep(m) ==
  /\ losses < Losses
  /\ reps' = reps \ {m}
  /\ losses' = losses + 1
  /\ ev' = ev \cup (IF m.ans = "ok" THEN Note("ok_lost") ELSE {})
  /\ UNCHANGED <<term, holder, top, rec, st, ctl, reqs, failed, crashes>>

\* An acceptor fails, maybe between a plan that names it and the call. A
\* second failure of a failed acceptor changes nothing.
Fail(a) ==
  /\ Cardinality(failed) < Fails
  /\ failed' = failed \cup {a}
  /\ ev' = ev \cup (IF \E c \in Ctrls : a \in ctl[c].plan.mem
                    THEN Note("fail_after_plan") ELSE {})
  /\ UNCHANGED <<term, holder, top, rec, st, ctl, reqs, reps, crashes, losses>>

Next ==
  \/ \E c \in Ctrls : ReadTerm(c) \/ Elect(c) \/ Plan(c) \/ Call(c) \/ Crash(c) \/ Restart(c)
  \/ \E m \in reqs : Process(m) \/ LoseReq(m)
  \/ \E m \in reps : Receive(m) \/ LoseRep(m)
  \/ \E g \in 2..NG : SealIn(g)
  \/ \E g \in Gens : Retire(g)
  \/ \E a \in Acc : Fail(a)

\* Fairness for the steps that must happen: the record answers each request,
\* each reply arrives, a controller sends its request again, and the journal
\* seals and retires. No fairness for an election, a plan or a fault.
Fairness ==
  /\ \A c \in Ctrls :
       /\ WF_vars(\E m \in reqs : m.c = c /\ Process(m))
       /\ WF_vars(\E m \in reps : m.c = c /\ Receive(m))
       /\ WF_vars(Call(c))
  /\ \A g \in 2..NG : WF_vars(SealIn(g))
  /\ \A g \in Gens : WF_vars(Retire(g))

Spec == Init /\ [][Next]_vars /\ Fairness

(***************************************************************************)
(* Properties.                                                             *)
(***************************************************************************)
\* At most one segment opens from one configuration.
OneFromEach ==
  \A g, h \in 2..NG : g # h /\ st[g] # "none" /\ st[h] # "none" => rec[g].from # rec[h].from

\* Only the holder of the current term opens a segment.
HolderOnly == [][top' # top => rec'[top'].by = holder /\ rec'[top'].tm = term]_vars

\* A stale term has no effect: a request whose term is not the record's
\* changes nothing in the table.
StaleNoEffect ==
  [][\A m \in reqs : m.tm # term /\ m \notin reqs' => UNCHANGED <<top, rec, st>>]_vars

\* No second reshape starts while a segment is unretired: one configuration
\* in force and at most one other that is not retired (plan rule 1).
OneUnretired == Cardinality({g \in Gens : st[g] \in {"pending", "inforce", "sealed"}}) <= 2

\* Every grid in the table meets F + 1 <= acks <= rows - F (#1257, PR #1269).
AllBounded == \A g \in Gens : st[g] # "none" => Bounded(rec[g].grid)

\* An answer is true: "ok" when the record holds the request, a refusal
\* when it does not.
AnswerTrue == \A m \in reps : (m.ans = "ok") <=> Taken(m.req)

\* After the faults stop, a reshape that was asked is answered (completed
\* or refused), unless its controller crashed, which drops the plan.
Answered == \A c \in Ctrls : ctl[c].plan # NoGrid ~> ctl[c].plan = NoGrid

\* After the faults stop, a configuration that a reshape opened comes into
\* force.
Settles == \A g \in Gens : st[g] = "pending" ~> st[g] # "pending"

\* The same for a grid that meets the bound. The control `live_nobound_old`
\* breaks it: a grid in force outside the bound blocks the seal of the next
\* grid, even of a grid that meets the bound.
SettlesBounded ==
  \A g \in Gens : st[g] = "pending" /\ Bounded(rec[g].grid) ~> st[g] # "pending"

\* A controller's life: a holder gives up the lead only when a refusal for
\* its term tells it, or at a crash (it keeps calling until then); only a
\* candidate becomes the holder; a candidate goes back to idle only when it
\* lost the compare-and-set; a controller that is down only restarts.
CtlSteps ==
  [][\A c \in Ctrls :
       LET s == ctl[c].st
           s2 == ctl'[c].st
       IN /\ s = "lead" /\ s2 # "lead" =>
               \/ crashes' > crashes
               \/ \E m \in reps \ reps' : m.c = c /\ m.ans = "term"
          /\ s2 = "lead" /\ s # "lead" => s = "cand"
          /\ s = "cand" /\ s2 = "idle" => term # ctl[c].seen
          /\ s = "down" => s2 \in {"down", "idle"}]_vars

\* Each configuration of the table moves one way, one state at a time:
\* none, pending, in force, sealed, retired.
StRank(x) == CASE x = "none" -> 0 [] x = "pending" -> 1 [] x = "inforce" -> 2
                [] x = "sealed" -> 3 [] x = "retired" -> 4
Chain ==
  [][\A g \in Gens : st'[g] = st[g] \/ StRank(st'[g]) = StRank(st[g]) + 1]_vars

\* A reachability check: the event `Reach` never happens. Its config must
\* find a violation, which shows that the model reaches the case.
NotReached == Reach \notin ev
=============================================================================
