---------------------------- MODULE HandoverTrace ----------------------------
(***************************************************************************)
(* The trace spec of the matchmaker set handover, on the `handover` walk  *)
(* of the `consensus-sim` cluster (#1231 (traced walks reach the rules),   *)
(* `crates/sim/consensus-sim/src/cluster/trace/handover.rs`).              *)
(*                                                                         *)
(* `Handover.tla` hands generation 0 to one successor. The walk hands a    *)
(* set over again and again, to targets it draws from the matchmaker pool, *)
(* so this module does not extend `Handover`: it states its rules for a    *)
(* chain of generations, over the state of one journal (its GC floor and   *)
(* its registrations; the walk's sibling journal is its own). The steps:   *)
(*   - "stop": a member of generation g that serves it freezes (`Stop`);   *)
(*   - "boot": a member of a set proposed for generation g holds its whole *)
(*     bootstrap, the floor `wm` and the registrations `regs` (`Close`,    *)
(*     `Bootstrap`). The state keeps every floor that a majority of        *)
(*     generation g - 1 acked, and every registration at or above its      *)
(*     floor that a majority of g - 1 held, and it holds no registration   *)
(*     that g - 1 never held (`CloseState` = "max");                       *)
(*   - "vote": a member of g, frozen or still serving, votes in the        *)
(*     decree that chooses the successor of g, for a set whose every       *)
(*     member holds its bootstrap (`FirstWrite` = "every"). A set is       *)
(*     chosen once a majority of g voted for it at one decree ballot       *)
(*     (`Decide`). A vote needs no freeze: the acceptors of `Decide` in    *)
(*     `Handover.tla` are the members of the old set, whatever their       *)
(*     phase, and the code's decree acceptor answers before its own        *)
(*     matchmaker froze. The state of the proposed set is judged at its    *)
(*     "boot";                                                             *)
(*   - "activate": a member of the set chosen for g serves g, with the     *)
(*     state of its bootstrap of that set (`Activate`);                    *)
(*   - "register" and "gcack": a matchmaker that serves its generation     *)
(*     registers a ballot at or above its floor, or raises its floor.      *)
(*                                                                         *)
(* `OneSuccessor` is the decree's agreement: each generation has at most   *)
(* one chosen successor.                                                   *)
(***************************************************************************)
EXTENDS Trace, Naturals, FiniteSets, Sequences

VARIABLES l,      \* the index of the next event
          gen,    \* matchmaker: its generation
          phase,  \* matchmaker: "active", "stopped" or "inactive"
          wm,     \* matchmaker: the journal's floor
          reg,    \* matchmaker: the journal's registrations
          held,   \* set of <<m, g, b>>: m held the registration b in g
          acked,  \* set of <<m, g, b>>: m acked the floor b in g
          boots,  \* set of [mm, gen, set, wm, regs]: whole bootstraps
          votes   \* set of [mm, gen, bal, set]: decree votes

vars == <<l, gen, phase, wm, reg, held, acked, boots, votes>>

e == Trace[l]

\* Whether an event before the current one is a `name` event that agrees
\* with the current one on each field of `fs`. The walk records a
\* registration and a GC ack of a (matchmaker, ballot) the first time only,
\* and a decree vote when the matchmaker's vote changes: a second record of
\* such a step is no step of the run.
TracedBefore(name, fs) ==
  \E j \in 1..(l - 1) :
    /\ Trace[j].event = name
    /\ \A f \in fs : f \in DOMAIN Trace[j] /\ f \in DOMAIN e /\ Trace[j][f] = e[f]
Has(f) == f \in DOMAIN e

Matchmakers == TraceMatchmakers
Gens == 0..TraceGens
Ballots == 0..TraceNB

Majorities(S) == {Q \in SUBSET S : 2 * Cardinality(Q) > Cardinality(S)}

\* The sets that a majority of the members S of generation g voted for at
\* one decree ballot.
ChosenBy(S, g) ==
  {s \in {v.set : v \in votes} :
     \E b \in 1..TraceND, Q \in Majorities(S) :
       \A m \in Q : [mm |-> m, gen |-> g, bal |-> b, set |-> s] \in votes}

RECURSIVE Set(_)
\* The members of generation g: the bootstrap set, then the chosen
\* successor of each generation ({} when none is chosen yet).
Set(g) ==
  IF g = 0 THEN TraceBootstrap
  ELSE LET c == IF Set(g - 1) = {} THEN {} ELSE ChosenBy(Set(g - 1), g - 1)
       IN IF c = {} THEN {} ELSE CHOOSE s \in c : TRUE

\* The floor b is effective in generation g: a majority acked it or above.
Effective(g, b) ==
  \E Q \in Majorities(Set(g)) : \A m \in Q : \E c \in b..TraceNB : <<m, g, c>> \in acked

\* A majority of generation g held the registration b.
Kept(g, b) == \E Q \in Majorities(Set(g)) : \A m \in Q : <<m, g, b>> \in held

Init ==
  /\ l = 1
  /\ gen = [m \in Matchmakers |-> 0]
  /\ phase = [m \in Matchmakers |-> IF m \in TraceBootstrap THEN "active" ELSE "inactive"]
  /\ wm = [m \in Matchmakers |-> 0]
  /\ reg = [m \in Matchmakers |-> {}]
  /\ held = {}
  /\ acked = {}
  /\ boots = {}
  /\ votes = {}

IsMatchmaker == Has("mm") /\ e.mm \in Matchmakers
IsGen == Has("gen") /\ e.gen \in Gens

Stop ==
  /\ e.event = "stop" /\ IsMatchmaker /\ IsGen
  /\ gen[e.mm] = e.gen /\ phase[e.mm] = "active" /\ e.mm \in Set(e.gen)
  /\ phase' = [phase EXCEPT ![e.mm] = "stopped"]
  /\ UNCHANGED <<gen, wm, reg, held, acked, boots, votes>>

Boot ==
  /\ e.event = "boot" /\ IsMatchmaker /\ IsGen /\ Has("set") /\ Has("wm") /\ Has("regs")
  /\ LET g == e.gen IN
     /\ g >= 1 /\ e.mm \in e.set /\ e.set \subseteq Matchmakers
     /\ \A b \in Ballots : Effective(g - 1, b) => e.wm >= b
     /\ \A b \in Ballots : Kept(g - 1, b) /\ b >= e.wm => b \in e.regs
     /\ \A b \in e.regs : b >= e.wm /\ \E m \in Matchmakers : <<m, g - 1, b>> \in held
     /\ boots' = boots \cup {[mm |-> e.mm, gen |-> g, set |-> e.set, wm |-> e.wm, regs |-> e.regs]}
  /\ UNCHANGED <<gen, phase, wm, reg, held, acked, votes>>

Vote ==
  /\ e.event = "vote" /\ IsMatchmaker /\ IsGen /\ Has("bal") /\ Has("set")
  /\ ~TracedBefore("vote", {"mm", "gen", "bal"})
  /\ LET g == e.gen IN
     /\ e.mm \in Set(g) /\ gen[e.mm] = g /\ phase[e.mm] # "inactive"
     /\ \A x \in e.set : \E r \in boots : r.mm = x /\ r.gen = g + 1 /\ r.set = e.set
     /\ votes' = votes \cup {[mm |-> e.mm, gen |-> g, bal |-> e.bal, set |-> e.set]}
  /\ UNCHANGED <<gen, phase, wm, reg, held, acked, boots>>

Activate ==
  /\ e.event = "activate" /\ IsMatchmaker /\ IsGen /\ Has("set") /\ Has("wm") /\ Has("regs")
  /\ LET g == e.gen IN
     /\ g >= 1 /\ gen[e.mm] < g
     /\ e.set = Set(g) /\ e.mm \in e.set
     /\ [mm |-> e.mm, gen |-> g, set |-> e.set, wm |-> e.wm, regs |-> e.regs] \in boots
     /\ gen' = [gen EXCEPT ![e.mm] = g]
     /\ phase' = [phase EXCEPT ![e.mm] = "active"]
     /\ wm' = [wm EXCEPT ![e.mm] = e.wm]
     /\ reg' = [reg EXCEPT ![e.mm] = e.regs]
     /\ held' = held \cup {<<e.mm, g, b>> : b \in e.regs}
  /\ UNCHANGED <<acked, boots, votes>>

Register ==
  /\ e.event = "register" /\ IsMatchmaker /\ Has("bal") /\ e.bal \in Ballots
  /\ ~TracedBefore("register", {"mm", "bal"})
  /\ phase[e.mm] = "active" /\ e.bal >= wm[e.mm]
  /\ reg' = [reg EXCEPT ![e.mm] = @ \cup {e.bal}]
  /\ held' = held \cup {<<e.mm, gen[e.mm], e.bal>>}
  /\ UNCHANGED <<gen, phase, wm, acked, boots, votes>>

GcAck ==
  /\ e.event = "gcack" /\ IsMatchmaker /\ Has("bal") /\ e.bal \in Ballots
  /\ ~TracedBefore("gcack", {"mm", "bal"})
  /\ phase[e.mm] = "active"
  /\ wm' = [wm EXCEPT ![e.mm] = IF e.bal > @ THEN e.bal ELSE @]
  /\ reg' = [reg EXCEPT ![e.mm] = {b \in @ : b >= wm'[e.mm]}]
  /\ acked' = acked \cup {<<e.mm, gen[e.mm], e.bal>>}
  /\ UNCHANGED <<gen, phase, held, boots, votes>>

Next ==
  /\ l <= Len(Trace)
  /\ Stop \/ Boot \/ Vote \/ Activate \/ Register \/ GcAck
  /\ l' = l + 1

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Safety *)

\* Each generation has at most one chosen successor.
OneSuccessor ==
  \A g \in Gens : Set(g) # {} => Cardinality(ChosenBy(Set(g), g)) <= 1

\* Violated exactly when a behaviour matched every event: the trace is
\* accepted.
TraceUnfinished == l <= Len(Trace)
=============================================================================
