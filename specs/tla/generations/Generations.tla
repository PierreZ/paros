----------------------------- MODULE Generations -----------------------------
(***************************************************************************)
(* The matchmaker set handover as the code runs it again and again, over   *)
(* the state of one journal (#1306 (trace specs extend the checked         *)
(* model)): `HandoverTrace` extends this module, and TLC checks it here on *)
(* bounded instances (`checks.txt`).                                       *)
(*                                                                         *)
(* `handover/` checks one handover with the target rules, and              *)
(* `handover-chain/` a chain of fixed targets with clients that reach one  *)
(* member at a time. This model has what the walk of `consensus-sim` does: *)
(* a chain of generations to targets drawn from the matchmaker pool, a     *)
(* member that skips a generation, several sets proposed for one           *)
(* generation, and the journal's registrations and GC floor carried from   *)
(* each generation to the next (`matchmaker/reconfigurer.rs`,              *)
(* `matchmaker/generation.rs`). The steps:                                 *)
(*   - `Stop`: a member of generation g that serves it freezes, and its    *)
(*     state is frozen with it;                                            *)
(*   - `Boot`: a member of a set proposed for generation g holds its       *)
(*     bootstrap: the floor and the registrations that the close of the    *)
(*     stop quorum gives (the maximum floor of a majority of generation    *)
(*     g - 1 that froze, and their registrations at or above it), or a     *)
(*     bootstrap of the same set that a peer holds already;                *)
(*   - `Vote`: a member of g, frozen or still serving, votes in the decree *)
(*     that chooses the successor of g, for a set whose every member holds *)
(*     its bootstrap. The decree is Paxos, and `decree/` checks it: here a *)
(*     vote is any vote that keeps one set chosen for each generation, a   *)
(*     superset of the votes that Paxos makes. `chosen` holds the set the  *)
(*     decree chose;                                                       *)
(*   - `Activate`: a member of the set chosen for g serves g, with the     *)
(*     state of its bootstrap, from any earlier generation;                *)
(*   - `Register` and `GcAck`: a member that serves registers a ballot at  *)
(*     or above its floor, or raises its floor.                            *)
(*                                                                         *)
(* Constants: `Matchmakers`, the pool; `Bootstrap`, the set of generation  *)
(* 0; `Targets`, the sets a handover may propose; `NG`, the last           *)
(* generation; `NB`, the last ballot of a registration or a floor; `ND`,   *)
(* the last decree ballot.                                                 *)
(*   Close  "stop": a bootstrap is the close of a majority of generation   *)
(*          g - 1 that froze (the rule). "one": a control, the frozen      *)
(*          state of one member. "live": a control, the state of a         *)
(*          majority of generation g - 1 that need not have frozen.        *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS Matchmakers, Bootstrap, Targets, NG, NB, ND, Close

Gens == 0..NG
Ballots == 0..NB
Decrees == 1..ND

ASSUME Bootstrap \in Targets /\ \A s \in Targets : s # {} /\ s \subseteq Matchmakers
ASSUME Close \in {"stop", "one", "live"}

VARIABLES gen,    \* matchmaker: its generation
          phase,  \* matchmaker: "active", "stopped" or "inactive"
          wm,     \* matchmaker: the journal's floor
          reg,    \* matchmaker: the journal's registrations
          frozen, \* set of [mm, gen, wm, regs]: the state a member froze with
          held,   \* set of <<m, g, b>>: m held the registration b in g
          acked,  \* set of <<m, g, b>>: m acked the floor b in g
          boots,  \* set of [mm, gen, set, wm, regs]: whole bootstraps
          votes,  \* set of [mm, gen, bal, set]: decree votes
          chosen  \* generation: the successor set its decree chose ({}: none)

vars == <<gen, phase, wm, reg, frozen, held, acked, boots, votes, chosen>>

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Majorities(S) == {Q \in SUBSET S : 2 * Cardinality(Q) > Cardinality(S)}

\* The sets that a majority of the members S of generation g voted for at
\* one decree ballot, among the votes V.
ChosenIn(V, S, g) ==
  {s \in {v.set : v \in V} :
     \E b \in Decrees, Q \in Majorities(S) :
       \A m \in Q : [mm |-> m, gen |-> g, bal |-> b, set |-> s] \in V}

\* The members of generation g: the bootstrap set, then the chosen
\* successor of each generation ({} when none is chosen yet).
Set(g) == IF g = 0 THEN Bootstrap ELSE chosen[g - 1]
ChosenBy(S, g) == ChosenIn(votes, S, g)

\* The floor b is effective in generation g: a majority acked it or above.
Effective(g, b) ==
  \E Q \in Majorities(Set(g)) : \A m \in Q : \E c \in b..NB : <<m, g, c>> \in acked

\* A majority of generation g held the registration b.
Kept(g, b) == \E Q \in Majorities(Set(g)) : \A m \in Q : <<m, g, b>> \in held

TypeOK ==
  /\ gen \in [Matchmakers -> Gens]
  /\ phase \in [Matchmakers -> {"active", "stopped", "inactive"}]
  /\ wm \in [Matchmakers -> Ballots]
  /\ reg \in [Matchmakers -> SUBSET Ballots]

Init ==
  /\ gen = [m \in Matchmakers |-> 0]
  /\ phase = [m \in Matchmakers |-> IF m \in Bootstrap THEN "active" ELSE "inactive"]
  /\ wm = [m \in Matchmakers |-> 0]
  /\ reg = [m \in Matchmakers |-> {}]
  /\ frozen = {}
  /\ held = {}
  /\ acked = {}
  /\ boots = {}
  /\ votes = {}
  /\ chosen = [g \in Gens |-> {}]

-----------------------------------------------------------------------------

\* Member m of the generation it serves freezes with its state.
Stop(m) ==
  /\ phase[m] = "active" /\ m \in Set(gen[m])
  /\ phase' = [phase EXCEPT ![m] = "stopped"]
  /\ frozen' = frozen \cup {[mm |-> m, gen |-> gen[m], wm |-> wm[m], regs |-> reg[m]]}
  /\ UNCHANGED <<gen, wm, reg, held, acked, boots, votes, chosen>>

\* The close of the states R: the maximum floor, and the registrations at
\* or above it.
Closes(R, w, regs) ==
  /\ w = Max({r.wm : r \in R})
  /\ regs = {b \in UNION {r.regs : r \in R} : b >= w}

\* A bootstrap of generation g that a close may give.
Closed(g, w, regs) ==
  CASE Close = "stop" ->
         \E R \in SUBSET {r \in frozen : r.gen = g - 1} :
           /\ \A r, t \in R : r.mm = t.mm => r = t
           /\ {r.mm : r \in R} \in Majorities(Set(g - 1))
           /\ Closes(R, w, regs)
    [] Close = "one" ->
         \E r \in frozen : r.gen = g - 1 /\ r.mm \in Set(g - 1) /\ w = r.wm /\ regs = r.regs
    [] Close = "live" ->
         \E Q \in Majorities(Set(g - 1)) :
           /\ \A m \in Q : gen[m] = g - 1
           /\ Closes({[wm |-> wm[m], regs |-> reg[m]] : m \in Q}, w, regs)

\* Member m of the set s proposed for generation g holds its bootstrap.
Boot(m, g, s, w, regs) ==
  /\ g \in 1..NG /\ s \in Targets /\ m \in s
  /\ Set(g - 1) # {}
  /\ \/ \E r \in boots : r.gen = g /\ r.set = s /\ r.wm = w /\ r.regs = regs
     \/ Closed(g, w, regs)
  /\ boots' = boots \cup {[mm |-> m, gen |-> g, set |-> s, wm |-> w, regs |-> regs]}
  /\ UNCHANGED <<gen, phase, wm, reg, frozen, held, acked, votes, chosen>>

\* Member m of generation g votes for the set s at decree ballot b. Every
\* member of s holds its bootstrap of s, and one set stays chosen.
Vote(m, g, b, s) ==
  LET V == votes \cup {[mm |-> m, gen |-> g, bal |-> b, set |-> s]} IN
  /\ g \in Gens /\ b \in Decrees /\ s \in Targets
  /\ m \in Set(g) /\ gen[m] = g /\ phase[m] # "inactive"
  /\ \A x \in s : \E r \in boots : r.mm = x /\ r.gen = g + 1 /\ r.set = s
  /\ \A v \in votes : v.gen = g /\ v.bal = b => v.set = s
  /\ chosen[g] \in {{}, s}
  /\ votes' = V
  /\ chosen' = IF s \in ChosenIn(V, Set(g), g) THEN [chosen EXCEPT ![g] = s] ELSE chosen
  /\ UNCHANGED <<gen, phase, wm, reg, frozen, held, acked, boots>>

\* Member m of the set chosen for generation g serves g, with the state of
\* its bootstrap.
Activate(m, g, s, w, regs) ==
  /\ g \in 1..NG /\ gen[m] < g
  /\ s = Set(g) /\ m \in s
  /\ [mm |-> m, gen |-> g, set |-> s, wm |-> w, regs |-> regs] \in boots
  /\ gen' = [gen EXCEPT ![m] = g]
  /\ phase' = [phase EXCEPT ![m] = "active"]
  /\ wm' = [wm EXCEPT ![m] = w]
  /\ reg' = [reg EXCEPT ![m] = regs]
  /\ held' = held \cup {<<m, g, b>> : b \in regs}
  /\ UNCHANGED <<frozen, acked, boots, votes, chosen>>

\* Member m that serves registers ballot b at or above its floor.
Register(m, b) ==
  /\ phase[m] = "active" /\ b >= wm[m]
  /\ reg' = [reg EXCEPT ![m] = @ \cup {b}]
  /\ held' = held \cup {<<m, gen[m], b>>}
  /\ UNCHANGED <<gen, phase, wm, frozen, acked, boots, votes, chosen>>

\* Member m that serves raises its floor to b and forgets what is below.
GcAck(m, b) ==
  /\ phase[m] = "active"
  /\ wm' = [wm EXCEPT ![m] = IF b > @ THEN b ELSE @]
  /\ reg' = [reg EXCEPT ![m] = {c \in @ : c >= wm'[m]}]
  /\ acked' = acked \cup {<<m, gen[m], b>>}
  /\ UNCHANGED <<gen, phase, frozen, held, boots, votes, chosen>>

Next ==
  \/ \E m \in Matchmakers : Stop(m)
  \/ \E m \in Matchmakers, g \in 1..NG, s \in Targets, w \in Ballots, regs \in SUBSET Ballots :
       Boot(m, g, s, w, regs) \/ Activate(m, g, s, w, regs)
  \/ \E m \in Matchmakers, g \in Gens, b \in Decrees, s \in Targets : Vote(m, g, b, s)
  \/ \E m \in Matchmakers, b \in Ballots : Register(m, b) \/ GcAck(m, b)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Safety *)

\* Each generation has at most one chosen successor.
OneSuccessor ==
  \A g \in Gens : Set(g) # {} => Cardinality(ChosenBy(Set(g), g)) <= 1

\* A member that serves a later generation holds every floor that a
\* majority of an earlier generation acked.
FloorCarried ==
  \A m \in Matchmakers, g \in Gens, b \in Ballots :
    phase[m] = "active" /\ g < gen[m] /\ Effective(g, b) => wm[m] >= b

\* A member that serves a later generation holds every registration that a
\* majority of an earlier generation held, at or above its floor.
RegsCarried ==
  \A m \in Matchmakers, g \in Gens, b \in Ballots :
    phase[m] = "active" /\ g < gen[m] /\ Kept(g, b) /\ b >= wm[m] => b \in reg[m]

\* Reachability controls: each must be violated.
NoActivation == \A m \in Matchmakers : gen[m] = 0
NoSkip == \A m \in Matchmakers : gen[m] <= 1 \/ \E r \in frozen : r.mm = m /\ r.gen = gen[m] - 1
=============================================================================
