------------------------- MODULE CampaignDiscovery -------------------------
(* A small model of how a candidate learns the matchmaker set in force, for *)
(* #1034 (a campaign's re-ask also asks the matchmaker pool outside its     *)
(* set), which extends the probe rule of #858 (boot from the cached         *)
(* registry) to campaigns.                                                  *)
(*                                                                          *)
(* The matchmaker sets of a tenant are a chain of generations, Sets[1] for  *)
(* generation 0, Sets[2] for generation 1, and so on. A handover moves the  *)
(* generation in force to the next one, and it bootstraps every member of  *)
(* its target. A matchmaker that leaves the set in force can be gone for    *)
(* good. A matchmaker of an older generation is frozen, and it may learn    *)
(* its successor (`Stopped { successor }`).                                 *)
(*                                                                          *)
(* One candidate believes a generation (`belief`), for example the birth    *)
(* set of its tenant, and campaigns. It asks matchmakers. A matchmaker that *)
(* knows a later generation answers with a `Generation` refusal that names  *)
(* it, and the candidate adopts that generation. A matchmaker active in the *)
(* generation it believes registers the campaign.                           *)
(*                                                                          *)
(* Discover        : TRUE  = a re-ask also goes to the pool outside the    *)
(*                           believed set (#1034).                         *)
(*                   FALSE = a re-ask goes to the believed set only.       *)
(* JudgeGeneration : TRUE  = a matchmaker registers only a campaign that   *)
(*                           names its own active generation              *)
(*                           (`judge_generation`).                         *)
(*                   FALSE = a control: an active matchmaker registers any *)
(*                           campaign.                                     *)
(*                                                                          *)
(* Not in the model: ballots, votes, Phase 1, the registrations' contents, *)
(* lost matchmaker state, more than one candidate and the pages of an      *)
(* answer.                                                                 *)
EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS Sets, Discover, JudgeGeneration

N == Len(Sets)
Members(g) == Sets[g + 1]
Pool == UNION {Sets[i] : i \in 1..N}

VARIABLES active,   \* the generation in force
          know,     \* matchmaker -> the latest generation it holds as in force
          gone,     \* the matchmakers that are gone for good
          belief,   \* the generation the candidate believes
          regs      \* <<matchmaker, generation named, generation in force>>

vars == <<active, know, gone, belief, regs>>

Init == /\ active = 0
        /\ know = [m \in Pool |-> 0]
        /\ gone = {}
        /\ belief = 0
        /\ regs = {}

\* A handover to the next generation: every member of its target holds the
\* bootstrap, thus none of them is gone.
Handover == /\ active < N - 1
            /\ Members(active + 1) \cap gone = {}
            /\ active' = active + 1
            /\ know' = [m \in Pool |->
                          IF m \in Members(active + 1) THEN active + 1 ELSE know[m]]
            /\ UNCHANGED <<gone, belief, regs>>

\* A frozen matchmaker learns its successor (`Stopped { successor }`).
LearnSuccessor(m) == /\ m \notin gone
                     /\ know[m] < active
                     /\ know' = [know EXCEPT ![m] = active]
                     /\ UNCHANGED <<active, gone, belief, regs>>

\* A matchmaker outside the set in force is gone for good.
Leave(m) == /\ m \notin gone
            /\ m \notin Members(active)
            /\ gone' = gone \cup {m}
            /\ UNCHANGED <<active, know, belief, regs>>

\* The matchmakers a re-ask of the campaign addresses.
Asked == IF Discover THEN Pool ELSE Members(belief)

\* A refusal that names a later generation: the candidate adopts it. An
\* outsider's refusal counts only when the later set is wholly in the pool.
Refuse(m) == /\ m \in Asked
             /\ m \notin gone
             /\ know[m] > belief
             /\ Members(know[m]) \subseteq Pool
             /\ belief' = know[m]
             /\ UNCHANGED <<active, know, gone, regs>>

\* A matchmaker active in the generation in force registers the campaign.
Register(m) == /\ m \in Asked
               /\ m \notin gone
               /\ m \in Members(active)
               /\ know[m] = active
               /\ JudgeGeneration => belief = know[m]
               /\ regs' = regs \cup {<<m, belief, active>>}
               /\ UNCHANGED <<active, know, gone, belief>>

Next == \/ Handover
        \/ \E m \in Pool : LearnSuccessor(m) \/ Leave(m) \/ Refuse(m) \/ Register(m)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
\* The candidate never believes a generation that is not in force yet.
AdoptInForce == belief <= active

\* A campaign is registered only under the generation in force.
RegInForce == \A r \in regs : r[2] = r[3]

\* A candidate behind the generation in force can still learn it: some
\* matchmaker it asks knows a later generation, or a frozen one can still
\* learn its successor and then tell it.
Learnable == \E m \in Asked \ gone : know[m] > belief
NotStuck == belief < active =>
               \/ Learnable
               \/ \E m \in Asked \ gone : ENABLED LearnSuccessor(m)
=============================================================================
