------------------------------- MODULE Retire -------------------------------
(* A small model of the retirement of an acceptor in Matchmaker Paxos, for   *)
(* #1002 (retire fence gaps). It compares the rules that decide when a       *)
(* removed node may shut down.                                               *)
(*                                                                           *)
(* The model has no values and no votes. The question is only this: after a  *)
(* node retires, can a later campaign still get a Phase-1 quorum of each     *)
(* configuration that the matchmakers give it?                               *)
(*                                                                           *)
(* Fence : "none"     = the fence `last_member_ballot` moves only when the   *)
(*                      node hears a configuration that names it (the rule   *)
(*                      before #893 (retire fence)).                         *)
(*         "volatile" = a campaign also raises the fence of the node that    *)
(*                      registers. A restart sets the fence to 0 (#893).     *)
(*         "durable"  = the same, and the fence survives a restart.          *)
(* Ask   : "none"     = the node retires on its five local conditions.       *)
(*         "quorum"   = the node first asks a matchmaker quorum. A           *)
(*                      matchmaker answers yes if it holds no registration   *)
(*                      above the watermark that names the node.             *)
(*         "quorumTomb" = the same, and a matchmaker that answers yes        *)
(*                      refuses each later registration that names the node. *)
(*         "allTomb"  = as "quorumTomb", but every matchmaker must answer.   *)
(* OperatorRule : TRUE = the operator retires no node that a                 *)
(*         reconfiguration above the watermark names, and names no retired   *)
(*         node in a reconfiguration (the operator's half, #198).            *)
(* Phase1All : TRUE = a Phase-1 quorum is every member (a flexible system    *)
(*         with q1 = n). FALSE = a majority.                                 *)
(*                                                                           *)
(* Not in the model: values and votes, the promises of the acceptors,        *)
(* matchmaker generations, the pages of an answer, lost matchmaker state.    *)
EXTENDS Naturals, FiniteSets

CONSTANTS Nodes, Matchmakers, Configs, C0, Leader0, NB, Restarts,
          Fence, Ask, OperatorRule, Phase1All

Ballots == 1..NB
Belief  == "belief"
Reconf  == "reconf"

VARIABLES status,   \* ballot -> "idle" | "matching" | "matched" | "leading" | "stale"
          owner,    \* ballot -> the node that campaigns at it
          cfgOf,    \* ballot -> its configuration
          kind,     \* ballot -> Belief | Reconf
          reg,      \* matchmaker -> set of <<ballot, config, kind>>
          wm,       \* matchmaker -> GC watermark
          tomb,     \* matchmaker -> nodes it refuses to register again
          replied,  \* ballot -> matchmakers that answered its registration
          hist,     \* ballot -> union of the records in those answers
          hwm,      \* ballot -> maximum watermark in those answers
          belief,   \* node -> <<ballot, config>>: `acceptors_since`, `acceptors`
          fence,    \* node -> `last_member_ballot`
          prom,     \* node -> the highest ballot of its own campaigns (durable)
          eff,      \* the floors that a matchmaker quorum holds
          acks,     \* node -> set of <<matchmaker, watermark>>: yes answers to its ask
          retired,  \* the nodes that shut down
          restarts  \* the number of restarts (the model allows `Restarts`)

vars == <<status, owner, cfgOf, kind, reg, wm, tomb, replied, hist, hwm,
          belief, fence, prom, eff, acks, retired, restarts>>

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Up(a, b) == IF a >= b THEN a ELSE b

MMQuorums == {Q \in SUBSET Matchmakers : 2 * Cardinality(Q) > Cardinality(Matchmakers)}
Q1(c) == IF Phase1All THEN {c}
         ELSE {Q \in SUBSET c : 2 * Cardinality(Q) > Cardinality(c)}

\* Ballot 0 is the bootstrap: C0, led by Leader0, held by every matchmaker.
Init == /\ status = [b \in Ballots |-> "idle"]
        /\ owner = [b \in Ballots |-> Leader0]
        /\ cfgOf = [b \in Ballots |-> C0]
        /\ kind = [b \in Ballots |-> Belief]
        /\ reg = [m \in Matchmakers |-> {<<0, C0, Reconf>>}]
        /\ wm = [m \in Matchmakers |-> 0]
        /\ tomb = [m \in Matchmakers |-> {}]
        /\ replied = [b \in Ballots |-> {}]
        /\ hist = [b \in Ballots |-> {}]
        /\ hwm = [b \in Ballots |-> 0]
        /\ belief = [n \in Nodes |-> <<0, C0>>]
        /\ fence = [n \in Nodes |-> 0]
        /\ prom = [n \in Nodes |-> 0]
        /\ eff = {}
        /\ acks = [n \in Nodes |-> {}]
        /\ retired = {}
        /\ restarts = 0

Leading == {b \in Ballots : status[b] = "leading"}
\* n leads the highest ballot that won.
IsLeader(n) == IF Leading = {} THEN n = Leader0 ELSE owner[Max(Leading)] = n
LeaderBallot == IF Leading = {} THEN 0 ELSE Max(Leading)

\* A node has one open campaign at a time.
Busy(n) == \E b \in Ballots : owner[b] = n /\ status[b] \in {"matching", "matched"}

Open(n, b, c, k) ==
    /\ status[b] = "idle"
    /\ n \notin retired
    /\ ~ Busy(n)
    /\ b > prom[n]
    /\ status' = [status EXCEPT ![b] = "matching"]
    /\ owner' = [owner EXCEPT ![b] = n]
    /\ cfgOf' = [cfgOf EXCEPT ![b] = c]
    /\ kind' = [kind EXCEPT ![b] = k]
    /\ prom' = [prom EXCEPT ![n] = b]
    /\ fence' = [fence EXCEPT ![n] = IF Fence # "none" /\ n \in c THEN Up(@, b) ELSE @]
    /\ UNCHANGED <<reg, wm, tomb, replied, hist, hwm, belief, eff, acks, retired, restarts>>

\* An ordinary campaign: a member of its own belief registers that belief.
Campaign(n, b) ==
    /\ n \in belief[n][2]
    /\ Open(n, b, belief[n][2], Belief)

\* A reconfiguration is a round change of the leader with a new configuration.
Reconfigure(n, b, c) ==
    /\ IsLeader(n)
    /\ c # belief[n][2]
    /\ OperatorRule => c \cap retired = {}
    /\ Open(n, b, c, Reconf)

\* Matchmaker m registers b if b is above each record it holds and not below
\* its watermark. It answers with what it holds.
Register(b, m) ==
    /\ status[b] = "matching"
    /\ m \notin replied[b]
    /\ b >= wm[m]
    /\ \A r \in reg[m] : r[1] < b
    /\ cfgOf[b] \cap tomb[m] = {}
    /\ reg' = [reg EXCEPT ![m] = @ \cup {<<b, cfgOf[b], kind[b]>>}]
    /\ replied' = [replied EXCEPT ![b] = @ \cup {m}]
    /\ hist' = [hist EXCEPT ![b] = @ \cup reg[m]]
    /\ hwm' = [hwm EXCEPT ![b] = Up(@, wm[m])]
    /\ UNCHANGED <<status, owner, cfgOf, kind, wm, tomb, belief, fence, prom, eff, acks, retired, restarts>>

Reconfs(b) == {r \in hist[b] : r[3] = Reconf}

\* The matchmaking phase closes at a quorum. A campaign whose answers name a
\* reconfiguration above the belief of its owner stops, and the owner adopts
\* that configuration (`MatchStep::StaleConfiguration`). Its registration stays.
MatchDone(b) ==
    /\ status[b] = "matching"
    /\ replied[b] \in MMQuorums
    /\ LET n == owner[b]
           R == {r \in Reconfs(b) : r[1] > belief[n][1]}
       IN  IF R = {}
           THEN /\ status' = [status EXCEPT ![b] = "matched"]
                /\ UNCHANGED <<belief, fence, restarts>>
           ELSE LET e == CHOOSE r \in R : \A x \in R : x[1] <= r[1]
                IN  /\ status' = [status EXCEPT ![b] = "stale"]
                    /\ belief' = [belief EXCEPT ![n] = <<e[1], e[2]>>]
                    /\ fence' = [fence EXCEPT ![n] = IF n \in e[2] THEN Up(@, e[1]) ELSE @]
    /\ UNCHANGED <<owner, cfgOf, kind, reg, wm, tomb, replied, hist, hwm, prom, eff, acks, retired, restarts>>

\* H_b: the configurations in the answers, at or above the maximum watermark.
PriorCfgs(b) == {r[2] : r \in {x \in hist[b] : x[1] >= hwm[b]}}

Alive(c) == \E Q \in Q1(c) : Q \cap retired = {}

\* Phase 1 needs a quorum of each configuration in H_b. A node that did not
\* retire always answers.
Phase1Done(b) ==
    /\ status[b] = "matched"
    /\ \A c \in PriorCfgs(b) : Alive(c)
    /\ status' = [status EXCEPT ![b] = "leading"]
    /\ UNCHANGED <<owner, cfgOf, kind, reg, wm, tomb, replied, hist, hwm,
                   belief, fence, prom, eff, acks, retired, restarts>>

\* Node n follows the leadership of b: it learns the configuration, bound to b.
Hear(n, b) ==
    /\ status[b] = "leading"
    /\ n \notin retired
    /\ b >= prom[n]
    /\ b > belief[n][1]
    /\ belief' = [belief EXCEPT ![n] = <<b, cfgOf[b]>>]
    /\ fence' = [fence EXCEPT ![n] = IF n \in cfgOf[b] THEN Up(@, b) ELSE @]
    /\ UNCHANGED <<status, owner, cfgOf, kind, reg, wm, tomb, replied, hist, hwm,
                   prom, eff, acks, retired, restarts>>

\* The leader of b raises the watermark of m to b. m drops the records below.
Gc(b, m) ==
    /\ status[b] = "leading"
    /\ wm[m] < b
    /\ wm' = [wm EXCEPT ![m] = b]
    /\ reg' = [reg EXCEPT ![m] = {r \in @ : r[1] >= b}]
    /\ UNCHANGED <<status, owner, cfgOf, kind, tomb, replied, hist, hwm,
                   belief, fence, prom, eff, acks, retired, restarts>>

\* The floor b is effective: a matchmaker quorum holds it.
Effective(b) ==
    /\ status[b] = "leading"
    /\ b \notin eff
    /\ \E Q \in MMQuorums : \A m \in Q : wm[m] >= b
    /\ eff' = eff \cup {b}
    /\ UNCHANGED <<status, owner, cfgOf, kind, reg, wm, tomb, replied, hist, hwm,
                   belief, fence, prom, acks, retired, restarts>>

\* A restart: the belief goes back to the bootstrap. The fence too, unless
\* it is durable.
Restart(n) ==
    /\ n \notin retired
    /\ restarts < Restarts
    /\ restarts' = restarts + 1
    /\ belief' = [belief EXCEPT ![n] = <<0, C0>>]
    /\ fence' = [fence EXCEPT ![n] = IF Fence = "durable" THEN @ ELSE 0]
    /\ acks' = [acks EXCEPT ![n] = {}]
    /\ UNCHANGED <<status, owner, cfgOf, kind, reg, wm, tomb, replied, hist, hwm,
                   prom, eff, retired>>

\* The membership probe: n adopts the reconfiguration that a matchmaker
\* quorum reports as effective (the `effective` scalar, which GC keeps).
EffCfg(Q) ==
    LET B == {b \in Ballots : kind[b] = Reconf /\ replied[b] \cap Q # {}}
    IN  IF B = {} THEN <<0, C0>> ELSE <<Max(B), cfgOf[Max(B)]>>

Probe(n, Q) ==
    /\ n \notin retired
    /\ LET e == EffCfg(Q)
       IN  /\ e[1] > belief[n][1]
           /\ belief' = [belief EXCEPT ![n] = e]
           /\ fence' = [fence EXCEPT ![n] = IF n \in e[2] THEN Up(@, e[1]) ELSE @]
    /\ UNCHANGED <<status, owner, cfgOf, kind, reg, wm, tomb, replied, hist, hwm,
                   prom, eff, acks, retired, restarts>>

\* The five conditions of `ColocatedNode::may_retire`.
MayRetire(n, w) ==
    /\ n \notin retired
    /\ w \in eff
    /\ n \notin belief[n][2]
    /\ ~ IsLeader(n)
    /\ w > fence[n]
    /\ belief[n][1] = w

\* The ask of #1002: m says yes if no record above w names n.
AskOne(n, w, m) ==
    /\ Ask # "none"
    /\ MayRetire(n, w)
    /\ <<m, w>> \notin acks[n]
    /\ \A r \in reg[m] : ~ (r[1] > w /\ n \in r[2])
    /\ acks' = [acks EXCEPT ![n] = @ \cup {<<m, w>>}]
    /\ tomb' = [tomb EXCEPT ![m] = IF Ask \in {"quorumTomb", "allTomb"} THEN @ \cup {n} ELSE @]
    /\ UNCHANGED <<status, owner, cfgOf, kind, reg, wm, replied, hist, hwm,
                   belief, fence, prom, eff, retired, restarts>>

Asked(n, w) ==
    LET A == {m \in Matchmakers : <<m, w>> \in acks[n]}
    IN  CASE Ask = "none" -> TRUE
          [] Ask = "allTomb" -> A = Matchmakers
          [] OTHER -> A \in MMQuorums

Retire(n, w) ==
    /\ MayRetire(n, w)
    /\ Asked(n, w)
    /\ OperatorRule =>
          ~ \E b \in Ballots : b > w /\ status[b] # "idle" /\ kind[b] = Reconf /\ n \in cfgOf[b]
    /\ retired' = retired \cup {n}
    /\ UNCHANGED <<status, owner, cfgOf, kind, reg, wm, tomb, replied, hist, hwm,
                   belief, fence, prom, eff, acks, restarts>>

Next ==
    \/ \E n \in Nodes, b \in Ballots : Campaign(n, b) \/ Hear(n, b)
    \/ \E n \in Nodes, b \in Ballots, c \in Configs : Reconfigure(n, b, c)
    \/ \E b \in Ballots, m \in Matchmakers : Register(b, m) \/ Gc(b, m)
    \/ \E b \in Ballots : MatchDone(b) \/ Phase1Done(b) \/ Effective(b)
    \/ \E n \in Nodes : Restart(n)
    \/ \E n \in Nodes, Q \in MMQuorums : Probe(n, Q)
    \/ \E n \in Nodes, w \in Ballots, m \in Matchmakers : AskOne(n, w, m)
    \/ \E n \in Nodes, w \in Ballots : Retire(n, w)

Spec == Init /\ [][Next]_vars

\* What a campaign gets from the matchmaker quorum Q.
Answer(Q) ==
    LET W == Max({wm[m] : m \in Q})
    IN  {r[2] : r \in {x \in UNION {reg[m] : m \in Q} : x[1] >= W}}

\* The property: no retirement strands a campaign. Each matchmaker quorum
\* gives only configurations that still have a Phase-1 quorum.
Electable == \A Q \in MMQuorums : \A c \in Answer(Q) : Alive(c)

\* The weak form: one matchmaker quorum is enough.
SomeElectable == \E Q \in MMQuorums : \A c \in Answer(Q) : Alive(c)

\* Not a real invariant. TLC must find a trace where a node retires: this
\* shows that the model reaches a retirement.
NoRetirement == retired = {}
=============================================================================
