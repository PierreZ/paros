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
(*         "quorumPrune" = as "quorumTomb", and a candidate drops a          *)
(*                      registration from its answers when a matchmaker      *)
(*                      quorum in those answers recorded, below that         *)
(*                      registration, the retirement of a node that it names.*)
(* OperatorRule : TRUE = the operator retires no node that a                 *)
(*         reconfiguration above the watermark names, and names no retired   *)
(*         node in a reconfiguration (the operator's half, #198).            *)
(* Rejoin : what a retired node does when a later booking starts it again   *)
(*         from its kept store (#1002, PR #1037). "off" = it never comes     *)
(*         back (the model before PR #1037). "ignored" = it comes back and   *)
(*         asks every matchmaker to drop its record, and each ignores the    *)
(*         ask (a control). "early" = a matchmaker drops the record before   *)
(*         the node is back (a control). "untagged" = the node is back, then *)
(*         each matchmaker drops the record; an ask of an earlier            *)
(*         incarnation can still arrive late (a control). "tagged" = the     *)
(*         same, and a matchmaker ignores an ask of an incarnation older     *)
(*         than the last rejoin it took.                                     *)
(* Kinds : ballot -> the kind a campaign at it may have ("any", "belief" or  *)
(*         "reconf"). It keeps the rejoin checks small: a shrink, a stale   *)
(*         campaign of the old configuration, then a regrow.                *)
(* Phase1All : TRUE = a Phase-1 quorum is every member (a flexible system    *)
(*         with q1 = n). FALSE = a majority.                                 *)
(*                                                                           *)
(* Not in the model: values and votes, the promises of the acceptors,        *)
(* matchmaker generations, the pages of an answer, lost matchmaker state.    *)
EXTENDS Naturals, FiniteSets

CONSTANTS Nodes, Matchmakers, Configs, C0, Leader0, NB, Restarts, Comebacks,
          Fence, Ask, OperatorRule, Phase1All, Rejoin, Kinds

Ballots == 1..NB
Belief  == "belief"
Reconf  == "reconf"

VARIABLES status,   \* ballot -> "idle" | "matching" | "matched" | "leading" | "stale"
          owner,    \* ballot -> the node that campaigns at it
          cfgOf,    \* ballot -> its configuration
          kind,     \* ballot -> Belief | Reconf
          reg,      \* matchmaker -> set of <<ballot, config, kind>>
          wm,       \* matchmaker -> GC watermark
          tomb,     \* matchmaker -> set of <<node, watermark, incarnation>>: the retirements it said yes to
          replied,  \* ballot -> matchmakers that answered its registration
          hist,     \* ballot -> union of the records in those answers
          hwm,      \* ballot -> maximum watermark in those answers
          belief,   \* node -> <<ballot, config>>: `acceptors_since`, `acceptors`
          fence,    \* node -> `last_member_ballot`
          prom,     \* node -> the highest ballot of its own campaigns (durable)
          eff,      \* the floors that a matchmaker quorum holds
          acks,     \* node -> set of <<matchmaker, watermark>>: yes answers to its ask
          retired,  \* the nodes that shut down
          restarts, \* the number of restarts (the model allows `Restarts`)
          inc,      \* node -> its incarnation: one more at each comeback
          asks,     \* the retirement asks in flight: <<node, watermark, incarnation>>
          gone,     \* matchmaker -> node -> the incarnation of the last rejoin it took
          joined,   \* node -> the matchmakers that took its rejoin
          back      \* the nodes that came back after a retirement

rvars == <<inc, asks, gone, joined, back>>
vars == <<status, owner, cfgOf, kind, reg, wm, tomb, replied, hist, hwm,
          belief, fence, prom, eff, acks, retired, restarts, inc, asks, gone, joined, back>>

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
        /\ inc = [n \in Nodes |-> 0]
        /\ asks = {}
        /\ gone = [m \in Matchmakers |-> [n \in Nodes |-> 0]]
        /\ joined = [n \in Nodes |-> {}]
        /\ back = {}

Leading == {b \in Ballots : status[b] = "leading"}
\* n leads the highest ballot that won.
IsLeader(n) == IF Leading = {} THEN n = Leader0 ELSE owner[Max(Leading)] = n
LeaderBallot == IF Leading = {} THEN 0 ELSE Max(Leading)

\* A node has one open campaign at a time.
Busy(n) == \E b \in Ballots : owner[b] = n /\ status[b] \in {"matching", "matched"}

Open(n, b, c, k) ==
    /\ status[b] = "idle"
    /\ Kinds[b] \in {"any", k}
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
    /\ UNCHANGED rvars

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
    /\ \A t \in tomb[m] : t[1] \notin cfgOf[b]
    /\ reg' = [reg EXCEPT ![m] = @ \cup {<<b, cfgOf[b], kind[b]>>}]
    /\ replied' = [replied EXCEPT ![b] = @ \cup {m}]
    /\ hist' = [hist EXCEPT ![b] = @ \cup reg[m]]
    /\ hwm' = [hwm EXCEPT ![b] = Up(@, wm[m])]
    /\ UNCHANGED <<status, owner, cfgOf, kind, wm, tomb, belief, fence, prom, eff, acks, retired, restarts>>
    /\ UNCHANGED rvars

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
    /\ UNCHANGED rvars

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
    /\ UNCHANGED rvars

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
    /\ UNCHANGED rvars

\* The leader of b raises the watermark of m to b. m drops the records below.
Gc(b, m) ==
    /\ status[b] = "leading"
    /\ wm[m] < b
    /\ wm' = [wm EXCEPT ![m] = b]
    /\ reg' = [reg EXCEPT ![m] = {r \in @ : r[1] >= b}]
    /\ UNCHANGED <<status, owner, cfgOf, kind, tomb, replied, hist, hwm,
                   belief, fence, prom, eff, acks, retired, restarts>>
    /\ UNCHANGED rvars

\* The floor b is effective: a matchmaker quorum holds it.
Effective(b) ==
    /\ status[b] = "leading"
    /\ b \notin eff
    /\ \E Q \in MMQuorums : \A m \in Q : wm[m] >= b
    /\ eff' = eff \cup {b}
    /\ UNCHANGED <<status, owner, cfgOf, kind, reg, wm, tomb, replied, hist, hwm,
                   belief, fence, prom, acks, retired, restarts>>
    /\ UNCHANGED rvars

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
    /\ UNCHANGED rvars

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
    /\ UNCHANGED rvars

\* The five conditions of `ColocatedNode::may_retire`.
MayRetire(n, w) ==
    /\ n \notin retired
    /\ w \in eff
    /\ n \notin belief[n][2]
    /\ ~ IsLeader(n)
    /\ w > fence[n]
    /\ belief[n][1] = w

\* The ask of #1002: m says yes if no record above w names n. With Rejoin
\* "off", m judges the ask while n may still retire. Otherwise the ask is a
\* message: n sends it while it may retire, and m judges it at any later time.
SendAsk(n, w) ==
    /\ Ask # "none"
    /\ Rejoin # "off"
    /\ MayRetire(n, w)
    /\ <<n, w, inc[n]>> \notin asks
    /\ asks' = asks \cup {<<n, w, inc[n]>>}
    /\ UNCHANGED <<status, owner, cfgOf, kind, reg, wm, tomb, replied, hist, hwm,
                   belief, fence, prom, eff, acks, retired, restarts, inc, gone, joined, back>>

AskOne(n, w, e, m) ==
    /\ Ask # "none"
    /\ IF Rejoin = "off" THEN MayRetire(n, w) /\ e = inc[n] ELSE <<n, w, e>> \in asks
    /\ <<m, w, e>> \notin acks[n]
    /\ \A r \in reg[m] : ~ (r[1] > w /\ n \in r[2])
    /\ Rejoin = "tagged" => e >= gone[m][n]
    /\ acks' = [acks EXCEPT ![n] = @ \cup {<<m, w, e>>}]
    /\ tomb' = [tomb EXCEPT ![m] = IF Ask # "quorum" THEN @ \cup {<<n, w, e>>} ELSE @]
    /\ UNCHANGED <<status, owner, cfgOf, kind, reg, wm, replied, hist, hwm,
                   belief, fence, prom, eff, retired, restarts>>
    /\ UNCHANGED rvars

Asked(n, w) ==
    LET A == {m \in Matchmakers : <<m, w, inc[n]>> \in acks[n]}
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
    /\ UNCHANGED rvars

\* A later booking starts the journal of a retired node again, from its kept
\* store: its promise and its fence are as they were. Its incarnation grows.
Comeback(n) ==
    /\ Rejoin # "off"
    /\ n \in retired
    /\ Cardinality(back) < Comebacks
    /\ retired' = retired \ {n}
    /\ back' = back \cup {n}
    /\ inc' = [inc EXCEPT ![n] = @ + 1]
    /\ joined' = [joined EXCEPT ![n] = IF Rejoin = "early" THEN @ ELSE {}]
    /\ acks' = [acks EXCEPT ![n] = {}]
    /\ UNCHANGED <<status, owner, cfgOf, kind, reg, wm, tomb, replied, hist, hwm,
                   belief, fence, prom, eff, restarts, asks, gone>>

\* m takes the rejoin of n: it drops each record that names n. With "early",
\* m takes it while n is still retired. With "tagged", it drops only the
\* records of earlier incarnations: a record of the incarnation that rejoins
\* is a new retirement of the node that came back.
RejoinOne(n, m) ==
    /\ Rejoin # "off"
    /\ m \notin joined[n]
    /\ IF Rejoin = "early"
       THEN n \in retired /\ Cardinality(back) < Comebacks
       ELSE n \in back /\ n \notin retired
    /\ joined' = [joined EXCEPT ![n] = @ \cup {m}]
    /\ tomb' = [tomb EXCEPT ![m] =
          CASE Rejoin = "ignored" -> @
            [] Rejoin = "tagged" -> {t \in @ : t[1] # n \/ t[3] >= inc[n]}
            [] OTHER -> {t \in @ : t[1] # n}]
    /\ gone' = [gone EXCEPT ![m][n] = IF Rejoin = "tagged" THEN inc[n] ELSE @]
    /\ UNCHANGED <<status, owner, cfgOf, kind, reg, wm, replied, hist, hwm,
                   belief, fence, prom, eff, acks, retired, restarts, inc, asks, back>>

Next ==
    \/ \E n \in Nodes, b \in Ballots : Campaign(n, b) \/ Hear(n, b)
    \/ \E n \in Nodes, b \in Ballots, c \in Configs : Reconfigure(n, b, c)
    \/ \E b \in Ballots, m \in Matchmakers : Register(b, m) \/ Gc(b, m)
    \/ \E b \in Ballots : MatchDone(b) \/ Phase1Done(b) \/ Effective(b)
    \/ \E n \in Nodes : Restart(n)
    \/ \E n \in Nodes, Q \in MMQuorums : Probe(n, Q)
    \/ \E n \in Nodes, w \in Ballots : SendAsk(n, w) \/ Retire(n, w)
    \/ \E n \in Nodes, w \in Ballots, e \in 0..Comebacks, m \in Matchmakers : AskOne(n, w, e, m)
    \/ \E n \in Nodes : Comeback(n)
    \/ \E n \in Nodes, m \in Matchmakers : RejoinOne(n, m)

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

\* "quorumPrune": what a candidate drops when the matchmakers of A answered.
PrunedBy(A, r) ==
    /\ Ask = "quorumPrune"
    /\ \E n \in r[2] : \E T \in MMQuorums :
          /\ T \subseteq A
          /\ \A m \in T : \E t \in tomb[m] : t[1] = n /\ t[2] < r[1]

Records(A) ==
    LET W == Max({wm[m] : m \in A})
    IN  {x \in UNION {reg[m] : m \in A} : x[1] >= W}

AnswerP(A) == {r[2] : r \in {x \in Records(A) : ~ PrunedBy(A, x)}}

\* With an answer of every matchmaker, a campaign is not stranded.
ElectableAll == \A c \in AnswerP(Matchmakers) : Alive(c)

\* The same with one matchmaker down, whichever it is.
ElectableOneDown ==
    \A A \in SUBSET Matchmakers :
        Cardinality(A) + 1 >= Cardinality(Matchmakers) => \A c \in AnswerP(A) : Alive(c)

\* A candidate drops only a registration whose campaign never completed its
\* matchmaking phase. Such a campaign sent no Prepare and no Accept, thus its
\* configuration holds no vote at that ballot.
PruneSafe ==
    \A A \in SUBSET Matchmakers : A # {} =>
        \A r \in Records(A) :
            (r[1] > 0 /\ PrunedBy(A, r)) => status[r[1]] \in {"idle", "matching"}

\* A node that came back holds, at each matchmaker that took its rejoin, no
\* record of an earlier incarnation. Judged per matchmaker, so it holds for
\* any set of them: once a matchmaker quorum took the rejoin, a
\* reconfiguration naming the node registers there. Nothing waits for the
\* rejoin to reach every matchmaker (#1037 (rejoin), 2026-11-12).
Nameable ==
    \A n \in back : n \notin retired =>
        \A m \in joined[n] : \A t \in tomb[m] : t[1] = n => t[3] = inc[n]

\* Not a real invariant. TLC must find a trace where a reconfiguration that
\* names a node that came back leads.
NoRenamed ==
    ~ \E b \in Ballots : status[b] = "leading" /\ kind[b] = Reconf /\ cfgOf[b] \cap back # {}

\* Not a real invariant. TLC must find a trace where a node retires: this
\* shows that the model reaches a retirement.
NoRetirement == retired = {}
=============================================================================
