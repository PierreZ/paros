-------------------------------- MODULE Reads --------------------------------
(* Leaderless quorum reads on Matchmaker MultiPaxos, for #1016 (model of     *)
(* leaderless reads). The base is `specs/tla/zero-stall/Bypass.tla`: the     *)
(* matchmaking phase, Phase 1 over every prior configuration, Phase 2 and    *)
(* the Phase-1 bypass of #579 (zero-stall reconfiguration). This module adds *)
(* the acks to the clients and one leaderless read.                          *)
(*                                                                           *)
(* A reader asks a Phase-1 quorum of the configuration of its read basis for *)
(* vote watermarks. It serves the read at the maximum of the watermarks and  *)
(* the fence of the basis (`paros-core`, `quorum_read.rs`).                  *)
(*                                                                           *)
(* Owner, Check, Mode : as in `Bypass.tla`.                                  *)
(* Basis : "won"    = a node learns its read basis only from a leadership    *)
(*                    that won its ballot (#260).                            *)
(*         "belief" = the basis is the configuration of the ballot that the  *)
(*                    node promised, with no fence (the bug that #260 fixed).*)
(* AckBallot : TRUE = a `PreReadAck` carries the ballot of the acceptor, and *)
(*                    an answer above the basis ends the read.               *)
(* Hold  : TRUE  = the decision hold of #579 (rule 4): a leader acks a slot  *)
(*                 only when each prior configuration has a Phase-1 quorum   *)
(*                 of promises for its ballot.                               *)
(* Learner : the options of #890 (learner above the leader). A reader can   *)
(*           campaign with a stale belief: it promises its own ballot, the   *)
(*           matchmakers refuse the campaign, and it sends no Prepare.       *)
(*   "off"    = no such campaign.                                            *)
(*   "strict" = a node whose promise is above a leadership never takes it as *)
(*              its read basis (`main` today).                               *)
(*   "own"    = option (b): a node takes the basis when the only promise     *)
(*              above it is its own abandoned campaign.                      *)
(*   "any"    = a node ignores its promise for the read basis.               *)
(*   Option (a), the leader campaigns above the promise, adds no rule to the *)
(*   model: it is one more ordinary campaign.                                *)
(*                                                                           *)
(* Not in the model: flexible and grid quorums (a quorum is a majority), GC  *)
(* watermarks, matchmaker generations, the replica tier, a read that expires.*)
EXTENDS Naturals, FiniteSets

CONSTANTS Acceptors, Matchmakers, Configs, Values, Slots, NB, Owner, Check, Mode,
          Readers, Basis, AckBallot, Hold, Learner

Ballots == 1..NB
Free    == "free"
Pending == "pending"

VARIABLES status,    \* ballot -> "matching" | "matched" | "leading" | "switched" | "abandoned"
          cfgOf,     \* ballot -> its configuration
          reg,       \* matchmaker -> set of <<ballot, config>>
          replied,   \* ballot -> matchmakers that answered its registration
          hist,      \* ballot -> union of the histories in those answers
          promised,  \* acceptor -> ballot
          votes,     \* set of <<acceptor, slot, ballot, value>>
          promises,  \* set of <<acceptor, ballot, votes of the acceptor then>>
          safe,      \* ballot -> slot -> Free | Pending | the one value allowed
          proposals, \* set of <<ballot, slot, value>>
          acked,     \* slots that a leader reported as decided to a client
          basis,     \* reader -> <<ballot, fence>> of its read basis (ballot 0: none)
          rd,        \* the one read of the model
          own        \* reader -> the ballot of its own abandoned campaign (0: none)

vars == <<status, cfgOf, reg, replied, hist, promised, votes, promises, safe, proposals,
          acked, basis, rd, own>>

NoRead == [state |-> "none", node |-> CHOOSE n \in Readers : TRUE, b |-> 0, fence |-> 0,
           snap |-> {}, answers |-> {}, index |-> 0]

Quorums(c) == {Q \in SUBSET c : 2 * Cardinality(Q) > Cardinality(c)}

\* Each ballot has one configuration, selected by its owner. The selection is
\* local to the owner, thus the model makes it in the initial state. A ballot
\* that no matchmaker registers is a ballot that was never used.
Init == /\ status = [b \in Ballots |-> "matching"]
        /\ cfgOf \in [Ballots -> Configs]
        /\ reg = [m \in Matchmakers |-> {}]
        /\ replied = [b \in Ballots |-> {}]
        /\ hist = [b \in Ballots |-> {}]
        /\ promised = [a \in Acceptors |-> 0]
        /\ votes = {}
        /\ promises = {}
        /\ safe = [b \in Ballots |-> [s \in Slots |-> Free]]
        /\ proposals = {}
        /\ acked = {}
        /\ basis = [n \in Readers |-> <<0, 0>>]
        /\ rd = NoRead
        /\ own = [n \in Readers |-> 0]

\* Matchmaker m registers b if it holds no ballot at or above b, and answers
\* with what it held.
Register(b, m) ==
    /\ status[b] = "matching"
    /\ m \notin replied[b]
    /\ \A r \in reg[m] : r[1] < b
    /\ reg' = [reg EXCEPT ![m] = @ \cup {<<b, cfgOf[b]>>}]
    /\ replied' = [replied EXCEPT ![b] = @ \cup {m}]
    /\ hist' = [hist EXCEPT ![b] = @ \cup reg[m]]
    /\ UNCHANGED <<status, cfgOf, promised, votes, promises, safe, proposals, acked, basis, rd, own>>

MatchDone(b) ==
    /\ status[b] = "matching"
    /\ 2 * Cardinality(replied[b]) > Cardinality(Matchmakers)
    /\ status' = [status EXCEPT ![b] = "matched"]
    /\ UNCHANGED <<cfgOf, reg, replied, hist, promised, votes, promises, safe, proposals, acked, basis, rd, own>>

PriorCfgs(b) == {r[2] : r \in hist[b]}

HasPending(b) == \E s \in Slots : safe[b][s] = Pending

\* Acceptor a promises b and reports its votes. The owner of b sends a
\* Prepare in a normal campaign, and after a bypass (the fence Phase 1 of #579).
Promise(a, b) ==
    /\ status[b] \in {"matched", "leading"}
    /\ a \in UNION PriorCfgs(b)
    /\ promised[a] <= b
    /\ ~ \E p \in promises : p[1] = a /\ p[2] = b
    /\ promised' = [promised EXCEPT ![a] = b]
    /\ promises' = promises \cup
          {<<a, b, {<<w[2], w[3], w[4]>> : w \in {x \in votes : x[1] = a}}>>}
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, votes, safe, proposals, acked, basis, rd, own>>

\* A promise quorum of EVERY prior configuration.
Covered(b) ==
    \A c \in PriorCfgs(b) :
        \E Q \in Quorums(c) : \A a \in Q : \E p \in promises : p[1] = a /\ p[2] = b

Reports(b) == UNION {p[3] : p \in {q \in promises : q[2] = b}}

\* The value of the highest vote below b that a promise reports, or Free.
SafeAt(b, s) ==
    LET R == {r \in Reports(b) : r[1] = s /\ r[2] < b}
    IN  IF R = {} THEN Free
        ELSE (CHOOSE r \in R : \A x \in R : x[2] <= r[2])[3]

\* The normal path: a full Phase 1.
Phase1Done(b) ==
    /\ status[b] = "matched"
    /\ Covered(b)
    /\ status' = [status EXCEPT ![b] = "leading"]
    /\ safe' = [safe EXCEPT ![b] = [s \in Slots |-> SafeAt(b, s)]]
    /\ UNCHANGED <<cfgOf, reg, replied, hist, promised, votes, promises, proposals, acked, basis, rd, own>>

Propose(b, s, v) ==
    /\ status[b] = "leading"
    /\ ~ \E p \in proposals : p[1] = b /\ p[2] = s
    /\ safe[b][s] = Free \/ safe[b][s] = v
    /\ proposals' = proposals \cup {<<b, s, v>>}
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promised, votes, promises, safe, acked, basis, rd, own>>

Accept(a, b, s, v) ==
    /\ <<b, s, v>> \in proposals
    /\ a \in cfgOf[b]
    /\ promised[a] <= b
    /\ <<a, s, b, v>> \notin votes
    /\ promised' = [promised EXCEPT ![a] = b]
    /\ votes' = votes \cup {<<a, s, b, v>>}
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promises, safe, proposals, acked, basis, rd, own>>

ProposedAt(b) == {p[2] : p \in {q \in proposals : q[1] = b}}
ValAt(b, s)   == (CHOOSE p \in proposals : p[1] = b /\ p[2] = s)[3]

\* The leader proposed each value that its Phase 1 recovered.
Settled(b) == \A s \in Slots : safe[b][s] # Free => s \in ProposedAt(b)

\* b2 is the next ballot of the owner of b.
NextOwn(b, b2) ==
    /\ b2 > b
    /\ Owner[b2] = Owner[b]
    /\ \A x \in Ballots : (x > b /\ x < b2) => Owner[x] # Owner[b]

\* The switch: the leader at b moves to b2 with no Phase 1 for the free slots.
Bypass(b, b2) ==
    /\ Mode # "none"
    /\ status[b] = "leading"
    /\ status[b2] = "matched"
    /\ NextOwn(b, b2)
    /\ Settled(b)
    /\ Check => \A r \in hist[b2] : ~ (r[1] > b /\ r[1] < b2)
    /\ Mode = "p212" =>
          \A s \in ProposedAt(b) : \A t \in Slots : t < s => t \in ProposedAt(b)
    /\ status' = [status EXCEPT ![b] = "switched", ![b2] = "leading"]
    /\ safe' = [safe EXCEPT ![b2] =
          [s \in Slots |->
              IF s \in ProposedAt(b)
              THEN (IF Mode = "carry" THEN ValAt(b, s) ELSE Pending)
              ELSE Free]]
    /\ UNCHANGED <<cfgOf, reg, replied, hist, promised, votes, promises, proposals, acked, basis, rd, own>>

\* "p212" only: the Phase 1 at the new ballot for the slots below the frontier.
Phase1Low(b) ==
    /\ status[b] = "leading"
    /\ HasPending(b)
    /\ Covered(b)
    /\ safe' = [safe EXCEPT ![b] =
          [s \in Slots |-> IF safe[b][s] = Pending THEN SafeAt(b, s) ELSE safe[b][s]]]
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promised, votes, promises, proposals, acked, basis, rd, own>>

\* ---------------------------------------------------------------------------
\* The acks and the read.

Max(S) == CHOOSE x \in S : \A y \in S : y <= x

\* Reader n campaigns at b with a stale belief. It promises b, the matchmakers
\* refuse the campaign, and no acceptor gets a Prepare for b.
OwnCampaign(n, b) ==
    /\ Learner # "off"
    /\ status[b] = "matching"
    /\ own[n] = 0
    /\ promised[n] < b
    /\ status' = [status EXCEPT ![b] = "abandoned"]
    /\ promised' = [promised EXCEPT ![n] = b]
    /\ own' = [own EXCEPT ![n] = b]
    /\ UNCHANGED <<cfgOf, reg, replied, hist, votes, promises, safe, proposals, acked,
                   basis, rd>>

\* The promise of reader n does not stop a read basis at ballot b.
Below(n, b) ==
    \/ promised[n] <= b
    \/ Learner = "any"
    \/ Learner = "own" /\ promised[n] = own[n] /\ n \notin cfgOf[b]

\* The leader of b tells a client that slot s is decided.
Ack(b, s) ==
    /\ status[b] = "leading"
    /\ s \notin acked
    /\ \E v \in Values : \E Q \in Quorums(cfgOf[b]) : \A a \in Q : <<a, s, b, v>> \in votes
    /\ Hold => Covered(b)
    /\ acked' = acked \cup {s}
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promised, votes, promises, safe,
                   proposals, basis, rd, own>>

\* Everything that an earlier configuration chose is at or below the fence of
\* b: the slots that the Phase 1 of b recovered, or that a bypass carried.
FenceOf(b) == Max({s \in Slots : safe[b][s] # Free} \cup {0})

\* "won": reader n hears a leadership that won b (its beat can be late, thus
\* a ballot that its owner already left counts too).
LearnBasis(n, b) ==
    /\ Basis = "won"
    /\ status[b] \in {"leading", "switched"}
    /\ Below(n, b)
    /\ b > basis[n][1]
    /\ basis' = [basis EXCEPT ![n] = <<b, FenceOf(b)>>]
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promised, votes, promises, safe,
                   proposals, acked, rd, own>>

BasisOf(n) == IF Basis = "won" THEN basis[n] ELSE <<promised[n], 0>>

\* Reader n opens a read. A node whose promise is above its basis opens none.
ReadStart(n) ==
    /\ rd.state = "none"
    /\ BasisOf(n)[1] > 0
    /\ Below(n, BasisOf(n)[1])
    /\ rd' = [state |-> "open", node |-> n, b |-> BasisOf(n)[1], fence |-> BasisOf(n)[2],
              snap |-> acked, answers |-> {}, index |-> 0]
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promised, votes, promises, safe,
                   proposals, acked, basis, own>>

Watermark(a) == Max({w[2] : w \in {x \in votes : x[1] = a}} \cup {0})

\* Acceptor a answers the PreRead with its vote watermark and its ballot. An
\* answer above the basis ends the read.
PreReadAck(a) ==
    /\ rd.state = "open"
    /\ a \in cfgOf[rd.b]
    /\ ~ \E x \in rd.answers : x[1] = a
    /\ rd' = IF AckBallot /\ promised[a] > rd.b
             THEN NoRead
             ELSE [rd EXCEPT !.answers = @ \cup {<<a, Watermark(a)>>}]
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promised, votes, promises, safe,
                   proposals, acked, basis, own>>

\* The reader learns a newer configuration: it ends its open read.
ReadAbandon ==
    /\ rd.state = "open"
    /\ BasisOf(rd.node)[1] # rd.b \/ ~ Below(rd.node, rd.b)
    /\ rd' = NoRead
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promised, votes, promises, safe,
                   proposals, acked, basis, own>>

\* The answers are a Phase-1 quorum of the basis configuration, and the
\* prefix of the reader covers the index.
ReadServe ==
    /\ rd.state = "open"
    /\ BasisOf(rd.node)[1] = rd.b
    /\ Below(rd.node, rd.b)
    /\ {x[1] : x \in rd.answers} \in Quorums(cfgOf[rd.b])
    /\ LET i == Max({x[2] : x \in rd.answers} \cup {rd.fence})
       IN  /\ \A s \in Slots : s <= i =>
                  \E v \in Values : \E b \in Ballots : \E Q \in Quorums(cfgOf[b]) :
                      \A a \in Q : <<a, s, b, v>> \in votes
           /\ rd' = [rd EXCEPT !.state = "done", !.index = i]
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promised, votes, promises, safe,
                   proposals, acked, basis, own>>

Next ==
    \/ \E b \in Ballots, m \in Matchmakers : Register(b, m)
    \/ \E b \in Ballots : MatchDone(b) \/ Phase1Done(b) \/ Phase1Low(b)
    \/ \E a \in Acceptors, b \in Ballots : Promise(a, b)
    \/ \E b \in Ballots, s \in Slots, v \in Values : Propose(b, s, v)
    \/ \E a \in Acceptors, p \in proposals : Accept(a, p[1], p[2], p[3])
    \/ \E b, b2 \in Ballots : Bypass(b, b2)
    \/ \E b \in Ballots, s \in Slots : Ack(b, s)
    \/ \E n \in Readers, b \in Ballots : LearnBasis(n, b)
    \/ \E n \in Readers : ReadStart(n)
    \/ \E n \in Readers, b \in Ballots : OwnCampaign(n, b)
    \/ \E a \in Acceptors : PreReadAck(a)
    \/ ReadAbandon \/ ReadServe

Spec == Init /\ [][Next]_vars

Chosen(s, v) ==
    \E b \in Ballots :
        \E Q \in Quorums(cfgOf[b]) : \A a \in Q : <<a, s, b, v>> \in votes

\* Safety: one value for each slot.
Agreement == \A s \in Slots : \A v, w \in Values : (Chosen(s, v) /\ Chosen(s, w)) => v = w

\* The read half of linearizability: a served read covers each slot that was
\* acked before the read started.
ReadSeesAcked == rd.state = "done" => \A s \in rd.snap : s <= rd.index

\* Not a real invariant. TLC must find a trace where a read that started after
\* an ack is served: this shows that the model reaches the case.
NoUsefulRead == ~ (rd.state = "done" /\ rd.snap # {})

\* Not a real invariant. TLC must find the state of #890: a leader, and a
\* reader outside its configuration whose own abandoned campaign is above it.
NoStranded ==
    ~ \E n \in Readers, b \in Ballots :
        /\ status[b] = "leading" /\ n \notin cfgOf[b]
        /\ own[n] > b /\ promised[n] = own[n]

\* Not a real invariant. TLC must find a served read of a reader whose promise
\* is above its basis: this shows that the option serves the read of #890.
NoReadAbove == ~ (rd.state = "done" /\ rd.snap # {} /\ promised[rd.node] > rd.b)
=============================================================================
