------------------------------- MODULE Bypass -------------------------------
(* A small model of Matchmaker MultiPaxos with Phase-1 bypassing, for        *)
(* #579 (zero-stall reconfiguration). It compares the ways to bypass.        *)
(*                                                                           *)
(* Owner  : which proposer owns each ballot. <<1, 1, 2, 2>> is the paper's   *)
(*          (r, id, s): a proposer owns two consecutive ballots.             *)
(*          <<1, 2, 1, 2>> is paros's (round, node): a ballot of the other   *)
(*          proposer is between two ballots of one proposer.                 *)
(* Check  : TRUE = bypass only if the matchmaker answers show no             *)
(*          registration between the two ballots.                            *)
(* Mode   : "none"  = no bypass (plain Matchmaker MultiPaxos).               *)
(*          "carry" = the leader proposes its own values again at the new    *)
(*                    ballot, with no Phase 1.                               *)
(*          "p212"  = frankenpaxos Phase212: the slots above the frontier    *)
(*                    are free at once; the slots below get a Phase 1 at     *)
(*                    the new ballot.                                        *)
(*                                                                           *)
(* Not in the model: GC watermarks, matchmaker generations, leaderless       *)
(* reads, lost durable state, the cooperative handoff.                       *)
EXTENDS Naturals, FiniteSets

CONSTANTS Acceptors, Matchmakers, Configs, Values, Slots, NB, Owner, Check, Mode

Ballots == 1..NB
Free    == "free"
Pending == "pending"

VARIABLES status,    \* ballot -> "matching" | "matched" | "leading" | "switched"
          cfgOf,     \* ballot -> its configuration
          reg,       \* matchmaker -> set of <<ballot, config>>
          replied,   \* ballot -> matchmakers that answered its registration
          hist,      \* ballot -> union of the histories in those answers
          promised,  \* acceptor -> ballot
          votes,     \* set of <<acceptor, slot, ballot, value>>
          promises,  \* set of <<acceptor, ballot, votes of the acceptor then>>
          safe,      \* ballot -> slot -> Free | Pending | the one value allowed
          proposals  \* set of <<ballot, slot, value>>

vars == <<status, cfgOf, reg, replied, hist, promised, votes, promises, safe, proposals>>

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

\* Matchmaker m registers b if it holds no ballot at or above b, and answers
\* with what it held.
Register(b, m) ==
    /\ status[b] = "matching"
    /\ m \notin replied[b]
    /\ \A r \in reg[m] : r[1] < b
    /\ reg' = [reg EXCEPT ![m] = @ \cup {<<b, cfgOf[b]>>}]
    /\ replied' = [replied EXCEPT ![b] = @ \cup {m}]
    /\ hist' = [hist EXCEPT ![b] = @ \cup reg[m]]
    /\ UNCHANGED <<status, cfgOf, promised, votes, promises, safe, proposals>>

MatchDone(b) ==
    /\ status[b] = "matching"
    /\ 2 * Cardinality(replied[b]) > Cardinality(Matchmakers)
    /\ status' = [status EXCEPT ![b] = "matched"]
    /\ UNCHANGED <<cfgOf, reg, replied, hist, promised, votes, promises, safe, proposals>>

PriorCfgs(b) == {r[2] : r \in hist[b]}

HasPending(b) == \E s \in Slots : safe[b][s] = Pending

\* Acceptor a promises b and reports its votes. The owner of b sends a
\* Prepare in a normal campaign, or after a "p212" bypass.
Promise(a, b) ==
    /\ \/ status[b] = "matched"
       \/ status[b] = "leading" /\ HasPending(b)
    /\ a \in UNION PriorCfgs(b)
    /\ promised[a] <= b
    /\ ~ \E p \in promises : p[1] = a /\ p[2] = b
    /\ promised' = [promised EXCEPT ![a] = b]
    /\ promises' = promises \cup
          {<<a, b, {<<w[2], w[3], w[4]>> : w \in {x \in votes : x[1] = a}}>>}
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, votes, safe, proposals>>

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
    /\ UNCHANGED <<cfgOf, reg, replied, hist, promised, votes, promises, proposals>>

Propose(b, s, v) ==
    /\ status[b] = "leading"
    /\ ~ \E p \in proposals : p[1] = b /\ p[2] = s
    /\ safe[b][s] = Free \/ safe[b][s] = v
    /\ proposals' = proposals \cup {<<b, s, v>>}
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promised, votes, promises, safe>>

Accept(a, b, s, v) ==
    /\ <<b, s, v>> \in proposals
    /\ a \in cfgOf[b]
    /\ promised[a] <= b
    /\ <<a, s, b, v>> \notin votes
    /\ promised' = [promised EXCEPT ![a] = b]
    /\ votes' = votes \cup {<<a, s, b, v>>}
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promises, safe, proposals>>

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
    /\ UNCHANGED <<cfgOf, reg, replied, hist, promised, votes, promises, proposals>>

\* "p212" only: the Phase 1 at the new ballot for the slots below the frontier.
Phase1Low(b) ==
    /\ status[b] = "leading"
    /\ HasPending(b)
    /\ Covered(b)
    /\ safe' = [safe EXCEPT ![b] =
          [s \in Slots |-> IF safe[b][s] = Pending THEN SafeAt(b, s) ELSE safe[b][s]]]
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promised, votes, promises, proposals>>

Next ==
    \/ \E b \in Ballots, m \in Matchmakers : Register(b, m)
    \/ \E b \in Ballots : MatchDone(b) \/ Phase1Done(b) \/ Phase1Low(b)
    \/ \E a \in Acceptors, b \in Ballots : Promise(a, b)
    \/ \E b \in Ballots, s \in Slots, v \in Values : Propose(b, s, v)
    \/ \E a \in Acceptors, p \in proposals : Accept(a, p[1], p[2], p[3])
    \/ \E b, b2 \in Ballots : Bypass(b, b2)

Spec == Init /\ [][Next]_vars

Chosen(s, v) ==
    \E b \in Ballots :
        \E Q \in Quorums(cfgOf[b]) : \A a \in Q : <<a, s, b, v>> \in votes

\* Safety: one value for each slot.
Agreement == \A s \in Slots : \A v, w \in Values : (Chosen(s, v) /\ Chosen(s, w)) => v = w

\* Not a real invariant. TLC must find a trace where a bypass leader has a
\* value chosen at its new ballot: this shows that the model reaches the case.
NoBypassChoice ==
    ~ \E b \in Ballots, s \in Slots, v \in Values :
        /\ \E b0 \in Ballots : status[b0] = "switched" /\ NextOwn(b0, b)
        /\ \E Q \in Quorums(cfgOf[b]) : \A a \in Q : <<a, s, b, v>> \in votes
=============================================================================
