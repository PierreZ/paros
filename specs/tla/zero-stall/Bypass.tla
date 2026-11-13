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
(* Gc     : "off"    = no garbage collection.                                *)
(*          "chosen" = the leader of b asks the matchmakers to forget the    *)
(*                     ballots below b when each slot that it recovered or   *)
(*                     carried is chosen at b (Matchmaker Paxos, section     *)
(*                     3.5), and, if Hold, when its Phase 1 is covered.      *)
(*          "early"  = a control: the leader asks with no condition.         *)
(* CheckWm: TRUE = bypass only if the highest watermark in the matchmaker    *)
(*          answers is at most the old ballot.                               *)
(* Reads  : TRUE = the leader acks the chosen slots to the clients, and the  *)
(*          model has the leaderless reads of Compartmentalized Paxos        *)
(*          (section 3.4) with the rule of paros (quorum_read.rs).           *)
(* Hold   : TRUE = the decision hold. A leader acks no slot before a quorum  *)
(*          of each prior configuration promised its ballot. After a bypass  *)
(*          the leader thus sends a Prepare, in "carry" mode too.            *)
(*                                                                           *)
(* CfgChoices : the configurations that the ballots can have. Each one is a  *)
(*          function from the ballots to Configs.                            *)
(*                                                                           *)
(* Not in the model: matchmaker generations, lost durable state, the         *)
(* cooperative handoff.                                                      *)
EXTENDS Naturals, FiniteSets

CONSTANTS Acceptors, Matchmakers, Configs, Values, Slots, NB, Owner, Check, Mode,
          Gc, CheckWm, Reads, Hold, CfgChoices

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
          proposals, \* set of <<ballot, slot, value>>
          wm,        \* matchmaker -> its GC watermark
          maxwm,     \* ballot -> the highest watermark in the answers
          acked      \* slots that a leader acked to a client

vars == <<status, cfgOf, reg, replied, hist, promised, votes, promises, safe, proposals,
          wm, maxwm, acked>>

Max(S) == CHOOSE x \in S : \A y \in S : y <= x

Quorums(c) == {Q \in SUBSET c : 2 * Cardinality(Q) > Cardinality(c)}

\* Each ballot has one configuration, selected by its owner. The selection is
\* local to the owner, thus the model makes it in the initial state. A ballot
\* that no matchmaker registers is a ballot that was never used.
Init == /\ status = [b \in Ballots |-> "matching"]
        /\ cfgOf \in CfgChoices
        /\ reg = [m \in Matchmakers |-> {}]
        /\ replied = [b \in Ballots |-> {}]
        /\ hist = [b \in Ballots |-> {}]
        /\ promised = [a \in Acceptors |-> 0]
        /\ votes = {}
        /\ promises = {}
        /\ safe = [b \in Ballots |-> [s \in Slots |-> Free]]
        /\ proposals = {}
        /\ wm = [m \in Matchmakers |-> 0]
        /\ maxwm = [b \in Ballots |-> 0]
        /\ acked = {}

\* Matchmaker m registers b if it holds no ballot at or above b and b is not
\* below its watermark. It answers with what it held and with its watermark.
Register(b, m) ==
    /\ status[b] = "matching"
    /\ m \notin replied[b]
    /\ \A r \in reg[m] : r[1] < b
    /\ wm[m] <= b
    /\ reg' = [reg EXCEPT ![m] = @ \cup {<<b, cfgOf[b]>>}]
    /\ replied' = [replied EXCEPT ![b] = @ \cup {m}]
    /\ hist' = [hist EXCEPT ![b] = @ \cup reg[m]]
    /\ maxwm' = [maxwm EXCEPT ![b] = Max({@, wm[m]})]
    /\ UNCHANGED <<status, cfgOf, promised, votes, promises, safe, proposals, wm, acked>>

MatchDone(b) ==
    /\ status[b] = "matching"
    /\ 2 * Cardinality(replied[b]) > Cardinality(Matchmakers)
    /\ status' = [status EXCEPT ![b] = "matched"]
    /\ UNCHANGED <<cfgOf, reg, replied, hist, promised, votes, promises, safe, proposals, wm, maxwm, acked>>

\* The prior configurations: the ones at or above the highest watermark.
PriorCfgs(b) == {r[2] : r \in {h \in hist[b] : h[1] >= maxwm[b]}}

HasPending(b) == \E s \in Slots : safe[b][s] = Pending

\* A promise quorum of EVERY prior configuration.
Covered(b) ==
    \A c \in PriorCfgs(b) :
        \E Q \in Quorums(c) : \A a \in Q : \E p \in promises : p[1] = a /\ p[2] = b

\* Acceptor a promises b and reports its votes. The owner of b sends a
\* Prepare in a normal campaign, after a "p212" bypass, or for the decision
\* hold after any bypass.
Promise(a, b) ==
    /\ \/ status[b] = "matched"
       \/ status[b] = "leading" /\ (HasPending(b) \/ (Hold /\ ~ Covered(b)))
    /\ a \in UNION PriorCfgs(b)
    /\ promised[a] <= b
    /\ ~ \E p \in promises : p[1] = a /\ p[2] = b
    /\ promised' = [promised EXCEPT ![a] = b]
    /\ promises' = promises \cup
          {<<a, b, {<<w[2], w[3], w[4]>> : w \in {x \in votes : x[1] = a}}>>}
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, votes, safe, proposals, wm, maxwm, acked>>

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
    /\ UNCHANGED <<cfgOf, reg, replied, hist, promised, votes, promises, proposals, wm, maxwm, acked>>

Propose(b, s, v) ==
    /\ status[b] = "leading"
    /\ ~ \E p \in proposals : p[1] = b /\ p[2] = s
    /\ safe[b][s] = Free \/ safe[b][s] = v
    /\ proposals' = proposals \cup {<<b, s, v>>}
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promised, votes, promises, safe, wm, maxwm, acked>>

Accept(a, b, s, v) ==
    /\ <<b, s, v>> \in proposals
    /\ a \in cfgOf[b]
    /\ promised[a] <= b
    /\ <<a, s, b, v>> \notin votes
    /\ promised' = [promised EXCEPT ![a] = b]
    /\ votes' = votes \cup {<<a, s, b, v>>}
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promises, safe, proposals, wm, maxwm, acked>>

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
    /\ CheckWm => maxwm[b2] <= b
    /\ Mode = "p212" =>
          \A s \in ProposedAt(b) : \A t \in Slots : t < s => t \in ProposedAt(b)
    /\ status' = [status EXCEPT ![b] = "switched", ![b2] = "leading"]
    /\ safe' = [safe EXCEPT ![b2] =
          [s \in Slots |->
              IF s \in ProposedAt(b)
              THEN (IF Mode = "carry" THEN ValAt(b, s) ELSE Pending)
              ELSE Free]]
    /\ UNCHANGED <<cfgOf, reg, replied, hist, promised, votes, promises, proposals, wm, maxwm, acked>>

\* "p212" only: the Phase 1 at the new ballot for the slots below the frontier.
Phase1Low(b) ==
    /\ status[b] = "leading"
    /\ HasPending(b)
    /\ Covered(b)
    /\ safe' = [safe EXCEPT ![b] =
          [s \in Slots |-> IF safe[b][s] = Pending THEN SafeAt(b, s) ELSE safe[b][s]]]
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promised, votes, promises, proposals, wm, maxwm, acked>>

ChosenAt(b, s) ==
    \E v \in Values :
        \E Q \in Quorums(cfgOf[b]) : \A a \in Q : <<a, s, b, v>> \in votes

\* The leader of b can ask the matchmakers to forget the ballots below b.
\* Each part of the condition is stable, thus a request that is late is the
\* same as a request that is sent now.
GcReady(b) ==
    /\ Gc # "off"
    /\ status[b] \in {"leading", "switched"}
    /\ Gc = "chosen" =>
          /\ ~ HasPending(b)
          /\ \A s \in Slots : safe[b][s] # Free => ChosenAt(b, s)
          /\ Hold => Covered(b)

\* Matchmaker m applies the GC request of b.
Collect(b, m) ==
    /\ GcReady(b)
    /\ wm[m] < b
    /\ wm' = [wm EXCEPT ![m] = b]
    /\ reg' = [reg EXCEPT ![m] = {r \in @ : r[1] >= b}]
    /\ UNCHANGED <<status, cfgOf, replied, hist, promised, votes, promises, safe,
                   proposals, maxwm, acked>>

\* The leader of b acks slot s to its client. A leader that switched can
\* still ack a slot that is chosen at its old ballot ("p212": the old rounds
\* stay open; "carry": a late quorum at the old ballot).
Ack(b, s) ==
    /\ Reads
    /\ status[b] \in {"leading", "switched"}
    /\ s \notin acked
    /\ ChosenAt(b, s)
    /\ Hold => Covered(b)
    /\ acked' = acked \cup {s}
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promised, votes, promises, safe,
                   proposals, wm, maxwm>>

Next ==
    \/ \E b \in Ballots, m \in Matchmakers : Register(b, m)
    \/ \E b \in Ballots : MatchDone(b) \/ Phase1Done(b) \/ Phase1Low(b)
    \/ \E a \in Acceptors, b \in Ballots : Promise(a, b)
    \/ \E b \in Ballots, s \in Slots, v \in Values : Propose(b, s, v)
    \/ \E a \in Acceptors, p \in proposals : Accept(a, p[1], p[2], p[3])
    \/ \E b, b2 \in Ballots : Bypass(b, b2)
    \/ \E b \in Ballots, m \in Matchmakers : Collect(b, m)
    \/ \E b \in Ballots, s \in Slots : Ack(b, s)

Spec == Init /\ [][Next]_vars

Chosen(s, v) ==
    \E b \in Ballots :
        \E Q \in Quorums(cfgOf[b]) : \A a \in Q : <<a, s, b, v>> \in votes

\* Safety: one value for each slot.
Agreement == \A s \in Slots : \A v, w \in Values : (Chosen(s, v) /\ Chosen(s, w)) => v = w

\* The fence of a leadership: the highest slot that it recovered or carried.
Fence(b) == Max({0} \cup {s \in Slots : safe[b][s] # Free})

\* A leaderless read (quorum_read.rs). A reader has the basis of a leadership
\* that won: its configuration, its ballot, its fence. It asks a quorum of
\* that configuration. It abandons the read if an answer shows a higher
\* ballot. If not, it reads at the highest slot that an answer voted in, and
\* at the fence or above.
\* The invariant: no read that starts now can miss a slot that is acked.
ReadSafe ==
    ~ \E b \in Ballots :
        /\ status[b] \in {"leading", "switched"}
        /\ \E Q \in Quorums(cfgOf[b]) :
              /\ \A a \in Q : promised[a] <= b
              /\ \E s \in acked :
                    /\ s > Fence(b)
                    /\ \A a \in Q : ~ \E w \in votes : w[1] = a /\ w[2] >= s

\* Not a real invariant. TLC must find a trace where a bypass leader has a
\* value chosen at its new ballot: this shows that the model reaches the case.
NoBypassChoice ==
    ~ \E b \in Ballots, s \in Slots, v \in Values :
        /\ \E b0 \in Ballots : status[b0] = "switched" /\ NextOwn(b0, b)
        /\ \E Q \in Quorums(cfgOf[b]) : \A a \in Q : <<a, s, b, v>> \in votes
=============================================================================
