-------------------------------- MODULE Reads --------------------------------
(* Leaderless quorum reads on Matchmaker MultiPaxos, for #1016 (model of     *)
(* leaderless reads). The base is `specs/tla/zero-stall/Bypass.tla`: the     *)
(* matchmaking phase, Phase 1 over every prior configuration, Phase 2 and    *)
(* the Phase-1 bypass of #579 (zero-stall reconfiguration). This module adds *)
(* the acks to the clients and one leaderless read.                          *)
(*                                                                           *)
(* A reader asks a Phase-1 quorum of the configuration of its read basis for *)
(* vote watermarks. It serves the read at the maximum of the watermarks and  *)
(* the fence of the basis (`paros-consensus`, `quorum_read.rs`).                  *)
(*                                                                           *)
(* Owner, Check, Mode : as in `Bypass.tla`.                                  *)
(* Basis : "won"    = a node learns its read basis only from a leadership    *)
(*                    that won its ballot (#260).                            *)
(*         "belief" = the basis is the configuration of the ballot that the  *)
(*                    node promised, with no fence (the bug that #260 fixed).*)
(* Fence : what a `PreReadAck` carries; an answer above the basis ends the  *)
(*         read (#1099 (rework 1.10)).                                       *)
(*   "promise" = the higher of the promise and the belief ballot of the      *)
(*               acceptor (`wire_read_fence`, the code since PR #1074).      *)
(*   "belief"  = the belief ballot alone (`acceptors_since`, the code before *)
(*               PR #1074): a control.                                       *)
(*   "none"    = no ballot: a control.                                       *)
(* Laggers : the acceptors that can ignore the configuration of a ballot    *)
(*         that they promise or accept, because their pool does not hold     *)
(*         each node of it yet (`learn_config`). The promise moves all the   *)
(*         same.                                                             *)
(* Restarters : the acceptors that can restart. The promise and the votes   *)
(*         of an acceptor are durable. Its belief is volatile: it goes back  *)
(*         to the bootstrap, bound to no ballot, and the node did not hear   *)
(*         what is in force. A membership probe tells it again. A reader     *)
(*         also forgets its read basis and its open read.                    *)
(* Refuse : TRUE = an acceptor that did not hear answers no `PreRead`        *)
(*         (`on_pre_read`, the code).                                        *)
(* Hold  : TRUE  = the decision hold of #579 (rule 4): a leader acks a slot  *)
(*                 only when each prior configuration has a Phase-1 quorum   *)
(*                 of promises for its ballot.                               *)
(* Learner : the options of #890 (learner above the leader). A reader can   *)
(*           campaign with a stale belief: it promises its own ballot, the   *)
(*           matchmakers refuse the campaign, and it sends no Prepare.       *)
(*   "off"    = no such campaign.                                            *)
(*   "strict" = a node whose promise is above a leadership never takes it as *)
(*              its read basis, and with ReaderGate = "promise" opens and    *)
(*              serves no read on it (`main` today).                         *)
(*   "own"    = option (b): a node takes the basis when the only promise     *)
(*              above it is its own abandoned campaign.                      *)
(*   "any"    = a node ignores its promise for the read basis.               *)
(*   Option (a), the leader campaigns above the promise, adds no rule to the *)
(*   model: it is one more ordinary campaign.                                *)
(* ReaderGate : what gates the reader itself, to open and to serve a read   *)
(*         (#1228 (stale read by a lagging reader)).                         *)
(*   "promise" = the rule `Below`: the promise of the reader (the code,      *)
(*               `read_basis` and `fence_quorum_reads` on the read fence).   *)
(*   "belief"  = the belief ballot of the reader alone, and the reader       *)
(*               counts its own answer with no fence (the code before the    *)
(*               fix of #1228): a control.                                   *)
(*                                                                           *)
(* Quorums : the quorum system is a parameter (#1102 (rework 1.13)). Each   *)
(*           configuration c has its Phase-1 quorums Q1s[c], its Phase-2     *)
(*           quorums Q2s[c], and the sets of acceptors that a reader asks,   *)
(*           Targets[c]: the whole membership under a majority and on the    *)
(*           partitions, one row on the full grid (`row_of`). A read         *)
(*           completes on a Phase-1 quorum of the answers. `MC.tla` builds   *)
(*           the grids with `Grid.tla` (`specs/tla/grid/`).                  *)
(* Seal  : "quorum" = Phase 1 takes a Phase-1 quorum of each prior           *)
(*                    configuration (the code).                              *)
(*         "reads"  = a candidate rule, not in the code: Phase 1 also takes  *)
(*                    one promise in each read target of each prior          *)
(*                    configuration, so each row hears of the campaign.      *)
(*                                                                           *)
(* Not in the model: GC watermarks, matchmaker generations, the replica      *)
(* tier, a read that expires, an acceptor that loses its disk.               *)
EXTENDS Naturals, FiniteSets

CONSTANTS Acceptors, Matchmakers, Configs, Values, Slots, NB, Owner, Check, Mode,
          Readers, Basis, Fence, Laggers, Restarters, Refuse, Hold, Learner, Q1s, Q2s,
          Targets, Seal, ReaderGate

ASSUME Seal \in {"quorum", "reads"}
ASSUME ReaderGate \in {"promise", "belief"}
ASSUME Fence \in {"promise", "belief", "none"}
ASSUME Laggers \cup Restarters \subseteq Acceptors
ASSUME Refuse \in BOOLEAN
\* Paxos safety rests on this: every Phase-1 quorum meets every Phase-2 quorum.
ASSUME \A c \in Configs : \A P \in Q1s[c], C \in Q2s[c] : P \cap C # {}
\* A reader asks a set that can hold a Phase-1 quorum.
ASSUME \A c \in Configs : \A T \in Targets[c] : \E P \in Q1s[c] : P \subseteq T

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
          own,       \* reader -> the ballot of its own abandoned campaign (0: none)
          since,     \* acceptor -> the ballot of the configuration it believes
          heard      \* acceptor -> TRUE when its belief is one it heard

vars == <<status, cfgOf, reg, replied, hist, promised, votes, promises, safe, proposals,
          acked, basis, rd, own, since, heard>>

NoRead == [state |-> "none", node |-> CHOOSE n \in Readers : TRUE, b |-> 0, fence |-> 0,
           snap |-> {}, to |-> {}, answers |-> {}, index |-> 0]

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
        \* The model starts after each acceptor heard the first configuration.
        /\ since = [a \in Acceptors |-> 0]
        /\ heard = [a \in Acceptors |-> TRUE]

\* Matchmaker m registers b if it holds no ballot at or above b, and answers
\* with what it held.
Register(b, m) ==
    /\ status[b] = "matching"
    /\ m \notin replied[b]
    /\ \A r \in reg[m] : r[1] < b
    /\ reg' = [reg EXCEPT ![m] = @ \cup {<<b, cfgOf[b]>>}]
    /\ replied' = [replied EXCEPT ![b] = @ \cup {m}]
    /\ hist' = [hist EXCEPT ![b] = @ \cup reg[m]]
    /\ UNCHANGED <<status, cfgOf, promised, votes, promises, safe, proposals, acked, basis, rd, own, since, heard>>

MatchDone(b) ==
    /\ status[b] = "matching"
    /\ 2 * Cardinality(replied[b]) > Cardinality(Matchmakers)
    /\ status' = [status EXCEPT ![b] = "matched"]
    /\ UNCHANGED <<cfgOf, reg, replied, hist, promised, votes, promises, safe, proposals, acked, basis, rd, own, since, heard>>

PriorCfgs(b) == {r[2] : r \in hist[b]}

HasPending(b) == \E s \in Slots : safe[b][s] = Pending

\* Acceptor a follows ballot b (a Prepare that it promised, an Accept that it
\* took): it learns the configuration of b when b is above its belief
\* (`learn_config`). A lagger can ignore that configuration, and its belief
\* stays where it was.
Learn(a, b) ==
    \/ /\ b > since[a]
       /\ since' = [since EXCEPT ![a] = b]
       /\ heard' = [heard EXCEPT ![a] = TRUE]
    \/ /\ b <= since[a] \/ a \in Laggers
       /\ UNCHANGED <<since, heard>>

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
    /\ Learn(a, b)
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, votes, safe, proposals, acked, basis, rd, own>>

PromisedAt(b) == {p[1] : p \in {q \in promises : q[2] = b}}

\* A promise quorum of EVERY prior configuration. With Seal = "reads", also
\* one promise in each read target of each.
Covered(b) ==
    \A c \in PriorCfgs(b) :
        /\ \E Q \in Q1s[c] : Q \subseteq PromisedAt(b)
        /\ Seal = "reads" => \A T \in Targets[c] : T \cap PromisedAt(b) # {}

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
    /\ UNCHANGED <<cfgOf, reg, replied, hist, promised, votes, promises, proposals, acked, basis, rd, own, since, heard>>

Propose(b, s, v) ==
    /\ status[b] = "leading"
    /\ ~ \E p \in proposals : p[1] = b /\ p[2] = s
    /\ safe[b][s] = Free \/ safe[b][s] = v
    /\ proposals' = proposals \cup {<<b, s, v>>}
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promised, votes, promises, safe, acked, basis, rd, own, since, heard>>

Accept(a, b, s, v) ==
    /\ <<b, s, v>> \in proposals
    /\ a \in cfgOf[b]
    /\ promised[a] <= b
    /\ <<a, s, b, v>> \notin votes
    /\ promised' = [promised EXCEPT ![a] = b]
    /\ votes' = votes \cup {<<a, s, b, v>>}
    /\ Learn(a, b)
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
    /\ UNCHANGED <<cfgOf, reg, replied, hist, promised, votes, promises, proposals, acked, basis, rd, own, since, heard>>

\* "p212" only: the Phase 1 at the new ballot for the slots below the frontier.
Phase1Low(b) ==
    /\ status[b] = "leading"
    /\ HasPending(b)
    /\ Covered(b)
    /\ safe' = [safe EXCEPT ![b] =
          [s \in Slots |-> IF safe[b][s] = Pending THEN SafeAt(b, s) ELSE safe[b][s]]]
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promised, votes, promises, proposals, acked, basis, rd, own, since, heard>>

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
                   basis, rd, since, heard>>

\* The promise of reader n does not stop a read basis at ballot b.
Below(n, b) ==
    \/ promised[n] <= b
    \/ Learner = "any"
    \/ Learner = "own" /\ promised[n] = own[n] /\ n \notin cfgOf[b]

\* The leader of b tells a client that slot s is decided.
Ack(b, s) ==
    /\ status[b] = "leading"
    /\ s \notin acked
    /\ \E v \in Values : \E Q \in Q2s[cfgOf[b]] : \A a \in Q : <<a, s, b, v>> \in votes
    /\ Hold => Covered(b)
    /\ acked' = acked \cup {s}
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promised, votes, promises, safe,
                   proposals, basis, rd, own, since, heard>>

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
                   proposals, acked, rd, own, since, heard>>

\* The gate of reader n to open, keep and serve a read on a basis at b.
ReadGate(n, b) == IF ReaderGate = "promise" THEN Below(n, b) ELSE since[n] <= b

BasisOf(n) == IF Basis = "won" THEN basis[n] ELSE <<promised[n], 0>>

\* Reader n opens a read and asks one read target of the basis configuration.
\* A node whose promise is above its basis opens none.
ReadStart(n) ==
    /\ rd.state = "none"
    /\ BasisOf(n)[1] > 0
    /\ ReadGate(n, BasisOf(n)[1])
    /\ \E T \in Targets[cfgOf[BasisOf(n)[1]]] :
          rd' = [state |-> "open", node |-> n, b |-> BasisOf(n)[1], fence |-> BasisOf(n)[2],
                 snap |-> acked, to |-> T, answers |-> {}, index |-> 0]
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promised, votes, promises, safe,
                   proposals, acked, basis, own, since, heard>>

Watermark(a) == Max({w[2] : w \in {x \in votes : x[1] = a}} \cup {0})

\* The ballot that the answer of acceptor a carries (`wire_read_fence`).
AckFence(a) ==
    CASE Fence = "promise" -> IF promised[a] > since[a] THEN promised[a] ELSE since[a]
      [] Fence = "belief"  -> since[a]
      [] Fence = "none"    -> 0

\* Acceptor a answers the PreRead with its vote watermark and its fence. An
\* answer above the basis ends the read. With Refuse, an acceptor that did
\* not hear answers nothing. The reader answers itself too; under
\* ReaderGate = "belief" its own answer carries no fence.
PreReadAck(a) ==
    /\ rd.state = "open"
    /\ a \in rd.to
    /\ ~ \E x \in rd.answers : x[1] = a
    /\ Refuse => heard[a]
    /\ rd' = IF AckFence(a) > rd.b /\ ~ (ReaderGate = "belief" /\ a = rd.node)
             THEN NoRead
             ELSE [rd EXCEPT !.answers = @ \cup {<<a, Watermark(a)>>}]
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promised, votes, promises, safe,
                   proposals, acked, basis, own, since, heard>>

\* The reader learns a newer configuration: it ends its open read.
ReadAbandon ==
    /\ rd.state = "open"
    /\ BasisOf(rd.node)[1] # rd.b \/ ~ ReadGate(rd.node, rd.b)
    /\ rd' = NoRead
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promised, votes, promises, safe,
                   proposals, acked, basis, own, since, heard>>

\* The answers are a Phase-1 quorum of the basis configuration, and the
\* prefix of the reader covers the index.
ReadServe ==
    /\ rd.state = "open"
    /\ BasisOf(rd.node)[1] = rd.b
    /\ ReadGate(rd.node, rd.b)
    /\ \E Q \in Q1s[cfgOf[rd.b]] : Q \subseteq {x[1] : x \in rd.answers}
    /\ LET i == Max({x[2] : x \in rd.answers} \cup {rd.fence})
       IN  /\ \A s \in Slots : s <= i =>
                  \E v \in Values : \E b \in Ballots : \E Q \in Q2s[cfgOf[b]] :
                      \A a \in Q : <<a, s, b, v>> \in votes
           /\ rd' = [rd EXCEPT !.state = "done", !.index = i]
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promised, votes, promises, safe,
                   proposals, acked, basis, own, since, heard>>

\* Acceptor a restarts. The promise and the votes are durable; the belief,
\* the read basis and the open read of a reader are not.
Restart(a) ==
    /\ a \in Restarters
    /\ since' = [since EXCEPT ![a] = 0]
    /\ heard' = [heard EXCEPT ![a] = FALSE]
    /\ basis' = IF a \in Readers THEN [basis EXCEPT ![a] = <<0, 0>>] ELSE basis
    /\ rd' = IF rd.state # "none" /\ rd.node = a THEN NoRead ELSE rd
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promised, votes, promises, safe,
                   proposals, acked, own>>

\* The membership probe of a node that did not hear: a matchmaker majority
\* answers, and the node adopts the configuration of the highest ballot that
\* the answers hold. Each ballot that an acceptor promised or accepted was
\* registered at a matchmaker majority first, thus the probe binds the belief
\* at or above it.
Probe(a) ==
    /\ ~ heard[a]
    /\ \E M \in SUBSET Matchmakers :
          /\ 2 * Cardinality(M) > Cardinality(Matchmakers)
          /\ since' = [since EXCEPT ![a] = Max({r[1] : r \in UNION {reg[m] : m \in M}} \cup {0})]
    /\ heard' = [heard EXCEPT ![a] = TRUE]
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promised, votes, promises, safe,
                   proposals, acked, basis, rd, own>>

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
    \/ \E a \in Acceptors : Restart(a) \/ Probe(a)
    \/ ReadAbandon \/ ReadServe

Spec == Init /\ [][Next]_vars

Chosen(s, v) ==
    \E b \in Ballots :
        \E Q \in Q2s[cfgOf[b]] : \A a \in Q : <<a, s, b, v>> \in votes

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
