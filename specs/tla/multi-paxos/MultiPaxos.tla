------------------------------ MODULE MultiPaxos ------------------------------
(***************************************************************************)
(* Plain Multi-Paxos as `paros-consensus` builds it (`ColocatedNode` with  *)
(* no matchmaker): one static configuration, a log of slots, colocated     *)
(* proposers and acceptors. Rework 1.14, #1185 (TLA+ model multi-paxos).   *)
(*                                                                         *)
(* A message is never lost from `msgs`, so a message can arrive late, two  *)
(* times, or never. Each ballot has one owner (`(round, node)` in the      *)
(* code). A ballot that a higher promise preempted only takes no more      *)
(* steps: a new campaign is a higher ballot.                               *)
(*                                                                         *)
(* What the model has, and the code it follows:                            *)
(*   - Phase 1 recovers the suffix (`node/election.rs`). The campaign asks *)
(*     from its first unlearned slot `from`; a promise reports every vote  *)
(*     at or above `from` (`Acceptor::promise_page`).                      *)
(*   - The candidate is its own first acceptor: it promises its ballot    *)
(*     when it opens Phase 1, and its promise is in the quorum it wins on. *)
(*   - The allocator frontier of a new leader (`try_become_leader`): one   *)
(*     past the highest slot its Phase 1 reported, never below `from`.     *)
(*   - The recovery (`RecoveryPolicy::Phase1Backed`): each slot from       *)
(*     `from` to the frontier gets the highest reported vote again, or a   *)
(*     `Noop` when no promise reported one (the election gap fill).        *)
(*   - Phase 2 for fresh commands at the frontier (`Proposer::allocate`).  *)
(*   - Commit: the leader learns a slot at a Phase-2 quorum of its ballot  *)
(*     (`node/learn.rs`); a node learns it from the leader's `Commit` or   *)
(*     catches up from any peer that learned it (`node/catch_up.rs`).      *)
(*   - A restart keeps only the durable state: the promise and the votes   *)
(*     that reached the disk (`HardState`, `WriteOp::AppendAccepted`) and  *)
(*     what the node learned. Every ballot the node owned dies with it.    *)
(*   - The quorum system is a constant: a majority or a grid (`Q1`, `Q2`,  *)
(*     `membership/quorum_system.rs`). A slot's Phase-2 quorums depend on  *)
(*     the slot (`column_of`: `slot % cols` on a grid).                    *)
(*                                                                         *)
(* Constants:                                                              *)
(*   Learners the nodes that catch up. The ballot owners always learn what *)
(*           they decide; a node that owns no ballot and catches up only   *)
(*           adds states, so a check may leave it out.                     *)
(*   Alloc   "frontier": fresh commands start at the frontier.             *)
(*           "prefix": a control, fresh commands start at `from`.          *)
(*   Gap     "noop": an unreported slot below the frontier gets a `Noop`.  *)
(*           "skip": a control, it gets nothing.                           *)
(*   Durable "before": an acceptor answers only after its promise or vote  *)
(*           is on disk (persist before send, `Ready`).                    *)
(*           "after": a control, it answers first and persists later.      *)
(*   Restarts how many restarts may happen (in the whole run).             *)
(*   Wipes   how many acceptors may lose their disk, each one time.        *)
(*   Identity TRUE: a wiped acceptor never takes a step again (it comes    *)
(*           back with a new node id, outside the configuration).          *)
(*           FALSE: a control, it answers again as the old acceptor.       *)
(*   Down    the acceptors that never take a step (liveness checks only).  *)
(*   Owner   the node that owns each ballot.                               *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Acceptors, Learners, Commands, Noop, NB, NS, Q1, Q2, Owner,
          Alloc, Gap, Durable, Restarts, Wipes, Identity, Down

Ballots == 1..NB
Slots == 0..(NS - 1)
Values == Commands \cup {Noop}
None == "none"

ASSUME Alloc \in {"frontier", "prefix"}
ASSUME Gap \in {"noop", "skip"}
ASSUME Durable \in {"before", "after"}
ASSUME Identity \in BOOLEAN
ASSUME Down \subseteq Acceptors
ASSUME Learners \subseteq Acceptors
ASSUME Noop \notin Commands
ASSUME Owner \in [Ballots -> Acceptors]
ASSUME Q2 \in [Slots -> SUBSET SUBSET Acceptors]
Vote == [bal : 0..NB, val : Values \cup {None}]
NoVote == [bal |-> 0, val |-> None]

VARIABLES prom,     \* acceptor: the promise in memory
          vote,     \* acceptor: the vote in memory, per slot
          dprom,    \* acceptor: the promise on disk
          dvote,    \* acceptor: the vote on disk, per slot
          learned,  \* node: the value it learned, per slot (durable)
          phase,    \* ballot: "idle", "p1", "p2" or "dead"
          next,     \* ballot: the allocator's next slot, once in Phase 2
          front,    \* ballot: the frontier its Phase 1 derived
          wiped,    \* the acceptors that lost their disk
          restarts, \* how many restarts happened
          msgs      \* every message sent

vars == <<prom, vote, dprom, dvote, learned, phase, next, front, wiped, restarts, msgs>>

Msg ==
  [type : {"1a"}, bal : Ballots, from : 0..NS]
    \cup [type : {"1b"}, acc : Acceptors, bal : Ballots, from : 0..NS, rep : [Slots -> Vote]]
    \cup [type : {"2a"}, bal : Ballots, slot : Slots, val : Values]
    \cup [type : {"2b"}, acc : Acceptors, bal : Ballots, slot : Slots, val : Values]

TypeOK ==
  /\ prom \in [Acceptors -> 0..NB]
  /\ dprom \in [Acceptors -> 0..NB]
  /\ vote \in [Acceptors -> [Slots -> Vote]]
  /\ dvote \in [Acceptors -> [Slots -> Vote]]
  /\ learned \in [Acceptors -> [Slots -> Values \cup {None}]]
  /\ phase \in [Ballots -> {"idle", "p1", "p2", "dead"}]
  /\ next \in [Ballots -> 0..NS]
  /\ front \in [Ballots -> 0..NS]
  /\ wiped \subseteq Acceptors
  /\ restarts \in Nat
  /\ msgs \subseteq Msg

Init ==
  /\ prom = [a \in Acceptors |-> 0]
  /\ dprom = [a \in Acceptors |-> 0]
  /\ vote = [a \in Acceptors |-> [s \in Slots |-> NoVote]]
  /\ dvote = [a \in Acceptors |-> [s \in Slots |-> NoVote]]
  /\ learned = [a \in Acceptors |-> [s \in Slots |-> None]]
  /\ phase = [b \in Ballots |-> "idle"]
  /\ next = [b \in Ballots |-> 0]
  /\ front = [b \in Ballots |-> 0]
  /\ wiped = {}
  /\ restarts = 0
  /\ msgs = {}

Send(m) == msgs' = msgs \cup {m}
SendAll(M) == msgs' = msgs \cup M

\* An acceptor that may take a step.
Live(a) == a \notin Down /\ (Identity => a \notin wiped)

\* The first slot node n has not learned: where its campaign starts.
FirstUnlearned(n) ==
  IF \A s \in Slots : learned[n][s] # None THEN NS
  ELSE CHOOSE s \in Slots : learned[n][s] = None /\ \A t \in Slots : t < s => learned[n][t] # None

\* What the acceptor keeps on disk at each step.
Persisted(old, new) == IF Durable = "before" THEN new ELSE old

-----------------------------------------------------------------------------
(* Phase 1 *)

\* The promise of acceptor a at ballot b, for a campaign from `from`: it
\* reports each vote at or above `from`.
PromiseMsg(a, b, from) ==
  [type |-> "1b", acc |-> a, bal |-> b, from |-> from,
   rep |-> [s \in Slots |-> IF s >= from THEN vote[a][s] ELSE NoVote]]

\* The owner of ballot b opens a campaign from its first unlearned slot. It
\* is its own first acceptor: it promises b, and its own records seed the
\* tally (`open_phase1` takes `acceptor.records()`).
Prepare(b) ==
  LET o == Owner[b]
      from == FirstUnlearned(o)
  IN /\ phase[b] = "idle"
     /\ Live(o)
     /\ prom[o] < b
     /\ phase' = [phase EXCEPT ![b] = "p1"]
     /\ prom' = [prom EXCEPT ![o] = b]
     /\ dprom' = [dprom EXCEPT ![o] = Persisted(dprom[o], b)]
     /\ SendAll({[type |-> "1a", bal |-> b, from |-> from], PromiseMsg(o, b, from)})
     /\ UNCHANGED <<vote, dvote, learned, next, front, wiped, restarts>>

\* Acceptor a promises ballot b and reports each vote at or above `from`.
Promise(a, b) ==
  /\ Live(a)
  /\ prom[a] < b
  /\ \E m \in msgs :
       /\ m.type = "1a" /\ m.bal = b
       /\ prom' = [prom EXCEPT ![a] = b]
       /\ dprom' = [dprom EXCEPT ![a] = Persisted(dprom[a], b)]
       /\ Send(PromiseMsg(a, b, m.from))
  /\ UNCHANGED <<vote, dvote, learned, phase, next, front, wiped, restarts>>

\* The highest vote that the promises S report at slot s.
Top(S, s) ==
  LET rep == {m \in S : m.rep[s].bal > 0}
  IN IF rep = {} THEN NoVote
     ELSE (CHOOSE m \in rep : \A n \in rep : n.rep[s].bal <= m.rep[s].bal).rep[s]

\* The frontier: one past the highest slot that the promises S report,
\* never below `from`.
Frontier(S, from) ==
  LET h == {s \in Slots : s >= from /\ \E m \in S : m.rep[s].bal > 0}
  IN IF h = {} THEN from ELSE (CHOOSE s \in h : \A t \in h : t <= s) + 1

\* The 2a messages of the recovery from `from` to the frontier f.
Recovery(S, b, from, f) ==
  {[type |-> "2a", bal |-> b, slot |-> s, val |-> Top(S, s).val] :
      s \in {t \in from..(f - 1) : Top(S, t).bal > 0}}
  \cup (IF Gap = "noop"
        THEN {[type |-> "2a", bal |-> b, slot |-> s, val |-> Noop] :
                 s \in {t \in from..(f - 1) : Top(S, t).bal = 0}}
        ELSE {})

\* The owner of ballot b wins Phase 1 on the promises S, one for each
\* acceptor, its own among them, and a Phase-1 quorum. It opens the recovery.
Win(b) ==
  /\ phase[b] = "p1"
  /\ \E S \in SUBSET {m \in msgs : m.type = "1b" /\ m.bal = b} :
       /\ \A m, n \in S : m.acc = n.acc => m = n
       /\ Owner[b] \in {m.acc : m \in S}
       /\ \E Q \in Q1 : Q \subseteq {m.acc : m \in S}
       /\ LET from == (CHOOSE m \in msgs : m.type = "1a" /\ m.bal = b).from
              f == Frontier(S, from)
          IN /\ front' = [front EXCEPT ![b] = f]
             /\ next' = [next EXCEPT ![b] = IF Alloc = "frontier" THEN f ELSE from]
             /\ SendAll(Recovery(S, b, from, f))
  /\ phase' = [phase EXCEPT ![b] = "p2"]
  /\ UNCHANGED <<prom, vote, dprom, dvote, learned, wiped, restarts>>

-----------------------------------------------------------------------------
(* Phase 2 *)

\* The leader of ballot b allocates the next slot to a fresh command.
Allocate(b, c) ==
  /\ phase[b] = "p2"
  /\ next[b] < NS
  /\ next' = [next EXCEPT ![b] = @ + 1]
  /\ Send([type |-> "2a", bal |-> b, slot |-> next[b], val |-> c])
  /\ UNCHANGED <<prom, vote, dprom, dvote, learned, phase, front, wiped, restarts>>

\* Acceptor a votes for a value of ballot b at slot s.
Accept(a, b, s) ==
  /\ Live(a)
  /\ prom[a] <= b
  /\ \E m \in msgs :
       /\ m.type = "2a" /\ m.bal = b /\ m.slot = s
       /\ vote[a][s] # [bal |-> b, val |-> m.val]
       /\ prom' = [prom EXCEPT ![a] = b]
       /\ vote' = [vote EXCEPT ![a][s] = [bal |-> b, val |-> m.val]]
       /\ dprom' = [dprom EXCEPT ![a] = Persisted(dprom[a], b)]
       /\ dvote' = [dvote EXCEPT ![a][s] = Persisted(dvote[a][s], [bal |-> b, val |-> m.val])]
       /\ Send([type |-> "2b", acc |-> a, bal |-> b, slot |-> s, val |-> m.val])
  /\ UNCHANGED <<learned, phase, next, front, wiped, restarts>>

\* The late write of an acceptor that answered first (Durable = "after").
Persist(a) ==
  /\ Durable = "after"
  /\ Live(a)
  /\ <<dprom[a], dvote[a]>> # <<prom[a], vote[a]>>
  /\ dprom' = [dprom EXCEPT ![a] = prom[a]]
  /\ dvote' = [dvote EXCEPT ![a] = vote[a]]
  /\ UNCHANGED <<prom, vote, learned, phase, next, front, wiped, restarts, msgs>>

-----------------------------------------------------------------------------
(* Commit and catch-up *)

\* The leader of ballot b learns slot s at a Phase-2 quorum and commits it.
Decide(b, s) ==
  /\ phase[b] = "p2"
  /\ learned[Owner[b]][s] = None
  /\ \E v \in Values, Q \in Q2[s] :
       /\ \A a \in Q : [type |-> "2b", acc |-> a, bal |-> b, slot |-> s, val |-> v] \in msgs
       /\ learned' = [learned EXCEPT ![Owner[b]][s] = v]
  /\ UNCHANGED <<prom, vote, dprom, dvote, phase, next, front, wiped, restarts, msgs>>

\* Node n catches up from peer p: it learns every slot that p learned (the
\* leader's `Commit`, or a `CatchUpRequest` to any peer). A learned value is
\* stable, so the model reads it from the peer and sends no message.
CatchUp(n, p) ==
  /\ n \in Learners
  /\ Live(n) /\ Live(p)
  /\ \E s \in Slots : learned[n][s] = None /\ learned[p][s] # None
  /\ learned' = [learned EXCEPT ![n] = [s \in Slots |-> IF @[s] = None THEN learned[p][s] ELSE @[s]]]
  /\ UNCHANGED <<prom, vote, dprom, dvote, phase, next, front, wiped, restarts, msgs>>

-----------------------------------------------------------------------------
(* Faults *)

\* Node a restarts: memory goes back to the disk, and its ballots die.
Restart(a) ==
  /\ Live(a)
  /\ restarts < Restarts
  /\ restarts' = restarts + 1
  /\ prom' = [prom EXCEPT ![a] = dprom[a]]
  /\ vote' = [vote EXCEPT ![a] = dvote[a]]
  /\ phase' = [b \in Ballots |-> IF Owner[b] = a /\ phase[b] \in {"p1", "p2"} THEN "dead" ELSE phase[b]]
  /\ UNCHANGED <<dprom, dvote, learned, next, front, wiped, msgs>>

\* Acceptor a loses its disk: it comes back with no promise and no vote.
Wipe(a) ==
  /\ Live(a)
  /\ Cardinality(wiped) < Wipes
  /\ wiped' = wiped \cup {a}
  /\ prom' = [prom EXCEPT ![a] = 0]
  /\ dprom' = [dprom EXCEPT ![a] = 0]
  /\ vote' = [vote EXCEPT ![a] = [s \in Slots |-> NoVote]]
  /\ dvote' = [dvote EXCEPT ![a] = [s \in Slots |-> NoVote]]
  /\ learned' = [learned EXCEPT ![a] = [s \in Slots |-> None]]
  /\ phase' = [b \in Ballots |-> IF Owner[b] = a /\ phase[b] \in {"p1", "p2"} THEN "dead" ELSE phase[b]]
  /\ UNCHANGED <<next, front, restarts, msgs>>

Next ==
  \/ \E b \in Ballots : Prepare(b) \/ Win(b)
  \/ \E b \in Ballots, c \in Commands : Allocate(b, c)
  \/ \E a \in Acceptors, b \in Ballots : Promise(a, b)
  \/ \E a \in Acceptors, b \in Ballots, s \in Slots : Accept(a, b, s)
  \/ \E b \in Ballots, s \in Slots : Decide(b, s)
  \/ \E n, p \in Acceptors : CatchUp(n, p)
  \/ \E a \in Acceptors : Persist(a) \/ Restart(a) \/ Wipe(a)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Liveness *)

\* The last ballot NB is the leader that no higher ballot preempts. Its
\* owner and each acceptor that is not down keep taking their steps.
Fairness ==
  /\ WF_vars(Prepare(NB))
  /\ WF_vars(Win(NB))
  /\ \A s \in Slots : WF_vars(Decide(NB, s))
  /\ \A a \in Acceptors \ Down :
       /\ WF_vars(Promise(a, NB))
       /\ \A s \in Slots : WF_vars(Accept(a, NB, s))
       /\ WF_vars(CatchUp(a, Owner[NB]))

LiveSpec == Spec /\ Fairness

\* The last leader learns every slot below its frontier and every slot
\* it proposed: no gap stays, and each proposed command is chosen.
Settled ==
  /\ phase[NB] = "p2"
  /\ \A s \in Slots :
       (s < front[NB] \/ \E m \in msgs : m.type = "2a" /\ m.bal = NB /\ m.slot = s)
         => learned[Owner[NB]][s] # None

Ends == <>[]Settled

\* Each node that is not down learns what the last leader learned.
Spreads == \A n \in Acceptors \ Down, s \in Slots :
             learned[Owner[NB]][s] # None ~> learned[n][s] # None

-----------------------------------------------------------------------------
(* Safety *)

\* A value is chosen at slot s once a Phase-2 quorum of s voted for it at
\* one ballot. A vote is a 2b message, so a restart does not unchoose it.
ChosenAt(b, s, v) ==
  \E Q \in Q2[s] : \A a \in Q : [type |-> "2b", acc |-> a, bal |-> b, slot |-> s, val |-> v] \in msgs
Chosen(s, v) == \E b \in Ballots : ChosenAt(b, s, v)

\* One command for each slot.
Agreement == \A s \in Slots, v, w \in Values : Chosen(s, v) /\ Chosen(s, w) => v = w

\* What a node learned is what was chosen.
LearnedChosen == \A n \in Acceptors, s \in Slots : learned[n][s] # None => Chosen(s, learned[n][s])

\* One proposal for each slot and ballot: a new leader allocates above each
\* slot that its Phase 1 reported, so its fresh commands never land on a
\* slot that its recovery proposes.
OneProposal ==
  \A m, n \in msgs : m.type = "2a" /\ n.type = "2a" /\ m.bal = n.bal /\ m.slot = n.slot => m = n

\* Reachability controls: each must be violated.
NoChoice == ~\E s \in Slots, v \in Values : Chosen(s, v)
NoNoopChosen == ~\E s \in Slots : Chosen(s, Noop) /\ \E v \in Commands, t \in Slots : t > s /\ Chosen(t, v)
=============================================================================
