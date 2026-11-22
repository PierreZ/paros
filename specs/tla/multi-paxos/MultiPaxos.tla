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
(*   - A learn is also a record (`mark_chosen`): the node raises its       *)
(*     promise to the ballot that chose the slot and keeps the chosen      *)
(*     value as its vote, unless it holds that value at a ballot at or     *)
(*     above. A promise raised past a campaign's ballot ends the campaign. *)
(*   - A restart keeps only the durable state: the promise and the votes   *)
(*     that reached the disk (`HardState`, `WriteOp::AppendAccepted`), and *)
(*     a prefix of what the node learned. The code rebuilds the chosen     *)
(*     prefix up to its chosen index; a relaxed write of the chosen index  *)
(*     (`MustSync::Relaxed`) can be lost, so the kept prefix can be any    *)
(*     prefix. Every ballot the node owned dies with it.                   *)
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
(*   Pick    "highest": Phase 1 takes the report of the highest ballot at  *)
(*           a slot (`merge_report`). "lowest": a control, it takes the    *)
(*           lowest. Two reports of different ballots at one slot need    *)
(*           three ballots, so the control is red only at `NB = 3`.        *)
(*   Restartable the nodes that may restart or lose their disk. A liveness *)
(*           check leaves out the owner of the last ballot, whose restart  *)
(*           or wipe ends its ballot.                                      *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Acceptors, Learners, Commands, Noop, NB, NS, Q1, Q2, Owner,
          Alloc, Gap, Durable, Restarts, Wipes, Identity, Down, Pick, Restartable

Ballots == 1..NB
Slots == 0..(NS - 1)
Values == Commands \cup {Noop}
None == "none"

ASSUME AllocType == Alloc \in {"frontier", "prefix"}
ASSUME GapType == Gap \in {"noop", "skip"}
ASSUME DurableType == Durable \in {"before", "after"}
ASSUME IdentityType == Identity \in BOOLEAN
ASSUME DownType == Down \subseteq Acceptors
ASSUME PickType == Pick \in {"highest", "lowest"}
ASSUME RestartableType == Restartable \subseteq Acceptors
ASSUME LearnersType == Learners \subseteq Acceptors
ASSUME NoopNotCommand == Noop \notin Commands
ASSUME OwnerType == Owner \in [Ballots -> Acceptors]
ASSUME Q2Type == Q2 \in [Slots -> SUBSET SUBSET Acceptors]
\* The facts below are what the proof of `Inv` (`MultiPaxosProof.tla`) uses
\* about the constants. TLC checks each one on each configuration.
ASSUME AcceptorsFinite == IsFiniteSet(Acceptors)
ASSUME NBType == NB \in Nat
ASSUME NSType == NS \in Nat
ASSUME RestartsType == Restarts \in Nat
ASSUME WipesType == Wipes \in Nat
ASSUME NoneNotValue == None \notin Values
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

\* The highest vote that the promises S report at slot s (the lowest with
\* the control `Pick = "lowest"`).
Top(S, s) ==
  LET rep == {m \in S : m.rep[s].bal > 0}
      Above(m, n) == IF Pick = "highest" THEN n.rep[s].bal <= m.rep[s].bal
                     ELSE m.rep[s].bal <= n.rep[s].bal
  IN IF rep = {} THEN NoVote
     ELSE (CHOOSE m \in rep : \A n \in rep : Above(m, n)).rep[s]

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

\* The owner of ballot b wins Phase 1 on the promises S, its own among them
\* (`OwnRecords`), and a Phase-1 quorum. It opens the recovery. An acceptor
\* promises a ballot once (`prom[a] < b`; `MsgOK` in `Inv`), so S holds one
\* promise for each acceptor with no guard; where a control forgets a
\* promise (Durable = "after", Identity = FALSE), a second promise of the
\* same acceptor only adds reports.
Win(b) ==
  /\ phase[b] = "p1"
  /\ \E S \in SUBSET {m \in msgs : m.type = "1b" /\ m.bal = b} :
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

\* The late write of an acceptor that answered first (Durable = "after"): the
\* disk catches up with memory. With Durable = "before" the disk already
\* holds memory (`Inv`: dprom = prom, dvote = vote), and so does a node that
\* is down (it never moved) or wiped (both are empty). A write of what the
\* disk holds changes no variable, so the step needs no guard on the mode,
\* on the difference or on `Live`.
Persist(a) ==
  /\ dprom' = [dprom EXCEPT ![a] = prom[a]]
  /\ dvote' = [dvote EXCEPT ![a] = vote[a]]
  /\ UNCHANGED <<prom, vote, learned, phase, next, front, wiped, restarts, msgs>>

-----------------------------------------------------------------------------
(* Commit and catch-up *)

\* Node n learns value v, chosen at ballot b, at the slots L. Each learn is
\* a record (`mark_chosen`): the promise rises to b, and the vote of each
\* slot becomes (b, v), unless the node holds v at a ballot at or above b
\* (a recorded ballot never falls). A promise raised past the ballot of a
\* campaign of n ends the campaign (`become_follower`).
\* `rec` gives the record of each slot of L: [bal, val].
LearnAll(n, rec) ==
  LET L == DOMAIN rec
      top == CHOOSE x \in {rec[s].bal : s \in L} : \A s \in L : rec[s].bal <= x
      p == IF top > prom[n] THEN top ELSE prom[n]
      Keeps(s) == vote[n][s].val = rec[s].val /\ vote[n][s].bal >= rec[s].bal
      nv == [s \in Slots |-> IF s \in L /\ ~Keeps(s) THEN rec[s] ELSE vote[n][s]]
  IN /\ learned' = [learned EXCEPT ![n] = [s \in Slots |-> IF s \in L THEN rec[s].val ELSE @[s]]]
     /\ prom' = [prom EXCEPT ![n] = p]
     /\ dprom' = [dprom EXCEPT ![n] = Persisted(dprom[n], p)]
     /\ vote' = [vote EXCEPT ![n] = nv]
     /\ dvote' = [dvote EXCEPT ![n] = [s \in Slots |-> IF s \in L /\ ~Keeps(s)
                                                      THEN Persisted(dvote[n][s], rec[s])
                                                      ELSE dvote[n][s]]]
     /\ phase' = [c \in Ballots |-> IF Owner[c] = n /\ phase[c] = "p1" /\ c < p
                                    THEN "dead" ELSE phase[c]]

\* The leader of ballot b learns slot s at a Phase-2 quorum and commits it.
\* A learn of a slot that the leader learned already writes the record of
\* the chosen value again (`mark_chosen`, `LearnRecordsHeld`) and, as every
\* learn, ends the leader's campaigns below its promise, so the step needs
\* no guard on what the leader learned.
Decide(b, s) ==
  /\ phase[b] = "p2"
  /\ \E v \in Values, Q \in Q2[s] :
       /\ \A a \in Q : [type |-> "2b", acc |-> a, bal |-> b, slot |-> s, val |-> v] \in msgs
       /\ LearnAll(Owner[b], [t \in {s} |-> [bal |-> b, val |-> v]])
  /\ UNCHANGED <<next, front, wiped, restarts, msgs>>

\* Node n catches up from peer p: it learns every slot that p learned (the
\* leader's `Commit`, or a `CatchUpRequest` to any peer). The answer
\* carries each slot's record at p (`serve_catchup`), which holds the
\* learned value (`LearnRecordsHeld`). A learned value is stable, so the
\* model reads it from the peer and sends no message.
CatchUp(n, p) ==
  /\ n \in Learners
  /\ Live(n) /\ Live(p)
  /\ \E s \in Slots : learned[n][s] = None /\ learned[p][s] # None
  /\ LearnAll(n, [s \in {t \in Slots : learned[n][t] = None /\ learned[p][t] # None} |-> vote[p][s]])
  /\ UNCHANGED <<next, front, wiped, restarts, msgs>>

-----------------------------------------------------------------------------
(* Faults *)

\* Node a restarts: memory goes back to the disk, its ballots die, and it
\* keeps a prefix of what it learned (up to its durable chosen index). A node
\* that is down or wiped holds on disk what it holds in memory, learned
\* nothing it can keep and owns no live ballot, so its restart only spends
\* the budget, and the step needs no `Live` guard.
Restart(a) ==
  /\ a \in Restartable
  /\ restarts < Restarts
  /\ restarts' = restarts + 1
  /\ prom' = [prom EXCEPT ![a] = dprom[a]]
  /\ vote' = [vote EXCEPT ![a] = dvote[a]]
  /\ phase' = [b \in Ballots |-> IF Owner[b] = a /\ phase[b] \in {"p1", "p2"} THEN "dead" ELSE phase[b]]
  /\ \E k \in 0..FirstUnlearned(a) :
       learned' = [learned EXCEPT ![a] = [s \in Slots |-> IF s < k THEN @[s] ELSE None]]
  /\ UNCHANGED <<dprom, dvote, next, front, wiped, msgs>>

\* Acceptor a loses its disk: it comes back with no promise and no vote.
Wipe(a) ==
  /\ Live(a)
  /\ a \in Restartable
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

\* A learned slot holds its record: the vote of the node is the learned
\* value (the code's assertion "a chosen slot holds its authoritative
\* accepted record", `node/learn.rs`).
LearnRecordsHeld ==
  \A n \in Acceptors, s \in Slots : learned[n][s] # None => vote[n][s].val = learned[n][s]

\* One proposal for each slot and ballot: a new leader allocates above each
\* slot that its Phase 1 reported, so its fresh commands never land on a
\* slot that its recovery proposes.
OneProposal ==
  \A m, n \in msgs : m.type = "2a" /\ n.type = "2a" /\ m.bal = n.bal /\ m.slot = n.slot => m = n

\* A won campaign's frontier is past every vote that its owner's own promise
\* reported: the candidate's own records seed its tally (`open_phase1` takes
\* `acceptor.records()`), so a leader never allocates a fresh command at a
\* slot it holds a vote at.
OwnRecords ==
  [][\A b \in Ballots :
       phase[b] = "p1" /\ phase'[b] = "p2" =>
         \A m \in msgs : m.type = "1b" /\ m.bal = b /\ m.acc = Owner[b] =>
           \A s \in Slots : m.rep[s].bal > 0 => s < front'[b]]_vars

-----------------------------------------------------------------------------
(* The inductive invariant (#1307 (inductive invariants, then proofs)).    *)
(*                                                                         *)
(* `Inv` holds in every initial state, and every step from a state where   *)
(* `Inv` holds goes to a state where `Inv` holds. `Inv` implies            *)
(* `Agreement`, `LearnedChosen`, `LearnRecordsHeld` and `OneProposal`, so  *)
(* they hold in every reachable state, with no bound on the run.           *)
(* `MC.tla` (`IndInit`) and the `inductive*.cfg` files check it with TLC   *)
(* on bounded instances: TLC starts from every state of the instance where *)
(* `Inv` holds and checks `Inv` after one step.                            *)
(*                                                                         *)
(* It is the invariant of single-decree Paxos for each slot (`SafeAt`, as  *)
(* in Lamport's proved `Paxos.tla`), with three additions:                 *)
(*   - a record is a vote or a learn (`mark_chosen`): the record of a      *)
(*     slot is a proposal, the node never voted above it, and a record     *)
(*     that is not the node's vote holds a value chosen at or below its    *)
(*     ballot;                                                             *)
(*   - a promise reports the record of each slot at or above `from`, and   *)
(*     the node did not vote between that record and the promise;          *)
(*   - the allocator: each slot at or above `next[b]` is free at ballot b  *)
(*     (no value can be chosen there below b), and ballot b proposed       *)
(*     nothing there.                                                      *)
(* It covers `Durable = "before"` and `Identity = TRUE`: the disk is the   *)
(* memory, and a wiped acceptor never takes a step again.                  *)
(***************************************************************************)

VotedFor(a, b, s, v) == [type |-> "2b", acc |-> a, bal |-> b, slot |-> s, val |-> v] \in msgs
Proposed(b, s, v) == [type |-> "2a", bal |-> b, slot |-> s, val |-> v] \in msgs

\* Acceptor a did not vote at ballot c and slot s, and it never will: its
\* promise is above c, or it is wiped and never takes a step again.
WontVoteIn(a, c, s) ==
  /\ \A v \in Values : ~VotedFor(a, c, s, v)
  /\ prom[a] > c \/ a \in wiped

\* No value other than v is chosen or can be chosen at slot s at a ballot
\* below b: a Phase-1 quorum voted v or will not vote at each such ballot.
SafeAt(b, s, v) ==
  \A c \in 1..(b - 1) : \E Q \in Q1 : \A a \in Q : VotedFor(a, c, s, v) \/ WontVoteIn(a, c, s)

\* No value can be chosen at slot s at a ballot below b.
FreeAt(b, s) == \A c \in 1..(b - 1) : \E Q \in Q1 : \A a \in Q : WontVoteIn(a, c, s)

\* A record r of acceptor a at slot s, held at a promise of ballot `below`
\* (NB + 1 for the record it holds now): a proposal, no vote of a above it
\* and below `below`, and a vote of a at its ballot is for its value.
RecordOK(a, s, r, below) ==
  /\ (r.bal = 0) = (r.val = None)
  /\ r.bal < below
  /\ r.bal > 0 => Proposed(r.bal, s, r.val)
  /\ \A c \in Ballots, v \in Values :
       VotedFor(a, c, s, v) /\ c < below => c < r.bal \/ (c = r.bal /\ v = r.val)

ChosenBy(s, b, v) == \E c \in 1..b : ChosenAt(c, s, v)

\* The state of each node.
\* The state of slot s at node a, when a is not wiped.
NodeSlotOK(a, s) ==
  \* Only a learner or a ballot owner learns.
  /\ learned[a][s] # None => a \in Learners \cup {Owner[b] : b \in Ballots}
  /\ RecordOK(a, s, vote[a][s], NB + 1)
  /\ vote[a][s].bal <= prom[a]
  \* A record that is not a vote comes from a learn.
  /\ vote[a][s].bal > 0 /\ ~VotedFor(a, vote[a][s].bal, s, vote[a][s].val)
       => ChosenBy(s, vote[a][s].bal, vote[a][s].val)
  \* A learned slot holds its record, chosen at or below it.
  /\ learned[a][s] # None =>
       /\ vote[a][s].val = learned[a][s]
       /\ ChosenBy(s, vote[a][s].bal, learned[a][s])

NodeOK(a) ==
    IF a \in wiped
    THEN /\ prom[a] = 0
         /\ \A s \in Slots : vote[a][s] = NoVote /\ learned[a][s] = None
    ELSE \A s \in Slots : NodeSlotOK(a, s)

NodeInv == \A a \in Acceptors : NodeOK(a)

\* The state of each ballot.
BallotOK(b) ==
    LET Has1a == \E m \in msgs : m.type = "1a" /\ m.bal = b
    IN /\ phase[b] = "idle" <=> ~Has1a
       /\ phase[b] \in {"idle", "p1"} => next[b] = 0 /\ front[b] = 0
       /\ phase[b] \in {"p2", "dead"} => front[b] <= next[b]
       /\ phase[b] \in {"p1", "p2"} => Owner[b] \notin wiped
       /\ phase[b] # "idle" => prom[Owner[b]] >= b \/ Owner[b] \in wiped
       \* The ballot proposed only below its allocator.
       /\ \A s \in Slots, v \in Values :
            Proposed(b, s, v) => phase[b] \in {"p2", "dead"} /\ s < next[b]
       \* Each slot at or above the allocator is free.
       /\ phase[b] = "p2" => \A s \in Slots : s >= next[b] => FreeAt(b, s)

BallotInv == \A b \in Ballots : BallotOK(b)

\* What each message says is true.
MsgOK(m) ==
    /\ m.type = "1a" =>
         /\ \A n \in msgs : n.type = "1a" /\ n.bal = m.bal => n = m
         /\ \E n \in msgs : n.type = "1b" /\ n.bal = m.bal /\ n.acc = Owner[m.bal]
    /\ m.type = "1b" =>
         /\ [type |-> "1a", bal |-> m.bal, from |-> m.from] \in msgs
         /\ \A n \in msgs : n.type = "1b" /\ n.bal = m.bal /\ n.acc = m.acc => n = m
         /\ m.acc \notin wiped => m.bal <= prom[m.acc]
         /\ \A s \in Slots :
              IF s >= m.from THEN RecordOK(m.acc, s, m.rep[s], m.bal) ELSE m.rep[s] = NoVote
    /\ m.type = "2a" =>
         /\ SafeAt(m.bal, m.slot, m.val)
         /\ \A n \in msgs : n.type = "2a" /\ n.bal = m.bal /\ n.slot = m.slot => n = m
    /\ m.type = "2b" =>
         /\ Proposed(m.bal, m.slot, m.val)
         /\ m.acc \notin wiped => m.bal <= prom[m.acc]

MsgInv == \A m \in msgs : MsgOK(m)

Inv ==
  /\ TypeOK
  /\ restarts <= Restarts
  /\ Cardinality(wiped) <= Wipes
  /\ dprom = prom
  /\ dvote = vote
  /\ NodeInv
  /\ BallotInv
  /\ MsgInv

\* The configurations that `Inv` covers.
InvApplies == Durable = "before" /\ Identity /\ Down = {}

\* Reachability controls: each must be violated.
NoChoice == ~\E s \in Slots, v \in Values : Chosen(s, v)
\* A node learned a slot it never voted for: its record comes from the learn.
NoLearnRecord == ~\E n \in Acceptors, s \in Slots :
                   /\ learned[n][s] # None
                   /\ \A m \in msgs : ~(m.type = "2b" /\ m.acc = n /\ m.slot = s)
NoNoopChosen == ~\E s \in Slots : Chosen(s, Noop) /\ \E v \in Commands, t \in Slots : t > s /\ Chosen(t, v)
=============================================================================
