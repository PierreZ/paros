-------------------------------- MODULE Decree --------------------------------
(***************************************************************************)
(* Single-decree Paxos as `paros-consensus` builds it (`decree.rs`,        *)
(* `Decree<Id, V>`): the shared Proposer and Acceptor over a log of one    *)
(* slot. Rework 1.15, #1186 (TLA+ model decree).                           *)
(*                                                                         *)
(* The spec is Lamport's message-passing Paxos (`Paxos.tla`, "Paxos Made   *)
(* Simple"). A message is never lost from `msgs`, so a message can arrive  *)
(* late, two times, or never. Each ballot has one proposer, and a ballot   *)
(* that stops (a `Nack` preempted it) is only a ballot that takes no more  *)
(* steps: a retry is a higher ballot.                                      *)
(*                                                                         *)
(* The two uses of a decree differ only in their constants:                *)
(*   - the matchmaker handover chooses the successor set over the          *)
(*     matchmakers under a majority, Phase1 = "quorum";                    *)
(*   - `cell init` chooses the cell plan over the listed members under a   *)
(*     majority, Phase1 = "every" (`Phase1Rule::EveryAcceptor`): Phase 1   *)
(*     waits for a promise of every member.                                *)
(*                                                                         *)
(* A wipe erases the disk of an acceptor: its promise and its vote are     *)
(* gone, and it answers Phase 1 as a new acceptor. With Identity = TRUE    *)
(* the wiped acceptor never votes again: it answers with a new node id,    *)
(* and the plan names the old one (`not_a_member`, the rule of `cell init` *)
(* in docs/architecture/03-1-cell-tenant-and-bootstrap.md). With           *)
(* Identity = FALSE it votes again as the old acceptor, which is all that  *)
(* `decree.rs` alone can see.                                              *)
(*                                                                         *)
(* Constants:                                                              *)
(*   Phase1  "quorum": Phase 2 opens at a Phase-1 quorum of promises.      *)
(*           "every": Phase 2 opens only when every acceptor promised.     *)
(*   Pick    "highest": the proposer takes the value of the highest vote   *)
(*           that the promises report, else any value (P2c).               *)
(*           "ignore": a control, the proposer takes any value.            *)
(*   Refuse  TRUE: a wiped acceptor is refused at boot as amnesia, and it   *)
(*           takes no step (docs/architecture/05-failure-model-and-zones.md, *)
(*           the rule of the matchmaker handover). FALSE: it answers as     *)
(*           Identity says.                                                *)
(*   Wipes   how many acceptors may be wiped, each at most one time.       *)
(*   Down    the acceptors that never take a step (liveness checks only).  *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Acceptors, Values, NB, Quorums, Phase1, Pick, Identity, Refuse, Wipes, Down

ASSUME Phase1Type == Phase1 \in {"quorum", "every"}
ASSUME Pick \in {"highest", "ignore"}
ASSUME IdentityType == Identity \in BOOLEAN
ASSUME RefuseType == Refuse \in BOOLEAN
ASSUME Down \subseteq Acceptors
\* The facts below are what the proof of `Inv` (`DecreeProof.tla`) uses
\* about the constants. TLC checks each one on each configuration.
ASSUME AcceptorsFinite == IsFiniteSet(Acceptors)
ASSUME NBType == NB \in Nat
ASSUME WipesType == Wipes \in Nat
ASSUME QuorumsType == Quorums \subseteq SUBSET Acceptors /\ Quorums # {}
\* Every two quorums intersect: the majority rule of the decree.
ASSUME QuorumsIntersect == \A Q, R \in Quorums : Q \cap R # {}

Ballots == 1..NB
None == "none"
ASSUME NoneNotValue == None \notin Values

VARIABLES prom,   \* acceptor: the highest ballot it promised or voted at
          vbal,   \* acceptor: the ballot of its vote, 0 when it has none
          vval,   \* acceptor: the value of its vote
          wiped,  \* the acceptors that lost their disk
          msgs    \* every message sent

vars == <<prom, vbal, vval, wiped, msgs>>

Msg ==
  [type : {"1a"}, bal : Ballots]
    \cup [type : {"1b"}, acc : Acceptors, bal : Ballots, vbal : 0..NB, vval : Values \cup {None}]
    \cup [type : {"2a"}, bal : Ballots, val : Values]
    \cup [type : {"2b"}, acc : Acceptors, bal : Ballots, val : Values]

TypeOK ==
  /\ prom \in [Acceptors -> 0..NB]
  /\ vbal \in [Acceptors -> 0..NB]
  /\ vval \in [Acceptors -> Values \cup {None}]
  /\ wiped \subseteq Acceptors
  /\ msgs \subseteq Msg

Init ==
  /\ prom = [a \in Acceptors |-> 0]
  /\ vbal = [a \in Acceptors |-> 0]
  /\ vval = [a \in Acceptors |-> None]
  /\ wiped = {}
  /\ msgs = {}

Send(m) == msgs' = msgs \cup {m}

\* The proposer of ballot b opens Phase 1.
Prepare(b) ==
  /\ [type |-> "1a", bal |-> b] \notin msgs
  /\ Send([type |-> "1a", bal |-> b])
  /\ UNCHANGED <<prom, vbal, vval, wiped>>

\* Acceptor a promises ballot b and reports its vote.
Promise(a, b) ==
  /\ a \notin Down
  /\ Refuse => a \notin wiped
  /\ [type |-> "1a", bal |-> b] \in msgs
  /\ prom[a] < b
  /\ prom' = [prom EXCEPT ![a] = b]
  /\ Send([type |-> "1b", acc |-> a, bal |-> b, vbal |-> vbal[a], vval |-> vval[a]])
  /\ UNCHANGED <<vbal, vval, wiped>>

\* The acceptors whose promises can open Phase 2.
Phase1Done(acc) ==
  IF Phase1 = "every" THEN acc = Acceptors ELSE \E Q \in Quorums : Q \subseteq acc

\* The value a proposer may select from the promises S: the value of a
\* promise that reports the highest vote. Two promises that report a vote
\* at the same ballot report the same value (a ballot has one 2a message),
\* so this set has one member. It is written without CHOOSE so that the
\* proof (`DecreeProof.tla`) needs no fact about the maximum of a set.
Selectable(S) ==
  LET rep == {m \in S : m.vbal > 0}
      top == {m \in rep : \A n \in rep : n.vbal <= m.vbal}
  IN IF Pick = "ignore" \/ rep = {} THEN Values ELSE {m.vval : m \in top}

\* The proposer of ballot b closes Phase 1 on the promises S, one for each
\* acceptor (the first that arrived; the proposer ignores a second promise of
\* the same acceptor), and opens Phase 2 with one value.
Propose(b) ==
  /\ ~\E m \in msgs : m.type = "2a" /\ m.bal = b
  /\ \E S \in SUBSET {m \in msgs : m.type = "1b" /\ m.bal = b} :
       /\ \A m, n \in S : m.acc = n.acc => m = n
       /\ Phase1Done({m.acc : m \in S})
       /\ \E v \in Selectable(S) : Send([type |-> "2a", bal |-> b, val |-> v])
  /\ UNCHANGED <<prom, vbal, vval, wiped>>

\* Acceptor a votes for the value of ballot b.
Accept(a, b) ==
  /\ a \notin Down
  /\ Identity => a \notin wiped
  /\ prom[a] <= b
  /\ vbal[a] # b
  /\ \E m \in msgs :
       /\ m.type = "2a" /\ m.bal = b
       /\ prom' = [prom EXCEPT ![a] = b]
       /\ vbal' = [vbal EXCEPT ![a] = b]
       /\ vval' = [vval EXCEPT ![a] = m.val]
       /\ Send([type |-> "2b", acc |-> a, bal |-> b, val |-> m.val])
  /\ UNCHANGED wiped

\* Acceptor a loses its disk.
Wipe(a) ==
  /\ a \notin wiped
  /\ a \notin Down
  /\ Cardinality(wiped) < Wipes
  /\ wiped' = wiped \cup {a}
  /\ prom' = [prom EXCEPT ![a] = 0]
  /\ vbal' = [vbal EXCEPT ![a] = 0]
  /\ vval' = [vval EXCEPT ![a] = None]
  /\ UNCHANGED msgs

Next ==
  \/ \E b \in Ballots : Prepare(b) \/ Propose(b)
  \/ \E a \in Acceptors, b \in Ballots : Promise(a, b) \/ Accept(a, b)
  \/ \E a \in Acceptors : Wipe(a)

Spec == Init /\ [][Next]_vars

\* Liveness: the last ballot NB is the retry that no higher ballot preempts,
\* as the caller reopens a preempted decree above every refusal. Its
\* proposer and every acceptor that is not down keep taking their steps.
Fairness ==
  /\ WF_vars(Prepare(NB))
  /\ WF_vars(Propose(NB))
  /\ \A a \in Acceptors \ Down : WF_vars(Promise(a, NB)) /\ WF_vars(Accept(a, NB))

LiveSpec == Spec /\ Fairness

\* A value is chosen once a quorum voted for it at one ballot. A vote is a
\* `2b` message, so a wipe does not unchoose a value.
ChosenAt(b, v) == \E Q \in Quorums : \A a \in Q : [type |-> "2b", acc |-> a, bal |-> b, val |-> v] \in msgs
Chosen(v) == \E b \in Ballots : ChosenAt(b, v)

\* Safety: one value is chosen, one time.
Agreement == \A v, w \in Values : Chosen(v) /\ Chosen(w) => v = w

\* Liveness: a decree with a live quorum ends.
Ends == <>(\E v \in Values : Chosen(v))

-----------------------------------------------------------------------------
(* The inductive invariant (#1307 (inductive invariants, then proofs)).    *)
(*                                                                         *)
(* `Inv` holds in every initial state, and every step from a state where   *)
(* `Inv` holds goes to a state where `Inv` holds. `Inv` implies            *)
(* `Agreement`, so `Agreement` holds in every reachable state, with no     *)
(* bound on the run. It is the invariant of Lamport's proved `Paxos.tla`   *)
(* (tlaplus/Examples), with the wipe: a wiped acceptor never takes a step  *)
(* again (`Refuse` and `Identity`), so it counts as one that will not      *)
(* vote. `MC.tla` (`IndInit`) and `inductive.cfg` check it with TLC on a   *)
(* bounded instance: TLC starts from every state of that instance where    *)
(* `Inv` holds and checks `Inv` after one step.                            *)
(***************************************************************************)

VotedFor(a, b, v) == [type |-> "2b", acc |-> a, bal |-> b, val |-> v] \in msgs

\* Acceptor a did not vote at ballot c, and it never will: its promise is
\* above c, or it is wiped and never votes again.
WontVoteIn(a, c) ==
  /\ \A v \in Values : ~VotedFor(a, c, v)
  /\ prom[a] > c \/ (Refuse /\ Identity /\ a \in wiped)

\* No value other than v is chosen or can be chosen at a ballot below b: a
\* quorum voted v or will not vote at each such ballot.
SafeAt(b, v) ==
  \A c \in 1..(b - 1) : \E Q \in Quorums : \A a \in Q : VotedFor(a, c, v) \/ WontVoteIn(a, c)

\* The state of an acceptor: its vote is its highest vote, and its promise
\* is at or above it. A wiped acceptor has an empty disk.
AccOK(a) ==
    IF a \in wiped
    THEN prom[a] = 0 /\ vbal[a] = 0 /\ vval[a] = None
    ELSE /\ vbal[a] <= prom[a]
         /\ (vbal[a] = 0) = (vval[a] = None)
         /\ vbal[a] > 0 => VotedFor(a, vbal[a], vval[a])
         /\ \A c \in (vbal[a] + 1)..NB, v \in Values : ~VotedFor(a, c, v)

AccInv == \A a \in Acceptors : AccOK(a)

\* What each message says is true.
MsgOK(m) ==
    /\ m.type = "1b" =>
         /\ [type |-> "1a", bal |-> m.bal] \in msgs
         /\ m.acc \notin wiped => m.bal <= prom[m.acc]
         /\ m.vbal < m.bal
         /\ (m.vbal = 0) = (m.vval = None)
         /\ m.vbal > 0 => VotedFor(m.acc, m.vbal, m.vval)
         /\ \A c \in (m.vbal + 1)..(m.bal - 1), v \in Values : ~VotedFor(m.acc, c, v)
    /\ m.type = "2a" =>
         /\ SafeAt(m.bal, m.val)
         /\ \A n \in msgs : n.type = "2a" /\ n.bal = m.bal => n = m
    /\ m.type = "2b" =>
         /\ [type |-> "2a", bal |-> m.bal, val |-> m.val] \in msgs
         /\ m.acc \notin wiped => m.bal <= prom[m.acc]

MsgInv == \A m \in msgs : MsgOK(m)

Inv ==
  /\ TypeOK
  /\ Cardinality(wiped) <= Wipes
  /\ AccInv
  /\ MsgInv

\* The invariant holds for the configurations where a wiped acceptor takes
\* no step: no wipe, or `Refuse` and `Identity`. With `Refuse = FALSE`, a
\* wiped acceptor answers Phase 1 with an empty report, and `Phase1 =
\* "every"` alone keeps `Agreement` (`init.cfg`); `Inv` does not cover it.
InvApplies == Wipes = 0 \/ (Refuse /\ Identity)

\* Reachability controls: each must be violated.
NoChoice == ~\E v \in Values : Chosen(v)
NoChoiceAfterWipe == ~(wiped # {} /\ \E v \in Values, b \in Ballots : ChosenAt(b, v) /\ b > 1)
=============================================================================
