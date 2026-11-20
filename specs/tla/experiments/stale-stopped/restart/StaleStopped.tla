----------------------------- MODULE StaleStopped -----------------------------
(***************************************************************************)
(* The ballot of the handover's decree across a restart of the node that   *)
(* runs it, for #1229 (a stale `Stopped` reply reuses a decree ballot).    *)
(*                                                                         *)
(* One node runs the matchmaker set handover (`MatchmakerReconfigurer`).   *)
(* It sends `Stop` to the matchmakers of the old generation, counts the    *)
(* first `Stopped` of each member until a majority answered, and opens the *)
(* successor decree at round max(round, floor) + 1, where the floor is the *)
(* highest decree promise that the counted `Stopped` replies carry. Then   *)
(* it runs single-decree Paxos over the matchmakers (`decree.rs`). The     *)
(* node can restart: its volatile state is lost and a new incarnation      *)
(* starts the handover again, with another target.                         *)
(*                                                                         *)
(* A message is never lost from `msgs`, so a message can arrive late, two  *)
(* times, or never. A `Stopped` reply sent to an earlier incarnation can   *)
(* so reach a later one. The ballots of other nodes are not modelled: a    *)
(* ballot names its node, so they never equal a ballot of this node.       *)
(*                                                                         *)
(* As in the code, an acceptor promises again at an equal ballot           *)
(* (`acceptor.rs`), the proposer counts a `Promised` and an `Accepted` by  *)
(* sender and ballot, and an `Accepted` carries no value (`message.rs`):   *)
(* the proposer learns the value it proposed once a majority sent          *)
(* `Accepted` at its ballot.                                               *)
(*                                                                         *)
(* Constants:                                                              *)
(*   Bind   how a new incarnation avoids a ballot of an earlier one.       *)
(*          "none": nothing ties a `Stopped` reply to an incarnation (the  *)
(*            code today, the control).                                    *)
(*          "nonce": `Stop` carries a nonce of the incarnation and         *)
(*            `Stopped` echoes it; only a reply with the current nonce     *)
(*            counts.                                                      *)
(*          "durable": the node writes its decree round to disk before it  *)
(*            sends the `Prepare` of that round, and a new incarnation     *)
(*            starts from it.                                              *)
(*          "durable2a": the same write, made only before the first        *)
(*            `Accept` of the round.                                       *)
(*   Floor  TRUE: the decree opens above the stop quorum's floor.          *)
(*          FALSE: a control, the floor is not used.                       *)
(*   NR     the highest round of a decree.                                 *)
(*   Restarts how many times the node may restart.                         *)
(*   Retries  TRUE: an incarnation may open its decree again at a higher   *)
(*            round. FALSE keeps the CI checks small.                      *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Acceptors, Values, NR, Restarts, Retries, Quorums, Bind, Floor

ASSUME Bind \in {"none", "nonce", "durable", "durable2a"}
ASSUME Floor \in BOOLEAN
ASSUME Retries \in BOOLEAN
ASSUME \A Q, R \in Quorums : Q \cap R # {}

Rounds == 1..NR
None == "none"
Max(a, b) == IF a > b THEN a ELSE b

VARIABLES prom,    \* acceptor: the highest round it promised or voted at
          vbal,    \* acceptor: the round of its vote, 0 when it has none
          vval,    \* acceptor: the value of its vote
          msgs,    \* every message sent
          inc,     \* node: its incarnation (a nonce is unique to one)
          dround,  \* node, on disk: the decree round (Bind = "durable*")
          round,   \* node, volatile: the last round this incarnation used
          phase,   \* node, volatile: "idle", "stop", "p1", "p2", "done"
          target,  \* node, volatile: the value it proposes
          acks,    \* node, volatile: the members whose `Stopped` counted
          floor,   \* node, volatile: the highest promise of those replies
          learned  \* the values a node learned as chosen

vars == <<prom, vbal, vval, msgs, inc, dround, round, phase, target, acks, floor, learned>>

Msg ==
  [type : {"stop"}, inc : 0..Restarts]
    \cup [type : {"stopped"}, acc : Acceptors, inc : 0..Restarts, prom : 0..NR]
    \cup [type : {"1a"}, bal : Rounds]
    \cup [type : {"1b"}, acc : Acceptors, bal : Rounds, vbal : 0..NR, vval : Values \cup {None}]
    \cup [type : {"2a"}, bal : Rounds, val : Values]
    \cup [type : {"2b"}, acc : Acceptors, bal : Rounds, val : Values]

TypeOK ==
  /\ prom \in [Acceptors -> 0..NR]
  /\ vbal \in [Acceptors -> 0..NR]
  /\ vval \in [Acceptors -> Values \cup {None}]
  /\ msgs \subseteq Msg
  /\ inc \in 0..Restarts
  /\ dround \in 0..NR
  /\ round \in 0..NR
  /\ phase \in {"idle", "stop", "p1", "p2", "done"}
  /\ target \in Values \cup {None}
  /\ acks \subseteq Acceptors
  /\ floor \in 0..NR
  /\ learned \subseteq Values

Init ==
  /\ prom = [a \in Acceptors |-> 0]
  /\ vbal = [a \in Acceptors |-> 0]
  /\ vval = [a \in Acceptors |-> None]
  /\ msgs = {}
  /\ inc = 0
  /\ dround = 0
  /\ round = 0
  /\ phase = "idle"
  /\ target = None
  /\ acks = {}
  /\ floor = 0
  /\ learned = {}

Send(m) == msgs' = msgs \cup {m}
Acc == <<prom, vbal, vval>>

\* The node starts a handover that proposes v: it sends `Stop`.
Start(v) ==
  /\ phase = "idle"
  /\ phase' = "stop"
  /\ target' = v
  /\ acks' = {}
  /\ floor' = 0
  /\ Send([type |-> "stop", inc |-> inc])
  /\ UNCHANGED <<prom, vbal, vval, inc, dround, round, learned>>

\* A matchmaker answers the `Stop` of incarnation i with its decree promise,
\* at any time after the `Stop` was sent, and the answer echoes the nonce.
\* The model sends one answer for each member and incarnation (the network
\* can still deliver it late, two times, or to a later incarnation): a
\* second answer to one incarnation is left out to keep the state space
\* small. The reconfigurer counts one answer of each member anyway.
Stopped(a) ==
  /\ \E i \in 0..inc :
       /\ [type |-> "stop", inc |-> i] \in msgs
       /\ ~\E m \in msgs : m.type = "stopped" /\ m.acc = a /\ m.inc = i
       /\ Send([type |-> "stopped", acc |-> a, inc |-> i, prom |-> prom[a]])
  /\ UNCHANGED <<prom, vbal, vval, inc, dround, round, phase, target, acks, floor, learned>>

\* The node counts the first `Stopped` of each member (`fold_stopping`).
FoldStopped ==
  /\ phase = "stop"
  /\ \E m \in msgs :
       /\ m.type = "stopped"
       /\ m.acc \notin acks
       /\ Bind = "nonce" => m.inc = inc
       /\ acks' = acks \cup {m.acc}
       /\ floor' = Max(floor, m.prom)
  /\ UNCHANGED <<prom, vbal, vval, msgs, inc, dround, round, phase, target, learned>>

\* The round from which a new decree opens: the volatile round, the round on
\* disk, and the stop quorum's floor.
Base ==
  Max(IF Bind \in {"durable", "durable2a"} THEN Max(round, dround) ELSE round,
      IF Floor THEN floor ELSE 0)

\* A majority answered `Stop`: the decree opens at Base + 1 and sends its
\* `Prepare`. With Bind = "durable" the round is on disk first.
Open ==
  /\ phase = "stop"
  /\ \E Q \in Quorums : Q \subseteq acks
  /\ Base + 1 \in Rounds
  /\ round' = Base + 1
  /\ dround' = IF Bind = "durable" THEN Base + 1 ELSE dround
  /\ phase' = "p1"
  /\ Send([type |-> "1a", bal |-> Base + 1])
  /\ UNCHANGED <<prom, vbal, vval, inc, target, acks, floor, learned>>

\* A preempted decree opens again at a higher round (`abort`, then `start`:
\* within one incarnation the round only rises).
Retry ==
  /\ Retries
  /\ phase \in {"p1", "p2"}
  /\ phase' = "stop"
  /\ acks' = {}
  /\ floor' = 0
  /\ Send([type |-> "stop", inc |-> inc])
  /\ UNCHANGED <<prom, vbal, vval, inc, dround, round, target, learned>>

\* A matchmaker promises round b, or promises it again, and reports its vote.
Promise(a, b) ==
  /\ [type |-> "1a", bal |-> b] \in msgs
  /\ prom[a] <= b
  /\ prom' = [prom EXCEPT ![a] = b]
  /\ Send([type |-> "1b", acc |-> a, bal |-> b, vbal |-> vbal[a], vval |-> vval[a]])
  /\ UNCHANGED <<vbal, vval, inc, dround, round, phase, target, acks, floor, learned>>

\* The value Phase 1 selects from the promises S (P2c), else the target.
Selected(S) ==
  LET rep == {m \in S : m.vbal > 0}
      top == CHOOSE m \in rep : \A n \in rep : n.vbal <= m.vbal
  IN IF rep = {} THEN target ELSE top.vval

\* Phase 1 closes on one promise of each member of a majority, any promise
\* at the round of this decree (the code counts them by sender and ballot),
\* and Phase 2 sends `Accept`. With Bind = "durable2a" the round is on disk
\* first.
Propose ==
  /\ phase = "p1"
  /\ \E S \in SUBSET {m \in msgs : m.type = "1b" /\ m.bal = round} :
       /\ \A m, n \in S : m.acc = n.acc => m = n
       /\ \E Q \in Quorums : Q = {m.acc : m \in S}
       /\ target' = Selected(S)
       /\ Send([type |-> "2a", bal |-> round, val |-> Selected(S)])
  /\ dround' = IF Bind = "durable2a" THEN Max(dround, round) ELSE dround
  /\ phase' = "p2"
  /\ UNCHANGED <<prom, vbal, vval, inc, round, acks, floor, learned>>

\* A matchmaker votes for the value of an `Accept` at round b. A vote for a
\* second value at one round is the assertion of `acceptor.rs`; the model
\* lets it happen so that `OneValuePerRound` reports it.
Accept(a, b) ==
  /\ prom[a] <= b
  /\ \E m \in msgs :
       /\ m.type = "2a" /\ m.bal = b
       /\ [type |-> "2b", acc |-> a, bal |-> b, val |-> m.val] \notin msgs
       /\ prom' = [prom EXCEPT ![a] = b]
       /\ vbal' = [vbal EXCEPT ![a] = b]
       /\ vval' = [vval EXCEPT ![a] = m.val]
       /\ Send([type |-> "2b", acc |-> a, bal |-> b, val |-> m.val])
  /\ UNCHANGED <<inc, dround, round, phase, target, acks, floor, learned>>

\* A majority sent `Accepted` at the round of this decree: the node learns
\* its own value. `Accepted` carries no value, so a vote for another value
\* at this round counts too.
Learn ==
  /\ phase = "p2"
  /\ \E Q \in Quorums : \A a \in Q : \E m \in msgs :
       m.type = "2b" /\ m.acc = a /\ m.bal = round
  /\ learned' = learned \cup {target}
  /\ phase' = "done"
  /\ UNCHANGED <<prom, vbal, vval, msgs, inc, dround, round, target, acks, floor>>

\* The node restarts: its volatile state is gone, its disk stays.
Restart ==
  /\ inc < Restarts
  /\ inc' = inc + 1
  /\ round' = 0
  /\ phase' = "idle"
  /\ target' = None
  /\ acks' = {}
  /\ floor' = 0
  /\ UNCHANGED <<prom, vbal, vval, msgs, dround, learned>>

Next ==
  \/ \E v \in Values : Start(v)
  \/ \E a \in Acceptors : Stopped(a)
  \/ FoldStopped \/ Open \/ Retry \/ Propose \/ Learn \/ Restart
  \/ \E a \in Acceptors, b \in Rounds : Promise(a, b) \/ Accept(a, b)

Spec == Init /\ [][Next]_vars

\* A value is chosen once a majority voted for it at one round.
ChosenAt(b, v) == \E Q \in Quorums : \A a \in Q : [type |-> "2b", acc |-> a, bal |-> b, val |-> v] \in msgs
Chosen(v) == \E b \in Rounds : ChosenAt(b, v)

\* Safety: one successor is chosen, and a node learns only that one.
Agreement == \A v, w \in Values : Chosen(v) /\ Chosen(w) => v = w
LearnOnlyChosen == \A v \in learned : Chosen(v)

\* One round, one value: no two votes of one round differ. A break is the
\* panic of `proposer.rs` (two Phase-1 reports of one ballot) or of
\* `acceptor.rs` (an `Accept` of a second value at a voted ballot).
OneValuePerRound ==
  \A m, n \in msgs :
    (m.type = "2b" /\ n.type = "2b" /\ m.bal = n.bal) => m.val = n.val

\* Reachability controls: each must be violated.
NoLearn == learned = {}
NoLearnAfterRestart == ~(inc > 0 /\ learned # {})
=============================================================================
