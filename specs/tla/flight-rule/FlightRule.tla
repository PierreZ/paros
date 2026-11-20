---------------------------- MODULE FlightRule ----------------------------
(***************************************************************************)
(* The flight rule of the journal's sans-IO contract (#1255 (journal       *)
(* sans-IO contract), rule 1; docs/architecture/04-roles.md section 4.2).  *)
(*                                                                         *)
(* A role is a pure state machine. Each step can emit one persist batch,   *)
(* with an id, and sends. Each send names the persist id it waits on. The  *)
(* driver may commit several batches in one storage commit (group commit), *)
(* in id order, and it transmits a send only once the batch that the send  *)
(* names is durable. Durability comes back to the role as                  *)
(* Input::Persisted(id); the model leaves that input out, because only the *)
(* driver's transmit side decides safety. A crash drops every batch that  *)
(* is not durable and every send that the driver still holds; the role     *)
(* restarts from                                                           *)
(* Input::Recovered(durable state).                                        *)
(*                                                                         *)
(* The protocol under the rule is single-decree Paxos on three acceptors.  *)
(* Only the acceptors keep durable state; that is where the rule matters.  *)
(* A reply that a step sends with no write of its own (a second copy of a  *)
(* Prepare or an Accept that the acceptor already holds) must still wait   *)
(* on the batch that recorded the state the reply reports: that batch may  *)
(* still be in flight.                                                     *)
(*                                                                         *)
(* Rule selects what the waits-on id of a send is:                         *)
(*   "named"  the id of the last batch the role emitted (the rule);        *)
(*   "own"    the id of the batch of this step, or none when the step      *)
(*            wrote nothing (control: the duplicate-message bug);          *)
(*   "none"   a send waits on nothing (control: send before persist).      *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS
  Acceptors,   \* the acceptor ids
  Values,      \* the values that proposers may propose
  Ballots,     \* the ballots, a finite set of naturals above 0
  ValueOf,     \* ValueOf[b]: the value that the proposer of ballot b proposes when free
  Quorums,     \* the quorums of Acceptors (every two intersect)
  Rule,        \* "named", "own" or "none"
  Crashes      \* how many acceptor crashes a behaviour may have

ASSUME Rule \in {"named", "own", "none"}
ASSUME Ballots \subseteq (Nat \ {0})
ASSUME \A Q1, Q2 \in Quorums : Q1 \cap Q2 # {}

\* The lowest ballot skips Phase 1: no lower ballot can hold a vote. Its
\* proposer starts with its Accept on the network.
First == CHOOSE b \in Ballots : \A c \in Ballots : b <= c

None == 0           \* the waits-on id of a send that waits on nothing
NoVote == [bal |-> 0, val |-> "none"]

\* The durable state of an acceptor: its promise and its vote.
States == [promised : Ballots \cup {0}, vote : [bal : Ballots \cup {0}, val : Values \cup {"none"}]]
Init0 == [promised |-> 0, vote |-> NoVote]

VARIABLES
  mem,        \* mem[a]: the state that the role of a holds in memory
  pending,    \* pending[a]: the batches emitted and not yet durable, <<id, state>> in id order
  durable,    \* durable[a]: the state on a's disk
  durableId,  \* durableId[a]: the id of the last durable batch of a
  nextId,     \* nextId[a]: the id of the next batch of a
  outbox,     \* outbox[a]: the sends that a's driver holds, <<msg, waitsOn>>
  net,        \* the messages on the network (a set: loss, reorder, duplicates)
  started,    \* the ballots whose proposer sent its Prepare
  proposed,   \* the ballots whose proposer sent its Accept
  crashes,    \* how many crashes happened
  grouped     \* a storage commit covered two batches or more (a ghost for a reach check)

vars == <<mem, pending, durable, durableId, nextId, outbox, net, started, proposed, crashes, grouped>>

Msgs ==
  [type : {"prepare"}, bal : Ballots]
    \cup [type : {"accept"}, bal : Ballots, val : Values]
    \cup [type : {"promise"}, from : Acceptors, bal : Ballots, vote : [bal : Ballots \cup {0}, val : Values \cup {"none"}]]
    \cup [type : {"accepted"}, from : Acceptors, bal : Ballots, val : Values]

TypeOK ==
  /\ mem \in [Acceptors -> States]
  /\ durable \in [Acceptors -> States]
  /\ durableId \in [Acceptors -> Nat]
  /\ nextId \in [Acceptors -> Nat \ {0}]
  /\ \A a \in Acceptors : \A i \in 1..Len(pending[a]) :
        pending[a][i][1] \in Nat /\ pending[a][i][2] \in States
  /\ \A a \in Acceptors : \A s \in outbox[a] : s[1] \in Msgs /\ s[2] \in Nat
  /\ net \subseteq Msgs
  /\ started \subseteq Ballots
  /\ proposed \subseteq Ballots
  /\ crashes \in 0..Crashes
  /\ grouped \in BOOLEAN

Init ==
  /\ mem = [a \in Acceptors |-> Init0]
  /\ pending = [a \in Acceptors |-> <<>>]
  /\ durable = [a \in Acceptors |-> Init0]
  /\ durableId = [a \in Acceptors |-> 0]
  /\ nextId = [a \in Acceptors |-> 1]
  /\ outbox = [a \in Acceptors |-> {}]
  /\ net = {[type |-> "accept", bal |-> First, val |-> ValueOf[First]]}
  /\ started = {First}
  /\ proposed = {First}
  /\ crashes = 0
  /\ grouped = FALSE

(***************************************************************************)
(* One step of the role of acceptor a. new is the state after the step;    *)
(* reply is the one send of the step. A step whose state changed emits one *)
(* batch. The waits-on id of the reply follows Rule.                       *)
(***************************************************************************)
RoleStep(a, new, reply) ==
  LET writes == new # mem[a]
      id == nextId[a]
      lastBatch == nextId[a] - 1
      waits == CASE Rule = "named" -> IF writes THEN id ELSE lastBatch
                 [] Rule = "own"   -> IF writes THEN id ELSE None
                 [] Rule = "none"  -> None
  IN /\ mem' = [mem EXCEPT ![a] = new]
     /\ pending' = IF writes THEN [pending EXCEPT ![a] = Append(@, <<id, new>>)] ELSE pending
     /\ nextId' = IF writes THEN [nextId EXCEPT ![a] = id + 1] ELSE nextId
     /\ IF waits <= durableId[a]
          THEN /\ net' = net \cup {reply}
               /\ UNCHANGED outbox
          ELSE /\ outbox' = [outbox EXCEPT ![a] = @ \cup {<<reply, waits>>}]
               /\ UNCHANGED net

\* Input: a Prepare. A Prepare at or above the promise is promised (again).
OnPrepare(a) ==
  \E m \in net :
    /\ m.type = "prepare"
    /\ m.bal >= mem[a].promised
    /\ RoleStep(a, [mem[a] EXCEPT !.promised = m.bal],
                [type |-> "promise", from |-> a, bal |-> m.bal, vote |-> mem[a].vote])
    /\ UNCHANGED <<durable, durableId, started, proposed, crashes, grouped>>

\* Input: an Accept. An Accept at or above the promise is voted (again).
OnAccept(a) ==
  \E m \in net :
    /\ m.type = "accept"
    /\ m.bal >= mem[a].promised
    /\ RoleStep(a, [promised |-> m.bal, vote |-> [bal |-> m.bal, val |-> m.val]],
                [type |-> "accepted", from |-> a, bal |-> m.bal, val |-> m.val])
    /\ UNCHANGED <<durable, durableId, started, proposed, crashes, grouped>>

(***************************************************************************)
(* The driver commits the pending batches up to the k-th, in id order, in  *)
(* one atomic storage commit (group commit). The commit is atomic: no torn *)
(* batch (rule 4 of the contract).                                         *)
(***************************************************************************)
Persist(a) ==
  \E k \in 1..Len(pending[a]) :
    LET id == pending[a][k][1]
        ready == {s \in outbox[a] : s[2] <= id}
    IN /\ durable' = [durable EXCEPT ![a] = pending[a][k][2]]
       /\ durableId' = [durableId EXCEPT ![a] = id]
       /\ pending' = [pending EXCEPT ![a] = SubSeq(@, k + 1, Len(@))]
       /\ outbox' = [outbox EXCEPT ![a] = @ \ ready]
       /\ net' = net \cup {s[1] : s \in ready}
       /\ grouped' = (grouped \/ k >= 2)
       /\ UNCHANGED <<mem, nextId, started, proposed, crashes>>

(***************************************************************************)
(* A crash: the batches that are not durable and the held sends are lost.  *)
(* The role restarts with Input::Recovered(durable state, durable id); its *)
(* next batch id is above every durable id, so a lost id is never reused   *)
(* by a send that survived.                                                *)
(***************************************************************************)
Crash(a) ==
  /\ crashes < Crashes
  /\ crashes' = crashes + 1
  /\ mem' = [mem EXCEPT ![a] = durable[a]]
  /\ pending' = [pending EXCEPT ![a] = <<>>]
  /\ outbox' = [outbox EXCEPT ![a] = {}]
  /\ nextId' = [nextId EXCEPT ![a] = durableId[a] + 1]
  /\ UNCHANGED <<durable, durableId, net, started, proposed, grouped>>

\* The proposer of ballot b. It keeps no durable state: ballots are unique by owner.
Prepare(b) ==
  /\ b \notin started
  /\ started' = started \cup {b}
  /\ net' = net \cup {[type |-> "prepare", bal |-> b]}
  /\ UNCHANGED <<mem, pending, durable, durableId, nextId, outbox, proposed, crashes, grouped>>

Propose(b) ==
  /\ b \in started
  /\ b \notin proposed
  /\ \E Q \in Quorums :
       LET ps == {m \in net : m.type = "promise" /\ m.bal = b /\ m.from \in Q}
           votes == {m.vote : m \in ps}
           top == CHOOSE v \in votes : \A w \in votes : w.bal <= v.bal
           val == IF top.bal = 0 THEN ValueOf[b] ELSE top.val
       IN /\ \A a \in Q : \E m \in ps : m.from = a
          /\ proposed' = proposed \cup {b}
          /\ net' = net \cup {[type |-> "accept", bal |-> b, val |-> val]}
  /\ UNCHANGED <<mem, pending, durable, durableId, nextId, outbox, started, crashes, grouped>>

Next ==
  \/ \E b \in Ballots : Prepare(b) \/ Propose(b)
  \/ \E a \in Acceptors :
       \/ OnPrepare(a) \/ OnAccept(a) \/ Persist(a)
       \/ Crash(a)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Properties.                                                             *)
(***************************************************************************)

\* v is chosen at ballot b: a quorum sent Accepted(b, v).
ChosenAt(b, v) ==
  \E Q \in Quorums : \A a \in Q :
    [type |-> "accepted", from |-> a, bal |-> b, val |-> v] \in net
Chosen == {v \in Values : \E b \in Ballots : ChosenAt(b, v)}

\* Paxos safety: at most one value is chosen.
Agreement == Cardinality(Chosen) <= 1

\* What a sent reply reports is on its sender's disk: no crash can take back
\* a promise or a vote that a peer already holds.
SentIsDurable ==
  \A m \in net :
    /\ m.type = "promise" => durable[m.from].promised >= m.bal
    /\ m.type = "accepted" => durable[m.from].vote.bal >= m.bal

(***************************************************************************)
(* Reach checks: each must be violated, so the model is not empty.        *)
(***************************************************************************)

\* A value is chosen after a crash.
NoChosenAfterCrash == ~(crashes > 0 /\ Chosen # {})
\* One storage commit covers two batches (group commit happens).
NoGroupCommit == ~grouped
=============================================================================
