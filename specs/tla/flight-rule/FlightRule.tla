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
(*                                                                         *)
(* THE FAULTS OF A STORAGE (#1414 (Storage lists the roles on a disk and   *)
(* reports typed faults), rule 4). A storage lists the roles that have     *)
(* records on its disk, and reports each fault as a typed value with one   *)
(* verdict:                                                                *)
(*   Full     a commit wrote nothing: its roles stop (`Stop::Failed`) and  *)
(*            start again from a scan of their disk.                       *)
(*   Lost     a disk lost a record of a role: the list still names the     *)
(*            role (faulty, not empty), and its scan reports `Lost`: the   *)
(*            role stops for good (`Stop::Damaged`).                       *)
(*   Unknown  a commit failed and the backend does not know what it wrote  *)
(*            (fsyncgate): the page cache may still show the batch that    *)
(*            is not durable, so a scan in the same process can lie. The   *)
(*            process stops, and a new one reads the disk.                 *)
(* Storage selects the verdicts:                                           *)
(*   "typed"          the verdicts above;                                  *)
(*   "lost_restore"   control: a role whose scan reports `Lost` starts     *)
(*                    from the records left;                               *)
(*   "list_readable"  control: the list leaves out a role whose records    *)
(*                    are damaged, and placement starts it again with no   *)
(*                    records;                                             *)
(*   "unknown_rescan" control: after `Unknown`, the roles stop and start   *)
(*                    again from a scan in the same process.               *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS
  Acceptors,   \* the acceptor ids
  Values,      \* the values that proposers may propose
  Ballots,     \* the ballots, a finite set of naturals above 0
  ValueOf,     \* ValueOf[b]: the value that the proposer of ballot b proposes when free
  Quorums,     \* the quorums of Acceptors (every two intersect)
  Rule,        \* "named", "own" or "none"
  Crashes,     \* how many acceptor crashes a behaviour may have
  Storage,     \* "typed", "lost_restore", "list_readable" or "unknown_rescan"
  Faults       \* how many storage faults a behaviour may have

ASSUME Rule \in {"named", "own", "none"}
ASSUME Storage \in {"typed", "lost_restore", "list_readable", "unknown_rescan"}
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
  grouped,    \* a storage commit covered two batches or more (a ghost for a reach check)
  down,       \* the acceptors that stopped for good (`Stop::Damaged`)
  faults,     \* how many storage faults happened
  faulted     \* the kinds of storage faults that happened (a ghost for reach checks)

vars == <<mem, pending, durable, durableId, nextId, outbox, net, started, proposed, crashes, grouped, down, faults, faulted>>
\* What a storage fault leaves alone.
proto == <<net, started, proposed, crashes, grouped>>

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
  /\ down \subseteq Acceptors
  /\ faults \in 0..Faults
  /\ faulted \subseteq {"full", "lost", "unknown"}

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
  /\ down = {}
  /\ faults = 0
  /\ faulted = {}

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
  /\ a \notin down
  /\ \E m \in net :
    /\ m.type = "prepare"
    /\ m.bal >= mem[a].promised
    /\ RoleStep(a, [mem[a] EXCEPT !.promised = m.bal],
                [type |-> "promise", from |-> a, bal |-> m.bal, vote |-> mem[a].vote])
  /\ UNCHANGED <<durable, durableId, started, proposed, crashes, grouped, down, faults, faulted>>

\* Input: an Accept. An Accept at or above the promise is voted (again).
OnAccept(a) ==
  /\ a \notin down
  /\ \E m \in net :
    /\ m.type = "accept"
    /\ m.bal >= mem[a].promised
    /\ RoleStep(a, [promised |-> m.bal, vote |-> [bal |-> m.bal, val |-> m.val]],
                [type |-> "accepted", from |-> a, bal |-> m.bal, val |-> m.val])
  /\ UNCHANGED <<durable, durableId, started, proposed, crashes, grouped, down, faults, faulted>>

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
       /\ UNCHANGED <<mem, nextId, started, proposed, crashes, down, faults, faulted>>

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
  /\ UNCHANGED <<durable, durableId, net, started, proposed, grouped, down, faults, faulted>>

(***************************************************************************)
(* The faults of a storage. Each one is a fault of acceptor a's disk; the  *)
(* role restarts as the verdict of Storage says. A behaviour has at most   *)
(* Faults of them.                                                         *)
(***************************************************************************)
\* The role of a starts again from `st`, with its batches and held sends
\* gone, and its next batch id above `id`.
Restart(a, st, id) ==
  /\ mem' = [mem EXCEPT ![a] = st]
  /\ pending' = [pending EXCEPT ![a] = <<>>]
  /\ outbox' = [outbox EXCEPT ![a] = {}]
  /\ nextId' = [nextId EXCEPT ![a] = id + 1]

\* Full: the commit of a's batches wrote nothing. The role stops and starts
\* again from a scan of its disk, which holds what it held. (With nothing to
\* commit, it is a restart; a role that stopped for good has nothing.)
Full(a) ==
  /\ faults < Faults
  /\ faults' = faults + 1
  /\ faulted' = faulted \cup {"full"}
  /\ Restart(a, durable[a], durableId[a])
  /\ UNCHANGED <<durable, durableId, down, proto>>

\* Lost: a's disk lost its records, and the process starts again. The list
\* names a role whose range is damaged, and its scan reports `Lost`. (A loss
\* of an empty disk loses nothing, but the role stops as well.)
Lose(a) ==
  /\ faults < Faults
  /\ faults' = faults + 1
  /\ faulted' = faulted \cup {"lost"}
  /\ durable' = [durable EXCEPT ![a] = Init0]
  /\ Restart(a, Init0, durableId[a])
  /\ IF Storage \in {"lost_restore", "list_readable"}
       THEN UNCHANGED down
       ELSE down' = down \cup {a}
  /\ UNCHANGED <<durableId, proto>>

\* Unknown: the commit of a's first batch failed, and the backend does not
\* know whether it wrote it. The disk does not hold it, but the page cache
\* of the process still shows it.
Unknown(a) ==
  /\ faults < Faults
  /\ faults' = faults + 1
  /\ pending[a] # <<>>
  /\ faulted' = faulted \cup {"unknown"}
  /\ LET id == pending[a][1][1]
         cached == pending[a][1][2]
     IN IF Storage = "unknown_rescan"
          \* The scan reads the cache: the role takes the batch for durable.
          THEN /\ Restart(a, cached, id)
               /\ durableId' = [durableId EXCEPT ![a] = id]
          \* A new process reads the disk.
          ELSE /\ Restart(a, durable[a], durableId[a])
               /\ UNCHANGED durableId
  /\ UNCHANGED <<durable, down, proto>>

\* The proposer of ballot b. It keeps no durable state: ballots are unique by owner.
Prepare(b) ==
  /\ b \notin started
  /\ started' = started \cup {b}
  /\ net' = net \cup {[type |-> "prepare", bal |-> b]}
  /\ UNCHANGED <<mem, pending, durable, durableId, nextId, outbox, proposed, crashes, grouped, down, faults, faulted>>

\* A promise at b answers the prepare of b, so a quorum of them exists only
\* once Prepare(b) ran.
Propose(b) ==
  /\ b \notin proposed
  /\ \E Q \in Quorums :
       LET ps == {m \in net : m.type = "promise" /\ m.bal = b /\ m.from \in Q}
           votes == {m.vote : m \in ps}
           top == CHOOSE v \in votes : \A w \in votes : w.bal <= v.bal
           val == IF top.bal = 0 THEN ValueOf[b] ELSE top.val
       IN /\ \A a \in Q : \E m \in ps : m.from = a
          /\ proposed' = proposed \cup {b}
          /\ net' = net \cup {[type |-> "accept", bal |-> b, val |-> val]}
  /\ UNCHANGED <<mem, pending, durable, durableId, nextId, outbox, started, crashes, grouped, down, faults, faulted>>

Next ==
  \/ \E b \in Ballots : Prepare(b) \/ Propose(b)
  \/ \E a \in Acceptors :
       \/ OnPrepare(a) \/ OnAccept(a) \/ Persist(a)
       \/ Crash(a)
       \/ Full(a) \/ Lose(a) \/ Unknown(a)

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
\* a promise or a vote that a peer already holds. A disk that lost a record
\* is the one exception, and its role stopped for good: it acts on nothing.
SentIsDurable ==
  \A m \in {n \in net : n.type \in {"promise", "accepted"} => n.from \notin down} :
    /\ m.type = "promise" => durable[m.from].promised >= m.bal
    /\ m.type = "accepted" => durable[m.from].vote.bal >= m.bal

(***************************************************************************)
(* Reach checks: each must be violated, so the model is not empty.        *)
(***************************************************************************)

\* A value is chosen after a crash.
NoChosenAfterCrash == ~(crashes > 0 /\ Chosen # {})
\* One storage commit covers two batches (group commit happens).
NoGroupCommit == ~grouped
\* A value is chosen after a storage fault of each kind.
NoChosenAfterFull == ~("full" \in faulted /\ Chosen # {})
NoChosenAfterLost == ~("lost" \in faulted /\ Chosen # {})
NoChosenAfterUnknown == ~("unknown" \in faulted /\ Chosen # {})
=============================================================================
