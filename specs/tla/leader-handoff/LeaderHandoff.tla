---------------------------- MODULE LeaderHandoff ----------------------------
(***************************************************************************)
(* The cooperative leader handoff of DPaxos, as `paros-consensus` builds   *)
(* it (`ColocatedNode::relinquish_to`, `node/handoff.rs`). Rework 2.8,     *)
(* #1187 (TLA+ model leader-handoff).                                      *)
(*                                                                         *)
(* The model is plain Multi-Paxos (`multi-paxos/`) with a leadership that  *)
(* can move from one node to another with no Phase 1. Leadership is a      *)
(* state of a node, not of a ballot: after a handoff, the ballot names     *)
(* its minter, and another node exercises it.                              *)
(*                                                                         *)
(* The handoff, and the code it follows:                                   *)
(*   - Only a leader that won Phase 1 at its own ballot hands it on: one   *)
(*     hop (`can_relinquish` requires `LeadershipOrigin::Elected`).        *)
(*   - The payload carries the ballot, the allocator frontier `nxt` and    *)
(*     the tail from the first unlearned slot to `nxt`: each slot of the   *)
(*     tail is decided (learned) or pending (an open round at the ballot). *)
(*     The leader refuses a handoff when a slot of the tail is neither.    *)
(*   - The leader steps down in the same step as it sends the payload.     *)
(*   - The payload names its successor (`to`).                             *)
(*   - The successor refuses the payload when its promise is above the     *)
(*     ballot, when it already leads at the ballot, or when the frontier   *)
(*     of the payload is below its own frontier or its first unlearned     *)
(*     slot (`on_relinquish`).                                             *)
(*   - To install, the successor raises its promise to the ballot, learns  *)
(*     the decided slots, adopts the frontier, and proposes each pending   *)
(*     slot again with the same command. It fills no gap with a `Noop`.    *)
(*   - A leader records each round that it opens as its own vote before   *)
(*     it sends the round (`record_own_round`), in a grid column it is not *)
(*     in too. A restart computes the frontier from the votes on disk      *)
(*     (`node/boot.rs`), so the frontier is durable. A leader whose        *)
(*     promise a higher ballot passed still opens a round, and records     *)
(*     nothing: a replayed payload is then refused by the promise guard.   *)
(*   - A learn is a record (`mark_chosen`): the node raises its promise to *)
(*     the ballot that chose the slot and keeps the chosen value as its    *)
(*     vote, unless it holds that value at a ballot at or above.           *)
(*   - A handoff leader whose learned prefix does not reach its frontier   *)
(*     can resign (`tick_handoff_fence`).                                  *)
(*   - A beat at a ballot at or above the promise of a leader makes it a   *)
(*     follower (`on_heartbeat`). `BeatGuard` keeps a leader that holds    *)
(*     the same ballot, the guard that `on_accept` has.                    *)
(*                                                                         *)
(* A message stays in `msgs` for ever, so it can arrive late or two times. *)
(* A message in `lost` is not delivered; a resend delivers it again.       *)
(* Persist before send holds everywhere: memory and disk are one state.   *)
(*                                                                         *)
(* Constants:                                                              *)
(*   Learners    the nodes that catch up from a peer.                      *)
(*   HandBallots the ballots whose leader may hand off.                    *)
(*   Steady      the nodes that never restart (liveness checks only).      *)
(*   Restarts    how many restarts may happen (in the whole run).          *)
(*   Drops       how many messages may be lost (in the whole run). A loss  *)
(*               only removes behaviors, so the safety checks set 0.       *)
(*   DropKinds   the kinds of message that may be lost ("2a", "rel").      *)
(*   Resign      TRUE: a handoff leader may resign on an uncovered fence.  *)
(* Rules, each with a control that turns it off:                           *)
(*   OneHop        TRUE: only an elected leader hands off.                 *)
(*                 FALSE: a control, a handoff leader hands on again.      *)
(*   FrontierGuard TRUE: refuse a payload below the frontier.              *)
(*                 FALSE: a control, a replayed payload rewinds it.        *)
(*   DurableFrontier TRUE: a leader votes for each round it opens.         *)
(*                 FALSE: a control, a restart forgets the frontier.       *)
(*   Pending       "keep": the successor proposes the pending slots again. *)
(*                 "drop": a control, it drops them.                       *)
(*   PromiseGuard  TRUE: a successor refuses a payload below its promise.  *)
(*                 FALSE: a control, it installs it (and keeps the higher  *)
(*                 promise).                                               *)
(*   RecordAll     TRUE: a leader records every round, in a grid column it *)
(*                 is not in too. FALSE: a control, only in its column.    *)
(*   Abdicate      TRUE: the leader steps down as it sends the payload.    *)
(*                 FALSE: a control, it keeps leading.                     *)
(*   Named         TRUE: only the successor that the payload names installs*)
(*                 it. FALSE: a control, any node installs it.             *)
(*   BeatGuard     TRUE: a beat at a leader's own ballot keeps it leader.  *)
(*                 FALSE: a control, the code of today: a late beat of the *)
(*                 predecessor deposes the successor (review of            *)
(*                 paros-consensus, finding 3; fixed by rework 2.7).       *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Acceptors, Learners, Commands, Noop, NB, NS, Q1, Q2, Owner,
          HandBallots, Steady, Restarts, Drops, DropKinds, Resign,
          OneHop, FrontierGuard, DurableFrontier, Pending, BeatGuard,
          PromiseGuard, RecordAll, Abdicate, Named

Ballots == 1..NB
Slots == 0..(NS - 1)
Values == Commands \cup {Noop}
None == "none"

ASSUME Learners \subseteq Acceptors
ASSUME Steady \subseteq Acceptors
ASSUME HandBallots \subseteq Ballots
ASSUME DropKinds \subseteq {"2a", "rel"}
ASSUME Noop \notin Commands
ASSUME Owner \in [Ballots -> Acceptors]
ASSUME Q2 \in [Slots -> SUBSET SUBSET Acceptors]
ASSUME Pending \in {"keep", "drop"}
ASSUME {Resign, OneHop, FrontierGuard, DurableFrontier, BeatGuard} \subseteq BOOLEAN
ASSUME {PromiseGuard, RecordAll, Abdicate, Named} \subseteq BOOLEAN

Vote == [bal : 0..NB, val : Values \cup {None}]
NoVote == [bal |-> 0, val |-> None]
Rounds == [Slots -> Values \cup {None}]
NoRounds == [s \in Slots |-> None]

VARIABLES prom,     \* acceptor: the promise
          vote,     \* acceptor: the vote, per slot
          learned,  \* node: the value it learned, per slot
          phase,    \* ballot: the campaign, "idle", "p1", "won" or "dead"
          role,     \* node: "follower" or "leader"
          bal,      \* node: the ballot it exercises as leader
          origin,   \* node: "elected" or "handoff"
          nxt,      \* node: the allocator frontier (kept on a step down)
          pend,     \* node: the open rounds of its leadership, per slot
          restarts, \* how many restarts happened
          drops,    \* how many messages were lost
          lost,     \* the messages that are lost now
          msgs      \* every message sent

vars == <<prom, vote, learned, phase, role, bal, origin, nxt, pend, restarts, drops, lost, msgs>>

Msg ==
  [type : {"1a"}, bal : Ballots, from : 0..NS]
    \cup [type : {"1b"}, acc : Acceptors, bal : Ballots, from : 0..NS, rep : [Slots -> Vote]]
    \cup [type : {"2a"}, bal : Ballots, slot : Slots, val : Values]
    \cup [type : {"2b"}, acc : Acceptors, bal : Ballots, slot : Slots, val : Values]
    \cup [type : {"beat"}, bal : Ballots, from : Acceptors]
    \cup [type : {"rel"}, from : Acceptors, to : Acceptors, bal : Ballots,
          lo : 0..NS, nxt : 0..NS, dec : Rounds, pen : Rounds]

TypeOK ==
  /\ prom \in [Acceptors -> 0..NB]
  /\ vote \in [Acceptors -> [Slots -> Vote]]
  /\ learned \in [Acceptors -> Rounds]
  /\ phase \in [Ballots -> {"idle", "p1", "won", "dead"}]
  /\ role \in [Acceptors -> {"follower", "leader"}]
  /\ bal \in [Acceptors -> 0..NB]
  /\ origin \in [Acceptors -> {"elected", "handoff"}]
  /\ nxt \in [Acceptors -> 0..NS]
  /\ pend \in [Acceptors -> Rounds]
  /\ restarts \in Nat
  /\ drops \in Nat
  /\ lost \subseteq msgs
  /\ msgs \subseteq Msg

Init ==
  /\ prom = [a \in Acceptors |-> 0]
  /\ vote = [a \in Acceptors |-> [s \in Slots |-> NoVote]]
  /\ learned = [a \in Acceptors |-> NoRounds]
  /\ phase = [b \in Ballots |-> "idle"]
  /\ role = [a \in Acceptors |-> "follower"]
  /\ bal = [a \in Acceptors |-> 0]
  /\ origin = [a \in Acceptors |-> "elected"]
  /\ nxt = [a \in Acceptors |-> 0]
  /\ pend = [a \in Acceptors |-> NoRounds]
  /\ restarts = 0
  /\ drops = 0
  /\ lost = {}
  /\ msgs = {}

Max(S) == CHOOSE x \in S : \A y \in S : y <= x

Send(m) == msgs' = msgs \cup {m}
SendAll(M) == msgs' = msgs \cup M

\* A message that arrives: sent and not lost.
Delivered == msgs \ lost

\* The first slot that the rounds f leave empty.
FirstNone(f) ==
  IF \A s \in Slots : f[s] # None THEN NS
  ELSE CHOOSE s \in Slots : f[s] = None /\ \A t \in Slots : t < s => f[t] # None

FirstUnlearned(n) == FirstNone(learned[n])

\* The frontier that a boot computes: one past the highest vote on disk,
\* never below the first unlearned slot.
BootFrontier(n) ==
  LET voted == {s \in Slots : vote[n][s].bal > 0}
  IN IF voted = {} THEN FirstUnlearned(n) ELSE Max({Max(voted) + 1, FirstUnlearned(n)})

\* A leader that can exercise its ballot: no higher promise passed it
\* (`record_own_round` refuses a round below the promise).
Effective(n) == role[n] = "leader" /\ prom[n] = bal[n]

\* Node n is in a Phase-2 quorum of slot s: on a grid, the slot's column.
InColumn(n, s) == \E Q \in Q2[s] : n \in Q

\* The slots of the rounds R that node n records as its own vote: every
\* round with DurableFrontier, only those of its column with the control
\* `RecordAll = FALSE`.
Recorded(n, R) ==
  IF DurableFrontier
  THEN {s \in Slots : R[s] # None /\ (RecordAll \/ InColumn(n, s))}
  ELSE {}

\* The own vote and the messages of the rounds R that node n opens at
\* ballot b.
OwnVotes(n, b, R) ==
  [s \in Slots |-> IF s \in Recorded(n, R) THEN [bal |-> b, val |-> R[s]] ELSE vote[n][s]]
RoundMsgs(n, b, R) ==
  {[type |-> "2a", bal |-> b, slot |-> s, val |-> R[s]] : s \in {t \in Slots : R[t] # None}}
    \cup {[type |-> "2b", acc |-> n, bal |-> b, slot |-> s, val |-> R[s]] : s \in Recorded(n, R)}

\* Node n learns the values `rec[s].val` at the slots of DOMAIN rec, each
\* chosen at ballot `rec[s].bal`. Each learn is a record (`mark_chosen`):
\* the promise rises to the ballot, and the vote becomes the record unless
\* the node holds that value at a ballot at or above.
LearnRecs(n, rec) ==
  LET L == DOMAIN rec
      top == IF L = {} THEN 0 ELSE Max({rec[s].bal : s \in L})
      Keeps(s) == vote[n][s].val = rec[s].val /\ vote[n][s].bal >= rec[s].bal
  IN [p |-> IF top > prom[n] THEN top ELSE prom[n],
      v |-> [s \in Slots |-> IF s \in L /\ ~Keeps(s) THEN rec[s] ELSE vote[n][s]],
      l |-> [s \in Slots |-> IF s \in L THEN rec[s].val ELSE learned[n][s]]]

-----------------------------------------------------------------------------
(* Phase 1, as in multi-paxos/ *)

PromiseMsg(a, b, from) ==
  [type |-> "1b", acc |-> a, bal |-> b, from |-> from,
   rep |-> [s \in Slots |-> IF s >= from THEN vote[a][s] ELSE NoVote]]

\* The owner of ballot b opens a campaign. A leader that campaigns stops
\* leading its old ballot.
Prepare(b) ==
  LET o == Owner[b]
      from == FirstUnlearned(o)
  IN /\ phase[b] = "idle"
     /\ prom[o] < b
     /\ phase' = [phase EXCEPT ![b] = "p1"]
     /\ prom' = [prom EXCEPT ![o] = b]
     /\ role' = [role EXCEPT ![o] = "follower"]
     /\ origin' = [origin EXCEPT ![o] = "elected"]
     /\ pend' = [pend EXCEPT ![o] = NoRounds]
     /\ SendAll({[type |-> "1a", bal |-> b, from |-> from], PromiseMsg(o, b, from)})
     /\ UNCHANGED <<vote, learned, bal, nxt, restarts, drops, lost>>

Promise(a, b) ==
  /\ prom[a] < b
  /\ \E m \in Delivered :
       /\ m.type = "1a" /\ m.bal = b
       /\ prom' = [prom EXCEPT ![a] = b]
       /\ Send(PromiseMsg(a, b, m.from))
  /\ UNCHANGED <<vote, learned, phase, role, bal, origin, nxt, pend, restarts, drops, lost>>

Top(S, s) ==
  LET rep == {m \in S : m.rep[s].bal > 0}
  IN IF rep = {} THEN NoVote
     ELSE (CHOOSE m \in rep : \A n \in rep : n.rep[s].bal <= m.rep[s].bal).rep[s]

Frontier(S, from) ==
  LET h == {s \in Slots : s >= from /\ \E m \in S : m.rep[s].bal > 0}
  IN IF h = {} THEN from ELSE Max(h) + 1

\* The owner of ballot b wins Phase 1 and leads. Each slot from `from` to
\* the frontier gets the highest reported vote or a `Noop`.
Win(b) ==
  LET o == Owner[b] IN
  /\ phase[b] = "p1"
  /\ prom[o] = b
  /\ \E S \in SUBSET {m \in Delivered : m.type = "1b" /\ m.bal = b} :
       /\ \A m, n \in S : m.acc = n.acc => m = n
       /\ o \in {m.acc : m \in S}
       /\ \E Q \in Q1 : Q \subseteq {m.acc : m \in S}
       /\ LET from == (CHOOSE m \in msgs : m.type = "1a" /\ m.bal = b).from
              f == Frontier(S, from)
              R == [s \in Slots |->
                      IF s >= from /\ s < f
                      THEN (IF Top(S, s).bal > 0 THEN Top(S, s).val ELSE Noop)
                      ELSE None]
          IN /\ nxt' = [nxt EXCEPT ![o] = f]
             /\ pend' = [pend EXCEPT ![o] = R]
             /\ vote' = [vote EXCEPT ![o] = OwnVotes(o, b, R)]
             /\ SendAll(RoundMsgs(o, b, R))
  /\ phase' = [phase EXCEPT ![b] = "won"]
  /\ role' = [role EXCEPT ![o] = "leader"]
  /\ bal' = [bal EXCEPT ![o] = b]
  /\ origin' = [origin EXCEPT ![o] = "elected"]
  /\ UNCHANGED <<prom, learned, restarts, drops, lost>>

-----------------------------------------------------------------------------
(* Phase 2 *)

\* The leader n opens a round for a fresh command at its frontier.
Allocate(n, c) ==
  LET s == nxt[n]
      R == [t \in Slots |-> IF t = s THEN c ELSE None]
  IN /\ Effective(n)
     /\ s < NS
     /\ nxt' = [nxt EXCEPT ![n] = s + 1]
     /\ pend' = [pend EXCEPT ![n][s] = c]
     /\ vote' = [vote EXCEPT ![n] = OwnVotes(n, bal[n], R)]
     /\ SendAll(RoundMsgs(n, bal[n], R))
     /\ UNCHANGED <<prom, learned, phase, role, bal, origin, restarts, drops, lost>>

\* A leader whose promise a higher ballot passed opens a round for a fresh
\* command: it sends the `Accept` and records nothing (`record_own_round`
\* refuses a round below the promise). The round cannot be chosen: every
\* Phase-2 quorum meets the quorum that raised the promise.
AllocateStale(n, c) ==
  LET s == nxt[n] IN
  /\ role[n] = "leader"
  /\ prom[n] > bal[n]
  /\ s < NS
  /\ nxt' = [nxt EXCEPT ![n] = s + 1]
  /\ pend' = [pend EXCEPT ![n][s] = c]
  /\ Send([type |-> "2a", bal |-> bal[n], slot |-> s, val |-> c])
  /\ UNCHANGED <<prom, vote, learned, phase, role, bal, origin, restarts, drops, lost>>

\* The leader n sends an open round again (`resend_pending`).
Resend(n, s) ==
  LET m == [type |-> "2a", bal |-> bal[n], slot |-> s, val |-> pend[n][s]] IN
  /\ Effective(n)
  /\ pend[n][s] # None
  /\ learned[n][s] = None
  /\ m \notin Delivered
  /\ msgs' = msgs \cup {m}
  /\ lost' = lost \ {m}
  /\ UNCHANGED <<prom, vote, learned, phase, role, bal, origin, nxt, pend, restarts, drops>>

Accept(a, b, s) ==
  /\ prom[a] <= b
  /\ \E m \in Delivered :
       /\ m.type = "2a" /\ m.bal = b /\ m.slot = s
       /\ vote[a][s] # [bal |-> b, val |-> m.val]
       /\ prom' = [prom EXCEPT ![a] = b]
       /\ vote' = [vote EXCEPT ![a][s] = [bal |-> b, val |-> m.val]]
       /\ Send([type |-> "2b", acc |-> a, bal |-> b, slot |-> s, val |-> m.val])
  /\ UNCHANGED <<learned, phase, role, bal, origin, nxt, pend, restarts, drops, lost>>

\* The leader n learns slot s at a Phase-2 quorum of its ballot.
Decide(n, s) ==
  /\ role[n] = "leader"
  /\ learned[n][s] = None
  /\ \E v \in Values, Q \in Q2[s] :
       /\ \A a \in Q : [type |-> "2b", acc |-> a, bal |-> bal[n], slot |-> s, val |-> v] \in msgs
       /\ LET r == LearnRecs(n, [t \in {s} |-> [bal |-> bal[n], val |-> v]])
          IN /\ learned' = [learned EXCEPT ![n] = r.l]
             /\ vote' = [vote EXCEPT ![n] = r.v]
             /\ prom' = [prom EXCEPT ![n] = r.p]
  /\ UNCHANGED <<phase, role, bal, origin, nxt, pend, restarts, drops, lost, msgs>>

\* Node n catches up from peer p (`Commit`, `CatchUpRequest`). The answer
\* carries the record of each slot at p (`serve_catchup`), which holds the
\* learned value (`LearnRecordsHeld`).
CatchUp(n, p) ==
  /\ n \in Learners
  /\ \E s \in Slots : learned[n][s] = None /\ learned[p][s] # None
  /\ LET r == LearnRecs(n, [s \in {t \in Slots : learned[n][t] = None /\ learned[p][t] # None} |-> vote[p][s]])
     IN /\ learned' = [learned EXCEPT ![n] = r.l]
        /\ vote' = [vote EXCEPT ![n] = r.v]
        /\ prom' = [prom EXCEPT ![n] = r.p]
  /\ UNCHANGED <<phase, role, bal, origin, nxt, pend, restarts, drops, lost, msgs>>

-----------------------------------------------------------------------------
(* The handoff *)

\* The slots of the tail of node n: from its first unlearned slot to its
\* frontier.
Tail(n) == {s \in Slots : s >= FirstUnlearned(n) /\ s < nxt[n]}

\* The leader n hands its ballot to node t and steps down in the same step.
\* Its frontier covers its first unlearned slot (`LeaderFrontier`, which
\* the code asserts in `relinquish_to`).
Relinquish(n, t) ==
  /\ Effective(n)
  /\ bal[n] \in HandBallots
  /\ OneHop => origin[n] = "elected"
  /\ t # n
  /\ \A s \in Tail(n) : learned[n][s] # None \/ pend[n][s] # None
  /\ Send([type |-> "rel", from |-> n, to |-> t, bal |-> bal[n],
           lo |-> FirstUnlearned(n), nxt |-> nxt[n],
           dec |-> [s \in Slots |-> IF s \in Tail(n) THEN learned[n][s] ELSE None],
           pen |-> [s \in Slots |-> IF s \in Tail(n) /\ learned[n][s] = None
                                    THEN pend[n][s] ELSE None]])
  /\ IF Abdicate
       THEN /\ role' = [role EXCEPT ![n] = "follower"]
            /\ origin' = [origin EXCEPT ![n] = "elected"]
            /\ pend' = [pend EXCEPT ![n] = NoRounds]
       ELSE UNCHANGED <<role, origin, pend>>
  /\ UNCHANGED <<prom, vote, learned, phase, bal, nxt, restarts, drops, lost>>

\* Node t installs the ballot that a payload hands to it, or refuses it.
\* The decided slots of the payload are learned as records at its ballot.
Install(t) ==
  \E m \in Delivered :
    /\ m.type = "rel" /\ (Named => m.to = t)
    /\ PromiseGuard => m.bal >= prom[t]
    /\ ~(role[t] = "leader" /\ bal[t] = m.bal)
    /\ FrontierGuard => m.nxt >= nxt[t]
    /\ m.nxt >= FirstUnlearned(t)
    /\ LET L == [s \in Slots |-> IF learned[t][s] = None THEN m.dec[s] ELSE learned[t][s]]
           cursor == Max({m.lo, FirstNone(L)})
           R == [s \in Slots |->
                   IF Pending = "keep" /\ s >= cursor /\ L[s] = None THEN m.pen[s] ELSE None]
           D == {s \in Slots : learned[t][s] = None /\ m.dec[s] # None}
           rec == LearnRecs(t, [s \in D |-> [bal |-> m.bal, val |-> m.dec[s]]])
           base == [s \in Slots |-> IF s \in D THEN rec.v[s] ELSE vote[t][s]]
       IN /\ learned' = [learned EXCEPT ![t] = L]
          /\ pend' = [pend EXCEPT ![t] = R]
          /\ vote' = [vote EXCEPT ![t] =
                        [s \in Slots |-> IF s \in Recorded(t, R) THEN [bal |-> m.bal, val |-> R[s]]
                                         ELSE base[s]]]
          /\ SendAll(RoundMsgs(t, m.bal, R))
          /\ prom' = [prom EXCEPT ![t] = IF rec.p > m.bal THEN rec.p ELSE m.bal]
    /\ role' = [role EXCEPT ![t] = "leader"]
    /\ bal' = [bal EXCEPT ![t] = m.bal]
    /\ origin' = [origin EXCEPT ![t] = "handoff"]
    /\ nxt' = [nxt EXCEPT ![t] = m.nxt]
    /\ UNCHANGED <<phase, restarts, drops, lost>>

\* A handoff leader whose learned prefix does not reach its frontier
\* resigns (`tick_handoff_fence`).
FenceResign(n) ==
  /\ Resign
  /\ role[n] = "leader"
  /\ origin[n] = "handoff"
  /\ \E s \in Slots : s < nxt[n] /\ learned[n][s] = None
  /\ role' = [role EXCEPT ![n] = "follower"]
  /\ origin' = [origin EXCEPT ![n] = "elected"]
  /\ pend' = [pend EXCEPT ![n] = NoRounds]
  /\ UNCHANGED <<prom, vote, learned, phase, bal, nxt, restarts, drops, lost, msgs>>

-----------------------------------------------------------------------------
(* Beats *)

Beat(n) ==
  /\ Effective(n)
  /\ Send([type |-> "beat", bal |-> bal[n], from |-> n])
  /\ UNCHANGED <<prom, vote, learned, phase, role, bal, origin, nxt, pend, restarts, drops, lost>>

\* A beat at or above the promise of leader n makes n a follower, except,
\* with BeatGuard, a beat at the ballot that n holds.
OnBeat(n) ==
  /\ role[n] = "leader"
  /\ \E m \in Delivered :
       /\ m.type = "beat" /\ m.from # n
       /\ m.bal >= prom[n]
       /\ ~(BeatGuard /\ m.bal = bal[n])
  /\ role' = [role EXCEPT ![n] = "follower"]
  /\ origin' = [origin EXCEPT ![n] = "elected"]
  /\ pend' = [pend EXCEPT ![n] = NoRounds]
  /\ UNCHANGED <<prom, vote, learned, phase, bal, nxt, restarts, drops, lost, msgs>>

-----------------------------------------------------------------------------
(* Faults *)

\* Node n restarts: it keeps its promise, its votes and what it learned. It
\* boots a follower, its campaign dies, and its frontier comes from disk.
Restart(n) ==
  /\ n \notin Steady
  /\ restarts < Restarts
  /\ restarts' = restarts + 1
  /\ role' = [role EXCEPT ![n] = "follower"]
  /\ origin' = [origin EXCEPT ![n] = "elected"]
  /\ pend' = [pend EXCEPT ![n] = NoRounds]
  /\ nxt' = [nxt EXCEPT ![n] = BootFrontier(n)]
  /\ phase' = [b \in Ballots |-> IF Owner[b] = n /\ phase[b] = "p1" THEN "dead" ELSE phase[b]]
  /\ UNCHANGED <<prom, vote, learned, bal, drops, lost, msgs>>

\* A message is lost.
Drop ==
  /\ drops < Drops
  /\ \E m \in Delivered :
       /\ m.type \in DropKinds
       /\ lost' = lost \cup {m}
  /\ drops' = drops + 1
  /\ UNCHANGED <<prom, vote, learned, phase, role, bal, origin, nxt, pend, restarts, msgs>>

Next ==
  \/ \E b \in Ballots : Prepare(b) \/ Win(b)
  \/ \E a \in Acceptors, b \in Ballots : Promise(a, b)
  \/ \E n \in Acceptors, c \in Commands : Allocate(n, c) \/ AllocateStale(n, c)
  \/ \E n \in Acceptors, s \in Slots : Resend(n, s) \/ Decide(n, s)
  \/ \E a \in Acceptors, b \in Ballots, s \in Slots : Accept(a, b, s)
  \/ \E n, p \in Acceptors : CatchUp(n, p) \/ Relinquish(n, p)
  \/ \E n \in Acceptors : Install(n) \/ FenceResign(n) \/ Beat(n) \/ OnBeat(n) \/ Restart(n)
  \/ Drop

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Liveness *)

\* The steps that a correct run takes in the end. A handoff, a fresh
\* command, a beat, a restart and a loss may happen, but need not.
Fairness ==
  /\ \A b \in Ballots : WF_vars(Prepare(b)) /\ WF_vars(Win(b))
  /\ \A a \in Acceptors, b \in Ballots : WF_vars(Promise(a, b))
  /\ \A a \in Acceptors, b \in Ballots, s \in Slots : WF_vars(Accept(a, b, s))
  /\ \A n \in Acceptors, s \in Slots : WF_vars(Decide(n, s)) /\ WF_vars(Resend(n, s))
  /\ \A n, p \in Acceptors : WF_vars(CatchUp(n, p))
  /\ \A n \in Acceptors : WF_vars(Install(n))

LiveSpec == Spec /\ Fairness

\* A leader can exercise its ballot and learned every slot below its
\* frontier: no open round stays open.
Settled == \E n \in Acceptors :
             /\ Effective(n)
             /\ \A s \in Slots : s < nxt[n] => learned[n][s] # None

\* A handoff ends, or an election replaces a successor that is gone.
Ends == <>[]Settled

-----------------------------------------------------------------------------
(* Safety *)

ChosenAt(b, s, v) ==
  \E Q \in Q2[s] : \A a \in Q : [type |-> "2b", acc |-> a, bal |-> b, slot |-> s, val |-> v] \in msgs
Chosen(s, v) == \E b \in Ballots : ChosenAt(b, s, v)

\* One command for each slot: no chosen value is lost across a handoff.
Agreement == \A s \in Slots, v, w \in Values : Chosen(s, v) /\ Chosen(s, w) => v = w

LearnedChosen == \A n \in Acceptors, s \in Slots : learned[n][s] # None => Chosen(s, learned[n][s])

\* One proposal for each slot and ballot, across the handoff.
OneProposal ==
  \A m, n \in msgs : m.type = "2a" /\ n.type = "2a" /\ m.bal = n.bal /\ m.slot = n.slot => m = n

\* The frontier of a leader that can exercise its ballot covers its first
\* unlearned slot (`assert_leader_invariants`).
LeaderFrontier == \A n \in Acceptors : Effective(n) => FirstUnlearned(n) <= nxt[n]

\* Every open round of a leader that can exercise its ballot is recorded
\* in its own log (`assert_leader_invariants`).
OwnRecorded ==
  \A n \in Acceptors, s \in Slots :
    Effective(n) /\ pend[n][s] # None /\ RecordAll /\ DurableFrontier =>
      vote[n][s] = [bal |-> bal[n], val |-> pend[n][s]]

\* A learned slot holds its record (`node/learn.rs`).
LearnRecordsHeld ==
  \A n \in Acceptors, s \in Slots : learned[n][s] # None => vote[n][s].val = learned[n][s]

\* One node at a time leads at a ballot.
OneHolder ==
  \A n, p \in Acceptors : n # p /\ role[n] = "leader" /\ role[p] = "leader" => bal[n] # bal[p]

\* A reachability control: a handoff leader learns a slot that it
\* proposed. It must be violated.
NoHandoffLearns ==
  ~\E n \in Acceptors, s \in Slots :
     /\ origin[n] = "handoff" /\ role[n] = "leader"
     /\ learned[n][s] # None
     /\ \E m \in msgs : m.type = "rel" /\ m.to = n /\ m.pen[s] # None
=============================================================================
