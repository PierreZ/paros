------------------------------ MODULE Truncation ------------------------------
(***************************************************************************)
(* Truncation in plain Multi-Paxos as `paros-consensus` builds it: the     *)
(* compaction floor of each acceptor, the decided `Truncate`, the trim-    *)
(* point jump (`TrimmedTo`) and the election gap fill. #1017 (truncation   *)
(* model). The log, Phase 1, Phase 2, commit and catch-up are the ones of  *)
(* `multi-paxos/`, on a majority, with no wipe.                            *)
(*                                                                         *)
(* A message is never lost from `msgs`, so a message can arrive late, two  *)
(* times, or never. Each ballot has one owner.                             *)
(*                                                                         *)
(* What the model adds, and the code it follows:                           *)
(*   - Each acceptor has a floor: the first slot it still holds. It holds  *)
(*     no vote and no learned value below it (`Acceptor::first_slot`,      *)
(*     `acceptor/retention.rs`). The floor is durable.                     *)
(*   - `Truncate` is a value in a slot. A node compacts when it learned    *)
(*     the slot and every slot below it: its floor goes one past the slot  *)
(*     (`ColocatedNode::compact`, clamped to the chosen prefix by          *)
(*     `Replica::compaction_target`). The floors of the nodes differ for   *)
(*     some time.                                                          *)
(*   - Phase 1 asks from the candidate's first unchosen slot. An acceptor  *)
(*     whose floor is above that slot does not promise (the floor guard,   *)
(*     `PrepareOutcome::BelowFloor` in `node/acceptor.rs`): its promise    *)
(*     could not report the slots it compacted.                            *)
(*   - An `Accept` below the floor is ignored (`AcceptOutcome::BelowFloor`).*)
(*   - Catch-up from a peer (`node/catch_up.rs`): a peer replays what it   *)
(*     learned at or above its floor; a node that asks from below the      *)
(*     floor of the peer gets `TrimmedTo` and jumps to that floor. The     *)
(*     jump carries no ballot: the promise never moves.                    *)
(*   - The vote watermark (`Acceptor::vote_watermark`): the last vote, or  *)
(*     the floor minus 1 when the acceptor holds no vote above its floor.  *)
(*                                                                         *)
(* Constants (each rule has a control):                                    *)
(*   FloorGuard  "nack": an acceptor below whose floor the campaign asks   *)
(*               does not promise. "ignore": a control, it promises and    *)
(*               reports only what it still holds.                         *)
(*   TrimPromise "keep": the trim jump keeps the promise. "copy": a        *)
(*               control, the jump takes the promise of the peer.          *)
(*   CompactWhen "prefix": a node compacts once it learned every slot up   *)
(*               to the `Truncate`. "learned": a control, it compacts once *)
(*               it learned the `Truncate` alone.                          *)
(*   Watermark   "floor": the watermark counts the floor. "votes": a       *)
(*               control, it counts the retained votes only.               *)
(*   Restarts    how many restarts may happen (in the whole run).          *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS Acceptors, Learners, Commands, Truncate, Noop, NB, NS, Q1, Q2, Owner,
          FloorGuard, TrimPromise, CompactWhen, Watermark, Restarts

Ballots == 1..NB
Slots == 0..(NS - 1)
Proposals == Commands \cup {Truncate}
Values == Proposals \cup {Noop}
None == "none"

ASSUME FloorGuard \in {"nack", "ignore"}
ASSUME TrimPromise \in {"keep", "copy"}
ASSUME CompactWhen \in {"prefix", "learned"}
ASSUME Watermark \in {"floor", "votes"}
ASSUME Learners \subseteq Acceptors
ASSUME Noop \notin Proposals /\ Truncate \notin Commands
ASSUME Owner \in [Ballots -> Acceptors]
ASSUME Q2 \in [Slots -> SUBSET SUBSET Acceptors]
Vote == [bal : 0..NB, val : Values \cup {None}]
NoVote == [bal |-> 0, val |-> None]

VARIABLES prom,     \* acceptor: the promise in memory
          vote,     \* acceptor: the vote, per slot (on disk before it answers)
          dprom,    \* acceptor: the promise on disk
          floor,    \* acceptor: the first slot it holds (durable)
          learned,  \* node: the value it learned, per slot (durable)
          phase,    \* ballot: "idle", "p1", "p2" or "dead"
          next,     \* ballot: the allocator's next slot, once in Phase 2
          restarts, \* how many restarts happened
          msgs      \* every message sent: the ghost of every vote

vars == <<prom, vote, dprom, floor, learned, phase, next, restarts, msgs>>

Msg ==
  [type : {"1a"}, bal : Ballots, from : 0..NS]
    \cup [type : {"1b"}, acc : Acceptors, bal : Ballots, from : 0..NS, rep : [Slots -> Vote]]
    \cup [type : {"2a"}, bal : Ballots, slot : Slots, val : Values]
    \cup [type : {"2b"}, acc : Acceptors, bal : Ballots, slot : Slots, val : Values]

TypeOK ==
  /\ prom \in [Acceptors -> 0..NB]
  /\ dprom \in [Acceptors -> 0..NB]
  /\ vote \in [Acceptors -> [Slots -> Vote]]
  /\ floor \in [Acceptors -> 0..NS]
  /\ learned \in [Acceptors -> [Slots -> Values \cup {None}]]
  /\ phase \in [Ballots -> {"idle", "p1", "p2", "dead"}]
  /\ next \in [Ballots -> 0..NS]
  /\ restarts \in Nat
  /\ msgs \subseteq Msg

Init ==
  /\ prom = [a \in Acceptors |-> 0]
  /\ dprom = [a \in Acceptors |-> 0]
  /\ vote = [a \in Acceptors |-> [s \in Slots |-> NoVote]]
  /\ floor = [a \in Acceptors |-> 0]
  /\ learned = [a \in Acceptors |-> [s \in Slots |-> None]]
  /\ phase = [b \in Ballots |-> "idle"]
  /\ next = [b \in Ballots |-> 0]
  /\ restarts = 0
  /\ msgs = {}

Send(m) == msgs' = msgs \cup {m}
SendAll(M) == msgs' = msgs \cup M

\* Node n knows slot s: it learned the value, or the slot is below its floor.
Holds(n, s) == s < floor[n] \/ learned[n][s] # None

\* The first slot node n does not hold: where its campaign and its catch-up
\* start (`first_unchosen`). Never below the floor.
FirstUnchosen(n) ==
  IF \A s \in Slots : Holds(n, s) THEN NS
  ELSE CHOOSE s \in Slots : ~Holds(n, s) /\ \A t \in Slots : t < s => Holds(n, t)

\* Drop every vote and every learned value below the floor f.
DropVotes(a, f) == [s \in Slots |-> IF s < f THEN NoVote ELSE vote[a][s]]
DropLearned(a, f) == [s \in Slots |-> IF s < f THEN None ELSE learned[a][s]]

-----------------------------------------------------------------------------
(* Phase 1 *)

\* The promise of acceptor a at ballot b, for a campaign from `from`: it
\* reports each vote at or above `from` that it still holds.
PromiseMsg(a, b, from) ==
  [type |-> "1b", acc |-> a, bal |-> b, from |-> from,
   rep |-> [s \in Slots |-> IF s >= from THEN vote[a][s] ELSE NoVote]]

\* The floor guard: may acceptor a promise a campaign from `from`?
MayPromise(a, from) == FloorGuard = "ignore" \/ from >= floor[a]

\* The owner of ballot b opens a campaign from its first unchosen slot. It
\* is its own first acceptor. Its own floor is never above that slot.
Prepare(b) ==
  LET o == Owner[b]
      from == FirstUnchosen(o)
  IN /\ phase[b] = "idle"
     /\ prom[o] < b
     /\ phase' = [phase EXCEPT ![b] = "p1"]
     /\ prom' = [prom EXCEPT ![o] = b]
     /\ dprom' = [dprom EXCEPT ![o] = b]
     /\ SendAll({[type |-> "1a", bal |-> b, from |-> from], PromiseMsg(o, b, from)})
     /\ UNCHANGED <<vote, floor, learned, next, restarts>>

\* Acceptor a promises ballot b, unless the floor guard refuses (a `Nack`
\* that moves nothing; the model sends nothing).
Promise(a, b) ==
  /\ prom[a] < b
  /\ \E m \in msgs :
       /\ m.type = "1a" /\ m.bal = b
       /\ MayPromise(a, m.from)
       /\ prom' = [prom EXCEPT ![a] = b]
       /\ dprom' = [dprom EXCEPT ![a] = b]
       /\ Send(PromiseMsg(a, b, m.from))
  /\ UNCHANGED <<vote, floor, learned, phase, next, restarts>>

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

\* The recovery from `from` to the frontier f: the highest reported vote
\* again, and a `Noop` for each slot with no report (the gap fill).
Recovery(S, b, from, f) ==
  {[type |-> "2a", bal |-> b, slot |-> s,
    val |-> IF Top(S, s).bal > 0 THEN Top(S, s).val ELSE Noop] : s \in from..(f - 1)}

\* The owner of ballot b wins Phase 1 on the promises S, one for each
\* acceptor, its own among them, and a Phase-1 quorum.
Win(b) ==
  /\ phase[b] = "p1"
  /\ \E S \in SUBSET {m \in msgs : m.type = "1b" /\ m.bal = b} :
       /\ \A m, n \in S : m.acc = n.acc => m = n
       /\ Owner[b] \in {m.acc : m \in S}
       /\ \E Q \in Q1 : Q \subseteq {m.acc : m \in S}
       /\ LET from == (CHOOSE m \in msgs : m.type = "1a" /\ m.bal = b).from
              f == Frontier(S, from)
          IN /\ next' = [next EXCEPT ![b] = f]
             /\ SendAll(Recovery(S, b, from, f))
  /\ phase' = [phase EXCEPT ![b] = "p2"]
  /\ UNCHANGED <<prom, vote, dprom, floor, learned, restarts>>

-----------------------------------------------------------------------------
(* Phase 2 *)

\* The leader of ballot b allocates the next slot to a fresh command or to
\* a `Truncate`.
Allocate(b, c) ==
  /\ phase[b] = "p2"
  /\ next[b] < NS
  /\ next' = [next EXCEPT ![b] = @ + 1]
  /\ Send([type |-> "2a", bal |-> b, slot |-> next[b], val |-> c])
  /\ UNCHANGED <<prom, vote, dprom, floor, learned, phase, restarts>>

\* Acceptor a votes for a value of ballot b at slot s. A slot below its
\* floor is chosen already: it ignores the `Accept`.
Accept(a, b, s) ==
  /\ prom[a] <= b
  /\ s >= floor[a]
  /\ \E m \in msgs :
       /\ m.type = "2a" /\ m.bal = b /\ m.slot = s
       /\ vote[a][s] # [bal |-> b, val |-> m.val]
       /\ prom' = [prom EXCEPT ![a] = b]
       /\ dprom' = [dprom EXCEPT ![a] = b]
       /\ vote' = [vote EXCEPT ![a][s] = [bal |-> b, val |-> m.val]]
       /\ Send([type |-> "2b", acc |-> a, bal |-> b, slot |-> s, val |-> m.val])
  /\ UNCHANGED <<floor, learned, phase, next, restarts>>

-----------------------------------------------------------------------------
(* Commit, catch-up, compaction and the trim jump *)

\* The leader of ballot b learns slot s at a Phase-2 quorum. A slot below
\* its floor is not learned again (`mark_chosen`).
Decide(b, s) ==
  LET o == Owner[b]
  IN /\ phase[b] = "p2"
     /\ ~Holds(o, s)
     /\ \E v \in Values, Q \in Q2[s] :
          /\ \A a \in Q : [type |-> "2b", acc |-> a, bal |-> b, slot |-> s, val |-> v] \in msgs
          /\ learned' = [learned EXCEPT ![o][s] = v]
     /\ UNCHANGED <<prom, vote, dprom, floor, phase, next, restarts, msgs>>

\* Node n catches up from peer p, asking from its first unchosen slot. The
\* peer is at or below that slot, so it replays what it learned (the model
\* reads the learned values; a learned value is stable).
CatchUp(n, p) ==
  /\ n \in Learners
  /\ FirstUnchosen(n) >= floor[p]
  /\ \E s \in Slots : ~Holds(n, s) /\ learned[p][s] # None
  /\ learned' = [learned EXCEPT ![n] = [s \in Slots |->
                   IF ~Holds(n, s) THEN learned[p][s] ELSE @[s]]]
  /\ UNCHANGED <<prom, vote, dprom, floor, phase, next, restarts, msgs>>

\* Node n asks peer p from below the floor of p. The peer answers
\* `TrimmedTo`: n jumps to the floor of p. The jump carries no ballot.
TrimTo(n, p) ==
  /\ n \in Learners
  /\ FirstUnchosen(n) < floor[p]
  /\ floor' = [floor EXCEPT ![n] = floor[p]]
  /\ vote' = [vote EXCEPT ![n] = DropVotes(n, floor[p])]
  /\ learned' = [learned EXCEPT ![n] = DropLearned(n, floor[p])]
  /\ IF TrimPromise = "keep"
       THEN UNCHANGED <<prom, dprom>>
       ELSE /\ prom' = [prom EXCEPT ![n] = prom[p]]
            /\ dprom' = [dprom EXCEPT ![n] = prom[p]]
  /\ UNCHANGED <<phase, next, restarts, msgs>>

\* Node a applied the `Truncate` at slot t: it compacts every slot up to t.
Compact(a, t) ==
  /\ learned[a][t] = Truncate
  /\ floor[a] <= t
  /\ CompactWhen = "prefix" => \A s \in 0..t : Holds(a, s)
  /\ floor' = [floor EXCEPT ![a] = t + 1]
  /\ vote' = [vote EXCEPT ![a] = DropVotes(a, t + 1)]
  /\ learned' = [learned EXCEPT ![a] = DropLearned(a, t + 1)]
  /\ UNCHANGED <<prom, dprom, phase, next, restarts, msgs>>

\* Node a restarts: the promise goes back to the disk, and its ballots die.
\* A vote, the floor and what it learned are durable.
Restart(a) ==
  /\ restarts < Restarts
  /\ restarts' = restarts + 1
  /\ prom' = [prom EXCEPT ![a] = dprom[a]]
  /\ phase' = [b \in Ballots |-> IF Owner[b] = a /\ phase[b] \in {"p1", "p2"} THEN "dead" ELSE phase[b]]
  /\ UNCHANGED <<vote, dprom, floor, learned, next, msgs>>

Next ==
  \/ \E b \in Ballots : Prepare(b) \/ Win(b)
  \/ \E b \in Ballots, c \in Proposals : Allocate(b, c)
  \/ \E a \in Acceptors, b \in Ballots : Promise(a, b)
  \/ \E a \in Acceptors, b \in Ballots, s \in Slots : Accept(a, b, s)
  \/ \E b \in Ballots, s \in Slots : Decide(b, s)
  \/ \E n, p \in Acceptors : CatchUp(n, p) \/ TrimTo(n, p)
  \/ \E a \in Acceptors, t \in Slots : Compact(a, t)
  \/ \E a \in Acceptors : Restart(a)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Safety *)

\* A value is chosen at slot s once a Phase-2 quorum of s voted for it at
\* one ballot. A vote is a 2b message, so a compaction does not unchoose it:
\* `msgs` is the ghost that keeps what was chosen.
ChosenAt(b, s, v) ==
  \E Q \in Q2[s] : \A a \in Q : [type |-> "2b", acc |-> a, bal |-> b, slot |-> s, val |-> v] \in msgs
Chosen(s, v) == \E b \in Ballots : ChosenAt(b, s, v)
IsChosen(s) == \E v \in Values : Chosen(s, v)

\* One value for each slot, the compacted slots included.
Agreement == \A s \in Slots, v, w \in Values : Chosen(s, v) /\ Chosen(s, w) => v = w

\* What a node learned is what was chosen.
LearnedChosen == \A n \in Acceptors, s \in Slots : learned[n][s] # None => Chosen(s, learned[n][s])

\* No fill of a chosen slot: a leader never proposes a `Noop` at a slot
\* where a lower ballot chose another value.
NoFillChosen ==
  \A m \in msgs :
    (m.type = "2a" /\ m.val = Noop) =>
      ~\E b \in 1..(m.bal - 1), v \in Proposals : ChosenAt(b, m.slot, v)

\* A floor is chosen: each slot below the floor of an acceptor is chosen.
FloorChosen == \A a \in Acceptors, s \in Slots : s < floor[a] => IsChosen(s)

\* A floor is truncated: each slot below the floor of an acceptor is at or
\* below a chosen `Truncate`. The slots below `first_seq` are the only ones
\* that a node may forget.
FloorTruncated ==
  \A a \in Acceptors, s \in Slots :
    s < floor[a] => \E t \in Slots : t >= s /\ Chosen(t, Truncate)

\* Recoverable: for each chosen slot, each Phase-1 quorum has a member that
\* holds a vote there, or a member whose floor is above the slot (and that
\* member refuses a campaign that asks from below it).
Recoverable ==
  \A s \in Slots, Q \in Q1 :
    IsChosen(s) => \E a \in Q : vote[a][s].bal > 0 \/ floor[a] > s

\* The vote watermark of acceptor a: its last vote, or its floor minus 1
\* (Watermark = "floor"). -1 is a log that never voted (`None`).
VoteWatermark(a) ==
  LET held == {s \in Slots : vote[a][s].bal > 0}
      top == IF held = {} THEN -1 ELSE CHOOSE s \in held : \A t \in held : t <= s
  IN IF Watermark = "floor" /\ floor[a] - 1 > top THEN floor[a] - 1 ELSE top

\* The read index covers the floor: the vote watermark of an acceptor is
\* never below a slot that it voted.
WatermarkCovers ==
  \A m \in msgs : m.type = "2b" => VoteWatermark(m.acc) >= m.slot

\* Reachability controls: each must be violated.
NoCompaction == \A a \in Acceptors : floor[a] = 0
NoSplitFloor == ~\E a, b \in Acceptors : floor[a] > 0 /\ floor[b] = 0 /\ \E s \in Slots : s >= floor[a] /\ IsChosen(s)
\* A node jumped: its floor covers a slot that it never voted.
NoJump == ~\E a \in Acceptors, s \in Slots :
            s < floor[a] /\ ~\E m \in msgs : m.type = "2b" /\ m.acc = a /\ m.slot = s
=============================================================================
