------------------------------ MODULE Truncation ------------------------------
(***************************************************************************)
(* Truncation in plain Multi-Paxos as `paros-consensus` builds it: the     *)
(* compaction floor of each acceptor, the decided `Truncate`, the trim-    *)
(* point jump (`TrimmedTo`) and the election gap fill (#1017 (truncation   *)
(* model)); the faulty entries of CTRL with the repair probe, the retry of *)
(* a candidate that a floor refused, and the floor as a fold of the        *)
(* journal (#1216 (truncation model, part 2)). The log, Phase 1, Phase 2,  *)
(* commit and catch-up are the ones of `multi-paxos/`, on a majority, with *)
(* no wipe.                                                                *)
(*                                                                         *)
(* A message is never lost from `msgs`, so a message can arrive late, two  *)
(* times, or never. Each ballot has one owner.                             *)
(*                                                                         *)
(* What the model adds, and the code it follows:                           *)
(*   - Each acceptor has a floor: the first slot it still holds. It holds  *)
(*     no vote and no learned value below it (`Acceptor::first_slot`,      *)
(*     `acceptor/retention.rs`). The floor is durable.                     *)
(*   - A truncation is a value in a slot: each value of `Cuts` releases   *)
(*     the records before its position `CutAt` (`Control::Truncate         *)
(*     { up_to }`). A position of `NS` or more releases every record before *)
(*     the slot. The fold raises                                           *)
(*     `first_seq` to the position, clamped to `next_seq`, and never       *)
(*     lowers it (`JournalState::apply`). A node compacts when it learned  *)
(*     the slot and every slot below it: its floor goes to the slot that   *)
(*     holds the record at `first_seq`, or one past the slot when no record*)
(*     is kept (`Replica::compaction_target`, `ColocatedNode::compact`).   *)
(*     The floors of the nodes differ for some time.                       *)
(*   - Phase 1 asks from the candidate's first unchosen slot. An acceptor  *)
(*     whose floor is above that slot does not promise (the floor guard,   *)
(*     `PrepareOutcome::BelowFloor` in `node/acceptor.rs`): its promise    *)
(*     could not report the slots it compacted.                            *)
(*   - A candidate that a floor refused campaigns again from its new first *)
(*     unchosen slot once a jump or a catch-up moved it (`on_nack`, then   *)
(*     the next campaign). The model keeps the ballot and sends a new `1a` *)
(*     from the new slot; the code opens the next round. An acceptor that  *)
(*     promised the old campaign promises the new one too.                 *)
(*   - An `Accept` below the floor is ignored (`AcceptOutcome::BelowFloor`).*)
(*   - Catch-up from a peer (`node/catch_up.rs`): a peer replays what it   *)
(*     learned at or above its floor; a node that asks from below the      *)
(*     floor of the peer gets `TrimmedTo` and jumps to that floor. The     *)
(*     jump carries no ballot: the promise never moves.                    *)
(*   - The vote watermark (`Acceptor::vote_watermark`): the last vote, or  *)
(*     the floor minus 1 when the acceptor holds no vote above its floor.  *)
(*   - A faulty entry (CTRL, `Acceptor::faulty`): a boot finds the record  *)
(*     of a slot damaged, with its ballot known and its value lost (`Lost`)*)
(*     (`read_back_log` in `node/boot.rs`). A scrub that reads the damage  *)
(*     first writes the record again from memory (`repair_record`): no     *)
(*     state of the model moves, so the model checks the damage that the   *)
(*     scrub did not reach. The boot drops the learned value of the slot:  *)
(*     the chosen map is rebuilt from the records. A later `Accept` over   *)
(*     the slot heals it (`record_accepted`).                              *)
(*   - The tri-state tally of Phase 1 (`slot_decidable` in `proposer.rs`): *)
(*     at a slot with a faulty report, the threshold is the highest        *)
(*     readable report. An answer qualifies when it is not a faulty report *)
(*     above the threshold. The slot is decided only when the qualifying   *)
(*     answers hold a Phase-1 quorum; else it is blocked, and the repair   *)
(*     probe (`proposer/probe.rs`) decides it later on more promises of    *)
(*     the same ballot, or drops it once the leader learned it.            *)
(*                                                                         *)
(* Constants (each rule has a control):                                    *)
(*   FloorGuard  "nack": an acceptor below whose floor the campaign asks   *)
(*               does not promise. "ignore": a control, it promises and    *)
(*               reports only what it still holds.                         *)
(*   TrimPromise "keep": the trim jump keeps the promise. "copy": a        *)
(*               control, the jump takes the promise of the peer.          *)
(*   CompactWhen "prefix": a node compacts once it learned every slot up   *)
(*               to the truncation. "learned": a control, it compacts once *)
(*               it learned the truncation alone.                          *)
(*   FloorRule   "fold": the floor is the slot of the record at            *)
(*               `first_seq`. "slot": a control, the floor goes one past   *)
(*               the slot of the truncation, whatever its position.        *)
(*   Watermark   "floor": the watermark counts the floor. "votes": a       *)
(*               control, it counts the retained votes only.               *)
(*   Tally       "ctrl": the tri-state tally. "none": a control, a faulty  *)
(*               report counts as no vote. "have": a control, a faulty     *)
(*               report qualifies as soon as one readable report exists    *)
(*               (CTRL's Case 1 read with no ballot threshold). The two    *)
(*               differ only when a faulty report is above a readable one: *)
(*               three ballots. "plus_one": a control, the threshold is    *)
(*               one above the highest readable report (#1263 (CTRL        *)
(*               recovery in the journal), item 5).                        *)
(*   TrimPoint   "floor": a `TrimmedTo` carries the floor of the peer, or  *)
(*               an older floor of it (a late or pushed answer). "learned":*)
(*               a control, the jump goes past any slot the peer holds.    *)
(*   FloorWrite  "atomic": the store drops the records below a new floor   *)
(*               and records the floor in one write (`WriteOp::Truncate`). *)
(*               "late": a control, the floor reaches the disk after the   *)
(*               records are gone, and a restart can read the old floor.   *)
(*   Retry       "jump": a refused candidate asks again from its new first *)
(*               unchosen slot. "none": a control, it never asks again.    *)
(*   Restarts    how many restarts may happen (in the whole run).          *)
(*   Faults      how many faulty entries a boot may find (in the whole     *)
(*               run), on the nodes `RotNodes` only.                       *)
(*   Rolls       the `Roll` values of segments (`Control::Roll`): a        *)
(*               control value that weighs nothing, like a `Noop`. Only    *)
(*               `held-floor/` (`HeldFloor.tla`, R7 of                     *)
(*               `docs/architecture/04-7-segments.md`) gives them a        *)
(*               meaning; every check of this folder has none.             *)
(*   HoldCmds    the `Hold` values of segments: a decided command that     *)
(*               weighs nothing, like a `Noop`. Only `held-floor/` gives   *)
(*               them a meaning (the floor at a held segment, decided);    *)
(*               every check of this folder has none.                      *)
(*   Retries     the retries of a write: a write at a position that the    *)
(*               log already holds. It takes no position. Only             *)
(*               `held-floor/` gives them a meaning (the record that the   *)
(*               fold of a retry reads); every check of this folder has    *)
(*               none.                                                     *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS Acceptors, Learners, Commands, Cuts, CutAt, Noop, Lost,
          NB, NS, Q1, Q2, Owner,
          FloorGuard, TrimPromise, CompactWhen, FloorRule, Watermark, Tally, Retry,
          TrimPoint, FloorWrite, Restarts, Faults, RotNodes, Rolls, HoldCmds,
          Retries

Ballots == 1..NB
Slots == 0..(NS - 1)
Truncations == Cuts
Proposals == Commands \cup Truncations \cup Rolls \cup HoldCmds \cup Retries
Values == Proposals \cup {Noop}
None == "none"

ASSUME FloorGuard \in {"nack", "ignore"}
ASSUME TrimPromise \in {"keep", "copy"}
ASSUME CompactWhen \in {"prefix", "learned"}
ASSUME FloorRule \in {"fold", "slot"}
ASSUME Watermark \in {"floor", "votes"}
ASSUME Tally \in {"ctrl", "none", "have", "plus_one"}
ASSUME TrimPoint \in {"floor", "learned"}
ASSUME FloorWrite \in {"atomic", "late"}
ASSUME Retry \in {"jump", "none"}
ASSUME Learners \subseteq Acceptors /\ RotNodes \subseteq Acceptors
ASSUME Noop \notin Proposals /\ Lost \notin Values
ASSUME (Cuts # {} \/ Rolls # {}) /\ Cuts \cap Commands = {}
ASSUME Rolls \cap (Commands \cup Cuts) = {}
ASSUME HoldCmds \cap (Commands \cup Cuts \cup Rolls) = {}
ASSUME Retries \cap (Commands \cup Cuts \cup Rolls \cup HoldCmds) = {}
ASSUME CutAt \in [Cuts -> Nat]
ASSUME CompactWhen = "learned" => FloorRule = "slot"
ASSUME Owner \in [Ballots -> Acceptors]
ASSUME Q2 \in [Slots -> SUBSET SUBSET Acceptors]
Vote == [bal : 0..NB, val : Values \cup {None, Lost}]
NoVote == [bal |-> 0, val |-> None]

VARIABLES prom,     \* acceptor: the promise (on disk before it answers)
          vote,     \* acceptor: the vote, per slot (on disk before it answers)
          floor,    \* acceptor: the first slot it holds
          dfloor,   \* acceptor: the floor on disk
          learned,  \* node: the value it learned, per slot
          phase,    \* ballot: "idle", "p1", "p2" or "dead"
          from,     \* ballot: the slot its campaign asks from
          next,     \* ballot: the allocator's next slot, once in Phase 2
          blocked,  \* ballot: the slots its repair probe still decides
          restarts, \* how many restarts happened
          faults,   \* how many faulty entries a boot found
          msgs      \* every message sent: the ghost of every vote

vars == <<prom, vote, floor, dfloor, learned, phase, from, next, blocked, restarts, faults, msgs>>

Msg ==
  [type : {"1a"}, bal : Ballots, from : 0..NS]
    \cup [type : {"1b"}, acc : Acceptors, bal : Ballots, from : 0..NS, rep : [Slots -> Vote]]
    \cup [type : {"2a"}, bal : Ballots, slot : Slots, val : Values]
    \cup [type : {"2b"}, acc : Acceptors, bal : Ballots, slot : Slots, val : Values]

TypeOK ==
  /\ prom \in [Acceptors -> 0..NB]
  /\ vote \in [Acceptors -> [Slots -> Vote]]
  /\ floor \in [Acceptors -> 0..NS]
  /\ dfloor \in [Acceptors -> 0..NS]
  /\ learned \in [Acceptors -> [Slots -> Values \cup {None}]]
  /\ phase \in [Ballots -> {"idle", "p1", "p2", "dead"}]
  /\ from \in [Ballots -> 0..NS]
  /\ next \in [Ballots -> 0..NS]
  /\ blocked \in [Ballots -> SUBSET Slots]
  /\ restarts \in Nat
  /\ faults \in Nat
  /\ msgs \subseteq Msg

Init ==
  /\ prom = [a \in Acceptors |-> 0]
  /\ vote = [a \in Acceptors |-> [s \in Slots |-> NoVote]]
  /\ floor = [a \in Acceptors |-> 0]
  /\ dfloor = [a \in Acceptors |-> 0]
  /\ learned = [a \in Acceptors |-> [s \in Slots |-> None]]
  /\ phase = [b \in Ballots |-> "idle"]
  /\ from = [b \in Ballots |-> 0]
  /\ next = [b \in Ballots |-> 0]
  /\ blocked = [b \in Ballots |-> {}]
  /\ restarts = 0
  /\ faults = 0
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

\* The ballots of node a die when it boots again.
KillBallots(a) ==
  [b \in Ballots |-> IF Owner[b] = a /\ phase[b] \in {"p1", "p2"} THEN "dead" ELSE phase[b]]

\* A vote that holds a value, and a faulty vote: ballot known, value lost.
Readable(v) == v.bal > 0 /\ v.val # Lost
Faulty(v) == v.bal > 0 /\ v.val = Lost

-----------------------------------------------------------------------------
(* The chosen log: a value is chosen at slot s once a Phase-2 quorum of s  *)
(* voted for it at one ballot. A vote is a 2b message, so a compaction or a *)
(* lost record does not unchoose it: `msgs` is the ghost that keeps what   *)
(* was chosen.                                                             *)

ChosenAt(b, s, v) ==
  \E Q \in Q2[s] : \A a \in Q : [type |-> "2b", acc |-> a, bal |-> b, slot |-> s, val |-> v] \in msgs
Chosen(s, v) == \E b \in Ballots : ChosenAt(b, s, v)
IsChosen(s) == \E v \in Values : Chosen(s, v)
ChosenVal(s) == CHOOSE v \in Values : Chosen(s, v)

-----------------------------------------------------------------------------
(* The journal fold over the chosen log. Each command is one record. The   *)
(* fold of a node is a function of the chosen log, so the model reads the  *)
(* chosen log in place of the node's fold.                                 *)

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : x <= y

\* `next_seq` before slot s: the records of the slots below s.
Pos(s) == Cardinality({t \in 0..(s - 1) : ChosenVal(t) \in Commands})

\* The position a truncation at slot s raises `first_seq` to: its `up_to`,
\* clamped to `next_seq`.
CutTo(s) == Min({CutAt[ChosenVal(s)], Pos(s)})

\* `first_seq` once the fold applied every slot up to t.
FirstSeq(t) ==
  LET cuts == {CutTo(s) : s \in {u \in 0..t : ChosenVal(u) \in Truncations}}
  IN IF cuts = {} THEN 0 ELSE Max(cuts)

\* The floor a compaction at the truncation of slot t moves to.
CompactTarget(t) ==
  IF FloorRule = "slot" THEN t + 1
  ELSE LET keep == {s \in 0..t : ChosenVal(s) \in Commands /\ Pos(s) >= FirstSeq(t)}
       IN IF keep = {} THEN t + 1 ELSE Min(keep)

-----------------------------------------------------------------------------
(* Phase 1 *)

\* The promise of acceptor a at ballot b, for a campaign from `f`: it
\* reports each vote at or above `f` that it still holds, faulty votes too.
PromiseMsg(a, b, f) ==
  [type |-> "1b", acc |-> a, bal |-> b, from |-> f,
   rep |-> [s \in Slots |-> IF s >= f THEN vote[a][s] ELSE NoVote]]

\* The floor guard: may acceptor a promise a campaign from `f`?
MayPromise(a, f) == FloorGuard = "ignore" \/ f >= floor[a]

\* The owner of ballot b opens a campaign from its first unchosen slot. It
\* is its own first acceptor. Its own floor is never above that slot.
Prepare(b) ==
  LET o == Owner[b]
      f == FirstUnchosen(o)
  IN /\ phase[b] = "idle"
     /\ prom[o] < b
     /\ phase' = [phase EXCEPT ![b] = "p1"]
     /\ from' = [from EXCEPT ![b] = f]
     /\ prom' = [prom EXCEPT ![o] = b]
     /\ SendAll({[type |-> "1a", bal |-> b, from |-> f], PromiseMsg(o, b, f)})
     /\ UNCHANGED <<vote, floor, dfloor, learned, next, blocked, restarts, faults>>

\* A jump or a catch-up moved the first unchosen slot of the owner of
\* ballot b during its Phase 1: the owner asks again from there. It does so
\* when a floor refused the campaign (an acceptor above whose floor it asks
\* has not promised it), but the model lets it ask again at any such time:
\* a `1a` from a later slot is safe whenever it is sent.
AskAgain(b) ==
  LET o == Owner[b]
      f == FirstUnchosen(o)
  IN /\ Retry = "jump"
     /\ phase[b] = "p1"
     /\ prom[o] = b
     /\ f > from[b]
     /\ from' = [from EXCEPT ![b] = f]
     /\ SendAll({[type |-> "1a", bal |-> b, from |-> f], PromiseMsg(o, b, f)})
     /\ UNCHANGED <<prom, vote, floor, dfloor, learned, phase, next, blocked, restarts, faults>>

\* Acceptor a promises ballot b, unless the floor guard refuses (a `Nack`
\* that moves nothing; the model sends nothing). A promise at its own ballot
\* answers too (`Acceptor::prepare` re-affirms it): the repair probe of a
\* leader with a blocked slot asks again an acceptor that took an `Accept`
\* of the ballot before its `Prepare` reached it.
Promise(a, b) ==
  /\ \/ prom[a] < b
     \/ /\ prom[a] = b
        /\ blocked[b] # {}
        /\ ~\E n \in msgs : n.type = "1b" /\ n.acc = a /\ n.bal = b
  /\ \E m \in msgs :
       /\ m.type = "1a" /\ m.bal = b
       /\ MayPromise(a, m.from)
       /\ prom' = [prom EXCEPT ![a] = b]
       /\ Send(PromiseMsg(a, b, m.from))
  /\ UNCHANGED <<vote, floor, dfloor, learned, phase, from, next, blocked, restarts, faults>>

\* The promises of ballot b that cover its campaign: one for each acceptor
\* at most. A promise from a lower slot reports more, never less.
PromiseSets(b) ==
  {S \in SUBSET {m \in msgs : m.type = "1b" /\ m.bal = b /\ m.from <= from[b]} :
     \A m, n \in S : m.acc = n.acc => m = n}

\* The highest readable vote that the promises S report at slot s.
Top(S, s) ==
  LET rep == {m \in S : Readable(m.rep[s])}
  IN IF rep = {} THEN NoVote
     ELSE (CHOOSE m \in rep : \A n \in rep : n.rep[s].bal <= m.rep[s].bal).rep[s]

\* The tri-state tally at slot s: may the promises S decide it? An answer
\* qualifies unless it is a faulty report above the highest readable one.
Decidable(S, s) ==
  \/ Tally = "none"
  \/ Tally = "have" /\ Top(S, s).bal > 0
  \/ ~\E m \in S : Faulty(m.rep[s])
  \/ LET thr == IF Tally = "plus_one" THEN Top(S, s).bal + 1 ELSE Top(S, s).bal
     IN \E Q \in Q1 : Q \subseteq {m.acc : m \in {n \in S : ~(Faulty(n.rep[s]) /\ n.rep[s].bal > thr)}}

\* The frontier: one past the highest slot that the promises S report,
\* faulty reports too, never below `f`.
Frontier(S, f) ==
  LET h == {s \in Slots : s >= f /\ \E m \in S : m.rep[s].bal > 0}
  IN IF h = {} THEN f ELSE Max(h) + 1

\* The value ballot b proposes again at slot s: the highest readable report,
\* or a `Noop` when there is none (the gap fill).
Refill(S, b, s) ==
  [type |-> "2a", bal |-> b, slot |-> s,
   val |-> IF Top(S, s).bal > 0 THEN Top(S, s).val ELSE Noop]

\* The owner of ballot b wins Phase 1 on the promises S of a Phase-1
\* quorum; its own promise need not be among them. It proposes again each slot that the tally
\* decides; the others go to the repair probe.
Win(b) ==
  /\ phase[b] = "p1"
  /\ \E S \in PromiseSets(b) :
       /\ \E Q \in Q1 : Q \subseteq {m.acc : m \in S}
       /\ LET f == Frontier(S, from[b])
              open == {s \in from[b]..(f - 1) : ~Decidable(S, s)}
          IN /\ next' = [next EXCEPT ![b] = f]
             /\ blocked' = [blocked EXCEPT ![b] = open]
             /\ SendAll({Refill(S, b, s) : s \in (from[b]..(f - 1)) \ open})
  /\ phase' = [phase EXCEPT ![b] = "p2"]
  /\ UNCHANGED <<prom, vote, floor, dfloor, learned, from, restarts, faults>>

\* The repair probe of ballot b settles a blocked slot s: more promises of
\* the same ballot arrived and decide it, or the leader learned it.
Probe(b, s) ==
  /\ phase[b] = "p2"
  /\ s \in blocked[b]
  /\ blocked' = [blocked EXCEPT ![b] = @ \ {s}]
  /\ \/ /\ Holds(Owner[b], s)
        /\ UNCHANGED msgs
     \/ \E S \in PromiseSets(b) :
          /\ \E Q \in Q1 : Q \subseteq {m.acc : m \in S}
          /\ Decidable(S, s)
          /\ Send(Refill(S, b, s))
  /\ UNCHANGED <<prom, vote, floor, dfloor, learned, phase, from, next, restarts, faults>>

-----------------------------------------------------------------------------
(* Phase 2 *)

\* The leader of ballot b allocates the next slot to a fresh command or to
\* a truncation.
Allocate(b, c) ==
  /\ phase[b] = "p2"
  /\ next[b] < NS
  /\ next' = [next EXCEPT ![b] = @ + 1]
  /\ Send([type |-> "2a", bal |-> b, slot |-> next[b], val |-> c])
  /\ UNCHANGED <<prom, vote, floor, dfloor, learned, phase, from, blocked, restarts, faults>>

\* Acceptor a votes for a value of ballot b at slot s. A slot below its
\* floor is chosen already: it ignores the `Accept`. A vote over a faulty
\* entry heals it.
Accept(a, b, s) ==
  /\ prom[a] <= b
  /\ s >= floor[a]
  /\ \E m \in msgs :
       /\ m.type = "2a" /\ m.bal = b /\ m.slot = s
       /\ prom' = [prom EXCEPT ![a] = b]
       /\ vote' = [vote EXCEPT ![a][s] = [bal |-> b, val |-> m.val]]
       /\ Send([type |-> "2b", acc |-> a, bal |-> b, slot |-> s, val |-> m.val])
  /\ UNCHANGED <<floor, dfloor, learned, phase, from, next, blocked, restarts, faults>>

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
     /\ UNCHANGED <<prom, vote, floor, dfloor, phase, from, next, blocked, restarts, faults, msgs>>

\* Node n catches up from peer p, asking from its first unchosen slot. The
\* peer is at or below that slot, so it replays what it learned (the model
\* reads the learned values; a learned value is stable).
CatchUp(n, p) ==
  /\ n \in Learners
  /\ FirstUnchosen(n) >= floor[p]
  /\ \E s \in Slots : ~Holds(n, s) /\ learned[p][s] # None
  /\ learned' = [learned EXCEPT ![n] = [s \in Slots |->
                   IF ~Holds(n, s) THEN learned[p][s] ELSE @[s]]]
  /\ UNCHANGED <<prom, vote, floor, dfloor, phase, from, next, blocked, restarts, faults, msgs>>

\* The points a `TrimmedTo` from peer p can carry: the floor of p, or an
\* older floor of it, since the answer can arrive late, or come unasked (a
\* follower answers a beat whose sender is below its floor,
\* `node/replication.rs`). The control `TrimPoint = "learned"` takes any
\* point in the chosen prefix of p, truncated or not.
TrimPoints(p) ==
  IF TrimPoint = "floor" THEN 1..floor[p] ELSE 1..FirstUnchosen(p)

\* Node n meets a `TrimmedTo` of peer p with a point above its own floor
\* (`on_trimmed_to` takes any such point, whatever the node's chosen
\* prefix): it jumps there. The jump carries no ballot. The store drops the
\* records and records the floor in one write.
TrimTo(n, p) ==
  /\ n \in Learners
  /\ \E f \in TrimPoints(p) :
       /\ f > floor[n]
       /\ floor' = [floor EXCEPT ![n] = f]
       /\ dfloor' = [dfloor EXCEPT ![n] = f]
       /\ vote' = [vote EXCEPT ![n] = DropVotes(n, f)]
       /\ learned' = [learned EXCEPT ![n] = DropLearned(n, f)]
  /\ IF TrimPromise = "keep"
       THEN UNCHANGED prom
       ELSE prom' = [prom EXCEPT ![n] = prom[p]]
  /\ UNCHANGED <<phase, from, next, blocked, restarts, faults, msgs>>

\* Node a applied the truncation at slot t: it compacts up to the target of
\* the fold, or stops below it (`ColocatedNode::compact(up_to)` clamps the
\* floor to the driver's `up_to`).
Compact(a, t) ==
  /\ learned[a][t] \in Truncations
  /\ CompactWhen = "prefix" => \A s \in 0..t : Holds(a, s)
  /\ \E f \in (floor[a] + 1)..CompactTarget(t) :
       /\ floor' = [floor EXCEPT ![a] = f]
       /\ dfloor' = [dfloor EXCEPT ![a] = IF FloorWrite = "atomic" THEN f ELSE @]
       /\ vote' = [vote EXCEPT ![a] = DropVotes(a, f)]
       /\ learned' = [learned EXCEPT ![a] = DropLearned(a, f)]
  /\ UNCHANGED <<prom, phase, from, next, blocked, restarts, faults, msgs>>

\* The late write of a floor (the control `FloorWrite = "late"`).
PersistFloor(a) ==
  /\ dfloor[a] # floor[a]
  /\ dfloor' = [dfloor EXCEPT ![a] = floor[a]]
  /\ UNCHANGED <<prom, vote, floor, learned, phase, from, next, blocked, restarts, faults, msgs>>

-----------------------------------------------------------------------------
(* Faults *)

\* Node a restarts: its ballots die, the floor comes back from the disk, and
\* it keeps a prefix of what it learned, at or above its floor. The promise
\* and the votes are on disk before the node answers. The chosen index is a
\* relaxed write (`SetChosenIndex` is not in `WriteOp::needs_sync`), so a
\* boot can come back with a lower first unchosen slot, never below the
\* floor (`node/boot.rs`).
Restart(a) ==
  /\ restarts < Restarts
  /\ restarts' = restarts + 1
  /\ phase' = KillBallots(a)
  /\ floor' = [floor EXCEPT ![a] = dfloor[a]]
  /\ \E k \in dfloor[a]..FirstUnchosen(a) :
       learned' = [learned EXCEPT ![a] = [s \in Slots |-> IF s < k THEN @[s] ELSE None]]
  /\ UNCHANGED <<prom, vote, dfloor, from, next, blocked, faults, msgs>>

\* Node a boots and finds its record of slot s damaged, which the scrub did
\* not reach first: the vote keeps its ballot and loses its value, and the
\* learned value of the slot goes with it.
Rot(a, s) ==
  /\ a \in RotNodes
  /\ faults < Faults
  /\ Readable(vote[a][s])
  /\ faults' = faults + 1
  /\ vote' = [vote EXCEPT ![a][s] = [bal |-> @.bal, val |-> Lost]]
  /\ learned' = [learned EXCEPT ![a][s] = None]
  /\ phase' = KillBallots(a)
  /\ UNCHANGED <<prom, floor, dfloor, from, next, blocked, restarts, msgs>>

Next ==
  \/ \E b \in Ballots : Prepare(b) \/ AskAgain(b) \/ Win(b)
  \/ \E b \in Ballots, c \in Proposals : Allocate(b, c)
  \/ \E a \in Acceptors, b \in Ballots : Promise(a, b)
  \/ \E a \in Acceptors, b \in Ballots, s \in Slots : Accept(a, b, s)
  \/ \E b \in Ballots, s \in Slots : Decide(b, s) \/ Probe(b, s)
  \/ \E n, p \in Acceptors : CatchUp(n, p) \/ TrimTo(n, p)
  \/ \E a \in Acceptors, t \in Slots : Compact(a, t)
  \/ \E a \in Acceptors : Restart(a) \/ (FloorWrite = "late" /\ PersistFloor(a))
  \/ \E a \in Acceptors, s \in Slots : Rot(a, s)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Liveness *)

\* The last ballot NB is the leader that no higher ballot preempts. Its
\* owner campaigns, asks again after a refusal, catches up and jumps, and
\* each acceptor keeps taking its steps.
Fairness ==
  LET o == Owner[NB]
  IN /\ WF_vars(Prepare(NB))
     /\ WF_vars(AskAgain(NB))
     /\ WF_vars(Win(NB))
     /\ \A s \in Slots : WF_vars(Decide(NB, s)) /\ WF_vars(Probe(NB, s))
     /\ \A a \in Acceptors :
          /\ WF_vars(Promise(a, NB))
          /\ \A s \in Slots : WF_vars(Accept(a, NB, s))
     /\ \A p \in Acceptors : WF_vars(CatchUp(o, p)) /\ WF_vars(TrimTo(o, p))

LiveSpec == Spec /\ Fairness

\* The last leader holds every slot below its allocator, and its repair
\* probe is closed: no chosen slot stays unapplied, a faulty one included.
Settled ==
  /\ phase[NB] = "p2"
  /\ blocked[NB] = {}
  /\ \A s \in Slots : s < next[NB] => Holds(Owner[NB], s)

Ends == <>[]Settled

-----------------------------------------------------------------------------
(* Safety *)

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
\* below a chosen truncation.
FloorTruncated ==
  \A a \in Acceptors, s \in Slots :
    s < floor[a] => \E t \in Slots, v \in Truncations : t >= s /\ Chosen(t, v)

\* A floor keeps the records: a record below a floor is below `first_seq`.
\* The slots below `first_seq` are the only ones that a node may forget.
FloorReleased ==
  \A a \in Acceptors, s \in Slots :
    (s < floor[a] /\ \A u \in 0..s : IsChosen(u)) /\ ChosenVal(s) \in Commands =>
      \E t \in Slots : t >= s /\ (\A u \in 0..t : IsChosen(u)) /\ FirstSeq(t) > Pos(s)

\* Recoverable: for each chosen slot, each Phase-1 quorum has a member that
\* reports a vote there to a campaign from the slot (a faulty one blocks
\* the tally), or a member that refuses that campaign (the floor guard).
\* With `FloorGuard = "ignore"` a member whose floor is above the slot
\* promises and reports nothing, so a quorum of such members recovers no
\* value.
Recoverable ==
  \A s \in Slots, Q \in Q1 :
    IsChosen(s) => \E a \in Q : vote[a][s].bal > 0 \/ ~MayPromise(a, s)

\* The value is kept: each chosen slot is below a floor, or a node can
\* still read its value (a readable vote or a learned value).
ValueKept ==
  \A s \in Slots :
    IsChosen(s) =>
      \E a \in Acceptors : floor[a] > s \/ Readable(vote[a][s]) \/ learned[a][s] # None

\* The vote watermark of acceptor a: its last vote, faulty votes too, or its
\* floor minus 1 (Watermark = "floor"). -1 is a log that never voted
\* (`None`).
VoteWatermark(a) ==
  LET held == {s \in Slots : vote[a][s].bal > 0}
      top == IF held = {} THEN -1 ELSE Max(held)
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
\* A floor keeps a record: a node applied a truncation, its floor is above
\* 0 and at or below the truncation's slot, and the fold lets it go no
\* further (the floor is the slot of the record at `first_seq`).
NoKeep == ~\E a \in Acceptors, t \in Slots :
            /\ 0 < floor[a] /\ floor[a] <= t
            /\ learned[a][t] \in Truncations
            /\ \A u \in 0..t : Holds(a, u)
            /\ CompactTarget(t) <= floor[a]
\* A boot finds a faulty entry at a chosen slot.
NoFaultyChosen == ~\E a \in Acceptors, s \in Slots : Faulty(vote[a][s]) /\ IsChosen(s)
\* A repair probe opens.
NoBlock == \A b \in Ballots : blocked[b] = {}
\* A refused candidate asks again.
NoRetry == ~\E b \in Ballots : phase[b] # "idle" /\ from[b] > 0 /\ \E m \in msgs : m.type = "1a" /\ m.bal = b /\ m.from < from[b]
=============================================================================
