-------------------------- MODULE TruncationReconfig --------------------------
(***************************************************************************)
(* Truncation across a reconfiguration onto empty stores (#1218            *)
(* (truncation model, part 3)). The log, the compaction floor, the floor   *)
(* guard, the trim-point jump and the retry of a refused candidate are the *)
(* ones of `truncation/`. This model adds a second configuration whose new *)
(* member starts with an empty store, the matchmaking of Matchmaker Paxos  *)
(* and the GC that forgets the first configuration.                        *)
(*                                                                         *)
(* The question: once the first configuration is forgotten, does each     *)
(* chosen slot stay recoverable over the Phase-1 quorums of the new one,   *)
(* when some slots are below the floor of a member and a new member holds  *)
(* nothing below its own floor?                                            *)
(*                                                                         *)
(* A message is never lost from `msgs`, so a message can arrive late, two  *)
(* times, or never. Each ballot has one owner.                             *)
(*                                                                         *)
(* What the model adds, and the code it follows:                           *)
(*   - A configuration per ballot: ballots below `R` use `C1`, ballots     *)
(*     from `R` up use `C2` (`ColocatedNode::reconfigure`: a              *)
(*     reconfiguration is a round change). A member of `C2` outside `C1`   *)
(*     starts with floor 0, no vote and nothing learned. Ballots from `R2` *)
(*     up use `C3`, a second reconfiguration whose new members start empty *)
(*     too.                                                                *)
(*   - Matchmaking (`matchmaking.rs`): a candidate registers its ballot    *)
(*     before its `Prepare`. A matchmaker refuses a ballot below one that  *)
(*     it registered. The candidate gets the configurations of the         *)
(*     registered ballots below its own, from the GC watermark up (`H_b`). *)
(*     Phase 1 needs a Phase-1 quorum of each configuration of `H_b` and   *)
(*     of its own; Phase 2 uses its own configuration only.                *)
(*   - Learning records the chosen value as the accepted record of the     *)
(*     node, at the choosing ballot, and raises the promise to it          *)
(*     (`mark_chosen` in `node/learn.rs`). A held record of the same value *)
(*     at a ballot at or above it stays. A catch-up copies the record of   *)
(*     the peer.                                                           *)
(*   - The GC (`node/gc.rs`, `Collector::covered`): the leader of ballot   *)
(*     `b` raises the matchmakers' watermark to `b`, so every later        *)
(*     candidate forgets the configurations below `b`. The model raises it *)
(*     in one step: a watermark is effective only once a matchmaker quorum *)
(*     acked it, and every later matchmaking quorum meets that quorum.     *)
(*                                                                         *)
(* Abstractions, to keep the state space small (part 2 checked each of     *)
(* them on one configuration):                                             *)
(*   - Every truncation releases every record before its slot: a node that *)
(*     compacts at the truncation of slot `t` moves its floor to `t + 1`.  *)
(*     The fold of `first_seq` is the subject of `truncation/`.            *)
(*   - No faulty entry (CTRL) and no restart: a restart keeps every       *)
(*     durable record, and `truncation/` checks CTRL.                      *)
(*   - A wipe (`Wipes`): an acceptor loses its whole store. `Identity`     *)
(*     TRUE is the rule (a wiped acceptor never takes a step again);       *)
(*     FALSE is a control (it comes back empty as the same acceptor).      *)
(*                                                                         *)
(* Constants (each rule has a control):                                    *)
(*   GcRule  "fence": the rule of the code. The leader forgets the older   *)
(*           configurations once a Phase-2 quorum of its own configuration *)
(*           reports a chosen index at or past its election fence `F`.     *)
(*           "won": a control, it forgets them as soon as it wins Phase 1. *)
(*   Cover   "chosen": a member counts toward the fence when it holds each *)
(*           slot up to `F` (it learned it, or the slot is below its       *)
(*           floor). "joined": a control, a new member counts too, with    *)
(*           floor 0 and no record.                                        *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS Acceptors, Learners, Commands, Cuts, Noop,
          NB, NS, Owner, C1, C2, R, C3, R2,
          GcRule, Cover, Wipes, Identity

Ballots == 1..NB
Slots == 0..(NS - 1)
Proposals == Commands \cup Cuts
Values == Proposals \cup {Noop}
None == "none"

ASSUME GcRule \in {"fence", "won"}
ASSUME Cover \in {"chosen", "joined"}
ASSUME Wipes \in Nat /\ Identity \in BOOLEAN
ASSUME Learners \subseteq Acceptors
ASSUME C1 \subseteq Acceptors /\ C2 \subseteq Acceptors /\ C1 # {} /\ C2 # {}
ASSUME R \in Ballots
ASSUME C3 \subseteq Acceptors /\ C3 # {} /\ R2 \in R..(NB + 1)
ASSUME Noop \notin Proposals
ASSUME Cuts # {} /\ Cuts \cap Commands = {}
ASSUME Owner \in [Ballots -> Acceptors]
Vote == [bal : 0..NB, val : Values \cup {None}]
NoVote == [bal |-> 0, val |-> None]

\* The configuration of ballot b, and its quorums: the majorities. Ballots
\* from `R2` up use `C3`: a second reconfiguration (`R2 = NB + 1`: none).
Conf(b) == IF b >= R2 THEN C3 ELSE IF b >= R THEN C2 ELSE C1
Maj(C) == {Q \in SUBSET C : 2 * Cardinality(Q) > Cardinality(C)}

\* The members of C2 and C3 that start with an empty store.
Joiners == (C2 \ C1) \cup (C3 \ (C1 \cup C2))

VARIABLES prom,     \* acceptor: the promise
          vote,     \* acceptor: the accepted record, per slot
          floor,    \* acceptor: the first slot it holds
          learned,  \* node: the value it learned, per slot
          phase,    \* ballot: "idle", "p1" or "p2"
          from,     \* ballot: the slot its campaign asks from
          next,     \* ballot: the allocator's next slot, once in Phase 2
          fence,    \* ballot: one past its election fence `F`, once in Phase 2
          hist,     \* ballot: the configurations of `H_b`, from matchmaking
          wm,       \* the GC watermark of the matchmakers
          msgs,     \* every message sent: the ghost of every vote
          wiped     \* the acceptors that lost their disk

vars == <<prom, vote, floor, learned, phase, from, next, fence, hist, wm, msgs, wiped>>

Msg ==
  [type : {"1a"}, bal : Ballots, from : 0..NS]
    \cup [type : {"1b"}, acc : Acceptors, bal : Ballots, from : 0..NS, rep : [Slots -> Vote]]
    \cup [type : {"2a"}, bal : Ballots, slot : Slots, val : Values]
    \cup [type : {"2b"}, acc : Acceptors, bal : Ballots, slot : Slots, val : Values]

TypeOK ==
  /\ prom \in [Acceptors -> 0..NB]
  /\ vote \in [Acceptors -> [Slots -> Vote]]
  /\ floor \in [Acceptors -> 0..NS]
  /\ learned \in [Acceptors -> [Slots -> Values \cup {None}]]
  /\ phase \in [Ballots -> {"idle", "p1", "p2"}]
  /\ from \in [Ballots -> 0..NS]
  /\ next \in [Ballots -> 0..NS]
  /\ fence \in [Ballots -> 0..NS]
  /\ hist \in [Ballots -> SUBSET {C1, C2, C3}]
  /\ wm \in 0..NB
  /\ msgs \subseteq Msg
  /\ wiped \subseteq Acceptors

Init ==
  /\ prom = [a \in Acceptors |-> 0]
  /\ vote = [a \in Acceptors |-> [s \in Slots |-> NoVote]]
  /\ floor = [a \in Acceptors |-> 0]
  /\ learned = [a \in Acceptors |-> [s \in Slots |-> None]]
  /\ phase = [b \in Ballots |-> "idle"]
  /\ from = [b \in Ballots |-> 0]
  /\ next = [b \in Ballots |-> 0]
  /\ fence = [b \in Ballots |-> 0]
  /\ hist = [b \in Ballots |-> {}]
  /\ wm = 0
  /\ msgs = {}
  /\ wiped = {}

Send(m) == msgs' = msgs \cup {m}
SendAll(M) == msgs' = msgs \cup M

Max(S) == CHOOSE x \in S : \A y \in S : y <= x

\* Node n takes steps: with Identity, a wiped acceptor never does again.
Up(n) == Identity => n \notin wiped

\* Node n knows slot s: it learned the value, or the slot is below its floor.
Holds(n, s) == s < floor[n] \/ learned[n][s] # None

\* The first slot node n does not hold (`first_unchosen`).
FirstUnchosen(n) ==
  IF \A s \in Slots : Holds(n, s) THEN NS
  ELSE CHOOSE s \in Slots : ~Holds(n, s) /\ \A t \in Slots : t < s => Holds(n, t)

\* Drop every vote and every learned value below the floor f.
DropVotes(a, f) == [s \in Slots |-> IF s < f THEN NoVote ELSE vote[a][s]]
DropLearned(a, f) == [s \in Slots |-> IF s < f THEN None ELSE learned[a][s]]

\* The configurations a Phase 1 of ballot b must cover: `H_b` and its own.
Confs(b) == hist[b] \cup {Conf(b)}

-----------------------------------------------------------------------------
(* The chosen log: a value is chosen at slot s once a Phase-2 quorum of    *)
(* the configuration of a ballot voted for it at that ballot. A vote is a  *)
(* 2b message, so a compaction does not unchoose it.                       *)

ChosenAt(b, s, v) ==
  \E Q \in Maj(Conf(b)) : \A a \in Q : [type |-> "2b", acc |-> a, bal |-> b, slot |-> s, val |-> v] \in msgs
Chosen(s, v) == \E b \in Ballots : ChosenAt(b, s, v)
IsChosen(s) == \E v \in Values : Chosen(s, v)

-----------------------------------------------------------------------------
(* Matchmaking and Phase 1 *)

\* The promise of acceptor a at ballot b, for a campaign from `f`.
PromiseMsg(a, b, f) ==
  [type |-> "1b", acc |-> a, bal |-> b, from |-> f,
   rep |-> [s \in Slots |-> IF s >= f THEN vote[a][s] ELSE NoVote]]

\* The owner of ballot b registers it at the matchmakers, gets `H_b`, and
\* opens a campaign from its first unchosen slot. A matchmaker refuses a
\* ballot below one that it registered.
Prepare(b) ==
  LET o == Owner[b]
      f == FirstUnchosen(o)
  IN /\ phase[b] = "idle"
     /\ Up(o)
     /\ \A c \in Ballots : c > b => phase[c] = "idle"
     /\ phase' = [phase EXCEPT ![b] = "p1"]
     /\ from' = [from EXCEPT ![b] = f]
     /\ hist' = [hist EXCEPT ![b] =
                   {Conf(c) : c \in {d \in Ballots : d < b /\ d >= wm /\ phase[d] # "idle"}}]
     /\ prom' = [prom EXCEPT ![o] = b]
     /\ SendAll({[type |-> "1a", bal |-> b, from |-> f], PromiseMsg(o, b, f)})
     /\ UNCHANGED <<wiped, vote, floor, learned, next, fence, wm>>

\* The owner of ballot b asks again from its first unchosen slot. It does so
\* when a floor refused the campaign and a jump or a catch-up moved that
\* slot, but the model lets it ask at any time in Phase 1: a `1a` is safe
\* to send from any slot, and Win takes only the promises from slots at or
\* below `from[b]`. The owner sends its own promise again, so it must still
\* be at ballot b: without that guard, a `1b` of ballot b leaves before
\* Prepare or after a higher promise. A wipe sets the promise to 0, so an
\* owner at ballot b is up. The phase guard bounds the
\* state space: a re-ask after Win sends promises that no step reads, and
\* without the guard `safety` grows from 0.4 to 3.7 million states.
AskAgain(b) ==
  LET o == Owner[b]
      f == FirstUnchosen(o)
  IN /\ phase[b] = "p1"
     /\ prom[o] = b
     /\ from' = [from EXCEPT ![b] = f]
     /\ SendAll({[type |-> "1a", bal |-> b, from |-> f], PromiseMsg(o, b, f)})
     /\ UNCHANGED <<wiped, prom, vote, floor, learned, phase, next, fence, hist, wm>>

\* Acceptor a of a configuration that the campaign covers promises ballot b,
\* unless the floor guard refuses: a campaign that asks from below its
\* floor gets a `Nack` (the model sends nothing).
Promise(a, b) ==
  /\ Up(a)
  /\ prom[a] < b
  /\ a \in UNION Confs(b)
  /\ \E m \in msgs :
       /\ m.type = "1a" /\ m.bal = b
       /\ m.from >= floor[a]
       /\ prom' = [prom EXCEPT ![a] = b]
       /\ Send(PromiseMsg(a, b, m.from))
  /\ UNCHANGED <<wiped, vote, floor, learned, phase, from, next, fence, hist, wm>>

\* The promises of ballot b that cover its campaign: one for each acceptor.
PromiseSets(b) ==
  {S \in SUBSET {m \in msgs : m.type = "1b" /\ m.bal = b /\ m.from <= from[b]} :
     \A m, n \in S : m.acc = n.acc => m = n}

\* The highest vote that the promises S report at slot s.
Top(S, s) ==
  LET rep == {m \in S : m.rep[s].bal > 0}
  IN IF rep = {} THEN NoVote
     ELSE (CHOOSE m \in rep : \A n \in rep : n.rep[s].bal <= m.rep[s].bal).rep[s]

\* One past the highest slot that the promises S report, never below `f`.
Frontier(S, f) ==
  LET h == {s \in Slots : s >= f /\ \E m \in S : m.rep[s].bal > 0}
  IN IF h = {} THEN f ELSE Max(h) + 1

Refill(S, b, s) ==
  [type |-> "2a", bal |-> b, slot |-> s,
   val |-> IF Top(S, s).bal > 0 THEN Top(S, s).val ELSE Noop]

\* The owner of ballot b wins Phase 1 on a Phase-1 quorum of each
\* configuration of `H_b` and of its own; its own promise need not be among
\* them (Paxos needs only the quorums). An owner that was wiped after it
\* opened the ballot may go on: it sends no promise once wiped. It proposes again
\* each slot from its campaign up to its frontier; the fence `F` is the
\* frontier minus 1.
Win(b) ==
  /\ phase[b] = "p1"
  /\ \E S \in PromiseSets(b) :
       /\ \A C \in Confs(b) : \E Q \in Maj(C) : Q \subseteq {m.acc : m \in S}
       /\ LET f == Frontier(S, from[b])
          IN /\ next' = [next EXCEPT ![b] = f]
             /\ fence' = [fence EXCEPT ![b] = f]
             /\ SendAll({Refill(S, b, s) : s \in from[b]..(f - 1)})
  /\ phase' = [phase EXCEPT ![b] = "p2"]
  /\ UNCHANGED <<wiped, prom, vote, floor, learned, from, hist, wm>>

-----------------------------------------------------------------------------
(* Phase 2 *)

Allocate(b, c) ==
  /\ phase[b] = "p2"
  /\ next[b] < NS
  /\ next' = [next EXCEPT ![b] = @ + 1]
  /\ Send([type |-> "2a", bal |-> b, slot |-> next[b], val |-> c])
  /\ UNCHANGED <<wiped, prom, vote, floor, learned, phase, from, fence, hist, wm>>

\* Acceptor a of the configuration of ballot b votes. A slot below its
\* floor is chosen already: it ignores the `Accept`.
Accept(a, b, s) ==
  /\ Up(a)
  /\ a \in Conf(b)
  /\ prom[a] <= b
  /\ s >= floor[a]
  /\ \E m \in msgs :
       /\ m.type = "2a" /\ m.bal = b /\ m.slot = s
       /\ vote[a][s] # [bal |-> b, val |-> m.val]
       /\ prom' = [prom EXCEPT ![a] = b]
       /\ vote' = [vote EXCEPT ![a][s] = [bal |-> b, val |-> m.val]]
       /\ Send([type |-> "2b", acc |-> a, bal |-> b, slot |-> s, val |-> m.val])
  /\ UNCHANGED <<wiped, floor, learned, phase, from, next, fence, hist, wm>>

-----------------------------------------------------------------------------
(* Learning, catch-up, compaction and the trim jump *)

\* The record a node keeps when it learns value v chosen at ballot c over
\* the record r it holds: a record of v at c or above stays.
LearnRecord(r, c, v) == IF r.val = v /\ r.bal >= c THEN r ELSE [bal |-> c, val |-> v]

\* The leader of ballot b learns slot s at a Phase-2 quorum, and records it.
Decide(b, s) ==
  LET o == Owner[b]
  IN /\ Up(o)
     /\ ~Holds(o, s)
     /\ \E v \in Values :
          /\ ChosenAt(b, s, v)
          /\ learned' = [learned EXCEPT ![o][s] = v]
          /\ vote' = [vote EXCEPT ![o][s] = LearnRecord(@, b, v)]
          /\ prom' = [prom EXCEPT ![o] = IF @ < b THEN b ELSE @]
     /\ UNCHANGED <<wiped, floor, phase, from, next, fence, hist, wm, msgs>>

\* Node n catches up from peer p, asking from its first unchosen slot. The
\* peer is at or below that slot: it replays its records of the slots it
\* learned. Node n records each one and raises its promise to the highest.
CatchUp(n, p) ==
  LET new == {s \in Slots : ~Holds(n, s) /\ learned[p][s] # None}
      top == Max({prom[n]} \cup {vote[p][s].bal : s \in new})
  IN /\ Up(n)
     /\ FirstUnchosen(n) >= floor[p]
     /\ learned' = [learned EXCEPT ![n] = [s \in Slots |->
                      IF s \in new THEN learned[p][s] ELSE @[s]]]
     /\ vote' = [vote EXCEPT ![n] = [s \in Slots |->
                   IF s \in new THEN LearnRecord(@[s], vote[p][s].bal, learned[p][s]) ELSE @[s]]]
     /\ prom' = [prom EXCEPT ![n] = top]
     /\ UNCHANGED <<wiped, floor, phase, from, next, fence, hist, wm, msgs>>

\* Node n asks peer p from below the floor of p. The peer answers
\* `TrimmedTo`: n jumps to the floor of p. The promise does not move.
TrimTo(n, p) ==
  /\ n \in Learners
  /\ Up(n)
  /\ FirstUnchosen(n) < floor[p]
  /\ floor' = [floor EXCEPT ![n] = floor[p]]
  /\ vote' = [vote EXCEPT ![n] = DropVotes(n, floor[p])]
  /\ learned' = [learned EXCEPT ![n] = DropLearned(n, floor[p])]
  /\ UNCHANGED <<wiped, prom, phase, from, next, fence, hist, wm, msgs>>

\* Node a learned the truncation at slot t and every slot below it: it
\* releases every record before t, so its floor goes to t + 1.
Compact(a, t) ==
  /\ Up(a)
  /\ learned[a][t] \in Cuts
  /\ \A s \in 0..t : Holds(a, s)
  /\ t + 1 > floor[a]
  /\ floor' = [floor EXCEPT ![a] = t + 1]
  /\ vote' = [vote EXCEPT ![a] = DropVotes(a, t + 1)]
  /\ learned' = [learned EXCEPT ![a] = DropLearned(a, t + 1)]
  /\ UNCHANGED <<wiped, prom, phase, from, next, fence, hist, wm, msgs>>

-----------------------------------------------------------------------------
(* GC *)

\* Member a of the configuration of ballot b counts toward the fence of b.
Covers(a, b) ==
  \/ \A s \in Slots : s < fence[b] => Holds(a, s)
  \/ Cover = "joined" /\ a \in Joiners

\* The leader of ballot b raises the GC watermark to b: every later
\* candidate forgets the configurations of the ballots below b.
Gc(b) ==
  /\ phase[b] = "p2"
  /\ Up(Owner[b])
  /\ wm < b
  /\ GcRule = "fence" =>
       \E Q \in Maj(Conf(b)) : \A a \in Q : Covers(a, b)
  /\ wm' = b
  /\ UNCHANGED <<wiped, prom, vote, floor, learned, phase, from, next, fence, hist, msgs>>

\* Acceptor a loses its disk: no promise, no record, nothing learned, floor
\* 0. With Identity it never takes a step again (`decree/` and
\* `multi-paxos/` have the same rule); with the control it comes back as
\* the same acceptor and answers with the empty store.
Wipe(a) ==
  /\ Up(a)
  /\ Cardinality(wiped) < Wipes
  /\ wiped' = wiped \cup {a}
  /\ prom' = [prom EXCEPT ![a] = 0]
  /\ vote' = [vote EXCEPT ![a] = [s \in Slots |-> NoVote]]
  /\ floor' = [floor EXCEPT ![a] = 0]
  /\ learned' = [learned EXCEPT ![a] = [s \in Slots |-> None]]
  /\ UNCHANGED <<phase, from, next, fence, hist, wm, msgs>>

Next ==
  \/ \E a \in Acceptors : Wipe(a)
  \/ \E b \in Ballots : Prepare(b) \/ AskAgain(b) \/ Win(b) \/ Gc(b)
  \/ \E b \in Ballots, c \in Proposals : Allocate(b, c)
  \/ \E a \in Acceptors, b \in Ballots : Promise(a, b)
  \/ \E a \in Acceptors, b \in Ballots, s \in Slots : Accept(a, b, s)
  \/ \E b \in Ballots, s \in Slots : Decide(b, s)
  \/ \E n, p \in Acceptors : CatchUp(n, p) \/ TrimTo(n, p)
  \/ \E a \in Acceptors, t \in Slots : Compact(a, t)

Spec == Init /\ [][Next]_vars

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

\* A record is of the chosen value: an accepted record at a chosen slot
\* holds that value, or its ballot is below every ballot that chose it.
RecordChosen ==
  \A a \in Acceptors, s \in Slots, v \in Values :
    (vote[a][s].bal > 0 /\ Chosen(s, v) /\ vote[a][s].val # v) =>
      \A b \in Ballots : ChosenAt(b, s, v) => vote[a][s].bal < b

\* The configurations that the next candidate must cover: those of the
\* registered ballots at or above the watermark.
Live == {Conf(c) : c \in {d \in Ballots : d >= wm /\ phase[d] # "idle"}}

\* Recoverable: for each chosen slot, one configuration that the next
\* candidate must cover has, in each of its Phase-1 quorums, a member that
\* holds a record there or whose floor is above the slot (and that member
\* refuses a campaign that asks from below it). A candidate needs a quorum
\* of each such configuration, so it meets that member. With Identity, a
\* quorum with a wiped member never answers, so it does not count.
Recoverable ==
  \A s \in Slots :
    IsChosen(s) =>
      \E C \in Live : \A Q \in Maj(C) :
        (\A a \in Q : Up(a)) => \E a \in Q : vote[a][s].bal > 0 \/ floor[a] > s

\* Reachability controls: each must be violated.
\* A slot chosen under the first configuration.
ChosenUnderC1 == \E b \in Ballots, s \in Slots, v \in Values : b < R /\ ChosenAt(b, s, v)
\* The GC forgets the first configuration after it chose a slot.
NoGc == ~(wm >= R /\ ChosenUnderC1)
\* A new member jumps: its floor covers a slot that it never voted.
NoJoinJump == ~\E a \in Joiners, s \in Slots :
                s < floor[a] /\ ~\E m \in msgs : m.type = "2b" /\ m.acc = a /\ m.slot = s
\* The GC forgets the first configuration while a member of the second has
\* compacted a slot and a new member holds nothing below that floor.
NoGcSplit == ~(wm >= R /\ \E a \in C2, j \in Joiners :
                 /\ floor[a] > 0 /\ floor[j] = 0
                 /\ \A s \in Slots : s < floor[a] => vote[j][s].bal = 0)
\* A ballot wins Phase 1 over the second configuration alone, once the
\* GC forgot the first after it chose a slot.
NoLateWin == ~(ChosenUnderC1 /\ \E b \in Ballots : b > R /\ phase[b] = "p2" /\ hist[b] = {C2})
\* A ballot of the third configuration wins with the first forgotten, after a
\* slot was chosen under the first.
NoThirdWin == ~(ChosenUnderC1 /\ \E b \in Ballots : b >= R2 /\ phase[b] = "p2" /\ C1 \notin hist[b])
\* A member of the second configuration is wiped after the GC forgot the
\* first, once a slot was chosen under the first.
NoWipeAfterGc == ~(wm >= R /\ ChosenUnderC1 /\ wiped \cap C2 # {})
=============================================================================
