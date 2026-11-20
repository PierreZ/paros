-------------------------------- MODULE Node ---------------------------------
(***************************************************************************)
(* The implementation model of `ColocatedNode` (`paros-consensus`), at the *)
(* boundary of the code: one action is one step that a batch of a node, a  *)
(* matchmaker or a read replica makes durable (#1306 (trace specs extend   *)
(* the checked model)). Every trace spec of this folder extends this       *)
(* module and only binds the events of a walk of `consensus-sim` to its    *)
(* actions, so the spec that judges the code is the spec that TLC checks   *)
(* here on bounded instances (`checks.txt`).                               *)
(*                                                                         *)
(* The small models of the other folders each check one rule, as design    *)
(* tools. This model holds the rules of the log together, as the code      *)
(* runs them:                                                              *)
(*   - the log (`multi-paxos/`): Phase 1 from the first slot the owner     *)
(*     does not hold, the frontier, the recovery with the `Noop` gap fill, *)
(*     Phase 2, the learns, the records of a learn (`mark_chosen`), and a  *)
(*     restart that keeps the disk and a prefix of the chosen index;       *)
(*   - matchmaking (`matchmaker/`): a ballot selects a configuration,      *)
(*     registers it, and closes on a matchmaker quorum; Phase 1 needs a    *)
(*     quorum of every configuration of `H_b` but the ones that only       *)
(*     abandoned ballots registered (`Election::drop_abandoned`); Phase 2  *)
(*     is in `C_b`; the GC request of `node/gc.rs`, the GC acks, the       *)
(*     retirement on an effective floor and the rejoin. With              *)
(*     `Matchmaking = FALSE` every ballot runs on the configuration        *)
(*     `Static` and no matchmaker step is enabled (plain Multi-Paxos);     *)
(*   - the quorums of a configuration are the constants `P1Quorums` and    *)
(*     `P2Quorums`: majorities or a grid (`grid/`);                        *)
(*   - the compaction floor (`truncation/`): the journal fold is an input  *)
(*     (the replica's fold, slot by slot), a compaction holds the slots    *)
(*     below it and releases them by the fold, a trim jump goes to the     *)
(*     floor of a peer and moves no promise, and every step of the log     *)
(*     takes the floor guards;                                             *)
(*   - the faulty entries of CTRL (`truncation/`): a record that keeps its *)
(*     ballot and loses its value, a promise in pages, the tri-state tally *)
(*     of a win, the repair probe, and the fold hole a campaign starts at; *)
(*   - the read replica (`compartments/`): its learns, its walk in slot    *)
(*     order, its floor, its boot and its leaderless read;                 *)
(*   - the leaderless reads of a node (`reads/`): the read basis of a won  *)
(*     leadership, the answers with their read fence, the fold of an       *)
(*     answer and the served index;                                        *)
(*   - the `Commit` of a proxy leader: a Phase-2 quorum of `C_b` chose the *)
(*     value at the round's ballot.                                        *)
(*                                                                         *)
(* The cooperative handoff and the rounds of a proxy are judged by         *)
(* `leader-handoff/` and `proxy-leader/` on their own walks. A ballot of   *)
(* this model may outlive its owner's restart (`led` of `Restart`): a      *)
(* handoff hands a ballot to a successor, and the successor's allocator    *)
(* goes on from `next`.                                                    *)
(*                                                                         *)
(* A message is never lost from `msgs`. A vote is a 2b message, so a       *)
(* restart, a compaction or a rotten record never unchooses a value.       *)
(*                                                                         *)
(* The rule constants: the first value of each is the rule of the code,    *)
(* each other value is a control that `checks.txt` shows TLC refuses.      *)
(*   Alloc       "frontier": fresh commands start at the frontier.         *)
(*               "prefix": they start at the campaign's first slot.        *)
(*   Gap         "noop": a gap below the frontier gets a `Noop`. "skip":   *)
(*               it gets nothing.                                          *)
(*   Pick        "highest": the recovery takes the highest reported vote.  *)
(*               "lowest": it takes the lowest.                            *)
(*   Phase1      "every": a Phase-1 quorum of each configuration of `H_b`. *)
(*               "own": a quorum of `C_b` alone.                           *)
(*   MatchCheck  TRUE: a matchmaker refuses a ballot below one it holds.   *)
(*   Gone        "owner": a configuration leaves `H_b` only when the owner *)
(*               of each of its ballots promised `b` and never prepared    *)
(*               that ballot. "any": it may leave whatever happened.       *)
(*   Gc          "learned": the GC request waits for a Phase-2 quorum of   *)
(*               `C_b` that holds the prefix below the frontier. "early":  *)
(*               it goes at the win.                                       *)
(*   Retire      "effective": a node retires above a floor that a          *)
(*               matchmaker quorum acked. "oneAck": one ack is enough.     *)
(*   FloorGuard  "nack": a promise asks from at or above the floor.        *)
(*               "ignore": it may ask from below.                          *)
(*   CompactWhen "prefix": a compaction holds every slot below its floor.  *)
(*               "any": it may skip a slot.                                *)
(*   Release     "fold": the fold released the records below the floor.    *)
(*               "none": the floor ignores the fold.                       *)
(*   TrimPoint   "floor": a jump goes to a floor some peer holds. "any":   *)
(*               it goes anywhere.                                         *)
(*   Watermark   "floor": a read answer counts the floor. "votes": it      *)
(*               counts the retained votes only.                           *)
(*   Tally       "ctrl": a faulty report above the highest readable one    *)
(*               blocks a slot. "none": a faulty report counts as no vote. *)
(*   RecordAt    "chosen": a record keeps a value chosen at its ballot or  *)
(*               below. "any": it keeps any value.                         *)
(*   RepRead     "quorum": a replica serves on a Phase-1 quorum of         *)
(*               answers. "one": on one answer.                            *)
(*   Fence       "promise": a read answer's fence is at or above the       *)
(*               promise. "none": it is any ballot.                        *)
(*   Commit      "backed": a proxy commits a chosen value. "any": any.     *)
(* `Learners` are the nodes that learn and keep records of a learn: every  *)
(* node in a trace; a check may leave out a node that owns no ballot.      *)
(* Bounds of a check (a trace sets each from its events): `Restarts`, the  *)
(* restarts; `Faults`, the rotten records; `MaxPos`, the positions of the  *)
(* fold; `Paged`, TRUE when a promise may come in pages; `Floors`, TRUE    *)
(* when nodes compact and jump; `Proxies`, TRUE when proxy leaders commit; *)
(* `Reads`, TRUE when nodes learn read bases and parties apply writes.     *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences

CONSTANTS Acceptors, Learners, Commands, Noop, NB, NS, Owner,
          Matchmaking, Static, Configs, Matchmakers,
          P1Quorums(_), P2Quorums(_),
          Replicas, Ctxs,
          Restarts, Faults, MaxPos, Paged, Floors, Proxies, Reads,
          Alloc, Gap, Pick, Phase1, MatchCheck, Gone, Gc, Retire,
          FloorGuard, CompactWhen, Release, TrimPoint, Watermark, Tally, RecordAt,
          RepRead, Fence, Commit

Ballots == 1..NB
Slots == 0..(NS - 1)
Values == Commands \cup {Noop}
None == "none"
Lost == "lost"

ASSUME Noop \notin Commands /\ Lost \notin Values /\ None \notin Values
ASSUME Owner \in [Ballots -> Acceptors] /\ Learners \subseteq Acceptors
ASSUME Matchmaking \in BOOLEAN /\ Paged \in BOOLEAN /\ MatchCheck \in BOOLEAN
ASSUME Floors \in BOOLEAN /\ Proxies \in BOOLEAN /\ Reads \in BOOLEAN
ASSUME Static \subseteq Acceptors /\ \A c \in Configs : c # {} /\ c \subseteq Acceptors
ASSUME Alloc \in {"frontier", "prefix"}
ASSUME Gap \in {"noop", "skip"}
ASSUME Pick \in {"highest", "lowest"}
ASSUME Phase1 \in {"every", "own"}
ASSUME Gone \in {"owner", "any"}
ASSUME Gc \in {"learned", "early"}
ASSUME Retire \in {"effective", "oneAck"}
ASSUME FloorGuard \in {"nack", "ignore"}
ASSUME CompactWhen \in {"prefix", "any"}
ASSUME Release \in {"fold", "none"}
ASSUME TrimPoint \in {"floor", "any"}
ASSUME Watermark \in {"floor", "votes"}
ASSUME Tally \in {"ctrl", "none"}
ASSUME RecordAt \in {"chosen", "any"}
ASSUME RepRead \in {"quorum", "one"}
ASSUME Fence \in {"promise", "none"}
ASSUME Commit \in {"backed", "any"}

Vote == [bal : 0..NB, val : Values \cup {None, Lost}]
NoVote == [bal |-> 0, val |-> None]
NoBasis == [bal |-> 0, fence |-> -1]
NoRead == [state |-> "none", node |-> None, b |-> 0, fence |-> -1, snap |-> -1,
           answers |-> {}, index |-> -1]
NoRRead == [state |-> "none", rep |-> None, snap |-> -1, index |-> -1]

Maj(S) == {Q \in SUBSET S : 2 * Cardinality(Q) > Cardinality(S)}
MQuorums == Maj(Matchmakers)
Max(S) == CHOOSE x \in S : \A y \in S : y <= x
Min(S) == CHOOSE x \in S : \A y \in S : x <= y

VARIABLES
  \* The log.
  prom,     \* node: its promise
  vote,     \* node: its record of each slot
  learned,  \* node: the value it learned at each slot
  phase,    \* ballot: "idle", "p1", "p2" or "dead"
  next,     \* ballot: the allocator's next slot
  front,    \* ballot: the frontier its Phase 1 derived
  won,      \* the ballots whose Phase 1 won
  msgs,     \* every message sent: 1a, 1b, 2a, 2b, ga, gb, commit
  restarts, \* how many restarts happened
  \* Matchmaking.
  cfg,      \* ballot: its configuration, {} before it selects one
  reg,      \* matchmaker: its registrations, <<ballot, configuration>>
  wm,       \* matchmaker: its GC watermark
  answers,  \* ballot: the matchmakers' answers, [mm, hist, w]
  matched,  \* ballot: TRUE once its matchmaking closed
  hist,     \* ballot: H_b, the configurations of its Phase 1
  retired,  \* the nodes that retired
  \* The compaction floor and CTRL.
  floor,    \* node: the first slot it holds
  fold,     \* the fold of the slots 0..Len(fold)-1: [high, first]
  rotted,   \* node: the slots of its prefix that a boot read faulty
  hole,     \* node: the slot its fold stops at (-1: none)
  faults,   \* how many records rotted
  \* The read replica.
  rlearned, \* replica: the value it holds at each slot
  rnext,    \* replica: the next slot its walk folds
  rfloor,   \* replica: the first slot it holds
  rrds,     \* read token: a read of a replica
  \* The leaderless reads.
  rbasis,   \* node: the ballot and the fence of its read basis
  racks,    \* the answers the acceptors sent: [acc, ctx, wm, fence]
  rds,      \* read token: a read of a node
  acked     \* the highest slot whose write completed (-1: none)

logVars == <<prom, vote, learned, phase, next, front, won, msgs, restarts>>
mmVars == <<cfg, reg, wm, answers, matched, hist, retired>>
floorVars == <<floor, fold, rotted, hole, faults>>
repVars == <<rlearned, rnext, rfloor, rrds>>
readVars == <<rbasis, racks, rds, acked>>
vars == <<logVars, mmVars, floorVars, repVars, readVars>>

Entry == Ballots \X Configs

TypeOK ==
  /\ prom \in [Acceptors -> 0..NB]
  /\ vote \in [Acceptors -> [Slots -> Vote]]
  /\ learned \in [Acceptors -> [Slots -> Values \cup {None}]]
  /\ phase \in [Ballots -> {"idle", "p1", "p2", "dead"}]
  /\ next \in [Ballots -> 0..NS]
  /\ front \in [Ballots -> 0..NS]
  /\ won \subseteq Ballots
  /\ restarts \in 0..Restarts
  /\ cfg \in [Ballots -> SUBSET Acceptors]
  /\ reg \in [Matchmakers -> SUBSET Entry]
  /\ wm \in [Matchmakers -> 0..NB]
  /\ matched \in [Ballots -> BOOLEAN]
  /\ hist \in [Ballots -> SUBSET SUBSET Acceptors]
  /\ retired \subseteq Acceptors
  /\ floor \in [Acceptors -> 0..NS]
  /\ rotted \in [Acceptors -> SUBSET Slots]
  /\ hole \in [Acceptors -> -1..(NS - 1)]
  /\ faults \in 0..Faults
  /\ rlearned \in [Replicas -> [Slots -> Values \cup {None}]]
  /\ rnext \in [Replicas -> 0..NS]
  /\ rfloor \in [Replicas -> 0..NS]
  /\ rbasis \in [Acceptors -> [bal : 0..NB, fence : -1..NS]]
  /\ acked \in -1..(NS - 1)

Init ==
  /\ prom = [a \in Acceptors |-> 0]
  /\ vote = [a \in Acceptors |-> [s \in Slots |-> NoVote]]
  /\ learned = [a \in Acceptors |-> [s \in Slots |-> None]]
  /\ phase = [b \in Ballots |-> "idle"]
  /\ next = [b \in Ballots |-> 0]
  /\ front = [b \in Ballots |-> 0]
  /\ won = {}
  /\ msgs = {}
  /\ restarts = 0
  /\ cfg = [b \in Ballots |-> IF Matchmaking THEN {} ELSE Static]
  /\ reg = [m \in Matchmakers |-> {}]
  /\ wm = [m \in Matchmakers |-> 0]
  /\ answers = [b \in Ballots |-> {}]
  /\ matched = [b \in Ballots |-> FALSE]
  /\ hist = [b \in Ballots |-> {}]
  /\ retired = {}
  /\ floor = [a \in Acceptors |-> 0]
  /\ fold = <<>>
  /\ rotted = [a \in Acceptors |-> {}]
  /\ hole = [a \in Acceptors |-> -1]
  /\ faults = 0
  /\ rlearned = [r \in Replicas |-> [s \in Slots |-> None]]
  /\ rnext = [r \in Replicas |-> 0]
  /\ rfloor = [r \in Replicas |-> 0]
  /\ rrds = [c \in Ctxs |-> NoRRead]
  /\ rbasis = [n \in Acceptors |-> NoBasis]
  /\ racks = {}
  /\ rds = [c \in Ctxs |-> NoRead]
  /\ acked = -1

-----------------------------------------------------------------------------
(* What the steps read *)

\* A node that may take a step: it did not retire.
Live(a) == a \notin retired

\* Node n holds slot s: it learned it, or the slot is below its floor.
Holds(n, s) == s < floor[n] \/ learned[n][s] # None

\* The first slot node n does not hold (`first_unchosen`).
FirstUnheld(n) ==
  IF \A s \in Slots : Holds(n, s) THEN NS
  ELSE CHOOSE s \in Slots : ~Holds(n, s) /\ \A t \in Slots : t < s => Holds(n, t)

\* Where a campaign of node n starts: the hole of its fold, or the first
\* slot it does not hold (`open_campaign`, `Replica::fold_hole`).
From(n) == IF hole[n] >= 0 THEN hole[n] ELSE FirstUnheld(n)

Readable(v) == v.bal > 0 /\ v.val # Lost
Faulty(v) == v.bal > 0 /\ v.val = Lost

\* A value is chosen at slot s and ballot b once a Phase-2 quorum of the
\* configuration of b voted for it at b.
ChosenAt(b, s, v) ==
  /\ cfg[b] # {}
  /\ \E Q \in P2Quorums(cfg[b]) :
       \A a \in Q : [type |-> "2b", acc |-> a, bal |-> b, slot |-> s, val |-> v] \in msgs
Chosen(s, v) == \E b \in Ballots : ChosenAt(b, s, v)
IsChosen(s) == \E v \in Values : Chosen(s, v)

\* The first slot of the campaign of ballot b.
CampaignFrom(b) == (CHOOSE m \in msgs : m.type = "1a" /\ m.bal = b).from

\* The history that the matchmaker answers S give: their union at or above
\* the maximum watermark.
Union(S) ==
  LET w == IF S = {} THEN 0 ELSE Max({r.w : r \in S})
  IN {e[2] : e \in {e \in UNION {r.hist : r \in S} : e[1] >= w}}

\* Configuration c of the history of ballot b that only abandoned ballots
\* registered, given the messages M: the owner of each such ballot x
\* promised b (so it never prepares x) and never prepared x.
AbandonedIn(b, c, M) ==
  /\ Matchmaking
  /\ \/ Gone = "any"
     \/ \A r \in answers[b] : \A x \in {en[1] : en \in {en \in r.hist : en[2] = c}} :
          /\ ~\E m \in M : m.type = "1a" /\ m.bal = x
          /\ \E m \in M : m.type = "1b" /\ m.acc = Owner[x] /\ m.bal = b
Abandoned(b, c) == AbandonedIn(b, c, msgs)

\* The acceptors A hold a Phase-1 quorum for ballot b: of every
\* configuration of its history that is not abandoned.
Phase1Quorum(b, A) ==
  IF Phase1 = "own"
  THEN \E Q \in P1Quorums(cfg[b]) : Q \subseteq A
  ELSE \A c \in hist[b] : Abandoned(b, c) \/ \E Q \in P1Quorums(c) : Q \subseteq A

-----------------------------------------------------------------------------
(* Matchmaking *)

\* The owner of ballot b selects its configuration.
Select(b, c) ==
  /\ Matchmaking
  /\ cfg[b] = {}
  /\ cfg' = [cfg EXCEPT ![b] = c]
  /\ UNCHANGED <<logVars, reg, wm, answers, matched, hist, retired, floorVars, repVars, readVars>>

\* Matchmaker m registers ballot b and answers with its registrations below
\* b. It refuses a configuration that names a retired node.
Register(m, b) ==
  /\ cfg[b] # {}
  /\ ~\E r \in answers[b] : r.mm = m
  /\ b >= wm[m]
  /\ MatchCheck => ~\E e \in reg[m] : e[1] >= b
  /\ cfg[b] \cap retired = {}
  /\ reg' = [reg EXCEPT ![m] = @ \cup {<<b, cfg[b]>>}]
  /\ answers' = [answers EXCEPT ![b] = @ \cup
                   {[mm |-> m, hist |-> {e \in reg[m] : e[1] < b}, w |-> wm[m]]}]
  /\ UNCHANGED <<logVars, cfg, wm, matched, hist, retired, floorVars, repVars, readVars>>

-----------------------------------------------------------------------------
(* Phase 1 *)

\* The promise of node a at ballot b from slot f, whole.
PromiseMsg(a, b, f) ==
  [type |-> "1b", acc |-> a, bal |-> b, from |-> f, upto |-> NS,
   rep |-> [s \in Slots |-> IF s >= f THEN vote[a][s] ELSE NoVote]]

\* The owner of ballot b closes its matchmaking on a matchmaker quorum (with
\* matchmakers) and opens Phase 1 from where its campaign starts. It is its
\* own first acceptor.
Prepare(b) ==
  LET o == Owner[b]
      f == From(o)
  IN /\ phase[b] = "idle"
     /\ Live(o)
     /\ prom[o] < b
     /\ IF Matchmaking
        THEN /\ ~matched[b]
             /\ cfg[b] # {}
             /\ \E S \in SUBSET answers[b] :
                  /\ {r.mm : r \in S} \in MQuorums
                  /\ hist' = [hist EXCEPT ![b] = Union(S)]
             /\ matched' = [matched EXCEPT ![b] = TRUE]
        ELSE /\ hist' = [hist EXCEPT ![b] = {Static}]
             /\ UNCHANGED matched
     /\ phase' = [phase EXCEPT ![b] = "p1"]
     /\ prom' = [prom EXCEPT ![o] = b]
     /\ msgs' = msgs \cup {[type |-> "1a", bal |-> b, from |-> f], PromiseMsg(o, b, f)}
     /\ UNCHANGED <<vote, learned, next, front, won, restarts, cfg, reg, wm, answers, retired,
                    floorVars, repVars, readVars>>

\* Node a sends a page of its promise of ballot b, with its records from
\* `lo` up to `hi` (`Limits::promise_page`; one page from `from` to `NS`
\* when promises are not paged). The first page answers the `1a` and
\* raises the promise; a continuation goes on from the end of a page before
\* it, at the same promise. Each answer so far is a 1b of its own. An
\* acceptor may answer again at the ballot it promised (`Acceptor::prepare`
\* re-affirms it). A page from past the first slot of the campaign that
\* goes on from no page answers a keep-alive (`Election::keep_alive_from`):
\* it raises the promise and sends nothing.
Page(a, b, lo, hi) ==
  LET first == IF [type |-> "1a", bal |-> b, from |-> lo] \in msgs /\ (prom[a] < b \/ (Paged /\ prom[a] = b))
               THEN {[type |-> "1b", acc |-> a, bal |-> b, from |-> lo, upto |-> lo,
                      rep |-> [s \in Slots |-> NoVote]]}
               ELSE {}
      cont == {m \in msgs : m.type = "1b" /\ m.acc = a /\ m.bal = b /\ m.upto = lo /\ prom[a] = b}
      Ext(m) == [m EXCEPT !.upto = hi,
                          !.rep = [s \in Slots |-> IF lo <= s /\ s < hi THEN vote[a][s] ELSE @[s]]]
  IN /\ Live(a)
     /\ Matchmaking => a \in UNION hist[b] \cup cfg[b]
     /\ FloorGuard = "nack" => lo >= floor[a]
     /\ lo <= hi /\ hi <= NS
     /\ ~Paged => hi = NS
     /\ \/ \E m \in first \cup cont : msgs' = msgs \cup {Ext(m)}
        \/ /\ Paged
           /\ \E m \in msgs : m.type = "1a" /\ m.bal = b /\ m.from < lo
           /\ prom[a] <= b
           /\ UNCHANGED msgs
     /\ prom' = [prom EXCEPT ![a] = b]
     /\ UNCHANGED <<vote, learned, phase, next, front, won, restarts, mmVars, floorVars,
                    repVars, readVars>>

\* The answers of ballot b a tally may read: one for each acceptor.
PromiseSets(b) ==
  {S \in SUBSET {m \in msgs : m.type = "1b" /\ m.bal = b} :
     \A m, n \in S : m.acc = n.acc => m = n}

\* The acceptors whose answer in S is complete.
Complete(S) == {m.acc : m \in {n \in S : n.upto = NS}}

\* The readable vote of the promises S at slot s that the recovery takes:
\* the highest (the lowest with the control `Pick = "lowest"`).
Top(S, s) ==
  LET rep == {m \in S : Readable(m.rep[s])}
      Above(m, n) == IF Pick = "highest" THEN n.rep[s].bal <= m.rep[s].bal
                     ELSE m.rep[s].bal <= n.rep[s].bal
  IN IF rep = {} THEN NoVote ELSE (CHOOSE m \in rep : \A n \in rep : Above(m, n)).rep[s]

\* The tri-state tally of ballot b at slot s (`slot_decidable`): a complete
\* answer qualifies unless it is a faulty report above the highest readable
\* one, and the qualifying answers decide the slot when they hold a Phase-1
\* quorum.
Decidable(b, S, s) ==
  \/ Tally = "none"
  \/ ~\E m \in S : Faulty(m.rep[s])
  \/ Phase1Quorum(b, Complete({n \in S : ~(Faulty(n.rep[s]) /\ n.rep[s].bal > Top(S, s).bal)}))

\* One past the highest slot that the promises S report, faulty reports
\* too, never below `lo`.
Frontier(S, lo) ==
  LET h == {s \in Slots : s >= lo /\ \E m \in S : m.rep[s].bal > 0}
  IN IF h = {} THEN lo ELSE Max(h) + 1

\* The value ballot b proposes again at slot s: the vote the recovery takes,
\* or a `Noop` (the gap fill).
Refill(S, b, s) ==
  [type |-> "2a", bal |-> b, slot |-> s,
   val |-> IF Top(S, s).bal > 0 THEN Top(S, s).val ELSE Noop]

\* The owner of ballot b wins Phase 1 on the answers S: its own and those
\* of a Phase-1 quorum are complete. It proposes again each slot from the
\* campaign's first slot to the frontier that the tally decides; the others
\* go to the repair probe.
Win(b) ==
  /\ phase[b] = "p1"
  /\ \E S \in PromiseSets(b) :
       LET acc == Complete(S)
           lo == CampaignFrom(b)
           f == Frontier(S, lo)
           open == {s \in lo..(f - 1) : ~Decidable(b, S, s)}
           fill == {s \in (lo..(f - 1)) \ open : Gap = "noop" \/ Top(S, s).bal > 0}
       IN /\ Owner[b] \in acc
          /\ Phase1Quorum(b, acc)
          /\ front' = [front EXCEPT ![b] = f]
          /\ next' = [next EXCEPT ![b] = IF Alloc = "frontier" THEN f ELSE lo]
          /\ msgs' = msgs \cup {Refill(S, b, s) : s \in fill}
  /\ phase' = [phase EXCEPT ![b] = "p2"]
  /\ won' = won \cup {b}
  /\ UNCHANGED <<prom, vote, learned, restarts, mmVars, floorVars, repVars, readVars>>

-----------------------------------------------------------------------------
(* Phase 2 *)

\* The leader of ballot b allocates its next slot to value v: a fresh
\* command, or a `Noop` (`fill_to_watermark` fills the log up to a read's
\* vote watermark).
Allocate(b, s, v) ==
  /\ phase[b] = "p2"
  /\ next[b] = s
  /\ next' = [next EXCEPT ![b] = @ + 1]
  /\ msgs' = msgs \cup {[type |-> "2a", bal |-> b, slot |-> s, val |-> v]}
  /\ UNCHANGED <<prom, vote, learned, phase, front, won, restarts, mmVars, floorVars,
                 repVars, readVars>>

\* The repair probe of ballot b decides slot s below the frontier that the
\* win left open: more answers of the same ballot decide it.
Probe(b, s, v) ==
  /\ phase[b] = "p2"
  /\ CampaignFrom(b) <= s /\ s < front[b]
  /\ ~\E m \in msgs : m.type = "2a" /\ m.bal = b /\ m.slot = s
  /\ \E S \in PromiseSets(b) :
       /\ Phase1Quorum(b, Complete(S))
       /\ Decidable(b, S, s)
       /\ Refill(S, b, s).val = v
       /\ msgs' = msgs \cup {Refill(S, b, s)}
  /\ UNCHANGED <<prom, vote, learned, phase, next, front, won, restarts, mmVars, floorVars,
                 repVars, readVars>>

\* Node a writes the record (b, v) at slot s. A member of `C_b`, or the
\* leader that records its own round (`record_own_round`), votes: the
\* `Accept` is in `msgs`, the promise rises to b and a 2b goes out. A node
\* that `C_b` does not name writes the record only for a value chosen at b
\* or below (`mark_chosen` from a `Commit` of b): no vote, no 2b. A slot
\* below the floor takes no record (`AcceptOutcome::BelowFloor`).
Accept(a, b, s, v) ==
  LET r == [bal |-> b, val |-> v] IN
  /\ Live(a)
  /\ s >= floor[a]
  /\ prom[a] <= b
  /\ vote[a][s] # r
  /\ vote' = [vote EXCEPT ![a][s] = r]
  /\ IF a \in cfg[b] \/ a = Owner[b]
     THEN /\ [type |-> "2a", bal |-> b, slot |-> s, val |-> v] \in msgs
          /\ prom' = [prom EXCEPT ![a] = b]
          /\ msgs' = msgs \cup {[type |-> "2b", acc |-> a, bal |-> b, slot |-> s, val |-> v]}
     ELSE /\ \E c \in 1..b : ChosenAt(c, s, v)
          /\ prom' = [prom EXCEPT ![a] = b]
          /\ UNCHANGED msgs
  /\ UNCHANGED <<learned, phase, next, front, won, restarts, mmVars, floorVars, repVars, readVars>>

-----------------------------------------------------------------------------
(* Learns, records and restarts *)

\* Node n keeps value v as its record of slot s at ballot b, below its
\* promise (`mark_chosen` keeps the ballot of the `Commit` or of the
\* catch-up that taught it the value). The value is chosen at b or below:
\* a later leader proposes the chosen value again at its own ballot, so a
\* node can learn it at that ballot before a quorum votes there. It is no
\* vote: it sends no 2b.
Record(n, b, s, v) ==
  /\ n \in Learners
  /\ Live(n)
  /\ s >= floor[n]
  /\ b < prom[n]
  /\ RecordAt = "chosen" => \E c \in 1..b : ChosenAt(c, s, v)
  /\ vote' = [vote EXCEPT ![n][s] = [bal |-> b, val |-> v]]
  /\ UNCHANGED <<prom, learned, phase, next, front, won, msgs, restarts, mmVars, floorVars,
                 repVars, readVars>>

\* Node n learns that v is chosen at slot s: a `Commit`, a catch-up, or the
\* payload of a handoff taught it. A slot below its floor is not learned.
Learn(n, s, v) ==
  /\ n \in Learners
  /\ Live(n)
  /\ s >= floor[n]
  /\ learned[n][s] = None
  /\ \A t \in Slots : t < s => Holds(n, t)
  /\ Chosen(s, v)
  /\ learned' = [learned EXCEPT ![n][s] = v]
  /\ UNCHANGED <<prom, vote, phase, next, front, won, msgs, restarts, mmVars, floorVars,
                 repVars, readVars>>

\* Node n boots again from its disk. The promise, the records and the floor
\* are durable before any answer. The chosen index is a relaxed write
\* (`MustSync::Relaxed`): a crash may lose a part of the learned prefix, so
\* the node keeps the slots below `upto`, and it held each of them. Its
\* campaigns die, and so does the ballot it led at (`led`, 0 for none); a
\* ballot it handed on lives on at its successor. Its fold has no hole
\* until a boot reads a record faulty again, and its read basis and its
\* open reads end.
Restart(n, upto, led) ==
  /\ Live(n)
  /\ restarts < Restarts
  /\ restarts' = restarts + 1
  /\ \A s \in Slots : s < upto => Holds(n, s)
  /\ learned' = [learned EXCEPT ![n] = [s \in Slots |-> IF s >= upto THEN None ELSE @[s]]]
  /\ phase' = [b \in Ballots |->
                 IF \/ Owner[b] = n /\ phase[b] = "p1"
                    \/ b = led /\ phase[b] = "p2"
                 THEN "dead" ELSE phase[b]]
  /\ rotted' = [rotted EXCEPT ![n] = {}]
  /\ hole' = [hole EXCEPT ![n] = -1]
  /\ rbasis' = [rbasis EXCEPT ![n] = NoBasis]
  /\ rds' = [c \in Ctxs |-> IF rds[c].state = "open" /\ rds[c].node = n
                           THEN [rds[c] EXCEPT !.state = "ended"] ELSE rds[c]]
  /\ UNCHANGED <<prom, vote, next, front, won, msgs, mmVars, floor, fold, faults, repVars,
                 racks, acked>>

-----------------------------------------------------------------------------
(* GC and retirement *)

\* The leader of ballot b asks the matchmakers to forget the ballots below
\* b (`node/gc.rs`): it won, and a Phase-2 quorum of `C_b` holds every slot
\* below the frontier of its Phase 1.
GcAsk(b) ==
  /\ Matchmaking
  /\ phase[b] = "p2"
  /\ Gc = "learned" =>
       \E Q \in P2Quorums(cfg[b]) : \A a \in Q, s \in Slots : s < front[b] => Holds(a, s)
  /\ [type |-> "ga", bal |-> b] \notin msgs
  /\ msgs' = msgs \cup {[type |-> "ga", bal |-> b]}
  /\ UNCHANGED <<prom, vote, learned, phase, next, front, won, restarts, mmVars, floorVars,
                 repVars, readVars>>

\* Matchmaker m raises its watermark to b, forgets what is below, and acks.
GcAck(m, b) ==
  /\ [type |-> "ga", bal |-> b] \in msgs
  /\ [type |-> "gb", mm |-> m, bal |-> b] \notin msgs
  /\ LET w == IF b > wm[m] THEN b ELSE wm[m]
     IN /\ wm' = [wm EXCEPT ![m] = w]
        /\ reg' = [reg EXCEPT ![m] = {e \in @ : e[1] >= w}]
  /\ msgs' = msgs \cup {[type |-> "gb", mm |-> m, bal |-> b]}
  /\ UNCHANGED <<prom, vote, learned, phase, next, front, won, restarts, cfg, answers, matched,
                 hist, retired, floorVars, repVars, readVars>>

\* The floors that a matchmaker quorum acked (one ack with the control).
Effective ==
  {f \in Ballots :
     IF Retire = "oneAck"
     THEN \E m \in Matchmakers : [type |-> "gb", mm |-> m, bal |-> f] \in msgs
     ELSE \E Q \in MQuorums : \A m \in Q : [type |-> "gb", mm |-> m, bal |-> f] \in msgs}

\* No registration at f or above names node a.
Unnamed(a, f) == \A m \in Matchmakers : \A e \in reg[m] : e[1] >= f => a \notin e[2]

\* The operator retires node a on an effective floor that no registration
\* at or above names (`ColocatedNode::may_retire`, `may_retire_heard`); it
\* shuts down and takes no step.
Shutdown(a) ==
  /\ Matchmaking
  /\ Live(a)
  /\ \E f \in Effective : Unnamed(a, f)
  /\ retired' = retired \cup {a}
  /\ UNCHANGED <<logVars, cfg, reg, wm, answers, matched, hist, floorVars, repVars, readVars>>

\* The operator books the retired node a again (`MatchPurpose::Rejoin`).
\* It boots from its disk at its next incarnation: it keeps its promise and
\* its records, and a configuration may name it again.
Rejoin(a) ==
  /\ a \in retired
  /\ retired' = retired \ {a}
  /\ UNCHANGED <<logVars, cfg, reg, wm, answers, matched, hist, floorVars, repVars, readVars>>

\* A proxy leader commits value v at slot s of a round of ballot b
\* (`proxy_leader.rs`): a Phase-2 quorum of `C_b` voted for it at b.
ProxyCommit(b, s, v) ==
  /\ Proxies
  /\ Commit = "backed" => ChosenAt(b, s, v)
  /\ msgs' = msgs \cup {[type |-> "commit", bal |-> b, slot |-> s, val |-> v]}
  /\ UNCHANGED <<prom, vote, learned, phase, next, front, won, restarts, mmVars, floorVars,
                 repVars, readVars>>

-----------------------------------------------------------------------------
(* The compaction floor *)

F(s) == fold[s + 1]

\* The fold of slot s by the first party that applied it: the end of its
\* records (`high`, 0 for none) and `first_seq` after it, which never goes
\* down. The fold is an input of this model: the replica's journal fold.
Fold(s, high, first) ==
  /\ Floors
  /\ s = Len(fold)
  /\ IsChosen(s)
  /\ first >= IF fold = <<>> THEN 0 ELSE F(Len(fold) - 1).first
  /\ fold' = Append(fold, [high |-> high, first |-> first])
  /\ UNCHANGED <<logVars, mmVars, floor, rotted, hole, faults, repVars, readVars>>

\* The fold released every record below floor f once node n folded each
\* slot it holds from 0 up to some t: they are below `first_seq` after t.
Released(n, f) ==
  \/ Release = "none"
  \/ /\ f <= Len(fold)
     /\ \E t \in Slots :
          /\ t < Len(fold)
          /\ \A u \in 0..t : Holds(n, u)
          /\ \A s \in 0..(f - 1) : F(s).high <= F(t).first

\* Node n moves its floor to f: it drops its records and learned values
\* below f, and the floor reaches the disk in the same write.
Raise(n, f) ==
  /\ floor' = [floor EXCEPT ![n] = f]
  /\ vote' = [vote EXCEPT ![n] = [s \in Slots |-> IF s < f THEN NoVote ELSE @[s]]]
  /\ learned' = [learned EXCEPT ![n] = [s \in Slots |-> IF s < f THEN None ELSE @[s]]]
  /\ UNCHANGED <<prom, phase, next, front, won, msgs, restarts, mmVars, fold, rotted, hole,
                 faults, repVars, readVars>>

\* Node n compacts below f (`ColocatedNode::compact`).
Compact(n, f) ==
  /\ Floors
  /\ Live(n)
  /\ f > floor[n] /\ f <= NS
  /\ CompactWhen = "prefix" => \A s \in Slots : s < f => Holds(n, s)
  /\ Released(n, f)
  /\ Raise(n, f)

\* Node n jumps to the trim point f of a peer (`TrimmedTo`): some peer holds
\* a floor at least as high. The jump carries no ballot.
Trim(n, f) ==
  /\ Floors
  /\ Live(n)
  /\ f > floor[n] /\ f <= NS
  /\ TrimPoint = "floor" => \E p \in Acceptors : floor[p] >= f
  /\ Raise(n, f)

\* A boot of node n reads its record of slot s faulty (CTRL): the record
\* keeps its ballot b and loses its value. The node keeps its chosen index
\* over the slot; its fold stops at the hole until a peer brings the value
\* back.
Rot(n, s, b) ==
  /\ Live(n)
  /\ faults < Faults
  /\ b > 0 /\ vote[n][s].bal = b
  /\ faults' = faults + 1
  /\ vote' = [vote EXCEPT ![n][s] = [bal |-> b, val |-> Lost]]
  /\ rotted' = [rotted EXCEPT ![n] = IF learned[n][s] # None THEN @ \cup {s} ELSE @]
  /\ UNCHANGED <<prom, learned, phase, next, front, won, msgs, restarts, mmVars, floor, fold,
                 hole, repVars, readVars>>

\* The fold of node n stops at slot s, a slot of its prefix that a boot read
\* faulty, or runs to the end (s = -1).
Hole(n, s) ==
  /\ \/ s = -1
     \/ s \in rotted[n] /\ s < FirstUnheld(n)
  /\ hole' = [hole EXCEPT ![n] = s]
  /\ UNCHANGED <<logVars, mmVars, floor, fold, rotted, faults, repVars, readVars>>

-----------------------------------------------------------------------------
(* The read replica *)

\* Replica r holds slot s: it learned it, or the slot is below its floor.
RHolds(r, s) == s < rfloor[r] \/ rlearned[r][s] # None

\* Replica r holds the value chosen at slot s at ballot b
\* (`WriteOp::Learned`). It never votes and counts in no quorum.
RLearn(r, b, s, v) ==
  /\ ChosenAt(b, s, v)
  /\ s >= rfloor[r]
  /\ rlearned' = [rlearned EXCEPT ![r][s] = v]
  /\ UNCHANGED <<logVars, mmVars, floorVars, rnext, rfloor, rrds, readVars>>

\* Replica r folds slot s: its walk is in slot order, on slots it holds.
RApply(r, s) ==
  /\ s = rnext[r]
  /\ RHolds(r, s)
  /\ rnext' = [rnext EXCEPT ![r] = s + 1]
  /\ UNCHANGED <<logVars, mmVars, floorVars, rlearned, rfloor, rrds, readVars>>

\* Replica r moves its floor to f, and its walk to f when it was below.
RRaise(r, f) ==
  /\ f > rfloor[r] /\ f <= NS
  /\ \A s \in Slots : s < f => IsChosen(s)
  /\ rfloor' = [rfloor EXCEPT ![r] = f]
  /\ rlearned' = [rlearned EXCEPT ![r] = [s \in Slots |-> IF s < f THEN None ELSE @[s]]]
  /\ rnext' = [rnext EXCEPT ![r] = IF @ < f THEN f ELSE @]
  /\ UNCHANGED <<logVars, mmVars, floorVars, rrds, readVars>>

\* Replica r compacts below a slot its walk folded.
RCompact(r, f) == f <= rnext[r] /\ RRaise(r, f)

\* Replica r jumps to a trim point.
RTrim(r, f) == RRaise(r, f)

\* Replica r boots again and folds its prefix again from its disk: its walk
\* is at `from`, it holds every slot below, and its open reads end.
RBoot(r, from) ==
  /\ from <= NS
  /\ \A s \in Slots : s < from => RHolds(r, s)
  /\ rnext' = [rnext EXCEPT ![r] = from]
  /\ rrds' = [c \in Ctxs |-> IF rrds[c].state = "open" /\ rrds[c].rep = r
                            THEN [rrds[c] EXCEPT !.state = "ended"] ELSE rrds[c]]
  /\ UNCHANGED <<logVars, mmVars, floorVars, rlearned, rfloor, readVars>>

\* Replica r opens read c (`ReplicaNode::quorum_read_in`), and remembers
\* the writes that completed before it.
RRead(r, c) ==
  /\ rrds[c].state = "none" /\ rds[c].state = "none"
  /\ rrds' = [rrds EXCEPT ![c] = [state |-> "open", rep |-> r, snap |-> acked, index |-> -1]]
  /\ UNCHANGED <<logVars, mmVars, floorVars, rlearned, rnext, rfloor, readVars>>

\* Replica r serves read c at index i: the answers of a Phase-1 quorum of
\* the configuration are at or below i, and the replica folded every slot
\* up to i.
RServe(r, c, i) ==
  LET q == rrds[c] IN
  /\ q.state = "open" /\ q.rep = r
  /\ IF RepRead = "quorum"
     THEN \E Q \in P1Quorums(Static) :
            \A a \in Q : \E k \in racks : k.acc = a /\ k.ctx = c /\ k.wm <= i
     ELSE \E k \in racks : k.ctx = c /\ k.wm <= i
  /\ i < rnext[r]
  /\ rrds' = [rrds EXCEPT ![c].state = "done", ![c].index = i]
  /\ UNCHANGED <<logVars, mmVars, floorVars, rlearned, rnext, rfloor, readVars>>

-----------------------------------------------------------------------------
(* The leaderless reads *)

\* The highest slot at which node a holds a record (-1: none).
TopVote(a) == Max({s \in Slots : vote[a][s].bal > 0} \cup {-1})

\* The vote watermark of node a (`Acceptor::vote_watermark`): its last
\* record, or its floor minus 1 when it holds no record above its floor.
VoteWatermark(a) ==
  IF Watermark = "floor" /\ floor[a] - 1 > TopVote(a) THEN floor[a] - 1 ELSE TopVote(a)

\* Acceptor a answers read c with its vote watermark and its read fence,
\* at or above its promise (`PreReadAck`).
ReadAck(a, c, fence) ==
  /\ Fence = "promise" => fence >= prom[a]
  /\ racks' = racks \cup {[acc |-> a, ctx |-> c, wm |-> VoteWatermark(a), fence |-> fence]}
  /\ UNCHANGED <<logVars, mmVars, floorVars, repVars, rbasis, rds, acked>>

\* Node n learns the read basis of the leadership that won ballot b, with
\* its fence: one below the frontier of its Phase 1. Its promise is not
\* above b, and its basis only moves forward.
Basis(n, b) ==
  /\ Reads
  /\ b \in won
  /\ prom[n] <= b
  /\ rbasis[n].bal <= b
  /\ rbasis' = [rbasis EXCEPT ![n] = [bal |-> b, fence |-> front[b] - 1]]
  /\ UNCHANGED <<logVars, mmVars, floorVars, repVars, racks, rds, acked>>

\* Node n opens read c on its basis. Its promise is not above the basis. A
\* reader of the configuration counts its own watermark at once.
ReadStart(n, c) ==
  LET b == rbasis[n].bal IN
  /\ rds[c].state = "none" /\ rrds[c].state = "none"
  /\ Live(n)
  /\ b > 0
  /\ prom[n] <= b
  /\ rds' = [rds EXCEPT ![c] =
               [state |-> "open", node |-> n, b |-> b, fence |-> rbasis[n].fence, snap |-> acked,
                answers |-> IF n \in cfg[b] THEN {[acc |-> n, wm |-> VoteWatermark(n)]} ELSE {},
                index |-> -1]]
  /\ UNCHANGED <<logVars, mmVars, floorVars, repVars, rbasis, racks, acked>>

\* The answers of read q hold a Phase-1 quorum of its configuration.
Whole(q) == \E Q \in P1Quorums(cfg[q.b]) : Q \subseteq {x.acc : x \in q.answers}

\* Node n folds the answer of acceptor a to its read c. An answer whose
\* fence is above the basis ends the read; any other counts while the
\* answers hold no Phase-1 quorum yet.
ReadFold(n, a, c) ==
  LET q == rds[c] IN
  /\ q.state = "open" /\ q.node = n
  /\ a \in cfg[q.b]
  /\ \E k \in racks :
       /\ k.acc = a /\ k.ctx = c
       /\ IF k.fence > q.b
          THEN rds' = [rds EXCEPT ![c].state = "ended"]
          ELSE /\ ~Whole(q)
               /\ ~\E x \in q.answers : x.acc = a
               /\ rds' = [rds EXCEPT ![c].answers = @ \cup {[acc |-> a, wm |-> k.wm]}]
  /\ UNCHANGED <<logVars, mmVars, floorVars, repVars, rbasis, racks, acked>>

\* Node n serves read c: the answers hold a Phase-1 quorum, its promise is
\* still not above the basis, and it holds every slot up to the index, the
\* maximum of the watermarks and the fence.
ReadServe(n, c) ==
  LET q == rds[c]
      i == Max({x.wm : x \in q.answers} \cup {q.fence})
  IN /\ q.state = "open" /\ q.node = n
     /\ prom[n] <= q.b
     /\ Whole(q)
     /\ \A s \in Slots : s <= i => Holds(n, s)
     /\ rds' = [rds EXCEPT ![c].state = "done", ![c].index = i]
     /\ UNCHANGED <<logVars, mmVars, floorVars, repVars, rbasis, racks, acked>>

\* A party applied every slot up to s: those writes completed. Some node or
\* replica holds each of them.
Applied(s) ==
  /\ Reads
  /\ s > acked
  /\ \A u \in 0..s : (\E n \in Acceptors : Holds(n, u)) \/ \E r \in Replicas : RHolds(r, u)
  /\ acked' = s
  /\ UNCHANGED <<logVars, mmVars, floorVars, repVars, rbasis, racks, rds>>

-----------------------------------------------------------------------------

Next ==
  \/ \E b \in Ballots, c \in Configs : Select(b, c)
  \/ \E m \in Matchmakers, b \in Ballots : Register(m, b) \/ GcAck(m, b)
  \/ \E b \in Ballots : Prepare(b) \/ Win(b) \/ GcAsk(b)
  \/ \E a \in Acceptors, b \in Ballots, lo \in 0..NS, hi \in 0..NS : Page(a, b, lo, hi)
  \/ \E b \in Ballots, s \in Slots, v \in Values :
       Allocate(b, s, v) \/ Probe(b, s, v) \/ ProxyCommit(b, s, v)
  \/ \E a \in Acceptors, b \in Ballots, s \in Slots, v \in Values : Accept(a, b, s, v) \/ Record(a, b, s, v)
  \/ \E a \in Acceptors, s \in Slots, v \in Values : Learn(a, s, v)
  \/ \E a \in Acceptors, upto \in 0..NS, led \in 0..NB : Restart(a, upto, led)
  \/ \E a \in Acceptors : Shutdown(a) \/ Rejoin(a)
  \/ \E s \in Slots, high, first \in 0..MaxPos : Fold(s, high, first)
  \/ \E a \in Acceptors, f \in 1..NS : Compact(a, f) \/ Trim(a, f)
  \/ \E a \in Acceptors, s \in Slots, b \in Ballots : Rot(a, s, b)
  \/ \E a \in Acceptors, s \in -1..(NS - 1) : Hole(a, s)
  \/ \E r \in Replicas, b \in Ballots, s \in Slots, v \in Values : RLearn(r, b, s, v)
  \/ \E r \in Replicas, s \in Slots : RApply(r, s)
  \/ \E r \in Replicas, f \in 1..NS : RCompact(r, f) \/ RTrim(r, f)
  \/ \E r \in Replicas, from \in 0..NS : RBoot(r, from)
  \/ \E r \in Replicas, c \in Ctxs : RRead(r, c)
  \/ \E r \in Replicas, c \in Ctxs, i \in -1..(NS - 1) : RServe(r, c, i)
  \/ \E a \in Acceptors, c \in Ctxs, fence \in 0..NB : ReadAck(a, c, fence)
  \/ \E n \in Acceptors, b \in Ballots : Basis(n, b)
  \/ \E n \in Acceptors, c \in Ctxs : ReadStart(n, c) \/ ReadServe(n, c)
  \/ \E n, a \in Acceptors, c \in Ctxs : ReadFold(n, a, c)
  \/ \E s \in Slots : Applied(s)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Safety *)

\* One value for each slot.
Agreement == \A s \in Slots, v, w \in Values : Chosen(s, v) /\ Chosen(s, w) => v = w

\* What a node learned is chosen.
LearnedChosen == \A n \in Acceptors, s \in Slots : learned[n][s] # None => Chosen(s, learned[n][s])

\* One proposal for each slot and ballot.
OneProposal ==
  \A m, n \in msgs : m.type = "2a" /\ n.type = "2a" /\ m.bal = n.bal /\ m.slot = n.slot => m = n

\* Each slot below a floor is chosen.
FloorChosen == \A a \in Acceptors, s \in Slots : s < floor[a] => IsChosen(s)

\* Each floor is released by the fold: the records below it are below
\* `first_seq` after some folded slot.
FloorReleased ==
  \A a \in Acceptors :
    floor[a] > 0 =>
      /\ floor[a] <= Len(fold)
      /\ \E t \in Slots : t < Len(fold) /\ \A s \in 0..(floor[a] - 1) : F(s).high <= F(t).first

\* No fill of a chosen slot: a leader never proposes a `Noop` at a slot
\* where a lower ballot chose a command.
NoFillChosen ==
  \A m \in msgs :
    (m.type = "2a" /\ m.val = Noop) =>
      ~\E b \in 1..(m.bal - 1), v \in Commands : ChosenAt(b, m.slot, v)

\* A read of a node or of a replica covers each write that completed before
\* it started.
ReadSeesAcked ==
  /\ \A c \in Ctxs : rds[c].state = "done" => rds[c].snap <= rds[c].index
  /\ \A c \in Ctxs : rrds[c].state = "done" => rrds[c].snap <= rrds[c].index

\* A retired node is named by no registration at an effective floor or
\* above.
RetiredUnnamed == \A a \in retired : \E f \in Effective : Unnamed(a, f)

\* A proxy commits only a value that is chosen at the round's ballot.
CommitBacked ==
  \A m \in msgs : m.type = "commit" => ChosenAt(m.bal, m.slot, m.val)

\* What a replica holds is chosen.
ReplicaChosen ==
  \A r \in Replicas, s \in Slots : rlearned[r][s] # None => Chosen(s, rlearned[r][s])

\* Reachability controls: each must be violated.
NoChoice == ~\E s \in Slots, v \in Values : Chosen(s, v)
NoCompaction == \A a \in Acceptors : floor[a] = 0
NoRecordAbove == ~\E n \in Acceptors, s \in Slots :
                   /\ vote[n][s].bal > 0
                   /\ ~ChosenAt(vote[n][s].bal, s, vote[n][s].val)
                   /\ \A m \in msgs : ~(m.type = "2b" /\ m.acc = n /\ m.slot = s /\ m.bal = vote[n][s].bal)
NoServe == \A c \in Ctxs : rds[c].state # "done" /\ rrds[c].state # "done"
NoRetire == retired = {}
NoBlockedFill == ~\E s \in Slots, n \in Acceptors : Faulty(vote[n][s]) /\ IsChosen(s)
=============================================================================
