------------------------------- MODULE CtrlGrid -------------------------------
(***************************************************************************)
(* CTRL recovery in the journal, part 1: the tri-state tally on a grid and *)
(* across the configurations of H_b, and the repair of a faulty record     *)
(* that a scrub or a boot finds (#1263 (CTRL recovery in the journal),     *)
(* items 1, 3 and 5).                                                      *)
(*                                                                         *)
(* `truncation/` checks CTRL on plain Multi-Paxos on a majority. The       *)
(* journal adds three things that it does not model:                       *)
(*   - Grid quorums (`QuorumSystem::Grid { rows, cols, acks }`): a write   *)
(*     needs `acks` acceptors of one column, Phase 1 needs                 *)
(*     `rows - acks + 1` acceptors of every column (`grid/`).              *)
(*   - Several configurations: a ballot is bound to one configuration      *)
(*     (`Cfg`), and until GC a candidate needs a Phase-1 quorum of every   *)
(*     configuration of its history H_b (Matchmaker Paxos, `matchmaker/`). *)
(*     A sealed segment is an older configuration of H_b, read below its   *)
(*     end (02-data-plane.md section 2.10).                                *)
(*   - The scrub of the grid controller: it finds a damaged record while   *)
(*     the acceptor runs, and the acceptor repairs it from a copy of the   *)
(*     chosen value. The journal keeps no record in memory, so a scrub     *)
(*     cannot rewrite the record from the acceptor's own memory, as the    *)
(*     old parosd scrubber did (05-failure-model-and-zones.md).            *)
(*                                                                         *)
(* The log, Phase 1, Phase 2, the gap fill and the repair probe are the    *)
(* ones of `truncation/`, with no floor and no truncation: a campaign asks *)
(* from slot 0. A message is never lost from `msgs`, so a message can      *)
(* arrive late, two times, or never. Each ballot has one owner.            *)
(*                                                                         *)
(* The tri-state tally (`slot_decidable` in `proposer.rs`): at a slot with *)
(* a faulty report, the threshold is the highest readable report. An      *)
(* answer qualifies unless it is a faulty report above the threshold. The *)
(* slot is decided only when the qualifying answers hold a Phase-1 quorum *)
(* of every configuration of H_b (`config.has_phase1_quorum` for each     *)
(* `prior` configuration); else the repair probe decides it later on more *)
(* promises of the same ballot, or drops it once the leader learned it.   *)
(*                                                                         *)
(* Constants (the first value of each is the design; each other value is   *)
(* a control):                                                             *)
(*   Tally       "ctrl": the tri-state tally above.                        *)
(*               "majority": the qualifying answers must hold a majority   *)
(*               of the members of each configuration, not a Phase-1       *)
(*               quorum of its grid (#1263 item 1).                        *)
(*               "plus_one": the threshold is one above the highest        *)
(*               readable report (#1263 item 5).                           *)
(*               "none": a faulty report counts as no vote.                *)
(*   Scope       "every": the tally asks every configuration of H_b.       *)
(*               "current": it asks the configuration of the ballot only.  *)
(*   Damage      "faulty": a damaged record keeps its ballot and loses its *)
(*               value, whether a scrub or a boot finds it.                *)
(*               "drop": the damaged record is dropped, and the slot reads *)
(*               as a slot with no vote (a torn tail).                     *)
(*   Repair      "chosen": the acceptor writes the record again with the   *)
(*               value that a node learned (a chosen value), under its own *)
(*               ballot. "peer": it writes the value of a peer's readable  *)
(*               vote under its own ballot. "none": no repair; only a      *)
(*               later `Accept` heals the record (`record_accepted`).      *)
(*   RepairWrite "atomic": the new record replaces the damaged one in one  *)
(*               write. "drop_first": the store first deletes the damaged  *)
(*               record, then writes the new one; the acceptor answers     *)
(*               nothing between the two, but a restart between the two    *)
(*               forgets that a repair was running.                        *)
(*   Faults      how many damaged records may be found (in the whole run), *)
(*               on the nodes `RotNodes` only.                             *)
(*   Restarts    how many restarts may happen (in the whole run).          *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS Acceptors, Commands, Noop, Lost, NB, NS, Owner, Cfg, Configs,
          Members, Q1, Q2, Tally, Scope, Damage, Repair, RepairWrite,
          Faults, RotNodes, Restarts

Ballots == 1..NB
Slots == 0..(NS - 1)
Values == Commands \cup {Noop}
None == "none"

ASSUME Tally \in {"ctrl", "majority", "plus_one", "none"}
ASSUME Scope \in {"every", "current"}
ASSUME Damage \in {"faulty", "drop"}
ASSUME Repair \in {"chosen", "peer", "none"}
ASSUME RepairWrite \in {"atomic", "drop_first"}
ASSUME Owner \in [Ballots -> Acceptors]
ASSUME Cfg \in [Ballots -> Configs]
ASSUME \A b \in Ballots : Owner[b] \in Members[Cfg[b]]
\* A configuration never comes back after a later one (a reconfiguration is
\* a round change to a higher ballot).
ASSUME \A x, y, z \in Ballots : x < y /\ y < z /\ Cfg[x] = Cfg[z] => Cfg[y] = Cfg[x]
ASSUME RotNodes \subseteq Acceptors /\ Noop \notin Commands /\ Lost \notin Values

Vote == [bal : 0..NB, val : Values \cup {None, Lost}]
NoVote == [bal |-> 0, val |-> None]

VARIABLES prom,      \* acceptor: the promise (on disk before it answers)
          vote,      \* acceptor: the record of each slot, as the disk holds it
          repairing, \* acceptor, in memory: the ballot of each slot whose
                     \* record a "drop_first" repair deleted and has not
                     \* written again (0: none)
          learned,   \* node: the value it learned, per slot
          phase,     \* ballot: "idle", "p1", "p2" or "dead"
          next,      \* ballot: the allocator's next slot, once in Phase 2
          blocked,   \* ballot: the slots its repair probe still decides
          faults,    \* how many damaged records were found
          restarts,  \* how many restarts happened
          fixed,     \* ghost: how many repairs wrote a record again
          msgs       \* every message sent: the ghost of every vote

vars == <<prom, vote, repairing, learned, phase, next, blocked, faults, restarts, fixed, msgs>>

Msg ==
  [type : {"1a"}, bal : Ballots]
    \cup [type : {"1b"}, acc : Acceptors, bal : Ballots, rep : [Slots -> Vote]]
    \cup [type : {"2a"}, bal : Ballots, slot : Slots, val : Values]
    \cup [type : {"2b"}, acc : Acceptors, bal : Ballots, slot : Slots, val : Values]

TypeOK ==
  /\ prom \in [Acceptors -> 0..NB]
  /\ vote \in [Acceptors -> [Slots -> Vote]]
  /\ repairing \in [Acceptors -> [Slots -> 0..NB]]
  /\ learned \in [Acceptors -> [Slots -> Values \cup {None}]]
  /\ phase \in [Ballots -> {"idle", "p1", "p2", "dead"}]
  /\ next \in [Ballots -> 0..NS]
  /\ blocked \in [Ballots -> SUBSET Slots]
  /\ faults \in Nat
  /\ restarts \in Nat
  /\ fixed \in Nat
  /\ msgs \subseteq Msg

Init ==
  /\ prom = [a \in Acceptors |-> 0]
  /\ vote = [a \in Acceptors |-> [s \in Slots |-> NoVote]]
  /\ repairing = [a \in Acceptors |-> [s \in Slots |-> 0]]
  /\ learned = [a \in Acceptors |-> [s \in Slots |-> None]]
  /\ phase = [b \in Ballots |-> "idle"]
  /\ next = [b \in Ballots |-> 0]
  /\ blocked = [b \in Ballots |-> {}]
  /\ faults = 0
  /\ restarts = 0
  /\ fixed = 0
  /\ msgs = {}

Send(m) == msgs' = msgs \cup {m}
SendAll(M) == msgs' = msgs \cup M

Max(S) == CHOOSE x \in S : \A y \in S : y <= x

Readable(v) == v.bal > 0 /\ v.val \notin {Lost, None}
Faulty(v) == v.bal > 0 /\ v.val = Lost

\* An acceptor in the middle of a "drop_first" repair answers nothing.
Busy(a) == \E s \in Slots : repairing[a][s] # 0

\* The history of ballot b: the configurations of every ballot up to b (no
\* GC in this model, so a sealed configuration stays in H_b).
Hist(b) == {Cfg[x] : x \in 1..b}

\* The ballots of node a die when it boots again.
KillBallots(a) ==
  [b \in Ballots |-> IF Owner[b] = a /\ phase[b] \in {"p1", "p2"} THEN "dead" ELSE phase[b]]

-----------------------------------------------------------------------------
(* The chosen log: a value is chosen at slot s once a Phase-2 quorum of    *)
(* the configuration of a ballot voted for it at that ballot. A vote is a  *)
(* 2b message, so a damaged record does not unchoose it.                   *)

ChosenAt(b, s, v) ==
  \E Q \in Q2[Cfg[b]] :
    \A a \in Q : [type |-> "2b", acc |-> a, bal |-> b, slot |-> s, val |-> v] \in msgs
Chosen(s, v) == \E b \in Ballots : ChosenAt(b, s, v)
IsChosen(s) == \E v \in Values : Chosen(s, v)

-----------------------------------------------------------------------------
(* Phase 1 *)

PromiseMsg(a, b) == [type |-> "1b", acc |-> a, bal |-> b, rep |-> vote[a]]

\* The owner of ballot b opens its campaign. It is its own first acceptor.
Prepare(b) ==
  LET o == Owner[b]
  IN /\ phase[b] = "idle"
     /\ prom[o] < b
     /\ ~Busy(o)
     /\ phase' = [phase EXCEPT ![b] = "p1"]
     /\ prom' = [prom EXCEPT ![o] = b]
     /\ SendAll({[type |-> "1a", bal |-> b], PromiseMsg(o, b)})
     /\ UNCHANGED <<vote, repairing, learned, next, blocked, faults, restarts, fixed>>

\* Acceptor a promises ballot b. A promise at its own ballot answers again
\* for the repair probe of a leader with a blocked slot (`Acceptor::prepare`
\* re-affirms it), when the acceptor took an `Accept` of the ballot first.
Promise(a, b) ==
  /\ ~Busy(a)
  /\ [type |-> "1a", bal |-> b] \in msgs
  /\ \/ prom[a] < b
     \/ /\ prom[a] = b
        /\ blocked[b] # {}
        /\ ~\E n \in msgs : n.type = "1b" /\ n.acc = a /\ n.bal = b /\ n.rep = vote[a]
  /\ prom' = [prom EXCEPT ![a] = b]
  /\ Send(PromiseMsg(a, b))
  /\ UNCHANGED <<vote, repairing, learned, phase, next, blocked, faults, restarts, fixed>>

\* The promises of ballot b: one for each acceptor at most.
PromiseSets(b) ==
  {S \in SUBSET {m \in msgs : m.type = "1b" /\ m.bal = b} :
     \A m, n \in S : m.acc = n.acc => m = n}

Accs(S) == {m.acc : m \in S}

\* A Phase-1 quorum of every configuration of H_b.
CoversHist(b, A) == \A c \in Hist(b) : \E Q \in Q1[c] : Q \subseteq A

\* The highest readable vote that the promises S report at slot s.
Top(S, s) ==
  LET rep == {m \in S : Readable(m.rep[s])}
  IN IF rep = {} THEN NoVote
     ELSE (CHOOSE m \in rep : \A n \in rep : n.rep[s].bal <= m.rep[s].bal).rep[s]

\* The threshold of the tally at slot s.
Threshold(S, s) == IF Tally = "plus_one" THEN Top(S, s).bal + 1 ELSE Top(S, s).bal

\* The answers that qualify at slot s: every answer but a faulty report
\* above the threshold.
Qualifying(S, s) ==
  Accs({m \in S : ~(Faulty(m.rep[s]) /\ m.rep[s].bal > Threshold(S, s))})

\* The configurations that the tally asks.
TallyConfigs(b) == IF Scope = "every" THEN Hist(b) ELSE {Cfg[b]}

\* The sets of qualifying answers that decide a slot in configuration c.
TallyQuorums(c) ==
  IF Tally = "majority"
  THEN {Q \in SUBSET Members[c] : 2 * Cardinality(Q) > Cardinality(Members[c])}
  ELSE Q1[c]

\* The tri-state tally at slot s: may the promises S of ballot b decide it?
Decidable(S, b, s) ==
  \/ Tally = "none"
  \/ ~\E m \in S : Faulty(m.rep[s])
  \/ \A c \in TallyConfigs(b) : \E Q \in TallyQuorums(c) : Q \subseteq Qualifying(S, s)

\* The frontier: one past the highest slot that the promises S report,
\* faulty reports too.
Frontier(S) ==
  LET h == {s \in Slots : \E m \in S : m.rep[s].bal > 0}
  IN IF h = {} THEN 0 ELSE Max(h) + 1

\* The value ballot b proposes again at slot s: the highest readable report,
\* or a `Noop` when there is none (the gap fill).
Refill(S, b, s) ==
  [type |-> "2a", bal |-> b, slot |-> s,
   val |-> IF Top(S, s).bal > 0 THEN Top(S, s).val ELSE Noop]

\* The owner of ballot b wins Phase 1 on the promises S, its own among
\* them, with a Phase-1 quorum of every configuration of H_b. It proposes
\* again each slot that the tally decides; the others go to the probe.
Win(b) ==
  /\ phase[b] = "p1"
  /\ \E S \in PromiseSets(b) :
       /\ Owner[b] \in Accs(S)
       /\ CoversHist(b, Accs(S))
       /\ LET f == Frontier(S)
              open == {s \in 0..(f - 1) : ~Decidable(S, b, s)}
          IN /\ next' = [next EXCEPT ![b] = f]
             /\ blocked' = [blocked EXCEPT ![b] = open]
             /\ SendAll({Refill(S, b, s) : s \in (0..(f - 1)) \ open})
  /\ phase' = [phase EXCEPT ![b] = "p2"]
  /\ UNCHANGED <<prom, vote, repairing, learned, faults, restarts, fixed>>

\* The repair probe of ballot b settles a blocked slot s: more promises of
\* the same ballot arrived and decide it, or the leader learned it.
Probe(b, s) ==
  /\ phase[b] = "p2"
  /\ s \in blocked[b]
  /\ blocked' = [blocked EXCEPT ![b] = @ \ {s}]
  /\ \/ /\ learned[Owner[b]][s] # None
        /\ UNCHANGED msgs
     \/ \E S \in PromiseSets(b) :
          /\ CoversHist(b, Accs(S))
          /\ Decidable(S, b, s)
          /\ Send(Refill(S, b, s))
  /\ UNCHANGED <<prom, vote, repairing, learned, phase, next, faults, restarts, fixed>>

-----------------------------------------------------------------------------
(* Phase 2 and commit *)

Allocate(b, c) ==
  /\ phase[b] = "p2"
  /\ next[b] < NS
  /\ next' = [next EXCEPT ![b] = @ + 1]
  /\ Send([type |-> "2a", bal |-> b, slot |-> next[b], val |-> c])
  /\ UNCHANGED <<prom, vote, repairing, learned, phase, blocked, faults, restarts, fixed>>

\* Acceptor a, a member of the configuration of ballot b, votes. A vote over
\* a faulty record heals it.
Accept(a, b, s) ==
  /\ a \in Members[Cfg[b]]
  /\ ~Busy(a)
  /\ prom[a] <= b
  /\ \E m \in msgs :
       /\ m.type = "2a" /\ m.bal = b /\ m.slot = s
       /\ vote[a][s] # [bal |-> b, val |-> m.val]
       /\ prom' = [prom EXCEPT ![a] = b]
       /\ vote' = [vote EXCEPT ![a][s] = [bal |-> b, val |-> m.val]]
       /\ Send([type |-> "2b", acc |-> a, bal |-> b, slot |-> s, val |-> m.val])
  /\ UNCHANGED <<repairing, learned, phase, next, blocked, faults, restarts, fixed>>

\* The leader of ballot b learns slot s at a Phase-2 quorum of its
\* configuration.
Decide(b, s) ==
  LET o == Owner[b]
  IN /\ phase[b] = "p2"
     /\ learned[o][s] = None
     /\ \E v \in Values, Q \in Q2[Cfg[b]] :
          /\ \A a \in Q : [type |-> "2b", acc |-> a, bal |-> b, slot |-> s, val |-> v] \in msgs
          /\ learned' = [learned EXCEPT ![o][s] = v]
     /\ UNCHANGED <<prom, vote, repairing, phase, next, blocked, faults, restarts, fixed, msgs>>

\* Node n learns slot s from a node that learned it (the catch-up, or the
\* `Commit` of the leader).
Learn(n, p, s) ==
  /\ learned[n][s] = None
  /\ learned[p][s] # None
  /\ learned' = [learned EXCEPT ![n][s] = learned[p][s]]
  /\ UNCHANGED <<prom, vote, repairing, phase, next, blocked, faults, restarts, fixed, msgs>>

-----------------------------------------------------------------------------
(* Damage, scrub, boot and repair *)

\* The record of slot s, damaged, as the acceptor reads it back.
Damaged(v) == IF Damage = "faulty" THEN [bal |-> v.bal, val |-> Lost] ELSE NoVote

\* A scrub of the grid controller finds the record of slot s of acceptor a
\* damaged. The acceptor keeps running: its ballots and what it learned stay.
ScrubFinds(a, s) ==
  /\ a \in RotNodes
  /\ faults < Faults
  /\ Readable(vote[a][s])
  /\ faults' = faults + 1
  /\ vote' = [vote EXCEPT ![a][s] = Damaged(@)]
  /\ UNCHANGED <<prom, repairing, learned, phase, next, blocked, restarts, fixed, msgs>>

\* Acceptor a boots and finds the record of slot s damaged. Its ballots die,
\* and the learned value of the slot goes (the chosen map is rebuilt from
\* the records). The same record state as a scrub: one repair path.
BootFinds(a, s) ==
  /\ a \in RotNodes
  /\ faults < Faults
  /\ Readable(vote[a][s])
  /\ faults' = faults + 1
  /\ vote' = [vote EXCEPT ![a][s] = Damaged(@)]
  /\ learned' = [learned EXCEPT ![a][s] = None]
  /\ repairing' = [repairing EXCEPT ![a] = [t \in Slots |-> 0]]
  /\ phase' = KillBallots(a)
  /\ UNCHANGED <<prom, next, blocked, restarts, fixed, msgs>>

\* The value that the repair of slot s of acceptor a writes.
RepairValues(a, s) ==
  IF Repair = "chosen" THEN {learned[n][s] : n \in {p \in Acceptors : learned[p][s] # None}}
  ELSE IF Repair = "peer" THEN {vote[p][s].val : p \in {q \in Acceptors : Readable(vote[q][s])}}
  ELSE {}

\* Acceptor a repairs its faulty record of slot s with a value from
\* `RepairValues`, under the ballot that the faulty record keeps. With
\* "atomic" it is one write. With "drop_first" the store deletes the record
\* first, and `RepairEnd` writes the new one.
RepairStart(a, s) ==
  /\ ~Busy(a)
  /\ Faulty(vote[a][s])
  /\ \E v \in RepairValues(a, s) :
       IF RepairWrite = "atomic"
       THEN /\ vote' = [vote EXCEPT ![a][s] = [bal |-> @.bal, val |-> v]]
            /\ fixed' = fixed + 1
            /\ UNCHANGED repairing
       ELSE /\ vote' = [vote EXCEPT ![a][s] = NoVote]
            /\ repairing' = [repairing EXCEPT ![a][s] = vote[a][s].bal]
            /\ UNCHANGED fixed
  /\ UNCHANGED <<prom, learned, phase, next, blocked, faults, restarts, msgs>>

RepairEnd(a, s) ==
  /\ repairing[a][s] # 0
  /\ \E v \in RepairValues(a, s) :
       vote' = [vote EXCEPT ![a][s] = [bal |-> repairing[a][s], val |-> v]]
  /\ repairing' = [repairing EXCEPT ![a][s] = 0]
  /\ fixed' = fixed + 1
  /\ UNCHANGED <<prom, learned, phase, next, blocked, faults, restarts, msgs>>

\* Node a restarts: its ballots die, and what it held only in memory goes
\* (a running repair, what it learned). The promise and the records are on
\* disk.
Restart(a) ==
  /\ restarts < Restarts
  /\ restarts' = restarts + 1
  /\ phase' = KillBallots(a)
  /\ repairing' = [repairing EXCEPT ![a] = [t \in Slots |-> 0]]
  /\ learned' = [learned EXCEPT ![a] = [t \in Slots |-> None]]
  /\ UNCHANGED <<prom, vote, next, blocked, faults, fixed, msgs>>

Next ==
  \/ \E b \in Ballots : Prepare(b) \/ Win(b)
  \/ \E b \in Ballots, c \in Commands : Allocate(b, c)
  \/ \E a \in Acceptors, b \in Ballots : Promise(a, b)
  \/ \E a \in Acceptors, b \in Ballots, s \in Slots : Accept(a, b, s)
  \/ \E b \in Ballots, s \in Slots : Decide(b, s) \/ Probe(b, s)
  \/ \E n, p \in Acceptors, s \in Slots : Learn(n, p, s)
  \/ \E a \in Acceptors, s \in Slots :
       ScrubFinds(a, s) \/ BootFinds(a, s) \/ RepairStart(a, s) \/ RepairEnd(a, s)
  \/ \E a \in Acceptors : Restart(a)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Liveness *)

\* The last ballot NB is the leader that no higher ballot preempts. Its
\* owner campaigns and decides, each member of its configuration votes,
\* each acceptor promises, and each faulty record is repaired.
Fairness ==
  /\ WF_vars(Prepare(NB))
  /\ WF_vars(Win(NB))
  /\ \A s \in Slots : WF_vars(Decide(NB, s)) /\ WF_vars(Probe(NB, s))
  /\ \A a \in Acceptors :
       /\ WF_vars(Promise(a, NB))
       /\ \A s \in Slots :
            /\ WF_vars(Accept(a, NB, s))
            /\ WF_vars(RepairStart(a, s))
            /\ WF_vars(RepairEnd(a, s))
            /\ \A p \in Acceptors : WF_vars(Learn(a, p, s))

LiveSpec == Spec /\ Fairness

\* The last leader holds every slot below its allocator, its repair probe is
\* closed, and no record stays faulty at a slot that it learned.
Settled ==
  /\ phase[NB] = "p2"
  /\ blocked[NB] = {}
  /\ \A s \in Slots : s < next[NB] => learned[Owner[NB]][s] # None
  /\ \A a \in Acceptors, s \in Slots : ~Faulty(vote[a][s]) \/ learned[Owner[NB]][s] = None

Ends == <>[]Settled

-----------------------------------------------------------------------------
(* Safety *)

\* One value for each slot.
Agreement == \A s \in Slots, v, w \in Values : Chosen(s, v) /\ Chosen(s, w) => v = w

\* What a node learned is what was chosen.
LearnedChosen == \A n \in Acceptors, s \in Slots : learned[n][s] # None => Chosen(s, learned[n][s])

\* No fill of a chosen slot: a leader never proposes a `Noop` at a slot
\* where a lower ballot chose another value.
NoFillChosen ==
  \A m \in msgs :
    (m.type = "2a" /\ m.val = Noop) =>
      ~\E b \in 1..(m.bal - 1), v \in Commands : ChosenAt(b, m.slot, v)

\* A repaired record holds the chosen value: a readable record at a chosen
\* slot under a ballot at or above the one that chose it holds that value.
RecordsTrue ==
  \A a \in Acceptors, s \in Slots, b \in Ballots, v \in Values :
    ChosenAt(b, s, v) /\ Readable(vote[a][s]) /\ vote[a][s].bal >= b => vote[a][s].val = v

\* Reachability controls: each must be violated.
\* A record of a chosen slot is faulty.
NoFaultyChosen == ~\E a \in Acceptors, s \in Slots : Faulty(vote[a][s]) /\ IsChosen(s)
\* A repair probe opens.
NoBlock == \A b \in Ballots : blocked[b] = {}
\* A repair writes a record again.
NoRepair == fixed = 0
\* A later configuration decides a slot that a faulty record of an older
\* one holds.
NoCrossDecide ==
  ~\E b \in Ballots, s \in Slots :
     /\ Cfg[b] # Cfg[1]
     /\ \E x \in 1..(b - 1) : Cfg[x] = Cfg[1] /\ \E v \in Commands : ChosenAt(x, s, v)
     /\ \E a \in Members[Cfg[1]] : Faulty(vote[a][s])
     /\ \E v \in Values : ChosenAt(b, s, v)
=============================================================================
