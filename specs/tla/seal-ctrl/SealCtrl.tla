------------------------------- MODULE SealCtrl -------------------------------
(***************************************************************************)
(* The seal end of a segment against a CTRL faulty record (#1313).         *)
(*                                                                         *)
(* Two configurations: the old grid (configuration 1) and the next one     *)
(* (configuration 2). Ballot 1 is the leader of the old grid; it is in     *)
(* Phase 2 from the start (the first leader of a fresh journal has nothing *)
(* to recover) and writes slots there. Each later ballot is a leader of    *)
(* the next configuration. Its Phase 1 needs a promise quorum of every     *)
(* configuration of its history (Matchmaker Paxos): the old one and its    *)
(* own. Phase 2 addresses its own configuration only. The first of these   *)
(* leaders that finished Phase 1 writes the end of the old segment once,   *)
(* by compare-and-set (`docs/architecture/02-data-plane.md` §2.10). A      *)
(* leader reads the end at matchmaking, before its Phase 1.                *)
(*                                                                         *)
(* CTRL (`specs/tla/truncation/`, #1216): a boot finds the record of a     *)
(* slot damaged, with its ballot known and its value lost (`Lost`). A      *)
(* promise reports the faulty record. The tri-state tally of Phase 1       *)
(* (`slot_decidable` in `proposer.rs`) counts at a slot only the answers   *)
(* that are not a faulty report above the highest readable report; the    *)
(* slot is decided when those answers hold a quorum of each configuration  *)
(* that counts at the slot, else it is blocked and the repair probe decides*)
(* it later on more promises of the same ballot. The frontier (the         *)
(* allocator's first slot) counts faulty reports too, as `proposer.rs`     *)
(* does.                                                                   *)
(*                                                                         *)
(* A message is never lost from `msgs`, so a message can arrive late, two  *)
(* times, or never. The matchmakers are one atomic register (the end):     *)
(* `segment-records/` checks how a matchmaker quorum keeps it.             *)
(*                                                                         *)
(* Constants (the first value of each is the rule of the plan that passes; *)
(* the others are candidates or controls):                                 *)
(*   EndRule  "any": the end is one above the highest slot where the       *)
(*            sealer's Phase 1 saw a vote in the old grid, a faulty vote   *)
(*            too. "readable": it counts readable votes only (the reading  *)
(*            of the second Fable review, new finding 1).                  *)
(*   ReadOld  "all": a Phase 1 reads every slot of the old grid, end or    *)
(*            not: one value for each slot rests on the promises alone,    *)
(*            and the end is a journal fact (the segments review of        *)
(*            2026-11-23, fix 2). "belowEnd": a Phase 1 that read an end   *)
(*            reads the old grid only below it; with the end of readable   *)
(*            votes it chooses two values (`end_readable`).                *)
(*   SealWait "none": the sealer writes the end right after its Phase 1,   *)
(*            from the promises of its win. "repaired": it writes the end  *)
(*            only once its repair probe closed every blocked slot, from   *)
(*            every promise it holds then (rule 2 of #1313).               *)
(*   Faults   how many faulty records a boot may find, on `RotNodes` only. *)
(*   Down     acceptors that never take a step (the liveness checks).      *)
(***************************************************************************)
EXTENDS Integers, FiniteSets

CONSTANTS Acceptors, Cfg, Gen, Cmd, Noop, Lost, NB, NS,
          EndRule, ReadOld, SealWait, Faults, RotNodes, Down

Ballots == 1..NB
Slots == 0..(NS - 1)
Commands == {Cmd[b] : b \in 1..NB}
Values == Commands \cup {Noop}
Cfgs == {1, 2}

ASSUME EndRule \in {"any", "readable"}
ASSUME ReadOld \in {"belowEnd", "all"}
ASSUME SealWait \in {"none", "repaired"}
ASSUME Cfg \in [Cfgs -> SUBSET Acceptors] /\ Cfg[1] \cap Cfg[2] = {}
ASSUME Gen \in [Ballots -> Cfgs] /\ Gen[1] = 1 /\ \A b \in 2..NB : Gen[b] = 2
ASSUME RotNodes \subseteq Acceptors /\ Down \subseteq Acceptors
ASSUME Noop \notin Commands /\ Lost \notin Values

Quorums(c) == {Q \in SUBSET Cfg[c] : 2 * Cardinality(Q) > Cardinality(Cfg[c])}
\* The configurations that Phase 1 of ballot b reads: its history.
Hist(b) == 1..Gen[b]
Max(S) == CHOOSE x \in S : \A y \in S : y <= x

Vote == [bal : 0..NB, val : Values \cup {Lost}]
NoVote == [bal |-> 0, val |-> Noop]
Readable(v) == v.bal > 0 /\ v.val # Lost
Faulty(v) == v.bal > 0 /\ v.val = Lost
NoEnd == -1

VARIABLES prom,    \* acceptor: the promise (on disk before it answers)
          vote,    \* acceptor: the vote, per slot (on disk before it answers)
          phase,   \* ballot: "idle", "p1" or "p2"
          rend,    \* ballot: the end it read at matchmaking, or NoEnd
          pset,    \* ballot: the promises its tally holds
          next,    \* ballot: the allocator's next slot, once in Phase 2
          blocked, \* ballot: the slots its repair probe still decides
          endw,    \* the end of the old segment at the matchmakers, or NoEnd
          faults,  \* how many faulty records a boot found
          msgs     \* every message sent: the ghost of every vote

vars == <<prom, vote, phase, rend, pset, next, blocked, endw, faults, msgs>>

TypeOK ==
  /\ prom \in [Acceptors -> 0..NB]
  /\ vote \in [Acceptors -> [Slots -> Vote]]
  /\ phase \in [Ballots -> {"idle", "p1", "p2"}]
  /\ rend \in [Ballots -> NoEnd..NS]
  /\ next \in [Ballots -> 0..NS]
  /\ blocked \in [Ballots -> SUBSET Slots]
  /\ endw \in NoEnd..NS
  /\ faults \in 0..Faults

Init ==
  /\ prom = [a \in Acceptors |-> 0]
  /\ vote = [a \in Acceptors |-> [s \in Slots |-> NoVote]]
  /\ phase = [b \in Ballots |-> IF b = 1 THEN "p2" ELSE "idle"]
  /\ rend = [b \in Ballots |-> NoEnd]
  /\ pset = [b \in Ballots |-> {}]
  /\ next = [b \in Ballots |-> 0]
  /\ blocked = [b \in Ballots |-> {}]
  /\ endw = NoEnd
  /\ faults = 0
  /\ msgs = {}

Send(m) == msgs' = msgs \cup {m}
SendAll(M) == msgs' = msgs \cup M

-----------------------------------------------------------------------------
(* The chosen log: a value is chosen at slot s once a majority of the      *)
(* configuration of ballot b voted for it at b. `msgs` keeps every vote,   *)
(* so a faulty record does not unchoose it.                                *)

ChosenAt(b, s, v) ==
  \E Q \in Quorums(Gen[b]) :
    \A a \in Q : [type |-> "2b", acc |-> a, bal |-> b, slot |-> s, val |-> v] \in msgs
Chosen(s, v) == \E b \in Ballots : ChosenAt(b, s, v)

-----------------------------------------------------------------------------
(* Phase 1 *)

\* A leader of the next configuration registers its ballot and reads the
\* end at the matchmakers, then sends its `Prepare`.
Prepare(b) ==
  /\ Gen[b] = 2
  /\ phase[b] = "idle"
  /\ phase' = [phase EXCEPT ![b] = "p1"]
  /\ rend' = [rend EXCEPT ![b] = endw]
  /\ UNCHANGED <<prom, vote, pset, next, blocked, endw, faults, msgs>>

\* Acceptor a promises ballot b. Its promise reports every vote it holds,
\* faulty votes too. A promise at its own ballot answers too
\* (`Acceptor::prepare` re-affirms it): the repair probe asks again an
\* acceptor that took an `Accept` of the ballot before the `Prepare`.
Promise(a, b) ==
  /\ a \notin Down
  /\ phase[b] \in {"p1", "p2"}
  /\ Gen[b] = 2
  /\ a \in UNION {Cfg[c] : c \in Hist(b)}
  /\ \/ prom[a] < b
     \/ /\ prom[a] = b
        /\ blocked[b] # {}
        /\ ~\E n \in msgs : n.type = "1b" /\ n.acc = a /\ n.bal = b
  /\ prom' = [prom EXCEPT ![a] = b]
  /\ Send([type |-> "1b", acc |-> a, bal |-> b, rep |-> vote[a]])
  /\ UNCHANGED <<vote, phase, rend, pset, next, blocked, endw, faults>>

\* Does the report of acceptor a count at slot s for ballot b? An old
\* acceptor's report counts below the end that b read (rule "belowEnd").
Counts(b, a, s) ==
  \/ a \in Cfg[Gen[b]]
  \/ ReadOld = "all"
  \/ rend[b] = NoEnd
  \/ s < rend[b]

\* The configurations whose quorum the tally of ballot b needs at slot s.
CfgsAt(b, s) ==
  {c \in Hist(b) : c = Gen[b] \/ ReadOld = "all" \/ rend[b] = NoEnd \/ s < rend[b]}

Reports(b, S, s) == {m \in S : Counts(b, m.acc, s)}

\* The highest readable vote that the promises S report at slot s.
Top(b, S, s) ==
  LET rep == {m \in Reports(b, S, s) : Readable(m.rep[s])}
  IN IF rep = {} THEN NoVote
     ELSE (CHOOSE m \in rep : \A n \in rep : n.rep[s].bal <= m.rep[s].bal).rep[s]

\* The tri-state tally at slot s: an answer qualifies unless it is a faulty
\* report above the highest readable one; the qualifying answers must hold
\* a quorum of each configuration that counts at s.
Decidable(b, S, s) ==
  LET q == {m.acc : m \in {n \in Reports(b, S, s) :
                             ~(Faulty(n.rep[s]) /\ n.rep[s].bal > Top(b, S, s).bal)}}
  IN \A c \in CfgsAt(b, s) : \E Q \in Quorums(c) : Q \subseteq q

\* The frontier: one past the highest slot that the promises S report,
\* faulty reports too.
Frontier(b, S) ==
  LET h == {s \in Slots : \E m \in Reports(b, S, s) : m.rep[s].bal > 0}
  IN IF h = {} THEN 0 ELSE Max(h) + 1

\* The end of the old segment from the promises S: one above the highest
\* slot where an old acceptor reports a vote (EndRule).
EndOf(b, S) ==
  LET old == {m \in S : m.acc \in Cfg[1]}
      seen(v) == IF EndRule = "any" THEN v.bal > 0 ELSE Readable(v)
      h == {s \in Slots : \E m \in old : seen(m.rep[s])}
  IN IF h = {} THEN 0 ELSE Max(h) + 1

\* The value ballot b proposes again at slot s: the highest readable
\* report, or a `Noop` (the gap fill).
Refill(b, S, s) ==
  [type |-> "2a", bal |-> b, slot |-> s,
   val |-> IF Top(b, S, s).bal > 0 THEN Top(b, S, s).val ELSE Noop]

\* The promise sets of ballot b: one promise for each acceptor at most.
PromiseSets(b) ==
  {S \in SUBSET {m \in msgs : m.type = "1b" /\ m.bal = b} :
     \A m, n \in S : m.acc = n.acc => m = n}

Covers(b, S) == \A c \in Hist(b) : \E Q \in Quorums(c) : Q \subseteq {m.acc : m \in S}

\* Ballot b wins Phase 1 on the promises S. It proposes again, in its own
\* configuration, each slot below its frontier that the tally decides; the
\* others go to the repair probe.
Win(b) ==
  /\ phase[b] = "p1"
  /\ \E S \in PromiseSets(b) :
       /\ Covers(b, S)
       /\ LET f == Frontier(b, S)
              open == {s \in 0..(f - 1) : ~Decidable(b, S, s)}
          IN /\ next' = [next EXCEPT ![b] = f]
             /\ blocked' = [blocked EXCEPT ![b] = open]
             /\ pset' = [pset EXCEPT ![b] = S]
             /\ SendAll({Refill(b, S, s) : s \in (0..(f - 1)) \ open})
  /\ phase' = [phase EXCEPT ![b] = "p2"]
  /\ UNCHANGED <<prom, vote, rend, endw, faults>>

\* A later promise of ballot b reaches its tally.
Absorb(b) ==
  /\ phase[b] = "p2"
  /\ Gen[b] = 2
  /\ \E m \in msgs :
       /\ m.type = "1b" /\ m.bal = b
       /\ m.acc \notin {n.acc : n \in pset[b]}
       /\ pset' = [pset EXCEPT ![b] = @ \cup {m}]
  /\ UNCHANGED <<prom, vote, phase, rend, next, blocked, endw, faults, msgs>>

\* The repair probe of ballot b settles a blocked slot s on the promises it
\* holds now.
Probe(b, s) ==
  /\ phase[b] = "p2"
  /\ s \in blocked[b]
  /\ Decidable(b, pset[b], s)
  /\ blocked' = [blocked EXCEPT ![b] = @ \ {s}]
  /\ Send(Refill(b, pset[b], s))
  /\ UNCHANGED <<prom, vote, phase, rend, pset, next, endw, faults>>

\* The seal: a leader of the next configuration that finished Phase 1 with
\* no end at the matchmakers writes it by compare-and-set from "none".
Seal(b) ==
  /\ Gen[b] = 2
  /\ phase[b] = "p2"
  /\ endw = NoEnd
  /\ SealWait = "repaired" => blocked[b] = {}
  /\ endw' = EndOf(b, pset[b])
  /\ UNCHANGED <<prom, vote, phase, rend, pset, next, blocked, faults, msgs>>

-----------------------------------------------------------------------------
(* Phase 2 *)

\* The leader of ballot b allocates its next slot to its fresh command
\* `Cmd[b]`. One command for each ballot is enough: two values differ when
\* two ballots of different configurations chose them.
Allocate(b) ==
  LET c == Cmd[b] IN
  /\ phase[b] = "p2"
  /\ next[b] < NS
  /\ next' = [next EXCEPT ![b] = @ + 1]
  /\ Send([type |-> "2a", bal |-> b, slot |-> next[b], val |-> c])
  /\ UNCHANGED <<prom, vote, phase, rend, pset, blocked, endw, faults>>

\* Acceptor a of the configuration of ballot b votes. A vote over a faulty
\* record heals it.
Accept(a, b, s) ==
  /\ a \notin Down
  /\ a \in Cfg[Gen[b]]
  /\ prom[a] <= b
  /\ \E m \in msgs :
       /\ m.type = "2a" /\ m.bal = b /\ m.slot = s
       /\ vote[a][s] # [bal |-> b, val |-> m.val]
       /\ prom' = [prom EXCEPT ![a] = b]
       /\ vote' = [vote EXCEPT ![a][s] = [bal |-> b, val |-> m.val]]
       /\ Send([type |-> "2b", acc |-> a, bal |-> b, slot |-> s, val |-> m.val])
  /\ UNCHANGED <<phase, rend, pset, next, blocked, endw, faults>>

-----------------------------------------------------------------------------
(* Faults *)

\* Acceptor a boots and finds its record of slot s damaged, which the scrub
\* did not reach first: the vote keeps its ballot and loses its value.
Rot(a, s) ==
  /\ a \in RotNodes
  /\ faults < Faults
  /\ Readable(vote[a][s])
  /\ faults' = faults + 1
  /\ vote' = [vote EXCEPT ![a][s] = [bal |-> @.bal, val |-> Lost]]
  /\ UNCHANGED <<prom, phase, rend, pset, next, blocked, endw, msgs>>

Next ==
  \/ \E b \in Ballots : Prepare(b) \/ Win(b) \/ Absorb(b) \/ Seal(b)
  \/ \E b \in Ballots : Allocate(b)
  \/ \E a \in Acceptors, b \in Ballots : Promise(a, b)
  \/ \E a \in Acceptors, b \in Ballots, s \in Slots : Accept(a, b, s)
  \/ \E b \in Ballots, s \in Slots : Probe(b, s)
  \/ \E a \in Acceptors, s \in Slots : Rot(a, s)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Safety *)

\* No slot chooses two values, across the seal.
Agreement == \A s \in Slots, v, w \in Values : Chosen(s, v) /\ Chosen(s, w) => v = w

\* The end is above every slot that the old grid chose. The rest of the
\* segment rules stand on it: the replicas of the old segment fold only the
\* slots below the end into the boundary state, and the old grid retires
\* once `F + 1` of them hold those slots (§2.10). A slot that the old grid
\* chose at or above the end is in no fold of the old segment.
EndCovers ==
  endw # NoEnd =>
    \A s \in Slots, v \in Values : ChosenAt(1, s, v) => s < endw

(* Liveness, as an invariant: every step is bounded, so a run ends in a    *)
(* state with no enabled step. With the acceptors of `Down` stopped, the   *)
(* end is written then, and every slot that the old grid chose is chosen   *)
(* in the next configuration too (the sealer's probe closed it).           *)
Ends ==
  (~ENABLED Next) =>
    /\ endw # NoEnd
    /\ \A s \in Slots, v \in Values :
         ChosenAt(1, s, v) => \E b \in 2..NB : ChosenAt(b, s, v)

(* Reach: a check that must be violated, so that a passing check is not    *)
(* empty. A faulty record of the old grid at a slot that the old grid      *)
(* chose, and the end written.                                             *)
NoFaultySeal ==
  ~(/\ endw # NoEnd
    /\ \E a \in Cfg[1], s \in Slots, v \in Values : Faulty(vote[a][s]) /\ ChosenAt(1, s, v))
================================================================================
