--------------------------- MODULE ConfigRegister ---------------------------
(***************************************************************************)
(* Rule 1 of the rework, "seal the latest configuration"                   *)
(* (docs/architecture/04-roles.md, step 1.6, #1095): for each journal the  *)
(* matchmakers hold a register, the configuration in force and at most one *)
(* open successor.                                                          *)
(*                                                                         *)
(* Each matchmaker holds a read generation `rg`, a write generation `wg`   *)
(* and the register `<<cur, suc>>` (FoundationDB's `LocalGenerationReg`:   *)
(* fdbserver/coordinator/Coordination.cpp; the write order of a recovery:  *)
(* fdbserver/ClusterRecovery.actor.cpp, where the new `DBCoreState` is     *)
(* written only after the new log system holds the recovered data).       *)
(*                                                                         *)
(* A candidate at ballot b, one slot:                                      *)
(*   1. reads the register at a matchmaker majority (each raises `rg` to   *)
(*      b) and takes the value of the highest `wg`;                        *)
(*   2. runs Phase 1 over a quorum of `cur`, and of `suc` when one is      *)
(*      open: this is the seal of the old set;                             *)
(*   3. with no open successor, either stays in `cur` (Phase 2 there, and  *)
(*      the register is not written: a campaign registers nothing), or     *)
(*      starts a move to a live configuration c: it announces             *)
(*      `<<cur, c>>` at a majority (a CAS at write generation 2b);         *)
(*      with an open successor it finishes that move forward, and first    *)
(*      announces it again at a majority, because the read can return a    *)
(*      successor that only a minority holds;                              *)
(*   4. runs Phase 2 in the target only;                                   *)
(*   5. after its value is chosen in the target, switches the register to  *)
(*      `<<target, none>>` at a majority (a CAS at 2b + 1).                *)
(* A candidate may stop after any step: no fairness below the top ballot. *)
(*                                                                         *)
(* A machine releases its store for the journal (and never answers again) *)
(* once one switch is durable: a matchmaker majority holds the same write  *)
(* (one generation), with no successor, and its configuration does not     *)
(* name the machine. A majority of different writes is not enough: a read  *)
(* can still return a later announce that names the old configuration.    *)
(*                                                                         *)
(* The constants that select a control (each must break its property):    *)
(*   Switch  = "before": the one-slot register of `cas-register`: the      *)
(*             target is written in force before Phase 2.                  *)
(*   Stale   = TRUE: the CAS ignores the read generation, so the belief of *)
(*             a candidate that a later read already passed is stored.     *)
(*   Abandon = TRUE: a candidate drops an open successor it read.          *)
(*   Reannounce = FALSE: a candidate finishes an open successor that it    *)
(*             read with no announce of its own, so a successor that only  *)
(*             a minority of matchmakers holds is switched in force.       *)
(*   Rejoin = TRUE: a move may name again, under the same id, a machine    *)
(*             that left a configuration in force (rule 5: a machine that  *)
(*             comes back is a new id, with a new store).                  *)
(*   Release = "mixed": a machine releases once each matchmaker of a       *)
(*             majority holds a register with no successor that does not   *)
(*             name it, from writes of different generations.              *)
(*   Release = "announce": a machine releases its store once a majority    *)
(*             holds a successor that does not name it, before the switch. *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Acceptors,     \* the machines; an acceptor is named by its machine
          Matchmakers,
          Host,          \* Host[m]: the machine that runs matchmaker m
          Values, NB, Configs, C0, Mem, Q1s, Q2s,
          Switch,        \* "after" (the rule) or "before" (control)
          Stale,         \* FALSE (the rule) or TRUE (control)
          Abandon,       \* FALSE (the rule) or TRUE (control)
          Reannounce,    \* TRUE (the rule) or FALSE (control)
          Rejoin,        \* FALSE (the rule) or TRUE (control)
          Release,       \* "switch" (the rule), "mixed" or "announce" (controls), "never"
          Crashes        \* how many machines may die for good

Ballots == 1..NB
None == "none"
MQ == {Q \in SUBSET Matchmakers : 2 * Cardinality(Q) > Cardinality(Matchmakers)}

VARIABLES rg, wg, cur, suc,           \* matchmaker state
          prom, votes,                \* acceptor state; a vote is <<a, b, v>>
          phase, reads, rcur, rsuc, p1, ncfg, lval, wrote,
          crashed, released,
          held                        \* machines a register ever named in force

vars == <<rg, wg, cur, suc, prom, votes, phase, reads, rcur, rsuc, p1, ncfg,
          lval, wrote, crashed, released, held>>

Gone == crashed \cup released
Live(c) == \E Q \in Q1s[c] : Q \cap Gone = {}
Up(m) == Host[m] \notin crashed

\* The matchmaker of Q whose write is the highest: a read at Q takes its register.
Top(Q) == CHOOSE m \in Q : \A n \in Q : wg[n] <= wg[m]

\* The configurations a candidate that reads now must reach: the register a
\* majority answers.
Answers == {<<cur[Top(Q)], suc[Top(Q)]>> : Q \in MQ}

\* The configurations that a read could return, or that a ballot works in.
InForce == ({r[1] : r \in Answers} \cup {r[2] : r \in Answers}) \ {None}

Moving(b) == ncfg[b] # rcur[b] \/ rsuc[b] # None

Init ==
  /\ rg = [m \in Matchmakers |-> 0]
  /\ wg = [m \in Matchmakers |-> 0]
  /\ cur = [m \in Matchmakers |-> C0]
  /\ suc = [m \in Matchmakers |-> None]
  /\ prom = [a \in Acceptors |-> 0]
  /\ votes = {}
  /\ phase = [b \in Ballots |-> "idle"]
  /\ reads = [b \in Ballots |-> {}]
  /\ rcur = [b \in Ballots |-> C0]
  /\ rsuc = [b \in Ballots |-> None]
  /\ p1 = [b \in Ballots |-> {}]
  /\ ncfg = [b \in Ballots |-> C0]
  /\ lval = [b \in Ballots |-> None]
  /\ wrote = [b \in Ballots |-> {}]
  /\ crashed = {}
  /\ released = {}
  /\ held = Mem[C0]

Read(b, m) ==
  /\ phase[b] \in {"idle", "read"}
  /\ Up(m)
  /\ rg[m] < b
  /\ rg' = [rg EXCEPT ![m] = b]
  /\ phase' = [phase EXCEPT ![b] = "read"]
  /\ reads' = [reads EXCEPT ![b] = @ \cup {m}]
  /\ UNCHANGED <<wg, cur, suc, prom, votes, rcur, rsuc, p1, ncfg, lval, wrote, crashed, released, held>>

EndRead(b) ==
  /\ phase[b] = "read"
  /\ \E Q \in MQ :
       /\ Q \subseteq reads[b]
       /\ rcur' = [rcur EXCEPT ![b] = cur[Top(Q)]]
       /\ rsuc' = [rsuc EXCEPT ![b] = IF Abandon THEN None ELSE suc[Top(Q)]]
  /\ phase' = [phase EXCEPT ![b] = "p1"]
  /\ UNCHANGED <<rg, wg, cur, suc, prom, votes, reads, p1, ncfg, lval, wrote, crashed, released, held>>

Asked(b) == Mem[rcur[b]] \cup (IF rsuc[b] = None THEN {} ELSE Mem[rsuc[b]])

Promise(b, a) ==
  /\ phase[b] = "p1"
  /\ a \in Asked(b) /\ a \notin Gone
  /\ prom[a] < b
  /\ prom' = [prom EXCEPT ![a] = b]
  /\ p1' = [p1 EXCEPT ![b] = @ \cup {<<a, {<<x[2], x[3]>> : x \in {y \in votes : y[1] = a}}>>}]
  /\ UNCHANGED <<rg, wg, cur, suc, votes, phase, reads, rcur, rsuc, ncfg, lval, wrote, crashed, released, held>>

Promised(b) == {r[1] : r \in p1[b]}
Covers(b, c) == \E Q \in Q1s[c] : Q \subseteq Promised(b)

\* Phase 1 done over every configuration the read returned. The value is
\* the one of the highest vote reported, else any. The target: the open
\* successor, else the configuration in force or any live one (a move).
EndP1(b) ==
  /\ phase[b] = "p1"
  /\ Covers(b, rcur[b])
  /\ IF rsuc[b] = None THEN TRUE ELSE Covers(b, rsuc[b])
  /\ LET rep == UNION {r[2] : r \in p1[b]}
         free == rep = {}
         top == IF free THEN 0 ELSE CHOOSE x \in {y[1] : y \in rep} : \A y \in rep : y[1] <= x
     IN \E v \in Values :
          /\ free \/ <<top, v>> \in rep
          /\ lval' = [lval EXCEPT ![b] = v]
  /\ \E c \in Configs :
       /\ IF rsuc[b] # None THEN c = rsuc[b]
          ELSE c = rcur[b] \/ (Live(c) /\ (Rejoin \/ Mem[c] \cap (held \ Mem[rcur[b]]) = {}))
       /\ ncfg' = [ncfg EXCEPT ![b] = c]
       /\ phase' = [phase EXCEPT ![b] =
            IF c = rcur[b] /\ rsuc[b] = None THEN "p2"
            ELSE IF rsuc[b] # None /\ ~Reannounce THEN "p2"
            ELSE IF Switch = "before" THEN "switch"
            ELSE "announce"]
  /\ UNCHANGED <<rg, wg, cur, suc, prom, votes, reads, rcur, rsuc, p1, wrote, crashed, released, held>>

\* The two register writes of a move. An announce writes <<cur, target>> at
\* generation 2b; a switch writes <<target, none>> at 2b + 1.
Gen(b) == IF phase[b] = "announce" THEN 2 * b ELSE 2 * b + 1

Write(b, m) ==
  /\ phase[b] \in {"announce", "switch"}
  /\ Up(m)
  /\ m \notin wrote[b]
  /\ wg[m] < Gen(b)
  /\ Stale \/ rg[m] <= b
  /\ wg' = [wg EXCEPT ![m] = Gen(b)]
  /\ cur' = [cur EXCEPT ![m] = IF phase[b] = "announce" THEN rcur[b] ELSE ncfg[b]]
  /\ suc' = [suc EXCEPT ![m] = IF phase[b] = "announce" THEN ncfg[b] ELSE None]
  /\ wrote' = [wrote EXCEPT ![b] = @ \cup {m}]
  /\ held' = held \cup Mem[cur'[m]]
  /\ UNCHANGED <<rg, prom, votes, phase, reads, rcur, rsuc, p1, ncfg, lval, crashed, released>>

EndWrite(b) ==
  /\ phase[b] \in {"announce", "switch"}
  /\ \E Q \in MQ : Q \subseteq wrote[b]
  /\ wrote' = [wrote EXCEPT ![b] = {}]
  /\ phase' = [phase EXCEPT ![b] =
       IF phase[b] = "announce" \/ Switch = "before" THEN "p2" ELSE "done"]
  /\ UNCHANGED <<rg, wg, cur, suc, prom, votes, reads, rcur, rsuc, p1, ncfg, lval, crashed, released, held>>

Accept(b, a) ==
  /\ phase[b] = "p2"
  /\ a \in Mem[ncfg[b]] /\ a \notin Gone
  /\ prom[a] <= b
  /\ <<a, b, lval[b]>> \notin votes
  /\ prom' = [prom EXCEPT ![a] = b]
  /\ votes' = votes \cup {<<a, b, lval[b]>>}
  /\ UNCHANGED <<rg, wg, cur, suc, phase, reads, rcur, rsuc, p1, ncfg, lval, wrote, crashed, released, held>>

ChosenAt(b, v) == \E Q \in Q2s[ncfg[b]] : \A a \in Q : <<a, b, v>> \in votes

\* Phase 2 done: the value is held at a Phase-2 quorum of the target. Then a
\* move switches the register; a campaign that stayed is done.
EndP2(b) ==
  /\ phase[b] = "p2"
  /\ ChosenAt(b, lval[b])
  /\ phase' = [phase EXCEPT ![b] =
       IF Moving(b) /\ Switch = "after" THEN "switch" ELSE "done"]
  /\ UNCHANGED <<rg, wg, cur, suc, prom, votes, reads, rcur, rsuc, p1, ncfg, lval, wrote, crashed, released, held>>

\* A machine releases its store for the journal, for good. It holds one as a
\* member of a configuration that a register named in force; a machine
\* that only a successor names keeps its store until that move switches.
MayRelease(a) ==
  CASE Release = "switch" ->
         \E Q \in MQ : \E g \in {wg[n] : n \in Q} :
           \A m \in Q : wg[m] = g /\ suc[m] = None /\ a \notin Mem[cur[m]]
    [] Release = "mixed" ->
         \E Q \in MQ : \A m \in Q : suc[m] = None /\ a \notin Mem[cur[m]]
    [] Release = "announce" ->
         \E Q \in MQ : \A m \in Q : suc[m] # None /\ a \notin Mem[suc[m]]
    [] OTHER -> FALSE

ReleaseStore(a) ==
  /\ a \notin Gone
  /\ a \in held
  /\ MayRelease(a)
  /\ released' = released \cup {a}
  /\ UNCHANGED <<rg, wg, cur, suc, prom, votes, phase, reads, rcur, rsuc, p1, ncfg, lval, wrote, crashed, held>>

\* A machine dies for good, up to Crashes machines, and only while every
\* configuration a read could return keeps a live Phase-1 quorum without it
\* (FoundationDB's kill policy). The policy knows the crashes, not the
\* releases: a release is the protocol's own act.
Crash(n) ==
  /\ Cardinality(crashed) < Crashes
  /\ n \notin crashed
  /\ \A c \in InForce : \E Q \in Q1s[c] : Q \cap (crashed \cup {n}) = {}
  /\ crashed' = crashed \cup {n}
  /\ UNCHANGED <<rg, wg, cur, suc, prom, votes, phase, reads, rcur, rsuc, p1, ncfg, lval, wrote, released, held>>

BallotNext(b) ==
  \/ \E m \in Matchmakers : Read(b, m) \/ Write(b, m)
  \/ EndRead(b) \/ EndP1(b) \/ EndWrite(b) \/ EndP2(b)
  \/ \E a \in Acceptors : Promise(b, a) \/ Accept(b, a)

Next ==
  \/ \E b \in Ballots : BallotNext(b)
  \/ \E a \in Acceptors : ReleaseStore(a) \/ Crash(a)

Spec == Init /\ [][Next]_vars

\* Liveness: the top ballot is never preempted, so with fairness on its own
\* steps it must win, whatever the lower ballots left behind.
LiveSpec == Spec /\ WF_vars(BallotNext(NB))

Chosen(v) == \E b \in Ballots : phase[b] \in {"p2", "switch", "done"} /\ ChosenAt(b, v)

Agreement == \A v, w \in Values : Chosen(v) /\ Chosen(w) => v = w

\* The generations of a matchmaker never go down: a write below the write
\* generation would bring back an older configuration.
GenUp == [][\A m \in Matchmakers : rg'[m] >= rg[m] /\ wg'[m] >= wg[m]]_vars

\* Whatever a matchmaker majority answers, each configuration in it still
\* has a live Phase-1 quorum.
Electable == \A r \in Answers : Live(r[1]) /\ IF r[2] = None THEN TRUE ELSE Live(r[2])

Wins == <>(phase[NB] = "done")

\* Reach: a move switched, a member of the old configuration released its
\* store, and a value is chosen.
NoSwitchedRelease ==
  ~(/\ released # {}
    /\ \E b \in Ballots : phase[b] = "done" /\ Moving(b)
    /\ \E v \in Values : Chosen(v))
=============================================================================
