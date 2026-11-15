------------------------------- MODULE Register -------------------------------
(***************************************************************************)
(* The matchmakers as a compare-and-set register, as FoundationDB's        *)
(* coordinators hold `DBCoreState` (fdbserver/coordinator/Coordination.cpp, *)
(* `LocalGenerationReg`; fdbserver/core/CoordinatedState.cpp). PR #1064    *)
(* (gone-node gap) compares it with paros's registrations.                 *)
(*                                                                         *)
(* A candidate at ballot b:                                                *)
(*   1. reads the register at a matchmaker majority: each matchmaker       *)
(*      raises its read generation to b and answers its written value;     *)
(*      the candidate takes the value of the highest write generation,     *)
(*      the configuration in force;                                        *)
(*   2. runs Phase 1 over that one configuration;                          *)
(*   3. writes its own configuration at a matchmaker majority: a           *)
(*      matchmaker takes the write only if its read generation is at most  *)
(*      b and its write generation below b (the CAS);                      *)
(*   4. runs Phase 2 in its configuration.                                 *)
(* A campaign that stops before step 3 leaves no configuration behind.     *)
(*                                                                         *)
(* Check = "cas" is the rule; "nocheck" drops the read generation from the *)
(* CAS, the control that must break Agreement.                             *)
(*                                                                         *)
(* Electable is the liveness form of `Retire.tla`: in every state, every   *)
(* configuration a matchmaker majority can answer still has a live         *)
(* Phase-1 quorum. A crash is allowed only while the configurations in     *)
(* force stay live (FoundationDB's kill policy).                           *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Acceptors, Matchmakers, Values, NB, Configs, C0, Mem, Q1s, Q2s,
          Check, Crashes

Ballots == 1..NB
None == "none"
MQ == {Q \in SUBSET Matchmakers : 2 * Cardinality(Q) > Cardinality(Matchmakers)}

VARIABLES rg, wg, rv,          \* matchmaker: read gen, write gen, value
          prom, votes,         \* acceptors: promise, every vote <<a, b, v>>
          phase, reads, lcfg, p1, ncfg, lval, wrote,
          crashed

vars == <<rg, wg, rv, prom, votes, phase, reads, lcfg, p1, ncfg, lval, wrote, crashed>>

Live(c) == \E Q \in Q1s[c] : Q \cap crashed = {}

\* The value a read at matchmakers Q returns: the highest write's.
MaxOf(Q) == CHOOSE v \in {rv[m] : m \in Q} :
              \E m \in Q : rv[m] = v /\ \A n \in Q : wg[n] <= wg[m]

\* Every configuration a read could return, and every write in flight.
InForce == {MaxOf(Q) : Q \in MQ} \cup {ncfg[b] : b \in {x \in Ballots : phase[x] \in {"write", "p2"}}}

Init ==
  /\ rg = [m \in Matchmakers |-> 0]
  /\ wg = [m \in Matchmakers |-> 0]
  /\ rv = [m \in Matchmakers |-> C0]
  /\ prom = [a \in Acceptors |-> 0]
  /\ votes = {}
  /\ phase = [b \in Ballots |-> "idle"]
  /\ reads = [b \in Ballots |-> {}]
  /\ lcfg = [b \in Ballots |-> C0]
  /\ p1 = [b \in Ballots |-> {}]
  /\ ncfg = [b \in Ballots |-> C0]
  /\ lval = [b \in Ballots |-> None]
  /\ wrote = [b \in Ballots |-> {}]
  /\ crashed = {}

Read(b, m) ==
  /\ phase[b] \in {"idle", "read"}
  /\ rg[m] < b
  /\ rg' = [rg EXCEPT ![m] = b]
  /\ phase' = [phase EXCEPT ![b] = "read"]
  /\ reads' = [reads EXCEPT ![b] = @ \cup {m}]
  /\ UNCHANGED <<wg, rv, prom, votes, lcfg, p1, ncfg, lval, wrote, crashed>>

EndRead(b) ==
  /\ phase[b] = "read"
  /\ \E Q \in MQ : Q \subseteq reads[b] /\ lcfg' = [lcfg EXCEPT ![b] = MaxOf(Q)]
  /\ phase' = [phase EXCEPT ![b] = "p1"]
  /\ UNCHANGED <<rg, wg, rv, prom, votes, reads, p1, ncfg, lval, wrote, crashed>>

Promise(b, a) ==
  /\ phase[b] = "p1"
  /\ a \in Mem[lcfg[b]] /\ a \notin crashed
  /\ prom[a] < b
  /\ prom' = [prom EXCEPT ![a] = b]
  /\ p1' = [p1 EXCEPT ![b] = @ \cup {<<a, {<<x[2], x[3]>> : x \in {y \in votes : y[1] = a}}>>}]
  /\ UNCHANGED <<rg, wg, rv, votes, phase, reads, lcfg, ncfg, lval, wrote, crashed>>

\* Phase 1 done: the value of the highest vote reported, else any. The
\* candidate picks its configuration, a live one.
EndP1(b) ==
  /\ phase[b] = "p1"
  /\ \E Q \in Q1s[lcfg[b]] :
       /\ Q \subseteq {r[1] : r \in p1[b]}
       /\ LET rep == UNION {r[2] : r \in {s \in p1[b] : s[1] \in Q}}
              free == rep = {}
              top == IF free THEN 0 ELSE CHOOSE x \in {y[1] : y \in rep} : \A y \in rep : y[1] <= x
          IN \E v \in Values :
               /\ free \/ <<top, v>> \in rep
               /\ lval' = [lval EXCEPT ![b] = v]
  /\ \E c \in Configs : Live(c) /\ ncfg' = [ncfg EXCEPT ![b] = c]
  /\ phase' = [phase EXCEPT ![b] = "write"]
  /\ UNCHANGED <<rg, wg, rv, prom, votes, reads, lcfg, p1, wrote, crashed>>

Write(b, m) ==
  /\ phase[b] = "write"
  /\ m \notin wrote[b]
  /\ wg[m] < b
  /\ Check = "nocheck" \/ rg[m] <= b
  /\ wg' = [wg EXCEPT ![m] = b]
  /\ rv' = [rv EXCEPT ![m] = ncfg[b]]
  /\ wrote' = [wrote EXCEPT ![b] = @ \cup {m}]
  /\ UNCHANGED <<rg, prom, votes, phase, reads, lcfg, p1, ncfg, lval, crashed>>

EndWrite(b) ==
  /\ phase[b] = "write"
  /\ \E Q \in MQ : Q \subseteq wrote[b]
  /\ phase' = [phase EXCEPT ![b] = "p2"]
  /\ UNCHANGED <<rg, wg, rv, prom, votes, reads, lcfg, p1, ncfg, lval, wrote, crashed>>

Accept(b, a) ==
  /\ phase[b] = "p2"
  /\ a \in Mem[ncfg[b]] /\ a \notin crashed
  /\ prom[a] <= b
  /\ <<a, b, lval[b]>> \notin votes
  /\ prom' = [prom EXCEPT ![a] = b]
  /\ votes' = votes \cup {<<a, b, lval[b]>>}
  /\ UNCHANGED <<rg, wg, rv, phase, reads, lcfg, p1, ncfg, lval, wrote, crashed>>

\* A machine dies for good, only while every configuration in force keeps a
\* live Phase-1 quorum without it.
Crash(n) ==
  /\ Cardinality(crashed) < Crashes
  /\ n \notin crashed
  /\ \A c \in InForce : \E Q \in Q1s[c] : Q \cap (crashed \cup {n}) = {}
  /\ crashed' = crashed \cup {n}
  /\ UNCHANGED <<rg, wg, rv, prom, votes, phase, reads, lcfg, p1, ncfg, lval, wrote>>

Next ==
  \/ \E b \in Ballots, m \in Matchmakers : Read(b, m) \/ Write(b, m)
  \/ \E b \in Ballots : EndRead(b) \/ EndP1(b) \/ EndWrite(b)
  \/ \E b \in Ballots, a \in Acceptors : Promise(b, a) \/ Accept(b, a)
  \/ \E n \in Acceptors : Crash(n)

Spec == Init /\ [][Next]_vars

Chosen(v) == \E b \in Ballots : \E Q \in Q2s[ncfg[b]] :
               phase[b] = "p2" /\ \A a \in Q : <<a, b, v>> \in votes

Agreement == \A v, w \in Values : Chosen(v) /\ Chosen(w) => v = w

\* Liveness as an invariant: whatever a matchmaker majority answers, its
\* configuration still has a live Phase-1 quorum.
Electable == \A Q \in MQ : Live(MaxOf(Q))

\* Reach: a value is chosen after a reconfiguration and a crash.
NoChosenAfterCrash == ~(crashed # {} /\ \E v \in Values : Chosen(v) /\ \E b \in Ballots : ncfg[b] # C0 /\ phase[b] = "p2")
=============================================================================
