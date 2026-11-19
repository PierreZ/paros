------------------------------ MODULE Matchmaker ------------------------------
(***************************************************************************)
(* Single-decree Matchmaker Paxos (Whittaker et al., "Matchmaker Paxos: A  *)
(* Reconfigurable Consensus Protocol", §3), as `paros-consensus` builds it *)
(* (`matchmaking.rs`, `matchmaker.rs`, `collector.rs`). #1200 (TLA+ model  *)
(* Matchmaker Paxos).                                                      *)
(*                                                                         *)
(* Each ballot has one proposer, and the proposer selects one              *)
(* configuration for its ballot. A reconfiguration is only a ballot whose  *)
(* configuration is new: a configuration is bound to its ballot and is     *)
(* never edited. A ballot runs three phases:                               *)
(*                                                                         *)
(*   1. Matchmaking. The proposer registers (b, C_b) at the matchmakers.   *)
(*      A matchmaker refuses b if it holds a registration at b or above,   *)
(*      or if b is below its GC watermark. Else it records (b, C_b) and    *)
(*      answers with its registrations below b and its watermark. The      *)
(*      proposer closes on the answers of a matchmaker quorum: the history *)
(*      H_b is the union of the answers, above the maximum watermark.      *)
(*   2. Phase 1 needs a promise quorum of every configuration in H_b. An   *)
(*      empty H_b needs no Phase 1.                                        *)
(*   3. Phase 2 addresses C_b alone.                                       *)
(*                                                                         *)
(* GC (§3.4, §3.5). The proposer of b asks the matchmakers to forget the   *)
(* ballots below b when its value is chosen at b (scenario 1) or when its  *)
(* Phase 1 found no vote (scenario 2). A matchmaker raises its watermark   *)
(* to b, deletes its registrations below b and acks. The floor b is        *)
(* effective once a matchmaker quorum acked it durably (`GcStep::          *)
(* Effective`). Only then may an acceptor that no registration at b or     *)
(* above names retire. A retired acceptor takes no more steps.             *)
(*                                                                         *)
(* A removed acceptor is not shut down: it keeps answering Phase 1 until   *)
(* it retires. The acceptor guards are pool-based: an acceptor answers a   *)
(* ballot whatever configuration it believes.                              *)
(*                                                                         *)
(* The retirement here takes the facts that it needs from the global       *)
(* state: no registration at the floor or above names the acceptor, and   *)
(* every matchmaker refuses a later registration that names it. That is    *)
(* the `allTomb` rule of `retire-fence/`, which checks how a node learns   *)
(* those facts.                                                            *)
(*                                                                         *)
(* Constants select the rules. The first value of each is the rule of the  *)
(* paper and of the code; each other value is a control.                   *)
(*   Phase1      "every": a Phase-1 quorum of each configuration in H_b.   *)
(*               "union": a majority of the union of H_b.                  *)
(*               "own": a majority of C_b alone.                           *)
(*   MatchCheck  TRUE: a matchmaker refuses b when it holds a registration *)
(*               at b or above. FALSE: it registers b anyway.              *)
(*   Histories   "quorum": the proposer closes on a matchmaker quorum.     *)
(*               "one": it closes on one answer.                           *)
(*   Reconfig    "round": Phase 2 addresses C_b. "edit": the proposer may  *)
(*               send its Phase 2 to another configuration, with no new    *)
(*               ballot.                                                   *)
(*   Gc          "paper": scenarios 1 and 2. "early": the proposer asks    *)
(*               for GC as soon as its Phase 1 is done. "off": no GC.      *)
(*   Retire      "effective": an acceptor retires above an effective       *)
(*               floor. "oneAck": the floor counts as effective at one     *)
(*               ack. "removed": an acceptor retires when the newest       *)
(*               configuration does not name it. "off": no retirement.     *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Acceptors,     \* the acceptor pool
          Configs,       \* the configurations a ballot can select
          Matchmakers,
          Values,
          NB,            \* the ballots are 1..NB
          Phase1, MatchCheck, Histories, Reconfig, Gc, Retire

ASSUME Phase1 \in {"every", "union", "own"}
ASSUME MatchCheck \in BOOLEAN
ASSUME Histories \in {"quorum", "one"}
ASSUME Reconfig \in {"round", "edit"}
ASSUME Gc \in {"paper", "early", "off"}
ASSUME Retire \in {"effective", "oneAck", "removed", "off"}
\* A configuration is not empty, so {} can stand for "none yet".
ASSUME \A c \in Configs : c # {} /\ c \subseteq Acceptors

Ballots == 1..NB
None == "none"

Maj(S) == {Q \in SUBSET S : 2 * Cardinality(Q) > Cardinality(S)}
\* Matchmaker quorums are majorities only.
MQuorums == Maj(Matchmakers)
Max(S) == CHOOSE x \in S : \A y \in S : y <= x

VARIABLES cfg,      \* ballot: its configuration, {} before matchmaking
          reg,      \* matchmaker: its registrations, a set of <<ballot, configuration>>
          wm,       \* matchmaker: its GC watermark
          answers,  \* ballot: the matchmaking answers, [mm, hist, w]
          matched,  \* ballot: TRUE once its matchmaking phase closed
          hist,     \* ballot: H_b, the configurations of its Phase 1
          p1,       \* ballot: "wait", "empty" (Phase 1 found no vote) or "found"
          prom,     \* acceptor: the highest ballot it promised or voted at
          vbal,     \* acceptor: the ballot of its vote, 0 when it has none
          vval,     \* acceptor: the value of its vote
          retired,  \* the acceptors that shut down
          msgs      \* every Paxos and GC message sent

vars == <<cfg, reg, wm, answers, matched, hist, p1, prom, vbal, vval, retired, msgs>>

Entry == Ballots \X Configs

Msg ==
  [type : {"1a"}, bal : Ballots]
    \cup [type : {"1b"}, acc : Acceptors, bal : Ballots, vbal : 0..NB, vval : Values \cup {None}]
    \cup [type : {"2a"}, bal : Ballots, val : Values, cfg : Configs]
    \cup [type : {"2b"}, acc : Acceptors, bal : Ballots, val : Values]
    \cup [type : {"ga"}, bal : Ballots]
    \cup [type : {"gb"}, mm : Matchmakers, bal : Ballots]

TypeOK ==
  /\ cfg \in [Ballots -> Configs \cup {{}}]
  /\ reg \in [Matchmakers -> SUBSET Entry]
  /\ wm \in [Matchmakers -> 0..NB]
  /\ answers \in [Ballots -> SUBSET [mm : Matchmakers, hist : SUBSET Entry, w : 0..NB]]
  /\ matched \in [Ballots -> BOOLEAN]
  /\ hist \in [Ballots -> SUBSET Configs]
  /\ p1 \in [Ballots -> {"wait", "empty", "found"}]
  /\ prom \in [Acceptors -> 0..NB]
  /\ vbal \in [Acceptors -> 0..NB]
  /\ vval \in [Acceptors -> Values \cup {None}]
  /\ retired \subseteq Acceptors
  /\ msgs \subseteq Msg

Init ==
  /\ cfg = [b \in Ballots |-> {}]
  /\ reg = [m \in Matchmakers |-> {}]
  /\ wm = [m \in Matchmakers |-> 0]
  /\ answers = [b \in Ballots |-> {}]
  /\ matched = [b \in Ballots |-> FALSE]
  /\ hist = [b \in Ballots |-> {}]
  /\ p1 = [b \in Ballots |-> "wait"]
  /\ prom = [a \in Acceptors |-> 0]
  /\ vbal = [a \in Acceptors |-> 0]
  /\ vval = [a \in Acceptors |-> None]
  /\ retired = {}
  /\ msgs = {}

Send(m) == msgs' = msgs \cup {m}

\* The history that the answers S give: their union above the maximum
\* watermark.
Union(S) ==
  LET w == Max({r.w : r \in S} \cup {0})
  IN {e[2] : e \in {e \in UNION {r.hist : r \in S} : e[1] >= w}}

\* A value is chosen at ballot b once a majority of the configuration of
\* its Phase 2 voted for it.
ChosenAt(b, v) ==
  \E m \in msgs :
    /\ m.type = "2a" /\ m.bal = b /\ m.val = v
    /\ \E Q \in Maj(m.cfg) : \A a \in Q : [type |-> "2b", acc |-> a, bal |-> b, val |-> v] \in msgs
Chosen(v) == \E b \in Ballots : ChosenAt(b, v)

(***************************************************************************)
(* Matchmaking                                                             *)
(***************************************************************************)

\* The proposer of ballot b selects its configuration and sends MatchA.
Select(b, c) ==
  /\ cfg[b] = {}
  /\ cfg' = [cfg EXCEPT ![b] = c]
  /\ UNCHANGED <<reg, wm, answers, matched, hist, p1, prom, vbal, vval, retired, msgs>>

\* Matchmaker m registers ballot b and answers with its registrations below b.
Register(m, b) ==
  /\ cfg[b] # {}
  /\ ~\E r \in answers[b] : r.mm = m
  /\ b >= wm[m]
  /\ MatchCheck => ~\E e \in reg[m] : e[1] >= b
  \* A matchmaker refuses a configuration that names a retired acceptor.
  /\ cfg[b] \cap retired = {}
  /\ reg' = [reg EXCEPT ![m] = @ \cup {<<b, cfg[b]>>}]
  /\ answers' = [answers EXCEPT ![b] = @ \cup
                   {[mm |-> m, hist |-> {e \in reg[m] : e[1] < b}, w |-> wm[m]]}]
  /\ UNCHANGED <<cfg, wm, matched, hist, p1, prom, vbal, vval, retired, msgs>>

\* The proposer of ballot b closes its matchmaking phase and opens Phase 1.
CloseMatch(b) ==
  /\ ~matched[b]
  /\ \E S \in SUBSET answers[b] :
       /\ IF Histories = "quorum" THEN {r.mm : r \in S} \in MQuorums ELSE Cardinality(S) = 1
       /\ hist' = [hist EXCEPT ![b] = Union(S)]
  /\ matched' = [matched EXCEPT ![b] = TRUE]
  /\ Send([type |-> "1a", bal |-> b])
  /\ UNCHANGED <<cfg, reg, wm, answers, p1, prom, vbal, vval, retired>>

(***************************************************************************)
(* Phase 1 and Phase 2                                                     *)
(***************************************************************************)

\* The configurations whose acceptors Phase 1 of ballot b asks.
Asked(b) == IF Phase1 = "own" THEN {cfg[b]} ELSE hist[b]

\* Acceptor a promises ballot b and reports its vote. The proposer sends its
\* 1a to the acceptors of the configurations that it asks. The acceptor's
\* guard is the pool, not a configuration: a removed acceptor answers until
\* it retires.
Promise(a, b) ==
  /\ a \notin retired
  /\ [type |-> "1a", bal |-> b] \in msgs
  /\ a \in UNION Asked(b)
  /\ prom[a] < b
  /\ prom' = [prom EXCEPT ![a] = b]
  /\ Send([type |-> "1b", acc |-> a, bal |-> b, vbal |-> vbal[a], vval |-> vval[a]])
  /\ UNCHANGED <<cfg, reg, wm, answers, matched, hist, p1, vbal, vval, retired>>

\* The promises acc close Phase 1 of ballot b.
Phase1Done(b, acc) ==
  CASE Phase1 = "every" -> \A c \in hist[b] : \E Q \in Maj(c) : Q \subseteq acc
    [] Phase1 = "union" -> hist[b] = {} \/ \E Q \in Maj(UNION hist[b]) : Q \subseteq acc
    [] Phase1 = "own" -> \E Q \in Maj(cfg[b]) : Q \subseteq acc

\* The value that a proposer may select from the promises S (P2c).
Selectable(S) ==
  LET rep == {m \in S : m.vbal > 0}
      top == CHOOSE m \in rep : \A n \in rep : n.vbal <= m.vbal
  IN IF rep = {} THEN Values ELSE {top.vval}

\* The proposer of ballot b closes Phase 1 on the promises S, one for each
\* acceptor that it asked, and opens Phase 2 in C_b.
Propose(b) ==
  /\ matched[b]
  /\ p1[b] = "wait"
  /\ \E S \in SUBSET {m \in msgs : m.type = "1b" /\ m.bal = b} :
       /\ \A m, n \in S : m.acc = n.acc => m = n
       /\ Phase1Done(b, {m.acc : m \in S})
       /\ p1' = [p1 EXCEPT ![b] = IF \E m \in S : m.vbal > 0 THEN "found" ELSE "empty"]
       /\ \E v \in Selectable(S), c \in IF Reconfig = "edit" THEN Configs ELSE {cfg[b]} :
            Send([type |-> "2a", bal |-> b, val |-> v, cfg |-> c])
  /\ UNCHANGED <<cfg, reg, wm, answers, matched, hist, prom, vbal, vval, retired>>

\* Acceptor a votes for the value of ballot b.
Accept(a, b) ==
  /\ a \notin retired
  /\ prom[a] <= b
  /\ vbal[a] # b
  /\ \E m \in msgs :
       /\ m.type = "2a" /\ m.bal = b /\ a \in m.cfg
       /\ prom' = [prom EXCEPT ![a] = b]
       /\ vbal' = [vbal EXCEPT ![a] = b]
       /\ vval' = [vval EXCEPT ![a] = m.val]
       /\ Send([type |-> "2b", acc |-> a, bal |-> b, val |-> m.val])
  /\ UNCHANGED <<cfg, reg, wm, answers, matched, hist, p1, retired>>

(***************************************************************************)
(* Garbage collection and retirement                                       *)
(***************************************************************************)

\* The proposer of ballot b asks the matchmakers to forget the ballots below b.
GcAsk(b) ==
  /\ [type |-> "ga", bal |-> b] \notin msgs
  /\ CASE Gc = "paper" -> p1[b] = "empty" \/ \E v \in Values : ChosenAt(b, v)
       [] Gc = "early" -> p1[b] # "wait"
       [] Gc = "off" -> FALSE
  /\ Send([type |-> "ga", bal |-> b])
  /\ UNCHANGED <<cfg, reg, wm, answers, matched, hist, p1, prom, vbal, vval, retired>>

\* Matchmaker m raises its watermark to b, forgets what is below, and acks.
GcAck(m, b) ==
  /\ [type |-> "ga", bal |-> b] \in msgs
  /\ [type |-> "gb", mm |-> m, bal |-> b] \notin msgs
  /\ LET w == IF b > wm[m] THEN b ELSE wm[m]
     IN /\ wm' = [wm EXCEPT ![m] = w]
        /\ reg' = [reg EXCEPT ![m] = {e \in @ : e[1] >= w}]
  /\ Send([type |-> "gb", mm |-> m, bal |-> b])
  /\ UNCHANGED <<cfg, answers, matched, hist, p1, prom, vbal, vval, retired>>

\* The floor f holds at the matchmakers Q.
AckedBy(f, Q) == \A m \in Q : [type |-> "gb", mm |-> m, bal |-> f] \in msgs

\* The floors that count as effective.
Effective ==
  {f \in Ballots :
     IF Retire = "oneAck" THEN \E m \in Matchmakers : AckedBy(f, {m})
                          ELSE \E Q \in MQuorums : AckedBy(f, Q)}

\* No registration at f or above names the acceptor a.
Unnamed(a, f) == \A m \in Matchmakers : \A e \in reg[m] : e[1] >= f => a \notin e[2]

\* The newest configuration that a matchmaking phase gave.
Newest == LET b == Max({x \in Ballots : matched[x]}) IN cfg[b]

\* Acceptor a shuts down.
Shutdown(a) ==
  /\ a \notin retired
  /\ CASE Retire \in {"effective", "oneAck"} -> \E f \in Effective : Unnamed(a, f)
       [] Retire = "removed" -> (\E b \in Ballots : matched[b]) /\ a \notin Newest
       [] Retire = "off" -> FALSE
  /\ retired' = retired \cup {a}
  /\ UNCHANGED <<cfg, reg, wm, answers, matched, hist, p1, prom, vbal, vval, msgs>>

Next ==
  \/ \E b \in Ballots, c \in Configs : Select(b, c)
  \/ \E m \in Matchmakers, b \in Ballots : Register(m, b) \/ GcAck(m, b)
  \/ \E b \in Ballots : CloseMatch(b) \/ Propose(b) \/ GcAsk(b)
  \/ \E a \in Acceptors, b \in Ballots : Promise(a, b) \/ Accept(a, b)
  \/ \E a \in Acceptors : Shutdown(a)

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Properties                                                              *)
(***************************************************************************)

\* Safety: one value is chosen.
Agreement == \A v, w \in Values : Chosen(v) /\ Chosen(w) => v = w

\* The history that the matchmaker quorum Q would give a new ballot above
\* every registration.
HistoryOf(Q) ==
  LET w == Max({wm[m] : m \in Q})
  IN {e[2] : e \in {e \in UNION {reg[m] : m \in Q} : e[1] >= w}}

\* No retirement strands a new ballot: whatever matchmaker quorum it hears,
\* each configuration of its history keeps a Phase-1 quorum that did not
\* retire.
Electable ==
  \A Q \in MQuorums : \A c \in HistoryOf(Q) : \E P \in Maj(c) : P \cap retired = {}

\* Reachability controls: each must be violated.
NoChoice == ~\E v \in Values : Chosen(v)
\* A value is chosen above a ballot whose configuration named an acceptor
\* that retired.
NoChoiceAboveRetired ==
  ~\E v \in Values, b, x \in Ballots, a \in retired :
     x < b /\ cfg[x] # {} /\ a \in cfg[x] /\ ChosenAt(b, v)
=============================================================================
