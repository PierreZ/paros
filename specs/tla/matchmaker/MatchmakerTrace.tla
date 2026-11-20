-------------------------- MODULE MatchmakerTrace --------------------------
(***************************************************************************)
(* The trace spec of `Matchmaker`: the model, forced to follow the events  *)
(* that one walk of the `consensus-sim` cluster recorded on a flavor with  *)
(* matchmakers (#1198 (trace checking),                                    *)
(* `crates/sim/consensus-sim/src/cluster/trace/matchmaker.rs`). `l` is the *)
(* index of the next event.                                                *)
(*                                                                         *)
(* `Matchmaker` decides one value; the cluster runs a log. The trace spec  *)
(* takes the matchmaking and the GC from the model, and writes the log as  *)
(* its own steps, with the rules of `multi-paxos/MultiPaxos.tla` and the   *)
(* configurations of the model:                                            *)
(*   - "select": the owner of a ballot selected its configuration and     *)
(*     sent its first registration (`Select`);                             *)
(*   - "register": a matchmaker registered a ballot (`Register`);          *)
(*   - "prepare": the owner closed its matchmaking on the answers of a     *)
(*     matchmaker quorum (`CloseMatch`) and opened Phase 1 from its first  *)
(*     unlearned slot, its own promise first, as in `MultiPaxos`'s         *)
(*     `Prepare`. `hist` is the code's `H_b`: the model's history, less    *)
(*     the configurations that the code drops (see below);                 *)
(*   - "promise": an acceptor of a configuration of `H_b` or of `C_b`      *)
(*     promised and reported its votes (the code sends its `Prepare` to    *)
(*     both);                                                              *)
(*   - "win": a Phase-1 quorum of every configuration of `H_b` promised    *)
(*     (`Phase1Done` with `Phase1 = "every"`), the owner among them; the   *)
(*     recovery and the frontier are `MultiPaxos`'s `Win`;                 *)
(*   - "propose", "accept", "record", "learn", "restart": as in            *)
(*     `multi-paxos/MultiPaxosTrace.tla`. An acceptor votes only at a      *)
(*     ballot whose configuration names it, or at its own ballot: a leader *)
(*     records each round it opens (`record_own_round`), and a vote that   *)
(*     `C_b` does not name counts toward no quorum. A node that `C_b` does *)
(*     not name writes a record at `b` only for a value chosen at `b` or   *)
(*     below: it learned the value (`mark_chosen`) from a `Commit` of `b`  *)
(*     above its promise, so the walk sees an "accept". By P2c every       *)
(*     proposal at or above the choosing ballot carries the same value, so *)
(*     the record is safe. A value is chosen at a ballot once a Phase-2    *)
(*     quorum of the configuration of that ballot voted for it;            *)
(*   - "gcask": the leader asks the matchmakers to forget the ballots      *)
(*     below its own (`GcAsk`). The model's guard is single-decree (a      *)
(*     chosen value, or a Phase 1 that found no vote). The log's guard is  *)
(*     the one of `node/gc.rs`: the leader won, and a Phase-2 quorum of    *)
(*     `C_b` learned every slot below the frontier of its Phase 1;         *)
(*   - "gcack": a matchmaker raised its watermark (`GcAck`).               *)
(*                                                                         *)
(* The code drops from `H_b` a configuration that only abandoned ballots   *)
(* registered (the gone-node rule, `Election::drop_abandoned`): the owner  *)
(* of each such ballot promised `b` and never prepared the ballot. The     *)
(* model does not have that rule. The trace spec checks it where the code  *)
(* uses it: in `hist` and in the win.                                      *)
(*                                                                         *)
(* A step down is no step here: a leader that stepped down sends nothing   *)
(* more. The single-decree variables of the model (`prom`, `vbal`, `vval`, *)
(* `p1`) and the retirement do not move. Ballots are the ranks of the      *)
(* code's `(round, node)` ballots in the run, from 1. Nodes, matchmakers   *)
(* and commands are strings; `Noop` is "noop".                             *)
(***************************************************************************)
EXTENDS Matchmaker, Trace, Naturals, Sequences, FiniteSets, TLC

VARIABLES l,
          lprom,   \* acceptor: the promise of the log
          lvote,   \* acceptor: its vote, per slot
          learned, \* node: the value it learned, per slot
          lphase,  \* ballot: "idle", "p1", "p2" or "dead"
          lnext,   \* ballot: the allocator's next slot
          lfront,  \* ballot: the frontier its Phase 1 derived
          lmsgs    \* every message of the log

logVars == <<lprom, lvote, learned, lphase, lnext, lfront, lmsgs>>

e == Trace[l]

Slots == 0..(TraceNS - 1)
Owner == TraceOwner
Vote == [bal : 0..NB, val : Values \cup {None}]
NoVote == [bal |-> 0, val |-> None]

TraceInit ==
  /\ Init
  /\ l = 1
  /\ lprom = [a \in Acceptors |-> 0]
  /\ lvote = [a \in Acceptors |-> [s \in Slots |-> NoVote]]
  /\ learned = [a \in Acceptors |-> [s \in Slots |-> None]]
  /\ lphase = [b \in Ballots |-> "idle"]
  /\ lnext = [b \in Ballots |-> 0]
  /\ lfront = [b \in Ballots |-> 0]
  /\ lmsgs = {}

\* A malformed event is a rejection, not a TLC error.
Has(f) == f \in DOMAIN e
IsNode(f) == Has(f) /\ e[f] \in Acceptors
IsMatchmaker == Has("mm") /\ e.mm \in Matchmakers
IsBallot == Has("bal") /\ e.bal \in Ballots
IsSlot == Has("slot") /\ e.slot \in Slots
IsValue == Has("val") /\ e.val \in Values

-----------------------------------------------------------------------------
(* The log *)

\* The first slot node n has not learned: where its campaign starts.
FirstUnlearned(n) ==
  IF \A s \in Slots : learned[n][s] # None THEN TraceNS
  ELSE CHOOSE s \in Slots : learned[n][s] = None /\ \A t \in Slots : t < s => learned[n][t] # None

\* The promise of acceptor a at ballot b: each vote at or above `from`.
PromiseMsg(a, b, from) ==
  [type |-> "1b", acc |-> a, bal |-> b, from |-> from,
   rep |-> [s \in Slots |-> IF s >= from THEN lvote[a][s] ELSE NoVote]]

\* The vote at slot s that a promise reports.
Reported(s) ==
  IF \E r \in e.rep : r.slot = s
  THEN LET r == CHOOSE r \in e.rep : r.slot = s IN [bal |-> r.bal, val |-> r.val]
  ELSE NoVote

\* The highest vote that the promises S report at slot s.
Top(S, s) ==
  LET rep == {m \in S : m.rep[s].bal > 0}
  IN IF rep = {} THEN NoVote
     ELSE (CHOOSE m \in rep : \A n \in rep : n.rep[s].bal <= m.rep[s].bal).rep[s]

\* One past the highest slot that the promises S report, never below `from`.
Frontier(S, from) ==
  LET h == {s \in Slots : s >= from /\ \E m \in S : m.rep[s].bal > 0}
  IN IF h = {} THEN from ELSE (CHOOSE s \in h : \A t \in h : t <= s) + 1

\* The 2a messages of the recovery from `from` to the frontier f: each
\* reported slot again, and a `Noop` in each gap.
Recovery(S, b, from, f) ==
  {[type |-> "2a", bal |-> b, slot |-> s,
    val |-> IF Top(S, s).bal > 0 THEN Top(S, s).val ELSE "noop"] : s \in from..(f - 1)}

\* The Phase-1 and the Phase-2 quorums of the log under configuration c:
\* its majorities. `MatchmakerGridTrace` replaces both with the quorums of
\* a grid over c.
P1Quorums(c) == Maj(c)
P2Quorums(c) == Maj(c)

\* A value is chosen at slot s and ballot b once a Phase-2 quorum of C_b
\* voted for it at b.
LChosenAt(b, s, v) ==
  \E Q \in P2Quorums(cfg[b]) : \A a \in Q :
    [type |-> "2b", acc |-> a, bal |-> b, slot |-> s, val |-> v] \in lmsgs
LChosen(s, v) == \E b \in Ballots : LChosenAt(b, s, v)

\* Configuration c of the history of ballot b that only abandoned ballots
\* registered: the owner of each such ballot x promised b in the messages
\* M (so it never prepares x), and never prepared x.
Abandoned(b, c, M) ==
  \A r \in answers[b] : \A x \in {en[1] : en \in {en \in r.hist : en[2] = c}} :
    /\ [type |-> "1a", bal |-> x] \notin msgs
    /\ \E m \in M : m.type = "1b" /\ m.acc = Owner[x] /\ m.bal = b

-----------------------------------------------------------------------------
(* The steps *)

TPrepare ==
  /\ e.event = "prepare" /\ IsBallot /\ IsNode("owner") /\ Has("from") /\ Has("hist")
  /\ Owner[e.bal] = e.owner
  /\ CloseMatch(e.bal)
  /\ LET b == e.bal
         o == e.owner
         from == FirstUnlearned(o)
     IN /\ e.from = from
        /\ lphase[b] = "idle"
        /\ lprom[o] < b
        /\ lphase' = [lphase EXCEPT ![b] = "p1"]
        /\ lprom' = [lprom EXCEPT ![o] = b]
        /\ lmsgs' = lmsgs \cup {[type |-> "1a", bal |-> b, from |-> from], PromiseMsg(o, b, from)}
        /\ e.hist \subseteq hist'[b]
        /\ \A c \in hist'[b] \ e.hist : Abandoned(b, c, lmsgs')
  /\ UNCHANGED <<lvote, learned, lnext, lfront>>

TPromise ==
  /\ e.event = "promise" /\ IsNode("acc") /\ IsBallot /\ Has("from") /\ Has("rep")
  /\ LET a == e.acc
         b == e.bal
     IN /\ [type |-> "1a", bal |-> b, from |-> e.from] \in lmsgs
        /\ a \in UNION hist[b] \cup cfg[b]
        /\ lprom[a] < b
        /\ \A s \in Slots : s >= e.from => lvote[a][s] = Reported(s)
        /\ \A r \in e.rep : r.slot \in Slots /\ r.slot >= e.from
        /\ lprom' = [lprom EXCEPT ![a] = b]
        /\ lmsgs' = lmsgs \cup {PromiseMsg(a, b, e.from)}
  /\ UNCHANGED <<lvote, learned, lphase, lnext, lfront>>
  /\ UNCHANGED vars

TWin ==
  /\ e.event = "win" /\ IsBallot /\ Has("front")
  /\ LET b == e.bal IN
     /\ lphase[b] = "p1"
     /\ \E S \in SUBSET {m \in lmsgs : m.type = "1b" /\ m.bal = b} :
          LET acc == {m.acc : m \in S}
              from == (CHOOSE m \in lmsgs : m.type = "1a" /\ m.bal = b).from
              f == Frontier(S, from)
          IN /\ \A m, n \in S : m.acc = n.acc => m = n
             /\ Owner[b] \in acc
             /\ \A c \in hist[b] : Abandoned(b, c, lmsgs) \/ \E Q \in P1Quorums(c) : Q \subseteq acc
             /\ f = e.front
             /\ lfront' = [lfront EXCEPT ![b] = f]
             /\ lnext' = [lnext EXCEPT ![b] = f]
             /\ lmsgs' = lmsgs \cup Recovery(S, b, from, f)
     /\ lphase' = [lphase EXCEPT ![b] = "p2"]
  /\ UNCHANGED <<lprom, lvote, learned>>
  /\ UNCHANGED vars

TPropose ==
  /\ e.event = "propose" /\ IsBallot /\ IsSlot /\ IsValue
  /\ LET m == [type |-> "2a", bal |-> e.bal, slot |-> e.slot, val |-> e.val] IN
     \/ /\ lphase[e.bal] = "p2"
        /\ lnext[e.bal] = e.slot
        /\ lnext' = [lnext EXCEPT ![e.bal] = @ + 1]
        /\ lmsgs' = lmsgs \cup {m}
        /\ UNCHANGED <<lprom, lvote, learned, lphase, lfront>>
     \/ /\ m \in lmsgs
        /\ UNCHANGED logVars
  /\ UNCHANGED vars

TAccept ==
  /\ e.event = "accept" /\ IsNode("acc") /\ IsBallot /\ IsSlot /\ IsValue
  /\ LET a == e.acc
         b == e.bal
         v == [bal |-> b, val |-> e.val]
     IN /\ lprom[a] <= b
        /\ lvote[a][e.slot] # v
        /\ lvote' = [lvote EXCEPT ![a][e.slot] = v]
        /\ IF a \in cfg[b] \/ a = Owner[b]
           THEN /\ [type |-> "2a", bal |-> b, slot |-> e.slot, val |-> e.val] \in lmsgs
                /\ lprom' = [lprom EXCEPT ![a] = b]
                /\ lmsgs' = lmsgs \cup {[type |-> "2b", acc |-> a, bal |-> b, slot |-> e.slot, val |-> e.val]}
           ELSE /\ \E c \in 1..b : LChosenAt(c, e.slot, e.val)
                /\ UNCHANGED <<lprom, lmsgs>>
  /\ UNCHANGED <<learned, lphase, lnext, lfront>>
  /\ UNCHANGED vars

\* A node keeps a value chosen at ballot b or below as its record of slot s
\* at b, below its promise (`mark_chosen` keeps the ballot of the `Commit`
\* it learned the value from). It is no vote: it sends no 2b.
TRecord ==
  /\ e.event = "record" /\ IsNode("node") /\ IsBallot /\ IsSlot /\ IsValue
  /\ \E c \in 1..e.bal : LChosenAt(c, e.slot, e.val)
  /\ lvote' = [lvote EXCEPT ![e.node][e.slot] = [bal |-> e.bal, val |-> e.val]]
  /\ UNCHANGED <<lprom, learned, lphase, lnext, lfront, lmsgs>>
  /\ UNCHANGED vars

\* A node learns one slot. A slot learned again is no step.
TLearn ==
  /\ e.event = "learn" /\ IsNode("node") /\ IsSlot /\ IsValue
  /\ IF learned[e.node][e.slot] = e.val THEN UNCHANGED logVars
     ELSE /\ learned[e.node][e.slot] = None
          /\ LChosen(e.slot, e.val)
          /\ learned' = [learned EXCEPT ![e.node][e.slot] = e.val]
          /\ UNCHANGED <<lprom, lvote, lphase, lnext, lfront, lmsgs>>
  /\ UNCHANGED vars

\* A node boots again from its disk: its campaigns die, and so does the
\* ballot it led (`led`, 0 for none; a ballot it handed on lives on at its
\* successor). The walk records only
\* what reached the disk, so memory is the disk. The code writes its chosen
\* index relaxed (`MustSync::Relaxed`), so a crash may lose a part of the
\* learned prefix: the node keeps only the slots below `upto`, and it
\* learned each of them before.
TRestart ==
  /\ e.event = "restart" /\ IsNode("node") /\ Has("upto") /\ Has("led")
  /\ \A s \in Slots : s < e.upto => learned[e.node][s] # None
  /\ learned' = [learned EXCEPT ![e.node] = [s \in Slots |-> IF s >= e.upto THEN None ELSE @[s]]]
  /\ lphase' = [b \in Ballots |->
                  IF \/ Owner[b] = e.node /\ lphase[b] = "p1"
                     \/ b = e.led /\ lphase[b] = "p2"
                  THEN "dead" ELSE lphase[b]]
  /\ UNCHANGED <<lprom, lvote, lnext, lfront, lmsgs>>
  /\ UNCHANGED vars

\* The leader of ballot b asks for GC: the guard of `node/gc.rs` for a log.
TGcAsk ==
  /\ e.event = "gcask" /\ IsBallot
  /\ LET b == e.bal IN
     /\ lphase[b] = "p2"
     /\ \E Q \in P2Quorums(cfg[b]) : \A a \in Q, s \in Slots : s < lfront[b] => learned[a][s] # None
     /\ [type |-> "ga", bal |-> b] \notin msgs
     /\ Send([type |-> "ga", bal |-> b])
  /\ UNCHANGED <<cfg, reg, wm, answers, matched, hist, p1, prom, vbal, vval, retired>>
  /\ UNCHANGED logVars

Step ==
  \/ /\ e.event = "select" /\ IsBallot /\ Has("cfg") /\ e.cfg \in Configs
     /\ Select(e.bal, e.cfg)
     /\ UNCHANGED logVars
  \/ /\ e.event = "register" /\ IsMatchmaker /\ IsBallot
     /\ Register(e.mm, e.bal)
     /\ UNCHANGED logVars
  \/ TPrepare
  \/ TPromise
  \/ TWin
  \/ TPropose
  \/ TAccept
  \/ TRecord
  \/ TLearn
  \/ TRestart
  \/ TGcAsk
  \/ /\ e.event = "gcack" /\ IsMatchmaker /\ IsBallot
     /\ GcAck(e.mm, e.bal)
     /\ UNCHANGED logVars

TraceNext == l <= Len(Trace) /\ Step /\ l' = l + 1

TraceSpec == TraceInit /\ [][TraceNext]_<<l, vars, logVars>>

-----------------------------------------------------------------------------
(* Safety *)

\* One command for each slot.
LogAgreement == \A s \in Slots, v, w \in Values : LChosen(s, v) /\ LChosen(s, w) => v = w

\* What a node learned is what was chosen.
LogLearnedChosen ==
  \A n \in Acceptors, s \in Slots : learned[n][s] # None => LChosen(s, learned[n][s])

\* Violated exactly when a behaviour matched every event: the trace is
\* accepted.
TraceUnfinished == l <= Len(Trace)
=============================================================================
