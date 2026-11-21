------------------------------ MODULE Abandon ------------------------------
(* A single-decree model of Matchmaker Paxos with values and votes, for the  *)
(* gone-node gap (2026-11-14). A registration above the GC floor puts its    *)
(* configuration in H_b of each later campaign. If that campaign was         *)
(* abandoned and its configuration lost its Phase-1 quorum, no later          *)
(* campaign wins. The rule under test lets a candidate skip a registration   *)
(* whose ballot provably never finished Phase 1: such a ballot holds no      *)
(* vote and can never get one (the idea of Vertical Paxos II, without a      *)
(* master).                                                                  *)
(*                                                                           *)
(* A campaign may register any configuration of Configs: a stale belief, a  *)
(* reconfiguration, or the configuration in force. It may stop at any time,  *)
(* before or after its first Prepare (a stale belief, a refusal, its owner   *)
(* gone for good). A ballot that stops takes no more step, which every      *)
(* behaviour already holds, so the model has no stop action.                 *)
(*                                                                           *)
(* Skip : "none"   = Phase 1 needs a quorum of each configuration in H_b.   *)
(*        "owner"  = the owner of ballot j promised the candidate, and its   *)
(*                   promise reports the highest ballot at which it sent a   *)
(*                   Prepare, durable before that Prepare: below j. A node   *)
(*                   never opens Phase 1 at a ballot below its own promise,  *)
(*                   so j never will.                                        *)
(*        "ownerNoGuard" = a control: the same without that guard.           *)
(*        "acceptor" = a candidate skips j when a set Ask of the acceptors  *)
(*                   of the configuration Anchor promised the candidate and  *)
(*                   none of them ever promised or accepted at j.            *)
(*        "both"   = "owner" or "acceptor".                                  *)
(* Ask  : "blocking" = a set that meets each Phase-1 quorum (a column of a   *)
(*                   grid whose Phase-1 quorums are rows). "quorum" = a      *)
(*                   Phase-1 quorum (a control on a grid).                   *)
(* Anchor : "floor" = the configuration of the leader at the highest GC      *)
(*                   watermark in the candidate's answers (C0 before any).  *)
(*          "newest" = a control: the newest reconfiguration in the answers. *)
(*                                                                           *)
(* KindSet : the kinds a campaign may register. Only Anchor = "newest"     *)
(*        reads the kind, so the other checks register beliefs alone.        *)
(*                                                                           *)
(* An acceptor that crashes and never comes back is one that takes no more  *)
(* step, which every behaviour already holds, so the model has no crash     *)
(* action. The GC is always on.                                              *)
(*                                                                           *)
(* Not in the model: matchmaker generations, lost durable state, wipes.     *)
EXTENDS Naturals, FiniteSets

CONSTANTS Acceptors, Matchmakers, Values, NB, Owner, Configs, C0, Mem, Q1s, Q2s,
          Skip, Ask, Anchor, KindSet

Ballots == 1..NB
Belief == "belief"
Reconf == "reconf"

VARIABLES status,  \* ballot -> "idle" | "matching" | "matched" | "preparing" | "leading"
          cfgOf,   \* ballot -> its configuration
          kind,    \* ballot -> Belief | Reconf
          reg,     \* matchmaker -> set of <<ballot, config, kind>>
          wm,      \* matchmaker -> its GC watermark
          wcfg,    \* matchmaker -> the configuration of the leader at its watermark
          replied, \* ballot -> the matchmakers that registered it
          hist,    \* ballot -> the union of the records in their answers
          hwm,     \* ballot -> the highest watermark in their answers
          hanc,    \* ballot -> the configuration at that watermark
          ap,      \* acceptor -> its promise
          seen,    \* acceptor -> the ballots it promised or accepted
          votes,   \* set of <<acceptor, ballot, value>>
          pmd,     \* ballot -> the acceptors that promised it
          pv,      \* ballot -> the votes that its promises reported
          val,     \* ballot -> the value it proposes
          prepd,   \* the ballots whose owner sent a Prepare
          skipped  \* the ballots that a candidate skipped

vars == <<status, cfgOf, kind, reg, wm, wcfg, replied, hist, hwm, hanc, ap, seen, votes,
          pmd, pv, val, prepd, skipped>>

Max(S) == CHOOSE x \in S : \A y \in S : y <= x
MMQuorums == {Q \in SUBSET Matchmakers : 2 * Cardinality(Q) > Cardinality(Matchmakers)}
Blocking(c) == {B \in SUBSET Mem[c] : \A Q \in Q1s[c] : B \cap Q # {}}
AskSets(c) == IF Ask = "blocking" THEN Blocking(c) ELSE Q1s[c]

Init == /\ status = [b \in Ballots |-> "idle"]
        /\ cfgOf = [b \in Ballots |-> C0]
        /\ kind = [b \in Ballots |-> Belief]
        /\ reg = [m \in Matchmakers |-> {<<0, C0, Reconf>>}]
        /\ wm = [m \in Matchmakers |-> 0]
        /\ wcfg = [m \in Matchmakers |-> C0]
        /\ replied = [b \in Ballots |-> {}]
        /\ hist = [b \in Ballots |-> {}]
        /\ hwm = [b \in Ballots |-> 0]
        /\ hanc = [b \in Ballots |-> C0]
        /\ ap = [a \in Acceptors |-> 0]
        /\ seen = [a \in Acceptors |-> {}]
        /\ votes = {}
        /\ pmd = [b \in Ballots |-> {}]
        /\ pv = [b \in Ballots |-> {}]
        /\ val = [b \in Ballots |-> CHOOSE v \in Values : TRUE]
        /\ prepd = {}
        /\ skipped = {}

\* The owner opens b with configuration c. Its own acceptor promises b first
\* (`campaign()` sets the promise before matchmaking). The owner's promise is
\* below b only while b is idle: the campaign raises it to b, and a promise
\* never goes down (`Monotone`).
Campaign(b, c, k) ==
    LET o == Owner[b] IN
    /\ ap[o] < b
    /\ status' = [status EXCEPT ![b] = "matching"]
    /\ cfgOf' = [cfgOf EXCEPT ![b] = c]
    /\ kind' = [kind EXCEPT ![b] = k]
    /\ ap' = [ap EXCEPT ![o] = b]
    /\ seen' = [seen EXCEPT ![o] = @ \cup {b}]
    /\ UNCHANGED <<reg, wm, wcfg, replied, hist, hwm, hanc, votes, pmd, pv, val, prepd,
                   skipped>>

\* A matchmaker registers b once: after it registered b, it holds b (or a
\* watermark above b), so the last two guards refuse b again.
Register(b, m) ==
    /\ status[b] = "matching"
    /\ b >= wm[m]
    /\ \A r \in reg[m] : r[1] < b
    /\ reg' = [reg EXCEPT ![m] = @ \cup {<<b, cfgOf[b], kind[b]>>}]
    /\ replied' = [replied EXCEPT ![b] = @ \cup {m}]
    /\ hist' = [hist EXCEPT ![b] = @ \cup reg[m]]
    /\ hwm' = [hwm EXCEPT ![b] = IF wm[m] > @ THEN wm[m] ELSE @]
    /\ hanc' = [hanc EXCEPT ![b] = IF wm[m] > hwm[b] THEN wcfg[m] ELSE @]
    /\ UNCHANGED <<status, cfgOf, kind, wm, wcfg, ap, seen, votes, pmd, pv, val, prepd,
                   skipped>>

MatchDone(b) ==
    /\ status[b] = "matching"
    /\ replied[b] \in MMQuorums
    /\ status' = [status EXCEPT ![b] = "matched"]
    /\ UNCHANGED <<cfgOf, kind, reg, wm, wcfg, replied, hist, hwm, hanc, ap, seen, votes,
                   pmd, pv, val, prepd, skipped>>

StartPrepare(b) ==
    /\ status[b] = "matched"
    /\ Skip # "ownerNoGuard" => ap[Owner[b]] <= b
    /\ status' = [status EXCEPT ![b] = "preparing"]
    /\ prepd' = prepd \cup {b}
    /\ UNCHANGED <<cfgOf, kind, reg, wm, wcfg, replied, hist, hwm, hanc, ap, seen, votes,
                   pmd, pv, val, skipped>>

\* A second promise of a to b changes nothing: a accepted no ballot since
\* (Accept at b needs b leading, below b needs a promise below b).
Promise(b, a) ==
    /\ status[b] = "preparing"
    /\ ap[a] <= b
    /\ ap' = [ap EXCEPT ![a] = b]
    /\ seen' = [seen EXCEPT ![a] = @ \cup {b}]
    /\ pmd' = [pmd EXCEPT ![b] = @ \cup {a}]
    /\ pv' = [pv EXCEPT ![b] = @ \cup {<<x[2], x[3]>> : x \in {y \in votes : y[1] = a}}]
    /\ UNCHANGED <<status, cfgOf, kind, reg, wm, wcfg, replied, hist, hwm, hanc, votes, val,
                   prepd, skipped>>

Newest(b) ==
    LET R == {r \in hist[b] : r[3] = Reconf}
    IN  IF R = {} THEN hanc[b] ELSE (CHOOSE r \in R : \A x \in R : x[1] <= r[1])[2]

AnchorOf(b) == IF Anchor = "floor" THEN hanc[b] ELSE Newest(b)

\* Candidate b may skip the registration x (never the one at the floor).
BySelf(b, x) ==
    /\ Skip \in {"owner", "ownerNoGuard", "both"}
    /\ Owner[x[1]] \in pmd[b]
    /\ x[1] \notin prepd
ByAcceptors(b, x) ==
    /\ Skip \in {"acceptor", "both"}
    /\ \E B \in AskSets(AnchorOf(b)) :
          /\ B \subseteq pmd[b]
          /\ \A a \in B : x[1] \notin seen[a]
MaySkip(b, x) == x[1] > hwm[b] /\ (BySelf(b, x) \/ ByAcceptors(b, x))

Phase1Done(b) ==
    /\ status[b] = "preparing"
    /\ LET X == {x \in hist[b] : x[1] >= hwm[b] /\ x[1] < b}
           S == {x \in X : MaySkip(b, x)}
       IN  /\ \A x \in X \ S : \E Q \in Q1s[x[2]] : Q \subseteq pmd[b]
           /\ skipped' = skipped \cup {x[1] : x \in S}
    /\ \E v \in Values :
          /\ pv[b] # {} =>
                v = (CHOOSE p \in pv[b] : \A q \in pv[b] : q[1] <= p[1])[2]
          /\ val' = [val EXCEPT ![b] = v]
    /\ status' = [status EXCEPT ![b] = "leading"]
    /\ UNCHANGED <<cfgOf, kind, reg, wm, wcfg, replied, hist, hwm, hanc, ap, seen, votes,
                   pmd, pv, prepd>>

Accept(b, a) ==
    /\ status[b] = "leading"
    /\ a \in Mem[cfgOf[b]]
    /\ ap[a] <= b
    /\ ap' = [ap EXCEPT ![a] = b]
    /\ seen' = [seen EXCEPT ![a] = @ \cup {b}]
    /\ votes' = votes \cup {<<a, b, val[b]>>}
    /\ UNCHANGED <<status, cfgOf, kind, reg, wm, wcfg, replied, hist, hwm, hanc, pmd, pv,
                   val, prepd, skipped>>

ChosenAt(b, v) == \E Q \in Q2s[cfgOf[b]] : \A a \in Q : <<a, b, v>> \in votes

\* Once the value of b is chosen at b, the watermark of m rises to b. The
\* leader of b sends it; a node that learned the choice may send it again.
GcStep(b, m) ==
    /\ ChosenAt(b, val[b])
    /\ wm[m] < b
    /\ wm' = [wm EXCEPT ![m] = b]
    /\ wcfg' = [wcfg EXCEPT ![m] = cfgOf[b]]
    /\ reg' = [reg EXCEPT ![m] = {r \in @ : r[1] >= b}]
    /\ UNCHANGED <<status, cfgOf, kind, replied, hist, hwm, hanc, ap, seen, votes, pmd, pv,
                   val, prepd, skipped>>

Next ==
    \/ \E b \in Ballots, c \in Configs, k \in KindSet : Campaign(b, c, k)
    \/ \E b \in Ballots, m \in Matchmakers : Register(b, m) \/ GcStep(b, m)
    \/ \E b \in Ballots : MatchDone(b) \/ StartPrepare(b)
                          \/ Phase1Done(b)
    \/ \E b \in Ballots, a \in Acceptors : Promise(b, a) \/ Accept(b, a)

Spec == Init /\ [][Next]_vars

Chosen == {v \in Values : \E b \in Ballots : ChosenAt(b, v)}

\* Safety: at most one value is chosen.
Agreement == Cardinality(Chosen) <= 1

\* A skipped ballot never leads: it would hold votes.
SkipSafe == \A j \in skipped : status[j] # "leading"

\* A ballot proposes one value: every vote at b is for the same value.
OneValue == \A x, y \in votes : x[2] = y[2] => x[3] = y[3]

\* Phase 2 addresses C_b alone: every vote at b comes from a member of C_b.
VotesInConfig == \A x \in votes : x[1] \in Mem[cfgOf[x[2]]]

\* A matchmaker holds the configuration and the kind that the ballot
\* campaigned with, and only for a ballot that campaigned.
RegTrue ==
    \A m \in Matchmakers : \A r \in reg[m] :
        r[1] > 0 => /\ status[r[1]] # "idle"
                    /\ r[2] = cfgOf[r[1]]
                    /\ r[3] = kind[r[1]]

\* A candidate knows every registration below it that a matchmaker of its
\* quorum holds: a matchmaker never registers b below a ballot it answered.
HistComplete ==
    \A c \in Ballots : \A m \in replied[c] : \A r \in reg[m] : r[1] < c => r \in hist[c]

\* The floor that a candidate learns is at or below its own ballot: a
\* matchmaker refuses a registration below its watermark.
FloorBelow == \A b \in Ballots : hwm[b] <= b

\* An acceptor promises only a ballot whose owner sent a Prepare: the owner
\* testimony of the skip rule counts on it.
PromisedPrepared == \A b \in Ballots : pmd[b] # {} => b \in prepd

\* A watermark sits at a ballot whose value is chosen, with its configuration.
WmChosen ==
    \A m \in Matchmakers : wm[m] > 0 => /\ ChosenAt(wm[m], val[wm[m]])
                                         /\ wcfg[m] = cfgOf[wm[m]]

Inv == /\ Agreement /\ SkipSafe /\ OneValue /\ VotesInConfig /\ RegTrue
       /\ HistComplete /\ FloorBelow /\ WmChosen /\ PromisedPrepared

\* An acceptor's promise and a matchmaker's watermark never go down, and a
\* ballot's life goes one way.
Rank(s) == CASE s = "idle" -> 0 [] s = "matching" -> 1 [] s = "matched" -> 2
             [] s = "preparing" -> 3 [] s = "leading" -> 4
Steps ==
    /\ \A a \in Acceptors : ap'[a] >= ap[a]
    /\ \A m \in Matchmakers : wm'[m] >= wm[m]
    /\ \A b \in Ballots : status'[b] = status[b] \/ Rank(status'[b]) > Rank(status[b])
Monotone == [][Steps]_vars

\* Not real invariants: TLC must find a skip, and a chosen value.
NoSkip == skipped = {}
NoChoice == Chosen = {}
=============================================================================
