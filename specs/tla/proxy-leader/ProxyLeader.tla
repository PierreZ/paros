----------------------------- MODULE ProxyLeader -----------------------------
(***************************************************************************)
(* The proxy leader of Compartmentalized Paxos as `paros-consensus` builds *)
(* it (`proxy_leader.rs`): the leader sequences, and a proxy runs the     *)
(* Phase 2 of a slot for it. Rework 2.9, #1188 (TLA+ model proxy-leader). *)
(*                                                                         *)
(* A proxy decides nothing: it votes on nothing and adopts no ballot. It   *)
(* only fans the `Accept` out, folds the `Accepted`s and announces the     *)
(* `Commit`. A learner believes a `Commit` with no check of its own        *)
(* (`node/learn.rs`, `on_commit`), so the safety of the log rests on one   *)
(* rule of the proxy: it sends `Commit` only on a Phase-2 quorum of the    *)
(* configuration that the round's `Accept` carried, at the round's ballot. *)
(*                                                                         *)
(* A message is never lost from `msgs`, so a message can arrive late, two  *)
(* times, or never. A re-send is the same message, so the model needs no   *)
(* re-send step: a proxy can read again any delegation in `msgs`.          *)
(*                                                                         *)
(* What the model has, and the code it follows:                            *)
(*   - Leaders, proxies and acceptors are three sets, as in the            *)
(*     compartmentalized deployment.                                       *)
(*   - Each ballot has one owner and one configuration `Cfg[b]`: a         *)
(*     reconfiguration is a round change (`ColocatedNode::reconfigure`).   *)
(*     Configuration 0 is the bootstrap configuration, the one a proxy     *)
(*     holds when it boots (`ProxyLeader::new`).                           *)
(*   - Phase 1 of ballot b asks a Phase-1 quorum of the configuration of   *)
(*     every lower ballot (`H_b` of Matchmaker Paxos, with no GC). This    *)
(*     Phase 1 is the seal of the older configurations: an acceptor that   *)
(*     promised b refuses every lower `Accept` (rule 1 seals the same way, *)
(*     `docs/architecture/04-roles.md`).                                   *)
(*   - A slot with no reported vote gets any command; a slot with one gets *)
(*     the highest reported vote again.                                    *)
(*   - The leader delegates a slot to a proxy (`Message::Accept` with      *)
(*     `reply_to = Party::Proxy`). It may delegate it again to another     *)
(*     proxy (`ColocatedNode::move_round`) while no `Commit` of the slot   *)
(*     reached it. A `Commit` can be late or lost, so the leader may move  *)
(*     a slot that a proxy already committed: the model lets it move a     *)
(*     slot at any time.                                                   *)
(*   - A proxy works for the highest ballot it was handed: a higher one    *)
(*     closes every lower round (`close_below`), a lower one is ignored.   *)
(*     It learns the configuration from the delegation (`learn_config`:    *)
(*     only a higher ballot moves it). It remembers the rounds it decided  *)
(*     (`done`) and ignores their re-delegation.                           *)
(*   - A proxy crash loses every round: the proxy boots empty with the     *)
(*     bootstrap configuration (nothing durable, `ProxyReady`).            *)
(*   - A `Nack` only closes a round, so the model leaves it out: a round   *)
(*     that stays open decides nothing more. The leader's take-back (it    *)
(*     decides the round itself, `take_back_delegated`) is the leader's    *)
(*     own tally of `multi-paxos/`; here a round ends at a proxy.          *)
(*                                                                         *)
(* Constants:                                                              *)
(*   Cfg     the configuration of each ballot, 0..NB (0: the bootstrap).   *)
(*   Members, Q1, Q2  for each configuration: its acceptors, its Phase-1   *)
(*           quorums, and its Phase-2 quorums for each slot (`column_of`). *)
(*   Tally   "round": the rule. A proxy decides a round over the           *)
(*           configuration that the round's `Accept` carried.              *)
(*           "belief": it decides over the configuration it believes now.  *)
(*   Hear    "delegation": a proxy learns a configuration only from a      *)
(*           delegation, which closes every lower round (the code today).  *)
(*           "any": it also learns the configuration of any ballot that    *)
(*           started (as from the register of rule 1), and keeps its       *)
(*           rounds open. With Tally = "belief" this is the control of     *)
(*           finding 10: a proxy counts over a configuration it did not    *)
(*           check.                                                        *)
(*   Count   "quorum": a proxy decides on a Phase-2 quorum of votes.       *)
(*           "short": a control, it decides one vote short of a quorum,    *)
(*           and the learners believe the `Commit`.                        *)
(*   Move    TRUE: the leader may delegate a slot again to another proxy.  *)
(*           FALSE: a control, a slot only goes to `ProxyOf[s]`.           *)
(*   Crashes how many proxy crashes may happen (in the whole run).         *)
(*   Down    the proxies that never take a step (liveness checks only).    *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Acceptors, Leaders, Proxies, Commands, NB, NS, Owner, Cfg,
          Members, Q1, Q2, ProxyOf, Tally, Hear, Count, Move, Crashes, Down

Ballots == 1..NB
Slots == 0..(NS - 1)
Configs == DOMAIN Members
None == "none"

ASSUME Tally \in {"round", "belief"}
ASSUME Hear \in {"delegation", "any"}
ASSUME Count \in {"quorum", "short"}
ASSUME Move \in BOOLEAN
ASSUME Down \subseteq Proxies
ASSUME Owner \in [Ballots -> Leaders]
ASSUME Cfg \in [0..NB -> Configs]
ASSUME ProxyOf \in [Slots -> Proxies]
ASSUME \A k \in Configs : Members[k] \subseteq Acceptors
ASSUME \A k \in Configs : \A Q \in Q1[k] : Q \subseteq Members[k]
ASSUME \A k \in Configs, s \in Slots : \A Q \in Q2[k][s] : Q \subseteq Members[k]

Vote == [bal : 0..NB, val : Commands \cup {None}]
NoVote == [bal |-> 0, val |-> None]
Round == [bal : 0..NB, val : Commands \cup {None}]
NoRound == [bal |-> 0, val |-> None]

VARIABLES prom,     \* acceptor: its promise
          vote,     \* acceptor: its vote, per slot
          phase,    \* ballot: "idle", "p1" or "p2"
          safe,     \* ballot: per slot, the value its Phase 1 must propose
          prop,     \* ballot: per slot, the value it proposed
          pbal,     \* proxy: the highest ballot it was handed
          pcfg,     \* proxy: the ballot whose configuration it believes
          round,    \* proxy: per slot, its open round
          done,     \* proxy: per slot, the ballot it decided the slot at
          crashes,  \* how many proxy crashes happened
          msgs      \* every message sent

vars == <<prom, vote, phase, safe, prop, pbal, pcfg, round, done, crashes, msgs>>

Msg ==
  [type : {"1a"}, bal : Ballots]
    \cup [type : {"1b"}, acc : Acceptors, bal : Ballots, rep : [Slots -> Vote]]
    \cup [type : {"del"}, bal : Ballots, slot : Slots, val : Commands, proxy : Proxies]
    \cup [type : {"2a"}, bal : Ballots, slot : Slots, val : Commands]
    \cup [type : {"2b"}, acc : Acceptors, bal : Ballots, slot : Slots, val : Commands]
    \cup [type : {"commit"}, bal : Ballots, slot : Slots, val : Commands]

TypeOK ==
  /\ prom \in [Acceptors -> 0..NB]
  /\ vote \in [Acceptors -> [Slots -> Vote]]
  /\ phase \in [Ballots -> {"idle", "p1", "p2"}]
  /\ safe \in [Ballots -> [Slots -> Commands \cup {None}]]
  /\ prop \in [Ballots -> [Slots -> Commands \cup {None}]]
  /\ pbal \in [Proxies -> 0..NB]
  /\ pcfg \in [Proxies -> 0..NB]
  /\ round \in [Proxies -> [Slots -> Round]]
  /\ done \in [Proxies -> [Slots -> 0..NB]]
  /\ crashes \in Nat
  /\ msgs \subseteq Msg

Init ==
  /\ prom = [a \in Acceptors |-> 0]
  /\ vote = [a \in Acceptors |-> [s \in Slots |-> NoVote]]
  /\ phase = [b \in Ballots |-> "idle"]
  /\ safe = [b \in Ballots |-> [s \in Slots |-> None]]
  /\ prop = [b \in Ballots |-> [s \in Slots |-> None]]
  /\ pbal = [p \in Proxies |-> 0]
  /\ pcfg = [p \in Proxies |-> 0]
  /\ round = [p \in Proxies |-> [s \in Slots |-> NoRound]]
  /\ done = [p \in Proxies |-> [s \in Slots |-> 0]]
  /\ crashes = 0
  /\ msgs = {}

Send(m) == msgs' = msgs \cup {m}

\* A proxy sent a `Commit` for slot s. A learner believes it with no check
\* of its own (`on_commit`), so the model reads what a node learns from the
\* `Commit`s.
Committed(s) == \E m \in msgs : m.type = "commit" /\ m.slot = s

\* The configurations whose Phase-1 quorums ballot b needs: those of every
\* lower ballot.
History(b) == {Cfg[c] : c \in 1..(b - 1)}

\* The acceptors that the `Accept` of ballot b at slot s goes to: the
\* slot's column on a grid, every member on a majority.
Addressees(b, s) == UNION Q2[Cfg[b]][s]

-----------------------------------------------------------------------------
(* Phase 1, run by the leader *)

Prepare(b) ==
  /\ phase[b] = "idle"
  /\ phase' = [phase EXCEPT ![b] = "p1"]
  /\ Send([type |-> "1a", bal |-> b])
  /\ UNCHANGED <<prom, vote, safe, prop, pbal, pcfg, round, done, crashes>>

\* Acceptor a of an earlier configuration promises ballot b and reports
\* its votes.
Promise(a, b) ==
  /\ [type |-> "1a", bal |-> b] \in msgs
  /\ a \in UNION {Members[k] : k \in History(b)}
  /\ prom[a] < b
  /\ prom' = [prom EXCEPT ![a] = b]
  /\ Send([type |-> "1b", acc |-> a, bal |-> b, rep |-> vote[a]])
  /\ UNCHANGED <<vote, phase, safe, prop, pbal, pcfg, round, done, crashes>>

\* The highest vote that the promises S report at slot s.
Top(S, s) ==
  LET rep == {m \in S : m.rep[s].bal > 0}
  IN IF rep = {} THEN NoVote
     ELSE (CHOOSE m \in rep : \A n \in rep : n.rep[s].bal <= m.rep[s].bal).rep[s]

\* The leader of ballot b wins on the promises S, one for each acceptor,
\* with a Phase-1 quorum of every configuration of its history.
Win(b) ==
  /\ phase[b] = "p1"
  /\ \E S \in SUBSET {m \in msgs : m.type = "1b" /\ m.bal = b} :
       /\ \A m, n \in S : m.acc = n.acc => m = n
       /\ \A k \in History(b) : \E Q \in Q1[k] : Q \subseteq {m.acc : m \in S}
       /\ safe' = [safe EXCEPT ![b] = [s \in Slots |-> Top(S, s).val]]
  /\ phase' = [phase EXCEPT ![b] = "p2"]
  /\ UNCHANGED <<prom, vote, prop, pbal, pcfg, round, done, crashes, msgs>>

-----------------------------------------------------------------------------
(* Phase 2: the leader sequences, a proxy fans out and folds *)

\* The leader of ballot b picks the value of slot s.
Propose(b, s, v) ==
  /\ phase[b] = "p2"
  /\ prop[b][s] = None
  /\ IF safe[b][s] # None THEN v = safe[b][s] ELSE v \in Commands
  /\ prop' = [prop EXCEPT ![b][s] = v]
  /\ UNCHANGED <<prom, vote, phase, safe, pbal, pcfg, round, done, crashes, msgs>>

\* The leader of ballot b delegates slot s to proxy p: the first time to
\* `ProxyOf[s]` (`ProxyId::of`), and again to any proxy while no `Commit`
\* of the slot reached it (`move_round`). The leader has no state of its
\* own here, and a `Commit` can be late, so a move needs no condition.
Delegate(b, s, p) ==
  /\ phase[b] = "p2"
  /\ prop[b][s] # None
  /\ p = ProxyOf[s] \/ Move
  /\ Send([type |-> "del", bal |-> b, slot |-> s, val |-> prop[b][s], proxy |-> p])
  /\ UNCHANGED <<prom, vote, phase, safe, prop, pbal, pcfg, round, done, crashes>>

\* Proxy p reads delegation m (`on_delegated`): a higher ballot closes
\* every lower round and moves the configuration it believes; it opens the
\* round and fans the `Accept` out. A lower ballot, an open round at the
\* same ballot, or a round it decided at that ballot changes nothing.
Delegated(p, m) ==
  /\ p \notin Down
  /\ m \in msgs
  /\ m.type = "del" /\ m.proxy = p
  /\ m.bal >= pbal[p]
  /\ done[p][m.slot] # m.bal
  /\ round[p][m.slot] # [bal |-> m.bal, val |-> m.val]
  /\ pbal' = [pbal EXCEPT ![p] = m.bal]
  /\ pcfg' = [pcfg EXCEPT ![p] = IF m.bal > @ THEN m.bal ELSE @]
  /\ round' = [round EXCEPT ![p] =
                 [s \in Slots |-> IF s = m.slot THEN [bal |-> m.bal, val |-> m.val]
                                  ELSE IF @[s].bal < m.bal THEN NoRound ELSE @[s]]]
  /\ Send([type |-> "2a", bal |-> m.bal, slot |-> m.slot, val |-> m.val])
  /\ UNCHANGED <<prom, vote, phase, safe, prop, done, crashes>>

\* The control of Hear = "any": proxy p learns the configuration of a
\* ballot that started, and keeps its rounds open.
Hears(p, b) ==
  /\ Hear = "any"
  /\ p \notin Down
  /\ phase[b] # "idle"
  /\ b > pcfg[p]
  /\ pcfg' = [pcfg EXCEPT ![p] = b]
  /\ UNCHANGED <<prom, vote, phase, safe, prop, pbal, round, done, crashes, msgs>>

\* Acceptor a votes for the value of ballot b at slot s.
Accept(a, b, s) ==
  /\ a \in Addressees(b, s)
  /\ prom[a] <= b
  /\ \E m \in msgs :
       /\ m.type = "2a" /\ m.bal = b /\ m.slot = s
       /\ vote[a][s] # [bal |-> b, val |-> m.val]
       /\ prom' = [prom EXCEPT ![a] = b]
       /\ vote' = [vote EXCEPT ![a][s] = [bal |-> b, val |-> m.val]]
       /\ Send([type |-> "2b", acc |-> a, bal |-> b, slot |-> s, val |-> m.val])
  /\ UNCHANGED <<phase, safe, prop, pbal, pcfg, round, done, crashes>>

Voted(a, b, s, v) == [type |-> "2b", acc |-> a, bal |-> b, slot |-> s, val |-> v] \in msgs

\* The votes that a proxy counts as a decision of value v at ballot b and
\* slot s over configuration k.
Counted(k, b, s, v) ==
  \E Q \in Q2[k][s] :
    IF Count = "quorum" THEN \A a \in Q : Voted(a, b, s, v)
    ELSE \E x \in Q : \A a \in Q \ {x} : Voted(a, b, s, v)

\* Proxy p decides its round at slot s: it sends `Commit` to the learners,
\* closes the round and remembers it decided.
Decide(p, s) ==
  LET r == round[p][s]
      k == IF Tally = "round" THEN Cfg[r.bal] ELSE Cfg[pcfg[p]]
  IN /\ p \notin Down
     /\ r # NoRound
     /\ Counted(k, r.bal, s, r.val)
     /\ round' = [round EXCEPT ![p][s] = NoRound]
     /\ done' = [done EXCEPT ![p][s] = r.bal]
     /\ Send([type |-> "commit", bal |-> r.bal, slot |-> s, val |-> r.val])
     /\ UNCHANGED <<prom, vote, phase, safe, prop, pbal, pcfg, crashes>>

\* Proxy p crashes and boots empty, with the bootstrap configuration.
Crash(p) ==
  /\ p \notin Down
  /\ crashes < Crashes
  /\ crashes' = crashes + 1
  /\ pbal' = [pbal EXCEPT ![p] = 0]
  /\ pcfg' = [pcfg EXCEPT ![p] = 0]
  /\ round' = [round EXCEPT ![p] = [s \in Slots |-> NoRound]]
  /\ done' = [done EXCEPT ![p] = [s \in Slots |-> 0]]
  /\ UNCHANGED <<prom, vote, phase, safe, prop, msgs>>

Next ==
  \/ \E b \in Ballots : Prepare(b) \/ Win(b)
  \/ \E a \in Acceptors, b \in Ballots : Promise(a, b)
  \/ \E b \in Ballots, s \in Slots, v \in Commands : Propose(b, s, v)
  \/ \E b \in Ballots, s \in Slots, p \in Proxies : Delegate(b, s, p)
  \/ \E p \in Proxies, m \in msgs : Delegated(p, m)
  \/ \E p \in Proxies, b \in Ballots : Hears(p, b)
  \/ \E a \in Acceptors, b \in Ballots, s \in Slots : Accept(a, b, s)
  \/ \E p \in Proxies, s \in Slots : Decide(p, s)
  \/ \E p \in Proxies : Crash(p)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Liveness *)

\* The last ballot NB is the leader that no higher ballot preempts. Its
\* owner, each acceptor and each proxy that is not down keep taking their
\* steps. A proxy in Down is gone for good.
Fairness ==
  /\ WF_vars(Prepare(NB))
  /\ WF_vars(Win(NB))
  /\ \A s \in Slots :
       /\ WF_vars(\E v \in Commands : Propose(NB, s, v))
       /\ \A p \in Proxies \ Down : WF_vars(Delegate(NB, s, p))
  /\ \A a \in Acceptors :
       /\ WF_vars(Promise(a, NB))
       /\ \A s \in Slots : WF_vars(Accept(a, NB, s))
  /\ \A p \in Proxies \ Down :
       /\ WF_vars(\E m \in msgs : m.bal = NB /\ Delegated(p, m))
       /\ \A s \in Slots : WF_vars(Decide(p, s))

LiveSpec == Spec /\ Fairness

\* Every slot gets a `Commit`, so the last leader learns it (`on_commit`):
\* a round whose proxy is gone moves to another proxy and ends.
Ends == <>(\A s \in Slots : Committed(s))

-----------------------------------------------------------------------------
(* Safety *)

\* A value is chosen at slot s once a Phase-2 quorum of the configuration
\* of ballot b voted for it at b.
ChosenAt(b, s, v) == \E Q \in Q2[Cfg[b]][s] : \A a \in Q : Voted(a, b, s, v)
Chosen(s, v) == \E b \in Ballots : ChosenAt(b, s, v)

\* One command for each slot.
Agreement == \A s \in Slots, v, w \in Commands : Chosen(s, v) /\ Chosen(s, w) => v = w

\* The rule of the proxy: each `Commit` is backed by a Phase-2 quorum of
\* the configuration that its round's `Accept` carried, at its ballot.
CommitBacked == \A m \in msgs : m.type = "commit" => ChosenAt(m.bal, m.slot, m.val)

\* A learner believes every `Commit` (`on_commit`), so two `Commit`s of
\* two values at one slot make two nodes learn two values.
CommitAgree == \A m, n \in msgs :
                 m.type = "commit" /\ n.type = "commit" /\ m.slot = n.slot => m.val = n.val

\* Reachability controls: each must be violated.
\* A proxy commits a round.
NoCommit == ~\E m \in msgs : m.type = "commit"
\* A proxy commits a round of a ballot whose configuration a higher ballot
\* sealed: its Phase 1 is won.
NoSealedCommit == ~\E m \in msgs : m.type = "commit" /\ \E c \in Ballots : c > m.bal /\ phase[c] = "p2"
\* A slot moves to a second proxy and that proxy decides it.
NoMovedCommit == ~\E s \in Slots, p \in Proxies : p # ProxyOf[s] /\ done[p][s] # 0
=============================================================================
