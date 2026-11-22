------------------------------- MODULE Bypass -------------------------------
(* A small model of Matchmaker MultiPaxos with Phase-1 bypassing, for        *)
(* #579 (zero-stall reconfiguration). It compares the ways to bypass.        *)
(*                                                                           *)
(* Owner  : which proposer owns each ballot. <<1, 1, 2, 2>> is the paper's   *)
(*          (r, id, s): a proposer owns two consecutive ballots.             *)
(*          <<1, 2, 1, 2>> is paros's (round, node): a ballot of the other   *)
(*          proposer is between two ballots of one proposer.                 *)
(* Check  : TRUE = bypass only if the matchmaker answers show no             *)
(*          registration between the two ballots.                            *)
(* Mode   : "none"  = no bypass (plain Matchmaker MultiPaxos).               *)
(*          "carry" = the leader proposes its own values again at the new    *)
(*                    ballot, with no Phase 1.                               *)
(*          "p212"  = frankenpaxos Phase212: the slots above the frontier    *)
(*                    are free at once; the slots below get a Phase 1 at     *)
(*                    the new ballot.                                        *)
(*                                                                           *)
(* Gc     : "off"    = no garbage collection.                                *)
(*          "chosen" = the leader of b asks the matchmakers to forget the    *)
(*                     ballots below b when each slot that it recovered or   *)
(*                     carried is chosen at b (Matchmaker Paxos, section     *)
(*                     3.5), and, if Hold, when its Phase 1 is covered.      *)
(*          "nofence" = a control: as "chosen", but the leader of b' asks    *)
(*                     before the Phase 1 at b' (the fence Phase 1) ends:    *)
(*                     the slots that wait for it do not count, and the hold *)
(*                     does not count.                                       *)
(*          "early"  = a control: the leader asks with no condition.         *)
(* CheckWm: TRUE = bypass only if the highest watermark in the matchmaker    *)
(*          answers is at most the old ballot.                               *)
(* Reads  : TRUE = the leader acks the chosen slots to the clients, and the  *)
(*          model has the leaderless reads of Compartmentalized Paxos        *)
(*          (section 3.4) with the rule of paros (quorum_read.rs).           *)
(* Hold   : TRUE = the decision hold. A leader acks no slot before a quorum  *)
(*          of each prior configuration promised its ballot. After a bypass  *)
(*          the leader thus sends a Prepare, in "carry" mode too.            *)
(*                                                                           *)
(* CfgChoices : the configurations that the ballots can have. Each one is a  *)
(*          function from the ballots to Configs.                            *)
(*                                                                           *)
(* Handoff: TRUE = the cooperative handoff of DPaxos. The node that minted   *)
(*          a ballot can hand it on one time to another node, with its       *)
(*          proposals at that ballot, while it leads at that ballot.         *)
(* Relinquish : TRUE = a node that handed its ballot on never bypasses from  *)
(*          that ballot: it drops its proactive matchmaking. A successor     *)
(*          never bypasses from a ballot that it did not mint.               *)
(*          FALSE = a control: the minter can still bypass after it handed   *)
(*          the ballot on.                                                   *)
(* SuccessorBypass : FALSE = the rule above. TRUE = a control: the          *)
(*          successor of a handoff bypasses from the ballot it got, to its   *)
(*          own next ballot.                                                 *)
(* HandoffCarry : TRUE = the handoff carries the slots that wait for the     *)
(*          Phase 1 at the ballot (the fence Phase 1). FALSE = a control:    *)
(*          the successor takes these slots as free.                         *)
(* ChainRule : TRUE = a leader bypasses only when each slot that it recovered *)
(*          or carried is proposed at its ballot: a second bypass waits for  *)
(*          the fence Phase 1 of the first. FALSE = a control: the slots     *)
(*          that wait for the fence Phase 1 do not count.                    *)
(*                                                                           *)
(* Restarts : the number of restarts. A restart loses what the node knows   *)
(*          that it proposed (`known`), and the node keeps every vote and    *)
(*          registration.                                                    *)
(* RestartBypass : FALSE = the rule: a restart ends each campaign and each   *)
(*          leadership of the node, so only the incarnation that won b       *)
(*          bypasses from b. TRUE = a control: the node still leads after    *)
(*          the restart, with what it knew lost.                             *)
(*                                                                           *)
(* ReadHold : TRUE = the rule of #1270 (bypass read order): the chosen      *)
(*          prefix of a reader counts a slot only once the ballot that      *)
(*          chose it is covered (`Covered`), so a slot that the hold keeps  *)
(*          from the client reaches no client through a read. FALSE = a     *)
(*          control: a read returns a slot that a ballot chose before its   *)
(*          fence Phase 1, and a later read on the old basis misses it.     *)
(*                                                                           *)
(* Gone   : the acceptors that take part in ballot 1 only: the old members  *)
(*          of a journal move that leave after the first leadership. A       *)
(*          liveness check uses it; every safety check has Gone = {}.        *)
(*                                                                           *)
(* Not in the model: matchmaker generations, lost durable state, a handoff   *)
(* message that is late or sent two times (`leader-handoff/` has them, and   *)
(* its control `two_hops`).                                                  *)
EXTENDS Naturals, FiniteSets

CONSTANTS Acceptors, Matchmakers, Configs, Values, Slots, NB, Owner, Check, Mode,
          Gc, CheckWm, Reads, Hold, CfgChoices, Handoff, Relinquish, HandoffCarry,
          ChainRule, SuccessorBypass, Restarts, RestartBypass, Gone, ReadHold

Ballots == 1..NB
Nodes   == {Owner[b] : b \in Ballots}
Free    == "free"
Pending == "pending"

\* A gone acceptor is in the configuration of ballot 1 only, so it votes at
\* ballot 1 only. Accept needs no guard of its own on Gone.
ASSUME \A f \in CfgChoices : \A b \in Ballots \ {1} : f[b] \cap Gone = {}

VARIABLES status,    \* ballot -> "matching" | "matched" | "leading" | "switched" | "dead"
          cfgOf,     \* ballot -> its configuration
          reg,       \* matchmaker -> set of <<ballot, config>>
          replied,   \* ballot -> matchmakers that answered its registration
          hist,      \* ballot -> union of the histories in those answers
          promised,  \* acceptor -> ballot
          votes,     \* set of <<acceptor, slot, ballot, value>>
          promises,  \* set of <<acceptor, ballot, votes of the acceptor then>>
          safe,      \* ballot -> slot -> Free | Pending | the one value allowed
          proposals, \* set of <<ballot, slot, value>>
          wm,        \* matchmaker -> its GC watermark
          maxwm,     \* ballot -> the highest watermark in the answers
          acked,     \* slots that a leader acked to a client
          holder,    \* ballot -> the node that leads at it
          known,     \* node -> the <<ballot, slot, value>> proposals that it knows
          via,       \* ballot -> the ballot that its leader bypassed from, or 0
          restarts,  \* the number of restarts so far
          seen       \* slots that a leaderless read returned to its client

vars == <<status, cfgOf, reg, replied, hist, promised, votes, promises, safe, proposals,
          wm, maxwm, acked, holder, known, via, restarts, seen>>

ext == <<holder, known, via, restarts>>

Max(S) == CHOOSE x \in S : \A y \in S : y <= x

Quorums(c) == {Q \in SUBSET c : 2 * Cardinality(Q) > Cardinality(c)}

\* Each ballot has one configuration, selected by its owner. The selection is
\* local to the owner, thus the model makes it in the initial state. A ballot
\* that no matchmaker registers is a ballot that was never used.
Init == /\ status = [b \in Ballots |-> "matching"]
        /\ cfgOf \in CfgChoices
        /\ reg = [m \in Matchmakers |-> {}]
        /\ replied = [b \in Ballots |-> {}]
        /\ hist = [b \in Ballots |-> {}]
        /\ promised = [a \in Acceptors |-> 0]
        /\ votes = {}
        /\ promises = {}
        /\ safe = [b \in Ballots |-> [s \in Slots |-> Free]]
        /\ proposals = {}
        /\ wm = [m \in Matchmakers |-> 0]
        /\ maxwm = [b \in Ballots |-> 0]
        /\ acked = {}
        /\ holder = [b \in Ballots |-> Owner[b]]
        /\ known = [n \in Nodes |-> {}]
        /\ via = [b \in Ballots |-> 0]
        /\ restarts = 0
        /\ seen = {}

\* Matchmaker m registers b if it holds no ballot at or above b and b is not
\* below its watermark. It answers with what it held and with its watermark.
\* The leader reads the answers only while it matches: its prior
\* configurations are fixed once it matched (HistFixed). A second
\* registration of b at m is refused by the check of the ballots that m
\* holds: a GC drops b from m only with a watermark above b.
Register(b, m) ==
    /\ status[b] = "matching"
    /\ \A r \in reg[m] : r[1] < b
    /\ wm[m] <= b
    /\ reg' = [reg EXCEPT ![m] = @ \cup {<<b, cfgOf[b]>>}]
    /\ replied' = [replied EXCEPT ![b] = @ \cup {m}]
    /\ hist' = [hist EXCEPT ![b] = @ \cup reg[m]]
    /\ maxwm' = [maxwm EXCEPT ![b] = Max({@, wm[m]})]
    /\ UNCHANGED <<status, cfgOf, promised, votes, promises, safe, proposals, wm, acked, seen, ext>>

MatchDone(b) ==
    /\ status[b] = "matching"
    /\ 2 * Cardinality(replied[b]) > Cardinality(Matchmakers)
    /\ status' = [status EXCEPT ![b] = "matched"]
    /\ UNCHANGED <<cfgOf, reg, replied, hist, promised, votes, promises, safe, proposals, wm, maxwm, acked, seen, ext>>

\* The prior configurations: the ones at or above the highest watermark.
PriorCfgs(b) == {r[2] : r \in {h \in hist[b] : h[1] >= maxwm[b]}}

HasPending(b) == \E s \in Slots : safe[b][s] = Pending

\* A promise quorum of EVERY prior configuration.
Covered(b) ==
    \A c \in PriorCfgs(b) :
        \E Q \in Quorums(c) : \A a \in Q : \E p \in promises : p[1] = a /\ p[2] = b

\* Acceptor a promises b and reports its votes. The owner of b sends a
\* Prepare in a normal campaign, after a "p212" bypass, or for the decision
\* hold after any bypass, never before its matching ended
\* (PrepareAfterMatch). A Prepare can come two times: a promise only raises
\* the ballot of the acceptor, and the acceptor reports its votes below b,
\* which no longer change once it promised b, so a second promise adds
\* nothing.
Promise(a, b) ==
    /\ \/ status[b] = "matched"
       \/ status[b] = "leading" /\ (HasPending(b) \/ (Hold /\ ~ Covered(b)))
    /\ a \in UNION PriorCfgs(b)
    /\ a \in Gone => b = 1
    /\ promised[a] <= b
    /\ promised' = [promised EXCEPT ![a] = b]
    /\ promises' = promises \cup
          {<<a, b, {<<w[2], w[3], w[4]>> : w \in {x \in votes : x[1] = a /\ x[3] < b}}>>}
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, votes, safe, proposals, wm, maxwm, acked, seen, ext>>

Reports(b) == UNION {p[3] : p \in {q \in promises : q[2] = b}}

\* The value of the highest vote below b that a promise reports, or Free.
SafeAt(b, s) ==
    LET R == {r \in Reports(b) : r[1] = s /\ r[2] < b}
    IN  IF R = {} THEN Free
        ELSE (CHOOSE r \in R : \A x \in R : x[2] <= r[2])[3]

\* The normal path: a full Phase 1.
Phase1Done(b) ==
    /\ status[b] = "matched"
    /\ Covered(b)
    /\ status' = [status EXCEPT ![b] = "leading"]
    /\ safe' = [safe EXCEPT ![b] = [s \in Slots |-> SafeAt(b, s)]]
    /\ UNCHANGED <<cfgOf, reg, replied, hist, promised, votes, promises, proposals, wm, maxwm, acked, seen, ext>>

\* The holder of b proposes. Only one node holds b at a time: a handoff
\* moves b with the proposals at b, and the old holder stops at once.
Propose(b, s, v) ==
    /\ status[b] = "leading"
    /\ ~ \E p \in proposals : p[1] = b /\ p[2] = s
    /\ safe[b][s] = Free \/ safe[b][s] = v
    /\ proposals' = proposals \cup {<<b, s, v>>}
    /\ known' = [known EXCEPT ![holder[b]] = @ \cup {<<b, s, v>>}]
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promised, votes, promises, safe, wm, maxwm, acked, seen,
                   holder, via, restarts>>

\* A gone acceptor votes at ballot 1 only (the ASSUME on Gone). A second
\* vote of the same round changes nothing.
Accept(a, b, s, v) ==
    /\ <<b, s, v>> \in proposals
    /\ a \in cfgOf[b]
    /\ promised[a] <= b
    /\ promised' = [promised EXCEPT ![a] = b]
    /\ votes' = votes \cup {<<a, s, b, v>>}
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promises, safe, proposals, wm, maxwm, acked, seen, ext>>

\* What node n knows that b proposed: its own proposals, and the proposals
\* that a handoff gave it.
ProposedAt(n, b) == {k[2] : k \in {x \in known[n] : x[1] = b}}
ValAt(n, b, s)   == (CHOOSE k \in known[n] : k[1] = b /\ k[2] = s)[3]

\* The leader proposed each value that its Phase 1 recovered. With
\* ChainRule, a slot that waits for the fence Phase 1 is not settled: a
\* leader bypasses again only after the fence Phase 1 of its last bypass.
Settled(n, b) ==
    \A s \in Slots :
        (IF ChainRule THEN safe[b][s] # Free ELSE safe[b][s] \notin {Free, Pending})
            => s \in ProposedAt(n, b)

\* b2 is the next ballot of node n above b.
NextOf(n, b, b2) ==
    /\ b2 > b
    /\ Owner[b2] = n
    /\ \A x \in Ballots : (x > b /\ x < b2) => Owner[x] # n

\* The nodes that can bypass from b. The rule: only the minter of b, while it
\* holds b. A successor never bypasses from a ballot that it did not mint:
\* on the paper's ballots, a ballot of the minter can be between b and the
\* next ballot of the successor, and no check sees it. The control
\* Relinquish = FALSE: the minter can bypass after it handed b on.
\* The control SuccessorBypass = TRUE: the holder of b can bypass too.
Bypassers(b) ==
    (IF Relinquish THEN {holder[b]} \cap {Owner[b]} ELSE {Owner[b]})
        \cup (IF SuccessorBypass THEN {holder[b]} ELSE {})

\* The switch: node n, which leads at b, moves to its next ballot b2 with
\* no Phase 1 for the free slots. With "p212", the slots that n proposed at
\* b wait for the Phase 1 at b2. A slot that n did not propose is free at b
\* (Settled) and stays free at b2, also below a slot that n proposed: the
\* slots need not be a prefix.
Bypass(b, b2) ==
    /\ Mode # "none"
    /\ status[b] = "leading"
    /\ status[b2] = "matched"
    /\ \E n \in Bypassers(b) :
          /\ NextOf(n, b, b2)
          /\ Settled(n, b)
          /\ Check => \A r \in hist[b2] : ~ (r[1] > b /\ r[1] < b2)
          /\ CheckWm => maxwm[b2] <= b
          /\ status' = [status EXCEPT ![b] = "switched", ![b2] = "leading"]
          /\ safe' = [safe EXCEPT ![b2] =
                [s \in Slots |->
                    IF s \in ProposedAt(n, b)
                    THEN (IF Mode = "carry" THEN ValAt(n, b, s) ELSE Pending)
                    ELSE Free]]
    /\ via' = [via EXCEPT ![b2] = b]
    /\ UNCHANGED <<cfgOf, reg, replied, hist, promised, votes, promises, proposals, wm, maxwm, acked, seen,
                   holder, known, restarts>>

\* The cooperative handoff (DPaxos, relinquish_to): the minter of b hands b
\* on to node n, one hop. The successor gets the proposals at b and leads at
\* b with no Phase 1. With HandoffCarry, it also gets the slots that wait
\* for the fence Phase 1 of b; with the control, it takes them as free.
\* A hand-on to the holder itself changes nothing with HandoffCarry, and
\* only drops the pending slots with the control.
HandOn(b, n) ==
    /\ Handoff
    /\ status[b] = "leading"
    /\ holder[b] = Owner[b]
    /\ holder' = [holder EXCEPT ![b] = n]
    /\ known' = [known EXCEPT ![n] = @ \cup {k \in known[holder[b]] : k[1] = b}]
    /\ IF HandoffCarry
       THEN UNCHANGED safe
       ELSE safe' = [safe EXCEPT ![b] =
                [s \in Slots |-> IF @[s] = Pending THEN Free ELSE @[s]]]
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promised, votes, promises, proposals, wm, maxwm,
                   acked, seen, via, restarts>>

\* "p212" only: the Phase 1 at the new ballot for the slots below the frontier.
\* With no slot that waits, it changes nothing.
Phase1Low(b) ==
    /\ status[b] = "leading"
    /\ Covered(b)
    /\ safe' = [safe EXCEPT ![b] =
          [s \in Slots |-> IF safe[b][s] = Pending THEN SafeAt(b, s) ELSE safe[b][s]]]
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promised, votes, promises, proposals, wm, maxwm, acked, seen, ext>>

ChosenAt(b, s) ==
    \E v \in Values :
        \E Q \in Quorums(cfgOf[b]) : \A a \in Q : <<a, s, b, v>> \in votes

\* The leader of b can ask the matchmakers to forget the ballots below b.
\* Each part of the condition is stable, thus a request that is late is the
\* same as a request that is sent now.
GcReady(b) ==
    /\ Gc # "off"
    /\ status[b] \in {"leading", "switched"}
    /\ Gc = "chosen" =>
          /\ ~ HasPending(b)
          /\ \A s \in Slots : safe[b][s] # Free => ChosenAt(b, s)
          /\ Hold => Covered(b)
    /\ Gc = "nofence" =>
          \A s \in Slots : safe[b][s] \notin {Free, Pending} => ChosenAt(b, s)

\* Matchmaker m applies the GC request of b.
Collect(b, m) ==
    /\ GcReady(b)
    /\ wm[m] < b
    /\ wm' = [wm EXCEPT ![m] = b]
    /\ reg' = [reg EXCEPT ![m] = {r \in @ : r[1] >= b}]
    /\ UNCHANGED <<status, cfgOf, replied, hist, promised, votes, promises, safe,
                   proposals, maxwm, acked, seen, ext>>

\* The leader of b acks slot s to its client. A leader that switched can
\* still ack a slot that is chosen at its old ballot ("p212": the old rounds
\* stay open; "carry": a late quorum at the old ballot).
\* A slot chosen at b is chosen whatever the status of b, so the model also
\* lets a dead leader ack it, and ack it again.
Ack(b, s) ==
    /\ ChosenAt(b, s)
    /\ Hold => Covered(b)
    /\ acked' = acked \cup {s}
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promised, votes, promises, safe,
                   proposals, wm, maxwm, ext, seen>>

\* The fence of a leadership: the highest slot that it recovered or carried.
Fence(b) == Max({0} \cup {s \in Slots : safe[b][s] # Free})

\* The vote watermark of acceptor a: the highest slot that it voted in.
Watermark(a) == Max({0} \cup {w[2] : w \in {x \in votes : x[1] = a}})

\* The index of a leaderless read at b over the answers of Q: the highest
\* watermark of the answers, and the fence or above.
ReadIndex(b, Q) == Max({Fence(b)} \cup {Watermark(a) : a \in Q})

\* A leaderless read on the basis of the leadership at b ends, and its client
\* gets the slots at or below its index. The reader asks a quorum Q of the
\* configuration of b, none of whose members promised above b (an answer
\* above b ends the read), and reads at the highest watermark of the answers
\* and at the fence or above. It serves only on its chosen prefix: each slot
\* at or below the index is chosen, with ReadHold at a covered ballot.
ReadReturn(b, Q) ==
    /\ status[b] \in {"leading", "switched"}
    /\ Q \in Quorums(cfgOf[b])
    /\ \A a \in Q : promised[a] <= b
    /\ \A s \in Slots : s <= ReadIndex(b, Q) =>
          \E c \in Ballots : ChosenAt(c, s) /\ (ReadHold => Covered(c))
    /\ seen' = seen \cup {s \in Slots : s <= ReadIndex(b, Q)}
    /\ UNCHANGED <<status, cfgOf, reg, replied, hist, promised, votes, promises, safe,
                   proposals, wm, maxwm, acked, ext>>

\* Node n restarts: it forgets what it proposed. With the rule, every
\* campaign and every leadership of n ends ("dead"): a dead ballot proposes,
\* bypasses and collects nothing. Its rounds stay in the votes.
Restart(n) ==
    /\ restarts < Restarts
    /\ restarts' = restarts + 1
    /\ known' = [known EXCEPT ![n] = {}]
    /\ IF RestartBypass
       THEN UNCHANGED status
       ELSE status' = [b \in Ballots |-> IF holder[b] = n THEN "dead" ELSE status[b]]
    /\ UNCHANGED <<cfgOf, reg, replied, hist, promised, votes, promises, safe, proposals, wm, maxwm,
                   acked, seen, holder, via>>

Next ==
    \/ \E b \in Ballots, m \in Matchmakers : Register(b, m)
    \/ \E b \in Ballots : MatchDone(b) \/ Phase1Done(b) \/ Phase1Low(b)
    \/ \E a \in Acceptors, b \in Ballots : Promise(a, b)
    \/ \E b \in Ballots, s \in Slots, v \in Values : Propose(b, s, v)
    \/ \E a \in Acceptors, p \in proposals : Accept(a, p[1], p[2], p[3])
    \/ \E b, b2 \in Ballots : Bypass(b, b2)
    \/ \E b \in Ballots, n \in Nodes : HandOn(b, n)
    \/ \E b \in Ballots, m \in Matchmakers : Collect(b, m)
    \* Reads = FALSE leaves the acks and the reads out of the run: only the
    \* read checks look at `acked` and `seen`, and the run is smaller.
    \/ Reads /\ \E b \in Ballots, s \in Slots : Ack(b, s)
    \/ Reads /\ \E b \in Ballots, Q \in SUBSET Acceptors : ReadReturn(b, Q)
    \/ \E n \in Nodes : Restart(n)

Spec == Init /\ [][Next]_vars

\* Every step of a campaign, a leadership and a read is fair; a handoff, a
\* GC request and a restart are not.
LiveSpec ==
    /\ Spec
    /\ \A b \in Ballots :
          /\ \A m \in Matchmakers : WF_vars(Register(b, m))
          /\ WF_vars(MatchDone(b)) /\ WF_vars(Phase1Done(b)) /\ WF_vars(Phase1Low(b))
          /\ \A a \in Acceptors : WF_vars(Promise(a, b))
          /\ \A s \in Slots : WF_vars(\E v \in Values : Propose(b, s, v))
          /\ \A s \in Slots : WF_vars(Reads /\ Ack(b, s))
          /\ \A b2 \in Ballots : WF_vars(Bypass(b, b2))
    /\ \A a \in Acceptors, p \in Ballots \X Slots \X Values :
          WF_vars(Accept(a, p[1], p[2], p[3]))

\* Each slot that a leader proposed is acked to its client in the end.
AckEnds == \A s \in Slots : (\E p \in proposals : p[2] = s) ~> s \in acked

Chosen(s, v) ==
    \E b \in Ballots :
        \E Q \in Quorums(cfgOf[b]) : \A a \in Q : <<a, s, b, v>> \in votes

\* Safety: one value for each slot.
Agreement == \A s \in Slots : \A v, w \in Values : (Chosen(s, v) /\ Chosen(s, w)) => v = w

\* A leaderless read (quorum_read.rs). A reader has the basis of a leadership
\* that won: its configuration, its ballot, its fence. It asks a quorum of
\* that configuration. It abandons the read if an answer shows a higher
\* ballot. If not, it reads at the highest slot that an answer voted in, and
\* at the fence or above.
\* The invariant: no read that starts now can miss a slot that is acked, or
\* a slot that an earlier read returned (`seen`).
ReadSafe ==
    ~ \E b \in Ballots :
        /\ status[b] \in {"leading", "switched"}
        /\ \E Q \in Quorums(cfgOf[b]) :
              /\ \A a \in Q : promised[a] <= b
              /\ \E s \in acked \cup seen :
                    /\ s > Fence(b)
                    /\ \A a \in Q : ~ \E w \in votes : w[1] = a /\ w[2] >= s

\* The index of each read that ends covers every slot that is acked or that
\* an earlier read returned. ReadSafe says it of each read that could start;
\* this says it of the reads that end, so it also judges the guards of
\* ReadReturn.
ReadsComplete ==
    [][\A b \in Ballots, Q \in SUBSET Acceptors :
          ReadReturn(b, Q) => acked \cup seen \subseteq {s \in Slots : s <= ReadIndex(b, Q)}]_vars

\* Matchmaking ends before Phase 1 (Matchmaker Paxos): no acceptor promises
\* a ballot that still matches.
PrepareAfterMatch == \A p \in promises : status[p[2]] # "matching"

\* The prior configurations of a ballot do not change once it matched: its
\* Phase 1 and its bypass check read them.
HistFixed == [][\A b \in Ballots :
                  status[b] # "matching" => hist'[b] = hist[b] /\ maxwm'[b] = maxwm[b]]_vars

\* Not a real invariant. TLC must find a trace where a bypass leader has a
\* value chosen at its new ballot: this shows that the model reaches the case.
NoBypassChoice ==
    ~ \E b \in Ballots, s \in Slots, v \in Values :
        /\ via[b] # 0
        /\ \E Q \in Quorums(cfgOf[b]) : \A a \in Q : <<a, s, b, v>> \in votes

\* The watermark half of the bypass check adds nothing to the registration
\* half: when the answers to b2 show no registration strictly between b and
\* b2, their highest watermark is at most b. A matchmaker collects at w only
\* for a leader at w, thus w registered at a matchmaker quorum, and that
\* quorum meets the quorum of b2 at a matchmaker that holds w or refused w.
WmRedundant ==
    \A b, b2 \in Ballots :
        (/\ status[b] = "leading"
         /\ status[b2] = "matched"
         /\ NextOf(holder[b], b, b2)
         /\ ~ \E r \in hist[b2] : r[1] > b /\ r[1] < b2)
            => maxwm[b2] <= b

\* Not a real invariant. TLC must find a trace with two bypasses in a row.
NoChain == ~ \E b \in Ballots : via[b] # 0 /\ via[via[b]] # 0

\* DPaxos hands a ballot on one hop: only its minter, while it leads at it.
\* A ballot that moved never moves again. HandOn is the one step that moves
\* a ballot, so this states the rule that its guards build.
HandOnOneHop ==
    [][\A b \in Ballots : holder'[b] # holder[b] =>
          /\ holder[b] = Owner[b]
          /\ status[b] = "leading"]_vars

\* Not a real invariant. TLC must find a trace where a successor of a
\* handoff leads at a ballot that its minter bypassed to.
NoHandoffAfterBypass == ~ \E b \in Ballots : via[b] # 0 /\ holder[b] # Owner[b]
=============================================================================
