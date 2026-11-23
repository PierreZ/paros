------------------------------ MODULE Election ------------------------------
(***************************************************************************)
(* The election over a journal, for #1420 (paros-election): the term of    *)
(* docs/architecture/03-3-coordinators-and-placement.md §3.3 (rule 3,      *)
(* decision 13), alone. A user elects any actor with it, and the           *)
(* coordinators of the universe, of a cell and of a tenant use it over     *)
(* their own control journal. It needs only a journal with a compare-and-  *)
(* set of the leader (`SetLeader(new, old)`, docs/journal/02-data-plane.md *)
(* §J2.3) and a fenced write.                                              *)
(*                                                                         *)
(* The journal is one atomic object: the other models check how Paxos     *)
(* keeps it. It holds the leader value (unset on a fresh journal, a uuid,  *)
(* or None in multi-writer mode, §J2.4.1) and a log. The log keeps the     *)
(* records that the fold of the election reads: each applied `SetLeader`  *)
(* and each term record. A renewal is a fenced write that the journal      *)
(* judges and the model does not keep, so the log stays short.             *)
(*                                                                         *)
(* The steps of the election (#1420):                                      *)
(*   Win        `SetLeader(new, old)`, with the leader of the actor's fold *)
(*              as `old` and a fresh uuid as `new`. The loser learns the   *)
(*              winner from the refusal. A candidate with no answer sends  *)
(*              the same call again: a refusal that names its own uuid     *)
(*              means that it won.                                         *)
(*   Number     the term number is the count of the terms in the fold:     *)
(*              each term record names its number, and a `SetLeader` is    *)
(*              not a record a reader sees (it takes no position). The     *)
(*              winner reads the journal up to the tail that the answer of *)
(*              its `SetLeader` names, and takes the number of the last    *)
(*              term record there, plus one. A term that never wrote its   *)
(*              term record takes no number: the next winner reuses it.    *)
(*   Hold       each write under the uuid is a renewal. The first write of *)
(*              a term is the term record: it names the holder's interface *)
(*              (the actor and its incarnation).                           *)
(*   Lose       the holder stops at its first refused write.               *)
(*   Hand on    `SetLeader(next, me)` with a fresh uuid; then the next     *)
(*              actor gets the uuid and the term number.                   *)
(*   Challenge  an actor that sees no live holder (one lease with no write *)
(*              under the current uuid, on its own clock) sends the        *)
(*              compare-and-set. With Eager, it may challenge at any time: *)
(*              a challenge that is too early costs availability, never    *)
(*              safety.                                                    *)
(*   Fence      an order that leaves the journal carries the term number;  *)
(*              a holder sends one only once its term record is in the     *)
(*              journal, so a number is never sent by two holders.         *)
(*              The receiver keeps the highest term it saw on disk and     *)
(*              refuses a lower one.                                       *)
(*   Mode       the election refuses a journal in multi-writer mode: it    *)
(*              never challenges a leader `None`.                          *)
(*                                                                         *)
(* The faults: an actor stops and comes back as a new incarnation (its     *)
(* memory and the messages to it are lost), a message is lost, a call is   *)
(* applied late, the user of the journal switches it to multi-writer mode. *)
(*                                                                         *)
(* The rules are constants; TRUE is the rule. Each control sets one to     *)
(* FALSE and must break a property:                                        *)
(*   Fence = FALSE        the receiver takes an order of any term.         *)
(*   Cas = FALSE          the journal takes a `SetLeader` whatever its     *)
(*                        `old`: a challenge from a view before a switch   *)
(*                        to multi-writer mode takes the journal back.     *)
(*                        (One holder for each number does not rest on     *)
(*                        it: the winner reads to its tail, and the        *)
(*                        journal fences each write by its uuid.)          *)
(*   ClearIface = FALSE   the fold keeps the interface across a            *)
(*                        `SetLeader`: a published interface outlives its  *)
(*                        term.                                            *)
(*   Fresh = FALSE        an actor that challenges again names the uuid it *)
(*                        took a term with before.                         *)
(*   Stop = FALSE         a holder keeps its term after a refused write.   *)
(*   RefuseMulti = FALSE  the election challenges a leader `None`.         *)
(*   OwnAnswer = FALSE    a holder takes the answer to a write of its      *)
(*                        earlier term as an answer for its current term.  *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Actors,
          NoActor,      \* a value that is no actor
          MaxUuids,     \* the uuids that the actors may draw: 1..MaxUuids
          MaxCrashes, MaxLoss, MaxHands, MaxUser,
          Eager,        \* TRUE: a challenge may depose a live holder
          Lag,          \* TRUE: a challenge may read a view that lags
          Fence, Cas, ClearIface, Fresh, Stop, RefuseMulti, OwnAnswer,
          Ev            \* for the reach checks: the event that must not happen

Uuids == 1..MaxUuids
Unset == 0              \* the leader of a fresh journal
None == MaxUuids + 1    \* the leader of a multi-writer journal: no uuid
NoIface == <<>>

VARIABLES inc,       \* the incarnation of each actor
          st,        \* "idle", "campaign", "lead", "handing"
          mine,      \* the uuid of its campaign, of its term, or of its hand on
          base,      \* <<old, term of old, next>>: what its call replaces, the next actor
          term,      \* the term it leads, 0 for none
          pub,       \* its term record is in the journal
          wfly,      \* a write of it is in flight
          jleader, jlog,   \* the journal: the leader value and the log
          net,       \* calls to the journal, their answers and hand-on messages
          onet,      \* the orders in flight to the receiver: their terms
          seen,      \* the receiver: the highest term it saw, on disk
          orders,    \* every order sent: <<term, actor, incarnation>>
          nuuid,     \* the uuids drawn so far
          owner,     \* owner[u]: <<actor, incarnation>> that took a term with u
          crashes, lost, hands, users,
          stale,     \* a write was taken in a term that is not the term it was sent in
          regress,   \* the receiver applied an order below one it applied
          userSet,   \* the user switched the journal to multi-writer mode
          reached    \* the event Ev happened

actor == <<inc, st, mine, base, term, pub, wfly>>
journal == <<jleader, jlog>>
receiver == <<onet, seen, orders>>
budget == <<crashes, lost, hands, users>>
ghost == <<stale, regress, userSet, reached>>
vars == <<actor, journal, net, receiver, nuuid, owner, budget, ghost>>

-----------------------------------------------------------------------------
\* The fold of the election over a prefix of the log: the leader value, the
\* number of the last term record and the published interface.
Step(f, r) ==
  IF r.k = "set"
  THEN [leader |-> r.u, term |-> f.term,
        iface |-> IF ClearIface THEN NoIface ELSE f.iface]
  ELSE [f EXCEPT !.term = r.t, !.iface = r.iface]

FoldOf(s) ==
  LET F[i \in 0..Len(s)] ==
        IF i = 0 THEN [leader |-> Unset, term |-> 0, iface |-> NoIface]
        ELSE Step(F[i - 1], s[i])
  IN F[Len(s)]

\* A fold of an actor reads a prefix of the log: any prefix, as a read of a
\* replica that lags (Lag), or the whole log. The liveness checks read the
\* whole log: a candidate folds the journal to its tail before it
\* challenges, and a challenge from a lagging view only spends a uuid.
Prefixes == IF Lag THEN {SubSeq(jlog, 1, p) : p \in 0..Len(jlog)} ELSE {jlog}
JTerm == FoldOf(jlog).term

\* The term number of u in the journal: the number of the last term record
\* of u, or 0 when u wrote none.
TermOfUuid(u) ==
  LET at == {i \in 1..Len(jlog) : jlog[i].k = "term" /\ jlog[i].u = u} IN
  IF at = {} THEN 0
  ELSE jlog[CHOOSE i \in at : \A j \in at : j <= i].t

Hit(e) == reached' = (reached \/ e = Ev)

\* A live holder: an actor that runs holds the current uuid (it may not know
\* yet that its call won), or a handed uuid is on its way to the next actor.
\* With no live holder, no write under the current uuid comes for a lease:
\* this is what the lease of each actor sees, on its own clock.
LiveHolder ==
  \/ \E b \in Actors : st[b] \in {"campaign", "lead", "handing"} /\ mine[b] = jleader
  \/ \E m \in net : m.k = "lead" /\ m.u = jleader

\* Every message has one shape, so that TLC can compare any two.
Blank == [k |-> "", from |-> NoActor, to |-> NoActor, inc |-> 0, u |-> 0, old |-> 0,
          next |-> NoActor, t |-> 0, ok |-> FALSE, leader |-> 0, rec |-> "",
          iface |-> NoIface, len |-> 0]
SL(a, u, old, nxt) == [Blank EXCEPT !.k = "sl", !.from = a, !.inc = inc[a], !.u = u,
                                    !.old = old, !.next = nxt]
TermRecord(a, u, t) ==
  [Blank EXCEPT !.k = "w", !.from = a, !.inc = inc[a], !.u = u, !.t = t, !.rec = "term",
                !.iface = <<a, inc[a]>>]

\* The messages of the current incarnation of a: its calls, and the answers
\* and hand-on messages to it.
Pending(a) == {m \in net : (m.from = a /\ m.inc = inc[a]) \/ m.to = a}

Init ==
  /\ inc = [a \in Actors |-> 1]
  /\ st = [a \in Actors |-> "idle"] /\ mine = [a \in Actors |-> 0]
  /\ base = [a \in Actors |-> <<Unset, 0, a>>] /\ term = [a \in Actors |-> 0]
  /\ pub = [a \in Actors |-> FALSE] /\ wfly = [a \in Actors |-> FALSE]
  /\ jleader = Unset /\ jlog = <<>>
  /\ net = {} /\ onet = {} /\ seen = 0 /\ orders = {}
  /\ nuuid = 0 /\ owner = [u \in Uuids |-> <<>>]
  /\ crashes = 0 /\ lost = 0 /\ hands = 0 /\ users = 0
  /\ stale = FALSE /\ regress = FALSE /\ userSet = FALSE /\ reached = FALSE

-----------------------------------------------------------------------------
\* The actors.

\* The uuid of a new campaign: a fresh draw, or (control Fresh = FALSE) the
\* last uuid the actor took a term with, when it is not the leader.
Took(a, v) == {u \in Uuids : owner[u] # <<>> /\ owner[u][1] = a /\ u # v.leader}
NewUuid(a, v) ==
  IF ~Fresh /\ Took(a, v) # {}
  THEN CHOOSE u \in Took(a, v) : \A w \in Took(a, v) : w <= u
  ELSE nuuid + 1

\* The view v is the fold of a prefix of the log: an actor may challenge
\* from a view that lags.
Challenge(a) == \E s \in Prefixes :
  LET v == FoldOf(s) IN
  LET u == NewUuid(a, v) IN
  /\ st[a] = "idle"
  /\ Eager \/ ~LiveHolder
  /\ RefuseMulti => v.leader # None
  /\ u \in Uuids
  /\ nuuid' = IF u > nuuid THEN u ELSE nuuid
  /\ st' = [st EXCEPT ![a] = "campaign"]
  /\ mine' = [mine EXCEPT ![a] = u]
  /\ base' = [base EXCEPT ![a] = <<v.leader, v.term, a>>]
  /\ net' = net \cup {SL(a, u, v.leader, a)}
  /\ UNCHANGED <<inc, term, pub, wfly, journal, receiver, owner, budget, ghost>>

\* A candidate, or a holder that hands on, with no message in flight (one
\* was lost) sends the same call again.
Retry(a) ==
  /\ st[a] \in {"campaign", "handing"}
  /\ Pending(a) = {}
  /\ net' = net \cup {SL(a, mine[a], base[a][1], base[a][3])}
  /\ UNCHANGED <<actor, journal, receiver, nuuid, owner, budget, ghost>>

\* Take term t with uuid u: the term record (the interface) goes first.
Take(a, u, t) ==
  /\ st' = [st EXCEPT ![a] = "lead"]
  /\ mine' = [mine EXCEPT ![a] = u]
  /\ term' = [term EXCEPT ![a] = t]
  /\ pub' = [pub EXCEPT ![a] = FALSE]
  /\ wfly' = [wfly EXCEPT ![a] = TRUE]
  /\ owner' = [owner EXCEPT ![u] = <<a, inc[a]>>]

\* The answer to the `SetLeader` of a campaign. A refusal that names the
\* candidate's own uuid is a win: an earlier copy of its call was applied.
\* The winner reads the log up to the length the answer names (the tail of
\* its view): its number is the number of the last term record there, plus
\* one. No term record lands between its win and that answer, since only
\* its uuid may write then.
CampaignAnswer(a) ==
  \E m \in net :
    /\ m.k = "slans" /\ m.to = a /\ st[a] = "campaign"
    /\ IF m.leader = mine[a]
       THEN LET t == FoldOf(SubSeq(jlog, 1, m.len)).term + 1 IN
            /\ Take(a, mine[a], t)
            /\ net' = (net \ {m}) \cup {TermRecord(a, mine[a], t)}
            /\ Hit(IF m.ok THEN "won" ELSE "won_by_refusal")
       ELSE /\ st' = [st EXCEPT ![a] = "idle"]
            /\ net' = net \ {m}
            /\ Hit("lost_cas")
            /\ UNCHANGED <<mine, term, pub, wfly, owner>>
    /\ UNCHANGED <<inc, base, journal, receiver, nuuid, budget, stale, regress,
                   userSet>>

\* A holder writes: its term record until the journal holds it, then a
\* renewal. One write of it is in flight at a time.
Renew(a) ==
  /\ st[a] = "lead" /\ ~wfly[a]
  /\ wfly' = [wfly EXCEPT ![a] = TRUE]
  /\ net' = net \cup {[TermRecord(a, mine[a], term[a])
                         EXCEPT !.rec = IF pub[a] THEN "renew" ELSE "term"]}
  /\ UNCHANGED <<inc, st, mine, base, term, pub, journal, receiver, nuuid, owner,
                 budget, ghost>>

\* A write with no answer (it or its answer was lost) is given up; the next
\* write goes again.
Timeout(a) ==
  /\ Pending(a) = {}
  /\ wfly' = [wfly EXCEPT ![a] = FALSE]
  /\ UNCHANGED <<inc, st, mine, base, term, pub, journal, net, receiver, nuuid,
                 owner, budget, ghost>>

\* The answer to a write. A refusal deposes the holder (Stop). An answer
\* counts only for the uuid that its write was sent under: an answer to a
\* write of an earlier term of the same actor neither deposes nor publishes
\* its new term (OwnAnswer).
Own(a, m) == OwnAnswer => m.u = mine[a]

WriteAnswer(a) ==
  \E m \in net :
    /\ m.k = "wans" /\ m.to = a
    /\ net' = net \ {m}
    /\ wfly' = [wfly EXCEPT ![a] = FALSE]
    /\ IF m.ok
       THEN /\ pub' = [pub EXCEPT ![a] = pub[a] \/ (m.rec = "term" /\ Own(a, m))]
            /\ UNCHANGED <<st, term, reached>>
       ELSE /\ IF Stop /\ st[a] = "lead" /\ Own(a, m)
               THEN /\ st' = [st EXCEPT ![a] = "idle"]
                    /\ term' = [term EXCEPT ![a] = 0]
                    /\ Hit(IF m.leader = None THEN "multi_stop" ELSE "deposed_stop")
               ELSE UNCHANGED <<st, term, reached>>
            /\ UNCHANGED pub
    /\ UNCHANGED <<inc, mine, base, journal, receiver, nuuid, owner, budget, stale,
                   regress, userSet>>

\* A holder sends an order out of the journal; the order carries the term.
\* It sends one only once its term record is in the journal: a holder that
\* has not published may share its number with the next winner.
Order(a) ==
  /\ st[a] = "lead"
  /\ pub[a]
  /\ onet' = onet \cup {term[a]}
  /\ orders' = orders \cup {<<term[a], a, inc[a]>>}
  /\ UNCHANGED <<actor, journal, net, seen, nuuid, owner, budget, ghost>>

\* A clean stop: `SetLeader(next, me)`, with a fresh uuid for the next actor.
HandOn(a, b) ==
  /\ st[a] = "lead" /\ hands < MaxHands
  /\ nuuid < MaxUuids
  /\ hands' = hands + 1
  /\ nuuid' = nuuid + 1
  /\ st' = [st EXCEPT ![a] = "handing"]
  /\ base' = [base EXCEPT ![a] = <<mine[a], term[a], b>>]
  /\ mine' = [mine EXCEPT ![a] = nuuid + 1]
  /\ net' = net \cup {SL(a, nuuid + 1, mine[a], b)}
  /\ UNCHANGED <<inc, term, pub, wfly, journal, receiver, owner, crashes, lost,
                 users, ghost>>

\* The answer to a hand on: the holder stops either way. On a win, the next
\* actor gets the uuid and the term number.
HandAnswer(a) ==
  \E m \in net :
    /\ m.k = "slans" /\ m.to = a /\ st[a] = "handing"
    /\ st' = [st EXCEPT ![a] = "idle"]
    /\ term' = [term EXCEPT ![a] = 0]
    /\ net' = (net \ {m}) \cup
              IF m.leader = mine[a]
              THEN {[Blank EXCEPT !.k = "lead", !.to = base[a][3], !.u = mine[a],
                                  !.t = base[a][2] + 1]}
              ELSE {}
    /\ UNCHANGED <<inc, mine, base, pub, wfly, journal, receiver, nuuid, owner,
                   budget, ghost>>

\* The next actor takes the handed term when it is idle. Else the term has
\* no holder, and a challenge takes the next one. A write of an earlier term
\* of the next actor may still be in flight: its answer does not count for
\* the new term (OwnAnswer).
Adopt(b) ==
  \E m \in net :
    /\ m.k = "lead" /\ m.to = b
    /\ IF st[b] = "idle"
       THEN /\ Take(b, m.u, m.t)
            /\ net' = (net \ {m}) \cup {TermRecord(b, m.u, m.t)}
            /\ Hit("adopted")
       ELSE /\ net' = net \ {m}
            /\ UNCHANGED <<st, mine, term, pub, wfly, owner, reached>>
    /\ UNCHANGED <<inc, base, journal, receiver, nuuid, budget, stale, regress,
                   userSet>>

-----------------------------------------------------------------------------
\* The journal.

\* The answer to a call goes to the incarnation that sent it: an actor that
\* stopped since gets no answer.
Reply(m, ans) == IF m.inc = inc[m.from] THEN {ans} ELSE {}

\* `SetLeader(u, old)`: taken iff old is the leader (Cas) and u is not.
ApplySetLeader ==
  \E m \in net :
    /\ m.k = "sl"
    /\ LET ok == (Cas => m.old = jleader) /\ m.u # jleader IN
       LET nl == IF ok THEN m.u ELSE jleader IN
       /\ jleader' = nl
       /\ jlog' = IF ok THEN Append(jlog, [k |-> "set", u |-> m.u, iface |-> NoIface,
                                           t |-> 0])
                  ELSE jlog
       /\ net' = (net \ {m}) \cup
                 Reply(m, [Blank EXCEPT !.k = "slans", !.to = m.from, !.ok = ok,
                                        !.leader = nl, !.next = m.next,
                                        !.len = Len(jlog')])
       /\ Hit(IF ~ok /\ m.old \in Uuids /\ m.old # jleader THEN "late_cas_refused"
              ELSE "")
    /\ UNCHANGED <<actor, receiver, nuuid, owner, budget, stale, regress, userSet>>

\* A fenced write is taken only while its uuid is the leader (§J2.4.1).
ApplyWrite ==
  \E m \in net :
    /\ m.k = "w"
    /\ LET ok == m.u = jleader IN
       /\ jlog' = IF ok /\ m.rec = "term"
                  THEN Append(jlog, [k |-> "term", u |-> m.u, iface |-> m.iface, t |-> m.t])
                  ELSE jlog
       /\ stale' = (stale \/ (ok /\ m.t < JTerm))
       /\ net' = (net \ {m}) \cup
                 Reply(m, [Blank EXCEPT !.k = "wans", !.to = m.from, !.ok = ok, !.u = m.u,
                                        !.t = m.t, !.rec = m.rec, !.leader = jleader])
    /\ UNCHANGED <<actor, jleader, receiver, nuuid, owner, budget, regress, userSet,
                   reached>>

\* The receiver of the orders: it refuses an order below its fence.
Deliver ==
  \E t \in onet :
    /\ onet' = onet \ {t}
    /\ IF Fence /\ t < seen
       THEN /\ Hit("stale_refused")
            /\ UNCHANGED <<seen, regress>>
       ELSE /\ seen' = IF t > seen THEN t ELSE seen
            /\ regress' = (regress \/ t < seen)
            /\ UNCHANGED reached
    /\ UNCHANGED <<actor, journal, net, orders, nuuid, owner, budget, stale, userSet>>

-----------------------------------------------------------------------------
\* The faults.

\* An actor stops: its memory and the messages to it are lost. It comes
\* back at once as a new incarnation, its fold where it was.
Crash(a) ==
  /\ crashes < MaxCrashes
  /\ crashes' = crashes + 1
  /\ inc' = [inc EXCEPT ![a] = inc[a] + 1]
  /\ st' = [st EXCEPT ![a] = "idle"] /\ mine' = [mine EXCEPT ![a] = 0]
  /\ term' = [term EXCEPT ![a] = 0] /\ pub' = [pub EXCEPT ![a] = FALSE]
  /\ wfly' = [wfly EXCEPT ![a] = FALSE]
  /\ base' = [base EXCEPT ![a] = <<Unset, 0, a>>]
  /\ net' = {m \in net : m.to # a}
  /\ UNCHANGED <<journal, receiver, nuuid, owner, lost, hands, users, ghost>>

Lose ==
  /\ lost < MaxLoss
  /\ \E m \in net : net' = net \ {m}
  /\ lost' = lost + 1
  /\ UNCHANGED <<actor, journal, receiver, nuuid, owner, crashes, hands, users,
                 ghost>>

\* The user of the journal switches it to multi-writer mode.
UserMulti ==
  /\ users < MaxUser
  /\ users' = users + 1
  /\ jleader' = None
  /\ jlog' = Append(jlog, [k |-> "set", u |-> None, iface |-> NoIface, t |-> 0])
  /\ userSet' = TRUE
  /\ UNCHANGED <<actor, net, receiver, nuuid, owner, crashes, lost, hands, stale,
                 regress, reached>>

-----------------------------------------------------------------------------
Faults == (\E a \in Actors : Crash(a)) \/ Lose \/ UserMulti

Next ==
  \/ \E a \in Actors :
       \/ Challenge(a) \/ Retry(a) \/ CampaignAnswer(a) \/ Renew(a)
       \/ Timeout(a) \/ WriteAnswer(a) \/ HandAnswer(a) \/ Adopt(a) \/ Order(a)
       \/ \E b \in Actors : HandOn(a, b)
  \/ ApplySetLeader \/ ApplyWrite \/ Deliver
  \/ Faults

Fairness ==
  /\ \A a \in Actors :
       /\ WF_vars(Challenge(a)) /\ WF_vars(Retry(a))
       /\ WF_vars(CampaignAnswer(a)) /\ WF_vars(Renew(a)) /\ WF_vars(Timeout(a))
       /\ WF_vars(WriteAnswer(a)) /\ WF_vars(HandAnswer(a)) /\ WF_vars(Adopt(a))
  /\ WF_vars(ApplySetLeader) /\ WF_vars(ApplyWrite) /\ WF_vars(Deliver)

Spec == Init /\ [][Next]_vars
LiveSpec == Spec /\ Fairness

-----------------------------------------------------------------------------
\* The properties.

\* A holder that published holds the number of its term record.
TermTrue ==
  \A a \in Actors : st[a] = "lead" /\ pub[a] => TermOfUuid(mine[a]) = term[a]

\* The numbers of the term records grow along the log: a later term never
\* takes a number that is taken. Two term records of one uuid are copies of
\* one write (a resend whose answer was lost).
TermsGrow ==
  \A i, j \in 1..Len(jlog) :
    /\ i < j /\ jlog[i].k = "term" /\ jlog[j].k = "term" /\ jlog[i].u # jlog[j].u
    => jlog[i].t < jlog[j].t

\* One holder sends the orders of one term.
OneOrderer ==
  \A x, y \in orders : x[1] = y[1] => x = y

\* A write is taken only in the term it was sent in: never below the last
\* term record.
WriteInTerm == ~stale

\* The receiver never applies an order below one it applied.
NoRegress == ~regress

\* A holder stops at its first refused write: the step that reads the
\* refusal of a write of its term leaves it with no term.
StopsAtRefusal ==
  [][\A a \in Actors :
       (/\ \E m \in net : m.k = "wans" /\ m.to = a /\ ~m.ok /\ m \notin net'
                          /\ m.u = mine[a]
        /\ st[a] = "lead" /\ wfly[a] /\ ~wfly'[a] /\ inc'[a] = inc[a])
       => term'[a] = 0]_vars

\* The interface that a fold publishes is the one of the holder of the
\* fold's term, in the incarnation that took it.
IfaceOfTerm ==
  \A s \in Prefixes :
    LET f == FoldOf(s) IN
    f.iface = NoIface \/ (f.leader \in Uuids /\ f.iface = owner[f.leader])

\* A holder that believes its interface is published finds its term record,
\* with its own incarnation, in the journal.
PubTrue ==
  \A a \in Actors :
    st[a] = "lead" /\ pub[a] =>
      \E i \in 1..Len(jlog) : jlog[i].k = "term" /\ jlog[i].u = mine[a]
                               /\ jlog[i].iface = <<a, inc[a]>>

\* The election never takes a journal out of multi-writer mode.
ModeKept == userSet => jleader = None

\* For the reach checks: the event Ev never happens.
NotReached == ~reached

\* Once the faults stop, an actor leads the term in force and its
\* interface is published.
Leads == <>[](\E a \in Actors : st[a] = "lead" /\ mine[a] = jleader /\ pub[a])

TypeOK ==
  /\ st \in [Actors -> {"idle", "campaign", "lead", "handing"}]
  /\ jleader \in Uuids \cup {Unset, None}
  /\ nuuid \in 0..MaxUuids
  /\ onet \subseteq 1..MaxUuids
=============================================================================
