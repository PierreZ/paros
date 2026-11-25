--------------------------- MODULE ControllerTerm ---------------------------
(***************************************************************************)
(* The term of the journal controller, on the model of Matchmaker Paxos    *)
(* (`../matchmaker/Matchmaker.tla`). Design: the journal controller is a   *)
(* role of the core that orders the changes of the shape of its journal.   *)
(* It is never the leader, and it proposes no value.                       *)
(*                                                                         *)
(* No paper has this role, so its one rule at the matchmakers is a         *)
(* departure (docs/architecture/04-7-segments.md §4.7.1). This model       *)
(* checks it: a check that passes with the rule, and a control that fails  *)
(* without it.                                                             *)
(*                                                                         *)
(* A controller holds a term t. The matchmakers are the acceptors of the   *)
(* term, and they stay passive:                                            *)
(*                                                                         *)
(*   1. Preside. The controller sends its term to the matchmakers. A       *)
(*      matchmaker with a lower promise raises its promise to t and        *)
(*      answers with its effective scalar: the highest ballot of a         *)
(*      reconfiguration that it registered (`JournalScalars::effective`).  *)
(*   2. The controller is elected on the answers of a matchmaker quorum.   *)
(*      Its view is the maximum of the answers.                            *)
(*   3. Order. An elected controller orders the leader to reconfigure. The *)
(*      leader opens a ballot b for the new configuration, as today, and   *)
(*      its registration carries the term of the order.                    *)
(*   4. The fence (the rule). A matchmaker refuses the registration of a   *)
(*      reconfiguration whose term is below its promise. A ballot that no  *)
(*      controller ordered (an election, which registers a belief) carries *)
(*      no term and is never refused by this rule.                         *)
(*                                                                         *)
(* What the rule gives (`NoSurprise`): a reconfiguration that an older     *)
(* term ordered, and that a matchmaker quorum registered, is at or below   *)
(* the view of each later elected term. So a new controller decides from a *)
(* view that no order of a deposed controller can pass.                    *)
(*                                                                         *)
(* What the rule does not give, and does not need to give: the agreement   *)
(* on a value. The fence only adds a refusal at a matchmaker, and a        *)
(* refusal is a lost answer, which Matchmaker Paxos already survives.      *)
(* `Agreement` and `Electable` pass with the fence and without it.         *)
(*                                                                         *)
(*   Fence   TRUE: rule 4. FALSE: a matchmaker ignores the term (control). *)
(*   View    "quorum": elected on a matchmaker quorum. "one": elected on   *)
(*           one answer (control).                                         *)
(***************************************************************************)
EXTENDS Matchmaker

CONSTANTS NT,      \* the terms are 1..NT
          Fence, View

ASSUME Fence \in BOOLEAN
ASSUME View \in {"quorum", "one"}

Terms == 1..NT

VARIABLES cprom,    \* matchmaker: the highest term it promised
          eff,      \* matchmaker: the highest ordered ballot it registered
          cans,     \* term: the answers to its Preside, <<matchmaker, eff>>
          elected,  \* term: TRUE once a quorum answered
          view,     \* term: the maximum effective ballot of its answers
          order     \* ballot: the term that ordered it, 0 for an election

cvars == <<cprom, eff, cans, elected, view, order>>
allvars == <<vars, cvars>>

CTypeOK ==
  /\ cprom \in [Matchmakers -> 0..NT]
  /\ eff \in [Matchmakers -> 0..NB]
  /\ cans \in [Terms -> SUBSET (Matchmakers \X (0..NB))]
  /\ elected \in [Terms -> BOOLEAN]
  /\ view \in [Terms -> 0..NB]
  /\ order \in [Ballots -> 0..NT]

CInit ==
  /\ Init
  /\ cprom = [m \in Matchmakers |-> 0]
  /\ eff = [m \in Matchmakers |-> 0]
  /\ cans = [t \in Terms |-> {}]
  /\ elected = [t \in Terms |-> FALSE]
  /\ view = [t \in Terms |-> 0]
  /\ order = [b \in Ballots |-> 0]

\* Matchmaker m promises term t and answers with its effective scalar.
Preside(m, t) ==
  /\ cprom[m] < t
  /\ cprom' = [cprom EXCEPT ![m] = t]
  /\ cans' = [cans EXCEPT ![t] = @ \cup {<<m, eff[m]>>}]
  /\ UNCHANGED <<vars, eff, elected, view, order>>

\* The controller of term t is elected on the answers S.
Elect(t) ==
  /\ ~elected[t]
  /\ \E S \in SUBSET cans[t] :
       /\ IF View = "quorum" THEN {r[1] : r \in S} \in MQuorums ELSE Cardinality(S) = 1
       /\ view' = [view EXCEPT ![t] = Max({r[2] : r \in S})]
  /\ elected' = [elected EXCEPT ![t] = TRUE]
  /\ UNCHANGED <<vars, cprom, eff, cans, order>>

\* The elected controller of term t orders a reconfiguration to c. The
\* leader opens ballot b for it.
Order(t, b, c) ==
  /\ elected[t]
  /\ Select(b, c)
  /\ order' = [order EXCEPT ![b] = t]
  /\ UNCHANGED <<cprom, eff, cans, elected, view>>

\* A candidate opens ballot b with no order: an election.
Belief(b, c) ==
  /\ Select(b, c)
  /\ UNCHANGED cvars

\* The registration of ballot b at matchmaker m, with the fence.
FRegister(m, b) ==
  /\ Fence => (order[b] = 0 \/ order[b] >= cprom[m])
  /\ Register(m, b)
  /\ eff' = [eff EXCEPT ![m] = IF order[b] > 0 /\ b > @ THEN b ELSE @]
  /\ UNCHANGED <<cprom, cans, elected, view, order>>

CNext ==
  \/ \E b \in Ballots, c \in Configs : Belief(b, c) \/ \E t \in Terms : Order(t, b, c)
  \/ \E m \in Matchmakers, b \in Ballots : FRegister(m, b)
  \/ \E m \in Matchmakers, t \in Terms : Preside(m, t)
  \/ \E t \in Terms : Elect(t)
  \/ /\ \/ \E m \in Matchmakers, b \in Ballots : GcAck(m, b)
        \/ \E b \in Ballots : CloseMatch(b) \/ Propose(b) \/ GcAsk(b)
        \/ \E a \in Acceptors, b \in Ballots : Promise(a, b) \/ Accept(a, b)
        \/ \E a \in Acceptors : Shutdown(a)
     /\ UNCHANGED cvars

CSpec == CInit /\ [][CNext]_allvars

\* A matchmaker quorum registered ballot b. `answers` keeps each answer, also
\* after GC forgot the registration.
Complete(b) == \E Q \in MQuorums : Q \subseteq {r.mm : r \in answers[b]}

\* The rule's property: no order of an older term passes the view of a later
\* elected term.
NoSurprise ==
  \A t \in Terms, b \in Ballots :
    (elected[t] /\ order[b] > 0 /\ order[b] < t /\ Complete(b)) => b <= view[t]

\* The effective scalar and the promise only grow.
CMonotone ==
  [][\A m \in Matchmakers : cprom'[m] >= cprom[m] /\ eff'[m] >= eff[m]]_allvars

\* Reachability controls: each must be violated.
\* A value is chosen at a ballot that a controller ordered.
NoOrderedChoice ==
  ~\E b \in Ballots, v \in Values : order[b] > 0 /\ ChosenAt(b, v)
\* A later term is elected, and an order of an older term is complete: the
\* later term saw it.
NoSeenOrder ==
  ~\E t \in Terms, b \in Ballots :
     elected[t] /\ order[b] > 0 /\ order[b] < t /\ Complete(b)
\* A matchmaker that promised a later term holds no registration of an
\* older term's order that came after: here, the fence refused nothing yet
\* visible. This control shows a state where the fence is the reason: an
\* ordered ballot that is selected, not complete, and that no quorum can
\* still register.
NoFencedOut ==
  ~\E b \in Ballots :
     /\ order[b] > 0 /\ ~Complete(b)
     /\ \A Q \in MQuorums : \E m \in Q :
          m \notin {r.mm : r \in answers[b]} /\ cprom[m] > order[b]
=============================================================================
