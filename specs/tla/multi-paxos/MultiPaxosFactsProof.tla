------------------------ MODULE MultiPaxosFactsProof ------------------------
(***************************************************************************)
(* The facts that the TLAPS proof of Multi-Paxos shares (#1307 (inductive  *)
(* invariants, then proofs), step 2): the hypotheses `Hyp`, the facts of   *)
(* the constants and of a state where `Inv` holds, `Inv` implies           *)
(* `Agreement`, `Init` implies `Inv`, and the lemmas that each step uses.  *)
(* `MultiPaxosProof.tla` is the map of the proof.                          *)
(***************************************************************************)
EXTENDS MultiPaxos, FiniteSetTheorems, WellFoundedInduction, TLAPS

\* Each Phase-1 quorum meets each Phase-2 quorum of each slot. It is not an
\* ASSUME of the model: the control `full_grid_one_column` breaks it.
QuorumFacts ==
  /\ Q1 \subseteq SUBSET Acceptors
  /\ Q1 # {}
  /\ \A s \in Slots : \A Q \in Q1 : \A R \in Q2[s] : Q \cap R # {}

\* The configurations that the proof covers.
Hyp == InvApplies /\ Pick = "highest" /\ Alloc = "frontier" /\ QuorumFacts

USE NBType, NSType, NoneNotValue, NoopNotCommand, OwnerType, LearnersType
    DEF Ballots, Slots, Values, NoVote

-----------------------------------------------------------------------------
(* Facts about the constants and the types.                                *)
(***************************************************************************)

\* A nonempty set of naturals has a least member (from the well-founded
\* order of Nat; TLAPS proves this with no induction rule).
LEMMA MinExists ==
  ASSUME NEW T \in SUBSET Nat, T # {}
  PROVE  \E x \in T : \A y \in T : x <= y
<1>1. \E x \in T : \A y \in T : ~(<<y, x>> \in OpToRel(<, Nat))
  BY NatLessThanWellFounded, WFMin
<1>2. PICK x \in T : \A y \in T : ~(<<y, x>> \in OpToRel(<, Nat))
  BY <1>1
<1>3. \A y \in T : ~(y < x)
  BY <1>2 DEF OpToRel
<1> QED
  BY <1>3

\* A nonempty set of naturals with an upper bound has a highest member: the
\* least upper bound is in the set.
LEMMA MaxExists ==
  ASSUME NEW N \in Nat, NEW S \in SUBSET (0..N), S # {}
  PROVE  \E k \in S : \A j \in S : j <= k
<1> DEFINE U == {k \in Nat : \A j \in S : j <= k}
<1>1. U \in SUBSET Nat /\ U # {}
  BY N \in U
<1>2. PICK u \in U : \A y \in U : u <= y
  BY <1>1, MinExists
<1>3. u \in S
  <2> SUFFICES ASSUME u \notin S PROVE FALSE
    OBVIOUS
  <2>1. PICK j \in S : TRUE
    OBVIOUS
  <2>2. u - 1 \in U
    BY <2>1
  <2> QED
    BY <2>2, <1>2
<1> QED
  BY <1>2, <1>3

LEMMA WipedFinite ==
  ASSUME TypeOK
  PROVE  /\ IsFiniteSet(wiped)
         /\ Cardinality(wiped) \in Nat
BY AcceptorsFinite, FS_Subset, FS_CardinalityType DEF TypeOK

\* The fields of each kind of message.
LEMMA MsgFields ==
  ASSUME TypeOK, NEW m \in msgs
  PROVE  /\ m.type = "1a" => m \in [type : {"1a"}, bal : Ballots, from : 0..NS]
         /\ m.type = "1b" => m \in [type : {"1b"}, acc : Acceptors, bal : Ballots, from : 0..NS,
                                    rep : [Slots -> Vote]]
         /\ m.type = "2a" => m \in [type : {"2a"}, bal : Ballots, slot : Slots, val : Values]
         /\ m.type = "2b" => m \in [type : {"2b"}, acc : Acceptors, bal : Ballots, slot : Slots,
                                    val : Values]
         /\ m.type \in {"1a", "1b", "2a", "2b"}
BY DEF TypeOK, Msg

-----------------------------------------------------------------------------
(* Facts of a state where `Inv` holds.                                     *)
(***************************************************************************)

\* A vote is for the proposal of its ballot and slot.
LEMMA VotedProposed ==
  ASSUME Inv, NEW a, NEW c, NEW s, NEW v, VotedFor(a, c, s, v)
  PROVE  Proposed(c, s, v)
<1>1. MsgOK([type |-> "2b", acc |-> a, bal |-> c, slot |-> s, val |-> v])
  BY DEF Inv, MsgInv, VotedFor
<1> QED
  BY <1>1 DEF MsgOK

\* A ballot proposes one value at each slot.
LEMMA OneValue ==
  ASSUME Inv, NEW c, NEW s, NEW v, NEW w, Proposed(c, s, v), Proposed(c, s, w)
  PROVE  v = w
<1>1. MsgOK([type |-> "2a", bal |-> c, slot |-> s, val |-> v])
  BY DEF Inv, MsgInv, Proposed
<1> QED
  BY <1>1 DEF MsgOK, Proposed

LEMMA ProposedSafe ==
  ASSUME Inv, NEW c, NEW s, NEW v, Proposed(c, s, v)
  PROVE  SafeAt(c, s, v)
<1>1. MsgOK([type |-> "2a", bal |-> c, slot |-> s, val |-> v])
  BY DEF Inv, MsgInv, Proposed
<1> QED
  BY <1>1 DEF MsgOK

\* A value chosen at a ballot is the value of every proposal of the slot at
\* that ballot or above.
LEMMA ChosenProposedAbove ==
  ASSUME Inv, Hyp, NEW s \in Slots, NEW b0 \in Ballots, NEW c \in Ballots,
         NEW v \in Values, NEW w \in Values,
         ChosenAt(b0, s, v), Proposed(c, s, w), b0 <= c
  PROVE  v = w
<1>1. PICK R \in Q2[s] : \A a \in R : VotedFor(a, b0, s, v)
  BY DEF ChosenAt, VotedFor
<1>2. CASE c = b0
  <2>1. PICK Q \in Q1 : TRUE
    BY DEF Hyp, QuorumFacts
  <2>2. PICK a \in R : TRUE
    BY <2>1 DEF Hyp, QuorumFacts
  <2>3. Proposed(b0, s, v)
    BY <1>1, <2>2, VotedProposed
  <2> QED
    BY <1>2, <2>3, OneValue
<1>3. CASE c > b0
  <2>1. SafeAt(c, s, w)
    BY ProposedSafe
  <2>2. PICK Q \in Q1 : \A a \in Q : VotedFor(a, b0, s, w) \/ WontVoteIn(a, b0, s)
    BY <2>1, <1>3 DEF SafeAt
  <2>3. PICK a \in Q \cap R : TRUE
    BY DEF Hyp, QuorumFacts
  <2>4. VotedFor(a, b0, s, w)
    BY <1>1, <2>2, <2>3 DEF WontVoteIn
  <2> QED
    BY <1>1, <2>3, <2>4, VotedProposed, OneValue
<1> QED
  BY <1>2, <1>3

THEOREM InvAgreement == Inv /\ Hyp => Agreement
<1> SUFFICES ASSUME Inv, Hyp, NEW s \in Slots, NEW v \in Values, NEW w \in Values,
                    Chosen(s, v), Chosen(s, w)
             PROVE  v = w
  BY DEF Agreement
<1>1. PICK b1 \in Ballots : ChosenAt(b1, s, v)
  BY DEF Chosen
<1>2. PICK b2 \in Ballots : ChosenAt(b2, s, w)
  BY DEF Chosen
\* A value chosen at ballot b was proposed at b.
<1>3. ASSUME NEW b \in Ballots, NEW u \in Values, ChosenAt(b, s, u) PROVE Proposed(b, s, u)
  <2>1. PICK R \in Q2[s] : \A a \in R : VotedFor(a, b, s, u)
    BY <1>3 DEF ChosenAt, VotedFor
  <2>2. PICK Q \in Q1 : TRUE
    BY DEF Hyp, QuorumFacts
  <2>3. PICK a \in R : TRUE
    BY <2>2 DEF Hyp, QuorumFacts
  <2> QED
    BY <2>1, <2>3, VotedProposed
<1>4. CASE b1 <= b2
  BY <1>1, <1>2, <1>3, <1>4, ChosenProposedAbove
<1>5. CASE b2 < b1
  BY <1>1, <1>2, <1>3, <1>5, ChosenProposedAbove
<1> QED
  BY <1>4, <1>5

THEOREM InitInv == Init => Inv
<1> SUFFICES ASSUME Init PROVE Inv
  OBVIOUS
<1>1. TypeOK
  BY DEF Init, TypeOK, Vote
<1>2. Cardinality(wiped) <= Wipes /\ restarts <= Restarts
  BY FS_EmptySet, WipesType, RestartsType DEF Init
<1>3. NodeInv
  BY DEF Init, NodeInv, NodeOK, NodeSlotOK, RecordOK, VotedFor, Proposed
<1>4. BallotInv
  BY DEF Init, BallotInv, BallotOK, Proposed
<1>5. MsgInv
  BY DEF Init, MsgInv
<1> QED
  BY <1>1, <1>2, <1>3, <1>4, <1>5 DEF Init, Inv
-----------------------------------------------------------------------------
(* What a step keeps.                                                      *)
(***************************************************************************)

\* A step keeps every vote, and every promise not to vote.
KeepsVW ==
  \A a \in Acceptors, c \in Ballots, s \in Slots :
    /\ \A v \in Values : VotedFor(a, c, s, v) => VotedFor(a, c, s, v)'
    /\ WontVoteIn(a, c, s) => WontVoteIn(a, c, s)'

\* The promises rise or the acceptor is wiped, the wiped stay wiped, and no
\* acceptor votes where it promised not to.
LEMMA KeepsVWFrom ==
  ASSUME TypeOK, msgs \subseteq msgs', wiped \subseteq wiped',
         \A a \in Acceptors : a \in wiped' \/ (prom'[a] \in Nat /\ prom'[a] >= prom[a]),
         \A a \in Acceptors, c \in Ballots, s \in Slots :
           WontVoteIn(a, c, s) => \A v \in Values : ~VotedFor(a, c, s, v)'
  PROVE  KeepsVW
<1> SUFFICES ASSUME NEW a \in Acceptors, NEW c \in Ballots, NEW s \in Slots
             PROVE  /\ \A v \in Values : VotedFor(a, c, s, v) => VotedFor(a, c, s, v)'
                    /\ WontVoteIn(a, c, s) => WontVoteIn(a, c, s)'
  BY DEF KeepsVW
<1>1. \A v \in Values : VotedFor(a, c, s, v) => VotedFor(a, c, s, v)'
  BY DEF VotedFor
<1>2. ASSUME WontVoteIn(a, c, s) PROVE WontVoteIn(a, c, s)'
  <2>1. prom[a] \in Nat
    BY DEF TypeOK
  <2> QED
    BY <1>2, <2>1 DEF WontVoteIn
<1> QED
  BY <1>1, <1>2

\* A step that sends no vote keeps every vote as it is.
LEMMA NoNewVote ==
  ASSUME NEW M, \A m \in M : m.type # "2b", msgs' = msgs \cup M
  PROVE  \A a, c, s, v : VotedFor(a, c, s, v)' = VotedFor(a, c, s, v)
BY DEF VotedFor

LEMMA SafeAtKeep ==
  ASSUME Hyp, KeepsVW, NEW b \in Ballots, NEW s \in Slots, NEW v \in Values, SafeAt(b, s, v)
  PROVE  SafeAt(b, s, v)'
<1> SUFFICES ASSUME NEW c \in 1..(b - 1)
             PROVE  \E Q \in Q1 : \A a \in Q : VotedFor(a, c, s, v)' \/ WontVoteIn(a, c, s)'
  BY DEF SafeAt
<1>1. PICK Q \in Q1 : \A a \in Q : VotedFor(a, c, s, v) \/ WontVoteIn(a, c, s)
  BY DEF SafeAt
<1>2. Q \subseteq Acceptors /\ c \in Ballots
  BY DEF Hyp, QuorumFacts
<1> QED
  BY <1>1, <1>2 DEF KeepsVW

LEMMA FreeAtKeep ==
  ASSUME Hyp, KeepsVW, NEW b \in Ballots, NEW s \in Slots, FreeAt(b, s)
  PROVE  FreeAt(b, s)'
<1> SUFFICES ASSUME NEW c \in 1..(b - 1)
             PROVE  \E Q \in Q1 : \A a \in Q : WontVoteIn(a, c, s)'
  BY DEF FreeAt
<1>1. PICK Q \in Q1 : \A a \in Q : WontVoteIn(a, c, s)
  BY DEF FreeAt
<1>2. Q \subseteq Acceptors /\ c \in Ballots
  BY DEF Hyp, QuorumFacts
<1> QED
  BY <1>1, <1>2 DEF KeepsVW

LEMMA FreeSafe ==
  ASSUME NEW b, NEW s, NEW v, FreeAt(b, s)
  PROVE  SafeAt(b, s, v)
BY DEF FreeAt, SafeAt

\* A record held below a promise of ballot `below` stays one when the
\* proposals only grow and no vote below `below` is added.
LEMMA RecordKeep ==
  ASSUME NEW a, NEW s, NEW r, NEW below, RecordOK(a, s, r, below), msgs \subseteq msgs',
         \A c \in Ballots, v \in Values : VotedFor(a, c, s, v)' /\ c < below => VotedFor(a, c, s, v)
  PROVE  RecordOK(a, s, r, below)'
BY DEF RecordOK, Proposed

\* A record of the node is a record below any promise above it.
LEMMA RecordBelow ==
  ASSUME NEW a, NEW s, NEW r, RecordOK(a, s, r, NB + 1), r.bal \in Nat,
         NEW below \in Ballots, r.bal < below
  PROVE  RecordOK(a, s, r, below)
BY DEF RecordOK

\* A message sent before the step stays true.
LEMMA OldMsg ==
  ASSUME Inv, Hyp, KeepsVW, NEW m \in msgs, msgs \subseteq msgs',
         \A x \in Acceptors : x \notin wiped' => /\ x \notin wiped
                                                /\ prom'[x] \in Nat
                                                /\ prom'[x] >= prom[x],
         m.type = "1a" => \A n \in msgs' : n.type = "1a" /\ n.bal = m.bal => n \in msgs,
         m.type = "1b" => /\ \A n \in msgs' : n.type = "1b" /\ n.bal = m.bal /\ n.acc = m.acc => n \in msgs
                          /\ \A s \in Slots, c \in Ballots, v \in Values :
                               VotedFor(m.acc, c, s, v)' /\ c < m.bal => VotedFor(m.acc, c, s, v),
         m.type = "2a" => \A n \in msgs' : n.type = "2a" /\ n.bal = m.bal /\ n.slot = m.slot => n \in msgs
  PROVE  MsgOK(m)'
<1>0. MsgOK(m) /\ TypeOK
  BY DEF Inv, MsgInv
<1>1. CASE m.type = "1a"
  BY <1>0, <1>1 DEF MsgOK
<1>2. CASE m.type = "1b"
  <2>1. m \in [type : {"1b"}, acc : Acceptors, bal : Ballots, from : 0..NS, rep : [Slots -> Vote]]
    BY <1>0, <1>2, MsgFields
  <2>2. \A s \in Slots : s >= m.from => RecordOK(m.acc, s, m.rep[s], m.bal)'
    BY <1>0, <1>2, RecordKeep DEF MsgOK
  <2>3. ASSUME m.acc \notin wiped' PROVE m.bal <= prom'[m.acc]
    <3>1. m.acc \notin wiped /\ prom'[m.acc] \in Nat /\ prom'[m.acc] >= prom[m.acc]
      BY <2>1, <2>3
    <3>2. m.bal <= prom[m.acc]
      BY <3>1, <1>0, <1>2 DEF MsgOK
    <3>3. prom[m.acc] \in Nat
      BY <2>1, <1>0 DEF TypeOK
    <3> QED
      BY <3>1, <3>2, <3>3, <2>1
  <2> QED
    BY <1>0, <1>2, <2>2, <2>3 DEF MsgOK
<1>3. CASE m.type = "2a"
  <2>1. m \in [type : {"2a"}, bal : Ballots, slot : Slots, val : Values]
    BY <1>0, <1>3, MsgFields
  <2>2. SafeAt(m.bal, m.slot, m.val)'
    BY <1>0, <1>3, <2>1, SafeAtKeep DEF MsgOK
  <2> QED
    BY <1>0, <1>3, <2>2 DEF MsgOK
<1>4. CASE m.type = "2b"
  <2>1. m \in [type : {"2b"}, acc : Acceptors, bal : Ballots, slot : Slots, val : Values]
    BY <1>0, <1>4, MsgFields
  <2>2. m.acc \notin wiped' => m.bal <= prom'[m.acc]
    BY <1>0, <1>4, <2>1 DEF MsgOK, TypeOK
  <2> QED
    BY <1>0, <1>4, <2>2 DEF MsgOK, Proposed
<1> QED
  BY <1>0, <1>1, <1>2, <1>3, <1>4, MsgFields

LEMMA ChosenByKeep ==
  ASSUME msgs \subseteq msgs', NEW s, NEW c, NEW v, ChosenBy(s, c, v)
  PROVE  ChosenBy(s, c, v)'
BY DEF ChosenBy, ChosenAt

\* A slot of a node that is not wiped, whose record the step does not
\* change, but for a higher promise and a learn that it forgets, stays true.
LEMMA NodeSlotKeep ==
  ASSUME Inv, NEW x \in Acceptors, x \notin wiped, NEW s \in Slots, msgs \subseteq msgs',
         vote'[x][s] = vote[x][s],
         learned'[x][s] = learned[x][s] \/ learned'[x][s] = None,
         prom'[x] \in Nat, prom'[x] >= prom[x],
         \A c \in Ballots, v \in Values : VotedFor(x, c, s, v)' = VotedFor(x, c, s, v)
  PROVE  NodeSlotOK(x, s)'
<1>1. NodeSlotOK(x, s) /\ vote[x][s] \in Vote /\ prom[x] \in Nat
  <2>1. NodeOK(x) /\ TypeOK
    BY DEF Inv, NodeInv
  <2> QED
    BY <2>1 DEF NodeOK, TypeOK
<1>2. RecordOK(x, s, vote[x][s], NB + 1)'
  BY <1>1, RecordKeep DEF NodeSlotOK
<1>3. \A c, v : ChosenBy(s, c, v) => ChosenBy(s, c, v)'
  BY ChosenByKeep
<1>4. vote'[x][s].bal <= prom'[x]
  BY <1>1 DEF NodeSlotOK, Vote
<1>5. (vote[x][s].bal > 0 /\ ~VotedFor(x, vote[x][s].bal, s, vote[x][s].val)
         => ChosenBy(s, vote[x][s].bal, vote[x][s].val))'
  <2> SUFFICES ASSUME vote[x][s].bal > 0, ~VotedFor(x, vote[x][s].bal, s, vote[x][s].val)'
               PROVE  ChosenBy(s, vote[x][s].bal, vote[x][s].val)'
    OBVIOUS
  <2>1. vote[x][s].bal \in Ballots /\ vote[x][s].val \in Values
    BY <1>1 DEF NodeSlotOK, RecordOK, Vote
  <2>2. ~VotedFor(x, vote[x][s].bal, s, vote[x][s].val)
    BY <2>1
  <2> QED
    BY <2>1, <2>2, <1>1, <1>3 DEF NodeSlotOK
<1>6. CASE learned'[x][s] = learned[x][s]
  <2>1. (learned[x][s] # None => x \in Learners \cup {Owner[b] : b \in Ballots})'
    BY <1>1, <1>6 DEF NodeSlotOK
  <2>2. (learned[x][s] # None => /\ vote[x][s].val = learned[x][s]
                                 /\ ChosenBy(s, vote[x][s].bal, learned[x][s]))'
    <3> SUFFICES ASSUME learned[x][s] # None
                 PROVE  /\ vote[x][s].val = learned[x][s]
                        /\ ChosenBy(s, vote[x][s].bal, learned[x][s])'
      BY <1>6
    <3>1. ChosenBy(s, vote[x][s].bal, learned[x][s]) /\ vote[x][s].val = learned[x][s]
      BY <1>1 DEF NodeSlotOK
    <3> QED
      BY <3>1, <1>3, <1>6
  <2> QED
    BY <2>1, <2>2, <1>2, <1>4, <1>5 DEF NodeSlotOK
<1>7. CASE learned'[x][s] = None
  BY <1>2, <1>4, <1>5, <1>7 DEF NodeSlotOK
<1> QED
  BY <1>6, <1>7

\* A node whose memory the step does not change, but for a higher promise
\* and a shorter learned prefix, stays true.
LEMMA NodeKeep ==
  ASSUME Inv, NEW x \in Acceptors, msgs \subseteq msgs',
         vote'[x] = vote[x],
         \A s \in Slots : learned'[x][s] = learned[x][s] \/ learned'[x][s] = None,
         x \in wiped' <=> x \in wiped,
         IF x \in wiped THEN prom'[x] = prom[x] ELSE prom'[x] \in Nat /\ prom'[x] >= prom[x],
         \A c \in Ballots, s \in Slots, v \in Values : VotedFor(x, c, s, v)' = VotedFor(x, c, s, v)
  PROVE  NodeOK(x)'
<1>0. NodeOK(x) /\ TypeOK
  BY DEF Inv, NodeInv
<1>1. CASE x \in wiped
  BY <1>0, <1>1 DEF NodeOK
<1>2. CASE x \notin wiped
  <2>0. x \notin wiped' /\ prom'[x] \in Nat /\ prom'[x] >= prom[x]
    BY <1>2
  <2> SUFFICES ASSUME NEW s \in Slots PROVE NodeSlotOK(x, s)'
    BY <2>0 DEF NodeOK
  <2> QED
    BY <1>2, <2>0, NodeSlotKeep
<1> QED
  BY <1>1, <1>2

\* A ballot that the step does not touch stays true.
LEMMA BallotKeep ==
  ASSUME Inv, Hyp, KeepsVW, NEW b \in Ballots, msgs \subseteq msgs', wiped \subseteq wiped',
         phase'[b] = phase[b], next'[b] = next[b], front'[b] = front[b],
         \A n \in msgs' : n.type \in {"1a", "2a"} /\ n.bal = b => n \in msgs,
         phase[b] \in {"p1", "p2"} => Owner[b] \notin wiped',
         Owner[b] \in wiped' \/ (prom'[Owner[b]] \in Nat /\ prom'[Owner[b]] >= prom[Owner[b]])
  PROVE  BallotOK(b)'
<1>0. BallotOK(b) /\ TypeOK
  BY DEF Inv, BallotInv
<1>1. (\E m \in msgs' : m.type = "1a" /\ m.bal = b) <=> (\E m \in msgs : m.type = "1a" /\ m.bal = b)
  OBVIOUS
<1>2. \A s \in Slots, v \in Values : Proposed(b, s, v)' => Proposed(b, s, v)
  BY DEF Proposed
<1>3. phase[b] = "p2" => \A s \in Slots : s >= next[b] => FreeAt(b, s)'
  BY <1>0, FreeAtKeep DEF BallotOK
<1>4. prom[Owner[b]] \in Nat
  BY <1>0 DEF TypeOK
<1> QED
  BY <1>0, <1>1, <1>2, <1>3, <1>4 DEF BallotOK

\* A ballot that the step ends (its owner restarts, loses its disk or
\* learns a higher ballot) stays true.
LEMMA BallotKill ==
  ASSUME Inv, NEW b \in Ballots, msgs \subseteq msgs', wiped \subseteq wiped',
         phase[b] \in {"p1", "p2"}, phase'[b] = "dead", next'[b] = next[b], front'[b] = front[b],
         \A n \in msgs' : n.type \in {"1a", "2a"} /\ n.bal = b => n \in msgs,
         Owner[b] \in wiped' \/ (prom'[Owner[b]] \in Nat /\ prom'[Owner[b]] >= prom[Owner[b]])
  PROVE  BallotOK(b)'
<1>0. BallotOK(b) /\ TypeOK
  BY DEF Inv, BallotInv
<1>2. \A s \in Slots, v \in Values : Proposed(b, s, v)' => Proposed(b, s, v)
  BY DEF Proposed
<1>4. prom[Owner[b]] \in Nat /\ next[b] \in Nat /\ front[b] \in Nat
  BY <1>0 DEF TypeOK
<1> QED
  BY <1>0, <1>2, <1>4 DEF BallotOK
-----------------------------------------------------------------------------
(* Each step keeps `Inv`.                                                  *)
(***************************************************************************)

LEMMA PersistedNew ==
  ASSUME Hyp
  PROVE  \A o, n : Persisted(o, n) = n
BY DEF Hyp, InvApplies, Persisted

LEMMA FirstUnlearnedType ==
  ASSUME TypeOK, NEW n \in Acceptors
  PROVE  FirstUnlearned(n) \in 0..NS
<1>1. CASE \A s \in Slots : learned[n][s] # None
  BY <1>1 DEF FirstUnlearned
<1>2. CASE ~\A s \in Slots : learned[n][s] # None
  <2> DEFINE T == {s \in Slots : learned[n][s] = None}
  <2>1. T \in SUBSET Nat /\ T # {}
    BY <1>2
  <2>2. PICK t \in T : \A y \in T : t <= y
    BY <2>1, MinExists
  <2>3. \E s \in Slots : learned[n][s] = None /\ \A u \in Slots : u < s => learned[n][u] # None
    BY <2>2
  <2> QED
    BY <1>2, <2>3 DEF FirstUnlearned
<1> QED
  BY <1>1, <1>2

\* No 2a and no 1a of an idle ballot was sent.
LEMMA IdleSilent ==
  ASSUME Inv, NEW b \in Ballots, phase[b] = "idle"
  PROVE  \A n \in msgs : n.type \in {"1a", "2a"} => n.bal # b
<1>0. BallotOK(b) /\ TypeOK
  BY DEF Inv, BallotInv
<1> SUFFICES ASSUME NEW n \in msgs, n.type \in {"1a", "2a"}, n.bal = b PROVE FALSE
  OBVIOUS
<1>1. CASE n.type = "1a"
  BY <1>0, <1>1 DEF BallotOK
<1>2. CASE n.type = "2a"
  <2>1. n \in [type : {"2a"}, bal : Ballots, slot : Slots, val : Values]
    BY <1>0, <1>2, MsgFields
  <2>2. Proposed(b, n.slot, n.val)
    BY <2>1 DEF Proposed
  <2> QED
    BY <1>0, <2>1, <2>2 DEF BallotOK
<1> QED
  BY <1>1, <1>2

\* An acceptor that is not wiped sent no promise above its promise.
LEMMA NoPromiseAbove ==
  ASSUME Inv, NEW a \in Acceptors, a \notin wiped, NEW b \in Ballots, prom[a] < b
  PROVE  \A n \in msgs : n.type = "1b" /\ n.acc = a => n.bal # b
<1>0. TypeOK /\ prom[a] \in Nat
  BY DEF Inv, TypeOK
<1> SUFFICES ASSUME NEW n \in msgs, n.type = "1b", n.acc = a, n.bal = b PROVE FALSE
  OBVIOUS
<1>1. MsgOK(n)
  BY DEF Inv, MsgInv
<1> QED
  BY <1>0, <1>1 DEF MsgOK

\* The records of a node that is not wiped hold below any ballot above its
\* promise, and they keep holding when its votes do not change.
LEMMA PromiseOK ==
  ASSUME Inv, NEW a \in Acceptors, a \notin wiped, NEW b \in Ballots, prom[a] < b,
         msgs \subseteq msgs',
         \A c \in Ballots, s \in Slots, v \in Values : VotedFor(a, c, s, v)' = VotedFor(a, c, s, v)
  PROVE  \A s \in Slots : \A r : r = vote[a][s] => RecordOK(a, s, r, b)'
<1> SUFFICES ASSUME NEW s \in Slots, NEW r, r = vote[a][s] PROVE RecordOK(a, s, r, b)'
  OBVIOUS
<1>0. NodeSlotOK(a, s) /\ vote[a][s] \in Vote /\ prom[a] \in Nat
  <2>1. NodeOK(a) /\ TypeOK
    BY DEF Inv, NodeInv
  <2>2. NodeSlotOK(a, s)
    BY <2>1 DEF NodeOK
  <2> QED
    BY <2>1, <2>2 DEF TypeOK
<1>1. r.bal \in Nat /\ r.bal < b
  BY <1>0 DEF NodeSlotOK, Vote
<1>2. RecordOK(a, s, r, NB + 1)
  BY <1>0 DEF NodeSlotOK
<1>3. RecordOK(a, s, r, b)
  BY <1>1, <1>2, RecordBelow
<1>4. \A c \in Ballots, v \in Values : VotedFor(a, c, s, v)' /\ c < b => VotedFor(a, c, s, v)
  OBVIOUS
<1> QED
  BY <1>3, <1>4, RecordKeep

=============================================================================
