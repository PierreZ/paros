------------------------- MODULE MultiPaxosWinProof -------------------------
(***************************************************************************)
(* The win of Phase 1 keeps `Inv`: the recovery from the promises, the     *)
(* frontier, and the allocator above it (#1307, step 2).                   *)
(***************************************************************************)
EXTENDS MultiPaxosFactsProof

USE NBType, NSType, NoneNotValue, NoopNotCommand, OwnerType, LearnersType
    DEF Ballots, Slots, Values, NoVote

\* The highest report of the promises S at slot s.
LEMMA TopFacts ==
  ASSUME Inv, Hyp, NEW b \in Ballots, NEW S \in SUBSET {m \in msgs : m.type = "1b" /\ m.bal = b},
         NEW s \in Slots
  PROVE  /\ Top(S, s) \in Vote
         /\ \A m \in S : m.rep[s].bal <= Top(S, s).bal
         /\ Top(S, s).bal > 0 => \E m \in S : m.rep[s] = Top(S, s)
         /\ Top(S, s).bal = 0 => \A m \in S : m.rep[s].bal = 0
<1> DEFINE rp == {m \in S : m.rep[s].bal > 0}
<1>0. \A m \in S : m.rep[s] \in Vote /\ m.rep[s].bal \in 0..NB
  <2>1. TypeOK
    BY DEF Inv
  <2> QED
    BY <2>1, MsgFields DEF Vote
<1>1. Top(S, s) = IF rp = {} THEN NoVote
                  ELSE (CHOOSE m \in rp : \A n \in rp : n.rep[s].bal <= m.rep[s].bal).rep[s]
  BY DEF Top, Hyp
<1>2. CASE rp = {}
  <2>1. \A m \in S : m.rep[s].bal = 0
    BY <1>0, <1>2
  <2>2. NoVote \in Vote
    BY DEF Vote
  <2>3. Top(S, s) = NoVote /\ NoVote.bal = 0
    BY <1>1, <1>2
  <2> QED
    BY <2>1, <2>2, <2>3
<1>3. CASE rp # {}
  <2>1. \E m \in rp : \A n \in rp : n.rep[s].bal <= m.rep[s].bal
    <3>0. \A m \in rp : m.rep[s].bal \in 0..NB
      BY <1>0
    <3>1a. {m.rep[s].bal : m \in rp} \in SUBSET (0..NB)
      BY <3>0
    <3>1b. PICK m0 \in rp : TRUE
      BY <1>3
    <3>1c. m0.rep[s].bal \in {m.rep[s].bal : m \in rp}
      BY <3>1b
    <3>1. {m.rep[s].bal : m \in rp} \in SUBSET (0..NB) /\ {m.rep[s].bal : m \in rp} # {}
      BY <3>1a, <3>1c
    <3>2. PICK k \in {m.rep[s].bal : m \in rp} : \A j \in {m.rep[s].bal : m \in rp} : j <= k
      BY <3>1, MaxExists
    <3> QED
      BY <3>2
  <2> DEFINE mm == CHOOSE m \in rp : \A n \in rp : n.rep[s].bal <= m.rep[s].bal
  <2>2. mm \in rp /\ \A n \in rp : n.rep[s].bal <= mm.rep[s].bal
    BY <2>1
  <2>3. Top(S, s) = mm.rep[s]
    BY <1>1, <1>3
  <2>4. mm \in S /\ mm.rep[s].bal > 0
    BY <2>2
  <2>5. Top(S, s) \in Vote /\ Top(S, s).bal > 0
    BY <2>3, <2>4, <1>0
  <2>6. \A m \in S : m.rep[s].bal <= Top(S, s).bal
    <3> SUFFICES ASSUME NEW m \in S PROVE m.rep[s].bal <= mm.rep[s].bal
      BY <2>3
    <3>1. CASE m \in rp
      BY <3>1, <2>2
    <3>2. CASE m \notin rp
      <4>1. m.rep[s].bal = 0 /\ mm.rep[s].bal \in Nat
        BY <3>2, <1>0, <2>4
      <4> QED
        BY <4>1
    <3> QED
      BY <3>1, <3>2
  <2>7. \E m \in S : m.rep[s] = Top(S, s)
    BY <2>3, <2>4
  <2> QED
    BY <2>5, <2>6, <2>7
<1> QED
  BY <1>2, <1>3

\* The frontier of the promises S lies between `fr` and NS, and no promise
\* of S reports a vote at or above it.
LEMMA FrontierFacts ==
  ASSUME Inv, NEW b, NEW S \in SUBSET {m \in msgs : m.type = "1b" /\ m.bal = b},
         NEW fr \in 0..NS
  PROVE  /\ Frontier(S, fr) \in fr..NS
         /\ \A s \in Slots : s >= Frontier(S, fr) => \A m \in S : m.rep[s].bal = 0
<1> DEFINE h == {s \in Slots : s >= fr /\ \E m \in S : m.rep[s].bal > 0}
<1>0. \A m \in S, s \in Slots : m.rep[s].bal \in 0..NB
  <2>1. TypeOK
    BY DEF Inv
  <2> QED
    BY <2>1, MsgFields DEF Vote
<1>1. Frontier(S, fr) = IF h = {} THEN fr ELSE (CHOOSE s \in h : \A t \in h : t <= s) + 1
  BY DEF Frontier
<1>2. CASE h = {}
  <2>1. Frontier(S, fr) = fr
    BY <1>1, <1>2
  <2>2. \A s \in Slots : s >= fr => \A m \in S : ~(m.rep[s].bal > 0)
    BY <1>2
  <2> QED
    BY <2>1, <2>2, <1>0
<1>3. CASE h # {}
  <2>0. h \in SUBSET (0..NS)
    OBVIOUS
  <2>1. \E k \in h : \A t \in h : t <= k
    BY <2>0, <1>3, MaxExists
  <2> DEFINE mx == CHOOSE s \in h : \A t \in h : t <= s
  <2>2. mx \in h /\ \A t \in h : t <= mx
    BY <2>1
  <2>3. Frontier(S, fr) = mx + 1
    BY <1>1, <1>3
  <2> HIDE DEF mx
  <2>4. mx \in Slots /\ mx >= fr
    BY <2>2
  <2>5. \A s \in Slots : s >= mx + 1 => \A m \in S : ~(m.rep[s].bal > 0)
    BY <2>2, <2>4
  <2> QED
    BY <1>0, <2>3, <2>4, <2>5
<1> QED
  BY <1>2, <1>3

\* A promise of S at ballot b, from `fr`, bounds the votes of its acceptor
\* below b at each slot from `fr`.
LEMMA ReportBound ==
  ASSUME Inv, NEW b \in Ballots, NEW m \in msgs, m.type = "1b", m.bal = b,
         NEW fr, m.from = fr, NEW s \in Slots, s >= fr,
         NEW c \in Ballots, c < b, NEW w \in Values, VotedFor(m.acc, c, s, w)
  PROVE  c <= m.rep[s].bal /\ (c = m.rep[s].bal => w = m.rep[s].val)
<1>1. RecordOK(m.acc, s, m.rep[s], b)
  BY DEF Inv, MsgInv, MsgOK
<1>2. m.rep[s] \in Vote
  BY MsgFields DEF Inv
<1>3. c < m.rep[s].bal \/ (c = m.rep[s].bal /\ w = m.rep[s].val)
  BY <1>1 DEF RecordOK
<1> QED
  BY <1>2, <1>3 DEF Vote

\* The acceptor of a promise at ballot b will not vote below b.
LEMMA ReportWont ==
  ASSUME Inv, NEW b \in Ballots, NEW m \in msgs, m.type = "1b", m.bal = b,
         NEW c \in Ballots, c < b, NEW s,
         \A w \in Values : ~VotedFor(m.acc, c, s, w)
  PROVE  WontVoteIn(m.acc, c, s)
<1>0. TypeOK
  BY DEF Inv
<1>1. m.acc \in Acceptors /\ prom[m.acc] \in Nat
  <2>1. m.acc \in Acceptors
    BY <1>0, MsgFields
  <2> QED
    BY <1>0, <2>1 DEF TypeOK
<1>2. m.acc \notin wiped => b <= prom[m.acc]
  BY DEF Inv, MsgInv, MsgOK
<1> QED
  BY <1>1, <1>2 DEF WontVoteIn

\* The Phase-1 facts of a win: the promises S at ballot b, from `fr`, cover
\* a Phase-1 quorum.
WinPromises(b, S, fr) ==
  /\ S \subseteq {m \in msgs : m.type = "1b" /\ m.bal = b}
  /\ \A m \in S : m.from = fr
  /\ \E Q \in Q1 : Q \subseteq {m.acc : m \in S}

\* A slot that no promise of S reports is free at b.
LEMMA RecoveryFree ==
  ASSUME Inv, Hyp, NEW b \in Ballots, NEW S, NEW fr, WinPromises(b, S, fr),
         NEW s \in Slots, s >= fr, \A m \in S : m.rep[s].bal = 0
  PROVE  FreeAt(b, s)
<1> SUFFICES ASSUME NEW c \in 1..(b - 1) PROVE \E Q \in Q1 : \A a \in Q : WontVoteIn(a, c, s)
  BY DEF FreeAt
<1>1. PICK Q \in Q1 : Q \subseteq {m.acc : m \in S}
  BY DEF WinPromises
<1>2. \A a \in Q : WontVoteIn(a, c, s)
  <2> SUFFICES ASSUME NEW a \in Q PROVE WontVoteIn(a, c, s)
    OBVIOUS
  <2>1. PICK m \in S : m.acc = a
    BY <1>1
  <2>2. m \in msgs /\ m.type = "1b" /\ m.bal = b /\ m.from = fr
    BY <2>1 DEF WinPromises
  <2>0. c \in Ballots /\ c < b
    OBVIOUS
  <2>3. \A w \in Values : ~VotedFor(m.acc, c, s, w)
    <3> SUFFICES ASSUME NEW w \in Values, VotedFor(m.acc, c, s, w) PROVE FALSE
      OBVIOUS
    <3>1. c <= m.rep[s].bal
      BY <2>0, <2>2, ReportBound
    <3>2. m.rep[s].bal = 0
      BY <2>1
    <3> QED
      BY <3>1, <3>2, <2>0
  <2>4. WontVoteIn(m.acc, c, s)
    BY <2>0, <2>2, <2>3, ReportWont
  <2> QED
    BY <2>1, <2>4
<1> QED
  BY <1>1, <1>2

\* The highest report of S at a slot is safe at b.
LEMMA RecoverySafe ==
  ASSUME Inv, Hyp, NEW b \in Ballots, NEW S, NEW fr, WinPromises(b, S, fr),
         NEW s \in Slots, s >= fr, NEW mt \in S, mt.rep[s].bal > 0,
         \A m \in S : m.rep[s].bal <= mt.rep[s].bal
  PROVE  SafeAt(b, s, mt.rep[s].val)
<1> DEFINE k == mt.rep[s].bal
           v == mt.rep[s].val
<1>0. mt \in msgs /\ mt.type = "1b" /\ mt.bal = b /\ mt.from = fr
  BY DEF WinPromises
<1>1. RecordOK(mt.acc, s, mt.rep[s], b) /\ mt.rep[s] \in Vote
  BY <1>0, MsgFields DEF Inv, MsgInv, MsgOK
<1>2. Proposed(k, s, v) /\ k \in Ballots /\ k < b /\ v \in Values
  <2>1. k \in 0..NB /\ v \in Values \cup {None} /\ k > 0
    BY <1>1 DEF Vote
  <2>2. (k = 0) = (v = None) /\ k < b /\ (k > 0 => Proposed(k, s, v))
    BY <1>1 DEF RecordOK
  <2> QED
    BY <2>1, <2>2
<1> SUFFICES ASSUME NEW c \in 1..(b - 1)
             PROVE  \E Q \in Q1 : \A a \in Q : VotedFor(a, c, s, v) \/ WontVoteIn(a, c, s)
  BY DEF SafeAt
<1>3. CASE c < k
  <2>1. SafeAt(k, s, v)
    BY <1>2, ProposedSafe
  <2>2. c \in 1..(k - 1)
    BY <1>2, <1>3
  <2> QED
    BY <2>1, <2>2 DEF SafeAt
<1>4. CASE c >= k
  <2>1. PICK Q \in Q1 : Q \subseteq {m.acc : m \in S}
    BY DEF WinPromises
  <2>2. \A a \in Q : VotedFor(a, c, s, v) \/ WontVoteIn(a, c, s)
    <3> SUFFICES ASSUME NEW a \in Q, ~VotedFor(a, c, s, v) PROVE WontVoteIn(a, c, s)
      OBVIOUS
    <3>1. PICK m \in S : m.acc = a
      BY <2>1
    <3>2. m \in msgs /\ m.type = "1b" /\ m.bal = b /\ m.from = fr /\ m.rep[s].bal \in Nat
      BY <3>1, MsgFields DEF WinPromises, Inv, Vote
    <3>3. \A w \in Values : ~VotedFor(a, c, s, w)
      <4> SUFFICES ASSUME NEW w \in Values, VotedFor(a, c, s, w) PROVE FALSE
        OBVIOUS
      <4>1. c <= m.rep[s].bal /\ (c = m.rep[s].bal => w = m.rep[s].val)
        <5>1. c \in Ballots /\ c < b /\ VotedFor(m.acc, c, s, w)
          BY <3>1
        <5> QED
          BY <5>1, <3>2, ReportBound
      <4>0. m.rep[s].bal <= k /\ c \in Ballots /\ c < b
        BY <3>1
      <4>2. c = k
        BY <4>0, <4>1, <3>2, <1>4, <1>2
      <4>3. Proposed(k, s, w)
        BY <4>2, VotedProposed
      <4> QED
        BY <4>2, <4>3, <1>2, OneValue
    <3>4. c \in Ballots /\ c < b /\ \A w \in Values : ~VotedFor(m.acc, c, s, w)
      BY <3>1, <3>3
    <3> QED
      BY <3>1, <3>2, <3>4, ReportWont
  <2> QED
    BY <2>1, <2>2
<1> QED
  BY <1>2, <1>3, <1>4

\* The form of a recovery message.
LEMMA RecoveryForm ==
  ASSUME NEW S, NEW b, NEW fr, NEW f, NEW n \in Recovery(S, b, fr, f)
  PROVE  /\ n = [type |-> "2a", bal |-> b, slot |-> n.slot, val |-> n.val]
         /\ n.slot \in fr..(f - 1)
         /\ \/ Top(S, n.slot).bal > 0 /\ n.val = Top(S, n.slot).val
            \/ Top(S, n.slot).bal = 0 /\ n.val = Noop
BY DEF Recovery

THEOREM WinInv ==
  ASSUME Inv, Hyp, NEW b \in Ballots, Win(b)
  PROVE  Inv'
<1>0. TypeOK /\ BallotOK(b)
  BY DEF Inv, BallotInv
<1>1. /\ phase[b] = "p1"
      /\ phase' = [phase EXCEPT ![b] = "p2"]
      /\ UNCHANGED <<prom, vote, dprom, dvote, learned, wiped, restarts>>
  BY DEF Win
<1> DEFINE fr == (CHOOSE m \in msgs : m.type = "1a" /\ m.bal = b).from
<1>2. PICK S \in SUBSET {m \in msgs : m.type = "1b" /\ m.bal = b} :
        /\ \E Q \in Q1 : Q \subseteq {m.acc : m \in S}
        /\ front' = [front EXCEPT ![b] = Frontier(S, fr)]
        /\ next' = [next EXCEPT ![b] = Frontier(S, fr)]
        /\ msgs' = msgs \cup Recovery(S, b, fr, Frontier(S, fr))
  BY DEF Win, SendAll, Hyp
<1> DEFINE f == Frontier(S, fr)
           R == Recovery(S, b, fr, f)
<1>3. fr \in 0..NS /\ \A m \in S : m.from = fr
  <2>1. \E m \in msgs : m.type = "1a" /\ m.bal = b
    BY <1>0, <1>1 DEF BallotOK
  <2> DEFINE m1a == CHOOSE m \in msgs : m.type = "1a" /\ m.bal = b
  <2>2. m1a \in msgs /\ m1a.type = "1a" /\ m1a.bal = b /\ fr = m1a.from
    BY <2>1
  <2> HIDE DEF m1a
  <2>3. fr \in 0..NS
    BY <2>2, <1>0, MsgFields
  <2>4. \A m \in S : m.from = fr
    <3> SUFFICES ASSUME NEW m \in S PROVE m.from = fr
      OBVIOUS
    <3>0. m \in msgs /\ m.type = "1b" /\ m.bal = b
      BY <1>2
    <3>1. [type |-> "1a", bal |-> b, from |-> m.from] \in msgs
      <4>1. MsgOK(m)
        BY <3>0 DEF Inv, MsgInv
      <4> QED
        BY <3>0, <4>1 DEF MsgOK
    <3>2. \A n \in msgs : n.type = "1a" /\ n.bal = b => n = m1a
      <4>1. MsgOK(m1a)
        BY <2>2 DEF Inv, MsgInv
      <4> QED
        BY <2>2, <4>1 DEF MsgOK
    <3>2b. [type |-> "1a", bal |-> b, from |-> m.from].type = "1a"
           /\ [type |-> "1a", bal |-> b, from |-> m.from].bal = b
           /\ [type |-> "1a", bal |-> b, from |-> m.from].from = m.from
      OBVIOUS
    <3>3. [type |-> "1a", bal |-> b, from |-> m.from] = m1a
      BY <3>1, <3>2, <3>2b
    <3> QED
      BY <3>3, <3>2b, <2>2
  <2> QED
    BY <2>3, <2>4
<1>4. WinPromises(b, S, fr)
  BY <1>2, <1>3 DEF WinPromises
<1>5. f \in fr..NS /\ \A s \in Slots : s >= f => \A m \in S : m.rep[s].bal = 0
  BY <1>3, FrontierFacts DEF Inv
<1>6. \A n \in R : /\ n = [type |-> "2a", bal |-> b, slot |-> n.slot, val |-> n.val]
                   /\ n.slot \in Slots /\ n.slot >= fr /\ n.slot < f
                   /\ \/ Top(S, n.slot).bal > 0 /\ n.val = Top(S, n.slot).val
                      \/ Top(S, n.slot).bal = 0 /\ n.val = Noop
  BY <1>3, <1>5, RecoveryForm
<1>6b. \A n \in R : n.type = "2a" /\ n.bal = b /\ n.slot \in Slots
  BY <1>6
<1>7. msgs' = msgs \cup R /\ msgs \subseteq msgs'
  BY <1>2
<1> HIDE DEF f, fr
<1>8. \A x, c, s, v : VotedFor(x, c, s, v)' = VotedFor(x, c, s, v)
  BY <1>6, <1>7 DEF VotedFor
<1>9. KeepsVW
  <2>1. \A x \in Acceptors, c \in Ballots, s \in Slots :
          WontVoteIn(x, c, s) => \A v \in Values : ~VotedFor(x, c, s, v)'
    BY <1>8 DEF WontVoteIn
  <2>2. \A x \in Acceptors : prom'[x] \in Nat /\ prom'[x] >= prom[x]
    BY <1>0, <1>1 DEF TypeOK
  <2> QED
    BY <1>0, <1>1, <1>7, <2>1, <2>2, KeepsVWFrom
<1>10. \A n \in msgs : n.type = "2a" => n.bal # b
  <2> SUFFICES ASSUME NEW n \in msgs, n.type = "2a", n.bal = b PROVE FALSE
    OBVIOUS
  <2>1. n = [type |-> "2a", bal |-> b, slot |-> n.slot, val |-> n.val]
        /\ n.slot \in Slots /\ n.val \in Values
    BY <1>0, MsgFields
  <2>2. Proposed(b, n.slot, n.val)
    BY <2>1 DEF Proposed
  <2> QED
    BY <2>1, <2>2, <1>0, <1>1 DEF BallotOK
<1>11. \A s \in Slots : s >= fr /\ Top(S, s).bal > 0 => SafeAt(b, s, Top(S, s).val)
  <2> SUFFICES ASSUME NEW s \in Slots, s >= fr, Top(S, s).bal > 0 PROVE SafeAt(b, s, Top(S, s).val)
    OBVIOUS
  <2>1. \A m \in S : m.rep[s].bal <= Top(S, s).bal
    BY <1>2, TopFacts
  <2>2. PICK mt \in S : mt.rep[s] = Top(S, s)
    BY <1>2, TopFacts
  <2>3. \A m \in S : m.rep[s].bal <= mt.rep[s].bal
    BY <2>1, <2>2
  <2> QED
    BY <1>4, <2>2, <2>3, RecoverySafe
<1>12. \A s \in Slots : s >= fr /\ Top(S, s).bal = 0 => FreeAt(b, s)
  BY <1>2, <1>4, TopFacts, RecoveryFree
<1>13. \A n \in R : n \in Msg /\ SafeAt(b, n.slot, n.val)
  <2> SUFFICES ASSUME NEW n \in R PROVE n \in Msg /\ SafeAt(b, n.slot, n.val)
    OBVIOUS
  <2>1. Top(S, n.slot) \in Vote
    BY <1>2, <1>6, TopFacts
  <2>2. CASE Top(S, n.slot).bal > 0 /\ n.val = Top(S, n.slot).val
    <3>1. n.val \in Values
      <4>1. PICK mt \in S : mt.rep[n.slot] = Top(S, n.slot)
        BY <2>2, <1>2, <1>6, TopFacts
      <4>2. RecordOK(mt.acc, n.slot, mt.rep[n.slot], b)
        <5>1. mt \in msgs /\ mt.type = "1b" /\ mt.bal = b /\ mt.from = fr
              /\ n.slot >= fr /\ n.slot \in Slots
          BY <1>2, <1>3, <1>6
        <5>2. MsgOK(mt)
          BY <5>1 DEF Inv, MsgInv
        <5> QED
          BY <5>1, <5>2 DEF MsgOK
      <4> QED
        BY <4>1, <4>2, <2>1, <2>2 DEF RecordOK, Vote
    <3> QED
      BY <3>1, <2>2, <1>6, <1>11 DEF Msg
  <2>3. CASE Top(S, n.slot).bal = 0 /\ n.val = Noop
    BY <2>3, <1>6, <1>12, FreeSafe DEF Msg
  <2> QED
    BY <2>2, <2>3, <1>6
<1>14. TypeOK'
  <2>0. f \in 0..NS
    BY <1>3, <1>5
  <2>1. front' \in [Ballots -> 0..NS] /\ next' \in [Ballots -> 0..NS]
    BY <1>0, <1>2, <2>0 DEF TypeOK, f
  <2>2. phase' \in [Ballots -> {"idle", "p1", "p2", "dead"}]
    BY <1>0, <1>1 DEF TypeOK
  <2> QED
    BY <1>0, <1>1, <1>7, <1>13, <2>1, <2>2 DEF TypeOK
<1>15. NodeInv'
  <2> SUFFICES ASSUME NEW x \in Acceptors PROVE NodeOK(x)'
    BY DEF NodeInv
  <2>1. IF x \in wiped THEN prom'[x] = prom[x] ELSE prom'[x] \in Nat /\ prom'[x] >= prom[x]
    BY <1>0, <1>1 DEF TypeOK
  <2> QED
    BY <1>1, <1>7, <1>8, <2>1, NodeKeep
<1>16. BallotInv'
  <2> SUFFICES ASSUME NEW c \in Ballots PROVE BallotOK(c)'
    BY DEF BallotInv
  <2>1. CASE c = b
    <3>1. phase'[b] = "p2" /\ front'[b] = f /\ next'[b] = f /\ f \in Nat
      BY <1>0, <1>2, <1>1, <1>3, <1>5 DEF TypeOK, f
    <3>2. \A s \in Slots, v \in Values : Proposed(b, s, v)' => s < f
      <4> SUFFICES ASSUME NEW s \in Slots, NEW v \in Values, Proposed(b, s, v)' PROVE s < f
        OBVIOUS
      <4>1. [type |-> "2a", bal |-> b, slot |-> s, val |-> v] \in R
        BY <1>7, <1>10 DEF Proposed
      <4> QED
        BY <4>1, <1>6
    <3>3. \A s \in Slots : s >= f => FreeAt(b, s)'
      <4> SUFFICES ASSUME NEW s \in Slots, s >= f PROVE FreeAt(b, s)'
        OBVIOUS
      <4>0. s >= fr /\ \A m \in S : m.rep[s].bal = 0
        BY <1>3, <1>5
      <4>1. FreeAt(b, s)
        BY <4>0, <1>4, RecoveryFree
      <4> QED
        BY <4>1, <1>9, FreeAtKeep
    <3>4. \E m \in msgs' : m.type = "1a" /\ m.bal = b
      BY <1>0, <1>1, <1>7 DEF BallotOK
    <3>5. Owner[b] \notin wiped' /\ prom'[Owner[b]] >= b
      BY <1>0, <1>1 DEF BallotOK
    <3> QED
      BY <2>1, <3>1, <3>2, <3>3, <3>4, <3>5 DEF BallotOK
  <2>2. CASE c # b
    <3>1. \A n \in msgs' : n.type \in {"1a", "2a"} /\ n.bal = c => n \in msgs
      BY <2>2, <1>6b, <1>7
    <3>2. phase[c] \in {"p1", "p2"} => Owner[c] \notin wiped'
      BY <1>1 DEF Inv, BallotInv, BallotOK
    <3>3. phase'[c] = phase[c] /\ next'[c] = next[c] /\ front'[c] = front[c]
      BY <2>2, <1>0, <1>1, <1>2 DEF TypeOK, f
    <3>4. prom'[Owner[c]] \in Nat /\ prom'[Owner[c]] >= prom[Owner[c]]
      BY <1>0, <1>1 DEF TypeOK
    <3> QED
      BY <1>1, <1>7, <1>9, <3>1, <3>2, <3>3, <3>4, BallotKeep
  <2> QED
    BY <2>1, <2>2
<1>17. MsgInv'
  <2> SUFFICES ASSUME NEW m \in msgs' PROVE MsgOK(m)'
    BY DEF MsgInv
  <2>1. CASE m \in msgs
    <3>1. m.type = "2a" => \A n \in msgs' : n.type = "2a" /\ n.bal = m.bal /\ n.slot = m.slot => n \in msgs
      BY <2>1, <1>6b, <1>7, <1>10
    <3>2. \A x \in Acceptors : x \notin wiped' => /\ x \notin wiped
                                                 /\ prom'[x] \in Nat
                                                 /\ prom'[x] >= prom[x]
      BY <1>0, <1>1 DEF TypeOK
    <3>3. m.type = "1a" => \A n \in msgs' : n.type = "1a" /\ n.bal = m.bal => n \in msgs
      BY <1>6b, <1>7
    <3>4. m.type = "1b" => /\ \A n \in msgs' : n.type = "1b" /\ n.bal = m.bal /\ n.acc = m.acc => n \in msgs
                           /\ \A s \in Slots, d \in Ballots, v \in Values :
                                VotedFor(m.acc, d, s, v)' /\ d < m.bal => VotedFor(m.acc, d, s, v)
      BY <1>6b, <1>7, <1>8
    <3> QED
      BY <2>1, <3>1, <3>2, <3>3, <3>4, <1>7, <1>9, OldMsg
  <2>2. CASE m \in R
    <3>0. /\ m \in Msg /\ m.type = "2a" /\ m.bal = b /\ m.slot \in Slots
          /\ SafeAt(b, m.slot, m.val)
      BY <2>2, <1>6b, <1>13
    <3>0b. m.val \in Values
      BY <3>0 DEF Msg
    <3>1. SafeAt(b, m.slot, m.val)'
      BY <3>0, <3>0b, <1>9, SafeAtKeep
    <3>2. \A n \in msgs' : n.type = "2a" /\ n.bal = b /\ n.slot = m.slot => n = m
      <4> SUFFICES ASSUME NEW n \in msgs', n.type = "2a", n.bal = b, n.slot = m.slot PROVE n = m
        OBVIOUS
      <4>1. n \in R
        BY <1>7, <1>10
      <4>2. n.val = m.val
        <5>0. /\ \/ Top(S, m.slot).bal > 0 /\ m.val = Top(S, m.slot).val
                 \/ Top(S, m.slot).bal = 0 /\ m.val = Noop
              /\ \/ Top(S, m.slot).bal > 0 /\ n.val = Top(S, m.slot).val
                 \/ Top(S, m.slot).bal = 0 /\ n.val = Noop
          BY <4>1, <2>2, <1>6
        <5> QED
          BY <5>0
      <4> QED
        BY <4>1, <4>2, <2>2, <1>6
    <3> QED
      BY <2>2, <1>6, <3>1, <3>2 DEF MsgOK
  <2> QED
    BY <2>1, <2>2, <1>7
<1> QED
  BY <1>1, <1>14, <1>15, <1>16, <1>17 DEF Inv

=============================================================================
