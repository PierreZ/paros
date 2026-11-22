------------------------ MODULE MultiPaxosLearnProof ------------------------
(***************************************************************************)
(* Learning and faults keep `Inv`: `Restart`, `Wipe`, `Decide` and        *)
(* `CatchUp`; `Persist` is a stutter step (#1307, step 2).                 *)
(***************************************************************************)
EXTENDS MultiPaxosFactsProof

USE NBType, NSType, NoneNotValue, NoopNotCommand, OwnerType, LearnersType
    DEF Ballots, Slots, Values, NoVote

\* With the disk equal to memory (`Inv`), `Persist` writes what the disk holds:
\* a stutter step.
THEOREM PersistStutter ==
  ASSUME Inv, NEW a \in Acceptors, Persist(a)
  PROVE  UNCHANGED vars
<1>1. TypeOK /\ dprom = prom /\ dvote = vote
  BY DEF Inv
<1>2. dprom' = dprom /\ dvote' = dvote
  BY <1>1 DEF Persist, TypeOK
<1> QED
  BY <1>2 DEF Persist, vars

THEOREM RestartInv ==
  ASSUME Inv, Hyp, NEW a \in Acceptors, Restart(a)
  PROVE  Inv'
<1>0. TypeOK /\ dprom = prom /\ dvote = vote
  BY DEF Inv
<1>1. /\ restarts < Restarts /\ restarts' = restarts + 1
      /\ prom' = prom /\ vote' = vote
      /\ phase' = [c \in Ballots |-> IF Owner[c] = a /\ phase[c] \in {"p1", "p2"} THEN "dead" ELSE phase[c]]
      /\ UNCHANGED <<dprom, dvote, next, front, wiped, msgs>>
  <2>1. prom' = [prom EXCEPT ![a] = prom[a]] /\ vote' = [vote EXCEPT ![a] = vote[a]]
    BY <1>0 DEF Restart
  <2> QED
    BY <2>1, <1>0 DEF Restart, TypeOK
<1>2. PICK k \in 0..FirstUnlearned(a) :
        learned' = [learned EXCEPT ![a] = [s \in Slots |-> IF s < k THEN @[s] ELSE None]]
  BY DEF Restart
<1>3. \A x \in Acceptors, s \in Slots : learned'[x][s] = learned[x][s] \/ learned'[x][s] = None
  BY <1>0, <1>2 DEF TypeOK
<1>4. KeepsVW
  BY <1>0, <1>1 DEF KeepsVW, WontVoteIn, VotedFor
<1>5. TypeOK'
  <2>1. learned' \in [Acceptors -> [Slots -> Values \cup {None}]]
    BY <1>0, <1>2 DEF TypeOK
  <2>2. phase' \in [Ballots -> {"idle", "p1", "p2", "dead"}]
    BY <1>0, <1>1 DEF TypeOK
  <2>3. restarts' \in Nat
    BY <1>0, <1>1 DEF TypeOK
  <2> QED
    BY <1>0, <1>1, <2>1, <2>2, <2>3 DEF TypeOK
<1>6. NodeInv'
  <2> SUFFICES ASSUME NEW x \in Acceptors PROVE NodeOK(x)'
    BY DEF NodeInv
  <2>1. IF x \in wiped THEN prom'[x] = prom[x] ELSE prom'[x] \in Nat /\ prom'[x] >= prom[x]
    BY <1>0, <1>1 DEF TypeOK
  <2> QED
    BY <1>1, <1>3, <2>1, NodeKeep DEF VotedFor
<1>7. BallotInv'
  <2> SUFFICES ASSUME NEW c \in Ballots PROVE BallotOK(c)'
    BY DEF BallotInv
  <2>0. prom'[Owner[c]] \in Nat /\ prom'[Owner[c]] >= prom[Owner[c]]
    BY <1>0, <1>1 DEF TypeOK
  <2>1. CASE Owner[c] = a /\ phase[c] \in {"p1", "p2"}
    <3>1. phase'[c] = "dead"
      BY <2>1, <1>1
    <3> QED
      BY <2>0, <2>1, <3>1, <1>1, BallotKill
  <2>2. CASE ~(Owner[c] = a /\ phase[c] \in {"p1", "p2"})
    <3>1. phase'[c] = phase[c]
      BY <2>2, <1>1
    <3>2. phase[c] \in {"p1", "p2"} => Owner[c] \notin wiped'
      BY <1>1 DEF Inv, BallotInv, BallotOK
    <3> QED
      BY <2>0, <3>1, <3>2, <1>1, <1>4, BallotKeep
  <2> QED
    BY <2>1, <2>2
<1>8. MsgInv'
  <2> SUFFICES ASSUME NEW m \in msgs' PROVE MsgOK(m)'
    BY DEF MsgInv
  <2>1. \A x \in Acceptors : x \notin wiped' => /\ x \notin wiped
                                               /\ prom'[x] \in Nat
                                               /\ prom'[x] >= prom[x]
    BY <1>0, <1>1 DEF TypeOK
  <2> QED
    BY <1>1, <1>4, <2>1, OldMsg DEF VotedFor
<1>9. /\ restarts' <= Restarts /\ Cardinality(wiped') <= Wipes
      /\ dprom' = prom' /\ dvote' = vote'
  BY <1>0, <1>1, RestartsType DEF Inv, TypeOK
<1> QED
  BY <1>5, <1>6, <1>7, <1>8, <1>9 DEF Inv

THEOREM WipeInv ==
  ASSUME Inv, Hyp, NEW a \in Acceptors, Wipe(a)
  PROVE  Inv'
<1>0. TypeOK
  BY DEF Inv
<1>1. /\ a \notin wiped /\ Cardinality(wiped) < Wipes
      /\ wiped' = wiped \cup {a}
      /\ prom' = [prom EXCEPT ![a] = 0]
      /\ dprom' = [dprom EXCEPT ![a] = 0]
      /\ vote' = [vote EXCEPT ![a] = [s \in Slots |-> NoVote]]
      /\ dvote' = [dvote EXCEPT ![a] = [s \in Slots |-> NoVote]]
      /\ learned' = [learned EXCEPT ![a] = [s \in Slots |-> None]]
      /\ phase' = [c \in Ballots |-> IF Owner[c] = a /\ phase[c] \in {"p1", "p2"} THEN "dead" ELSE phase[c]]
      /\ UNCHANGED <<next, front, restarts, msgs>>
  BY DEF Wipe, Live, Hyp, InvApplies
<1>2. \A x \in Acceptors : x # a => prom'[x] = prom[x] /\ vote'[x] = vote[x] /\ learned'[x] = learned[x]
  BY <1>0, <1>1 DEF TypeOK
<1>3. \A x \in Acceptors : x \in wiped' \/ (prom'[x] \in Nat /\ prom'[x] >= prom[x])
  BY <1>0, <1>1, <1>2 DEF TypeOK
<1>4. KeepsVW
  <2>1. \A x \in Acceptors, c \in Ballots, s \in Slots :
          WontVoteIn(x, c, s) => \A v \in Values : ~VotedFor(x, c, s, v)'
    BY <1>1 DEF WontVoteIn, VotedFor
  <2>2. wiped \subseteq wiped' /\ msgs \subseteq msgs'
    BY <1>1
  <2> QED
    BY <1>0, <1>3, <2>1, <2>2, KeepsVWFrom
<1>5. TypeOK'
  <2>1. NoVote \in Vote
    BY DEF Vote
  <2> QED
    BY <1>0, <1>1, <2>1 DEF TypeOK
<1>6. Cardinality(wiped') <= Wipes
  <2>1. IsFiniteSet(wiped) /\ Cardinality(wiped) \in Nat
    BY <1>0, WipedFinite
  <2>2. Cardinality(wiped') = Cardinality(wiped) + 1
    BY <1>1, <2>1, FS_AddElement
  <2> QED
    BY <1>1, <2>1, <2>2, WipesType
<1>7. NodeInv'
  <2> SUFFICES ASSUME NEW x \in Acceptors PROVE NodeOK(x)'
    BY DEF NodeInv
  <2>1. CASE x = a
    BY <2>1, <1>0, <1>1 DEF NodeOK, TypeOK
  <2>2. CASE x # a
    <3>0. prom'[x] = prom[x] /\ prom[x] \in Nat
      BY <2>2, <1>0, <1>2 DEF TypeOK
    <3>1. IF x \in wiped THEN prom'[x] = prom[x] ELSE prom'[x] \in Nat /\ prom'[x] >= prom[x]
      BY <3>0
    <3>2. x \in wiped' <=> x \in wiped
      BY <2>2, <1>1
    <3> QED
      BY <2>2, <1>1, <1>2, <3>1, <3>2, NodeKeep DEF VotedFor
  <2> QED
    BY <2>1, <2>2
<1>8. BallotInv'
  <2> SUFFICES ASSUME NEW c \in Ballots PROVE BallotOK(c)'
    BY DEF BallotInv
  <2>0. Owner[c] \in wiped' \/ (prom'[Owner[c]] \in Nat /\ prom'[Owner[c]] >= prom[Owner[c]])
    BY <1>3
  <2>1. CASE Owner[c] = a /\ phase[c] \in {"p1", "p2"}
    <3>1. phase'[c] = "dead"
      BY <2>1, <1>1
    <3> QED
      BY <2>0, <2>1, <3>1, <1>1, BallotKill
  <2>2. CASE ~(Owner[c] = a /\ phase[c] \in {"p1", "p2"})
    <3>1. phase'[c] = phase[c]
      BY <2>2, <1>1
    <3>2. phase[c] \in {"p1", "p2"} => Owner[c] \notin wiped'
      BY <2>2, <1>1 DEF Inv, BallotInv, BallotOK
    <3> QED
      BY <2>0, <3>1, <3>2, <1>1, <1>4, BallotKeep
  <2> QED
    BY <2>1, <2>2
<1>9. MsgInv'
  <2> SUFFICES ASSUME NEW m \in msgs' PROVE MsgOK(m)'
    BY DEF MsgInv
  <2>1. \A x \in Acceptors : x \notin wiped' => /\ x \notin wiped
                                               /\ prom'[x] \in Nat
                                               /\ prom'[x] >= prom[x]
    BY <1>1, <1>3
  <2> QED
    BY <1>1, <1>4, <2>1, OldMsg DEF VotedFor
<1> QED
  BY <1>0, <1>1, <1>5, <1>6, <1>7, <1>8, <1>9 DEF Inv


\* A learn of chosen records keeps `Inv`: the shared step of `Decide` and
\* `CatchUp`.
LEMMA LearnInv ==
  ASSUME Inv, Hyp, NEW n \in Acceptors, n \notin wiped,
         n \in Learners \cup {Owner[c] : c \in Ballots},
         NEW L \in SUBSET Slots, L # {},
         NEW rec \in [L -> [bal : Ballots, val : Values]],
         \A s \in L : ChosenBy(s, rec[s].bal, rec[s].val) /\ Proposed(rec[s].bal, s, rec[s].val),
         LearnAll(n, rec),
         UNCHANGED <<next, front, wiped, restarts, msgs>>
  PROVE  Inv'
<1> DEFINE top == CHOOSE x \in {rec[s].bal : s \in DOMAIN rec} : \A s \in DOMAIN rec : rec[s].bal <= x
           p == IF top > prom[n] THEN top ELSE prom[n]
           Keeps(s) == vote[n][s].val = rec[s].val /\ vote[n][s].bal >= rec[s].bal
           nv == [s \in Slots |-> IF s \in DOMAIN rec /\ ~Keeps(s) THEN rec[s] ELSE vote[n][s]]
<1>0. TypeOK /\ DOMAIN rec = L
  BY DEF Inv
<1>1. /\ learned' = [learned EXCEPT ![n] = [s \in Slots |-> IF s \in DOMAIN rec THEN rec[s].val ELSE @[s]]]
      /\ prom' = [prom EXCEPT ![n] = p]
      /\ vote' = [vote EXCEPT ![n] = nv]
      /\ phase' = [c \in Ballots |-> IF Owner[c] = n /\ phase[c] = "p1" /\ c < p
                                     THEN "dead" ELSE phase[c]]
      /\ dprom' = [dprom EXCEPT ![n] = p]
  BY PersistedNew DEF LearnAll
<1>2. top \in Ballots /\ \A s \in L : rec[s].bal <= top
  <2>1. {rec[s].bal : s \in L} \in SUBSET (0..NB) /\ {rec[s].bal : s \in L} # {}
    OBVIOUS
  <2>2. \E k \in {rec[s].bal : s \in L} : \A j \in {rec[s].bal : s \in L} : j <= k
    BY <2>1, MaxExists
  <2> QED
    BY <2>2, <1>0
<1>3. prom[n] \in 0..NB /\ p \in 0..NB /\ p >= prom[n] /\ p >= top
  <2>1. prom[n] \in 0..NB
    BY <1>0 DEF TypeOK
  <2> QED
    BY <2>1, <1>2
<1> HIDE DEF top
<1>4. \A x \in Acceptors : prom'[x] \in Nat /\ prom'[x] >= prom[x]
  BY <1>0, <1>1, <1>3 DEF TypeOK
<1>5. \A x, c, s, v : VotedFor(x, c, s, v)' = VotedFor(x, c, s, v)
  BY DEF VotedFor
<1>5a. msgs' = msgs /\ msgs \subseteq msgs' /\ wiped' = wiped
  OBVIOUS
<1>6. KeepsVW
  <2>1. \A x \in Acceptors, c \in Ballots, s \in Slots :
          WontVoteIn(x, c, s) => \A v \in Values : ~VotedFor(x, c, s, v)'
    BY <1>5 DEF WontVoteIn
  <2> QED
    BY <1>0, <1>4, <2>1, KeepsVWFrom
<1>7. TypeOK'
  <2>1. \A s \in L : rec[s] \in Vote
    BY DEF Vote
  <2>2. nv \in [Slots -> Vote]
    BY <2>1, <1>0 DEF TypeOK
  <2>3. prom' \in [Acceptors -> 0..NB] /\ dprom' \in [Acceptors -> 0..NB]
    BY <1>0, <1>1, <1>3 DEF TypeOK, Inv
  <2>4. dvote' \in [Acceptors -> [Slots -> Vote]]
    <3>1. dvote' = [dvote EXCEPT ![n] = [s \in Slots |-> IF s \in DOMAIN rec /\ ~Keeps(s)
                                                       THEN rec[s] ELSE dvote[n][s]]]
      BY PersistedNew DEF LearnAll
    <3>2. dvote = vote
      BY DEF Inv
    <3> QED
      BY <3>1, <3>2, <2>1, <1>0 DEF TypeOK
  <2>5. learned' \in [Acceptors -> [Slots -> Values \cup {None}]]
    BY <1>0, <1>1 DEF TypeOK
  <2>6. phase' \in [Ballots -> {"idle", "p1", "p2", "dead"}]
    BY <1>0, <1>1 DEF TypeOK
  <2>7. vote' \in [Acceptors -> [Slots -> Vote]]
    BY <1>0, <1>1, <2>2 DEF TypeOK
  <2> QED
    BY <1>0, <2>3, <2>4, <2>5, <2>6, <2>7 DEF TypeOK
<1>8. dprom' = prom' /\ dvote' = vote'
  <2>1. dvote' = [dvote EXCEPT ![n] = [s \in Slots |-> IF s \in DOMAIN rec /\ ~Keeps(s)
                                                     THEN rec[s] ELSE dvote[n][s]]]
    BY PersistedNew DEF LearnAll
  <2>2. dvote = vote /\ dprom = prom
    BY DEF Inv
  <2> QED
    BY <1>1, <2>1, <2>2, <1>0 DEF TypeOK
<1>9. NodeInv'
  <2> SUFFICES ASSUME NEW x \in Acceptors PROVE NodeOK(x)'
    BY DEF NodeInv
  <2>1. CASE x # n
    <3>1. IF x \in wiped THEN prom'[x] = prom[x] ELSE prom'[x] \in Nat /\ prom'[x] >= prom[x]
      BY <2>1, <1>0, <1>1, <1>4 DEF TypeOK
    <3>2. vote'[x] = vote[x] /\ (\A s \in Slots : learned'[x][s] = learned[x][s])
          /\ (x \in wiped' <=> x \in wiped)
      BY <2>1, <1>0, <1>1 DEF TypeOK
    <3> QED
      BY <3>1, <3>2, <1>5, NodeKeep
  <2>2. CASE x = n
    <3> SUFFICES ASSUME NEW s \in Slots PROVE NodeSlotOK(n, s)'
      BY <2>2 DEF NodeOK
    <3>0. prom'[n] = p
      BY <1>0, <1>1 DEF TypeOK
    <3>1. CASE s \notin L
      <4>1. vote'[n][s] = vote[n][s] /\ learned'[n][s] = learned[n][s]
        BY <3>1, <1>0, <1>1 DEF TypeOK
      <4>2. prom'[n] \in Nat /\ prom'[n] >= prom[n]
        BY <3>0, <1>3
      <4>3. \A c \in Ballots, w \in Values : VotedFor(n, c, s, w)' = VotedFor(n, c, s, w)
        BY <1>5
      <4> QED
        BY <4>1, <4>2, <4>3, <1>5a, NodeSlotKeep
    <3>2. CASE s \in L
      <4>0. NodeSlotOK(n, s) /\ vote[n][s] \in Vote
        <5>1. NodeOK(n)
          BY DEF Inv, NodeInv
        <5> QED
          BY <5>1, <1>0 DEF NodeOK, TypeOK
      <4>1. learned'[n][s] = rec[s].val /\ vote'[n][s] = IF Keeps(s) THEN vote[n][s] ELSE rec[s]
        BY <3>2, <1>0, <1>1 DEF TypeOK
      <4>2. ChosenBy(s, rec[s].bal, rec[s].val) /\ Proposed(rec[s].bal, s, rec[s].val)
            /\ rec[s].bal \in Ballots /\ rec[s].val \in Values
        BY <3>2
      <4>3. rec[s].bal <= p
        <5>0. rec[s] \in [bal : Ballots, val : Values]
          BY <3>2
        <5>1. rec[s].bal <= top /\ rec[s].bal \in Nat
          BY <5>0, <3>2, <1>2
        <5> QED
          BY <5>1, <1>2, <1>3
      <4>4. \A c \in Ballots : rec[s].bal <= c => ChosenBy(s, c, rec[s].val)
        BY <4>2 DEF ChosenBy
      <4>5. (learned[n][s] # None => n \in Learners \cup {Owner[c] : c \in Ballots})'
        OBVIOUS
      <4>6. CASE Keeps(s)
        <5>1. vote'[n][s] = vote[n][s] /\ vote[n][s].bal \in Ballots
          BY <4>6, <4>1, <4>0, <4>2 DEF Vote
        <5>2. ChosenBy(s, vote[n][s].bal, rec[s].val)
          BY <5>1, <4>6, <4>4
        <5>3. RecordOK(n, s, vote[n][s], NB + 1)'
          <6>1. RecordOK(n, s, vote[n][s], NB + 1)
            BY <4>0 DEF NodeSlotOK
          <6>2. \A c \in Ballots, w \in Values :
                  VotedFor(n, c, s, w)' /\ c < NB + 1 => VotedFor(n, c, s, w)
            BY <1>5
          <6> QED
            BY <6>1, <6>2, <5>1, <1>5a, RecordKeep
        <5>4. vote[n][s].bal <= prom'[n]
          <6>1. vote[n][s].bal <= prom[n] /\ vote[n][s].bal \in Nat
            BY <4>0 DEF NodeSlotOK, Vote
          <6> QED
            BY <6>1, <1>3, <3>0
        <5>6. ChosenBy(s, vote[n][s].bal, rec[s].val)'
          BY <5>1, <5>2, <1>5a, ChosenByKeep
        <5>7. vote[n][s].val = rec[s].val
          BY <4>6
        <5> QED
          BY <4>1, <4>5, <5>1, <5>3, <5>4, <5>6, <5>7 DEF NodeSlotOK
      <4>7. CASE ~Keeps(s)
        <5>1. vote'[n][s] = rec[s]
          BY <4>7, <4>1
        <5>2. vote[n][s].bal < rec[s].bal
          <6>1. CASE vote[n][s].bal = 0
            BY <6>1, <4>2
          <6>2. CASE vote[n][s].bal > 0
            <7>1. Proposed(vote[n][s].bal, s, vote[n][s].val) /\ vote[n][s].bal \in Ballots
                  /\ vote[n][s].val \in Values
              BY <6>2, <4>0 DEF NodeSlotOK, RecordOK, Vote
            <7>2. PICK c0 \in 1..rec[s].bal : ChosenAt(c0, s, rec[s].val)
              BY <4>2 DEF ChosenBy
            <7>3. ~(vote[n][s].bal >= rec[s].bal)
              <8> SUFFICES ASSUME vote[n][s].bal >= rec[s].bal PROVE FALSE
                OBVIOUS
              <8>1. rec[s].val = vote[n][s].val
                BY <7>1, <7>2, <4>2, ChosenProposedAbove
              <8> QED
                BY <8>1, <4>7
            <7> QED
              BY <7>1, <7>3, <4>2
          <6> QED
            BY <6>1, <6>2, <4>0 DEF Vote
        <5>3. RecordOK(n, s, rec[s], NB + 1)
          <6>1. \A c \in Ballots, w \in Values : VotedFor(n, c, s, w) => c < rec[s].bal
            BY <5>2, <4>0, <4>2 DEF NodeSlotOK, RecordOK, Vote
          <6> QED
            BY <6>1, <4>2 DEF RecordOK
        <5>4. RecordOK(n, s, rec[s], NB + 1)'
          <6>1. \A c \in Ballots, w \in Values :
                  VotedFor(n, c, s, w)' /\ c < NB + 1 => VotedFor(n, c, s, w)
            BY <1>5
          <6> QED
            BY <5>3, <6>1, <1>5a, RecordKeep
        <5>5. ChosenBy(s, rec[s].bal, rec[s].val)'
          BY <4>2, <1>5a, ChosenByKeep
        <5> QED
          BY <4>1, <4>3, <4>5, <5>1, <5>4, <5>5, <3>0 DEF NodeSlotOK
      <4> QED
        BY <4>6, <4>7
    <3> QED
      BY <3>1, <3>2
  <2> QED
    BY <2>1, <2>2
<1>10. BallotInv'
  <2> SUFFICES ASSUME NEW c \in Ballots PROVE BallotOK(c)'
    BY DEF BallotInv
  <2>0. prom'[Owner[c]] \in Nat /\ prom'[Owner[c]] >= prom[Owner[c]]
    BY <1>4
  <2>1. CASE Owner[c] = n /\ phase[c] = "p1" /\ c < p
    <3>1. phase'[c] = "dead"
      BY <2>1, <1>1
    <3> QED
      BY <2>0, <2>1, <3>1, BallotKill
  <2>2. CASE ~(Owner[c] = n /\ phase[c] = "p1" /\ c < p)
    <3>1. phase'[c] = phase[c]
      BY <2>2, <1>1
    <3>2. phase[c] \in {"p1", "p2"} => Owner[c] \notin wiped'
      BY DEF Inv, BallotInv, BallotOK
    <3> QED
      BY <2>0, <3>1, <3>2, <1>6, BallotKeep
  <2> QED
    BY <2>1, <2>2
<1>11. MsgInv'
  <2> SUFFICES ASSUME NEW m \in msgs' PROVE MsgOK(m)'
    BY DEF MsgInv
  <2>1. \A x \in Acceptors : x \notin wiped' => /\ x \notin wiped
                                               /\ prom'[x] \in Nat
                                               /\ prom'[x] >= prom[x]
    BY <1>4
  <2> QED
    BY <1>5, <1>6, <2>1, OldMsg
<1> QED
  BY <1>7, <1>8, <1>9, <1>10, <1>11 DEF Inv

THEOREM DecideInv ==
  ASSUME Inv, Hyp, NEW b \in Ballots, NEW s \in Slots, Decide(b, s)
  PROVE  Inv'
<1>0. TypeOK /\ BallotOK(b)
  BY DEF Inv, BallotInv
<1>1. phase[b] = "p2" /\ UNCHANGED <<next, front, wiped, restarts, msgs>>
  BY DEF Decide
<1>2. PICK v \in Values, R \in Q2[s] :
        /\ \A a \in R : [type |-> "2b", acc |-> a, bal |-> b, slot |-> s, val |-> v] \in msgs
        /\ LearnAll(Owner[b], [t \in {s} |-> [bal |-> b, val |-> v]])
  BY DEF Decide
<1>3. ChosenAt(b, s, v)
  BY <1>2 DEF ChosenAt, VotedFor
<1>4. PICK a \in R : TRUE
  <2>1. PICK Q \in Q1 : TRUE
    BY DEF Hyp, QuorumFacts
  <2> QED
    BY <2>1 DEF Hyp, QuorumFacts
<1>5. Proposed(b, s, v)
  BY <1>2, <1>4, VotedProposed DEF VotedFor
<1>6. ChosenBy(s, b, v)
  BY <1>3 DEF ChosenBy
<1>7. Owner[b] \notin wiped
  BY <1>0, <1>1 DEF BallotOK
<1> DEFINE rec == [t \in {s} |-> [bal |-> b, val |-> v]]
<1>8. rec \in [{s} -> [bal : Ballots, val : Values]]
  OBVIOUS
<1>9. Owner[b] \in Acceptors /\ Owner[b] \in Learners \cup {Owner[c] : c \in Ballots}
  OBVIOUS
<1>10. {s} \in SUBSET Slots /\ {s} # {}
  OBVIOUS
<1>11. \A t \in {s} : ChosenBy(t, rec[t].bal, rec[t].val) /\ Proposed(rec[t].bal, t, rec[t].val)
  BY <1>5, <1>6
<1>12. LearnAll(Owner[b], rec) /\ UNCHANGED <<next, front, wiped, restarts, msgs>>
  BY <1>1, <1>2
<1> QED
  BY <1>7, <1>8, <1>9, <1>10, <1>11, <1>12, LearnInv

THEOREM CatchUpInv ==
  ASSUME Inv, Hyp, NEW n \in Acceptors, NEW q \in Acceptors, CatchUp(n, q)
  PROVE  Inv'
<1> DEFINE L == {t \in Slots : learned[n][t] = None /\ learned[q][t] # None}
           rec == [t \in L |-> vote[q][t]]
<1>0. TypeOK
  BY DEF Inv
<1>1. /\ n \in Learners /\ n \notin wiped /\ q \notin wiped /\ L # {}
      /\ LearnAll(n, rec)
      /\ UNCHANGED <<next, front, wiped, restarts, msgs>>
  BY DEF CatchUp, Live, Hyp, InvApplies
<1>2. \A t \in L : /\ rec[t] \in [bal : Ballots, val : Values]
                   /\ ChosenBy(t, rec[t].bal, rec[t].val)
                   /\ Proposed(rec[t].bal, t, rec[t].val)
  <2> SUFFICES ASSUME NEW t \in L
               PROVE  /\ vote[q][t] \in [bal : Ballots, val : Values]
                      /\ ChosenBy(t, vote[q][t].bal, vote[q][t].val)
                      /\ Proposed(vote[q][t].bal, t, vote[q][t].val)
    OBVIOUS
  <2>1. NodeSlotOK(q, t) /\ vote[q][t] \in Vote
    <3>1. NodeOK(q)
      BY DEF Inv, NodeInv
    <3> QED
      BY <3>1, <1>0, <1>1 DEF NodeOK, TypeOK
  <2>2. ChosenBy(t, vote[q][t].bal, vote[q][t].val) /\ vote[q][t].bal \in Ballots
    BY <2>1 DEF NodeSlotOK, ChosenBy, Vote
  <2>3. vote[q][t].val \in Values /\ Proposed(vote[q][t].bal, t, vote[q][t].val)
    BY <2>1, <2>2 DEF NodeSlotOK, RecordOK, Vote
  <2> QED
    BY <2>1, <2>2, <2>3 DEF Vote
<1>3. rec \in [L -> [bal : Ballots, val : Values]]
  BY <1>2
<1> QED
  BY <1>1, <1>2, <1>3, LearnInv

=============================================================================
