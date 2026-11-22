----------------------- MODULE MultiPaxosPhase2Proof ------------------------
(***************************************************************************)
(* Phase 2 keeps `Inv`: `Allocate` and `Accept` (#1307, step 2).           *)
(***************************************************************************)
EXTENDS MultiPaxosFactsProof

USE NBType, NSType, NoneNotValue, NoopNotCommand, OwnerType, LearnersType
    DEF Ballots, Slots, Values, NoVote

THEOREM AllocateInv ==
  ASSUME Inv, Hyp, NEW b \in Ballots, NEW c \in Commands, Allocate(b, c)
  PROVE  Inv'
<1> DEFINE m2a == [type |-> "2a", bal |-> b, slot |-> next[b], val |-> c]
<1>0. TypeOK /\ BallotOK(b)
  BY DEF Inv, BallotInv
<1>1. /\ phase[b] = "p2" /\ next[b] < NS
      /\ next' = [next EXCEPT ![b] = @ + 1]
      /\ msgs' = msgs \cup {m2a}
      /\ UNCHANGED <<prom, vote, dprom, dvote, learned, phase, front, wiped, restarts>>
  BY DEF Allocate, Send
<1>2. next[b] \in Slots /\ c \in Values
  BY <1>0, <1>1 DEF TypeOK
<1>3. msgs \subseteq msgs'
  BY <1>1
<1>4. \A x, d, s, v : VotedFor(x, d, s, v)' = VotedFor(x, d, s, v)
  BY <1>1 DEF VotedFor
<1>5. KeepsVW
  <2>1. \A x \in Acceptors, d \in Ballots, s \in Slots :
          WontVoteIn(x, d, s) => \A v \in Values : ~VotedFor(x, d, s, v)'
    BY <1>4 DEF WontVoteIn
  <2>2. \A x \in Acceptors : prom'[x] \in Nat /\ prom'[x] >= prom[x]
    BY <1>0, <1>1 DEF TypeOK
  <2> QED
    BY <1>0, <1>1, <1>3, <2>1, <2>2, KeepsVWFrom
<1>6. \A v \in Values : ~Proposed(b, next[b], v)
  BY <1>0, <1>2 DEF BallotOK
<1>7. FreeAt(b, next[b])
  BY <1>0, <1>1, <1>2 DEF BallotOK
<1>8. TypeOK'
  <2>1. next' \in [Ballots -> 0..NS]
    BY <1>0, <1>1 DEF TypeOK
  <2>2. m2a \in Msg
    BY <1>2 DEF Msg
  <2> QED
    BY <1>0, <1>1, <2>1, <2>2 DEF TypeOK
<1>9. NodeInv'
  <2> SUFFICES ASSUME NEW x \in Acceptors PROVE NodeOK(x)'
    BY DEF NodeInv
  <2>1. IF x \in wiped THEN prom'[x] = prom[x] ELSE prom'[x] \in Nat /\ prom'[x] >= prom[x]
    BY <1>0, <1>1 DEF TypeOK
  <2>2. vote'[x] = vote[x] /\ (\A s \in Slots : learned'[x][s] = learned[x][s])
        /\ (x \in wiped' <=> x \in wiped)
    BY <1>1
  <2> QED
    BY <1>3, <1>4, <2>1, <2>2, NodeKeep
<1>10. BallotInv'
  <2> SUFFICES ASSUME NEW d \in Ballots PROVE BallotOK(d)'
    BY DEF BallotInv
  <2>1. CASE d = b
    <3>1. phase'[b] = "p2" /\ next'[b] = next[b] + 1 /\ front'[b] = front[b]
          /\ front[b] <= next[b] /\ next[b] \in Nat /\ front[b] \in Nat
      BY <1>0, <1>1 DEF TypeOK, BallotOK
    <3>2. \A s \in Slots, v \in Values : Proposed(b, s, v)' => s < next[b] + 1
      <4> SUFFICES ASSUME NEW s \in Slots, NEW v \in Values, Proposed(b, s, v)'
                   PROVE  s < next[b] + 1
        OBVIOUS
      <4>1. CASE Proposed(b, s, v)
        BY <4>1, <1>0, <1>2 DEF BallotOK
      <4>2. CASE ~Proposed(b, s, v)
        BY <4>2, <1>1, <1>2 DEF Proposed
      <4> QED
        BY <4>1, <4>2
    <3>3. \A s \in Slots : s >= next[b] + 1 => FreeAt(b, s)'
      <4> SUFFICES ASSUME NEW s \in Slots, s >= next[b] + 1 PROVE FreeAt(b, s)'
        OBVIOUS
      <4>1. FreeAt(b, s)
        BY <1>0, <1>1, <1>2 DEF BallotOK
      <4> QED
        BY <4>1, <1>5, FreeAtKeep
    <3>4. (\E m \in msgs' : m.type = "1a" /\ m.bal = b)
      BY <1>0, <1>1, <1>3 DEF BallotOK
    <3>5. Owner[b] \notin wiped' /\ prom'[Owner[b]] >= b
      BY <1>0, <1>1 DEF BallotOK
    <3> QED
      BY <2>1, <3>1, <3>2, <3>3, <3>4, <3>5 DEF BallotOK
  <2>2. CASE d # b
    <3>1. \A n \in msgs' : n.type \in {"1a", "2a"} /\ n.bal = d => n \in msgs
      BY <2>2, <1>1
    <3>2. phase[d] \in {"p1", "p2"} => Owner[d] \notin wiped'
      BY <1>1 DEF Inv, BallotInv, BallotOK
    <3>3. phase'[d] = phase[d] /\ next'[d] = next[d] /\ front'[d] = front[d]
      BY <2>2, <1>0, <1>1 DEF TypeOK
    <3>4. prom'[Owner[d]] \in Nat /\ prom'[Owner[d]] >= prom[Owner[d]]
      BY <1>0, <1>1 DEF TypeOK
    <3> QED
      BY <1>1, <1>3, <1>5, <3>1, <3>2, <3>3, <3>4, BallotKeep
  <2> QED
    BY <2>1, <2>2
<1>11. MsgInv'
  <2> SUFFICES ASSUME NEW m \in msgs' PROVE MsgOK(m)'
    BY DEF MsgInv
  <2>1. CASE m \in msgs
    <3>1. m.type = "2a" => \A n \in msgs' : n.type = "2a" /\ n.bal = m.bal /\ n.slot = m.slot => n \in msgs
      <4> SUFFICES ASSUME m.type = "2a", NEW n \in msgs', n.type = "2a", n.bal = m.bal,
                          n.slot = m.slot, n = m2a
                   PROVE  FALSE
        BY <1>1
      <4>1. m = [type |-> "2a", bal |-> m.bal, slot |-> m.slot, val |-> m.val]
        BY <2>1, <1>0, MsgFields
      <4>2. Proposed(b, next[b], m.val) /\ m.val \in Values
        BY <4>1, <2>1, <1>0, MsgFields DEF Proposed
      <4> QED
        BY <4>2, <1>6
    <3>2. \A x \in Acceptors : x \notin wiped' => /\ x \notin wiped
                                                 /\ prom'[x] \in Nat
                                                 /\ prom'[x] >= prom[x]
      BY <1>0, <1>1 DEF TypeOK
    <3>3. m.type = "1a" => \A n \in msgs' : n.type = "1a" /\ n.bal = m.bal => n \in msgs
      BY <1>1
    <3>4. m.type = "1b" => /\ \A n \in msgs' : n.type = "1b" /\ n.bal = m.bal /\ n.acc = m.acc => n \in msgs
                           /\ \A s \in Slots, d \in Ballots, v \in Values :
                                VotedFor(m.acc, d, s, v)' /\ d < m.bal => VotedFor(m.acc, d, s, v)
      BY <1>1, <1>4
    <3> QED
      BY <2>1, <3>1, <3>2, <3>3, <3>4, <1>3, <1>5, OldMsg
  <2>2. CASE m = m2a
    <3>0. m.type = "2a" /\ m.bal = b /\ m.slot = next[b] /\ m.val = c
      BY <2>2
    <3>1. SafeAt(m.bal, m.slot, m.val)
      BY <3>0, <1>7, FreeSafe
    <3>2. SafeAt(m.bal, m.slot, m.val)'
      BY <3>0, <3>1, <1>2, <1>5, SafeAtKeep
    <3>3. \A n \in msgs' : n.type = "2a" /\ n.bal = m.bal /\ n.slot = m.slot => n = m
      <4> SUFFICES ASSUME NEW n \in msgs, n.type = "2a", n.bal = b, n.slot = next[b] PROVE FALSE
        BY <1>1, <2>2, <3>0
      <4>1. n = [type |-> "2a", bal |-> b, slot |-> next[b], val |-> n.val] /\ n.val \in Values
        BY <1>0, MsgFields
      <4> QED
        BY <4>1, <1>6 DEF Proposed
    <3> QED
      BY <3>0, <3>2, <3>3 DEF MsgOK
  <2> QED
    BY <2>1, <2>2, <1>1
<1> QED
  BY <1>1, <1>8, <1>9, <1>10, <1>11 DEF Inv


THEOREM AcceptInv ==
  ASSUME Inv, Hyp, NEW a \in Acceptors, NEW b \in Ballots, NEW s \in Slots, Accept(a, b, s)
  PROVE  Inv'
<1>0. TypeOK
  BY DEF Inv
<1>1. /\ a \notin wiped /\ prom[a] <= b
      /\ UNCHANGED <<learned, phase, next, front, wiped, restarts>>
  BY DEF Accept, Live, Hyp, InvApplies
<1>2. PICK m0 \in msgs : /\ m0.type = "2a" /\ m0.bal = b /\ m0.slot = s
                         /\ vote[a][s] # [bal |-> b, val |-> m0.val]
                         /\ prom' = [prom EXCEPT ![a] = b]
                         /\ vote' = [vote EXCEPT ![a][s] = [bal |-> b, val |-> m0.val]]
                         /\ dprom' = [dprom EXCEPT ![a] = b]
                         /\ dvote' = [dvote EXCEPT ![a][s] = [bal |-> b, val |-> m0.val]]
                         /\ msgs' = msgs \cup {[type |-> "2b", acc |-> a, bal |-> b, slot |-> s,
                                                val |-> m0.val]}
  BY DEF Accept, Send, Persisted, Hyp, InvApplies
<1> DEFINE v == m0.val
           m2b == [type |-> "2b", acc |-> a, bal |-> b, slot |-> s, val |-> v]
<1>3. m0 = [type |-> "2a", bal |-> b, slot |-> s, val |-> v] /\ v \in Values
  BY <1>0, <1>2, MsgFields
<1>4. Proposed(b, s, v)
  BY <1>3 DEF Proposed
<1>5. msgs' = msgs \cup {m2b} /\ msgs \subseteq msgs'
  BY <1>2
<1>6. \A x, c, t, w : VotedFor(x, c, t, w)' <=> VotedFor(x, c, t, w) \/ (x = a /\ c = b /\ t = s /\ w = v)
  BY <1>5 DEF VotedFor
<1>7. \A x \in Acceptors : prom'[x] \in Nat /\ prom'[x] >= prom[x]
  BY <1>0, <1>1, <1>2 DEF TypeOK
<1>8. prom[a] \in Nat /\ prom'[a] = b
  BY <1>0, <1>2 DEF TypeOK
<1>9. KeepsVW
  <2>1. \A x \in Acceptors, c \in Ballots, t \in Slots :
          WontVoteIn(x, c, t) => \A w \in Values : ~VotedFor(x, c, t, w)'
    BY <1>1, <1>6, <1>8 DEF WontVoteIn
  <2>2. wiped \subseteq wiped'
    BY <1>1
  <2> QED
    BY <1>0, <1>5, <1>7, <2>1, <2>2, KeepsVWFrom
<1>10. TypeOK'
  <2>1. prom' \in [Acceptors -> 0..NB] /\ dprom' \in [Acceptors -> 0..NB]
    BY <1>0, <1>2 DEF TypeOK
  <2>2. [bal |-> b, val |-> v] \in Vote
    BY <1>3 DEF Vote
  <2>3. vote' \in [Acceptors -> [Slots -> Vote]] /\ dvote' \in [Acceptors -> [Slots -> Vote]]
    BY <1>0, <1>2, <2>2 DEF TypeOK
  <2>4. m2b \in Msg
    BY <1>3 DEF Msg
  <2> QED
    BY <1>0, <1>1, <1>5, <2>1, <2>3, <2>4 DEF TypeOK
<1>11. NodeInv'
  <2> SUFFICES ASSUME NEW x \in Acceptors PROVE NodeOK(x)'
    BY DEF NodeInv
  <2>1. CASE x # a
    <3>1. IF x \in wiped THEN prom'[x] = prom[x] ELSE prom'[x] \in Nat /\ prom'[x] >= prom[x]
      BY <2>1, <1>0, <1>2, <1>7 DEF TypeOK
    <3>2. vote'[x] = vote[x] /\ (\A t \in Slots : learned'[x][t] = learned[x][t])
          /\ (x \in wiped' <=> x \in wiped)
      BY <2>1, <1>0, <1>1, <1>2 DEF TypeOK
    <3>3. \A c \in Ballots, t \in Slots, w \in Values : VotedFor(x, c, t, w)' = VotedFor(x, c, t, w)
      BY <2>1, <1>5 DEF VotedFor
    <3> QED
      BY <1>5, <3>1, <3>2, <3>3, NodeKeep
  <2>2. CASE x = a
    <3> SUFFICES ASSUME NEW t \in Slots PROVE NodeSlotOK(a, t)'
      BY <2>2, <1>1 DEF NodeOK
    <3>1. CASE t # s
      <4>1. vote'[a][t] = vote[a][t] /\ learned'[a][t] = learned[a][t]
        BY <3>1, <1>0, <1>1, <1>2 DEF TypeOK
      <4>2. \A c \in Ballots, w \in Values : VotedFor(a, c, t, w)' = VotedFor(a, c, t, w)
        BY <3>1, <1>5 DEF VotedFor
      <4> QED
        BY <1>1, <1>5, <1>7, <4>1, <4>2, NodeSlotKeep
    <3>2. CASE t = s
      <4>0. NodeSlotOK(a, s) /\ vote[a][s] \in Vote
        <5>1. NodeOK(a)
          BY DEF Inv, NodeInv
        <5> QED
          BY <5>1, <1>0, <1>1 DEF NodeOK, TypeOK
      <4>1. vote'[a][s] = [bal |-> b, val |-> v] /\ learned'[a][s] = learned[a][s]
        BY <1>0, <1>1, <1>2 DEF TypeOK
      <4>2. vote[a][s].bal <= b
        BY <4>0, <1>1, <1>8 DEF NodeSlotOK, Vote
      <4>3. RecordOK(a, s, [bal |-> b, val |-> v], NB + 1)'
        <5>1. \A c \in Ballots, w \in Values :
                VotedFor(a, c, s, w)' => c < b \/ (c = b /\ w = v)
          <6> SUFFICES ASSUME NEW c \in Ballots, NEW w \in Values, VotedFor(a, c, s, w)'
                       PROVE  c < b \/ (c = b /\ w = v)
            OBVIOUS
          <6>1. CASE VotedFor(a, c, s, w)
            <7>1. c <= vote[a][s].bal
              BY <6>1, <4>0 DEF NodeSlotOK, RecordOK, Vote
            <7>2. ASSUME c = b PROVE w = v
              <8>1. Proposed(b, s, w)
                BY <7>2, <6>1, VotedProposed
              <8> QED
                BY <8>1, <1>4, OneValue
            <7> QED
              BY <7>1, <7>2, <4>2, <4>0 DEF Vote
          <6>2. CASE ~VotedFor(a, c, s, w)
            BY <6>2, <1>6
          <6> QED
            BY <6>1, <6>2
        <5>2. Proposed(b, s, v)'
          BY <1>4, <1>5 DEF Proposed
        <5> QED
          BY <5>1, <5>2, <1>3 DEF RecordOK
      <4>4. (learned[a][s] # None => a \in Learners \cup {Owner[c] : c \in Ballots})'
        BY <4>0, <4>1 DEF NodeSlotOK
      <4>5. VotedFor(a, b, s, v)'
        BY <1>6
      <4>6. (learned[a][s] # None => /\ v = learned[a][s]
                                     /\ ChosenBy(s, b, learned[a][s]))'
        <5> SUFFICES ASSUME learned[a][s] # None
                     PROVE  v = learned[a][s] /\ ChosenBy(s, b, learned[a][s])'
          BY <4>1
        <5>1. ChosenBy(s, vote[a][s].bal, learned[a][s]) /\ vote[a][s].val = learned[a][s]
          BY <4>0 DEF NodeSlotOK
        <5>2. PICK c \in 1..vote[a][s].bal : ChosenAt(c, s, learned[a][s])
          BY <5>1 DEF ChosenBy
        <5>3. learned[a][s] \in Values
          BY <1>0, <5>1 DEF TypeOK
        <5>4. c \in Ballots /\ c <= b
          BY <4>0, <4>2 DEF Vote
        <5>5. v = learned[a][s]
          BY <5>2, <5>3, <5>4, <1>3, <1>4, ChosenProposedAbove
        <5>6. ChosenBy(s, b, learned[a][s])
          BY <5>2, <5>4 DEF ChosenBy
        <5>7. \A l : l = learned[a][s] => ChosenBy(s, b, l)'
          BY <5>6, <1>5, ChosenByKeep
        <5> QED
          BY <5>5, <5>7, <4>1
      <4> QED
        BY <3>2, <4>1, <4>3, <4>4, <4>5, <4>6, <1>8 DEF NodeSlotOK
    <3> QED
      BY <3>1, <3>2
  <2> QED
    BY <2>1, <2>2
<1>12. BallotInv'
  <2> SUFFICES ASSUME NEW c \in Ballots PROVE BallotOK(c)'
    BY DEF BallotInv
  <2>1. \A n \in msgs' : n.type \in {"1a", "2a"} /\ n.bal = c => n \in msgs
    BY <1>5
  <2>2. phase[c] \in {"p1", "p2"} => Owner[c] \notin wiped'
    BY <1>1 DEF Inv, BallotInv, BallotOK
  <2>3. phase'[c] = phase[c] /\ next'[c] = next[c] /\ front'[c] = front[c]
    BY <1>1
  <2>4. prom'[Owner[c]] \in Nat /\ prom'[Owner[c]] >= prom[Owner[c]]
    BY <1>7
  <2>5. wiped \subseteq wiped'
    BY <1>1
  <2> QED
    BY <1>5, <1>9, <2>1, <2>2, <2>3, <2>4, <2>5, BallotKeep
<1>13. MsgInv'
  <2> SUFFICES ASSUME NEW m \in msgs' PROVE MsgOK(m)'
    BY DEF MsgInv
  <2>1. CASE m \in msgs
    <3>1. m.type = "1b" => \A t \in Slots, c \in Ballots, w \in Values :
                             VotedFor(m.acc, c, t, w)' /\ c < m.bal => VotedFor(m.acc, c, t, w)
      <4> SUFFICES ASSUME m.type = "1b", NEW t \in Slots, NEW c \in Ballots, NEW w \in Values,
                          VotedFor(m.acc, c, t, w)', c < m.bal, ~VotedFor(m.acc, c, t, w)
                   PROVE  FALSE
        OBVIOUS
      <4>1. m.acc = a /\ c = b
        BY <1>6
      <4>2. m.bal <= prom[a] /\ m.bal \in Nat
        <5>1. MsgOK(m)
          BY <2>1 DEF Inv, MsgInv
        <5>2. m.bal \in Ballots
          BY <2>1, <1>0, MsgFields
        <5> QED
          BY <5>1, <5>2, <4>1, <1>1 DEF MsgOK
      <4> QED
        BY <4>1, <4>2, <1>1, <1>8
    <3>2. \A x \in Acceptors : x \notin wiped' => /\ x \notin wiped
                                                 /\ prom'[x] \in Nat
                                                 /\ prom'[x] >= prom[x]
      BY <1>1, <1>7
    <3> QED
      BY <2>1, <3>1, <3>2, <1>5, <1>9, OldMsg
  <2>2. CASE m = m2b
    <3>0. m.type = "2b" /\ m.acc = a /\ m.bal = b /\ m.slot = s /\ m.val = v
      BY <2>2
    <3>1. Proposed(m.bal, m.slot, m.val)'
      BY <3>0, <1>4, <1>5 DEF Proposed
    <3>2. m.acc \notin wiped' => m.bal <= prom'[m.acc]
      BY <3>0, <1>8
    <3> QED
      BY <3>0, <3>1, <3>2 DEF MsgOK
  <2> QED
    BY <2>1, <2>2, <1>5
<1> QED
  BY <1>1, <1>2, <1>10, <1>11, <1>12, <1>13 DEF Inv

=============================================================================
