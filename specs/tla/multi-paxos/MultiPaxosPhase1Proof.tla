----------------------- MODULE MultiPaxosPhase1Proof ------------------------
(***************************************************************************)
(* Phase 1 keeps `Inv`: `Prepare` and `Promise` (#1307, step 2).           *)
(***************************************************************************)
EXTENDS MultiPaxosFactsProof

USE NBType, NSType, NoneNotValue, NoopNotCommand, OwnerType, LearnersType
    DEF Ballots, Slots, Values, NoVote

THEOREM PrepareInv ==
  ASSUME Inv, Hyp, NEW b \in Ballots, Prepare(b)
  PROVE  Inv'
<1> DEFINE o == Owner[b]
           fr == FirstUnlearned(o)
           m1a == [type |-> "1a", bal |-> b, from |-> fr]
           m1b == PromiseMsg(o, b, fr)
<1>0. TypeOK /\ o \in Acceptors
  BY DEF Inv
<1>a. /\ o \notin wiped /\ prom[o] < b
      /\ phase' = [phase EXCEPT ![b] = "p1"]
      /\ prom' = [prom EXCEPT ![o] = b]
      /\ dprom' = [dprom EXCEPT ![o] = b]
      /\ msgs' = msgs \cup {m1a, m1b}
      /\ UNCHANGED <<vote, dvote, learned, next, front, wiped, restarts>>
  BY PersistedNew DEF Prepare, Live, SendAll, Hyp, InvApplies
\* A ballot that left "idle" holds its owner's promise (`BallotOK`), so a
\* promise below b says that b is still "idle".
<1>b. phase[b] = "idle"
  <2>1. BallotOK(b)
    BY DEF Inv, BallotInv
  <2>2. phase[b] # "idle" => prom[o] >= b \/ o \in wiped
    BY <2>1 DEF BallotOK
  <2>3. prom[o] \in Nat
    BY <1>0 DEF TypeOK
  <2> QED
    BY <1>a, <2>2, <2>3
<1>1. /\ phase[b] = "idle" /\ o \notin wiped /\ prom[o] < b
      /\ phase' = [phase EXCEPT ![b] = "p1"]
      /\ prom' = [prom EXCEPT ![o] = b]
      /\ dprom' = [dprom EXCEPT ![o] = b]
      /\ msgs' = msgs \cup {m1a, m1b}
      /\ UNCHANGED <<vote, dvote, learned, next, front, wiped, restarts>>
  BY <1>a, <1>b
<1>2. fr \in 0..NS
  BY <1>0, FirstUnlearnedType
<1>3. m1b = [type |-> "1b", acc |-> o, bal |-> b, from |-> fr,
             rep |-> [s \in Slots |-> IF s >= fr THEN vote[o][s] ELSE NoVote]]
  BY DEF PromiseMsg
<1>4. msgs' = msgs \cup {m1a, m1b} /\ msgs \subseteq msgs'
  BY <1>1
<1>5. \A a, c, s, v : VotedFor(a, c, s, v)' = VotedFor(a, c, s, v)
  BY <1>3, <1>4 DEF VotedFor
<1>6. \A x \in Acceptors : prom'[x] \in Nat /\ prom'[x] >= prom[x]
  BY <1>0, <1>1 DEF TypeOK
<1>7. KeepsVW
  <2>1. \A a \in Acceptors, c \in Ballots, s \in Slots :
          WontVoteIn(a, c, s) => \A v \in Values : ~VotedFor(a, c, s, v)'
    BY <1>5 DEF WontVoteIn
  <2>2. wiped \subseteq wiped'
    BY <1>1
  <2> QED
    BY <1>0, <1>4, <1>6, <2>1, <2>2, KeepsVWFrom
<1>8. TypeOK'
  <2>1. prom' \in [Acceptors -> 0..NB] /\ dprom' \in [Acceptors -> 0..NB]
    BY <1>0, <1>1 DEF TypeOK
  <2>2. phase' \in [Ballots -> {"idle", "p1", "p2", "dead"}]
    BY <1>0, <1>1 DEF TypeOK
  <2>3. m1a \in Msg
    BY <1>2 DEF Msg
  <2>4. m1b \in Msg
    <3>1. \A s \in Slots : vote[o][s] \in Vote
      BY <1>0 DEF TypeOK
    <3>2. NoVote \in Vote
      BY DEF Vote
    <3>3. [s \in Slots |-> IF s >= fr THEN vote[o][s] ELSE NoVote] \in [Slots -> Vote]
      BY <3>1, <3>2
    <3> QED
      BY <1>0, <1>2, <1>3, <3>3 DEF Msg
  <2> QED
    BY <1>0, <1>1, <1>4, <2>1, <2>2, <2>3, <2>4 DEF TypeOK
<1>9. NodeInv'
  <2> SUFFICES ASSUME NEW x \in Acceptors PROVE NodeOK(x)'
    BY DEF NodeInv
  <2>1. IF x \in wiped THEN prom'[x] = prom[x] ELSE prom'[x] \in Nat /\ prom'[x] >= prom[x]
    BY <1>0, <1>1, <1>6 DEF TypeOK
  <2>2. vote'[x] = vote[x] /\ (\A s \in Slots : learned'[x][s] = learned[x][s])
        /\ (x \in wiped' <=> x \in wiped)
    BY <1>1
  <2>3. \A c \in Ballots, s \in Slots, v \in Values : VotedFor(x, c, s, v)' = VotedFor(x, c, s, v)
    BY <1>5
  <2> QED
    BY <1>4, <2>1, <2>2, <2>3, NodeKeep
<1>10. BallotInv'
  <2> SUFFICES ASSUME NEW c \in Ballots PROVE BallotOK(c)'
    BY DEF BallotInv
  <2>0. \A n \in msgs : n.type \in {"1a", "2a"} => n.bal # b
    BY <1>1, IdleSilent
  <2>1. CASE c = b
    <3>1. BallotOK(b)
      BY DEF Inv, BallotInv
    <3>2. next[b] = 0 /\ front[b] = 0
      BY <3>1, <1>1 DEF BallotOK
    <3>3. \A s \in Slots, v \in Values : ~Proposed(b, s, v)'
      <4> SUFFICES ASSUME NEW s \in Slots, NEW v \in Values, Proposed(b, s, v)' PROVE FALSE
        OBVIOUS
      <4> DEFINE r == [type |-> "2a", bal |-> b, slot |-> s, val |-> v]
      <4>1. r \in msgs \/ r = m1a \/ r = m1b
        BY <1>4 DEF Proposed
      <4>2. r.type = "2a" /\ r.bal = b /\ m1a.type = "1a" /\ m1b.type = "1b"
        BY <1>3
      <4>3. r \notin msgs
        BY <2>0, <4>2
      <4> QED
        BY <4>1, <4>2, <4>3
    <3>4. phase'[b] = "p1" /\ next'[b] = 0 /\ front'[b] = 0 /\ prom'[o] = b
      BY <1>0, <1>1, <3>2 DEF TypeOK
    <3>5. \E n \in msgs' : n.type = "1a" /\ n.bal = b
      <4>1. m1a \in msgs' /\ m1a.type = "1a" /\ m1a.bal = b
        BY <1>4
      <4> QED
        BY <4>1
    <3>6. Owner[b] \notin wiped'
      BY <1>1
    <3> QED
      BY <2>1, <3>3, <3>4, <3>5, <3>6 DEF BallotOK
  <2>2. CASE c # b
    <3>1. \A n \in msgs' : n.type \in {"1a", "2a"} /\ n.bal = c => n \in msgs
      BY <2>2, <1>3, <1>4
    <3>2. phase[c] \in {"p1", "p2"} => Owner[c] \notin wiped'
      BY <1>1 DEF Inv, BallotInv, BallotOK
    <3>3. phase'[c] = phase[c] /\ next'[c] = next[c] /\ front'[c] = front[c]
      BY <2>2, <1>0, <1>1 DEF TypeOK
    <3>4. Owner[c] \in wiped' \/ (prom'[Owner[c]] \in Nat /\ prom'[Owner[c]] >= prom[Owner[c]])
      BY <1>6
    <3>5. wiped \subseteq wiped'
      BY <1>1
    <3> QED
      BY <1>4, <1>7, <3>1, <3>2, <3>3, <3>4, <3>5, BallotKeep
  <2> QED
    BY <2>1, <2>2
<1>11. MsgInv'
  <2> SUFFICES ASSUME NEW m \in msgs' PROVE MsgOK(m)'
    BY DEF MsgInv
  <2>0. \A n \in msgs : n.type \in {"1a", "2a"} => n.bal # b
    BY <1>1, IdleSilent
  <2>1. \A n \in msgs : n.type = "1b" /\ n.acc = o => n.bal # b
    BY <1>0, <1>1, NoPromiseAbove
  <2>2. CASE m \in msgs
    <3>1. m.type = "1a" => \A n \in msgs' : n.type = "1a" /\ n.bal = m.bal => n \in msgs
      BY <2>0, <2>2, <1>3, <1>4
    <3>2. m.type = "1b" => /\ \A n \in msgs' : n.type = "1b" /\ n.bal = m.bal /\ n.acc = m.acc => n \in msgs
                           /\ \A s \in Slots, c \in Ballots, v \in Values :
                                VotedFor(m.acc, c, s, v)' /\ c < m.bal => VotedFor(m.acc, c, s, v)
      BY <2>1, <2>2, <1>3, <1>4, <1>5
    <3>3. m.type = "2a" => \A n \in msgs' : n.type = "2a" /\ n.bal = m.bal /\ n.slot = m.slot => n \in msgs
      BY <1>3, <1>4
    <3>4. \A x \in Acceptors : x \notin wiped' => /\ x \notin wiped
                                                 /\ prom'[x] \in Nat
                                                 /\ prom'[x] >= prom[x]
      BY <1>1, <1>6
    <3> QED
      BY <2>2, <3>1, <3>2, <3>3, <3>4, <1>4, <1>7, OldMsg
  <2>3. CASE m = m1a
    <3>1. \A n \in msgs' : n.type = "1a" /\ n.bal = b => n = m1a
      BY <2>0, <1>3, <1>4
    <3>2. m1b \in msgs' /\ m1b.type = "1b" /\ m1b.bal = b /\ m1b.acc = Owner[b]
      BY <1>3, <1>4
    <3> QED
      BY <2>3, <3>1, <3>2 DEF MsgOK
  <2>4. CASE m = m1b
    <3>1. \A n \in msgs' : n.type = "1b" /\ n.bal = b /\ n.acc = o => n = m1b
      BY <2>1, <1>3, <1>4
    <3>2. \A s \in Slots : \A r : r = vote[o][s] => RecordOK(o, s, r, b)'
      <4>1. o \in Acceptors /\ o \notin wiped /\ prom[o] < b /\ msgs \subseteq msgs'
        BY <1>0, <1>1, <1>4
      <4>2. \A c \in Ballots, s \in Slots, v \in Values : VotedFor(o, c, s, v)' = VotedFor(o, c, s, v)
        BY <1>5
      <4> QED
        BY <4>1, <4>2, PromiseOK
    <3>0. m.type = "1b" /\ m.acc = o /\ m.bal = b /\ m.from = fr
      BY <2>4, <1>3
    <3>3. \A s \in Slots : IF s >= m.from THEN RecordOK(m.acc, s, m.rep[s], m.bal)' ELSE m.rep[s] = NoVote
      <4> SUFFICES ASSUME NEW s \in Slots
                   PROVE  IF s >= fr THEN RecordOK(o, s, m.rep[s], b)' ELSE m.rep[s] = NoVote
        BY <3>0
      <4>1. m.rep[s] = IF s >= fr THEN vote[o][s] ELSE NoVote
        BY <2>4, <1>3
      <4>2. CASE s >= fr
        <5>1. m.rep[s] = vote[o][s]
          BY <4>1, <4>2
        <5> QED
          BY <4>2, <5>1, <3>2
      <4>3. CASE ~(s >= fr)
        BY <4>1, <4>3
      <4> QED
        BY <4>2, <4>3
    <3>4. [type |-> "1a", bal |-> m.bal, from |-> m.from] \in msgs'
      BY <3>0, <1>4
    <3>5. prom'[o] = b
      BY <1>0, <1>1 DEF TypeOK
    <3>6. \A n \in msgs' : n.type = "1b" /\ n.bal = m.bal /\ n.acc = m.acc => n = m
      BY <3>0, <3>1, <2>4
    <3> QED
      BY <3>0, <3>3, <3>4, <3>5, <3>6 DEF MsgOK
  <2> QED
    BY <2>2, <2>3, <2>4, <1>4
<1> QED
  BY <1>1, <1>8, <1>9, <1>10, <1>11 DEF Inv
THEOREM PromiseInv ==
  ASSUME Inv, Hyp, NEW a \in Acceptors, NEW b \in Ballots, Promise(a, b)
  PROVE  Inv'
<1>0. TypeOK
  BY DEF Inv
<1>1. /\ a \notin wiped /\ prom[a] < b
      /\ UNCHANGED <<vote, dvote, learned, phase, next, front, wiped, restarts>>
  BY DEF Promise, Live, Hyp, InvApplies
<1>2. PICK m0 \in msgs : /\ m0.type = "1a" /\ m0.bal = b
                         /\ prom' = [prom EXCEPT ![a] = b]
                         /\ dprom' = [dprom EXCEPT ![a] = b]
                         /\ msgs' = msgs \cup {PromiseMsg(a, b, m0.from)}
  BY PersistedNew DEF Promise, Send
<1> DEFINE fr == m0.from
           m1b == PromiseMsg(a, b, fr)
<1>3. m0 = [type |-> "1a", bal |-> b, from |-> fr] /\ fr \in 0..NS
  BY <1>0, <1>2, MsgFields
<1>4. m1b = [type |-> "1b", acc |-> a, bal |-> b, from |-> fr,
             rep |-> [s \in Slots |-> IF s >= fr THEN vote[a][s] ELSE NoVote]]
  BY DEF PromiseMsg
<1>5. msgs' = msgs \cup {m1b} /\ msgs \subseteq msgs'
  BY <1>2
<1>6. \A x, c, s, v : VotedFor(x, c, s, v)' = VotedFor(x, c, s, v)
  BY <1>4, <1>5 DEF VotedFor
<1>7. \A x \in Acceptors : prom'[x] \in Nat /\ prom'[x] >= prom[x]
  BY <1>0, <1>1, <1>2 DEF TypeOK
<1>8. KeepsVW
  <2>1. \A x \in Acceptors, c \in Ballots, s \in Slots :
          WontVoteIn(x, c, s) => \A v \in Values : ~VotedFor(x, c, s, v)'
    BY <1>6 DEF WontVoteIn
  <2>2. wiped \subseteq wiped'
    BY <1>1
  <2> QED
    BY <1>0, <1>5, <1>7, <2>1, <2>2, KeepsVWFrom
<1>9. TypeOK'
  <2>1. prom' \in [Acceptors -> 0..NB] /\ dprom' \in [Acceptors -> 0..NB]
    BY <1>0, <1>2 DEF TypeOK
  <2>2. m1b \in Msg
    <3>1. \A s \in Slots : vote[a][s] \in Vote
      BY <1>0 DEF TypeOK
    <3>2. NoVote \in Vote
      BY DEF Vote
    <3>3. [s \in Slots |-> IF s >= fr THEN vote[a][s] ELSE NoVote] \in [Slots -> Vote]
      BY <3>1, <3>2
    <3> QED
      BY <1>3, <1>4, <3>3 DEF Msg
  <2> QED
    BY <1>0, <1>1, <1>5, <2>1, <2>2 DEF TypeOK
<1>10. NodeInv'
  <2> SUFFICES ASSUME NEW x \in Acceptors PROVE NodeOK(x)'
    BY DEF NodeInv
  <2>1. IF x \in wiped THEN prom'[x] = prom[x] ELSE prom'[x] \in Nat /\ prom'[x] >= prom[x]
    BY <1>0, <1>1, <1>2, <1>7 DEF TypeOK
  <2>2. vote'[x] = vote[x] /\ (\A s \in Slots : learned'[x][s] = learned[x][s])
        /\ (x \in wiped' <=> x \in wiped)
    BY <1>1
  <2>3. \A c \in Ballots, s \in Slots, v \in Values : VotedFor(x, c, s, v)' = VotedFor(x, c, s, v)
    BY <1>6
  <2> QED
    BY <1>5, <2>1, <2>2, <2>3, NodeKeep
<1>11. BallotInv'
  <2> SUFFICES ASSUME NEW c \in Ballots PROVE BallotOK(c)'
    BY DEF BallotInv
  <2>1. \A n \in msgs' : n.type \in {"1a", "2a"} /\ n.bal = c => n \in msgs
    BY <1>4, <1>5
  <2>2. phase[c] \in {"p1", "p2"} => Owner[c] \notin wiped'
    BY <1>1 DEF Inv, BallotInv, BallotOK
  <2>3. phase'[c] = phase[c] /\ next'[c] = next[c] /\ front'[c] = front[c]
    BY <1>1
  <2>4. Owner[c] \in wiped' \/ (prom'[Owner[c]] \in Nat /\ prom'[Owner[c]] >= prom[Owner[c]])
    BY <1>7
  <2>5. wiped \subseteq wiped'
    BY <1>1
  <2> QED
    BY <1>5, <1>8, <2>1, <2>2, <2>3, <2>4, <2>5, BallotKeep
<1>12. MsgInv'
  <2> SUFFICES ASSUME NEW m \in msgs' PROVE MsgOK(m)'
    BY DEF MsgInv
  <2>1. \A n \in msgs : n.type = "1b" /\ n.acc = a => n.bal # b
    BY <1>0, <1>1, NoPromiseAbove
  <2>2. CASE m \in msgs
    <3>1. m.type = "1a" => \A n \in msgs' : n.type = "1a" /\ n.bal = m.bal => n \in msgs
      BY <1>4, <1>5
    <3>2. m.type = "1b" => /\ \A n \in msgs' : n.type = "1b" /\ n.bal = m.bal /\ n.acc = m.acc => n \in msgs
                           /\ \A s \in Slots, c \in Ballots, v \in Values :
                                VotedFor(m.acc, c, s, v)' /\ c < m.bal => VotedFor(m.acc, c, s, v)
      BY <2>1, <2>2, <1>4, <1>5, <1>6
    <3>3. m.type = "2a" => \A n \in msgs' : n.type = "2a" /\ n.bal = m.bal /\ n.slot = m.slot => n \in msgs
      BY <1>4, <1>5
    <3>4. \A x \in Acceptors : x \notin wiped' => /\ x \notin wiped
                                                 /\ prom'[x] \in Nat
                                                 /\ prom'[x] >= prom[x]
      BY <1>1, <1>7
    <3> QED
      BY <2>2, <3>1, <3>2, <3>3, <3>4, <1>5, <1>8, OldMsg
  <2>3. CASE m = m1b
    <3>0. m.type = "1b" /\ m.acc = a /\ m.bal = b /\ m.from = fr
      BY <2>3 DEF PromiseMsg
    <3>1. \A n \in msgs' : n.type = "1b" /\ n.bal = b /\ n.acc = a => n = m1b
      BY <2>1, <1>4, <1>5
    <3>2. \A s \in Slots : \A r : r = vote[a][s] => RecordOK(a, s, r, b)'
      <4>1. a \in Acceptors /\ a \notin wiped /\ prom[a] < b /\ msgs \subseteq msgs'
        BY <1>1, <1>5
      <4>2. \A c \in Ballots, s \in Slots, v \in Values : VotedFor(a, c, s, v)' = VotedFor(a, c, s, v)
        BY <1>6
      <4> QED
        BY <4>1, <4>2, PromiseOK
    <3>3. \A s \in Slots : IF s >= m.from THEN RecordOK(m.acc, s, m.rep[s], m.bal)' ELSE m.rep[s] = NoVote
      <4> SUFFICES ASSUME NEW s \in Slots
                   PROVE  IF s >= fr THEN RecordOK(a, s, m.rep[s], b)' ELSE m.rep[s] = NoVote
        BY <3>0
      <4>1. m.rep[s] = IF s >= fr THEN vote[a][s] ELSE NoVote
        BY <2>3, <1>4
      <4>2. CASE s >= fr
        <5>1. m.rep[s] = vote[a][s]
          BY <4>1, <4>2
        <5> QED
          BY <4>2, <5>1, <3>2
      <4>3. CASE ~(s >= fr)
        BY <4>1, <4>3
      <4> QED
        BY <4>2, <4>3
    <3>4. [type |-> "1a", bal |-> m.bal, from |-> m.from] \in msgs'
      BY <3>0, <1>3, <1>5
    <3>5. prom'[a] = b
      BY <1>0, <1>2 DEF TypeOK
    <3>6. \A n \in msgs' : n.type = "1b" /\ n.bal = m.bal /\ n.acc = m.acc => n = m
      BY <3>0, <3>1, <2>3
    <3> QED
      BY <3>0, <3>3, <3>4, <3>5, <3>6 DEF MsgOK
  <2> QED
    BY <2>2, <2>3, <1>5
<1> QED
  BY <1>1, <1>2, <1>9, <1>10, <1>11, <1>12 DEF Inv

=============================================================================
