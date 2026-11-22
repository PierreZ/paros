----------------------------- MODULE DecreeProof -----------------------------
(***************************************************************************)
(* The TLAPS proof that `Inv` of `Decree.tla` is an inductive invariant    *)
(* and that it implies `Agreement` (#1307 (inductive invariants, then      *)
(* proofs), step 2). TLC checks the same facts on bounded instances        *)
(* (`inductive*.cfg`); this proof holds for every number of acceptors,     *)
(* values and ballots, and for every quorum system whose quorums           *)
(* intersect.                                                              *)
(*                                                                         *)
(* The proof covers the configurations of `InvApplies` with the proposer   *)
(* rule `Pick = "highest"`: no wipe, or a wiped acceptor that never takes  *)
(* a step again (`Refuse` and `Identity`). `Phase1` is free: "quorum" and  *)
(* "every" are both covered.                                               *)
(*                                                                         *)
(* Run: `nix develop .#tla --command scripts/check-proofs.sh`.             *)
(***************************************************************************)
EXTENDS Decree, FiniteSetTheorems, WellFoundedInduction, TLAPS

\* The configurations that the proof covers.
Hyp == InvApplies /\ Pick = "highest"

USE NBType, WipesType, NoneNotValue DEF Ballots

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

\* With no wipe allowed, no acceptor is wiped; so a wiped acceptor exists
\* only when it never takes a step again.
LEMMA WipedRule ==
  ASSUME TypeOK, Cardinality(wiped) <= Wipes, InvApplies, NEW a \in wiped
  PROVE  Refuse /\ Identity
<1>1. CASE Wipes = 0
  <2>1. Cardinality(wiped) = 0
    BY <1>1, WipedFinite
  <2> QED
    BY <2>1, WipedFinite, FS_EmptySet
<1> QED
  BY <1>1 DEF InvApplies

\* The fields of each kind of message.
LEMMA MsgFields ==
  ASSUME TypeOK, NEW m \in msgs
  PROVE  /\ m.type = "1a" => m \in [type : {"1a"}, bal : Ballots]
         /\ m.type = "1b" => m \in [type : {"1b"}, acc : Acceptors, bal : Ballots,
                                    vbal : 0..NB, vval : Values \cup {None}]
         /\ m.type = "2a" => m \in [type : {"2a"}, bal : Ballots, val : Values]
         /\ m.type = "2b" => m \in [type : {"2b"}, acc : Acceptors, bal : Ballots, val : Values]
         /\ m.type \in {"1a", "1b", "2a", "2b"}
BY DEF TypeOK, Msg

-----------------------------------------------------------------------------
(* The value that a proposer selects.                                      *)
(***************************************************************************)

\* With `Pick = "highest"`, the selected value is a value; either no
\* promise reports a vote, or it is the value of a highest reported vote.
LEMMA SelectableProps ==
  ASSUME Inv, Pick = "highest", NEW b \in Ballots,
         NEW S \in SUBSET {m \in msgs : m.type = "1b" /\ m.bal = b},
         NEW v \in Selectable(S)
  PROVE  /\ v \in Values
         /\ \/ \A m \in S : m.vbal = 0
            \/ \E m \in S : /\ m.vbal > 0
                            /\ m.vval = v
                            /\ \A n \in S : n.vbal <= m.vbal
<1> DEFINE rep == {m \in S : m.vbal > 0}
<1>0. \A m \in S : /\ m \in [type : {"1b"}, acc : Acceptors, bal : Ballots,
                             vbal : 0..NB, vval : Values \cup {None}]
                   /\ (m.vbal = 0) = (m.vval = None)
  BY MsgFields DEF Inv, MsgInv, MsgOK
<1>1. CASE rep = {}
  <2>1. v \in Values
    BY <1>1 DEF Selectable
  <2>2. \A m \in S : m.vbal = 0
    BY <1>0, <1>1
  <2> QED
    BY <2>1, <2>2
<1>2. CASE rep # {}
  <2>1. \E m \in rep : \A n \in rep : n.vbal <= m.vbal
    <3> DEFINE B == {m.vbal : m \in rep}
    <3>1. B \in SUBSET (0..NB) /\ B # {}
      BY <1>0, <1>2
    <3>2. PICK k \in B : \A j \in B : j <= k
      BY <3>1, MaxExists
    <3>3. PICK m \in rep : m.vbal = k
      OBVIOUS
    <3> QED
      BY <3>2, <3>3
  <2> DEFINE top == CHOOSE m \in rep : \A n \in rep : n.vbal <= m.vbal
  <2>2. top \in rep /\ \A n \in rep : n.vbal <= top.vbal
    BY <2>1
  <2>3. v = top.vval
    BY <1>2 DEF Selectable
  <2>4. top.vval \in Values
    BY <1>0, <2>2, NoneNotValue
  <2>5. \A n \in S : n.vbal <= top.vbal
    BY <1>0, <2>2
  <2> QED
    BY <2>2, <2>3, <2>4, <2>5
<1> QED
  BY <1>1, <1>2

\* A proposer that closes Phase 1 on the promises S selects a safe value.
LEMMA ProposeSafe ==
  ASSUME Inv, Hyp, NEW b \in Ballots,
         NEW S \in SUBSET {m \in msgs : m.type = "1b" /\ m.bal = b},
         Phase1Done({m.acc : m \in S}),
         NEW v \in Selectable(S)
  PROVE  SafeAt(b, v)
<1> USE DEF Inv, TypeOK
<1>0. \A m \in S : /\ m \in [type : {"1b"}, acc : Acceptors, bal : Ballots,
                             vbal : 0..NB, vval : Values \cup {None}]
                   /\ m.acc \notin wiped => m.bal <= prom[m.acc]
                   /\ m.vbal < m.bal
                   /\ m.vbal > 0 => VotedFor(m.acc, m.vbal, m.vval)
                   /\ \A c \in (m.vbal + 1)..(m.bal - 1), w \in Values : ~VotedFor(m.acc, c, w)
  BY MsgFields DEF MsgInv, MsgOK
<1>1. PICK Q \in Quorums : Q \subseteq {m.acc : m \in S}
  <2>1. CASE Phase1 = "every"
    <3>1. {m.acc : m \in S} = Acceptors
      BY <2>1 DEF Phase1Done
    <3>2. PICK Q \in Quorums : TRUE
      BY QuorumsType
    <3> QED
      BY <3>1, <3>2, QuorumsType
  <2>2. CASE Phase1 = "quorum"
    BY <2>2 DEF Phase1Done
  <2> QED
    BY <2>1, <2>2, Phase1Type
<1>2. Q \subseteq Acceptors
  BY QuorumsType
\* An acceptor of Q that did not vote at a ballot below b never will.
<1>3. ASSUME NEW a \in Q, NEW c \in 1..(b - 1), \A w \in Values : ~VotedFor(a, c, w)
      PROVE  WontVoteIn(a, c)
  <2>1. PICK m \in S : m.acc = a
    BY <1>1
  <2>2. CASE a \in wiped
    BY <2>2, <1>3, WipedRule DEF WontVoteIn, Hyp
  <2>3. CASE a \notin wiped
    <3>1. prom[a] >= b
      BY <2>1, <2>3, <1>0
    <3>2. prom[a] \in Nat
      BY <1>2
    <3> QED
      BY <3>1, <3>2, <1>3 DEF WontVoteIn, Ballots
  <2> QED
    BY <2>2, <2>3
\* An acceptor of Q votes at no ballot between its reported vote and b.
<1>4. ASSUME NEW a \in Q, NEW m \in S, m.acc = a, NEW c \in 1..(b - 1), c > m.vbal
      PROVE  WontVoteIn(a, c)
  <2>1. \A w \in Values : ~VotedFor(a, c, w)
    BY <1>4, <1>0
  <2> QED
    BY <2>1, <1>3
<1> SUFFICES ASSUME NEW c \in 1..(b - 1)
             PROVE  \E R \in Quorums : \A a \in R : VotedFor(a, c, v) \/ WontVoteIn(a, c)
  BY DEF SafeAt
<1>5. CASE \A m \in S : m.vbal = 0
  <2> SUFFICES ASSUME NEW a \in Q PROVE WontVoteIn(a, c)
    OBVIOUS
  <2>1. PICK m \in S : m.acc = a
    BY <1>1
  <2> QED
    BY <1>5, <1>4, <2>1
<1>6. CASE \E m \in S : /\ m.vbal > 0 /\ m.vval = v /\ \A n \in S : n.vbal <= m.vbal
  <2>1. PICK top \in S : /\ top.vbal > 0 /\ top.vval = v /\ \A n \in S : n.vbal <= top.vbal
    BY <1>6
  <2> DEFINE k == top.vbal
  <2>2. k \in Ballots /\ k < b
    BY <1>0, <2>1
  <2>3. VotedFor(top.acc, k, v)
    BY <1>0, <2>1
  <2>4. /\ [type |-> "2a", bal |-> k, val |-> v] \in msgs
        /\ SafeAt(k, v)
        /\ \A n \in msgs : n.type = "2a" /\ n.bal = k => n = [type |-> "2a", bal |-> k, val |-> v]
    <3>1. [type |-> "2a", bal |-> k, val |-> v] \in msgs
      BY <2>3 DEF VotedFor, MsgInv, MsgOK
    <3> QED
      BY <3>1 DEF MsgInv, MsgOK
  <2>5. CASE c > k
    <3> SUFFICES ASSUME NEW a \in Q PROVE WontVoteIn(a, c)
      OBVIOUS
    <3>1. PICK m \in S : m.acc = a
      BY <1>1
    <3>2. c > m.vbal
      BY <2>1, <2>5, <3>1, <1>0
    <3> QED
      BY <3>1, <3>2, <1>4
  <2>6. CASE c = k
    <3> SUFFICES ASSUME NEW a \in Q PROVE VotedFor(a, c, v) \/ WontVoteIn(a, c)
      OBVIOUS
    <3>1. CASE \E w \in Values : VotedFor(a, c, w)
      <4>1. PICK w \in Values : VotedFor(a, c, w)
        BY <3>1
      <4>2. [type |-> "2a", bal |-> k, val |-> w] \in msgs
        BY <4>1, <2>6 DEF VotedFor, MsgInv, MsgOK
      <4>3. w = v
        BY <4>2, <2>4
      <4> QED
        BY <4>1, <4>3
    <3>2. CASE \A w \in Values : ~VotedFor(a, c, w)
      BY <3>2, <1>3
    <3> QED
      BY <3>1, <3>2
  <2>7. CASE c < k
    <3>1. c \in 1..(k - 1)
      BY <2>2, <2>7
    <3> QED
      BY <2>4, <3>1 DEF SafeAt
  <2> QED
    BY <2>2, <2>5, <2>6, <2>7
<1> QED
  BY <1>5, <1>6, SelectableProps DEF Hyp

-----------------------------------------------------------------------------
(* Each step keeps `Inv`.                                                  *)
(***************************************************************************)

THEOREM InitInv == Init => Inv
<1> SUFFICES ASSUME Init PROVE Inv
  OBVIOUS
<1>1. TypeOK
  BY NBType DEF Init, TypeOK
<1>2. Cardinality(wiped) <= Wipes
  BY FS_EmptySet, WipesType DEF Init
<1>3. AccInv
  BY DEF Init, AccInv, AccOK, VotedFor
<1>4. MsgInv
  BY DEF Init, MsgInv, MsgOK
<1> QED
  BY <1>1, <1>2, <1>3, <1>4 DEF Inv

\* A vote or a promise to vote at no ballot stays true.
LEMMA VoteOrWontStable ==
  ASSUME Inv, Hyp, [Next]_vars, NEW a \in Acceptors, NEW c \in Ballots, NEW v \in Values,
         VotedFor(a, c, v) \/ WontVoteIn(a, c)
  PROVE  VotedFor(a, c, v)' \/ WontVoteIn(a, c)'
<1> USE DEF Inv, TypeOK
<1>0. msgs \subseteq msgs'
  BY DEF Next, vars, Prepare, Promise, Propose, Accept, Wipe, Send
<1>1. CASE VotedFor(a, c, v)
  BY <1>1, <1>0 DEF VotedFor
<1>2. CASE ~VotedFor(a, c, v) /\ WontVoteIn(a, c)
  <2>1. \A w \in Values : ~VotedFor(a, c, w)
    BY <1>2 DEF WontVoteIn
  <2>2. ASSUME NEW b \in Ballots, Prepare(b) PROVE WontVoteIn(a, c)'
    BY <2>2, <1>2 DEF Prepare, Send, WontVoteIn, VotedFor
  <2>3. ASSUME NEW b \in Ballots, Propose(b) PROVE WontVoteIn(a, c)'
    BY <2>3, <1>2 DEF Propose, Send, WontVoteIn, VotedFor
  <2>4. ASSUME NEW x \in Acceptors, NEW b \in Ballots, Promise(x, b) PROVE WontVoteIn(a, c)'
    <3>1. prom'[a] >= prom[a] /\ prom'[a] \in Nat
      BY <2>4 DEF Promise
    <3> QED
      BY <3>1, <2>4, <1>2 DEF Promise, Send, WontVoteIn, VotedFor
  <2>5. ASSUME NEW x \in Acceptors, NEW b \in Ballots, Accept(x, b) PROVE WontVoteIn(a, c)'
    <3>1. PICK m \in msgs : /\ m.type = "2a" /\ m.bal = b
                            /\ prom' = [prom EXCEPT ![x] = b]
                            /\ msgs' = msgs \cup {[type |-> "2b", acc |-> x, bal |-> b, val |-> m.val]}
      BY <2>5 DEF Accept, Send
    <3>2. x # a \/ c # b
      \* If x = a, the acceptor was promised at or below b and not wiped
      \* with Identity, so it was not a "won't vote" at b.
      <4> SUFFICES ASSUME x = a, c = b PROVE FALSE
        OBVIOUS
      <4>1. prom[a] <= b
        BY <2>5 DEF Accept
      <4>2. ~(Refuse /\ Identity /\ a \in wiped)
        BY <2>5 DEF Accept
      <4> QED
        BY <4>1, <4>2, <1>2 DEF WontVoteIn
    <3>3. \A w \in Values : ~VotedFor(a, c, w)'
      BY <3>1, <3>2, <2>1 DEF VotedFor
    <3>4. prom'[a] >= prom[a] /\ prom'[a] \in Nat
      BY <3>1, <2>5 DEF Accept
    <3> QED
      BY <3>3, <3>4, <2>5, <1>2 DEF Accept, WontVoteIn
  <2>6. ASSUME NEW x \in Acceptors, Wipe(x) PROVE WontVoteIn(a, c)'
    <3>1. Refuse /\ Identity
      BY <2>6, WipedFinite DEF Wipe, Hyp, InvApplies
    <3>2. CASE x = a
      BY <3>1, <3>2, <2>6, <2>1 DEF Wipe, WontVoteIn, VotedFor
    <3>3. CASE x # a
      BY <3>3, <2>6, <1>2 DEF Wipe, WontVoteIn, VotedFor
    <3> QED
      BY <3>2, <3>3
  <2>7. CASE UNCHANGED vars
    BY <2>7, <1>2 DEF vars, WontVoteIn, VotedFor
  <2> QED
    BY <2>2, <2>3, <2>4, <2>5, <2>6, <2>7 DEF Next
<1> QED
  BY <1>1, <1>2

LEMMA SafeAtStable ==
  ASSUME Inv, Hyp, [Next]_vars, NEW b \in Ballots, NEW v \in Values, SafeAt(b, v)
  PROVE  SafeAt(b, v)'
<1> SUFFICES ASSUME NEW c \in 1..(b - 1)
             PROVE  \E Q \in Quorums : \A a \in Q : VotedFor(a, c, v)' \/ WontVoteIn(a, c)'
  BY DEF SafeAt
<1>1. PICK Q \in Quorums : \A a \in Q : VotedFor(a, c, v) \/ WontVoteIn(a, c)
  BY DEF SafeAt
<1>2. c \in Ballots /\ Q \subseteq Acceptors
  BY QuorumsType DEF Ballots
<1> QED
  BY <1>1, <1>2, VoteOrWontStable

\* A step that sends no vote keeps every vote.
LEMMA NoNewVote ==
  ASSUME NEW m, m.type # "2b", msgs' = msgs \cup {m}
  PROVE  \A a, c, v : VotedFor(a, c, v)' = VotedFor(a, c, v)
BY DEF VotedFor

\* A message sent before the step stays true when the messages, the votes
\* and the promises of the acceptors that are not wiped only grow, no vote
\* lands in a range that a promise reports empty, and no second 2a message
\* is sent at a ballot.
LEMMA OldMsg ==
  ASSUME Inv, Hyp, [Next]_vars, NEW m \in msgs,
         msgs \subseteq msgs',
         \A x \in Acceptors : x \notin wiped' => /\ x \notin wiped
                                                /\ prom'[x] \in Nat
                                                /\ prom'[x] >= prom[x],
         m.type = "1b" => \A c \in (m.vbal + 1)..(m.bal - 1), v \in Values : ~VotedFor(m.acc, c, v)',
         m.type = "2a" => \A n \in msgs' : n.type = "2a" /\ n.bal = m.bal => n \in msgs
  PROVE  MsgOK(m)'
<1> USE DEF Inv, TypeOK
<1>0. MsgOK(m)
  BY DEF MsgInv
<1>1. CASE m.type = "1a"
  BY <1>1 DEF MsgOK
<1>2. CASE m.type = "1b"
  <2>1. m \in [type : {"1b"}, acc : Acceptors, bal : Ballots, vbal : 0..NB, vval : Values \cup {None}]
    BY <1>2, MsgFields
  <2>2. [type |-> "1a", bal |-> m.bal] \in msgs'
    BY <1>0, <1>2 DEF MsgOK
  <2>3. m.acc \notin wiped' => m.bal <= prom'[m.acc]
    BY <1>0, <1>2, <2>1 DEF MsgOK
  <2>4. m.vbal > 0 => VotedFor(m.acc, m.vbal, m.vval)'
    BY <1>0, <1>2 DEF MsgOK, VotedFor
  <2> QED
    BY <1>0, <1>2, <2>2, <2>3, <2>4 DEF MsgOK
<1>3. CASE m.type = "2a"
  <2>1. m \in [type : {"2a"}, bal : Ballots, val : Values]
    BY <1>3, MsgFields
  <2>2. SafeAt(m.bal, m.val)'
    BY <1>0, <1>3, <2>1, SafeAtStable DEF MsgOK
  <2>3. \A n \in msgs' : n.type = "2a" /\ n.bal = m.bal => n = m
    BY <1>0, <1>3 DEF MsgOK
  <2> QED
    BY <1>3, <2>2, <2>3 DEF MsgOK
<1>4. CASE m.type = "2b"
  <2>1. m \in [type : {"2b"}, acc : Acceptors, bal : Ballots, val : Values]
    BY <1>4, MsgFields
  <2>2. [type |-> "2a", bal |-> m.bal, val |-> m.val] \in msgs'
    BY <1>0, <1>4 DEF MsgOK
  <2>3. m.acc \notin wiped' => m.bal <= prom'[m.acc]
    BY <1>0, <1>4, <2>1 DEF MsgOK
  <2> QED
    BY <1>4, <2>2, <2>3 DEF MsgOK
<1> QED
  BY <1>1, <1>2, <1>3, <1>4, MsgFields

\* Each action keeps each conjunct of `Inv`.
THEOREM PrepareInv ==
  ASSUME Inv, Hyp, NEW b \in Ballots, Prepare(b)
  PROVE  Inv'
<1> USE DEF Inv, TypeOK
<1> DEFINE m1a == [type |-> "1a", bal |-> b]
<1>0. msgs' = msgs \cup {m1a} /\ UNCHANGED <<prom, vbal, vval, wiped>>
  BY DEF Prepare, Send
<1>1. \A a, c, v : VotedFor(a, c, v)' = VotedFor(a, c, v)
  BY <1>0, NoNewVote
<1>2. TypeOK'
  BY <1>0 DEF Msg
<1>3. AccInv'
  BY <1>0, <1>1 DEF AccInv, AccOK
<1>4. MsgInv'
  <2> SUFFICES ASSUME NEW m \in msgs' PROVE MsgOK(m)'
    BY DEF MsgInv, MsgOK
  <2>1. CASE m = m1a
    BY <2>1 DEF MsgOK
  <2>2. CASE m \in msgs
    <3>1. m.type = "1b" => \A c \in (m.vbal + 1)..(m.bal - 1), v \in Values : ~VotedFor(m.acc, c, v)'
      BY <2>2, <1>1 DEF MsgInv, MsgOK
    <3>2. \A x \in Acceptors : x \notin wiped' => /\ x \notin wiped
                                                  /\ prom'[x] \in Nat
                                                  /\ prom'[x] >= prom[x]
      BY <1>0
    <3>3. m.type = "2a" => \A n \in msgs' : n.type = "2a" /\ n.bal = m.bal => n \in msgs
      BY <1>0
    <3>4. [Next]_vars /\ msgs \subseteq msgs'
      BY <1>0 DEF Next
    <3> QED
      BY <2>2, <3>1, <3>2, <3>3, <3>4, OldMsg
  <2> QED
    BY <2>1, <2>2, <1>0
<1> QED
  BY <1>0, <1>2, <1>3, <1>4

THEOREM ProposeInv ==
  ASSUME Inv, Hyp, NEW b \in Ballots, Propose(b)
  PROVE  Inv'
<1> USE DEF Inv, TypeOK
<1>0. PICK S \in SUBSET {m \in msgs : m.type = "1b" /\ m.bal = b} :
        /\ Phase1Done({m.acc : m \in S})
        /\ \E v \in Selectable(S) : Send([type |-> "2a", bal |-> b, val |-> v])
  BY DEF Propose
<1>1. PICK v \in Selectable(S) : msgs' = msgs \cup {[type |-> "2a", bal |-> b, val |-> v]}
  BY <1>0 DEF Send
<1> DEFINE m2a == [type |-> "2a", bal |-> b, val |-> v]
<1>2. UNCHANGED <<prom, vbal, vval, wiped>> /\ ~\E m \in msgs : m.type = "2a" /\ m.bal = b
  BY DEF Propose
<1>3. \A a, c, w : VotedFor(a, c, w)' = VotedFor(a, c, w)
  BY <1>1, NoNewVote
<1>4. v \in Values /\ SafeAt(b, v)
  BY <1>0, SelectableProps, ProposeSafe DEF Hyp
<1>5. TypeOK'
  BY <1>1, <1>2, <1>4 DEF Msg
<1>6. AccInv'
  BY <1>2, <1>3 DEF AccInv, AccOK
<1>7. MsgInv'
  <2> SUFFICES ASSUME NEW m \in msgs' PROVE MsgOK(m)'
    BY DEF MsgInv, MsgOK
  <2>1. CASE m = m2a
    <3>1. SafeAt(b, v)'
      BY <1>4, SafeAtStable DEF Next
    <3>2. \A n \in msgs' : n.type = "2a" /\ n.bal = b => n = m2a
      BY <1>1, <1>2
    <3> QED
      BY <2>1, <3>1, <3>2 DEF MsgOK
  <2>2. CASE m \in msgs
    <3>1. m.type = "1b" => \A c \in (m.vbal + 1)..(m.bal - 1), w \in Values : ~VotedFor(m.acc, c, w)'
      BY <2>2, <1>3 DEF MsgInv, MsgOK
    <3>2. m.type = "2a" => m.bal # b
      BY <2>2, <1>2
    <3>3. m.type = "2a" => \A n \in msgs' : n.type = "2a" /\ n.bal = m.bal => n \in msgs
      BY <3>2, <1>1
    <3>4. \A x \in Acceptors : x \notin wiped' => /\ x \notin wiped
                                                  /\ prom'[x] \in Nat
                                                  /\ prom'[x] >= prom[x]
      BY <1>2
    <3>5. [Next]_vars /\ msgs \subseteq msgs'
      BY <1>1 DEF Next
    <3> QED
      BY <2>2, <3>1, <3>3, <3>4, <3>5, OldMsg
  <2> QED
    BY <2>1, <2>2, <1>1
<1> QED
  BY <1>2, <1>5, <1>6, <1>7

THEOREM PromiseInv ==
  ASSUME Inv, Hyp, NEW a \in Acceptors, NEW b \in Ballots, Promise(a, b)
  PROVE  Inv'
<1> USE DEF Inv, TypeOK
<1> DEFINE m1b == [type |-> "1b", acc |-> a, bal |-> b, vbal |-> vbal[a], vval |-> vval[a]]
<1>0. /\ msgs' = msgs \cup {m1b}
      /\ prom' = [prom EXCEPT ![a] = b]
      /\ UNCHANGED <<vbal, vval, wiped>>
      /\ [type |-> "1a", bal |-> b] \in msgs
      /\ prom[a] < b
  BY DEF Promise, Send
<1>1. a \notin wiped
  BY WipedRule, <1>0 DEF Promise, Hyp
<1>2. \A x, c, v : VotedFor(x, c, v)' = VotedFor(x, c, v)
  BY <1>0, NoNewVote
<1>3. TypeOK'
  BY <1>0 DEF Msg, Ballots
<1>4. AccInv'
  BY <1>0, <1>1, <1>2 DEF AccInv, AccOK
<1>5. MsgInv'
  <2> SUFFICES ASSUME NEW m \in msgs' PROVE MsgOK(m)'
    BY DEF MsgInv, MsgOK
  <2>1. CASE m = m1b
    <3>1. vbal[a] <= prom[a] /\ (vbal[a] = 0) = (vval[a] = None)
          /\ (vbal[a] > 0 => VotedFor(a, vbal[a], vval[a]))
          /\ \A c \in (vbal[a] + 1)..NB, v \in Values : ~VotedFor(a, c, v)
      BY <1>1 DEF AccInv, AccOK
    <3> QED
      BY <2>1, <3>1, <1>0, <1>2 DEF Ballots, MsgOK
  <2>2. CASE m \in msgs
    <3>1. m.type = "1b" => \A c \in (m.vbal + 1)..(m.bal - 1), v \in Values : ~VotedFor(m.acc, c, v)'
      BY <2>2, <1>2 DEF MsgInv, MsgOK
    <3>2. \A x \in Acceptors : x \notin wiped' => /\ x \notin wiped
                                                  /\ prom'[x] \in Nat
                                                  /\ prom'[x] >= prom[x]
      BY <1>0
    <3>3. m.type = "2a" => \A n \in msgs' : n.type = "2a" /\ n.bal = m.bal => n \in msgs
      BY <1>0
    <3>4. [Next]_vars /\ msgs \subseteq msgs'
      BY <1>0 DEF Next
    <3> QED
      BY <2>2, <3>1, <3>2, <3>3, <3>4, OldMsg
  <2> QED
    BY <2>1, <2>2, <1>0
<1> QED
  BY <1>0, <1>3, <1>4, <1>5

THEOREM AcceptInv ==
  ASSUME Inv, Hyp, NEW a \in Acceptors, NEW b \in Ballots, Accept(a, b)
  PROVE  Inv'
<1> USE DEF Inv, TypeOK
<1>0. PICK m2 \in msgs :
        /\ m2.type = "2a" /\ m2.bal = b
        /\ prom' = [prom EXCEPT ![a] = b]
        /\ vbal' = [vbal EXCEPT ![a] = b]
        /\ vval' = [vval EXCEPT ![a] = m2.val]
        /\ msgs' = msgs \cup {[type |-> "2b", acc |-> a, bal |-> b, val |-> m2.val]}
  BY DEF Accept, Send
<1> DEFINE m2b == [type |-> "2b", acc |-> a, bal |-> b, val |-> m2.val]
<1>1. UNCHANGED wiped /\ prom[a] <= b
  BY DEF Accept
<1>2. a \notin wiped
  BY WipedRule DEF Accept, Hyp
<1>3. m2.val \in Values /\ m2 = [type |-> "2a", bal |-> b, val |-> m2.val]
  BY <1>0, MsgFields
<1>4. \A x, c, v : VotedFor(x, c, v)' = (VotedFor(x, c, v) \/ (x = a /\ c = b /\ v = m2.val))
  BY <1>0 DEF VotedFor
\* The old votes of a are at or below its promise, so at or below b.
<1>5. \A c \in Ballots, v \in Values : VotedFor(a, c, v) => c <= b
  <2> SUFFICES ASSUME NEW c \in Ballots, NEW v \in Values, VotedFor(a, c, v), c > b
               PROVE  FALSE
    BY DEF Ballots
  <2>1. vbal[a] <= prom[a] /\ \A d \in (vbal[a] + 1)..NB, w \in Values : ~VotedFor(a, d, w)
    BY <1>2 DEF AccInv, AccOK
  <2> QED
    BY <2>1, <1>1 DEF Ballots
<1>6. TypeOK'
  BY <1>0, <1>1, <1>3 DEF Msg, Ballots
<1>7. AccInv'
  <2> SUFFICES ASSUME NEW x \in Acceptors PROVE AccOK(x)'
    BY DEF AccInv, AccOK
  <2>1. CASE x = a
    <3>1. a \notin wiped'
      BY <1>1, <1>2
    <3>2. vbal'[a] = b /\ prom'[a] = b /\ vval'[a] = m2.val
      BY <1>0
    <3>3. VotedFor(a, b, m2.val)'
      BY <1>4, <1>3
    <3>4. \A c \in (b + 1)..NB, v \in Values : ~VotedFor(a, c, v)'
      <4> SUFFICES ASSUME NEW c \in (b + 1)..NB, NEW v \in Values, VotedFor(a, c, v)'
                   PROVE  FALSE
        OBVIOUS
      <4>1. VotedFor(a, c, v)
        BY <1>0 DEF VotedFor
      <4> QED
        BY <4>1, <1>5
    <3>5. (b = 0) = (m2.val = None)
      BY <1>3, NoneNotValue
    <3> QED
      BY <2>1, <3>1, <3>2, <3>3, <3>4, <3>5 DEF AccOK
  <2>2. CASE x # a
    BY <2>2, <1>0, <1>1, <1>4 DEF AccInv, AccOK
  <2> QED
    BY <2>1, <2>2
<1>8. MsgInv'
  <2> SUFFICES ASSUME NEW m \in msgs' PROVE MsgOK(m)'
    BY DEF MsgInv, MsgOK
  <2>1. CASE m = m2b
    BY <2>1, <1>0, <1>3 DEF MsgOK
  <2>2. CASE m \in msgs
    <3>1. \A x \in Acceptors : x \notin wiped' => /\ x \notin wiped
                                                  /\ prom'[x] \in Nat
                                                  /\ prom'[x] >= prom[x]
      BY <1>0, <1>1
    \* A promise of a is at or below b, so the new vote at b is not in the
    \* range that the promise reports empty.
    <3>2. ASSUME m.type = "1b"
          PROVE  \A c \in (m.vbal + 1)..(m.bal - 1), v \in Values : ~VotedFor(m.acc, c, v)'
      <4>1. m \in [type : {"1b"}, acc : Acceptors, bal : Ballots, vbal : 0..NB, vval : Values \cup {None}]
        BY <3>2, <2>2, MsgFields
      <4>0. MsgOK(m)
        BY <2>2 DEF MsgInv
      <4>2. \A c \in (m.vbal + 1)..(m.bal - 1), v \in Values : ~VotedFor(m.acc, c, v)
        BY <3>2, <4>0 DEF MsgOK
      <4>3. m.acc = a => m.bal <= b
        BY <3>2, <4>0, <1>1, <1>2, <4>1 DEF MsgOK
      <4> SUFFICES ASSUME NEW c \in (m.vbal + 1)..(m.bal - 1), NEW v \in Values,
                          VotedFor(m.acc, c, v)'
                   PROVE  FALSE
        OBVIOUS
      <4>4. VotedFor(m.acc, c, v) \/ (m.acc = a /\ c = b)
        BY <1>0 DEF VotedFor
      <4> QED
        BY <4>1, <4>2, <4>3, <4>4
    <3>3. m.type = "2a" => \A n \in msgs' : n.type = "2a" /\ n.bal = m.bal => n \in msgs
      BY <1>0
    <3>4. [Next]_vars /\ msgs \subseteq msgs'
      BY <1>0 DEF Next
    <3> QED
      BY <2>2, <3>1, <3>2, <3>3, <3>4, OldMsg
  <2> QED
    BY <2>1, <2>2, <1>0
<1> QED
  BY <1>1, <1>6, <1>7, <1>8

THEOREM WipeInv ==
  ASSUME Inv, Hyp, NEW a \in Acceptors, Wipe(a)
  PROVE  Inv'
<1> USE DEF Inv, TypeOK
<1>0. /\ Cardinality(wiped) < Wipes
      /\ wiped' = wiped \cup {a}
      /\ prom' = [prom EXCEPT ![a] = 0]
      /\ vbal' = [vbal EXCEPT ![a] = 0]
      /\ vval' = [vval EXCEPT ![a] = None]
      /\ UNCHANGED msgs
  BY DEF Wipe
<1>1. TypeOK'
  BY <1>0
<1>2. Cardinality(wiped') <= Wipes
  <2>1. IsFiniteSet(wiped) /\ Cardinality(wiped) \in Nat
    BY WipedFinite
  \* A wipe of an acceptor that is already wiped leaves the set as it is.
  <2>2. CASE a \in wiped
    BY <2>2, <1>0
  <2>3. CASE a \notin wiped
    <3>1. Cardinality(wiped \cup {a}) = Cardinality(wiped) + 1
      BY <2>1, <2>3, FS_AddElement
    <3> QED
      BY <2>1, <3>1, <1>0
  <2> QED
    BY <2>2, <2>3
<1>3. AccInv'
  BY <1>0 DEF AccInv, AccOK, VotedFor
<1>4. MsgInv'
  <2> SUFFICES ASSUME NEW m \in msgs' PROVE MsgOK(m)'
    BY DEF MsgInv, MsgOK
  <2>1. \A x \in Acceptors : x \notin wiped' => /\ x \notin wiped
                                                /\ prom'[x] \in Nat
                                                /\ prom'[x] >= prom[x]
    BY <1>0
  <2>2. m.type = "1b" => \A c \in (m.vbal + 1)..(m.bal - 1), v \in Values : ~VotedFor(m.acc, c, v)'
    BY <1>0 DEF MsgInv, MsgOK, VotedFor
  <2>3. m.type = "2a" => \A n \in msgs' : n.type = "2a" /\ n.bal = m.bal => n \in msgs
    BY <1>0
  <2>4. [Next]_vars /\ msgs \subseteq msgs' /\ m \in msgs
    BY <1>0 DEF Next
  <2> QED
    BY <2>1, <2>2, <2>3, <2>4, OldMsg
<1> QED
  BY <1>1, <1>2, <1>3, <1>4

THEOREM StepInv ==
  ASSUME Inv, Hyp, [Next]_vars
  PROVE  Inv'
<1>1. CASE UNCHANGED vars
  BY <1>1 DEF vars, Inv, TypeOK, AccInv, AccOK, MsgInv, MsgOK, SafeAt, WontVoteIn, VotedFor
<1> QED
  BY <1>1, PrepareInv, ProposeInv, PromiseInv, AcceptInv, WipeInv DEF Next

-----------------------------------------------------------------------------
(* `Inv` implies `Agreement`.                                              *)
(***************************************************************************)

LEMMA ChosenOrdered ==
  ASSUME Inv, NEW v \in Values, NEW w \in Values, NEW b1 \in Ballots, NEW b2 \in Ballots,
         b1 <= b2, ChosenAt(b1, v), ChosenAt(b2, w)
  PROVE  v = w
<1> USE DEF Inv, TypeOK
<1>0. \A x \in Acceptors, c \in Ballots, u \in Values :
        VotedFor(x, c, u) => [type |-> "2a", bal |-> c, val |-> u] \in msgs
  BY DEF MsgInv, MsgOK, VotedFor
<1>1. \A c \in Ballots, u1, u2 \in Values :
        [type |-> "2a", bal |-> c, val |-> u1] \in msgs
        /\ [type |-> "2a", bal |-> c, val |-> u2] \in msgs => u1 = u2
  BY DEF MsgInv, MsgOK
<1>2. PICK Q1 \in Quorums : \A x \in Q1 : VotedFor(x, b1, v)
  BY DEF ChosenAt, VotedFor
<1>3. PICK Q2 \in Quorums : \A x \in Q2 : VotedFor(x, b2, w)
  BY DEF ChosenAt, VotedFor
<1>4. Q1 \subseteq Acceptors /\ Q2 \subseteq Acceptors
  BY QuorumsType
<1>5. CASE b1 = b2
  <2>1. PICK x \in Q1 \cap Q2 : TRUE
    BY QuorumsIntersect
  <2> QED
    BY <2>1, <1>0, <1>1, <1>2, <1>3, <1>4, <1>5
<1>6. CASE b1 < b2
  <2>1. PICK y \in Q2 : TRUE
    BY QuorumsIntersect
  <2>2. SafeAt(b2, w)
    <3>1. [type |-> "2a", bal |-> b2, val |-> w] \in msgs
      BY <2>1, <1>0, <1>3, <1>4
    <3>2. MsgOK([type |-> "2a", bal |-> b2, val |-> w])
      BY <3>1 DEF MsgInv
    <3> QED
      BY <3>2 DEF MsgOK
  <2>3. PICK R \in Quorums : \A x \in R : VotedFor(x, b1, w) \/ WontVoteIn(x, b1)
    BY <2>2, <1>6 DEF SafeAt, Ballots
  <2>4. PICK x \in R \cap Q1 : TRUE
    BY QuorumsIntersect
  <2>5. VotedFor(x, b1, w)
    BY <2>3, <2>4, <1>2 DEF WontVoteIn
  <2> QED
    BY <2>4, <2>5, <1>0, <1>1, <1>2, QuorumsType
<1> QED
  BY <1>5, <1>6 DEF Ballots

THEOREM InvAgreement == Inv => Agreement
<1> SUFFICES ASSUME Inv, NEW v \in Values, NEW w \in Values, Chosen(v), Chosen(w)
             PROVE  v = w
  BY DEF Agreement
<1>1. PICK b1 \in Ballots : ChosenAt(b1, v)
  BY DEF Chosen
<1>2. PICK b2 \in Ballots : ChosenAt(b2, w)
  BY DEF Chosen
<1> QED
  BY <1>1, <1>2, ChosenOrdered DEF Ballots
-----------------------------------------------------------------------------
(* The theorem. `InitInv` and `StepInv` are the two premises of the TLA   *)
(* proof rule INV1 (Lamport, "The Temporal Logic of Actions", 1994):      *)
(*   Init => Inv,  Inv /\ [Next]_vars => Inv'  |-  Init /\ [][Next]_vars => []Inv. *)
(* TLAPS checks a use of that rule with its PTL backend, LS4, which the   *)
(* TLAPS of nixpkgs does not ship. So the last step is the one step that  *)
(* TLAPS does not check here: it is the rule itself, applied as written.  *)
(***************************************************************************)

THEOREM Safety == Hyp => (Spec => []Agreement)
<1>1. Hyp /\ Init => Inv
  BY InitInv
<1>2. Hyp /\ Inv /\ [Next]_vars => Inv'
  BY StepInv
<1>3. Inv => Agreement
  BY InvAgreement
<1> QED
  PROOF OMITTED
=============================================================================
