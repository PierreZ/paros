--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS Decree, TLC

Maj(S) == {Q \in SUBSET S : 2 * Cardinality(Q) > Cardinality(S)}
Majorities == Maj(Acceptors)

Symmetry == Permutations(Values) \cup Permutations(Acceptors)

-----------------------------------------------------------------------------
(* The states of the bounded instance where `Inv` holds (#1307).           *)
(*                                                                         *)
(* TLC cannot enumerate `msgs \in SUBSET Msg` and keep the states where    *)
(* `Inv` holds: `Msg` has too many members. `IndInit` builds the same set  *)
(* of states from smaller choices, each one a fact that `Inv` forces:      *)
(*   - at most one value is proposed at each ballot (`MsgInv`, 2a);        *)
(*   - a vote is for the value proposed at its ballot (2b);                *)
(*   - an acceptor's vote is its highest vote (`AccInv`);                  *)
(*   - a promise reports the highest vote of its acceptor below the        *)
(*     promised ballot (1b: a vote, and no vote between);                  *)
(*   - a promise or a vote is at or below the acceptor's promise, unless   *)
(*     the acceptor is wiped.                                              *)
(* Every other choice is free, but for `SafeAt`, which the builder judges. *)
(* So `IndInit` is every state of the instance where `Inv` holds, and a    *)
(* check from `IndInit` with the invariant `Inv` checks that               *)
(* `Inv /\ Next => Inv'`.                                                  *)
(***************************************************************************)

Max(S) == IF S = {} THEN 0 ELSE CHOOSE x \in S : \A y \in S : y <= x

\* The facts of `Decree` judged on chosen messages, promises and wipes.
Pre(M, pr, W) == INSTANCE Decree WITH msgs <- M, prom <- pr, wiped <- W

\* The 2a and 2b messages of the proposals P and the votes V.
M2(P, V) ==
  {[type |-> "2a", bal |-> b, val |-> P[b]] : b \in {c \in Ballots : P[c] # None}}
    \cup {[type |-> "2b", acc |-> x[1], bal |-> x[2], val |-> P[x[2]]] : x \in V}

\* The highest ballot below b that acceptor a voted at in V, 0 if none.
Below(V, a, b) == Max({x[2] : x \in {y \in V : y[1] = a /\ y[2] < b}})

\* Each choice is a fact that `Inv` forces, and `SafeAt` is judged as soon
\* as the votes are known. The check's `INVARIANT Inv` judges every state
\* that `IndInit` builds, so a builder wider than `Inv` fails the check.
IndStates ==
  \E W \in {X \in SUBSET Acceptors : Cardinality(X) <= Wipes},
    pr \in [Acceptors -> 0..NB],
    P \in [Ballots -> Values \cup {None}] :
    /\ \A a \in W : pr[a] = 0
    \* The votes, as <<acceptor, ballot>>: each one at a proposed ballot.
    /\ \E V \in SUBSET {x \in Acceptors \X Ballots : P[x[2]] # None /\ (x[1] \in W \/ x[2] <= pr[x[1]])} :
         \* `= TRUE` makes TLC judge the test as a value: an `\E` inside a test
         \* of an initial predicate would be enumerated as a choice.
         /\ (\A b \in Ballots : P[b] # None => Pre(M2(P, V), pr, W)!SafeAt(b, P[b])) = TRUE
         \* The promises, as <<acceptor, ballot>>, and the 1a messages.
         /\ \E R \in SUBSET {x \in Acceptors \X Ballots : x[1] \in W \/ x[2] <= pr[x[1]]},
               A \in SUBSET Ballots :
              /\ {x[2] : x \in R} \subseteq A
              /\ wiped = W
              /\ prom = pr
              /\ vbal = [a \in Acceptors |-> IF a \in W THEN 0 ELSE Below(V, a, NB + 1)]
              /\ vval = [a \in Acceptors |-> IF vbal[a] = 0 THEN None ELSE P[vbal[a]]]
              /\ msgs = M2(P, V)
                   \cup {[type |-> "1a", bal |-> b] : b \in A}
                   \cup {[type |-> "1b", acc |-> x[1], bal |-> x[2], vbal |-> Below(V, x[1], x[2]),
                          vval |-> IF Below(V, x[1], x[2]) = 0 THEN None ELSE P[Below(V, x[1], x[2])]] :
                           x \in R}

IndInit ==
  /\ Assert(InvApplies, "Inv does not cover this configuration")
  /\ IndStates
=============================================================================
