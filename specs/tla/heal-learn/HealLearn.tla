------------------------------ MODULE HealLearn -------------------------------
(***************************************************************************)
(* The heal of a sealed segment, learned from the chain (the liveness Open *)
(* of R8 of `docs/architecture/04-7-segments.md`). The journal controller *)
(* orders the heal; the leader node runs its `Decree` at the matchmakers   *)
(* (the recommendation of R8). The `Decree` may be decided after the node  *)
(* that ran it lost its leadership, and the fold of the log does not see   *)
(* it. The tally of the leader must judge the segment again over the      *)
(* healed set (`held-reseat/`), and the new replica must learn that the    *)
(* set names it, to pull the segment from a peer (`peer-pull/`) and        *)
(* report it whole.                                                        *)
(*                                                                         *)
(* The chain is abstract: `decree` is the state of the one heal at the     *)
(* matchmakers. A learner holds what it learned in `knows`; a leader       *)
(* forgets it when it loses its leadership (the tally is volatile). The    *)
(* new replica pulls and reports once it knows; the tally judges once the  *)
(* leader knows and the report came.                                       *)
(*                                                                         *)
(* Constants (the rule with two controls):                                 *)
(*   Learn  "beat": the leader and the new replica read the heal from the  *)
(*          chain on each beat (the rule). "own": a control, only the     *)
(*          node that ran the `Decree` learns it, from its decision (the   *)
(*          new replica still reads the chain).                            *)
(*          "election": a control, a leader reads the chain only when it   *)
(*          wins, and a replica only at its boot.                          *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS
  Nodes,      \* the nodes that can lead
  NewRep,     \* the replica that the heal adds
  Elections,  \* how many changes of leader a behaviour may have
  Learn

ASSUME NewRep \notin Nodes /\ Elections \in Nat
ASSUME Learn \in {"beat", "own", "election"}

Learners == Nodes \cup {NewRep}

VARIABLES
  leader,     \* the node that leads
  decree,     \* the heal at the matchmakers: "none", "proposed", "chosen"
  ran,        \* the node that ran the `Decree`, or NewRep for none
  knows,      \* the learners that know the decided heal
  booted,     \* the new replica booted (it reads the chain then)
  whole,      \* the new replica pulled the segment and reported it
  judged,     \* the tally judged the segment over the healed set
  elections

vars == <<leader, decree, ran, knows, booted, whole, judged, elections>>

TypeOK ==
  /\ leader \in Nodes
  /\ decree \in {"none", "proposed", "chosen"}
  /\ ran \in Learners
  /\ knows \subseteq Learners
  /\ booted \in BOOLEAN
  /\ whole \in BOOLEAN
  /\ judged \in BOOLEAN
  /\ elections \in 0..Elections

Init ==
  /\ leader \in Nodes
  /\ decree = "none"
  /\ ran = NewRep
  /\ knows = {}
  /\ booted = FALSE
  /\ whole = FALSE
  /\ judged = FALSE
  /\ elections = 0

\* The controller's order reaches the leader, which runs the `Decree`.
Run ==
  /\ decree = "none"
  /\ decree' = "proposed"
  /\ ran' = leader
  /\ UNCHANGED <<leader, knows, booted, whole, judged, elections>>

\* The matchmakers decide the `Decree`. The node that ran it learns it if
\* it still leads.
Decide ==
  /\ decree = "proposed"
  /\ decree' = "chosen"
  /\ knows' = IF ran = leader THEN knows \cup {leader} ELSE knows
  /\ UNCHANGED <<leader, ran, booted, whole, judged, elections>>

\* A node wins the leadership (another node, or the leader after a
\* restart); its tally starts empty. Under "election" it reads the chain
\* as it wins.
Elect(n) ==
  /\ elections < Elections
  /\ leader' = n
  /\ knows' = (knows \ Nodes)
              \cup (IF Learn = "election" /\ decree = "chosen" THEN {n} ELSE {})
  /\ judged' = FALSE
  /\ elections' = elections + 1
  /\ UNCHANGED <<decree, ran, booted, whole>>

\* The new replica boots, or restarts; under "election" it reads the
\* chain then.
Boot ==
  /\ booted' = TRUE
  /\ knows' = IF Learn = "election" /\ decree = "chosen" THEN knows \cup {NewRep} ELSE knows
  /\ UNCHANGED <<leader, decree, ran, whole, judged, elections>>

\* A beat: the leader, or the booted new replica, reads the heal from the
\* chain.
Read(x) ==
  /\ decree = "chosen"
  /\ \/ x = leader /\ Learn = "beat"
     \/ x = NewRep /\ booted /\ Learn # "election"
  /\ knows' = knows \cup {x}
  /\ UNCHANGED <<leader, decree, ran, booted, whole, judged, elections>>

\* The new replica, which knows the set names it, pulls the segment from a
\* peer and reports it whole.
Pull ==
  /\ NewRep \in knows
  /\ whole' = TRUE
  /\ UNCHANGED <<leader, decree, ran, knows, booted, judged, elections>>

\* The tally of the leader, which knows the heal, judges the segment again
\* over the healed set.
Judge ==
  /\ leader \in knows
  /\ whole
  /\ judged' = TRUE
  /\ UNCHANGED <<leader, decree, ran, knows, booted, whole, elections>>

Next ==
  \/ Run \/ Decide \/ Boot \/ Pull \/ Judge
  \/ \E n \in Nodes : Elect(n)
  \/ \E x \in Learners : Read(x)

Spec == Init /\ [][Next]_vars
        /\ WF_vars(Run) /\ WF_vars(Decide) /\ WF_vars(Boot)
        /\ WF_vars(Pull) /\ WF_vars(Judge)
        /\ \A x \in Learners : WF_vars(Read(x))

\* Sound: a heal at the matchmakers was run by a node, only a decided heal
\* is known, the new replica pulls only once it
\* knows the set names it, and the tally judges the healed set only while
\* its leader knows the heal and the new replica is whole.
Sound ==
  /\ decree # "none" => ran \in Nodes
  /\ knows # {} => decree = "chosen"
  /\ whole => NewRep \in knows
  /\ judged => leader \in knows /\ whole

\* Heals: the tally of the leader judges the segment over the healed set.
Heals == <>[]judged
=============================================================================
