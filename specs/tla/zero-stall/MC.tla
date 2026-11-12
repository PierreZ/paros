--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS Bypass, TLC

\* paros: ballot (round, node). Ballot 2 of proposer 2 is between ballots 1 and 3.
OwnerParos == <<1, 2, 1, 2>>
\* The paper: round (r, id, s). A proposer owns two consecutive ballots.
OwnerPaper == <<1, 1, 2, 2>>

\* Three ballots: enough for one bypass (1 to 3) with a rival ballot (2) between.
OwnerParos3 == <<1, 2, 1>>
OwnerPaper3 == <<1, 1, 2>>

\* Two configurations with one common member (a reconfiguration that replaces
\* one member). A quorum of a configuration of two is the two members.
ConfigsOverlap == {{"a1", "a2"}, {"a2", "a3"}}
\* Two configurations with no common member (a journal move).
ConfigsDisjoint == {{"a1", "a2"}, {"a3", "a4"}}

\* Each ballot selects any configuration.
CfgAny == [Ballots -> Configs]
\* One journal move on the paper's ballots: the leader bypasses from ballot 1
\* to ballot 2 with the new members, and a rival leads at ballot 3. A fixed
\* move keeps the GC checks small.
CfgMovePaper3 == {<<{"a1", "a2"}, {"a3", "a4"}, {"a3", "a4"}>>}

Symmetry == Permutations(Values) \cup Permutations(Matchmakers)
=============================================================================
