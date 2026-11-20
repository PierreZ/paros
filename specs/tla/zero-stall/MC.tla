--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS Bypass, TLC

\* paros: ballot (round, node), four ballots. Both proposers can bypass:
\* proposer 1 from 1 to 3, proposer 2 from 2 to 4.
OwnerParos == <<1, 2, 1, 2>>

\* Three ballots: enough for one bypass (1 to 3) with a rival ballot (2) between.
OwnerParos3 == <<1, 2, 1>>
OwnerPaper3 == <<1, 1, 2>>

\* Four ballots and three proposers: ballots 2 and 3 of two rivals are between
\* ballots 1 and 4 of proposer 1. A rival can collect at 3 the registration of
\* 2 (the watermark half of the bypass check).
OwnerParos4 == <<1, 2, 3, 1>>
\* Four ballots, two bypasses in a row: proposer 1 owns 1, 2 and 3; a rival
\* leads at 4.
OwnerChain4 == <<1, 1, 1, 2>>
\* Three ballots, two bypasses in a row, no rival: small enough for CI.
OwnerChain3 == <<1, 1, 1>>

\* Two configurations with one common member (a reconfiguration that replaces
\* one member). A quorum of a configuration of two is the two members.
ConfigsOverlap == {{"a1", "a2"}, {"a2", "a3"}}
\* Two configurations with no common member (a journal move).
ConfigsDisjoint == {{"a1", "a2"}, {"a3", "a4"}}

\* Two configurations of one acceptor: the smallest acceptor side, for the
\* checks of the matchmaker side alone.
ConfigsSingle == {{"a1"}, {"a2"}}

\* Each ballot selects any configuration.
CfgAny == [Ballots -> Configs]
\* One journal move on the paper's ballots: the leader bypasses from ballot 1
\* to ballot 2 with the new members, and a rival leads at ballot 3. A fixed
\* move keeps the GC checks small.
CfgMovePaper3 == {<<{"a1", "a2"}, {"a3", "a4"}, {"a3", "a4"}>>}
\* Two moves in a row, then a rival at the last configuration.
\* The watermark checks: a few fixed moves on four ballots, with the rivals
\* on the new or the old members.
CfgWm4 == {<<{"a1", "a2"}, {"a3", "a4"}, {"a1", "a2"}, {"a3", "a4"}>>,
           <<{"a1", "a2"}, {"a3", "a4"}, {"a3", "a4"}, {"a1", "a2"}>>,
           <<{"a1", "a2"}, {"a1", "a2"}, {"a3", "a4"}, {"a3", "a4"}>>}
\* The same on three ballots: a rival between, on the old or the new members.
CfgWm3 == {<<{"a1", "a2"}, {"a3", "a4"}, {"a3", "a4"}>>,
           <<{"a1", "a2"}, {"a1", "a2"}, {"a3", "a4"}>>}
CfgChain3 == {<<{"a1", "a2"}, {"a3", "a4"}, {"a1", "a2"}>>}
CfgChain4 == {<<{"a1", "a2"}, {"a3", "a4"}, {"a1", "a2"}, {"a1", "a2"}>>}

Symmetry == Permutations(Values) \cup Permutations(Matchmakers)
=============================================================================
