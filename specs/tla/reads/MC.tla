--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS Reads, TLC

\* paros: ballot (round, node). Ballot 2 of proposer 2 is between ballots 1 and 3.
OwnerParos3 == <<1, 2, 1>>

\* Two ballots of two proposers, and two ballots of one proposer (a bypass).
OwnerTwo  == <<1, 2>>
OwnerSame == <<1, 1>>

\* A configuration of three members, then one of two with one common member.
\* A majority of the first can choose a slot without the common member.
ConfigsRead == {{"a1", "a2", "a3"}, {"a3", "a4"}}

\* One reader that is only in the old configuration, and the common member.
ReadersBoth == {"a1", "a3"}

\* The learner checks: ballot 3 is only the abandoned campaign of a reader.
OnlyOwn3 == status[3] \in {"matching", "abandoned"} /\ replied[3] = {}

Symmetry == Permutations(Values) \cup Permutations(Matchmakers)
=============================================================================
