--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write.               *)
EXTENDS TruncationReconfig, TLC

\* The acceptor names, model values that the configurations set.
CONSTANTS a1, a2, a3, a4, a5

\* The owners of the ballots: a1 leads under C1, a3 opens the
\* reconfiguration, and a2, a member of both configurations, campaigns
\* after it.
Own == [b \in Ballots |-> IF b = 1 THEN a1 ELSE IF b = 2 THEN a3 ELSE a2]
\* Two reconfigurations: a1 leads under C1, a3 opens C2, a4 opens C3.
OwnChain == [b \in Ballots |-> IF b = 1 THEN a1 ELSE IF b = 2 THEN a3 ELSE a4]
=============================================================================
