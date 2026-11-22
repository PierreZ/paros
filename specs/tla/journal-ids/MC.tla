--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS JournalIds, TLC

\* j1 and j3 belong to the cell tenant of cA, j2 to the user tenant u1, which
\* is placed in cA: j1 and j2 can draw the same JournalId in two tenants, and
\* j3 can draw the id that j1 had before its delete.
MCOwner == [j \in {"j1", "j2", "j3"} |-> IF j = "j2" THEN "u1" ELSE "cA"]
=============================================================================
