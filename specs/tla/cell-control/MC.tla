--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS CellControl, TLC

\* The cell control journal on i1..i3, one data set on i2..i4.
Mem2 == [s \in {"cj", "s"} |-> IF s = "cj" THEN {"i1", "i2", "i3"} ELSE {"i2", "i3", "i4"}]
\* Three machines: both sets on i1..i3, no spare.
Mem3 == [s \in {"cj", "s"} |-> {"i1", "i2", "i3"}]
=============================================================================
