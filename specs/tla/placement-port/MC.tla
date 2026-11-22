--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS PlacementPort, TLC

\* The cell's control journal on m1..m3, one journal on m3..m5: m3 holds both.
Mem2 == [j \in {"cell", "j1"} |-> IF j = "cell" THEN {"m1", "m2", "m3"} ELSE {"m3", "m4", "m5"}]
=============================================================================
