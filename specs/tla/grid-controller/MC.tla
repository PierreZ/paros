--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS GridController, TLC

\* The pool of four acceptors, the first grid on three of them.
AccPool == {"a1", "a2", "a3", "a4"}
\* Two member sets: the first grid, and the one that heals a1 out. Each with each Phase-2 size: the
\* controller may ask for any of them, the full grid (acks = rows) and
\* acks = 1 too, which break F + 1 <= acks <= rows - F for F = 1.
Mems == {{"a1", "a2", "a3"}, {"a2", "a3", "a4"}}
GridSet == {[mem |-> M, acks |-> k] : M \in Mems, k \in 1..3}
\* Only the grids that meet the bound: a smaller space for the safety checks
\* that do not need the bad grids.
GoodGrids == {[mem |-> M, acks |-> 2] : M \in Mems}
First == [mem |-> {"a1", "a2", "a3"}, acks |-> 2]
=============================================================================
