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
\* The controls of the bound for liveness: one grid outside the bound, on
\* a2..a4. acks = 1 needs all three rows for Phase 1; acks = 3 needs all
\* three rows for Phase 2. One failure blocks the seal into it (`One`, `All`)
\* or out of it (`One`, then `First` again).
One == {First, [mem |-> {"a2", "a3", "a4"}, acks |-> 1]}
All == {First, [mem |-> {"a2", "a3", "a4"}, acks |-> 3]}
=============================================================================
