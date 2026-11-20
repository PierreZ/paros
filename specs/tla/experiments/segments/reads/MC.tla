--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. TLC computes  *)
(* each of them once.                                                      *)
EXTENDS SegmentReads, Grid, TLC

\* The smallest full grid: 2 x 1 (`QuorumSystem::grid(2, 1)`). Its one column
\* writes (both members), and each row is one member, so one member elects
\* and a reader asks one member. Two rows do not meet, as on every full grid.
\* Configuration 1 is {a1, a2}, configuration 2 is {a2, a3} (a3 replaces a1),
\* configuration 3 is {a3, a4} (a4 replaces a2).
SeqNarrow == <<<<"a1", "a2">>, <<"a2", "a3">>, <<"a3", "a4">>>>

\* The full grid of 2 x 2 (`QuorumSystem::grid(2, 2)`), as in `reads/`:
\* configuration 1 over a1..a4, configuration 2 where a5 replaces a1,
\* configuration 3 where a6 replaces a2. The layout is row-major over the
\* sorted members. Configuration 1 has the rows {a1, a2} and {a3, a4} and the
\* columns {a1, a3} and {a2, a4}; configuration 2 has the rows {a2, a3} and
\* {a4, a5} and the columns {a2, a4} and {a3, a5}.
SeqSquare == <<<<"a1", "a2", "a3", "a4">>,
               <<"a2", "a3", "a4", "a5">>,
               <<"a3", "a4", "a5", "a6">>>>

Layout(seq, cols) ==
  [cfg |-> [g \in 1..3 |-> GridMembers(seq[g])],
   q1 |-> [g \in 1..3 |-> GridQ1(seq[g], cols, 2, 0)],
   q2 |-> [g \in 1..3 |-> GridQ2(seq[g], cols, 2)],
   rows |-> [g \in 1..3 |-> GridReadTargets(seq[g], cols, 2)],
   cols |-> [g \in 1..3 |-> {GridColumn(seq[g], cols, j) : j \in 0..(cols - 1)}]]

Narrow == Layout(SeqNarrow, 1)
Square == Layout(SeqSquare, 2)

\* Both layouts are full grids (acks = rows = 2): a reader asks one row.
ASSUME \A g \in 1..3 :
  /\ GridAdmits(SeqNarrow[g], 1, 2) /\ GridRows(SeqNarrow[g], 1) = 2
  /\ GridAdmits(SeqSquare[g], 2, 2) /\ GridRows(SeqSquare[g], 2) = 2

NarrowCfg == Narrow.cfg
NarrowQ1 == Narrow.q1
NarrowQ2 == Narrow.q2
NarrowRows == Narrow.rows
NarrowCols == Narrow.cols
SquareCfg == Square.cfg
SquareQ1 == Square.q1
SquareQ2 == Square.q2
SquareRows == Square.rows
SquareCols == Square.cols
=============================================================================
