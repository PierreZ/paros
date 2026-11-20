--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS Degraded, TLC

G(s, nc, na) == [seq |-> s, cols |-> nc, acks |-> na]

\* The journal grid for F = 1 and two columns: rows 3, cols 2, acks 2. The old
\* grid has the columns {a1, a3, a5} and {a2, a4, a6}; two crashes (a1, a3)
\* take the first column below quorum.
OldGrid == G(<<"a1", "a2", "a3", "a4", "a5", "a6">>, 2, 2)

\* A shrink to the column that is not below quorum: the majority of a2, a4
\* and a6 (rows 3, cols 1, acks 2). It needs no new acceptor.
LShrink == [c \in {"old", "new"} |->
              IF c = "old" THEN OldGrid ELSE G(<<"a2", "a4", "a6">>, 1, 2)]

\* A heal: a7 and a8 replace a1 and a3. The layout is row-major over the
\* sorted members, so the new grid has the columns {a2, a5, a7} and
\* {a4, a6, a8}: the survivors a2, a4 and a6 change columns.
LHeal == [c \in {"old", "new"} |->
            IF c = "old" THEN OldGrid
            ELSE G(<<"a2", "a4", "a5", "a6", "a7", "a8">>, 2, 2)]

\* A reshape to a majority of three: a5, the one survivor of the column that
\* is below quorum, a6 of the other column, and a new acceptor a7. A write
\* quorum of it ({a5, a7}) holds no acceptor of the healthy old column, so it
\* does not fence a read of the old grid.
LSwap == [c \in {"old", "new"} |->
            IF c = "old" THEN OldGrid ELSE G(<<"a5", "a6", "a7">>, 1, 2)]

CrashColumn == {"a1", "a3"}
\* The same, and one more crash in the other column: F down there.
CrashColumnPlus == {"a1", "a2", "a3"}

\* The smallest grid with columns (rows 1, cols 2, acks 1, so F = 0), for
\* the fast checks. The old grid has the columns {a1} and {a2}; a1 crashes.
\* The heal replaces a1 by a3: the new grid has the columns {a2} and {a3},
\* so the survivor a2 changes columns.
LSmall == [c \in {"old", "new"} |->
             IF c = "old" THEN G(<<"a1", "a2">>, 2, 1) ELSE G(<<"a2", "a3">>, 2, 1)]
CrashSmall == {"a1"}

\* A move between two majorities of three with no common member, for the
\* seal end.
LMove == [c \in {"old", "new"} |->
            IF c = "old" THEN G(<<"a1", "a2", "a3">>, 1, 2) ELSE G(<<"a4", "a5", "a6">>, 1, 2)]

\* A move between two majorities of three with one common member, all live:
\* the smallest case where a read on the old basis can see a vote that a
\* second read on the old basis does not see.
LMaj == [c \in {"old", "new"} |->
           IF c = "old" THEN G(<<"a1", "a2", "a3">>, 1, 2) ELSE G(<<"a3", "a4", "a5">>, 1, 2)]

\* One leader: ballot 1 in the old grid, its next ballot 2 in the new grid.
Owner2 == <<1, 1>>
Cfg2 == <<"old", "new">>
\* A rival leads at ballot 3 in the new grid after the bypass.
Owner3 == <<1, 1, 2>>
Cfg3 == <<"old", "new", "new">>

Symmetry == Permutations(Values) \cup Permutations(Matchmakers)
=============================================================================
