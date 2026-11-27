------------------------------- MODULE MC -----------------------------------
(* The replicas and reads of the checks of `SegmentReseat.tla`.           *)
EXTENDS SegmentReseat, TLC

CONSTANTS a, b, c

\* Reads of slots 1 and 3 at the removed member a, of slot 1 at the new
\* member c, and of slot 3 at the member b that stays.
Reads == {<<a, 1>>, <<a, 3>>, <<c, 1>>, <<b, 3>>}
=============================================================================
