--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS HandoverScalars, TLC

\* The handover replaces X with W.
Old == {"X", "Y", "Z"}
New == {"Y", "Z", "W"}
\* The one node that retires and rejoins.
OneNode == {"n"}
NoNode == {}
ReconfOne == {1}
NoReconf == {}
=============================================================================
