--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS HandoverGc, TLC

\* The handover replaces X with W.
Old == {"X", "Y", "Z"}
New == {"Y", "Z", "W"}
=============================================================================
