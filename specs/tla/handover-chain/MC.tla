--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS HandoverChain, TLC

\* Three generations: replace X with W, then Y with V. Z is in every set,
\* so it can skip generation 1.
Chain3 == << {"X", "Y", "Z"}, {"Y", "Z", "W"}, {"Z", "W", "V"} >>
\* Two generations, for the controls.
Chain2 == << {"X", "Y", "Z"}, {"Y", "Z", "W"} >>
=============================================================================
