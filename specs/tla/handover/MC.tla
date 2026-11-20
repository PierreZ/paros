--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS Handover, TLC

\* The set of generation 0, and the targets that the coordinator can name:
\* replace X with W, then (when Y is down) replace X and Y with W and V, and
\* the resize to five members.
Old == {"X", "Y", "Z"}
Named == << {"Y", "Z", "W"}, {"Z", "W", "V"}, {"V", "W", "X", "Y", "Z"} >>

\* For the liveness checks: whichever machine is down, one target of three
\* live machines is left.
NamedLive == << {"Y", "Z", "W"}, {"Z", "W", "V"}, {"Y", "W", "V"},
                {"Y", "Z", "V"}, {"V", "W", "X", "Y", "Z"} >>
\* One target: the smallest handover, for the controls of the client side.
NamedOne == << {"Y", "Z", "W"} >>
=============================================================================
