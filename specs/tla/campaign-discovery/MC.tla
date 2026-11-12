--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write. *)
EXTENDS CampaignDiscovery, TLC

\* The witness of #1034: the birth set {A}, handed over to {C}, then to
\* {C, B}; A then leaves for good.
Witness == <<{"A"}, {"C"}, {"B", "C"}>>

\* Four sets of three, each handover replacing one member, so that the
\* last set shares no member with the first.
Wide == <<{"X", "Y", "Z"}, {"Y", "Z", "W"}, {"Z", "W", "V"}, {"W", "V", "U"}>>
=============================================================================
