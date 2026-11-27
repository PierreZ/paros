--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write, over          *)
(* `PeerPull.tla`: the replicas of the set, as a sequence.                 *)
EXTENDS PeerPull, TLC

CONSTANTS r1, r2, n0, n1, lag

\* One replica peer, r1.
One == <<r1>>

\* Two replica peers: r1 first, then r2.
Two == <<r1, r2>>
=============================================================================
