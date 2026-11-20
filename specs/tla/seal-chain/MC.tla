--------------------------------- MODULE MC ---------------------------------
(* The constants that a TLC configuration file cannot write.               *)
EXTENDS SealChain, TLC

CONSTANTS u1, u2

\* A write and a `Truncate` of u1, and the `SetLeader` that moves the leader
\* from u1 to u2. After the `SetLeader`, a write or a `Truncate` of u1 is
\* the deposed writer.
Values == {[op |-> "w", u |-> u1],
           [op |-> "tr", u |-> u1],
           [op |-> "sl", new |-> u2, old |-> u1]}

\* The liveness checks need no fold: two values keep the state space small.
Few == {[op |-> "w", u |-> u1],
        [op |-> "sl", new |-> u2, old |-> u1]}
=============================================================================
