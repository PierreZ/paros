------------------------- MODULE LeaderHandoffTrace -------------------------
(***************************************************************************)
(* The trace spec of the cooperative handoff (`node/handoff.rs`):          *)
(* `NodeTrace`, the binding of the walk's events to `Node`, with nothing   *)
(* of its own. Its walks keep the log, each step down, each handoff with   *)
(* its payload and each install (`Spec::keeps`).                           *)
(***************************************************************************)
EXTENDS NodeTrace
=============================================================================
