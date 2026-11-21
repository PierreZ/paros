---------------------------- MODULE RetireTrace -----------------------------
(***************************************************************************)
(* The trace spec of `MatchmakerTrace` and the retirement and the rejoin   *)
(* of a node, on the `retiring` flavor (`retire.rs`): `NodeTrace`, the     *)
(* binding of the walk's events to `Node`, with nothing of its own. The    *)
(* walk keeps the events of this spec (`Spec::keeps`).                     *)
(***************************************************************************)
EXTENDS NodeTrace
=============================================================================
