------------------------ MODULE MatchmakerGridTrace -------------------------
(***************************************************************************)
(* The trace spec of `MatchmakerTrace` with each configuration a grid and  *)
(* the `Commit` of each proxy leader, on the steered `reconfiguring`       *)
(* flavor (`matchmaker.rs`): `NodeTrace`, the binding of the walk's events *)
(* to `Node`, with nothing of its own. The walk keeps the events of this   *)
(* spec (`Spec::keeps`).                                                   *)
(***************************************************************************)
EXTENDS NodeTrace
=============================================================================
