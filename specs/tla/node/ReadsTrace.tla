----------------------------- MODULE ReadsTrace -----------------------------
(***************************************************************************)
(* The trace spec of `MatchmakerTrace` and the leaderless reads of a node  *)
(* (the read basis, the answers and their fence, the fold of an answer,    *)
(* the served index), on the lagging-reads flavor (`reads.rs`):            *)
(* `NodeTrace`, the binding of the walk's events to `Node`, with nothing   *)
(* of its own. The walk keeps the events of this spec (`Spec::keeps`).     *)
(***************************************************************************)
EXTENDS NodeTrace
=============================================================================
