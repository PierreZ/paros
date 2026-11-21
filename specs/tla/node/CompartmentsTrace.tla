------------------------- MODULE CompartmentsTrace --------------------------
(***************************************************************************)
(* The trace spec of the log, the floors of the acceptors and the read     *)
(* replica (its learns, its walk, its floor, its boot and its leaderless   *)
(* read), on the flavors with read replicas (`compartments.rs`):           *)
(* `NodeTrace`, the binding of the walk's events to `Node`, with nothing   *)
(* of its own. The walk keeps the events of this spec (`Spec::keeps`).     *)
(***************************************************************************)
EXTENDS NodeTrace
=============================================================================
