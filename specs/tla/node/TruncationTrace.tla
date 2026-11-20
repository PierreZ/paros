-------------------------- MODULE TruncationTrace ---------------------------
(***************************************************************************)
(* The trace spec of the log, the journal fold, each compaction and each   *)
(* trim jump, on the `compaction` flavor (`truncation.rs`): `NodeTrace`,   *)
(* the binding of the walk's events to `Node`, with nothing of its own.    *)
(* The walk keeps the events of this spec (`Spec::keeps`).                 *)
(***************************************************************************)
EXTENDS NodeTrace
=============================================================================
