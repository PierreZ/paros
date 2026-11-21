---------------------------- MODULE FaultsTrace -----------------------------
(***************************************************************************)
(* The trace spec of `TruncationTrace` and the faulty entries of CTRL (a   *)
(* boot that reads a record faulty, the pages of a promise, the tri-state  *)
(* tally, the repair probe, the fold hole), on the flavors `disk-faults`   *)
(* and `disk-faults-3` (`faults.rs`): `NodeTrace`, the binding of the      *)
(* walk's events to `Node`, with nothing of its own. The walk keeps the    *)
(* events of this spec (`Spec::keeps`).                                    *)
(***************************************************************************)
EXTENDS NodeTrace
=============================================================================
