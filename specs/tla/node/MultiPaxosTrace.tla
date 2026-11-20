-------------------------- MODULE MultiPaxosTrace ---------------------------
(***************************************************************************)
(* The trace spec of the log of every cluster flavor without matchmakers   *)
(* (`multi_paxos.rs`): `NodeTrace`, the binding of the walk's events to    *)
(* `Node`, with nothing of its own. The walk keeps the events of this spec *)
(* (`Spec::keeps`).                                                        *)
(***************************************************************************)
EXTENDS NodeTrace
=============================================================================
