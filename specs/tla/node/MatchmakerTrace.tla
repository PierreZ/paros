-------------------------- MODULE MatchmakerTrace ---------------------------
(***************************************************************************)
(* The trace spec of matchmaking, Phase 1 over every configuration of      *)
(* `H_b`, Phase 2 in `C_b` and the GC, on the flavors with matchmakers     *)
(* (`matchmaker.rs`): `NodeTrace`, the binding of the walk's events to     *)
(* `Node`, with nothing of its own. The walk keeps the events of this spec *)
(* (`Spec::keeps`).                                                        *)
(***************************************************************************)
EXTENDS NodeTrace
=============================================================================
