-------------------------- MODULE ProxyLeaderTrace --------------------------
(***************************************************************************)
(* The trace spec of the rounds of the proxy leaders (`proxy_leader.rs`):  *)
(* `NodeTrace`, the binding of the walk's events to `Node`, with nothing   *)
(* of its own. Its walks keep the log and the steps of the proxies: each   *)
(* delegation, fan-out, `Commit`, close and crash, the leader's own        *)
(* `Accept` (`direct`) and each step down (`Spec::keeps`).                 *)
(***************************************************************************)
EXTENDS NodeTrace
=============================================================================
