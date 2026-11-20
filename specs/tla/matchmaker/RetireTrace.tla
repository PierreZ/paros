----------------------------- MODULE RetireTrace -----------------------------
(***************************************************************************)
(* The trace spec of the retirement of `Matchmaker`, on the `retiring`     *)
(* walk of the `consensus-sim` cluster (#1231 (traced walks reach the      *)
(* rules), `crates/sim/consensus-sim/src/cluster/retire.rs`).              *)
(*                                                                         *)
(* This module extends `MatchmakerTrace` (matchmaking, the log and the GC) *)
(* and adds the retirement of the model, `Retire` = "effective" with       *)
(* `Tomb`:                                                                 *)
(*   - "retire": the operator retired a node, and it shut down             *)
(*     (`Shutdown`). Some floor is effective (a matchmaker quorum acked    *)
(*     it), and no registration at the floor or above names the node.      *)
(*     The operator retires on the floor that a leader made effective      *)
(*     (`ColocatedNode::may_retire`) or on the floor a beat carried        *)
(*     (`ColocatedNode::may_retire_heard`): both are floors that a         *)
(*     matchmaker quorum acked. From then on, a matchmaker refuses a       *)
(*     configuration that names the node (`MatchRefusal::Retired`), and    *)
(*     the node takes no step;                                             *)
(*   - "rejoin": the operator booked the retired node again                *)
(*     (`MatchPurpose::Rejoin`, #1037 (rejoin)). It is no longer retired,  *)
(*     and a configuration may name it again. Its boot is the "restart"    *)
(*     that follows. The model has no rejoin: this is the trace spec's own *)
(*     step. The node boots from its disk at its next incarnation, so it   *)
(*     keeps its promise and its votes.                                    *)
(* A node that the walk books again while it still runs (it lingered) was  *)
(* never retired, and its rejoin is no step.                               *)
(***************************************************************************)
EXTENDS MatchmakerTrace

\* The node an event names, as an acceptor of the log.
Actor ==
  CASE e.event = "prepare" -> e.owner
    [] e.event \in {"promise", "accept"} -> e.acc
    [] e.event \in {"record", "learn", "restart"} -> e.node
    [] OTHER -> None

\* A retired node takes no step.
Awake == Actor \notin retired

TRetire ==
  /\ e.event = "retire" /\ IsNode("node")
  /\ Shutdown(e.node)
  /\ UNCHANGED logVars

TRejoin ==
  /\ e.event = "rejoin" /\ IsNode("node")
  /\ e.node \in retired
  /\ retired' = retired \ {e.node}
  /\ UNCHANGED <<cfg, reg, wm, answers, matched, hist, p1, prom, vbal, vval, msgs>>
  /\ UNCHANGED logVars

RetireNext ==
  /\ l <= Len(Trace)
  /\ \/ Awake /\ Step
     \/ TRetire
     \/ TRejoin
  /\ l' = l + 1

RetireSpec == TraceInit /\ [][RetireNext]_<<l, vars, logVars>>

-----------------------------------------------------------------------------
(* Safety *)

\* A retired node is named by no registration at an effective floor or
\* above, and no matchmaker took one since.
RetiredUnnamed ==
  \A a \in retired : \E f \in Effective : Unnamed(a, f)
=============================================================================
