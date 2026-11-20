------------------------ MODULE MatchmakerGridTrace ------------------------
(***************************************************************************)
(* The trace spec of `Matchmaker` on grids: `MatchmakerTrace`, with each   *)
(* configuration a grid, on the `reconfigured-compartments` walk of the    *)
(* `consensus-sim` cluster (#1231 (traced walks reach the rules),          *)
(* `crates/sim/consensus-sim/src/cluster/trace/matchmaker.rs`).            *)
(*                                                                         *)
(* The walk reconfigures a grid of `TraceCols` columns and `TraceAcks`     *)
(* acks: each configuration that a ballot selects is a grid of its own     *)
(* members, in the row-major order of the code (`TraceLayouts`). The steps *)
(* are the steps of `MatchmakerTrace`. Only the quorums of the log change: *)
(*   - a Phase-1 quorum of a configuration holds `rows - acks + 1`         *)
(*     acceptors of each of its columns (`GridQ1`), so a win needs that of *)
(*     every configuration of `H_b`;                                       *)
(*   - a Phase-2 quorum of a configuration is `acks` acceptors of one of   *)
(*     its columns (`GridQ2`). A value is chosen at a ballot when one      *)
(*     column of the configuration of that ballot holds it, and the GC     *)
(*     request asks that one column learned the prefix.                    *)
(* The matchmaker quorums stay majorities.                                 *)
(*                                                                         *)
(* The walk has proxy leaders, so the trace spec adds one step that       *)
(* `Matchmaker` does not have:                                             *)
(*   - "commit": a proxy leader sent the `Commit` of its round. A Phase-2  *)
(*     quorum of the configuration of the round's ballot voted for the     *)
(*     value at that ballot (`CommitBacked` of `proxy-leader/`): one       *)
(*     column of the grid of `C_b`, never a quorum of an older             *)
(*     configuration.                                                      *)
(* A delegation and a fan-out change no state of the log: a delegated     *)
(* round is a round of its ballot, and its votes are "accept" steps. The   *)
(* successor of a handoff proposes at the ballot that it took over, as in  *)
(* `MultiPaxosTrace`. `proxy-leader/` judges the rounds of a proxy across  *)
(* its crash, and `leader-handoff/` the guards of a handoff, on their own  *)
(* walks. The reads of the walk are not steps here.                        *)
(***************************************************************************)
EXTENDS MatchmakerTrace, Grid

\* The members of configuration c in the row-major order of its grid.
Layout(c) == CHOOSE M \in TraceLayouts : GridMembers(M) = c

\* Each configuration of the run has one layout, and it tiles its grid.
ASSUME \A c \in TraceConfigs :
         /\ Cardinality({M \in TraceLayouts : GridMembers(M) = c}) = 1
         /\ GridAdmits(Layout(c), TraceCols, TraceAcks)

\* The quorums of the grid of configuration c. A ballot that selected no
\* configuration yet (`cfg[b] = {}`) has none.
GridP1Quorums(c) == IF c = {} THEN {} ELSE GridQ1(Layout(c), TraceCols, TraceAcks, 0)
GridP2Quorums(c) == IF c = {} THEN {} ELSE GridQ2(Layout(c), TraceCols, TraceAcks)

\* A proxy leader sent the `Commit` of slot s at ballot b: the trace spec's
\* own step, see above.
TCommit ==
  /\ e.event = "commit" /\ IsBallot /\ IsSlot /\ IsValue /\ Has("proxy")
  /\ LChosenAt(e.bal, e.slot, e.val)
  /\ UNCHANGED <<vars, logVars>>

GridNext == l <= Len(Trace) /\ (Step \/ TCommit) /\ l' = l + 1

GridSpec == TraceInit /\ [][GridNext]_<<l, vars, logVars>>
=============================================================================
