//! The operator RPCs the node loop answers from the core: compaction,
//! acceptor-set reconfiguration, retirement and inspection. Each handler
//! validates the wire request, feeds or reads the core, reports the decision,
//! and hands the loop the reply; the loop owns the settle tail and the reply
//! seam.

use std::collections::BTreeSet;

use paros_core::{
    AcceptorConfig, Ballot, ColocatedNode, Control, NodeId, ProposeResult, ReconfigureRefusal,
    ReconfigureResult, Slot,
};

use crate::audit::Audit;
use crate::grpc::{
    CompactAck, InspectReply, Reconfigure, ReconfigureAck, RetireAck, RetireRequest,
    WireQuorumSystem, common, quorum_system_from_proto, quorum_system_to_proto,
};
use crate::storage::NodeStorage;

use super::events::reconfigure_outcome;
use super::snap_repair::SnapRepair;

/// The application permits dropping the log prefix up to `up_to`. Only the
/// leader admits it: it proposes a `Truncate` control command into the next
/// slot, decided by ordinary Paxos and forwarded to every node, each of which
/// truncates lazily when it applies that slot. A non-leader redirects (like
/// `propose`).
///
/// The coupling rule (#101, CTRL §3.5): a `Truncate{up_to}` is proposed only
/// once a quorum holds the decided snapshot at (or past) `up_to` — that is
/// what makes chunk repair sound once the log below the floor is gone. A
/// request no decided point covers first seeds a `Snap` marker and answers
/// `accepted: false`; the client's retry finds the point once the quorum's
/// custody advertisements land. Proposal-side policy only — the acceptor
/// paths stay fully opaque.
pub(crate) fn compact(
    node: &mut ColocatedNode,
    snap: &mut SnapRepair,
    up_to: u64,
    self_id: u64,
) -> CompactAck {
    if !node.is_leader() {
        return CompactAck {
            leader: node.leader().map(|n| n.0),
            accepted: false,
            first_slot: node.acceptor().first_slot().0,
        };
    }
    // The quorum question goes through the configuration in force, never a
    // raw count: an ack from a node the current acceptor set no longer names
    // does not witness custody.
    let covered = snap
        .acks
        .iter()
        .filter(|(_, holders)| node.acceptors().has_phase2_quorum(holders))
        .map(|(&point, _)| point)
        .max();
    let propose_marker = |node: &mut ColocatedNode, snap: &mut SnapRepair| {
        if snap.marker_pending.is_none()
            && let ProposeResult::Accepted(slot) = node.propose_snap_marker()
        {
            snap.marker_pending = Some(slot);
            tracing::info!(node = self_id, at = slot.0, "snap_marker_proposed");
        }
    };
    let accepted = if let Some(point) = covered {
        let truncate_to = Slot(up_to.min(point.0));
        // Honest ack: `accepted: true` only when the Truncate proposal was
        // actually admitted. `propose_control` can refuse (a step-down raced
        // this request), and the client's retry handles `accepted: false`
        // exactly like the coupling refusal below.
        let proposed = matches!(
            node.propose_control(Control::Truncate { up_to: truncate_to }),
            ProposeResult::Accepted(_)
        );
        tracing::info!(
            node = self_id,
            requested = up_to,
            up_to = truncate_to.0,
            point = point.0,
            accepted = proposed,
            "truncate_coupled_to_snap_point"
        );
        if up_to > point.0 {
            // The request outruns the covered prefix: seed the next point so
            // a later compact can go further.
            propose_marker(node, snap);
        }
        proposed
    } else {
        propose_marker(node, snap);
        false
    };
    CompactAck {
        leader: Some(self_id),
        accepted,
        first_slot: node.acceptor().first_slot().0,
    }
}

/// An online reconfiguration (#122): the leader moves to a fresh ballot
/// registered with the new acceptor set. Refusable like `Compact` — a
/// non-leader redirects, a plain deployment refuses outright, and an
/// unsettled leadership asks the client to retry. Reported here; the loop
/// settles the batch it opened before the ack leaves.
pub(crate) fn reconfigure<A: Audit>(
    node: &mut ColocatedNode,
    audit: &A,
    self_id: u64,
    req: &Reconfigure,
) -> ReconfigureAck {
    let members: Vec<NodeId> = req.members.iter().copied().map(NodeId).collect();
    // The quorum system is the request's (#140): a data change like the
    // membership. Wire input, so a system the membership does not admit is
    // *refused* here — the one place it is validated — where
    // `AcceptorConfig::new` would panic on it.
    let quorum_system = quorum_system_from_proto(&WireQuorumSystem::from(req));
    let distinct = members.iter().collect::<BTreeSet<_>>().len();
    let result = match quorum_system {
        _ if members.is_empty() => ReconfigureResult::Refused(ReconfigureRefusal::UnknownMember),
        Ok(quorum_system) if quorum_system.admits(distinct) => {
            node.reconfigure(&AcceptorConfig::new(members.clone(), quorum_system))
        }
        _ => ReconfigureResult::Refused(ReconfigureRefusal::Malformed),
    };
    audit.reconfigure_acked(NodeId(self_id), &members, result);
    let (accepted, refusal, round) = reconfigure_outcome(result);
    let leader = match result {
        ReconfigureResult::NotLeader(hint) => hint.map(|n| n.0),
        _ => Some(self_id),
    };
    tracing::info!(
        node = self_id,
        members = members.len() as u64,
        accepted,
        refusal,
        round = round.unwrap_or(0),
        "reconfigure_acked"
    );
    ReconfigureAck {
        leader,
        accepted,
        refusal: refusal.to_string(),
        round,
    }
}

/// Operator decommissioning (#123): a node a leader's GC named retirable is
/// shut down for good. The decision is the core's
/// (`ColocatedNode::may_retire`), because the deciding condition is a
/// protocol fact — an effective GC watermark strictly above every ballot a
/// configuration naming this node was bound to — and not the operator's
/// belief that it read a retirable list. Only reads the core: an accepted
/// retirement takes effect on the loop's next tick.
pub(crate) fn retire<A: Audit>(
    node: &ColocatedNode,
    audit: &A,
    self_id: u64,
    req: &RetireRequest,
) -> RetireAck {
    let watermark = req.gc_watermark.map(|b| Ballot {
        round: b.round,
        node: NodeId(b.node),
    });
    let accepted = watermark.is_some_and(|w| node.may_retire(w));
    let refusal = if accepted {
        ""
    } else if !node.config().has_matchmakers() {
        "plain"
    } else if node.is_leader() {
        "leader"
    } else if node.is_acceptor() {
        "member"
    } else {
        "not_collected"
    };
    audit.retire_acked(NodeId(self_id), accepted, refusal);
    tracing::info!(node = self_id, accepted, refusal, "retire_acked");
    RetireAck {
        accepted,
        refusal: refusal.to_string(),
    }
}

/// A pure read of the core and the store: what an operator (or a client's
/// composer) sees of this node.
pub(crate) async fn inspect<S: NodeStorage>(node: &ColocatedNode, storage: &S) -> InspectReply {
    let since = node.acceptors_since();
    let matchmakers = node.matchmaker_set();
    let (gc_watermark, retirable) =
        node.gc_effective()
            .map_or((None, Vec::new()), |(w, retired)| {
                (
                    Some(common::Ballot {
                        round: w.round,
                        node: w.node.0,
                    }),
                    retired.iter().map(|n| n.0).collect(),
                )
            });
    let (quorum_system, phase1_quorum, phase2_quorum, rows, cols) =
        quorum_system_to_proto(node.acceptors().quorum_system()).into_parts();
    InspectReply {
        chosen_index: node.hard_state().chosen_index.map(|slot| slot.0),
        first_slot: node.acceptor().first_slot().0,
        snapshot: storage.snapshot().await,
        members: node.acceptors().members().iter().map(|n| n.0).collect(),
        quorum_system,
        phase1_quorum,
        phase2_quorum,
        rows,
        cols,
        config_ballot: Some(common::Ballot {
            round: since.round,
            node: since.node.0,
        }),
        leader: node.is_leader(),
        matchmaker_generation: matchmakers.map_or(0, |set| set.generation.0),
        matchmakers: matchmakers
            .map_or_else(Vec::new, |set| set.members().iter().map(|m| m.0).collect()),
        retirable,
        gc_watermark,
    }
}
