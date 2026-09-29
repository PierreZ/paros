//! The operator RPCs the node loop answers from the core: compaction,
//! acceptor-set reconfiguration, retirement and inspection. Each handler
//! validates the wire request, feeds or reads the core, reports the decision,
//! and hands the loop the reply; the loop owns the settle tail and the reply
//! seam.

use std::collections::BTreeSet;

use paros_core::{
    AcceptorConfig, Ballot, ColocatedNode, Control, MatchmakerId, NodeId, ProposeResult,
    ReconfigureRefusal, ReconfigureResult, Slot, StartRefusal,
};

use crate::audit::Audit;
use crate::rpc::{
    InspectReply, Reconfigure, ReconfigureAck, RetireAck, RetireRequest, TrimAck, WireQuorumSystem,
    common, quorum_system_from_proto, quorum_system_to_proto,
};

use super::events::reconfigure_outcome;
use super::handover::HandoverDriver;

/// A journal `Trim` (#185, formerly `Compact`): the client permits dropping
/// the log prefix up to `up_to`. Only the leader admits it: it proposes a
/// `Truncate` control command into the next slot, decided by ordinary Paxos
/// and forwarded to every node, each of which truncates lazily when its walk
/// reaches that slot — one replicated trim point, so every reader gets one
/// answer. A non-leader redirects (like an append).
///
/// There is no precondition (#186): the client decides when its own state
/// covers the prefix — the application lives in the client, and a laggard
/// below the point jumps with `TrimmedTo`.
#[tracing::instrument(level = "debug", skip_all, fields(node = self_id))]
pub(crate) fn trim(node: &mut ColocatedNode, up_to: u64, self_id: u64) -> TrimAck {
    if !node.is_leader() {
        return TrimAck {
            leader: node.leader().map(|n| n.0),
            accepted: false,
            trim_point: node.acceptor().first_slot().0,
            unknown_journal: false,
        };
    }
    // Honest ack: `accepted: true` only when the `Truncate` proposal was
    // actually admitted — `propose_control` can refuse (a step-down raced
    // this request), and the client's retry handles `accepted: false`.
    let accepted = matches!(
        node.propose_control(Control::Truncate { up_to: Slot(up_to) }),
        ProposeResult::Accepted(_)
    );
    tracing::info!(node = self_id, up_to, accepted, "trim_proposed");
    TrimAck {
        leader: Some(self_id),
        accepted,
        trim_point: node.acceptor().first_slot().0,
        unknown_journal: false,
    }
}

/// An online reconfiguration (#122): the leader moves to a fresh ballot
/// registered with the new acceptor set. Refusable like `Compact` — a
/// non-leader redirects, a plain deployment refuses outright, and an
/// unsettled leadership asks the client to retry. Reported here; the loop
/// settles the batch it opened before the ack leaves.
#[tracing::instrument(level = "debug", skip_all, fields(node = self_id))]
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
#[tracing::instrument(level = "debug", skip_all, fields(node = self_id))]
pub(crate) fn retire<A: Audit>(
    node: &ColocatedNode,
    audit: &A,
    self_id: u64,
    req: &RetireRequest,
) -> RetireAck {
    let watermark = req.gc_watermark.map(Ballot::from);
    let accepted = watermark.is_some_and(|w| node.may_retire(w));
    let refusal = if accepted {
        ""
    } else if !node.config().has_matchmakers() {
        "plain"
    } else if node.is_leader() {
        "leader"
    } else if node.is_acceptor() {
        "member"
    } else if watermark
        .is_some_and(|w| node.acceptors_since() != w && w > node.last_member_ballot())
    {
        "stale"
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

/// A matchmaker-set reconfiguration request (#125): any node may drive it.
/// Refusable like every operator request — a plain deployment, an empty
/// target, a matchmaker this node has no link to, or a handover already in
/// flight; otherwise the handover starts and the refusal is empty. The loop
/// reports the start and puts its requests on the wire.
#[tracing::instrument(level = "debug", skip_all, fields(node = node.config().id.0))]
pub(crate) fn reconfigure_matchmakers(
    node: &ColocatedNode,
    handover: &mut HandoverDriver,
    target: &[MatchmakerId],
    is_known: impl Fn(&MatchmakerId) -> bool,
) -> &'static str {
    match node.matchmaker_set() {
        None => "no_matchmakers",
        Some(_) if target.is_empty() => "empty",
        Some(_) if !target.iter().all(is_known) => "unknown_matchmaker",
        Some(current) => match handover.start(current, target.to_vec()) {
            Ok(()) => "",
            Err(StartRefusal::Busy) => "busy",
            Err(StartRefusal::Empty) => "empty",
        },
    }
}

/// A pure read of the core: what an operator (or a client's composer) sees
/// of this node.
#[tracing::instrument(level = "debug", skip_all, fields(node = node.config().id.0))]
pub(crate) fn inspect(node: &ColocatedNode) -> InspectReply {
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
