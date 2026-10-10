//! The operator RPCs the node loop answers from the core: acceptor-set
//! reconfiguration, retirement and inspection. Each handler
//! validates the wire request, feeds or reads the core, reports the decision,
//! and hands the loop the reply; the loop owns the settle tail and the reply
//! seam.

use std::collections::BTreeSet;

use paros_core::{
    AcceptorConfig, Ballot, ColocatedNode, MatchmakerId, NodeId, ReconfigureRefusal,
    ReconfigureResult, StartRefusal,
};

use crate::audit::Audit;
use crate::machine::ControlJournals;
use crate::rpc::{
    InspectReply, MatchmakersRefusal, Reconfigure, ReconfigureAck, RetireAck, RetireRefusal,
    RetireRequest, WireQuorumSystem, common, journal_state_to_proto, quorum_system_from_proto,
    quorum_system_to_proto,
};

use super::events::reconfigure_outcome;
use super::handover::HandoverDriver;

/// An online reconfiguration (#122): the leader moves to a fresh ballot
/// registered with the new acceptor set. Refusable — a
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
    // A started reconfiguration re-campaigns at the ballot it reports.
    if let ReconfigureResult::Started(ballot) = result {
        assert!(
            node.ballot() == ballot,
            "a started reconfiguration runs at its ballot"
        );
        assert!(!node.is_leader(), "a reconfiguring leader re-campaigns");
    }
    audit.reconfigure_acked(NodeId(self_id), &members, result);
    let (accepted, refusal, round) = reconfigure_outcome(result);
    assert!(
        accepted == matches!(result, ReconfigureResult::Started(_)),
        "a reconfiguration is accepted exactly when it started"
    );
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
    let refusal = retire_refusal(node, watermark);
    assert!(
        refusal.is_none() == watermark.is_some_and(|w| node.may_retire(w)),
        "a retirement is refused exactly when the core does not admit it"
    );
    audit.retire_acked(NodeId(self_id), refusal);
    let accepted = refusal.is_none();
    let label = refusal.map_or("", RetireRefusal::label);
    tracing::info!(node = self_id, accepted, refusal = label, "retire_acked");
    RetireAck {
        accepted,
        refusal: label.to_string(),
    }
}

/// Which leg refuses a retirement at `watermark` (`None`: the core admits
/// it, [`ColocatedNode::may_retire`]). The legs after the first are only
/// the operator's diagnosis; the decision is the core's.
fn retire_refusal(node: &ColocatedNode, watermark: Option<Ballot>) -> Option<RetireRefusal> {
    if watermark.is_some_and(|w| node.may_retire(w)) {
        return None;
    }
    let refusal = retire_refusal_reason(node, watermark);
    // A refusal names a reason that holds.
    match refusal {
        RetireRefusal::Plain => {
            assert!(!node.config().has_matchmakers(), "a plain refusal is plain");
        }
        RetireRefusal::Leader => assert!(node.is_leader(), "a leader refusal names a leader"),
        RetireRefusal::Member => assert!(node.is_acceptor(), "a member refusal names a member"),
        _ => {}
    }
    Some(refusal)
}

/// Why [`retire_refusal`] refuses, once it has established that it does.
fn retire_refusal_reason(node: &ColocatedNode, watermark: Option<Ballot>) -> RetireRefusal {
    if !node.config().has_matchmakers() {
        RetireRefusal::Plain
    } else if node.is_leader() {
        RetireRefusal::Leader
    } else if node.is_acceptor() {
        RetireRefusal::Member
    } else if watermark
        .is_some_and(|w| node.acceptors_since() != w && w > node.last_member_ballot())
    {
        RetireRefusal::Stale
    } else {
        RetireRefusal::NotCollected
    }
}

/// A matchmaker-set reconfiguration request (#125): any node may drive it.
/// Refusable like every operator request — a plain deployment, an empty
/// target, a matchmaker this node has no link to, or a handover already in
/// flight; otherwise the handover starts. The loop reports the start and
/// puts its requests on the wire.
#[tracing::instrument(level = "debug", skip_all, fields(node = node.config().id.0))]
pub(crate) fn reconfigure_matchmakers(
    node: &ColocatedNode,
    handover: &mut HandoverDriver,
    target: &[MatchmakerId],
    is_known: impl Fn(&MatchmakerId) -> bool,
) -> Result<(), MatchmakersRefusal> {
    match node.matchmaker_set() {
        None => Err(MatchmakersRefusal::NoMatchmakers),
        Some(_) if target.is_empty() => Err(MatchmakersRefusal::Empty),
        Some(_) if !target.iter().all(is_known) => Err(MatchmakersRefusal::UnknownMatchmaker),
        Some(current) => {
            let started =
                handover
                    .start(current, target.to_vec())
                    .map_err(|refusal| match refusal {
                        StartRefusal::Busy => MatchmakersRefusal::Busy,
                        StartRefusal::Empty => MatchmakersRefusal::Empty,
                    });
            if started.is_ok() {
                assert!(handover.is_busy(), "an accepted handover is running");
            }
            started
        }
    }
}

/// The node's own facts (#243): its id, its cell and the control journals'
/// identifiers, and nothing about any journal — the answer to a node-only
/// `Inspect`, and the half every answer carries. An identifier this node does not
/// know (no cell plan, or a cell that does not host the fleet) is left at
/// `0` on the wire: absent, never a default.
pub(crate) fn node_facts(self_id: u64, cell: Option<&ControlJournals>) -> InspectReply {
    let (control_tenant, control_journal) =
        cell.map_or((0, 0), |cell| (cell.cell.tenant.0, cell.cell.journal.0));
    let (fleet_tenant, fleet_journal) = cell
        .and_then(|cell| cell.fleet)
        .map_or((0, 0), |fleet| (fleet.tenant.0, fleet.journal.0));
    let (election_tenant, election_journal) = cell
        .and_then(|cell| cell.election)
        .map_or((0, 0), |election| (election.tenant.0, election.journal.0));
    // A node-only answer names the cell's control journals or none at all.
    if cell.is_none() {
        assert!(
            control_tenant == 0,
            "a node outside a cell names no control journal"
        );
        assert!(
            fleet_tenant == 0,
            "a node outside a cell names no fleet journal"
        );
    }
    InspectReply {
        node: self_id,
        cell_id: cell.map_or(0, |cell| cell.cell_id),
        control_tenant,
        control_journal,
        fleet_tenant,
        fleet_journal,
        election_tenant,
        election_journal,
        ..InspectReply::default()
    }
}

/// A pure read of the core: what an operator (or a client's composer) sees
/// of this node and of the journal `node` runs.
#[tracing::instrument(level = "debug", skip_all, fields(node = node.config().id.0))]
pub(crate) fn inspect(node: &ColocatedNode, cell: Option<&ControlJournals>) -> InspectReply {
    let facts = node_facts(node.config().id.0, cell);
    assert!(
        facts.node == node.config().id.0,
        "an inspection answers for the node inspected"
    );
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
        folded: node.replica().folded().0,
        journal: Some(journal_state_to_proto(node.replica().journal())),
        ..facts
    }
}
