//! The operator RPCs the node loop answers from the core: acceptor-set
//! reconfiguration, retirement and inspection. Each handler
//! validates the wire request, feeds or reads the core, reports the decision,
//! and hands the loop the reply; the loop owns the settle tail and the reply
//! seam.

use std::collections::BTreeSet;

use paros_core::matchmaker::StartRefusal;
use paros_core::membership::{AcceptorConfig, MatchmakerId, MatchmakerSet};
use paros_core::node::{ReconfigureRefusal, ReconfigureResult};
use paros_core::{Ballot, ColocatedNode, NodeId};

use paros_model::machine::ControlJournals;
use paros_observe::audit::Audit;
use paros_wire::{
    AcceptedVote, InspectReply, MatchmakersRefusal, Reconfigure, ReconfigureAck, RetireAck,
    RetireRefusal, RetireRequest, WireQuorumSystem, common, counts_from_proto,
    journal_state_to_proto, quorum_system_from_proto, quorum_system_to_proto, zoned_members,
};

use super::handover::HandoverDriver;
use paros_observe::events::reconfigure_outcome;

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
    // The labels ride beside the members (#215 (zones)): absent, or one per
    // member, else the request is malformed.
    let zoned = zoned_members(&req.members, req.domains.clone());
    let distinct = members.iter().collect::<BTreeSet<_>>().len();
    // The counts ride beside the members (#754 (counts off the format
    // marker)): an absent one keeps the count in force.
    let counts = counts_from_proto(
        req.proxies.unwrap_or(node.acceptors().proxy_count() as u64),
        req.replicas
            .unwrap_or(node.acceptors().replica_count() as u64),
    );
    let result = match (quorum_system, zoned, counts) {
        _ if members.is_empty() => ReconfigureResult::Refused(ReconfigureRefusal::UnknownMember),
        (Ok(quorum_system), Ok(zoned), Ok((proxies, replicas)))
            if quorum_system.admits(distinct) =>
        {
            let config = match zoned {
                Some(zoned) => AcceptorConfig::zoned(zoned, quorum_system),
                None => AcceptorConfig::new(members.clone(), quorum_system),
            }
            .with_cell(req.cell_id)
            .with_counts(proxies, replicas);
            node.reconfigure(&config)
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

/// A handoff on request (#687 (rebalance a cell)): hand `node`'s Phase-2
/// authority to member `to` (`relinquish_to`, the cooperative handoff), when
/// the core allows it. Refused when the node does not lead, its leadership
/// is not settled, it may not hand its ballot on (only the minter does, one
/// hop), or `to` is not another member of the configuration in force.
#[tracing::instrument(level = "debug", skip_all, fields(node = self_id, to = to.0))]
pub(crate) fn handoff<A: Audit>(
    node: &mut ColocatedNode,
    audit: &A,
    self_id: u64,
    to: NodeId,
) -> paros_wire::HandoffAck {
    if !node.can_relinquish() {
        return handoff_refused("not_leader");
    }
    if !node.handoff_candidates().contains(&to) {
        moonpool_assertions::reachable!("operator: a handoff names a node that is not a member");
        return handoff_refused("not_member");
    }
    let Some(handoff) = node.relinquish_to(to) else {
        return handoff_refused("not_leader");
    };
    assert_eq!(handoff.to, to, "a handoff goes to the member asked");
    assert!(!node.is_leader(), "a relinquished leadership demotes");
    assert!(!node.can_relinquish(), "an authority is relinquished once");
    audit.authority_relinquished(NodeId(self_id), handoff);
    moonpool_assertions::reachable!("operator: a leader hands its journal on, on request");
    tracing::info!(
        node = self_id,
        to = to.0,
        round = handoff.ballot.round,
        next_slot = handoff.next_slot.0,
        "authority_relinquished_on_request"
    );
    paros_wire::HandoffAck {
        handed: true,
        refusal: String::new(),
    }
}

/// A refused handoff (#687), with its reason.
pub(crate) fn handoff_refused(why: &str) -> paros_wire::HandoffAck {
    assert!(!why.is_empty(), "a refusal names its reason");
    paros_wire::HandoffAck {
        handed: false,
        refusal: why.to_string(),
    }
}

/// The answer to a `Reconfigure` that names no journal, or one the node does
/// not serve (#439 (operator calls name their journal)): refused, no leader
/// named, nothing started.
pub(crate) fn unknown_journal() -> ReconfigureAck {
    let ack = ReconfigureAck {
        leader: None,
        accepted: false,
        refusal: "unknown_journal".to_string(),
        round: None,
    };
    assert!(!ack.accepted, "an unknown journal starts nothing");
    assert!(ack.round.is_none(), "an unknown journal names no round");
    ack
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
    } else if watermark.is_some_and(|w| node.retirement_unchecked(w)) {
        // Every other leg holds: the matchmakers were not asked yet which
        // registrations name the node (#1002 (retire fence gaps)). The
        // node loop opened that check; the operator asks again.
        RetireRefusal::Unchecked
    } else {
        RetireRefusal::NotCollected
    }
}

/// A matchmaker-set reconfiguration request (#125): any node may drive it.
/// Refusable like every operator request — a plain deployment, an empty
/// target, a matchmaker this node has no link to, a handover already in
/// flight, or a `generation` (the one the caller read; `None` unfenced)
/// that is no longer the one in force (#563 (set release races a late
/// handover)); otherwise the handover starts. The loop reports the start
/// and puts its requests on the wire.
#[tracing::instrument(level = "debug", skip_all, fields(node = node.config().id.0))]
pub(crate) fn reconfigure_matchmakers(
    node: &ColocatedNode,
    handover: &mut HandoverDriver,
    (target, generation): (&[MatchmakerId], Option<u64>),
    is_known: impl Fn(&MatchmakerId) -> bool,
) -> Result<(), MatchmakersRefusal> {
    match node.matchmaker_set() {
        None => Err(MatchmakersRefusal::NoMatchmakers),
        Some(_) if target.is_empty() => Err(MatchmakersRefusal::Empty),
        Some(_) if !target.iter().all(is_known) => Err(MatchmakersRefusal::UnknownMatchmaker),
        Some(current) if generation.is_some_and(|g| current.generation.0 != g) => {
            // A fenced request names the generation its caller read: a
            // handover that landed since (the caller's own, read a round
            // late) would be asked again onto the set in force, and a
            // second handover would freeze the set a campaign of the
            // tenant's journals is registering at (witness: sweep recipe
            // 17119192588939264734 44446:3821631989232733998 on 3b0fd47,
            // the cell tenant's set handed over twice, and the registry
            // reconfigured into the second freeze lost its leader).
            moonpool_assertions::reachable!("operator: a stale matchmaker handover is refused");
            assert!(
                generation != Some(current.generation.0),
                "a stale fence names another generation than the one in force"
            );
            Err(MatchmakersRefusal::StaleGeneration)
        }
        Some(current) => {
            assert!(
                generation.is_none_or(|g| current.generation.0 == g),
                "a fenced handover starts from the generation its caller read"
            );
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

/// The most accepted records an `Inspect` reply lists: the last ones, so
/// a reply stays small however long the log is.
pub(crate) const INSPECT_ACCEPTED_TAIL: usize = 16;

/// The matchmaker set an `Inspect` of a journal names: the one its tenant's
/// journals on this node believe in force, `tenant` (the latest generation
/// any of them knows, `Journals::believed_matchmakers`), once it is past
/// the journal's own belief `own`. A handover of the tenant's set starts
/// from that belief and its fence is judged against it (#563 (set release
/// races a late handover)), so the read that fences it answers the same
/// belief: a journal whose leader never campaigned again still believes
/// the generation before a handover, while a sibling journal learned the
/// one in force from a refusal. Witness replay-main 8632691373259099510 on
/// 8dae561: the coordinator read a hosted tenant's control journal at
/// generation 0 on the one machine its slots were left on, the node drove
/// the handover through its sibling at generation 1, and every fenced ask
/// was refused as stale until the run's end. A journal without a set
/// names none, whatever its siblings believe.
fn believed_set<'a>(
    own: Option<&'a MatchmakerSet>,
    tenant: Option<&'a MatchmakerSet>,
) -> Option<&'a MatchmakerSet> {
    let set = match (own, tenant) {
        (Some(own), Some(tenant)) if tenant.generation > own.generation => Some(tenant),
        (own, _) => own,
    };
    assert!(
        set.is_some() == own.is_some(),
        "a journal names a set only when it believes one"
    );
    assert!(
        set.is_none_or(|set| own.is_some_and(|own| set.generation >= own.generation)),
        "a journal never names a generation below its own belief"
    );
    set
}

/// A pure read of the core: what an operator (or a client's composer) sees
/// of this node and of the journal `node` runs. `tenant_set` is the set the
/// journal's tenant believes in force on this node ([`believed_set`]).
#[tracing::instrument(level = "debug", skip_all, fields(node = node.config().id.0))]
pub(crate) fn inspect(
    node: &ColocatedNode,
    cell: Option<&ControlJournals>,
    tenant_set: Option<&MatchmakerSet>,
) -> InspectReply {
    let facts = node_facts(node.config().id.0, cell);
    assert!(
        facts.node == node.config().id.0,
        "an inspection answers for the node inspected"
    );
    let since = node.acceptors_since();
    let matchmakers = believed_set(node.matchmaker_set(), tenant_set);
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
        proxies: node.acceptors().proxy_count() as u64,
        replicas: node.acceptors().replica_count() as u64,
        promised: Some(common::Ballot {
            round: node.acceptor().promised().round,
            node: node.acceptor().promised().node.0,
        }),
        vote_watermark: node.acceptor().vote_watermark().map(|slot| slot.0),
        accepted_count: node.acceptor().records().len() as u64,
        accepted_tail: node
            .acceptor()
            .records()
            .iter()
            .rev()
            .take(INSPECT_ACCEPTED_TAIL)
            .rev()
            .map(|(slot, (ballot, _))| AcceptedVote {
                slot: slot.0,
                ballot: Some(common::Ballot {
                    round: ballot.round,
                    node: ballot.node.0,
                }),
            })
            .collect(),
        ..facts
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paros_core::contract::{Config, Limits, MemStorage};
    use paros_core::journal::{JournalId, JournalIdentifier, TenantId};

    #[test]
    fn an_inspect_names_the_set_the_tenant_believes_on_this_node() {
        use paros_core::membership::{MatchmakerGeneration, MatchmakerId};
        let bootstrap = MatchmakerSet::new(
            MatchmakerGeneration(0),
            vec![MatchmakerId(1), MatchmakerId(2)],
        );
        let later = MatchmakerSet::new(MatchmakerGeneration(1), vec![MatchmakerId(2)]);
        // A sibling journal that learned a later generation names it.
        assert_eq!(believed_set(Some(&bootstrap), Some(&later)), Some(&later));
        // The journal's own belief is the latest: its own.
        assert_eq!(believed_set(Some(&later), Some(&bootstrap)), Some(&later));
        assert_eq!(believed_set(Some(&later), Some(&later)), Some(&later));
        assert_eq!(believed_set(Some(&later), None), Some(&later));
        // A journal without a set names none, whatever its siblings believe.
        assert_eq!(believed_set(None, Some(&later)), None);
    }

    #[test]
    fn a_handoff_is_refused_by_a_node_that_does_not_lead() {
        let storage = MemStorage::new(Config {
            peers: vec![NodeId(0), NodeId(1), NodeId(2)],
            ..Config::new(NodeId(0), JournalIdentifier::new(TenantId(1), JournalId(1)))
        });
        let mut node = ColocatedNode::new(&storage, Limits::DEFAULT);
        let audit = paros_observe::audit::NoAudit;
        let ack = handoff(&mut node, &audit, 0, NodeId(1));
        assert!(!ack.handed);
        assert_eq!(ack.refusal, "not_leader");
        assert!(!node.is_leader(), "a refusal changes nothing");
    }

    #[test]
    fn a_matchmaker_handover_is_fenced_by_the_generation_its_caller_read() {
        use paros_core::membership::MatchmakerId;
        let storage = MemStorage::new(Config {
            peers: vec![NodeId(0), NodeId(1), NodeId(2)],
            matchmakers: vec![MatchmakerId(0), MatchmakerId(1), MatchmakerId(2)],
            ..Config::new(NodeId(0), JournalIdentifier::new(TenantId(1), JournalId(1)))
        });
        let node = ColocatedNode::new(&storage, Limits::DEFAULT);
        let generation = node
            .matchmaker_set()
            .map(|set| set.generation.0)
            .expect("a matchmaker deployment believes a set");
        let target = [MatchmakerId(0)];
        // A fence naming another generation is refused, and starts nothing.
        let mut stale = HandoverDriver::new(NodeId(0), 1);
        assert_eq!(
            reconfigure_matchmakers(&node, &mut stale, (&target, Some(generation + 1)), |_| true),
            Err(MatchmakersRefusal::StaleGeneration)
        );
        assert!(!stale.is_busy(), "a refused handover starts nothing");
        // The generation in force, or no fence, starts the handover.
        let mut fenced = HandoverDriver::new(NodeId(0), 1);
        assert_eq!(
            reconfigure_matchmakers(&node, &mut fenced, (&target, Some(generation)), |_| true),
            Ok(())
        );
        assert!(
            fenced.is_busy(),
            "a fenced handover at the generation in force starts"
        );
        let mut unfenced = HandoverDriver::new(NodeId(0), 1);
        assert_eq!(
            reconfigure_matchmakers(&node, &mut unfenced, (&target, None), |_| true),
            Ok(())
        );
        assert!(unfenced.is_busy(), "an unfenced handover starts");
    }

    #[test]
    fn a_refused_handoff_names_its_reason() {
        let ack = handoff_refused("unknown_journal");
        assert!(!ack.handed);
        assert_eq!(ack.refusal, "unknown_journal");
    }
}
