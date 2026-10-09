//! The driver's matchmaker wire (#120, #123, #125): the link per matchmaker,
//! the batch of requests a drained `Ready` hands the loop, the detached RPC
//! tasks that carry them, and the reports of what each answer did to the open
//! campaign.

use std::collections::BTreeMap;
use std::future::Future;
use std::time::Duration;

use moonpool_core::{Detach, Providers, TaskProvider, TimeProvider};
use moonpool_rpc::RpcError;
use paros_core::{
    Ballot, ColocatedNode, GcAck, GcRequest, MatchOutcome, MatchReply, MatchRequest, MatchStep,
    MatchmakerId, NodeId, ReconfigureReply, ReconfigureRequest, Slot,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::audit::Audit;
use crate::driver::events::{reconfigure_kind, registration_history_hash};
use crate::rpc::{
    MatchmakerClient, garbage_collect_ack_from_wire, match_reply_from_wire,
    reconfigure_reply_from_wire, wire_garbage_collect, wire_match_request,
    wire_reconfigure_request,
};

use super::ready::Outbox;

/// The driver's **matchmaker links** (#120): one client per matchmaker of the
/// deployment (riding the node's RPC runtime), and the inbox the answers come back through.
/// Empty on plain Multi-Paxos, whose driver never speaks the matchmaker
/// contract.
pub(crate) struct MatchmakerLinks<P: Providers> {
    pub(crate) clients: BTreeMap<MatchmakerId, MatchmakerClient<P>>,
    pub(crate) replies: mpsc::Sender<MatchReply>,
    pub(crate) gc_acks: mpsc::Sender<GcAck>,
    pub(crate) reconfigure_replies: mpsc::Sender<ReconfigureReply>,
    pub(crate) timeout: Duration,
    pub(crate) shutdown: CancellationToken,
}

/// This driver's link to one matchmaker, or `None` with a warning: a request
/// addressed to a matchmaker there is no channel to (a learned successor
/// naming a machine outside the deployment) is dropped rather than sent. Kept
/// ahead of each sender's audit/trace prelude, so an undeliverable request is
/// never reported as one that left.
fn link_to<P: Providers>(
    links: &MatchmakerLinks<P>,
    self_id: u64,
    matchmaker: MatchmakerId,
) -> Option<MatchmakerClient<P>> {
    let client = links.clients.get(&matchmaker);
    if client.is_none() {
        tracing::warn!(
            node = self_id,
            matchmaker = matchmaker.0,
            "unknown matchmaker"
        );
    }
    client.cloned()
}

/// Carry one matchmaker RPC on its own detached task and feed its decoded
/// answer back into the node loop's inbox. Shared by all three
/// matchmaker-wire request kinds (#120 matchmaking, #123 GC, #125 handover),
/// which differ only in what they encode, which method they call and which
/// inbox the answer lands in — all of that is `rpc` and `sink`.
///
/// The task draws no randomness and consults no hook (AGENTS.md: a hook answer
/// is a randomness draw, and a detached task is not where the simulation steps
/// deterministically). A lost, late or undecodable answer is simply not fed
/// back — which is exactly what each kind's per-tick re-send exists for.
fn spawn_matchmaker_rpc<P, R, Fut>(
    providers: &P,
    links: &MatchmakerLinks<P>,
    self_id: u64,
    kind: &'static str,
    task: &'static str,
    sink: mpsc::Sender<R>,
    rpc: Fut,
) where
    P: Providers,
    R: Send + 'static,
    Fut: Future<Output = Result<Result<R, &'static str>, RpcError>> + Send + 'static,
{
    let time = providers.time().clone();
    let timeout = links.timeout;
    let shutdown = links.shutdown.clone();
    providers
        .task()
        .spawn_task(task, async move {
            let answer = moonpool_core::select! {
                biased;
                () = shutdown.cancelled() => return,
                result = time.timeout(timeout, rpc) => result,
            };
            match answer {
                Ok(Ok(Ok(reply))) => {
                    let _ = sink.send(reply).await;
                }
                Ok(Ok(Err(error))) => {
                    tracing::warn!(node = self_id, kind, error, "bad matchmaker reply");
                }
                Ok(Err(error)) => {
                    tracing::debug!(node = self_id, kind, %error, "matchmaker RPC failed");
                }
                Err(_) => tracing::debug!(node = self_id, kind, "matchmaker RPC timed out"),
            }
        })
        .detach();
}

/// Send one batch's matchmaker-wire requests.
pub(crate) fn send_outbox<P: Providers, A: Audit>(
    providers: &P,
    links: &MatchmakerLinks<P>,
    audit: &A,
    self_id: u64,
    outbox: Outbox,
) {
    // Every matchmaker-wire request a batch hands back speaks for this node.
    assert!(
        outbox
            .match_requests
            .iter()
            .all(|(_, r)| r.from.0 == self_id),
        "a registration leaves in this node's name"
    );
    assert!(
        outbox.gc_requests.iter().all(|(_, r)| r.from.0 == self_id),
        "a GC request leaves in this node's name"
    );
    send_match_requests(providers, links, audit, self_id, outbox.match_requests);
    send_gc_requests(
        providers,
        links,
        audit,
        self_id,
        outbox.gc_requests,
        outbox.gc_fence,
    );
}

/// Send one batch of garbage-collection requests (#123), each as its own RPC
/// task whose ack is fed back into the node loop through the ack inbox.
#[tracing::instrument(level = "trace", skip_all, fields(node = self_id, requests = requests.len()))]
fn send_gc_requests<P: Providers, A: Audit>(
    providers: &P,
    links: &MatchmakerLinks<P>,
    audit: &A,
    self_id: u64,
    requests: Vec<(MatchmakerId, GcRequest)>,
    fence: Option<Slot>,
) {
    for (matchmaker, request) in requests {
        let Some(client) = link_to(links, self_id, matchmaker) else {
            continue;
        };
        audit.gc_request_sent(
            NodeId(self_id),
            matchmaker,
            request.generation.0,
            request.watermark,
            fence,
        );
        tracing::info!(
            node = self_id,
            matchmaker = matchmaker.0,
            generation = request.generation.0,
            round = request.watermark.round,
            fence = fence.map_or(-1_i64, |s| i64::try_from(s.0).unwrap_or(i64::MAX)),
            "gc_request_sent"
        );
        let wire = wire_garbage_collect(&request);
        spawn_matchmaker_rpc(
            providers,
            links,
            self_id,
            "gc",
            "paros-gc-request",
            links.gc_acks.clone(),
            async move {
                client
                    .collect
                    .try_get_reply(&wire)
                    .await
                    .map(garbage_collect_ack_from_wire)
            },
        );
    }
}

/// Send one batch of matchmaker-reconfiguration requests (#125), each as its
/// own RPC task whose reply is fed back into the node loop.
#[tracing::instrument(level = "trace", skip_all, fields(node = self_id, requests = requests.len()))]
pub(crate) fn send_reconfigure_requests<P: Providers, A: Audit>(
    providers: &P,
    links: &MatchmakerLinks<P>,
    audit: &A,
    self_id: u64,
    requests: Vec<(MatchmakerId, ReconfigureRequest)>,
) {
    assert!(
        requests.iter().all(|(_, r)| r.from().0 == self_id),
        "a handover request leaves in this node's name"
    );
    for (matchmaker, request) in requests {
        let Some(client) = link_to(links, self_id, matchmaker) else {
            continue;
        };
        audit.reconfigure_request_sent(NodeId(self_id), matchmaker, &request);
        tracing::info!(
            node = self_id,
            matchmaker = matchmaker.0,
            kind = reconfigure_kind(&request),
            "reconfigure_request_sent"
        );
        let wire = wire_reconfigure_request(&request);
        spawn_matchmaker_rpc(
            providers,
            links,
            self_id,
            "reconfigure",
            "paros-reconfigure-request",
            links.reconfigure_replies.clone(),
            async move {
                client
                    .reconfigure
                    .try_get_reply(&wire)
                    .await
                    .map(reconfigure_reply_from_wire)
            },
        );
    }
}

/// Surface a matchmaking phase or a membership probe the batch just opened
/// (#120, #173): once per campaign or probe, keyed on its ballot, and
/// *before* the batch's requests leave — the audit folds the opening ahead
/// of its first request.
#[tracing::instrument(level = "trace", skip_all, fields(node = self_id))]
pub(crate) fn surface_matchmaking<A: Audit>(
    node: &ColocatedNode,
    last_matchmaking: &mut Option<Ballot>,
    audit: &A,
    self_id: u64,
) {
    if let Some(probe) = node.membership_probe()
        && *last_matchmaking != Some(probe.ballot())
    {
        let ballot = probe.ballot();
        *last_matchmaking = Some(ballot);
        let generation = node.matchmaker_set().map_or(0, |set| set.generation.0);
        audit.membership_probe_opened(NodeId(self_id), ballot, probe.believed(), generation);
        tracing::info!(
            node = self_id,
            round = ballot.round,
            members = probe.believed().members().len() as u64,
            "membership_probe_opened"
        );
    }
    if let Some(m) = node.matchmaking_role()
        && *last_matchmaking != Some(m.ballot())
    {
        let (ballot, config, kind) = (m.ballot(), m.config(), m.kind());
        *last_matchmaking = Some(ballot);
        let generation = node.matchmaker_set().map_or(0, |set| set.generation.0);
        audit.matchmaking_started(NodeId(self_id), ballot, config, kind, generation);
        tracing::info!(
            node = self_id,
            round = ballot.round,
            members = config.members().len() as u64,
            reconfiguration = kind.is_reconfiguration(),
            "matchmaking_started"
        );
    }
    // Each phase is surfaced once, keyed by its ballot, and a probe and a
    // campaign never coexist.
    assert!(
        node.membership_probe().is_none() || node.matchmaking_role().is_none(),
        "a probe and a campaign never coexist"
    );
    if let Some(m) = node.matchmaking_role() {
        assert!(
            *last_matchmaking == Some(m.ballot()),
            "an open campaign has been surfaced"
        );
    }
}

/// Send one batch of matchmaking requests, each as its own RPC task whose
/// answer (if any) is fed back into the node loop through the reply inbox.
/// The task draws no randomness and consults no hook — a lost or late reply
/// is exactly what [`ColocatedNode::resend_matchmaking`] exists for.
#[tracing::instrument(level = "trace", skip_all, fields(node = self_id, requests = requests.len()))]
fn send_match_requests<P: Providers, A: Audit>(
    providers: &P,
    links: &MatchmakerLinks<P>,
    audit: &A,
    self_id: u64,
    requests: Vec<(MatchmakerId, MatchRequest)>,
) {
    assert!(
        requests.iter().all(|(_, r)| r.from.0 == self_id),
        "a matchmaking request leaves in this node's name"
    );
    for (matchmaker, request) in requests {
        let Some(client) = link_to(links, self_id, matchmaker) else {
            continue;
        };
        if request.purpose.is_probe() {
            audit.membership_probe_sent(NodeId(self_id), matchmaker, request.ballot);
            tracing::info!(
                node = self_id,
                matchmaker = matchmaker.0,
                round = request.ballot.round,
                "membership_probe_sent"
            );
        } else {
            audit.match_request_sent(NodeId(self_id), matchmaker, request.ballot);
            tracing::info!(
                node = self_id,
                matchmaker = matchmaker.0,
                round = request.ballot.round,
                "match_request_sent"
            );
        }
        let wire = wire_match_request(&request);
        spawn_matchmaker_rpc(
            providers,
            links,
            self_id,
            "matchmaking",
            "paros-matchmaking-request",
            links.replies.clone(),
            async move {
                client
                    .matchmake
                    .try_get_reply(&wire)
                    .await
                    .map(match_reply_from_wire)
            },
        );
    }
}

/// Which `Registered` answer a candidate folded: the watermark it reported
/// and a digest of the history it carried. Taken from the reply *before* it
/// is folded — the point the audit's registering check needs, in place of a
/// search over every copy the matchmaker ever sent. `None` for a refusal.
pub(crate) fn folded_answer(reply: &MatchReply) -> Option<(Ballot, u64)> {
    let folded = folded_answer_unchecked(reply);
    assert!(
        folded.is_some() == matches!(reply.outcome, MatchOutcome::Registered { .. }),
        "only a registration's answer is folded"
    );
    folded
}

/// [`folded_answer`] before its postcondition.
fn folded_answer_unchecked(reply: &MatchReply) -> Option<(Ballot, u64)> {
    match &reply.outcome {
        MatchOutcome::Registered {
            history,
            gc_watermark,
            ..
        } => Some((*gc_watermark, registration_history_hash(history))),
        MatchOutcome::Probed { .. } | MatchOutcome::Refused(_) => None,
    }
}

/// Report a matchmaking phase that closed with a quorum and opened Phase 1.
fn report_completed<A: Audit>(
    node: &ColocatedNode,
    audit: &A,
    self_id: u64,
    ballot: Ballot,
    prior: &[paros_core::AcceptorConfig],
    watermark: Ballot,
    registered_by: usize,
) {
    // A completed registration rests on a quorum: at least one answer.
    assert!(
        registered_by > 0,
        "a completed matchmaking was registered by someone"
    );
    audit.matchmaking_completed(
        NodeId(self_id),
        ballot,
        prior,
        watermark,
        registered_by,
        node.matchmaking_disagreements(),
    );
    tracing::info!(
        node = self_id,
        round = ballot.round,
        prior = prior.len() as u64,
        watermark_round = watermark.round,
        registered_by = registered_by as u64,
        "matchmaking_completed"
    );
}

/// Report a membership probe that closed (#173). A probe's partial answer
/// (`MatchStep::ProbeAnswered`) moves nothing a checker judges: only its
/// closing is a transition.
fn report_probe_closed<A: Audit>(audit: &A, self_id: u64, ballot: Ballot, step: &MatchStep) {
    assert!(
        matches!(step, MatchStep::ProbeClosed { .. }),
        "only a closed probe is reported closed"
    );
    let MatchStep::ProbeClosed { effective, member } = *step else {
        return;
    };
    audit.membership_probe_closed(NodeId(self_id), ballot, effective, member);
    tracing::info!(
        node = self_id,
        round = ballot.round,
        effective_round = effective.map_or(0, |b| b.round),
        member,
        "membership_probe_closed"
    );
}

/// Report a closed probe's late answer that moved this node's belief (#278).
fn report_probe_late<A: Audit>(
    audit: &A,
    self_id: u64,
    matchmaker: MatchmakerId,
    ballot: Ballot,
    step: &MatchStep,
) {
    assert!(
        matches!(step, MatchStep::ProbeLate { .. }),
        "only a late probe answer is reported late"
    );
    let MatchStep::ProbeLate { effective, member } = *step else {
        return;
    };
    audit.membership_probe_late(NodeId(self_id), ballot, effective, member);
    tracing::info!(
        node = self_id,
        matchmaker = matchmaker.0,
        round = ballot.round,
        effective_round = effective.map_or(0, |b| b.round),
        member,
        "membership_probe_late"
    );
}

/// Pair of [`folded_answer`]: a step that moved a registration came from a
/// registration's answer, and a probe's step from a probe's answer.
fn assert_step_source(folded: bool, step: &MatchStep) {
    if matches!(
        step,
        MatchStep::Registered { .. } | MatchStep::Paged { .. } | MatchStep::Completed { .. }
    ) {
        assert!(folded, "a registration step folded a registration");
    }
    if matches!(
        step,
        MatchStep::ProbeAnswered | MatchStep::ProbeClosed { .. } | MatchStep::ProbeLate { .. }
    ) {
        assert!(!folded, "a probe step folded no registration");
    }
}

/// Report what one matchmaker reply did to the open campaign. `folded` is
/// [`folded_answer`] for that reply.
#[tracing::instrument(level = "trace", skip_all, fields(node = self_id))]
pub(crate) fn report_match_step<A: Audit>(
    node: &ColocatedNode,
    audit: &A,
    self_id: u64,
    matchmaker: MatchmakerId,
    ballot: Ballot,
    folded: Option<(Ballot, u64)>,
    step: &MatchStep,
) {
    assert_step_source(folded.is_some(), step);
    let (folded_watermark, folded_hash) = folded.unwrap_or_default();
    match step {
        // #189: an unknown member abandons the campaign until the registry
        // catches up; nothing to report beyond the step itself.
        MatchStep::Ignored | MatchStep::ProbeAnswered | MatchStep::UnknownMember => {}
        MatchStep::Registered { remaining } => {
            // Short of the quorum: someone is still owed.
            assert!(
                *remaining > 0,
                "a registration short of its quorum still waits"
            );
            audit.match_registered_by(
                NodeId(self_id),
                matchmaker,
                ballot,
                *remaining,
                folded_watermark,
                folded_hash,
            );
            tracing::info!(
                node = self_id,
                matchmaker = matchmaker.0,
                round = ballot.round,
                remaining = *remaining as u64,
                watermark_round = folded_watermark.round,
                "match_registered_by"
            );
        }
        MatchStep::Paged { next } => {
            audit.match_paged(
                NodeId(self_id),
                matchmaker,
                ballot,
                *next,
                folded_watermark,
                folded_hash,
            );
            tracing::info!(
                node = self_id,
                matchmaker = matchmaker.0,
                round = ballot.round,
                next_round = next.round,
                watermark_round = folded_watermark.round,
                "match_paged"
            );
        }
        MatchStep::Completed {
            prior,
            watermark,
            registered_by,
        } => {
            // The closing reply is a registration too: fold it before the
            // completion so the audit's registering set is the full quorum.
            audit.match_registered_by(
                NodeId(self_id),
                matchmaker,
                ballot,
                0,
                folded_watermark,
                folded_hash,
            );
            report_completed(
                node,
                audit,
                self_id,
                ballot,
                prior,
                *watermark,
                *registered_by,
            );
        }
        MatchStep::StaleConfiguration { newest } => {
            audit.matchmaking_stale_configuration(NodeId(self_id), ballot, *newest);
            tracing::info!(
                node = self_id,
                round = ballot.round,
                newest_round = newest.round,
                "matchmaking_stale_configuration"
            );
        }
        MatchStep::Superseded { set } => {
            audit.matchmakers_learned(NodeId(self_id), set);
            tracing::info!(
                node = self_id,
                matchmaker = matchmaker.0,
                round = ballot.round,
                generation = set.generation.0,
                members = set.members().len() as u64,
                "matchmakers_learned"
            );
        }
        MatchStep::ProbeClosed { .. } => report_probe_closed(audit, self_id, ballot, step),
        MatchStep::ProbeLate { .. } => report_probe_late(audit, self_id, matchmaker, ballot, step),
        MatchStep::Refused(refusal) => {
            audit.matchmaking_refused(NodeId(self_id), matchmaker, ballot, refusal.clone());
            tracing::info!(
                node = self_id,
                matchmaker = matchmaker.0,
                round = ballot.round,
                reason = ?refusal,
                "matchmaking_refused"
            );
        }
    }
}
