//! The driver's matchmaker wire (#120, #123, #125): the reconnecting link per
//! matchmaker, the batch of requests a drained `Ready` hands the loop, the
//! detached RPC tasks that carry them, and the reports of what each answer did
//! to the open campaign.

use std::collections::BTreeMap;
use std::future::Future;
use std::time::Duration;

use moonpool_core::{Detach, Providers, TaskProvider, TimeProvider};
use moonpool_hyper::ReconnectingChannel;
use paros_core::{
    Ballot, GcAck, GcRequest, MatchOutcome, MatchReply, MatchRequest, MatchStep, MatchmakerId,
    NodeId, RawNode, ReconfigureReply, ReconfigureRequest, Slot,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::audit::Audit;
use crate::driver::events::registration_history_hash;
use crate::grpc::{
    ParosMatchmakerClient, garbage_collect_ack_from_wire, match_reply_from_wire,
    reconfigure_reply_from_wire, wire_garbage_collect, wire_match_request,
    wire_reconfigure_request,
};
use crate::matchmaker::reconfigure_kind;

use super::ready::Outbox;

/// The driver's **matchmaker links** (#120): one reconnecting channel per
/// matchmaker of the deployment, and the inbox the answers come back through.
/// Empty on plain Multi-Paxos, whose driver never speaks the matchmaker
/// contract.
pub(crate) struct MatchmakerLinks<P: Providers> {
    pub(crate) clients:
        BTreeMap<MatchmakerId, ParosMatchmakerClient<ReconnectingChannel<P, tonic::body::Body>>>,
    pub(crate) replies: mpsc::Sender<MatchReply>,
    pub(crate) gc_acks: mpsc::Sender<GcAck>,
    pub(crate) reconfigure_replies: mpsc::Sender<ReconfigureReply>,
    pub(crate) timeout: Duration,
    pub(crate) shutdown: CancellationToken,
}

/// One matchmaker's reconnecting gRPC client, as this driver holds it.
type MatchmakerClient<P> = ParosMatchmakerClient<ReconnectingChannel<P, tonic::body::Body>>;

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
    Fut: Future<Output = Result<Result<R, &'static str>, tonic::Status>> + Send + 'static,
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
                Ok(Err(status)) => {
                    tracing::debug!(node = self_id, kind, %status, "matchmaker RPC failed");
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
        let Some(mut client) = link_to(links, self_id, matchmaker) else {
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
                    .garbage_collect(wire)
                    .await
                    .map(|response| garbage_collect_ack_from_wire(response.into_inner()))
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
    for (matchmaker, request) in requests {
        let Some(mut client) = link_to(links, self_id, matchmaker) else {
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
                    .reconfigure(wire)
                    .await
                    .map(|response| reconfigure_reply_from_wire(response.into_inner()))
            },
        );
    }
}

/// Surface a matchmaking phase the batch just opened (#120): once per
/// campaign, keyed on its ballot, and *before* the batch's requests leave —
/// the audit folds the campaign's opening ahead of its first request.
#[tracing::instrument(level = "trace", skip_all, fields(node = self_id))]
pub(crate) fn surface_matchmaking<A: Audit>(
    node: &RawNode,
    last_matchmaking: &mut Option<Ballot>,
    audit: &A,
    self_id: u64,
) {
    if let Some((ballot, config, kind)) = node.matchmaking()
        && *last_matchmaking != Some(ballot)
    {
        *last_matchmaking = Some(ballot);
        let generation = node.matchmaker_set().generation.0;
        audit.matchmaking_started(NodeId(self_id), ballot, config, kind, generation);
        tracing::info!(
            node = self_id,
            round = ballot.round,
            members = config.members().len() as u64,
            reconfiguration = kind.is_reconfiguration(),
            "matchmaking_started"
        );
    }
}

/// Send one batch of matchmaking requests, each as its own RPC task whose
/// answer (if any) is fed back into the node loop through the reply inbox.
/// The task draws no randomness and consults no hook — a lost or late reply
/// is exactly what [`RawNode::resend_matchmaking`] exists for.
#[tracing::instrument(level = "trace", skip_all, fields(node = self_id, requests = requests.len()))]
fn send_match_requests<P: Providers, A: Audit>(
    providers: &P,
    links: &MatchmakerLinks<P>,
    audit: &A,
    self_id: u64,
    requests: Vec<(MatchmakerId, MatchRequest)>,
) {
    for (matchmaker, request) in requests {
        let Some(mut client) = link_to(links, self_id, matchmaker) else {
            continue;
        };
        audit.match_request_sent(NodeId(self_id), matchmaker, request.ballot);
        tracing::info!(
            node = self_id,
            matchmaker = matchmaker.0,
            round = request.ballot.round,
            "match_request_sent"
        );
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
                    .matchmake(wire)
                    .await
                    .map(|response| match_reply_from_wire(response.into_inner()))
            },
        );
    }
}

/// Which `Registered` answer a candidate folded: the watermark it reported
/// and a digest of the history it carried. Taken from the reply *before* it
/// is folded — the point the audit's registering check needs, in place of a
/// search over every copy the matchmaker ever sent. `None` for a refusal.
pub(crate) fn folded_answer(reply: &MatchReply) -> Option<(Ballot, u64)> {
    match &reply.outcome {
        MatchOutcome::Registered {
            history,
            gc_watermark,
            ..
        } => Some((*gc_watermark, registration_history_hash(history))),
        MatchOutcome::Refused(_) => None,
    }
}

/// Report what one matchmaker reply did to the open campaign. `folded` is
/// [`folded_answer`] for that reply.
#[tracing::instrument(level = "trace", skip_all, fields(node = self_id))]
pub(crate) fn report_match_step<A: Audit>(
    node: &RawNode,
    audit: &A,
    self_id: u64,
    matchmaker: MatchmakerId,
    ballot: Ballot,
    folded: Option<(Ballot, u64)>,
    step: &MatchStep,
) {
    let (folded_watermark, folded_hash) = folded.unwrap_or_default();
    match step {
        MatchStep::Ignored => {}
        MatchStep::Registered { remaining } => {
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
            audit.matchmaking_completed(
                NodeId(self_id),
                ballot,
                prior,
                *watermark,
                *registered_by,
                node.matchmaking_disagreements(),
            );
            tracing::info!(
                node = self_id,
                round = ballot.round,
                prior = prior.len() as u64,
                watermark_round = watermark.round,
                registered_by = *registered_by as u64,
                "matchmaking_completed"
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
                members = set.members.len() as u64,
                "matchmakers_learned"
            );
        }
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
