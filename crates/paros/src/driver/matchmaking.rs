//! The driver's matchmaker wire (#120, #123, #125): the reconnecting link per
//! matchmaker, the batch of requests a drained `Ready` hands the loop, the
//! detached RPC tasks that carry them, and the reports of what each answer did
//! to the open campaign.

use std::collections::BTreeMap;
use std::time::Duration;

use moonpool_core::{Detach, Providers, TaskProvider, TimeProvider};
use moonpool_hyper::ReconnectingChannel;
use paros_core::{
    Ballot, GcAck, GcRequest, MatchReply, MatchRequest, MatchStep, MatchmakerId, NodeId, RawNode,
    ReconfigureReply, ReconfigureRequest, Slot,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::audit::Audit;
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
        let Some(client) = links.clients.get(&matchmaker) else {
            tracing::warn!(
                node = self_id,
                matchmaker = matchmaker.0,
                "unknown matchmaker"
            );
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
        let mut client = client.clone();
        let acks = links.gc_acks.clone();
        let time = providers.time().clone();
        let timeout = links.timeout;
        let shutdown = links.shutdown.clone();
        let wire = wire_garbage_collect(&request);
        providers
            .task()
            .spawn_task("paros-gc-request", async move {
                let answer = moonpool_core::select! {
                    biased;
                    () = shutdown.cancelled() => return,
                    result = time.timeout(timeout, client.garbage_collect(wire)) => result,
                };
                match answer {
                    Ok(Ok(response)) => {
                        match garbage_collect_ack_from_wire(response.into_inner()) {
                            Ok(ack) => {
                                let _ = acks.send(ack).await;
                            }
                            Err(error) => tracing::warn!(node = self_id, error, "bad gc ack"),
                        }
                    }
                    Ok(Err(status)) => {
                        tracing::debug!(node = self_id, %status, "gc RPC failed");
                    }
                    Err(_) => tracing::debug!(node = self_id, "gc RPC timed out"),
                }
            })
            .detach();
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
        let Some(client) = links.clients.get(&matchmaker) else {
            tracing::warn!(
                node = self_id,
                matchmaker = matchmaker.0,
                "unknown matchmaker"
            );
            continue;
        };
        audit.reconfigure_request_sent(NodeId(self_id), matchmaker, &request);
        tracing::info!(
            node = self_id,
            matchmaker = matchmaker.0,
            kind = reconfigure_kind(&request),
            "reconfigure_request_sent"
        );
        let mut client = client.clone();
        let replies = links.reconfigure_replies.clone();
        let time = providers.time().clone();
        let timeout = links.timeout;
        let shutdown = links.shutdown.clone();
        let wire = wire_reconfigure_request(&request);
        providers
            .task()
            .spawn_task("paros-reconfigure-request", async move {
                let answer = moonpool_core::select! {
                    biased;
                    () = shutdown.cancelled() => return,
                    result = time.timeout(timeout, client.reconfigure(wire)) => result,
                };
                match answer {
                    Ok(Ok(response)) => match reconfigure_reply_from_wire(response.into_inner()) {
                        Ok(reply) => {
                            let _ = replies.send(reply).await;
                        }
                        Err(error) => {
                            tracing::warn!(node = self_id, error, "bad reconfigure reply");
                        }
                    },
                    Ok(Err(status)) => {
                        tracing::debug!(node = self_id, %status, "reconfigure RPC failed");
                    }
                    Err(_) => tracing::debug!(node = self_id, "reconfigure RPC timed out"),
                }
            })
            .detach();
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
    if let Some((ballot, config, reconfiguration)) = node.matchmaking()
        && *last_matchmaking != Some(ballot)
    {
        *last_matchmaking = Some(ballot);
        let generation = node.matchmaker_set().generation.0;
        audit.matchmaking_started(NodeId(self_id), ballot, config, reconfiguration, generation);
        tracing::info!(
            node = self_id,
            round = ballot.round,
            members = config.members.len() as u64,
            reconfiguration,
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
        let Some(client) = links.clients.get(&matchmaker) else {
            tracing::warn!(
                node = self_id,
                matchmaker = matchmaker.0,
                "unknown matchmaker"
            );
            continue;
        };
        audit.match_request_sent(NodeId(self_id), matchmaker, request.ballot);
        tracing::info!(
            node = self_id,
            matchmaker = matchmaker.0,
            round = request.ballot.round,
            "match_request_sent"
        );
        let mut client = client.clone();
        let replies = links.replies.clone();
        let time = providers.time().clone();
        let timeout = links.timeout;
        let shutdown = links.shutdown.clone();
        let wire = wire_match_request(&request);
        providers
            .task()
            .spawn_task("paros-matchmaking-request", async move {
                let answer = moonpool_core::select! {
                    biased;
                    () = shutdown.cancelled() => return,
                    result = time.timeout(timeout, client.matchmake(wire)) => result,
                };
                match answer {
                    Ok(Ok(response)) => match match_reply_from_wire(response.into_inner()) {
                        Ok(reply) => {
                            let _ = replies.send(reply).await;
                        }
                        Err(error) => tracing::warn!(node = self_id, error, "bad match reply"),
                    },
                    Ok(Err(status)) => {
                        tracing::debug!(node = self_id, %status, "matchmaking RPC failed");
                    }
                    Err(_) => tracing::debug!(node = self_id, "matchmaking RPC timed out"),
                }
            })
            .detach();
    }
}

/// Report what one matchmaker reply did to the open campaign.
#[tracing::instrument(level = "trace", skip_all, fields(node = self_id))]
pub(crate) fn report_match_step<A: Audit>(
    node: &RawNode,
    audit: &A,
    self_id: u64,
    matchmaker: MatchmakerId,
    ballot: Ballot,
    step: &MatchStep,
) {
    match step {
        MatchStep::Ignored => {}
        MatchStep::Registered { remaining } => {
            audit.match_registered_by(NodeId(self_id), matchmaker, ballot, *remaining);
            tracing::info!(
                node = self_id,
                matchmaker = matchmaker.0,
                round = ballot.round,
                remaining = *remaining as u64,
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
            audit.match_registered_by(NodeId(self_id), matchmaker, ballot, 0);
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
