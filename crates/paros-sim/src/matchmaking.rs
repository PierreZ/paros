//! The client side of matchmaking: the Chain workload's stand-in for the
//! leader's matchmaking phase (a later issue), driving every matchmaker of a
//! seed that deploys them and checking what it is told.
//!
//! The client acts as a proposer: it mints ballots `{ round, node: proposer }`
//! (one proposer per client identity, so the ballot's `node` carries the
//! proposer identity the registry keys on), registers the seed's acceptor
//! configuration under them, and deliberately walks every refusal leg — the
//! same request again (answered from the retained history, which a GC in
//! between may have shrunk), the same ballot with different bytes, a ballot
//! below its own last one, and a request below a watermark it raised. Every
//! outcome is judged twice: here, against what this client itself was told
//! earlier by the same matchmaker (client-visible consistency), and in
//! `crate::audit::matchmaker`, against what the matchmaker durably holds.
//!
//! A refusal's payload (`highest`, `watermark`) is asserted against, never
//! *used*: the next ballot comes from this client's own counter, exactly as
//! `Nack.promised` is a diagnostic and not a campaign hint.

use std::collections::BTreeMap;
use std::time::Duration;

use futures::future::join_all;
use moonpool_hyper::ReconnectingChannel;
use moonpool_sim::{
    SimContext, SimProviders, SimulationError, SimulationResult, TimeProvider, assert_always,
    assert_reachable,
};
use paros::{
    AcceptorConfig, Ballot, MatchOutcome, MatchRefusal, MatchRequest, NodeId,
    ParosMatchmakerClient, QuorumSystem, config_hash, garbage_collect_ack_from_wire,
    match_reply_from_wire, parse_addr, wire_garbage_collect, wire_match_request,
};

/// Proposer identities the workload's clients mint ballots under sit above
/// the acceptor id space, so a client's ballot can never be mistaken for a
/// node's campaign ballot.
const PROPOSER_ID_BASE: u64 = 100;

/// What this client knows about one matchmaker, from that matchmaker's own
/// replies.
#[derive(Default)]
struct Model {
    /// Ballots this matchmaker registered for this client, with the
    /// configuration hash each carried.
    acked: BTreeMap<Ballot, u64>,
    /// The history the first `Registered` reply for each ballot carried.
    first_history: BTreeMap<Ballot, Vec<(Ballot, u64)>>,
    /// The highest watermark this matchmaker has reported.
    watermark: Ballot,
}

/// Sticky per-run coverage facts for the matchmaking operations — a flag
/// set, one independent bit per gate (the `crate::audit` flag-set waiver).
#[derive(Default)]
#[allow(clippy::struct_excessive_bools)]
struct Coverage {
    fresh: bool,
    duplicate: bool,
    conflict: bool,
    stale: bool,
    majority: bool,
    collected: bool,
    below_watermark: bool,
}

/// One client's matchmaking driver.
pub(crate) struct MatchmakingClient {
    proposer: NodeId,
    clients: Vec<ParosMatchmakerClient<ReconnectingChannel<SimProviders, tonic::body::Body>>>,
    channels: Vec<ReconnectingChannel<SimProviders, tonic::body::Body>>,
    /// The configuration this client registers: the seed's acceptor set.
    config: AcceptorConfig,
    /// A different configuration under an already-registered ballot: the
    /// write-once probe.
    conflicting: AcceptorConfig,
    next_round: u64,
    /// The last fresh request, for the duplicate and conflict probes.
    last: Option<(Ballot, AcceptorConfig)>,
    models: Vec<Model>,
    coverage: Coverage,
}

/// The shape of one matchmaking operation, drawn per step.
#[derive(Clone, Copy, Debug)]
enum Policy {
    /// A new ballot above every one this client minted.
    Fresh,
    /// The last request, again (the idempotent re-answer from the retained
    /// history).
    Duplicate,
    /// The last ballot with different bytes (must be refused).
    Conflict,
    /// A ballot below the last one (refused wherever the last one landed).
    Stale,
}

impl Policy {
    fn from_draw(draw: u64) -> Self {
        match draw % 8 {
            0..=4 => Self::Fresh,
            5 => Self::Duplicate,
            6 => Self::Conflict,
            _ => Self::Stale,
        }
    }
}

impl MatchmakingClient {
    /// A client over the seed's matchmakers, registering `acceptors` as its
    /// configuration.
    pub(crate) fn new(
        ctx: &SimContext,
        matchmakers: &[String],
        acceptors: &[String],
        client_id: u64,
        channel_config: &moonpool_hyper::ChannelConfig,
    ) -> SimulationResult<Self> {
        let mut clients = Vec::with_capacity(matchmakers.len());
        let mut channels = Vec::with_capacity(matchmakers.len());
        for ip in matchmakers {
            let addr = parse_addr(ip)?;
            let origin = http::Uri::try_from(format!("http://{addr}"))
                .map_err(|e| SimulationError::InvalidState(format!("bad gRPC origin: {e}")))?;
            let channel = ReconnectingChannel::new(ctx.providers(), addr, channel_config.clone());
            clients.push(ParosMatchmakerClient::with_origin(channel.clone(), origin));
            channels.push(channel);
        }
        let members: Vec<NodeId> = (0..acceptors.len() as u64).map(NodeId).collect();
        let config = AcceptorConfig::new(members.clone(), QuorumSystem::Majority);
        // Different bytes, same shape: a disjoint membership.
        let conflicting = AcceptorConfig::new(
            members.iter().map(|n| NodeId(n.0 + 1000)).collect(),
            QuorumSystem::Majority,
        );
        Ok(Self {
            proposer: NodeId(PROPOSER_ID_BASE + client_id),
            clients,
            channels,
            config,
            conflicting,
            next_round: 1,
            last: None,
            models: matchmakers.iter().map(|_| Model::default()).collect(),
            coverage: Coverage::default(),
        })
    }

    fn ballot(&self, round: u64) -> Ballot {
        Ballot {
            round,
            node: self.proposer,
        }
    }

    /// One matchmaking operation: shape the request by `draw`'s policy, send
    /// it to every matchmaker concurrently, and judge every answer.
    #[tracing::instrument(level = "debug", skip_all, fields(proposer = self.proposer.0))]
    pub(crate) async fn matchmake(&mut self, ctx: &SimContext, draw: u64, timeout: Duration) {
        let policy = Policy::from_draw(draw);
        let request = match policy {
            Policy::Fresh => {
                // Occasionally skip rounds, so the history a later request
                // sees is not always contiguous.
                let round = self.next_round;
                self.next_round = round + 1 + (draw >> 8) % 3;
                let request =
                    MatchRequest::new(self.proposer, self.ballot(round), self.config.clone());
                self.last = Some((request.ballot, request.config.clone()));
                if !self.coverage.fresh {
                    assert_reachable!("chain: a matchmaking request executes");
                    self.coverage.fresh = true;
                }
                request
            }
            Policy::Duplicate => {
                let Some((ballot, config)) = self.last.clone() else {
                    return;
                };
                if !self.coverage.duplicate {
                    assert_reachable!("chain: a duplicate matchmaking request executes");
                    self.coverage.duplicate = true;
                }
                MatchRequest::new(self.proposer, ballot, config)
            }
            Policy::Conflict => {
                let Some((ballot, _)) = self.last.clone() else {
                    return;
                };
                if !self.coverage.conflict {
                    assert_reachable!("chain: a conflicting matchmaking request executes");
                    self.coverage.conflict = true;
                }
                MatchRequest::new(self.proposer, ballot, self.conflicting.clone())
            }
            Policy::Stale => {
                let Some((ballot, _)) = self.last.clone() else {
                    return;
                };
                let Some(round) = ballot.round.checked_sub(1 + (draw >> 8) % 3) else {
                    return;
                };
                if round == 0 {
                    return;
                }
                if !self.coverage.stale {
                    assert_reachable!("chain: a stale matchmaking request executes");
                    self.coverage.stale = true;
                }
                MatchRequest::new(self.proposer, self.ballot(round), self.config.clone())
            }
        };
        tracing::info!(
            proposer = self.proposer.0,
            round = request.ballot.round,
            policy = ?policy,
            "chain_matchmake_submitted"
        );
        let replies = self.broadcast(ctx, &request, timeout).await;
        let mut registered = 0_usize;
        for (index, reply) in replies.into_iter().enumerate() {
            let Some(reply) = reply else {
                continue;
            };
            assert_always!(
                reply.ballot == request.ballot && reply.to == self.proposer,
                "chain: a match reply echoes its request",
                { "matchmaker" => index, "round" => reply.ballot.round }
            );
            self.judge(index, &request, &reply.outcome);
            if matches!(reply.outcome, MatchOutcome::Registered { .. }) {
                registered += 1;
            }
        }
        if registered * 2 > self.clients.len() && !self.coverage.majority {
            assert_reachable!(
                "chain: a matchmaking request is registered by a majority of matchmakers"
            );
            self.coverage.majority = true;
        }
    }

    /// One garbage-collection operation: raise every matchmaker's watermark
    /// to the highest ballot this client has seen registered, then ask below
    /// it and expect the floor to hold.
    #[tracing::instrument(level = "debug", skip_all, fields(proposer = self.proposer.0))]
    pub(crate) async fn garbage_collect(&mut self, ctx: &SimContext, timeout: Duration) {
        let Some(watermark) = self
            .models
            .iter()
            .filter_map(|m| m.acked.keys().next_back().copied())
            .max()
        else {
            return;
        };
        if !self.coverage.collected {
            assert_reachable!("chain: a garbage-collect request executes");
            self.coverage.collected = true;
        }
        tracing::info!(
            proposer = self.proposer.0,
            round = watermark.round,
            "chain_garbage_collect_submitted"
        );
        let time = ctx.time().clone();
        let shutdown = ctx.shutdown().clone();
        let acks = join_all(self.clients.iter().map(|client| {
            let mut client = client.clone();
            let time = time.clone();
            let shutdown = shutdown.clone();
            let request = wire_garbage_collect(self.proposer, watermark);
            async move {
                moonpool_sim::select! {
                    response = client.garbage_collect(request) => response
                        .ok()
                        .and_then(|r| garbage_collect_ack_from_wire(r.into_inner()).ok()),
                    _ = time.sleep(timeout) => None,
                    () = shutdown.cancelled() => None,
                }
            }
        }))
        .await;
        for (index, ack) in acks.into_iter().enumerate() {
            let Some((matchmaker, reported)) = ack else {
                continue;
            };
            assert_always!(
                matchmaker.0 == index as u64 && reported >= watermark,
                "chain: a garbage-collect ack reports a watermark at or above the request",
                { "matchmaker" => index, "requested_round" => watermark.round, "reported_round" => reported.round }
            );
            let model = &mut self.models[index];
            assert_always!(
                reported >= model.watermark,
                "chain: a matchmaker's reported watermark never regresses",
                { "matchmaker" => index }
            );
            model.watermark = reported;
        }
        // Now below the floor: strictly under the watermark this client asked
        // for (its own ballot, one proposer id down at the same round).
        let below = Ballot {
            round: watermark.round,
            node: NodeId(self.proposer.0 - 1),
        };
        let request = MatchRequest::new(self.proposer, below, self.config.clone());
        let replies = self.broadcast(ctx, &request, timeout).await;
        for (index, reply) in replies.into_iter().enumerate() {
            let Some(reply) = reply else {
                continue;
            };
            self.judge(index, &request, &reply.outcome);
        }
    }

    /// Send `request` to every matchmaker concurrently; `None` where the
    /// answer did not arrive in time (ambiguous, never assumed).
    async fn broadcast(
        &self,
        ctx: &SimContext,
        request: &MatchRequest,
        timeout: Duration,
    ) -> Vec<Option<paros::MatchReply>> {
        let time = ctx.time().clone();
        let shutdown = ctx.shutdown().clone();
        join_all(self.clients.iter().map(|client| {
            let mut client = client.clone();
            let time = time.clone();
            let shutdown = shutdown.clone();
            let wire = wire_match_request(request);
            async move {
                moonpool_sim::select! {
                    response = client.matchmake(wire) => response
                        .ok()
                        .and_then(|r| match_reply_from_wire(r.into_inner()).ok()),
                    _ = time.sleep(timeout) => None,
                    () = shutdown.cancelled() => None,
                }
            }
        }))
        .await
    }

    /// Judge one matchmaker's answer against this client's own model of it.
    fn judge(&mut self, index: usize, request: &MatchRequest, outcome: &MatchOutcome) {
        let ballot = request.ballot;
        let model = &mut self.models[index];
        match outcome {
            MatchOutcome::Registered {
                history,
                gc_watermark,
            } => {
                let history: Vec<(Ballot, u64)> =
                    history.iter().map(|(b, c)| (*b, config_hash(c))).collect();
                assert_always!(
                    history
                        .iter()
                        .all(|(b, _)| *b < ballot && *b >= *gc_watermark),
                    "chain: a match history stays below its ballot and at or above its watermark",
                    { "matchmaker" => index, "round" => ballot.round }
                );
                assert_always!(
                    *gc_watermark >= model.watermark,
                    "chain: a matchmaker's reported watermark never regresses",
                    { "matchmaker" => index }
                );
                model.watermark = *gc_watermark;
                // Everything this matchmaker told this client it registered
                // in the window must be in the history, byte for byte.
                for (acked, hash) in model.acked.range(*gc_watermark..ballot) {
                    assert_always!(
                        history.contains(&(*acked, *hash)),
                        "chain: a match history covers the client's own earlier registrations",
                        { "matchmaker" => index, "round" => ballot.round, "missing_round" => acked.round }
                    );
                }
                let hash = config_hash(&request.config);
                if let Some(prior) = model.acked.get(&ballot) {
                    // A re-answer: the same registration, and a history that
                    // at most shrank.
                    assert_always!(
                        *prior == hash,
                        "chain: a matchmaker registers one ballot once for this client",
                        { "matchmaker" => index, "round" => ballot.round }
                    );
                    if let Some(first) = model.first_history.get(&ballot) {
                        assert_always!(
                            history.iter().all(|h| first.contains(h)),
                            "chain: a duplicate match reply never adds history",
                            { "matchmaker" => index, "round" => ballot.round }
                        );
                    }
                } else {
                    model.acked.insert(ballot, hash);
                    model.first_history.insert(ballot, history);
                }
                tracing::info!(
                    proposer = self.proposer.0,
                    matchmaker = index,
                    round = ballot.round,
                    "chain_matchmake_registered"
                );
            }
            MatchOutcome::Refused(MatchRefusal::Stale { highest }) => {
                assert_always!(
                    *highest >= ballot,
                    "chain: a stale refusal names a ballot at or above the request",
                    { "matchmaker" => index, "round" => ballot.round, "highest_round" => highest.round }
                );
                tracing::info!(
                    proposer = self.proposer.0,
                    matchmaker = index,
                    round = ballot.round,
                    "chain_matchmake_refused_stale"
                );
            }
            MatchOutcome::Refused(MatchRefusal::BelowWatermark { watermark }) => {
                assert_always!(
                    *watermark > ballot && *watermark >= model.watermark,
                    "chain: a below-watermark refusal names a watermark above the request",
                    { "matchmaker" => index, "round" => ballot.round, "watermark_round" => watermark.round }
                );
                model.watermark = *watermark;
                if !self.coverage.below_watermark {
                    assert_reachable!("chain: a below-watermark matchmaking request is refused");
                    self.coverage.below_watermark = true;
                }
                tracing::info!(
                    proposer = self.proposer.0,
                    matchmaker = index,
                    round = ballot.round,
                    "chain_matchmake_refused_below_watermark"
                );
            }
        }
    }
}

impl Drop for MatchmakingClient {
    fn drop(&mut self) {
        for channel in &self.channels {
            channel.close();
        }
    }
}
