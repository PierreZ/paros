//! The chain client's RPC retry layer: one request (or one bounded retry
//! loop at a leader) per call, judged into a terminal outcome. No function
//! here draws randomness; every choice a retry makes is read off the reply.
//!
//! The four `*_once` calls clone what they need **when called** and return
//! the request's future, exactly as the closures they replaced did, so the
//! run's call sequence — and the `select!`s each one races — is unchanged.

use std::future::Future;
use std::time::Duration;

use futures::FutureExt;
use moonpool_sim::{SimContext, SimTimeProvider, TimeProvider, assert_always};
use paros::{
    Compact, InspectReply, InspectRequest, ParosClient, ParosInternalClient, Propose, ProposeAck,
    QuorumSystem, Reconfigure, ReconfigureMatchmakers, quorum_system_to_proto,
};

use super::ChainConfig;
use crate::client::SimChannel;

pub(super) enum ProposalResult {
    Acked { leader: Option<u64>, slot: u64 },
    Rejected { leader: Option<u64> },
    Ambiguous,
}

impl ProposalResult {
    /// Judge one `Propose` RPC's answer: a transport error is ambiguous, a
    /// reply is committed or a redirect.
    fn from_response(
        response: Result<tonic::Response<ProposeAck>, tonic::Status>,
        seq: u64,
    ) -> Self {
        let Ok(response) = response else {
            return Self::Ambiguous;
        };
        let ack = response.into_inner();
        assert_always!(ack.seq == seq, "chain: proposal ack echoes request");
        if ack.committed {
            Self::Acked {
                leader: ack.leader,
                slot: ack.slot.unwrap_or_default(),
            }
        } else {
            Self::Rejected { leader: ack.leader }
        }
    }
}

pub(super) enum CompactResult {
    Accepted { leader: Option<u64> },
    Rejected { leader: Option<u64> },
    Ambiguous,
}

/// The terminal outcome of one matchmaker-set reconfiguration operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ReconfigureMatchmakersResult {
    Started { generation: u64 },
    Refused { refusal: String },
    Ambiguous,
}

/// The terminal outcome of one reconfiguration operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ReconfigureResult {
    Started {
        leader: Option<u64>,
        round: u64,
    },
    Refused {
        leader: Option<u64>,
        refusal: String,
    },
    Ambiguous,
}

/// Race `request` against `timeout` and the run's shutdown: `fallback` when
/// either comes first (an ambiguous observation, never a refusal).
pub(super) async fn within<T>(
    ctx: &SimContext,
    timeout: Duration,
    fallback: T,
    request: impl Future<Output = T>,
) -> T {
    moonpool_sim::select! {
        result = request => result,
        _ = ctx.time().sleep(timeout) => fallback,
        () = ctx.shutdown().cancelled() => fallback,
    }
}

/// One `Inspect` probe of `client`, bounded like every other request.
pub(super) async fn inspect(
    ctx: &SimContext,
    client: &mut ParosInternalClient<SimChannel>,
    timeout: Duration,
) -> Option<InspectReply> {
    let probe = client
        .inspect(InspectRequest {})
        .map(|response| response.ok().map(tonic::Response::into_inner));
    within(ctx, timeout, None, probe).await
}

/// One `Propose` of `payload` as `(client_id, seq)` to `target`. With
/// `abandon` the client stops listening after 10 ms and records the
/// observation as ambiguous.
pub(super) fn propose_once(
    clients: &[ParosClient<SimChannel>],
    time: &SimTimeProvider,
    client_id: u64,
    target: usize,
    seq: u64,
    payload: Vec<u8>,
    abandon: bool,
) -> impl Future<Output = ProposalResult> + use<> {
    let mut client = clients[target].clone();
    let time = time.clone();
    async move {
        let call = client.propose(Propose {
            client: client_id,
            seq,
            command: payload,
        });
        if abandon {
            moonpool_sim::select! {
                response = call => ProposalResult::from_response(response, seq),
                _ = time.sleep(Duration::from_millis(10)) => ProposalResult::Ambiguous,
            }
        } else {
            ProposalResult::from_response(call.await, seq)
        }
    }
}

/// One compaction request up to `up_to`, starting at `target` and following
/// redirects for at most `config.compact_attempts` asks.
pub(super) fn compact_once(
    clients: &[ParosClient<SimChannel>],
    time: &SimTimeProvider,
    config: &ChainConfig,
    target: usize,
    up_to: u64,
) -> impl Future<Output = CompactResult> + use<> {
    let clients = clients.to_vec();
    let time = time.clone();
    let config = *config;
    async move {
        let mut client = clients[target].clone();
        // The #101 coupling makes compaction a two-phase dance: the
        // first ask usually seeds the `Snap` marker and answers
        // `accepted: false`; once a quorum advertises the decided
        // point, a retry gets the `Truncate` proposed. A few
        // beat-spaced retries at the same leader complete the dance
        // within one workload operation, keeping truncation pressure
        // (and everything downstream of raised floors) at its
        // pre-coupling cadence.
        let mut attempt_target = target;
        for _attempt in 0..config.compact_attempts {
            let outcome = moonpool_sim::select! {
                response = client.compact(Compact { up_to }) => match response {
                    Ok(response) => {
                        let ack = response.into_inner();
                        if ack.accepted {
                            CompactResult::Accepted { leader: ack.leader }
                        } else {
                            CompactResult::Rejected { leader: ack.leader }
                        }
                    }
                    Err(_) => CompactResult::Ambiguous,
                },
                _ = time.sleep(Duration::from_millis(config.request_timeout_ms)) => CompactResult::Ambiguous,
            };
            match outcome {
                CompactResult::Rejected { leader: Some(next) }
                    if usize::try_from(next).is_ok_and(|next| next == attempt_target) =>
                {
                    // Same leader, not yet coupled: give the marker a
                    // beat to decide and the custody acks to land.
                    if time
                        .sleep(Duration::from_millis(config.compact_beat_ms))
                        .await
                        .is_err()
                    {
                        return outcome;
                    }
                }
                CompactResult::Rejected { leader: Some(next) } => {
                    let Ok(next) = usize::try_from(next) else {
                        return outcome;
                    };
                    attempt_target = next;
                    client = clients[attempt_target % clients.len()].clone();
                }
                terminal => return terminal,
            }
        }
        CompactResult::Ambiguous
    }
}

/// One acceptor reconfiguration onto `members` under `quorum_system`,
/// starting at `target`: redirects are followed, an `unsettled` leader is
/// re-asked a beat later, every other refusal is terminal.
pub(super) fn reconfigure_once(
    clients: &[ParosClient<SimChannel>],
    time: &SimTimeProvider,
    config: &ChainConfig,
    target: usize,
    members: Vec<u64>,
    quorum_system: QuorumSystem,
) -> impl Future<Output = ReconfigureResult> + use<> {
    let clients = clients.to_vec();
    let time = time.clone();
    let config = *config;
    async move {
        let mut attempt_target = target % clients.len();
        let mut client = clients[attempt_target].clone();
        let wire = quorum_system_to_proto(quorum_system);
        for _attempt in 0..config.reconfigure_attempts {
            let request = Reconfigure {
                members: members.clone(),
                quorum_system: wire.quorum_system,
                phase1_quorum: wire.phase1_quorum,
                phase2_quorum: wire.phase2_quorum,
                rows: wire.rows,
                cols: wire.cols,
            };
            let outcome = moonpool_sim::select! {
                response = client.reconfigure(request) => match response {
                    Ok(response) => {
                        let ack = response.into_inner();
                        if ack.accepted {
                            ReconfigureResult::Started { leader: ack.leader, round: ack.round.unwrap_or(0) }
                        } else {
                            ReconfigureResult::Refused { leader: ack.leader, refusal: ack.refusal }
                        }
                    }
                    Err(_) => ReconfigureResult::Ambiguous,
                },
                _ = time.sleep(Duration::from_millis(config.request_timeout_ms)) => ReconfigureResult::Ambiguous,
            };
            match &outcome {
                // A redirect: follow the hint. Every other refusal is
                // terminal for this operation — `unsettled` included,
                // after one beat at the same leader.
                ReconfigureResult::Refused {
                    leader: Some(next),
                    refusal,
                } if refusal == "not_leader" => {
                    let Ok(next) = usize::try_from(*next) else {
                        return outcome;
                    };
                    attempt_target = next % clients.len();
                    client = clients[attempt_target].clone();
                }
                ReconfigureResult::Refused { refusal, .. } if refusal == "unsettled" => {
                    if time
                        .sleep(Duration::from_millis(config.reconfigure_beat_ms))
                        .await
                        .is_err()
                    {
                        return outcome;
                    }
                }
                _ => return outcome,
            }
        }
        ReconfigureResult::Ambiguous
    }
}

/// One matchmaker-set reconfiguration onto `members`, asked of `target`: a
/// `busy` reconfigurer is re-asked a beat later, every other refusal is
/// terminal.
pub(super) fn reconfigure_matchmakers_once(
    clients: &[ParosClient<SimChannel>],
    time: &SimTimeProvider,
    config: &ChainConfig,
    target: usize,
    members: Vec<u64>,
) -> impl Future<Output = ReconfigureMatchmakersResult> + use<> {
    let clients = clients.to_vec();
    let time = time.clone();
    let config = *config;
    async move {
        let mut client = clients[target % clients.len()].clone();
        for _attempt in 0..config.reconfigure_matchmakers_attempts {
            let request = ReconfigureMatchmakers {
                members: members.clone(),
            };
            let outcome = moonpool_sim::select! {
                response = client.reconfigure_matchmakers(request) => match response {
                    Ok(response) => {
                        let ack = response.into_inner();
                        if ack.accepted {
                            ReconfigureMatchmakersResult::Started { generation: ack.generation.unwrap_or(0) }
                        } else {
                            ReconfigureMatchmakersResult::Refused { refusal: ack.refusal }
                        }
                    }
                    Err(_) => ReconfigureMatchmakersResult::Ambiguous,
                },
                _ = time.sleep(Duration::from_millis(config.request_timeout_ms)) => ReconfigureMatchmakersResult::Ambiguous,
            };
            match &outcome {
                // A busy reconfigurer finishes on its own cadence:
                // re-ask a beat later. Every other refusal is
                // terminal for this operation.
                ReconfigureMatchmakersResult::Refused { refusal } if refusal == "busy" => {
                    if time
                        .sleep(Duration::from_millis(config.reconfigure_beat_ms))
                        .await
                        .is_err()
                    {
                        return outcome;
                    }
                }
                _ => return outcome,
            }
        }
        ReconfigureMatchmakersResult::Ambiguous
    }
}
