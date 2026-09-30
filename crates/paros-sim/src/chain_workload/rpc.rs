//! The chain client's RPC retry layer: one request (or one bounded retry
//! loop at a leader) per call, judged into a terminal outcome. No function
//! here draws randomness; every choice a retry makes is read off the reply.
//!
//! The `*_once` calls clone what they need **when called** and return the
//! request's future, so the run's call sequence — and the `select!`s each
//! one races — is fixed by the caller alone.

use std::future::Future;
use std::time::Duration;

use moonpool_rpc::RpcError;
use moonpool_sim::{SimContext, SimTimeProvider, TimeProvider, assert_always, assert_reachable};
use paros::{
    Entry, InspectReply, JournalId, JournalState, QuorumSystem, Read, ReadAck, Reconfigure,
    ReconfigureMatchmakers, SetLeader, Truncate, Write, WriteAck, journal_state_from_proto,
    quorum_system_to_proto, wire::public::WriteOutcome,
};

use super::ChainConfig;
use crate::client::SimClient;

/// The terminal outcome of one `Write` attempt (#204).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum WriteResult {
    /// The journal holds the batch at `[seq, seq + count)`: accepted now,
    /// or (`duplicate`) acked from the log as the retry of a write accepted
    /// there earlier.
    Written {
        seq: u64,
        count: u64,
        duplicate: bool,
    },
    /// Refused in place: a stale or foreign writer, a position that is not
    /// the next one, or a retry whose bytes differ. `state` names the
    /// current writer and the next position.
    Refused { state: JournalState },
    /// The position is below `first_seq`: whether it was written is
    /// unknowable, and `state` says where the journal stands.
    Truncated { state: JournalState },
    /// No verdict: redirected (with a hint) or not answered.
    Redirect { leader: Option<u64> },
    /// No answer in time.
    Ambiguous,
}

/// A journal state off the wire, judged well-formed.
pub(super) fn state_of(state: Option<paros::wire::common::JournalState>) -> JournalState {
    let decoded = journal_state_from_proto(state);
    assert_always!(
        decoded.is_ok(),
        "chain: a node answers a well-formed journal state"
    );
    decoded.unwrap_or_default()
}

impl WriteResult {
    /// Judge one `Write` RPC's answer: a transport error is ambiguous, a
    /// reply is a verdict or a redirect. On a journal `created` at runtime
    /// an unknown answer is no verdict: a member that has not folded the
    /// create yet does not serve it.
    fn from_response(response: Result<WriteAck, RpcError>, created: bool) -> Self {
        let Some(ack) = response.ok() else {
            return Self::Ambiguous;
        };
        if created && ack.unknown_journal {
            assert_reachable!("system: a member that has not folded a create refuses its journal");
            return Self::Redirect { leader: None };
        }
        assert_always!(
            !ack.unknown_journal,
            "chain: a node serves the journal the client names"
        );
        match ack.outcome() {
            WriteOutcome::Accepted | WriteOutcome::Duplicate => Self::Written {
                seq: ack.seq,
                count: ack.count,
                duplicate: ack.outcome() == WriteOutcome::Duplicate,
            },
            WriteOutcome::Refused => Self::Refused {
                state: state_of(ack.state),
            },
            WriteOutcome::Truncated => Self::Truncated {
                state: state_of(ack.state),
            },
            WriteOutcome::None => Self::Redirect { leader: ack.leader },
        }
    }
}

/// The terminal outcome of one `SetLeader` attempt (#204).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum SetLeaderResult {
    /// The compare-and-swap won: `state` is the new generation.
    Won { state: JournalState },
    /// It lost: `state` names the current writer.
    Lost { state: JournalState },
    /// No verdict.
    Redirect { leader: Option<u64> },
    /// No answer in time.
    Ambiguous,
}

/// The terminal outcome of one truncation operation.
pub(super) enum TruncateResult {
    Applied { state: JournalState },
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
    client: &SimClient,
    journal: JournalId,
    timeout: Duration,
) -> Option<InspectReply> {
    let probe = async { client.inspect_journal(journal.0).await.ok() };
    within(ctx, timeout, None, probe).await
}

/// One `Write` of `entry` to `journal` at `target`. With `abandon` the
/// client stops listening after 10 ms and records the observation as
/// ambiguous. `created` says the journal was created at runtime (#189),
/// so a member may not serve it yet.
pub(super) fn write_once(
    clients: &[SimClient],
    time: &SimTimeProvider,
    journal: JournalId,
    target: usize,
    entry: &Entry,
    abandon: bool,
    created: bool,
) -> impl Future<Output = WriteResult> + use<> {
    let client = clients[target].clone();
    let time = time.clone();
    let request = Write {
        journal: journal.0,
        generation: entry.generation.0,
        owner: entry.owner.0,
        seq: entry.seq.0,
        records: entry.records.iter().map(|r| r.0.clone()).collect(),
    };
    async move {
        let call = client.write(&request);
        if abandon {
            moonpool_sim::select! {
                response = call => WriteResult::from_response(response, created),
                _ = time.sleep(Duration::from_millis(10)) => WriteResult::Ambiguous,
            }
        } else {
            WriteResult::from_response(call.await, created)
        }
    }
}

/// One `SetLeader(expected, owner)` asked of `target`; `created` as for
/// [`write_once`].
pub(super) fn set_leader_once(
    clients: &[SimClient],
    journal: JournalId,
    target: usize,
    expected: u64,
    owner: u64,
    created: bool,
) -> impl Future<Output = SetLeaderResult> + use<> {
    let client = clients[target].clone();
    let request = SetLeader {
        journal: journal.0,
        expected,
        owner,
    };
    async move {
        let Ok(ack) = client.set_leader(&request).await else {
            return SetLeaderResult::Ambiguous;
        };
        if created && ack.unknown_journal {
            assert_reachable!("system: a member that has not folded a create refuses its journal");
            return SetLeaderResult::Redirect { leader: None };
        }
        assert_always!(
            !ack.unknown_journal,
            "chain: a node serves the journal the client names"
        );
        match (ack.decided, ack.won) {
            (true, true) => SetLeaderResult::Won {
                state: state_of(ack.state),
            },
            (true, false) => SetLeaderResult::Lost {
                state: state_of(ack.state),
            },
            _ => SetLeaderResult::Redirect { leader: ack.leader },
        }
    }
}

/// One journal `Read` of `journal` from `from`, asked of `client`.
pub(super) fn read_once(
    client: &SimClient,
    journal: u64,
    from: u64,
    limit: u64,
    wait_ms: u64,
) -> impl Future<Output = Option<ReadAck>> + use<> {
    let client = client.clone();
    let request = Read {
        journal,
        from_seq: from,
        limit,
        wait_ms,
    };
    async move { client.read(&request).await.ok() }
}

/// One truncation below `up_to`, starting at `target` and following
/// redirects for at most `config.compact_attempts` asks.
pub(super) fn truncate_once(
    clients: &[SimClient],
    time: &SimTimeProvider,
    journal: JournalId,
    config: &ChainConfig,
    target: usize,
    up_to: u64,
) -> impl Future<Output = TruncateResult> + use<> {
    let clients = clients.to_vec();
    let time = time.clone();
    let config = *config;
    async move {
        let mut attempt_target = target % clients.len();
        for _attempt in 0..config.compact_attempts {
            let client = clients[attempt_target].clone();
            let request = Truncate {
                journal: journal.0,
                up_to,
            };
            let outcome = moonpool_sim::select! {
                response = client.truncate(&request) => match response {
                    Ok(ack) if ack.decided => TruncateResult::Applied { state: state_of(ack.state) },
                    Ok(ack) => TruncateResult::Rejected { leader: ack.leader },
                    Err(_) => TruncateResult::Ambiguous,
                },
                _ = time.sleep(Duration::from_millis(config.request_timeout_ms)) => TruncateResult::Ambiguous,
            };
            match outcome {
                // A redirect to another node: follow it. The same node, or
                // none named: a beat later, at the next node.
                TruncateResult::Rejected { leader: Some(next) }
                    if usize::try_from(next).is_ok_and(|next| next != attempt_target) =>
                {
                    attempt_target = usize::try_from(next).unwrap_or(0) % clients.len();
                }
                TruncateResult::Rejected { .. } => {
                    if time
                        .sleep(Duration::from_millis(config.compact_beat_ms))
                        .await
                        .is_err()
                    {
                        return outcome;
                    }
                    attempt_target = (attempt_target + 1) % clients.len();
                }
                terminal => return terminal,
            }
        }
        TruncateResult::Ambiguous
    }
}

/// One acceptor reconfiguration onto `members` under `quorum_system`,
/// starting at `target`: redirects are followed, an `unsettled` leader is
/// re-asked a beat later, every other refusal is terminal.
pub(super) fn reconfigure_once(
    clients: &[SimClient],
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
                response = client.reconfigure(&request) => match response {
                    Ok(ack) => {
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
    clients: &[SimClient],
    time: &SimTimeProvider,
    config: &ChainConfig,
    target: usize,
    members: Vec<u64>,
) -> impl Future<Output = ReconfigureMatchmakersResult> + use<> {
    let clients = clients.to_vec();
    let time = time.clone();
    let config = *config;
    async move {
        let client = clients[target % clients.len()].clone();
        for _attempt in 0..config.reconfigure_matchmakers_attempts {
            let request = ReconfigureMatchmakers {
                members: members.clone(),
            };
            let outcome = moonpool_sim::select! {
                response = client.reconfigure_matchmakers(&request) => match response {
                    Ok(ack) => {
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
