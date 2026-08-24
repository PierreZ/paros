//! The client [`Workload`]: drives proposals at a node and emits the standard
//! `client_issued` / `client_acknowledged` / `client_failed` observability
//! contract the oracles read back.

use std::time::Duration;

use async_trait::async_trait;
use futures::future::join_all;
use moonpool_hyper::{ChannelConfig, ReconnectingChannel};
use moonpool_sim::sim::config_random_bool;
use moonpool_sim::{
    SimContext, SimulationError, SimulationResult, TaskProvider, TimeProvider, Workload,
    assert_always, assert_sometimes,
};

use paros::{Compact, ParosClient, Propose, Read, parse_addr};

use crate::{CHAOS_DURATION_MS, GAP_MS, REQUESTS, SETTLE_MS, TIMEOUT_MS};

/// Probability a run draws [`WorkloadMode::Quiet`], FDB-knob style: the rare
/// extreme of "how much does this client ask for", against the ordinary
/// [`REQUESTS`] default. Deliberately the rare mode — it commits a single value,
/// so it exercises none of the multi-slot streaming, truncation or snapshot paths
/// the other two modes drive the coverage gates with. One run in eight is often
/// enough for the sweep to reach the boundary it exists for, and rare enough to
/// leave the rest of the seed space alone.
const QUIET_PROB: f64 = 0.125;

/// Granularity of [`WorkloadMode::Quiet`]'s pre-proposal idle, in simulated ms:
/// the delay is a 4-bit draw times this step, so it lands uniformly across the
/// chaos window. Anything finer would be false precision — the point is only that
/// the single decision can fall anywhere chaos is firing, not at one fixed
/// instant chaos would have to coincide with.
const QUIET_DELAY_STEP_MS: u64 = CHAOS_DURATION_MS / 16;

/// Run one synchronous cleanup action on every exit path from its scope.
struct OnDrop<F: FnOnce()> {
    action: Option<F>,
}

impl<F: FnOnce()> OnDrop<F> {
    fn new(action: F) -> Self {
        Self {
            action: Some(action),
        }
    }
}

impl<F: FnOnce()> Drop for OnDrop<F> {
    fn drop(&mut self) {
        if let Some(action) = self.action.take() {
            action();
        }
    }
}

/// Per-run client workload mode, drawn once from the (uncounted) config RNG —
/// the same stream `swarm_for_seed` draws from — so the modes rotate across
/// the seed sweep without a config knob, and drawing any of them never
/// perturbs the counted `SIM_RNG` stream that message jitter and fault
/// injection draw from (the pinned `REGRESSION_SEEDS` scenarios stay
/// reproducible either way). Recorded via a `client_workload_mode` event so
/// the linearizability oracle can tell which per-seq ordering guarantee this
/// run's history satisfies.
enum WorkloadMode {
    /// One outstanding op at a time: `W0 R0 W1 R1 …`, the program order the
    /// linearizability oracle's C1-C3 checks linearize against.
    Sequential,
    /// Fire `depth` proposals concurrently (fresh seqs, no per-write await),
    /// join them, then optionally read. Leaves multiple undecided slots in
    /// flight at once, unreachable under `Sequential`.
    Pipelined {
        /// Number of proposals in flight at once, `2..=8`.
        depth: u32,
    },
    /// **One decision, then silence**: idle for `delay_ms`, commit a single
    /// proposal, and idle again through the settle tail. No read, and no
    /// compaction ping either — a client that keeps asking the leader to
    /// truncate is a client that is still talking, and the truncate would decide
    /// a *second* slot, which is the one thing this mode must not do.
    ///
    /// It exists for one boundary the other two modes can never reach: a cluster
    /// whose whole history is slot 0. Slot 0 is where "the leader has chosen
    /// nothing" and "the leader has chosen its first slot" collide, so it is the
    /// only prefix at which a watermark that cannot represent *empty* is
    /// indistinguishable from a real one (#56). The other modes lift the prefix
    /// off that boundary within milliseconds, and the ambiguity evaporates long
    /// before any quiescence-gated oracle can look at it.
    ///
    /// The pre-proposal idle is what makes the interesting fault reachable: the
    /// single decision has to land while chaos is firing for a partition or a
    /// crash to cost one follower the only `Commit` the run will ever send.
    /// Proposing immediately would only ever be covered by a fault that happens
    /// to start at zero.
    Quiet {
        /// Simulated ms to idle before the single proposal.
        delay_ms: u64,
    },
}

/// A client that interleaves a fixed number of proposals with reads and records
/// each outcome. Each proposal is deduplicated by `(client_id, seq)`; on a
/// redirect (a non-leader replies `committed = false`) the client cycles to the
/// next node until the leader holds the request and commits it (ack-on-commit).
/// Each run draws a [`WorkloadMode`] once (see `client_workload_mode` in the
/// trace): `Sequential` follows each write with a read of the applied
/// watermark, so the recorded history alternates `W0 R0 W1 R1 …` — the program
/// order the linearizability oracle linearizes against; `Pipelined` fires
/// several fresh-seq proposals concurrently, joins them, then optionally reads
/// once, at the cost of that oracle's C1-C3 checks (valid only for the single
/// sequential client); `Quiet` commits one proposal and then says nothing more,
/// leaving a cluster whose entire chosen history is slot 0. This exercises the
/// redirect path and, under chaos, leader loss and re-election on all three.
pub struct ProposeClient;

#[async_trait]
impl Workload for ProposeClient {
    fn name(&self) -> &'static str {
        "propose-client"
    }

    // One client script per mode: the write and read attempt loops are
    // deliberately the same shape (cycle-on-redirect, one deadline) and shared
    // by every mode, so the strict `W0 R0 W1 R1 …` alternation `Sequential`
    // relies on stays easy to audit as one straight-line loop.
    #[allow(clippy::too_many_lines)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let servers = ctx.topology().all_process_ips().to_vec();
        if servers.is_empty() {
            return Ok(());
        }

        // Validate every endpoint before starting reconnecting-channel tasks.
        let endpoints = servers
            .iter()
            .map(|ip| {
                let addr = parse_addr(ip)?;
                let origin = http::Uri::try_from(format!("http://{addr}"))
                    .map_err(|e| SimulationError::InvalidState(format!("bad gRPC origin: {e}")))?;
                Ok((addr, origin))
            })
            .collect::<SimulationResult<Vec<_>>>()?;

        // A generated tonic client per node. Each multiplexes over a
        // reconnecting h2 channel on the same simulated provider network the
        // nodes use. Keep one channel clone so the workload can terminate all
        // background connect/backoff/keepalive work when it exits.
        let (clients, channels): (Vec<_>, Vec<_>) = endpoints
            .into_iter()
            .map(|(addr, origin)| {
                let channel =
                    ReconnectingChannel::new(ctx.providers(), addr, ChannelConfig::default());
                let client = ParosClient::with_origin(channel.clone(), origin);
                (client, channel)
            })
            .unzip();
        let _channel_guard = OnDrop::new(move || {
            for channel in channels {
                channel.close();
            }
        });

        let time = ctx.time().clone();
        let shutdown = ctx.shutdown().clone();
        let client_id = u64::try_from(ctx.client_id()).unwrap_or(0);
        let n = clients.len();
        let mut acknowledged: u32 = 0;
        let mut reads_acked: u32 = 0;
        // Highest slot this client has seen committed, the compaction watermark it
        // hands to every node (playing the application that owns compaction).
        let mut max_slot: Option<u64> = None;

        // Draw this run's mode. `depth` comes from three independent coin
        // flips (mirrors `swarm_for_seed`'s own per-decision style) rather
        // than an integer range draw, so it stays on the uncounted config RNG
        // stream too — see the [`WorkloadMode`] doc for why that matters.
        let mode = if config_random_bool(QUIET_PROB) {
            let mut bits: u64 = 0;
            for _ in 0..4 {
                bits = (bits << 1) | u64::from(config_random_bool(0.5));
            }
            let delay_ms = bits * QUIET_DELAY_STEP_MS;
            tracing::info!(client_id, mode = "quiet", delay_ms, "client_workload_mode");
            WorkloadMode::Quiet { delay_ms }
        } else if config_random_bool(0.5) {
            let mut bits: u32 = 0;
            for _ in 0..3 {
                bits = (bits << 1) | u32::from(config_random_bool(0.5));
            }
            let depth = 2 + bits % 7;
            tracing::info!(client_id, mode = "pipelined", depth, "client_workload_mode");
            WorkloadMode::Pipelined { depth }
        } else {
            tracing::info!(client_id, mode = "sequential", "client_workload_mode");
            WorkloadMode::Sequential
        };

        // One write attempt for `seq`: send to a node, and on a redirect (a
        // non-leader replies `committed = false`) cycle to the next node until
        // the leader holds the request and commits it (ack-on-commit), all
        // bounded by the per-proposal deadline. Dedup by `(client_id, seq)`
        // makes the cycling safe (at-most-once). The committed ack carries the
        // slot, so the caller can track the chosen prefix. Shared by both
        // modes: `Pipelined` drives several of these concurrently via
        // `join_all` instead of awaiting one at a time.
        let write_one = |seq: u64| {
            let clients = &clients;
            let time = &time;
            let shutdown = &shutdown;
            async move {
                tracing::info!(client_id, seq_id = seq, "client_issued");
                let attempt = async {
                    let mut target = usize::try_from(seq).unwrap_or(0) % n;
                    loop {
                        let proposal = Propose {
                            client: client_id,
                            seq,
                            command: seq.to_le_bytes().to_vec(),
                        };
                        let mut client = clients[target].clone();
                        if let Ok(response) = client.propose(proposal).await {
                            let ack = response.into_inner();
                            assert_always!(ack.seq == seq, "ack echoes the proposal it answered");
                            if ack.committed {
                                break (ack.leader, ack.slot);
                            }
                        }
                        target = (target + 1) % n;
                        time.sleep(Duration::from_millis(GAP_MS)).await.ok();
                    }
                };
                let outcome: Option<(Option<u64>, Option<u64>)> = moonpool_sim::select! {
                    v = attempt => Some(v),
                    () = shutdown.cancelled() => None,
                    _ = time.sleep(Duration::from_millis(TIMEOUT_MS)) => None,
                };
                outcome
            }
        };

        // Read `seq`'s applied watermark. Same redirect-cycling shape as
        // `write_one`, bounded by the same deadline.
        let read_one = |seq: u64| {
            let clients = &clients;
            let time = &time;
            let shutdown = &shutdown;
            async move {
                tracing::info!(client_id, seq_id = seq, "client_read_issued");
                let attempt = async {
                    let mut target = usize::try_from(seq).unwrap_or(0) % n;
                    let mut attempts: u64 = 0;
                    loop {
                        attempts += 1;
                        let request = Read {
                            client: client_id,
                            seq,
                        };
                        let mut client = clients[target].clone();
                        if let Ok(response) = client.read(request).await {
                            let ack = response.into_inner();
                            assert_always!(
                                ack.seq == seq,
                                "read ack echoes the request it answered"
                            );
                            if ack.committed {
                                break (ack.read_index, attempts);
                            }
                        }
                        target = (target + 1) % n;
                        time.sleep(Duration::from_millis(GAP_MS)).await.ok();
                    }
                };
                let outcome: Option<(Option<u64>, u64)> = moonpool_sim::select! {
                    v = attempt => Some(v),
                    () = shutdown.cancelled() => None,
                    _ = time.sleep(Duration::from_millis(TIMEOUT_MS)) => None,
                };
                outcome
            }
        };

        // Write terminal-outcome bookkeeping: the ack event carries the
        // committed slot and the node that answered. *Every* committed ack now
        // names a slot — the dedup fast path included (see
        // `paros::ProposeResult::Chosen`) — so no committed ack is exempt from
        // the oracles any more: the linearizability oracle constrains it, and
        // `oracle::AppliedAckOracle` checks the acking node had really applied
        // that slot by then. Also does the leader-driven truncation ping: it
        // tells the leader the highest slot this client has seen chosen; the
        // leader decides a `Truncate` control command into the log, and every
        // node truncates lazily when it applies that slot (one cluster-wide
        // floor, forwarded by normal replication + catch-up). Fire-and-forget
        // to the leader hint; a node still down when the truncate is decided
        // comes back below the floor, which is what makes snapshot restore
        // reachable. The ack is observed via `EV_COMPACTED`. Shared by all three
        // modes — `ping_compaction` is what [`WorkloadMode::Quiet`] switches off,
        // because the truncate the ping decides would be a second chosen slot and
        // lift that mode's prefix off the boundary it exists to sit on.
        let mut handle_write = |seq: u64,
                                outcome: Option<(Option<u64>, Option<u64>)>,
                                ping_compaction: bool| {
            if let Some((leader, slot)) = outcome {
                acknowledged += 1;
                match (slot, leader) {
                    (Some(s), Some(node)) => {
                        tracing::info!(
                            client_id,
                            seq_id = seq,
                            slot = s,
                            node,
                            "client_acknowledged"
                        );
                    }
                    (Some(s), None) => {
                        tracing::info!(client_id, seq_id = seq, slot = s, "client_acknowledged");
                    }
                    (None, _) => tracing::info!(client_id, seq_id = seq, "client_acknowledged"),
                }
                if let Some(s) = slot {
                    max_slot = Some(max_slot.map_or(s, |m| m.max(s)));
                }
                if let (true, Some(up_to), Some(leader_id)) = (ping_compaction, max_slot, leader) {
                    let idx = usize::try_from(leader_id).unwrap_or(0);
                    if idx < n {
                        let mut client = clients[idx].clone();
                        ctx.task()
                            .spawn_task("paros-grpc-compact", async move {
                                let _ = client.compact(Compact { up_to }).await;
                            })
                            .detach();
                    }
                }
            } else {
                tracing::info!(client_id, seq_id = seq, "client_failed");
            }
        };

        // Read terminal-outcome bookkeeping. The ack event carries the
        // observed watermark; an absent `read_index` field is the empty
        // applied prefix (`None`), which the oracle orders below `Some(0)`.
        // Only the modes that read use it.
        let mut handle_read = |seq: u64, outcome: Option<(Option<u64>, u64)>| match outcome {
            Some((Some(read_index), attempts)) => {
                reads_acked += 1;
                tracing::info!(
                    client_id,
                    seq_id = seq,
                    read_index,
                    attempts,
                    "client_read_acknowledged"
                );
            }
            Some((None, attempts)) => {
                reads_acked += 1;
                tracing::info!(
                    client_id,
                    seq_id = seq,
                    attempts,
                    "client_read_acknowledged"
                );
            }
            None => {
                tracing::info!(client_id, seq_id = seq, "client_read_failed");
            }
        };

        match mode {
            WorkloadMode::Sequential => {
                // Read `seq`, after write `seq`'s terminal event (program
                // order: the oracle derives real-time precedence from this
                // alternation).
                for seq in 0..u64::from(REQUESTS) {
                    if shutdown.is_cancelled() {
                        break;
                    }
                    let outcome = write_one(seq).await;
                    handle_write(seq, outcome, true);
                    let read_outcome = read_one(seq).await;
                    handle_read(seq, read_outcome);
                    // A small gap so node ticks interleave and the timeline
                    // spreads out.
                    time.sleep(Duration::from_millis(GAP_MS)).await.ok();
                }
            }
            WorkloadMode::Pipelined { depth } => {
                // Fire `depth` fresh-seq proposals concurrently (no per-write
                // await), join them, then optionally read the batch's
                // highest committed seq.
                let depth = u64::from(depth);
                let mut seq = 0u64;
                while seq < u64::from(REQUESTS) {
                    if shutdown.is_cancelled() {
                        break;
                    }
                    let end = (seq + depth).min(u64::from(REQUESTS));
                    let outcomes = join_all((seq..end).map(write_one)).await;
                    let mut last_committed = None;
                    for (s, outcome) in (seq..end).zip(outcomes) {
                        if outcome.is_some() {
                            last_committed = Some(s);
                        }
                        handle_write(s, outcome, true);
                    }
                    if let Some(read_seq) = last_committed {
                        let read_outcome = read_one(read_seq).await;
                        handle_read(read_seq, read_outcome);
                    }
                    seq = end;
                    time.sleep(Duration::from_millis(GAP_MS)).await.ok();
                }
            }
            WorkloadMode::Quiet { delay_ms } => {
                // Idle up to the drawn point in the chaos window, commit exactly
                // one proposal, and then stop talking — no read, no compaction
                // ping (see [`WorkloadMode::Quiet`]).
                moonpool_sim::select! {
                    _ = time.sleep(Duration::from_millis(delay_ms)) => {}
                    () = shutdown.cancelled() => {}
                }
                if !shutdown.is_cancelled() {
                    let outcome = write_one(0).await;
                    handle_write(0, outcome, false);
                }
                // Sleep out the remainder of the chaos window before falling into
                // the shared settle tail below. Both are needed: the tail is what
                // keeps the cluster ticking, and reaching past the chaos window
                // first is what lets the liveness oracles' quiescence gate open at
                // all on a run whose one decision may have landed near its end.
                moonpool_sim::select! {
                    _ = time.sleep(Duration::from_millis(
                        CHAOS_DURATION_MS.saturating_sub(delay_ms),
                    )) => {}
                    () = shutdown.cancelled() => {}
                }
            }
        }

        // Under eventual synchrony a stable leader commits proposals; this also
        // wires the `assert_sometimes!` contract into the harness.
        assert_sometimes!(
            acknowledged > 0,
            "a client run acknowledges at least one committed proposal"
        );
        assert_sometimes!(
            reads_acked > 0,
            "a client run commits at least one linearizable read"
        );

        // Settle / quiescence window. The single workload returning is what
        // triggers the harness shutdown (every node then leaves its loop), so
        // without this pause the cluster stops ticking the instant the last
        // proposal is acked — a lagging follower would never get a chance to run
        // commit-replay catch-up. Chaos has ended by now (see `CHAOS_DURATION`),
        // so this is a quiet tail in which the leader keeps heartbeating and any
        // node still short of the chosen prefix converges. The `ConvergenceOracle`
        // asserts over exactly this tail.
        moonpool_sim::select! {
            _ = time.sleep(Duration::from_millis(SETTLE_MS)) => {}
            () = shutdown.cancelled() => {}
        }
        Ok(())
    }
}
