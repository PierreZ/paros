//! The client [`Workload`]: drives proposals at a node and emits the standard
//! `client_issued` / `client_acknowledged` / `client_failed` observability
//! contract the oracles read back.

use std::time::Duration;

use async_trait::async_trait;
use futures::future::join_all;
use moonpool_sim::sim::config_random_bool;
use moonpool_sim::{
    SimContext, SimulationError, SimulationResult, TimeProvider, Workload, assert_always,
    assert_sometimes,
};
use moonpool_transport::NetTransportBuilder;

use paros::{Compact, Paros, Propose, Read, WLTOKEN_PAROS, parse_addr};

use crate::{GAP_MS, REQUESTS, SETTLE_MS, TIMEOUT_MS};

/// Per-run client workload mode, drawn once from the (uncounted) config RNG —
/// the same stream `swarm_for_seed` draws from — so both modes rotate across
/// the seed sweep without a config knob, and drawing either mode never
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
/// sequential client). This exercises the redirect path and, under chaos,
/// leader loss and re-election on both paths.
pub struct ProposeClient;

#[async_trait]
impl Workload for ProposeClient {
    fn name(&self) -> &'static str {
        "propose-client"
    }

    // One client script per mode: the write and read attempt loops are
    // deliberately the same shape (cycle-on-redirect, one deadline) and shared
    // by both modes, so the strict `W0 R0 W1 R1 …` alternation `Sequential`
    // relies on stays easy to audit as one straight-line loop.
    #[allow(clippy::too_many_lines)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let servers = ctx.topology().all_process_ips().to_vec();
        if servers.is_empty() {
            return Ok(());
        }

        let my_addr = parse_addr(ctx.my_ip())?;
        let transport = NetTransportBuilder::new(ctx.providers().clone())
            .local_address(my_addr)
            .build_listening()
            .await
            .map_err(|e| SimulationError::InvalidState(format!("client transport: {e}")))?;

        // A typed client per node (addressed by address + well-known token, no
        // discovery), so proposals can round-robin across proposers.
        let clients = servers
            .iter()
            .map(|ip| {
                Ok(Paros::client_well_known(
                    parse_addr(ip)?,
                    WLTOKEN_PAROS,
                    &transport,
                ))
            })
            .collect::<SimulationResult<Vec<_>>>()?;

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
        let mode = if config_random_bool(0.5) {
            let mut bits: u32 = 0;
            for _ in 0..3 {
                bits = (bits << 1) | u32::from(config_random_bool(0.5));
            }
            let depth = 2 + bits % 7;
            tracing::info!(mode = "pipelined", depth, "client_workload_mode");
            WorkloadMode::Pipelined { depth }
        } else {
            tracing::info!(mode = "sequential", "client_workload_mode");
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
                tracing::info!(seq_id = seq, "client_issued");
                let attempt = async {
                    let mut target = usize::try_from(seq).unwrap_or(0) % n;
                    loop {
                        let proposal = Propose {
                            client: client_id,
                            seq,
                            command: seq.to_le_bytes().to_vec(),
                        };
                        if let Ok(ack) = clients[target].propose.get_reply(proposal).await {
                            assert_always!(ack.seq == seq, "ack echoes the proposal it answered");
                            if ack.committed {
                                break (ack.leader, ack.slot);
                            }
                        }
                        target = (target + 1) % n;
                        time.sleep(Duration::from_millis(GAP_MS)).await.ok();
                    }
                };
                let outcome: Option<(Option<u64>, Option<u64>)> = tokio::select! {
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
                tracing::info!(seq_id = seq, "client_read_issued");
                let attempt = async {
                    let mut target = usize::try_from(seq).unwrap_or(0) % n;
                    let mut attempts: u64 = 0;
                    loop {
                        attempts += 1;
                        let request = Read {
                            client: client_id,
                            seq,
                        };
                        if let Ok(ack) = clients[target].read.get_reply(request).await {
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
                let outcome: Option<(Option<u64>, u64)> = tokio::select! {
                    v = attempt => Some(v),
                    () = shutdown.cancelled() => None,
                    _ = time.sleep(Duration::from_millis(TIMEOUT_MS)) => None,
                };
                outcome
            }
        };

        // Write terminal-outcome bookkeeping: the ack event carries the
        // committed slot when known (the `Chosen` dedup-retry path acks
        // without one); the linearizability oracle only constrains
        // slot-carrying acks. Also does the leader-driven truncation ping: it
        // tells the leader the highest slot this client has seen chosen; the
        // leader decides a `Truncate` control command into the log, and every
        // node truncates lazily when it applies that slot (one cluster-wide
        // floor, forwarded by normal replication + catch-up). Fire-and-forget
        // to the leader hint; a node still down when the truncate is decided
        // comes back below the floor, which is what makes snapshot restore
        // reachable. The ack is observed via `EV_COMPACTED`. Shared by both
        // modes.
        let mut handle_write = |seq: u64, outcome: Option<(Option<u64>, Option<u64>)>| {
            if let Some((leader, slot)) = outcome {
                acknowledged += 1;
                if let Some(s) = slot {
                    tracing::info!(seq_id = seq, slot = s, "client_acknowledged");
                    max_slot = Some(max_slot.map_or(s, |m| m.max(s)));
                } else {
                    tracing::info!(seq_id = seq, "client_acknowledged");
                }
                if let (Some(up_to), Some(leader_id)) = (max_slot, leader) {
                    let idx = usize::try_from(leader_id).unwrap_or(0);
                    if idx < n {
                        let _ = clients[idx].compact.send(Compact { up_to });
                    }
                }
            } else {
                tracing::info!(seq_id = seq, "client_failed");
            }
        };

        // Read terminal-outcome bookkeeping. The ack event carries the
        // observed watermark; an absent `read_index` field is the empty
        // applied prefix (`None`), which the oracle orders below `Some(0)`.
        // Shared by both modes.
        let mut handle_read = |seq: u64, outcome: Option<(Option<u64>, u64)>| match outcome {
            Some((Some(read_index), attempts)) => {
                reads_acked += 1;
                tracing::info!(
                    seq_id = seq,
                    read_index,
                    attempts,
                    "client_read_acknowledged"
                );
            }
            Some((None, attempts)) => {
                reads_acked += 1;
                tracing::info!(seq_id = seq, attempts, "client_read_acknowledged");
            }
            None => {
                tracing::info!(seq_id = seq, "client_read_failed");
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
                    handle_write(seq, outcome);
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
                        handle_write(s, outcome);
                    }
                    if let Some(read_seq) = last_committed {
                        let read_outcome = read_one(read_seq).await;
                        handle_read(read_seq, read_outcome);
                    }
                    seq = end;
                    time.sleep(Duration::from_millis(GAP_MS)).await.ok();
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
        tokio::select! {
            _ = time.sleep(Duration::from_millis(SETTLE_MS)) => {}
            () = shutdown.cancelled() => {}
        }
        Ok(())
    }
}
