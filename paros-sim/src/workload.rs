//! The client [`Workload`]: drives proposals at a node and emits the standard
//! `client_issued` / `client_acknowledged` / `client_failed` observability
//! contract the oracles read back.

use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{
    SimContext, SimulationError, SimulationResult, TimeProvider, Workload, assert_always,
    assert_sometimes, buggify_with_prob,
};
use moonpool_transport::NetTransportBuilder;

use paros::{Compact, Paros, Propose, Read, WLTOKEN_PAROS, parse_addr};

use crate::{GAP_MS, REQUESTS, SETTLE_MS, TIMEOUT_MS};

/// A client that interleaves a fixed number of proposals with reads and records
/// each outcome. Each proposal is deduplicated by `(client_id, seq)`; on a
/// redirect (a non-leader replies `committed = false`) the client cycles to the
/// next node until the leader holds and commits it (ack-on-commit). Each write
/// is followed by a read of the applied watermark, so the recorded history
/// alternates `W0 R0 W1 R1 …` — the program order the linearizability oracle
/// linearizes against. This exercises the redirect path and, under chaos,
/// leader loss and re-election on both paths.
pub struct ProposeClient;

#[async_trait]
impl Workload for ProposeClient {
    fn name(&self) -> &'static str {
        "propose-client"
    }

    // One sequential client script: the write and read attempt loops are
    // deliberately the same shape (cycle-on-redirect, one deadline), and the
    // strict `W0 R0 W1 R1 …` alternation the oracle linearizes against is
    // easiest to audit as one straight-line function.
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

        for seq in 0..u64::from(REQUESTS) {
            if shutdown.is_cancelled() {
                break;
            }

            tracing::info!(seq_id = seq, "client_issued");

            // Send to a node; on a redirect (a non-leader replies `committed =
            // false`) cycle to the next node until the leader holds the request and
            // commits it (ack-on-commit), all bounded by the per-proposal deadline.
            // Dedup by `(client_id, seq)` makes the cycling safe (at-most-once). The
            // committed ack carries the slot, so we can track the chosen prefix.
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
                            // Carry the leader hint alongside the slot so the
                            // truncate watermark below can be sent to the leader.
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

            if let Some((leader, slot)) = outcome {
                acknowledged += 1;
                // The ack event carries the committed slot when known. A dedup
                // ack (`ProposeResult::Chosen`) carries none, so the oracle
                // recovers that slot from the cluster's own `value_chosen`
                // stream rather than leaving the retried writes unconstrained.
                if let Some(s) = slot {
                    tracing::info!(seq_id = seq, slot = s, "client_acknowledged");
                    max_slot = Some(max_slot.map_or(s, |m| m.max(s)));
                } else {
                    tracing::info!(seq_id = seq, "client_acknowledged");
                }
                // Leader-driven truncation: tell the leader the highest slot this
                // client has seen chosen; it decides a `Truncate` control command
                // into the log, and every node truncates lazily when it applies
                // that slot (one cluster-wide floor, forwarded by normal
                // replication + catch-up). Fire-and-forget to the leader hint; a
                // node still down when the truncate is decided comes back below the
                // floor, which is what makes snapshot restore reachable. The ack is
                // observed via `EV_COMPACTED`.
                if let (Some(up_to), Some(leader_id)) = (max_slot, leader) {
                    let idx = usize::try_from(leader_id).unwrap_or(0);
                    if idx < n {
                        let _ = clients[idx].compact.send(Compact { up_to });
                    }
                }
            } else {
                tracing::info!(seq_id = seq, "client_failed");
            }

            // Read `seq`, after write `seq`'s terminal event (program order: the
            // oracle derives real-time precedence from this alternation). Same
            // redirect-cycling shape as the write, bounded by the same deadline.
            tracing::info!(seq_id = seq, "client_read_issued");
            let read_attempt = async {
                let mut target = usize::try_from(seq).unwrap_or(0) % n;
                let mut attempts: u64 = 0;
                loop {
                    attempts += 1;
                    let request = Read {
                        client: client_id,
                        seq,
                    };
                    if let Ok(ack) = clients[target].read.get_reply(request).await {
                        assert_always!(ack.seq == seq, "read ack echoes the request it answered");
                        if ack.committed {
                            break (ack.read_index, attempts);
                        }
                    }
                    target = (target + 1) % n;
                    time.sleep(Duration::from_millis(GAP_MS)).await.ok();
                }
            };
            let read_outcome: Option<(Option<u64>, u64)> = tokio::select! {
                v = read_attempt => Some(v),
                () = shutdown.cancelled() => None,
                _ = time.sleep(Duration::from_millis(TIMEOUT_MS)) => None,
            };
            match read_outcome {
                // The ack event carries the observed watermark; an absent
                // `read_index` field is the empty applied prefix (`None`), which
                // the oracle orders below `Some(0)`.
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
            }

            // A small gap so node ticks interleave and the timeline spreads out.
            time.sleep(Duration::from_millis(GAP_MS)).await.ok();

            // Occasionally the client just… stops, leaving the cluster idle with a
            // very short log for the whole settle tail. That state is otherwise
            // unreachable here (a client that keeps writing hides any staleness
            // behind the next commit), and it is exactly where a follower that
            // missed the only decided slot must still be healed by heartbeats
            // alone. `buggify` keeps it rare and seed-deterministic.
            if seq > 0 && buggify_with_prob!(0.05) {
                tracing::info!(after = seq, "client_went_idle");
                // Hold the run open for an extra settle window: the point of going
                // idle is to *watch* the idle cluster, and a node that only learns
                // a decided slot from heartbeat reconciliation needs that quiet
                // stretch to be long enough to be judged (see the convergence
                // oracle's grace).
                tokio::select! {
                    _ = time.sleep(Duration::from_millis(SETTLE_MS)) => {}
                    () = shutdown.cancelled() => {}
                }
                break;
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
