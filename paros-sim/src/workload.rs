//! The client [`Workload`]: drives proposals at a node and emits the standard
//! `client_issued` / `client_acknowledged` / `client_failed` observability
//! contract the oracles read back.

use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{
    SimContext, SimulationError, SimulationResult, TimeProvider, Workload, assert_always,
    assert_sometimes,
};
use moonpool_transport::NetTransportBuilder;

use paros::{Compact, Paros, Propose, WLTOKEN_PAROS, parse_addr};

use crate::{GAP_MS, REQUESTS, SETTLE_MS, TIMEOUT_MS};

/// A client that sends a fixed number of proposals and records each outcome.
/// Each proposal is deduplicated by `(client_id, seq)`; on a redirect (a
/// non-leader replies `committed = false`) the client cycles to the next node
/// until the leader holds and commits it (ack-on-commit). This exercises the
/// redirect path and, under chaos, leader loss and re-election.
pub struct ProposeClient;

#[async_trait]
impl Workload for ProposeClient {
    fn name(&self) -> &'static str {
        "propose-client"
    }

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
                            break ack.slot;
                        }
                    }
                    target = (target + 1) % n;
                    time.sleep(Duration::from_millis(GAP_MS)).await.ok();
                }
            };
            let outcome: Option<Option<u64>> = tokio::select! {
                v = attempt => Some(v),
                () = shutdown.cancelled() => None,
                _ = time.sleep(Duration::from_millis(TIMEOUT_MS)) => None,
            };

            if let Some(slot) = outcome {
                acknowledged += 1;
                tracing::info!(seq_id = seq, "client_acknowledged");
                if let Some(s) = slot {
                    max_slot = Some(max_slot.map_or(s, |m| m.max(s)));
                }
                // Notify every node it may compact its log up to the highest slot
                // this client has seen chosen. Each node clamps to its own chosen
                // index, so floors diverge per node (the aggressive policy): a node
                // that lagged keeps a lower floor, which is what makes a below-floor
                // Prepare reachable. Fire-and-forget; the ack is not awaited.
                if let Some(up_to) = max_slot {
                    for client in &clients {
                        let _ = client.compact.send(Compact { up_to });
                    }
                }
            } else {
                tracing::info!(seq_id = seq, "client_failed");
            }

            // A small gap so node ticks interleave and the timeline spreads out.
            time.sleep(Duration::from_millis(GAP_MS)).await.ok();
        }

        // Under eventual synchrony a stable leader commits proposals; this also
        // wires the `assert_sometimes!` contract into the harness.
        assert_sometimes!(
            acknowledged > 0,
            "a client run acknowledges at least one committed proposal"
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
