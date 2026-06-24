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

use paros::{Paros, Propose, WLTOKEN_PAROS, parse_addr};

use crate::{GAP_MS, REQUESTS, TIMEOUT_MS};

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

        for seq in 0..u64::from(REQUESTS) {
            if shutdown.is_cancelled() {
                break;
            }

            tracing::info!(seq_id = seq, "client_issued");

            // Send to a node; on a redirect (a non-leader replies `committed =
            // false`) cycle to the next node until the leader holds the request and
            // commits it (ack-on-commit), all bounded by the per-proposal deadline.
            // Dedup by `(client_id, seq)` makes the cycling safe (at-most-once).
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
                            break true;
                        }
                    }
                    target = (target + 1) % n;
                    time.sleep(Duration::from_millis(GAP_MS)).await.ok();
                }
            };
            let acked = tokio::select! {
                v = attempt => v,
                () = shutdown.cancelled() => false,
                _ = time.sleep(Duration::from_millis(TIMEOUT_MS)) => false,
            };

            if acked {
                acknowledged += 1;
                tracing::info!(seq_id = seq, "client_acknowledged");
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
        Ok(())
    }
}
