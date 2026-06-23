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

/// A client that sends a fixed number of proposals to the first node in the
/// cluster and records the outcome of each. With no chaos (Stage 1), every
/// proposal is acknowledged.
pub struct ProposeClient;

#[async_trait]
impl Workload for ProposeClient {
    fn name(&self) -> &'static str {
        "propose-client"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let servers = ctx.topology().all_process_ips().to_vec();
        let Some(server_ip) = servers.first().cloned() else {
            return Ok(());
        };

        let my_addr = parse_addr(ctx.my_ip())?;
        let transport = NetTransportBuilder::new(ctx.providers().clone())
            .local_address(my_addr)
            .build_listening()
            .await
            .map_err(|e| SimulationError::InvalidState(format!("client transport: {e}")))?;

        // A typed client for the node's Paros interface, addressed by the server's
        // address + the well-known token — no discovery, no magic ids.
        let client = Paros::client_well_known(parse_addr(&server_ip)?, WLTOKEN_PAROS, &transport);

        let time = ctx.time().clone();
        let shutdown = ctx.shutdown().clone();
        let mut acknowledged: u32 = 0;

        for seq in 0..u64::from(REQUESTS) {
            if shutdown.is_cancelled() {
                break;
            }

            tracing::info!(seq_id = seq, "client_issued");

            let proposal = Propose {
                seq,
                command: seq.to_le_bytes().to_vec(),
            };
            // Reliable RPC, abandoned if it doesn't return within the deadline.
            let result = tokio::select! {
                r = client.propose.get_reply(proposal) => Some(r),
                () = shutdown.cancelled() => None,
                _ = time.sleep(Duration::from_millis(TIMEOUT_MS)) => None,
            };

            if let Some(Ok(ack)) = result {
                assert_always!(ack.seq == seq, "ack echoes the proposal it answered");
                acknowledged += 1;
                tracing::info!(seq_id = seq, "client_acknowledged");
            } else {
                tracing::info!(seq_id = seq, "client_failed");
            }

            // A small gap so node ticks interleave and the timeline spreads out.
            time.sleep(Duration::from_millis(GAP_MS)).await.ok();
        }

        // Without chaos every proposal comes back; this also wires the
        // `assert_sometimes!` contract into the harness.
        assert_sometimes!(
            acknowledged > 0,
            "a client run acknowledges at least one proposal"
        );
        Ok(())
    }
}
