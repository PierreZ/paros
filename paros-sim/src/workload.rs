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

/// A client that sends a fixed number of proposals, **round-robin across all
/// nodes**, and records the outcome of each. Spreading proposals over the
/// cluster makes several nodes propose concurrently — competing proposers that
/// genuinely exercise the value-selection rule and the at-most-one-chosen
/// invariant (under chaos, dueling/livelock is observable here).
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
            let client = &clients[usize::try_from(seq).unwrap_or(0) % clients.len()];
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
