//! The sim-side adapter: a moonpool [`Process`] that runs the provider-generic
//! [`paros::run_node`] driver under `SimProviders`.
//!
//! All the driver logic lives in `paros`; this just bridges the sim boundary —
//! it pulls the providers, local address, and shutdown token out of
//! [`SimContext`] and hands them to the same `run_node` a production
//! `tokio::main` would call.

use async_trait::async_trait;
use moonpool_sim::{Process, SimContext, SimulationResult};
use paros::{Config, MemStorage, parse_addr, run_node};

/// A paros node in the simulation. Stage 1 nodes are interchangeable (no
/// protocol, no membership yet).
pub struct NodeProcess;

#[async_trait]
impl Process for NodeProcess {
    fn name(&self) -> &'static str {
        "paros-node"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        run_node(
            ctx.providers().clone(),
            MemStorage::new(Config::default()),
            parse_addr(ctx.my_ip())?,
            ctx.shutdown().clone(),
        )
        .await
    }
}
