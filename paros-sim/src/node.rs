//! The sim-side adapter: a moonpool [`Process`] that runs the provider-generic
//! [`paros::run_node`] driver under `SimProviders`.
//!
//! All the driver logic lives in `paros`; this just bridges the sim boundary —
//! it derives a cluster-consistent membership from the topology, then pulls the
//! providers, local address, and shutdown token out of [`SimContext`] and hands
//! them to the same `run_node` a production `tokio::main` would call.

use std::net::IpAddr;

use async_trait::async_trait;
use moonpool_sim::{Process, SimContext, SimulationResult};
use paros::{Config, MemStorage, NodeId, parse_addr, run_node};

/// A paros node in the simulation.
pub struct NodeProcess;

#[async_trait]
impl Process for NodeProcess {
    fn name(&self) -> &'static str {
        "paros-node"
    }

    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        // Build the full cluster membership. `all_process_ips()` excludes this
        // node, so add `my_ip` and sort numerically: every node derives the
        // *same* ordered list, so `NodeId(i) <-> ips[i]` is consistent
        // cluster-wide without any coordination.
        let my_ip = ctx.my_ip().to_string();
        let mut ips: Vec<String> = ctx.topology().all_process_ips().to_vec();
        ips.push(my_ip.clone());
        ips.sort_by_key(|ip| ip.parse::<IpAddr>().ok());
        ips.dedup();

        let members = ips
            .iter()
            .enumerate()
            .map(|(i, ip)| {
                parse_addr(ip)
                    .map(|addr| (NodeId(u64::try_from(i).expect("node index fits u64")), addr))
            })
            .collect::<SimulationResult<Vec<_>>>()?;

        let self_rank = NodeId(
            u64::try_from(
                ips.iter()
                    .position(|ip| ip == &my_ip)
                    .expect("self is a member"),
            )
            .expect("node index fits u64"),
        );
        let config = Config {
            id: self_rank,
            peers: members.iter().map(|(id, _)| *id).collect(),
        };

        run_node(
            ctx.providers().clone(),
            MemStorage::new(config),
            parse_addr(&my_ip)?,
            members,
            ctx.shutdown().clone(),
        )
        .await
    }
}
