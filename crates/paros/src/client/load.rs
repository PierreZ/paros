//! How busy a cell's machines are (#424), from the caller's side: one
//! `Load` to each machine, all at once, each within a timeout.
//!
//! The caller learns the machines (names and addresses) from the cell's
//! `View` (`crate::client::views`), then asks every machine here. A machine
//! that does not answer in time is reported as such, never guessed:
//! `parosctl` prints `no metrics`, as fdbcli does. `parosctl machine
//! list|show` and the simulation's `LOAD` operation call this same
//! function.

use std::collections::BTreeMap;
use std::time::Duration;

use moonpool_core::{Detach, Providers, TaskProvider};
use moonpool_rpc::RpcHandle;

use crate::client::bootstrap::call_once;
use crate::rpc::machine as wire;
use crate::rpc::methods::LoadRpc;
use crate::{Address, Names};

/// What one machine's `Load` came to.
#[derive(Clone, Debug, PartialEq)]
pub enum LoadOutcome {
    /// The machine answered; `windowed` is false when it has no full
    /// window yet.
    Answered(wire::LoadAck),
    /// No answer within the timeout.
    Silent,
}

/// Ask every machine of `machines` (by node id, at its address) for its
/// `Load`, all at once, each within `timeout`; the outcome per node id.
#[tracing::instrument(level = "debug", skip_all, fields(machines = machines.len()))]
pub async fn ask_all<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    names: &Names,
    machines: &[(u64, Address)],
    timeout: Duration,
) -> BTreeMap<u64, LoadOutcome> {
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    for (node, target) in machines {
        let (providers_, rpc, names, target, sender) = (
            providers.clone(),
            rpc.clone(),
            names.clone(),
            target.clone(),
            sender.clone(),
        );
        let node = *node;
        providers
            .task()
            .spawn_task("paros-client-load", async move {
                let reply = call_once::<P, LoadRpc>(
                    &providers_,
                    &rpc,
                    &names,
                    &target,
                    &wire::Load {},
                    timeout,
                )
                .await;
                let outcome = match reply {
                    Some(Ok(ack)) => LoadOutcome::Answered(ack),
                    _ => LoadOutcome::Silent,
                };
                // The caller may be gone: nobody waits for this answer.
                let _ = sender.send((node, outcome));
            })
            .detach();
    }
    drop(sender);
    let mut outcomes = BTreeMap::new();
    while let Some((node, outcome)) = receiver.recv().await {
        outcomes.insert(node, outcome);
    }
    assert!(
        outcomes.len() <= machines.len(),
        "one outcome per machine at most"
    );
    outcomes
}
