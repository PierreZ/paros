//! `parosctl init` (#196, #216): the cell step of the fleet's bootstrap.
//!
//! Sent to the first server — a waiting seed every seed's join list names —
//! which identifies every seed, mints the cell's id and forms every seed
//! (`paros::machine`); then the first cell coordinator claims the cell
//! control journal with `SetLeader(expected_gen = 0)`
//! (`paros::client::bootstrap::claim_cell`). A re-run resumes: a seed that
//! already serves the cell is asked for it, and the claim is made if it is
//! still missing. A cell whose control journal has an owner was initialized
//! before, and `init` is refused. Creating the meta tenant and registering
//! the cell in its directory are the fleet steps of #229.

use std::net::SocketAddr;
use std::time::Duration;

use clap::Args;
use moonpool_core::TokioProviders;
use moonpool_rpc::RpcHandle;
use paros::NodeId;
use paros::client::Client;
use paros::client::bootstrap::{self, ClaimCellOutcome, InitOutcome};
use paros::machine::CELL_CONTROL;
use serde_json::json;

use crate::Ending;
use crate::output::{Printer, note};

/// `parosctl init`.
#[derive(Args, Debug)]
pub struct InitArgs {
    /// How long the seed may take to form the cell, and the cell to elect
    /// its first leader, in milliseconds.
    #[arg(long, default_value = "30000")]
    patience_ms: u64,
}

/// `parosctl init`: form the cell at `addrs[0]`, then claim its control
/// journal through a client `connect` builds over its members.
pub async fn run(
    providers: &TokioProviders,
    rpc: &RpcHandle<TokioProviders>,
    addrs: &[SocketAddr],
    connect: impl Fn(&[(u64, SocketAddr)]) -> Client<TokioProviders>,
    out: &Printer,
    args: InitArgs,
) -> Ending {
    let Some(&target) = addrs.first() else {
        note("no server to send init to");
        return Ending::Unreachable;
    };
    let patience = Duration::from_millis(args.patience_ms);
    let (servers, coordinator, cell) = match bootstrap::init(providers, rpc, target, patience).await
    {
        InitOutcome::Formed(plan) => {
            note(&format!(
                "formed cell {} over {} seeds",
                plan.cell_id,
                plan.members.len()
            ));
            let servers: Vec<(u64, SocketAddr)> = plan
                .members
                .iter()
                .map(|(id, addr)| (id.0, *addr))
                .collect();
            (servers, plan.coordinator(), Some(plan.cell_id))
        }
        InitOutcome::Refused(refusal) => {
            out.emit(
                || format!("init refused: {refusal}"),
                || json!({ "outcome": "refused", "refusal": refusal }),
            );
            return Ending::Refused;
        }
        InitOutcome::Malformed => {
            note("the seed answered with a plan that does not decode");
            return Ending::Unreachable;
        }
        InitOutcome::Unreachable => {
            note(&format!("no machine answered init at {target}"));
            return Ending::Unreachable;
        }
        // No machine endpoint: the target serves a cell already (a re-run
        // after its formation), or nothing listens there.
        InitOutcome::NotWaiting => {
            let servers = bootstrap::discover(providers, rpc, addrs, patience).await;
            if servers.is_empty() {
                note(&format!("nothing answered init or inspect at {target}"));
                return Ending::Unreachable;
            }
            let client = connect(&servers);
            let Some(view) = client.inspect(0, CELL_CONTROL).await else {
                note("no server described the cell control journal");
                return Ending::Unreachable;
            };
            let Some(coordinator) = view.members.iter().copied().min() else {
                note("the cell control journal names no member");
                return Ending::Unreachable;
            };
            (servers, NodeId(coordinator), None)
        }
    };
    let client = connect(&servers);
    match bootstrap::claim_cell(&client, coordinator, patience).await {
        ClaimCellOutcome::Claimed { generation } => {
            out.emit(
                || {
                    format!(
                        "initialized cell{} coordinator={} generation={generation} members={}",
                        cell.map_or_else(String::new, |c| format!(" {c}")),
                        coordinator.0,
                        servers.len()
                    )
                },
                || {
                    json!({
                        "outcome": "initialized",
                        "cell": cell,
                        "coordinator": coordinator.0,
                        "generation": generation,
                        "members": servers
                            .iter()
                            .map(|(id, addr)| json!({ "node": id, "addr": addr.to_string() }))
                            .collect::<Vec<_>>(),
                    })
                },
            );
            Ending::Success
        }
        ClaimCellOutcome::AlreadyInitialized { owner, generation } => {
            out.emit(
                || format!("init refused: the cell is already initialized (owner={owner:?} generation={generation})"),
                || json!({ "outcome": "refused", "refusal": "already_initialized", "owner": owner, "generation": generation }),
            );
            Ending::Refused
        }
        ClaimCellOutcome::Unavailable => {
            note("the cell did not confirm its control journal in time: run init again");
            Ending::Unreachable
        }
        ClaimCellOutcome::Ambiguous => {
            note("the claim's answer never came: it may have won; run init again");
            Ending::Ambiguous
        }
    }
}
