//! `parosctl init` (#196, #216, #229): the fleet's bootstrap.
//!
//! Sent to the first server — a waiting seed every seed's join list names —
//! which identifies every seed, mints the cell's id and forms every seed
//! (`paros::machine`); then the first cell coordinator claims the cell
//! control journal with `SetLeader(expected_gen = 0)`
//! (`paros::client::bootstrap::claim_cell`), and runs the fleet steps:
//! meta records the cell (minting the fleet's id, drawn here), the cell
//! records its half, meta marks the cell `READY`
//! (`paros::client::bootstrap::register_fleet`). A re-run resumes: a seed
//! that already serves the cell is asked for it (its `Inspect` names the
//! cell), the claim is made if it is still missing, and the fleet steps
//! pick up where they stopped. A cell whose fleet steps are all done was
//! initialized before, and `init` is refused.

use std::net::SocketAddr;
use std::time::Duration;

use clap::Args;
use moonpool_core::{Providers, RandomProvider, TokioProviders};
use moonpool_rpc::RpcHandle;
use paros::NodeId;
use paros::client::Client;
use paros::client::bootstrap::{self, ClaimCellOutcome, InitOutcome, RegisterFleetOutcome};
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
            (servers, plan.coordinator(), plan.cell_id)
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
            note(&format!(
                "init at {target} decided nothing in time: a seed is not up yet; run it again"
            ));
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
            if view.cell_id == 0 {
                note("the cell control journal's server names no cell");
                return Ending::Unreachable;
            }
            (servers, NodeId(coordinator), view.cell_id)
        }
    };
    let client = connect(&servers);
    let claimed = match bootstrap::claim_cell(&client, coordinator, patience).await {
        // Claimed now, or by the coordinator in an earlier run: the fleet
        // steps run (or resume).
        ClaimCellOutcome::Claimed { generation } | ClaimCellOutcome::Owned { generation } => {
            generation
        }
        ClaimCellOutcome::AlreadyInitialized { owner, generation } => {
            out.emit(
                || format!("init refused: the cell is already initialized (owner={owner:?} generation={generation})"),
                || json!({ "outcome": "refused", "refusal": "already_initialized", "owner": owner, "generation": generation }),
            );
            return Ending::Refused;
        }
        ClaimCellOutcome::Unavailable => {
            note("the cell did not confirm its control journal in time: run init again");
            return Ending::Unreachable;
        }
        ClaimCellOutcome::Ambiguous => {
            note("the claim's answer never came: it may have won; run init again");
            return Ending::Ambiguous;
        }
    };
    // The fleet's id, if this run is the one that mints it (non-zero).
    let fleet_draw = loop {
        let id: u64 = providers.random().random();
        if id != 0 {
            break id;
        }
    };
    let claimed = Claimed {
        coordinator,
        cell,
        generation: claimed,
        fleet_draw,
    };
    fleet_steps(&client, out, &claimed, &servers, patience).await
}

/// The cell control journal claimed, as `init` resumes from it.
#[derive(Clone, Copy)]
struct Claimed {
    coordinator: NodeId,
    cell: u64,
    generation: u64,
    fleet_draw: u64,
}

/// `init`'s fleet steps, as the coordinator that claimed the cell control
/// journal: register the cell in meta and in itself.
async fn fleet_steps(
    client: &Client<TokioProviders>,
    out: &Printer,
    claimed: &Claimed,
    servers: &[(u64, SocketAddr)],
    patience: Duration,
) -> Ending {
    let Claimed {
        coordinator,
        cell,
        generation: claimed,
        fleet_draw,
    } = *claimed;
    match bootstrap::register_fleet(client, coordinator, cell, fleet_draw, patience).await {
        RegisterFleetOutcome::Registered { context } => {
            out.emit(
                || {
                    format!(
                        "initialized fleet {} cell {} coordinator={} generation={claimed} members={}",
                        context.fleet_id,
                        context.cell_id,
                        coordinator.0,
                        servers.len()
                    )
                },
                || {
                    json!({
                        "outcome": "initialized",
                        "fleet": context.fleet_id,
                        "cell": context.cell_id,
                        "coordinator": coordinator.0,
                        "generation": claimed,
                        "members": servers
                            .iter()
                            .map(|(id, addr)| json!({ "node": id, "addr": addr.to_string() }))
                            .collect::<Vec<_>>(),
                    })
                },
            );
            Ending::Success
        }
        RegisterFleetOutcome::AlreadyRegistered { context } => {
            out.emit(
                || format!("init refused: the cell is already initialized (fleet={} cell={})", context.fleet_id, context.cell_id),
                || json!({ "outcome": "refused", "refusal": "already_initialized", "fleet": context.fleet_id, "cell": context.cell_id }),
            );
            Ending::Refused
        }
        RegisterFleetOutcome::Refused(refusal) => {
            out.emit(
                || format!("init refused: {refusal:?}"),
                || json!({ "outcome": "refused", "refusal": format!("{refusal:?}") }),
            );
            Ending::Refused
        }
        RegisterFleetOutcome::Unavailable => {
            note("meta or the cell control journal did not answer in time: run init again");
            Ending::Unreachable
        }
    }
}
