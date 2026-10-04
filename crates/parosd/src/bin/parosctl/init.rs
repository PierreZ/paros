//! `parosctl init` (#196, #216, #229): the fleet's bootstrap — the cell
//! step, then the fleet steps.
//!
//! Sent to the first server — a waiting seed every seed's join list names —
//! which identifies every seed, mints the cell's id and forms every seed
//! (`paros::machine`); then the first cell coordinator claims the cell
//! control journal with `SetLeader(expected_gen = 0)`
//! (`paros::client::bootstrap::claim_cell`). A re-run resumes: a seed that
//! already serves the cell is asked for it, and the claim is made if it is
//! still missing.
//!
//! Then the fleet steps (#229, `paros::client::fleet`): the cell records the
//! fleet on its side, meta (`1/1`, served by the cell's seeds) records the
//! fleet's id — drawn here, kept from the cell's side on a re-run — and
//! adds the cell, `READY`. Both journals are written as the cell
//! coordinator. Every step is idempotent: `init` is refused only when it
//! found nothing left to do.

use std::net::SocketAddr;
use std::time::Duration;

use clap::Args;
use moonpool_core::TokioProviders;
use moonpool_core::{Providers, RandomProvider};
use moonpool_rpc::RpcHandle;
use paros::NodeId;
use paros::client::Client;
use paros::client::bootstrap::{self, ClaimCellOutcome, InitOutcome};
use paros::client::fleet::Step;
use paros::machine::CELL_CONTROL;
use serde_json::json;

use crate::Ending;
use crate::fleet::{interrupted, refusal_text, session, steps};
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
            (servers, NodeId(coordinator), None)
        }
    };
    let client = connect(&servers);
    let claimed = match bootstrap::claim_cell(&client, coordinator, patience).await {
        ClaimCellOutcome::Claimed { generation } => Some(generation),
        // Claimed by an earlier run: the fleet steps resume, and decide
        // whether anything was left to do.
        ClaimCellOutcome::AlreadyInitialized { .. } => None,
        ClaimCellOutcome::Unavailable => {
            note("the cell did not confirm its control journal in time: run init again");
            return Ending::Unreachable;
        }
        ClaimCellOutcome::Ambiguous => {
            note("the claim's answer never came: it may have won; run init again");
            return Ending::Ambiguous;
        }
    };
    fleet_steps(
        providers,
        &client,
        &servers,
        coordinator,
        cell,
        claimed,
        out,
    )
    .await
}

/// `init`'s fleet half, after the cell step: `cell` is the cell's id when
/// this run formed it, `claimed` the generation when this run claimed the
/// cell control journal.
async fn fleet_steps(
    providers: &TokioProviders,
    client: &Client<TokioProviders>,
    servers: &[(u64, SocketAddr)],
    coordinator: NodeId,
    cell: Option<u64>,
    claimed: Option<u64>,
    out: &Printer,
) -> Ending {
    let fleet_draw = loop {
        let id: u64 = providers.random().random();
        if id != 0 {
            break id;
        }
    };
    let ids: Vec<u64> = servers.iter().map(|(id, _)| *id).collect();
    let mut fleet = session(client, coordinator, &ids);
    let run = fleet.init(client, 0, cell, fleet_draw).await;
    match run.outcome {
        Step::Done { .. } if claimed.is_none() && run.steps.is_empty() => {
            out.emit(
                || "init refused: the fleet is already initialized".to_string(),
                || json!({ "outcome": "refused", "refusal": "already_initialized" }),
            );
            Ending::Refused
        }
        Step::Done {
            result: (fleet_id, cell_id),
            ..
        } => {
            out.emit(
                || {
                    format!(
                        "initialized fleet={fleet_id} cell={cell_id} coordinator={} members={} steps={}",
                        coordinator.0,
                        servers.len(),
                        steps(&run.steps).join(",")
                    )
                },
                || {
                    json!({
                        "outcome": "initialized",
                        "fleet": fleet_id,
                        "cell": cell_id,
                        "coordinator": coordinator.0,
                        "generation": claimed,
                        "steps": steps(&run.steps),
                        "members": servers
                            .iter()
                            .map(|(id, addr)| json!({ "node": id, "addr": addr.to_string() }))
                            .collect::<Vec<_>>(),
                    })
                },
            );
            Ending::Success
        }
        Step::Refused(refusal) => {
            let text = refusal_text(&refusal);
            out.emit(
                || format!("init refused: {text}"),
                || json!({ "outcome": "refused", "refusal": text }),
            );
            Ending::Refused
        }
        Step::Interrupted(stop) => interrupted(&stop),
        Step::Advanced(_) => unreachable!("a run never ends advanced"),
    }
}
