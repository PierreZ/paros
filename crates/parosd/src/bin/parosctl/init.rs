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
//! Then the fleet steps (#229, `paros::client::fleet`): meta (served by the
//! cell's seeds) records the fleet's id — drawn here, kept on a re-run — and
//! adds the cell with its cell tenant, the cell records the fleet on its
//! side, and meta marks the cell `READY`. No frame is fixed (§3.8): a first
//! run takes them from the plan it formed and prints them, a re-run learns
//! them from the seeds' `Inspect`. Both journals are written as the cell
//! coordinator. Every step is idempotent: `init` is refused only when it
//! found nothing left to do.

use std::net::SocketAddr;
use std::time::Duration;

use clap::Args;
use moonpool_core::TokioProviders;
use moonpool_rpc::RpcHandle;
use paros::client::Client;
use paros::client::bootstrap::{self, ClaimCellOutcome, InitOutcome};
use paros::client::fleet::{FleetFrames, Step};
use paros::{JournalKey, NodeId};
use serde_json::json;

use crate::Ending;
use crate::fleet::{interrupted, nonzero, refusal_text, session, steps};
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
    let (servers, coordinator, frames, users) =
        match bootstrap::init(providers, rpc, target, patience).await {
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
                let Some(meta) = plan.meta else {
                    note("the seed formed a cell that hosts no meta");
                    return Ending::Unreachable;
                };
                // The static assignment's user journals, drawn at `init` like
                // every frame: the only time they are printed.
                let users: Vec<JournalKey> = plan
                    .journals
                    .iter()
                    .copied()
                    .filter(|j| *j != plan.control && *j != meta)
                    .collect();
                let frames = FleetFrames {
                    cell_id: plan.cell_id,
                    cell: plan.control,
                    meta,
                };
                (servers, plan.coordinator(), frames, users)
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
                // No frame is fixed (§3.8): the cell's are learned from it.
                let Some(frames) = bootstrap::cell_frames(&client).await else {
                    note("no server named its cell's frames");
                    return Ending::Unreachable;
                };
                let Some(view) = client.inspect(0, frames.cell).await else {
                    note("no server described the cell control journal");
                    return Ending::Unreachable;
                };
                let Some(coordinator) = view.members.iter().copied().min() else {
                    note("the cell control journal names no member");
                    return Ending::Unreachable;
                };
                (servers, NodeId(coordinator), frames, Vec::new())
            }
        };
    let client = connect(&servers);
    let claimed = match bootstrap::claim_cell(&client, frames.cell, coordinator, patience).await {
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
        (coordinator, frames),
        &users,
        (claimed, patience),
        out,
    )
    .await
}

/// `init`'s fleet half, after the cell step, over the cell's `frames` as
/// its `coordinator`: `users` are the user journals this run's formation
/// drew (printed once), `claimed` the generation when this run claimed the
/// cell control journal, and `patience` how long a step a moving leader
/// interrupted is retried.
async fn fleet_steps(
    providers: &TokioProviders,
    client: &Client<TokioProviders>,
    servers: &[(u64, SocketAddr)],
    (coordinator, frames): (NodeId, FleetFrames),
    users: &[JournalKey],
    (claimed, patience): (Option<u64>, Duration),
    out: &Printer,
) -> Ending {
    let ids: Vec<u64> = servers.iter().map(|(id, _)| *id).collect();
    let mut fleet = session(client, frames, coordinator, &ids);
    let run = fleet.init(client, 0, nonzero(providers), patience).await;
    match run.outcome {
        Step::Done { .. } if claimed.is_none() && run.steps.is_empty() => {
            out.emit(
                || "init refused: the fleet is already initialized".to_string(),
                || json!({ "outcome": "refused", "refusal": "already_initialized" }),
            );
            Ending::Refused
        }
        Step::Done {
            result: fleet_id, ..
        } => {
            let cell_id = frames.cell_id;
            let users: Vec<String> = users.iter().map(ToString::to_string).collect();
            out.emit(
                || {
                    format!(
                        "initialized fleet={fleet_id} cell={cell_id} coordinator={} members={} control={} meta={} journals={} steps={}",
                        coordinator.0,
                        servers.len(),
                        frames.cell,
                        frames.meta,
                        users.join(","),
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
                        "control": frames.cell.to_string(),
                        "meta": frames.meta.to_string(),
                        "journals": users,
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
