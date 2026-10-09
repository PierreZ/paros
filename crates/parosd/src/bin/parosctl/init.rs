//! `parosctl init` (#196, #216, #229): the fleet's bootstrap — the cell
//! step, then the fleet steps.
//!
//! Sent to the first server — a waiting seed every seed's join list names —
//! which identifies every seed, mints the cell's id and forms every seed
//! (`paros::machine`); then `init` claims the cell control journal with
//! `SetLeader(new, old = none)` under a leader uuid of its own
//! (`paros::client::bootstrap::claim_cell`). A re-run resumes: a seed that
//! already serves the cell is asked for it, and the claim is made if it is
//! still missing.
//!
//! The whole operation is `paros::client::initialize` (#246), which the
//! simulation runs too; this command prints what it came to.
//!
//! Then the fleet steps (#229, `paros::client::fleet`): the fleet tenant (served by the
//! cell's seeds) records the fleet's id — drawn here, kept on a re-run — and
//! adds the cell with its cell tenant, the cell records the fleet on its
//! side, and the fleet tenant marks the cell `READY`. No identifier is fixed (§3.8): a first
//! run takes them from the plan it formed and prints them, a re-run learns
//! them from the seeds' `Inspect`. Both journals are written under this
//! run's leader uuids (#241). Every step is idempotent: `init` is refused only when it
//! found nothing left to do.

use std::net::SocketAddr;
use std::time::Duration;

use clap::Args;
use moonpool_core::TokioProviders;
use moonpool_rpc::RpcHandle;
use paros::client::Client;
use paros::client::initialize::{self, InitParams, InitRefusal, InitRun, Initialized, Unreachable};
use serde_json::json;

use crate::Ending;
use crate::fleet::{interrupted, leader_seed, nonzero, refusal_text, steps};
use crate::output::{Printer, note};

/// `parosctl init`.
#[derive(Args, Debug)]
pub struct InitArgs {
    /// How long the seed may take to form the cell, and the cell to elect
    /// its first leader, in milliseconds.
    #[arg(long, default_value = "30000")]
    patience_ms: u64,
}

/// `parosctl init`: form the cell at `addrs[0]`, claim its control journal
/// and run the fleet steps, through a client `connect` builds over its
/// members (`paros::client::initialize`).
pub async fn run(
    providers: &TokioProviders,
    rpc: &RpcHandle<TokioProviders>,
    addrs: &[SocketAddr],
    connect: impl Fn(&[(u64, SocketAddr)]) -> Client<TokioProviders>,
    out: &Printer,
    args: InitArgs,
) -> Ending {
    let params = InitParams {
        patience: Duration::from_millis(args.patience_ms),
        fleet_id: nonzero(providers),
        leader_seed: leader_seed(providers),
    };
    match initialize::initialize(providers, rpc, addrs, connect, params).await {
        InitRun::Initialized(done) => {
            if !done.users.is_empty() {
                note(&format!(
                    "formed cell {} over {} seeds",
                    done.journals.cell_id,
                    done.servers.len()
                ));
            }
            print_initialized(out, &done);
            Ending::Success
        }
        InitRun::AlreadyInitialized(_) => {
            out.emit(
                || "init refused: the fleet is already initialized".to_string(),
                || json!({ "outcome": "refused", "refusal": "already_initialized" }),
            );
            Ending::Refused
        }
        InitRun::Refused(InitRefusal::Formation(refusal)) => {
            out.emit(
                || format!("init refused: {refusal}"),
                || json!({ "outcome": "refused", "refusal": refusal }),
            );
            Ending::Refused
        }
        InitRun::Refused(InitRefusal::NoFleet) => {
            note("the cell names no fleet journal");
            Ending::Refused
        }
        InitRun::Refused(InitRefusal::Fleet(refusal)) => {
            let text = refusal_text(&refusal);
            out.emit(
                || format!("init refused: {text}"),
                || json!({ "outcome": "refused", "refusal": text }),
            );
            Ending::Refused
        }
        InitRun::Unreachable(why) => {
            note(&unreachable_text(why, addrs.first()));
            Ending::Unreachable
        }
        InitRun::Ambiguous => {
            note("the claim's answer never came: it may have won; run init again");
            Ending::Ambiguous
        }
        InitRun::Interrupted(stop) => interrupted(&stop),
    }
}

/// What `init` waited on in vain, for the operator.
fn unreachable_text(why: Unreachable, target: Option<&SocketAddr>) -> String {
    let target = target.map_or_else(|| "?".to_string(), ToString::to_string);
    match why {
        Unreachable::NoTarget => "no server to send init to".into(),
        Unreachable::Malformed => "the seed answered with a plan that does not decode".into(),
        Unreachable::Formation => {
            format!("init at {target} decided nothing in time: a seed is not up yet; run it again")
        }
        Unreachable::NothingAnswered => format!("nothing answered init or inspect at {target}"),
        Unreachable::NoControlJournals => "no server named its cell's control journals".into(),
        Unreachable::NoCoordinator => {
            "no server described the cell control journal's members".into()
        }
        Unreachable::Claim => {
            "the cell did not confirm its control journal in time: run init again".into()
        }
    }
}

/// Print an initialized fleet: every identifier, the only time the user
/// journals are named.
fn print_initialized(out: &Printer, done: &Initialized) {
    let journals = done.journals;
    let fleet_control = journals.fleet.map(|f| f.to_string()).unwrap_or_default();
    let users: Vec<String> = done.users.iter().map(ToString::to_string).collect();
    out.emit(
        || {
            format!(
                "initialized fleet={} cell={} coordinator={} members={} control={} fleet_control={} journals={} steps={}",
                done.fleet_id,
                journals.cell_id,
                done.coordinator.0,
                done.servers.len(),
                journals.cell,
                fleet_control,
                users.join(","),
                steps(&done.steps).join(",")
            )
        },
        || {
            json!({
                "outcome": "initialized",
                "fleet": done.fleet_id,
                "cell": journals.cell_id,
                "coordinator": done.coordinator.0,
                "leader": done.claimed.map(|leader| leader.to_string()),
                "steps": steps(&done.steps),
                "control": journals.cell.to_string(),
                "fleet_control": fleet_control,
                "journals": users,
                "members": done
                    .servers
                    .iter()
                    .map(|(id, addr)| json!({ "node": id, "addr": addr.to_string() }))
                    .collect::<Vec<_>>(),
            })
        },
    );
}
