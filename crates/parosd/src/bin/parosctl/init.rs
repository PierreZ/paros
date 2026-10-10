//! `parosctl init` (#196, #216, #229, #277): the fleet's bootstrap — the
//! cell step, then the fleet steps.
//!
//! The cell step is `cell init` over the founding members (`--members`, the
//! servers by default): sent to the first listed machine still idle, which
//! drives the cell decree — a single-decree Paxos on the cell plan, every
//! listed machine an acceptor and all of them the quorum (`paros::machine`);
//! then `init` waits for the cell's first coordinator: the founding members
//! campaign in the cell's election journal, and the winner installs its uuid
//! on the cell control journal (#240). A re-run resumes: an interrupted
//! decree is finished by whichever listed machine is asked.
//!
//! The whole operation is `paros::client::initialize` (#246), which the
//! simulation runs too; this command prints what it came to.
//!
//! Then the fleet steps (#229, `paros::client::fleet`): the fleet tenant (served by the
//! cell's members) records the fleet's id — drawn here, kept on a re-run — and
//! adds the cell with its cell tenant, the cell records the fleet on its
//! side, and the fleet tenant marks the cell `READY`. No identifier is fixed (§3.8): a first
//! run takes them from the plan it formed and prints them, a re-run learns
//! them from the members' `Inspect`. Both journals are written under this
//! run's leader uuids (#241). Every step is idempotent: `init` is refused only when it
//! found nothing left to do.

use std::time::Duration;

use clap::Args;
use moonpool_core::TokioProviders;
use moonpool_rpc::RpcHandle;
use paros::client::Client;
use paros::client::initialize::{self, InitParams, InitRefusal, InitRun, Initialized, Unreachable};
use paros::{Address, Names};
use serde_json::json;

use crate::Ending;
use crate::fleet::{interrupted, leader_seed, nonzero, refusal_text, steps};
use crate::output::{Printer, note};
use paros::name::Abbreviations;

/// `parosctl init`.
#[derive(Args, Debug)]
pub struct InitArgs {
    /// The founding members, comma-separated `HOST:PORT`s: the idle
    /// machines the cell forms on, every one of them needed, each by the
    /// address it advertises (`PAROS_ADVERTISE`, #257: a machine refuses a
    /// list that does not name it so). A name that resolves to several
    /// machines stands for them all. The servers (`--servers`) by default.
    #[arg(long, value_delimiter = ',')]
    members: Vec<String>,
    /// How long the decree may take to form the cell, and the cell to elect
    /// its first leader, in milliseconds.
    #[arg(long, default_value = "30000")]
    patience_ms: u64,
    /// The universe's name, for people (#252, #399). A re-run never
    /// renames: the first name written stays.
    #[arg(long, default_value = "universe", value_parser = label)]
    universe_name: String,
    /// The cell's name, for people (#252, #399): unique among the
    /// universe's cells. A re-run never renames.
    #[arg(long, default_value = "cell-1", value_parser = label)]
    cell_name: String,
}

/// A name for people: no `/`, no space, no control character.
fn label(text: &str) -> Result<String, String> {
    paros::name::check_label(text)
        .map(|()| text.to_string())
        .map_err(|e| e.to_string())
}

impl InitArgs {
    /// The founding members, without duplicates: `--members`, or `servers`
    /// when none is given.
    ///
    /// # Errors
    ///
    /// A member that is malformed or does not resolve.
    pub fn members(&self, servers: &[Address]) -> Result<Vec<Address>, String> {
        if self.members.is_empty() {
            return Ok(servers.to_vec());
        }
        let mut members = Vec::new();
        for entry in self
            .members
            .iter()
            .map(|m| m.trim())
            .filter(|m| !m.is_empty())
        {
            for addr in
                crate::resolve::expand(entry).map_err(|e| format!("bad member {entry:?}: {e}"))?
            {
                if !members.contains(&addr) {
                    members.push(addr);
                }
            }
        }
        Ok(members)
    }
}

/// `parosctl init`: form the cell over `members`, wait for its coordinator
/// and run the fleet steps, through a client `connect` builds over its
/// members (`paros::client::initialize`).
pub async fn run(
    providers: &TokioProviders,
    rpc: &RpcHandle<TokioProviders>,
    names: &Names,
    members: &[Address],
    connect: impl Fn(&[(u64, Address)]) -> Client<TokioProviders>,
    out: &Printer,
    args: &InitArgs,
) -> Ending {
    let params = InitParams {
        patience: Duration::from_millis(args.patience_ms),
        fleet_id: nonzero(providers),
        leader_seed: leader_seed(providers),
        names: (args.universe_name.clone(), args.cell_name.clone()),
    };
    match initialize::initialize(providers, rpc, names, members, connect, params).await {
        InitRun::Initialized(done) => {
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
            note(&unreachable_text(why, members.first()));
            Ending::Unreachable
        }
        InitRun::Interrupted(stop) => interrupted(&stop),
    }
}

/// What `init` waited on in vain, for the operator.
fn unreachable_text(why: Unreachable, target: Option<&Address>) -> String {
    let target = target.map_or_else(|| "?".to_string(), ToString::to_string);
    match why {
        Unreachable::NoTarget => "no member to form the cell on".into(),
        Unreachable::Malformed => "a member answered with a plan that does not decode".into(),
        Unreachable::Formation => format!(
            "cell init over {target} and the other members decided nothing in time: a member \
             is not up yet; run it again"
        ),
        Unreachable::NothingAnswered => format!(
            "nothing answered init at {target}, or no cell answered inspect from a majority of \
             the members"
        ),
        Unreachable::NoControlJournals => "no server named its cell's control journals".into(),
        Unreachable::NoMembers => "no server described the cell control journal's members".into(),
        Unreachable::NoCoordinator => {
            "the cell elected no coordinator in time: a founding member is not up yet; run init \
             again"
                .into()
        }
    }
}

/// Print an initialized fleet: every control journal's identifier. Ids
/// print abbreviated (#239 (names at the edge)); `--json` prints them whole.
fn print_initialized(out: &Printer, done: &Initialized) {
    let journals = done.journals;
    let fleet_control = journals.fleet.map(|f| f.to_string()).unwrap_or_default();
    let election = journals.election.map(|e| e.to_string()).unwrap_or_default();
    out.emit(
        || {
            let ids = Abbreviations::new(
                [done.fleet_id, journals.cell_id, done.coordinator.0]
                    .into_iter()
                    .chain(journals.fleet.into_iter().chain(journals.election).chain([journals.cell]).flat_map(|j| [j.tenant.0, j.journal.0])),
            );
            format!(
                "initialized fleet={} cell={} coordinator={} members={} control=id:{} election={} fleet_control={} steps={}",
                ids.id(done.fleet_id),
                ids.id(journals.cell_id),
                ids.id(done.coordinator.0),
                done.servers.len(),
                ids.journal(journals.cell),
                journals.election.map_or_else(|| "none".to_string(), |e| format!("id:{}", ids.journal(e))),
                journals.fleet.map_or_else(|| "none".to_string(), |f| format!("id:{}", ids.journal(f))),
                steps(&done.steps).join(",")
            )
        },
        || {
            json!({
                "outcome": "initialized",
                "fleet": done.fleet_id,
                "cell": journals.cell_id,
                "coordinator": done.coordinator.0,
                "steps": steps(&done.steps),
                "control": journals.cell.to_string(),
                "election": election,
                "fleet_control": fleet_control,
                "members": done
                    .servers
                    .iter()
                    .map(|(id, addr)| json!({ "node": id, "addr": addr.to_string() }))
                    .collect::<Vec<_>>(),
            })
        },
    );
}
