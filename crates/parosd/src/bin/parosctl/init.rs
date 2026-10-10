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
use crate::output::{Printer, note, short};
use paros::name::{Abbreviations, full_hex};

/// `parosctl init`.
#[derive(Args, Debug)]
pub struct InitArgs {
    /// The founding members, comma-separated `HOST:PORT`s: the idle
    /// machines the cell forms on, every one of them needed (a name that
    /// resolves to several machines stands for them all). The servers
    /// (`--servers`) by default.
    #[arg(long, value_delimiter = ',')]
    members: Vec<String>,
    /// How long the decree may take to form the cell, and the cell to elect
    /// its first leader, in milliseconds.
    #[arg(long, default_value = "30000")]
    patience_ms: u64,
}

impl InitArgs {
    /// The founding members, resolved, without duplicates: `--members`, or
    /// `servers` when none is given.
    ///
    /// # Errors
    ///
    /// A member that does not resolve.
    pub fn members(&self, servers: &[SocketAddr]) -> Result<Vec<SocketAddr>, String> {
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
            for addr in crate::resolve::resolve_all(entry)
                .map_err(|e| format!("bad member {entry:?}: {e}"))?
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
    members: &[SocketAddr],
    connect: impl Fn(&[(u64, SocketAddr)]) -> Client<TokioProviders>,
    out: &Printer,
    args: &InitArgs,
) -> Ending {
    let params = InitParams {
        patience: Duration::from_millis(args.patience_ms),
        fleet_id: nonzero(providers),
        leader_seed: leader_seed(providers),
    };
    match initialize::initialize(providers, rpc, members, connect, params).await {
        InitRun::Initialized(done) => {
            if !done.users.is_empty() {
                note(&format!(
                    "formed cell {} over {} members",
                    short(done.journals.cell_id),
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
            note(&unreachable_text(why, members.first()));
            Ending::Unreachable
        }
        InitRun::Interrupted(stop) => interrupted(&stop),
    }
}

/// What `init` waited on in vain, for the operator.
fn unreachable_text(why: Unreachable, target: Option<&SocketAddr>) -> String {
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

/// Print an initialized universe: every identifier, the only time the user
/// journals are named. Ids print abbreviated (#239), but a user journal
/// prints whole, `id:TENANT/JOURNAL` in hex: no later listing knows it, so
/// no prefix of it resolves (until #210 names every user journal).
fn print_initialized(out: &Printer, done: &Initialized) {
    let journals = done.journals;
    let fleet_control = journals.fleet.map(|f| f.to_string()).unwrap_or_default();
    let election = journals.election.map(|e| e.to_string()).unwrap_or_default();
    let users: Vec<String> = done.users.iter().map(ToString::to_string).collect();
    out.emit(
        || {
            let ids = Abbreviations::new(
                [done.fleet_id, journals.cell_id, done.coordinator.0]
                    .into_iter()
                    .chain(journals.fleet.into_iter().chain(journals.election).chain([journals.cell]).flat_map(|j| [j.tenant.0, j.journal.0])),
            );
            let users: Vec<String> = done
                .users
                .iter()
                .map(|j| format!("id:{}/{}", full_hex(j.tenant.0), full_hex(j.journal.0)))
                .collect();
            format!(
                "initialized universe={} cell={} coordinator={} members={} control=id:{} election={} universe_control={} journals={} steps={}",
                ids.id(done.fleet_id),
                ids.id(journals.cell_id),
                ids.id(done.coordinator.0),
                done.servers.len(),
                ids.journal(journals.cell),
                journals.election.map_or_else(|| "none".to_string(), |e| format!("id:{}", ids.journal(e))),
                journals.fleet.map_or_else(|| "none".to_string(), |f| format!("id:{}", ids.journal(f))),
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
                "steps": steps(&done.steps),
                "control": journals.cell.to_string(),
                "election": election,
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
