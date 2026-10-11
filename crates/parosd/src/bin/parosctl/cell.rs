//! `parosctl cell add-machine <addr>` (#216): admit an idle machine into the
//! cell the servers belong to, through `paros::client::cell`. The machine
//! is registered in the cell control journal, then told its cell with
//! `Admit`. A machine never joins a cell on its own: this call is the
//! authority. A re-run resumes, and a machine already in the cell is left
//! unchanged.
//!
//! `parosctl cell drain-machine <machine>` (#474 (journals on admitted
//! machines)): mark a registered machine draining in the cell control
//! journal. The cell coordinator then moves every journal off it.

use std::time::Duration;

use clap::{Args, Subcommand};
use moonpool_core::TokioProviders;
use paros::client::Client;
use paros::client::bootstrap::{cell_members, control_journals};
use paros::client::cell::CellSession;
use paros::client::fleet::Step;
use paros::{Address, NodeId};
use serde_json::json;

use crate::Ending;
use crate::fleet::{interrupted, leader_seed, refused, steps};
use crate::labels::Labels;
use crate::output::{Printer, note};
use crate::views::Asker;

/// `parosctl cell`.
#[derive(Args, Debug)]
pub struct CellArgs {
    /// How long a step interrupted by a moving leader or a machine still
    /// starting is retried, in milliseconds.
    #[arg(long, default_value = "30000")]
    patience_ms: u64,
    #[command(subcommand)]
    pub command: CellAdminCommand,
}

#[derive(Subcommand, Debug)]
pub enum CellAdminCommand {
    /// Admit an idle machine into the cell: register it in the cell control
    /// journal, then send it `Admit`. Send it to any member (`--servers`).
    AddMachine {
        /// The idle machine's address, `HOST:PORT`: a literal or a name,
        /// resolved as it is dialed. The registry records the address the
        /// machine advertises (#257).
        addr: String,
    },
    /// Drain a registered machine: the cell coordinator moves every journal
    /// off it (#474). Send it to any member (`--servers`).
    DrainMachine {
        /// The machine's name, or its node id in full (16 hex digits).
        machine: String,
    },
    /// List the universe's cells and their state (#399).
    List,
    /// Show a cell: its state, its coordinator, its slots per class and
    /// its machines by standing (#399). The servers' cell; a name checks
    /// that the servers are that cell's.
    Show {
        /// The cell's name.
        cell: Option<String>,
    },
}

/// `parosctl cell …`, through `client` (the cell's `servers`, id and address
/// each).
pub async fn run(
    asker: &Asker<'_>,
    client: &Client<TokioProviders>,
    servers: &[(u64, Address)],
    labels: &Labels,
    out: &Printer,
    args: CellArgs,
) -> Ending {
    let addr = match args.command {
        CellAdminCommand::AddMachine { addr } => addr,
        CellAdminCommand::DrainMachine { machine } => {
            return drain(asker, client, servers, labels, out, &machine).await;
        }
        _ => unreachable!("the views are answered in crate::views"),
    };
    let target = match Address::parse(&addr) {
        Ok(target) => target,
        Err(error) => {
            note(&format!("bad machine address {addr:?}: {error}"));
            return Ending::Refused;
        }
    };
    let mut session = match session(asker, client, servers).await {
        Ok(session) => session,
        Err(ending) => return ending,
    };
    let journals = session.journals();
    let patience = Duration::from_millis(args.patience_ms);
    let run = session
        .add_machine(
            asker.providers,
            asker.rpc,
            client,
            asker.names,
            0,
            &target,
            patience,
        )
        .await;
    match run.outcome {
        Step::Done { result, .. } => {
            let outcome = if run.steps.is_empty() {
                "unchanged"
            } else {
                "admitted"
            };
            out.emit(
                || {
                    format!(
                        "{outcome} machine={target} cell={}{}",
                        labels.cell(journals.cell_id),
                        if run.steps.is_empty() {
                            String::new()
                        } else {
                            format!(" steps={}", steps(&run.steps).join(","))
                        }
                    )
                },
                || {
                    json!({
                        "outcome": outcome,
                        "node": result.0,
                        "cell": journals.cell_id,
                        "addr": target.to_string(),
                        "steps": steps(&run.steps),
                    })
                },
            );
            Ending::Success
        }
        Step::Refused(refusal) => refused(out, &refusal, labels),
        Step::Interrupted(stop) => interrupted(&stop),
        Step::Advanced(_) => unreachable!("a run never ends advanced"),
    }
}

/// A session over the cell the servers belong to: its control journals, and
/// its founding members among the servers.
async fn session(
    asker: &Asker<'_>,
    client: &Client<TokioProviders>,
    servers: &[(u64, Address)],
) -> Result<CellSession, Ending> {
    let Some(journals) = control_journals(client).await else {
        note("no server named its cell's control journals: is the cell initialized?");
        return Err(Ending::Unreachable);
    };
    // The founding members: the cell control journal's members, with the
    // addresses of the servers named among them.
    let Some(members) = cell_members(client, journals.cell).await else {
        note("no server served the cell control journal");
        return Err(Ending::Unreachable);
    };
    let founders: Vec<(NodeId, Address)> = servers
        .iter()
        .filter(|(id, _)| members.contains(id))
        .map(|(id, addr)| (NodeId(*id), addr.clone()))
        .collect();
    Ok(CellSession::new(
        journals,
        founders,
        leader_seed(asker.providers),
        client.tunables().checkpoint_policy(),
    ))
}

/// `parosctl cell drain-machine <machine>` (#474): `DrainNode` for the
/// registered machine `machine` names, unless it is draining already.
async fn drain(
    asker: &Asker<'_>,
    client: &Client<TokioProviders>,
    servers: &[(u64, Address)],
    labels: &Labels,
    out: &Printer,
    machine: &str,
) -> Ending {
    let mut session = match session(asker, client, servers).await {
        Ok(session) => session,
        Err(ending) => return ending,
    };
    if let Err(stop) = session.open(client, 0).await {
        return interrupted(&stop);
    }
    let registered: Vec<u64> = session.registry().nodes().map(|(id, _)| id.0).collect();
    let node = match labels.machine_named(machine, &registered) {
        Some(id) => NodeId(id),
        None => match paros::name::parse_full(machine) {
            Some(id) if registered.contains(&id) => NodeId(id),
            _ => {
                note(&format!("no registered machine is named {machine}"));
                return Ending::Refused;
            }
        },
    };
    match session.drain(client, 0, node).await {
        Ok(wrote) => {
            let outcome = if wrote { "draining" } else { "unchanged" };
            out.emit(
                || format!("{outcome} machine={}", labels.machine(node.0)),
                || json!({ "outcome": outcome, "node": node.0 }),
            );
            Ending::Success
        }
        Err(stop) => interrupted(&stop),
    }
}
