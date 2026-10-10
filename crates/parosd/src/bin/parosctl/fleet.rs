//! `parosctl tenant create|delete|list` (#229): the fleet operations of
//! `paros::client::fleet` against the fleet directory and the cell's control
//! journal, and what `init` shares with them (the session, the outcomes'
//! endings and labels).
//!
//! No identifier is fixed (`docs/architecture.md` §3.8): the cell's and the fleet tenant's
//! control journals are learned from the servers' `Inspect`
//! (`paros::client::bootstrap::cell_frames`). Every operation here claims
//! both journals under a leader uuid of its own (#241), drawn per run, so it
//! fences whatever run led them before (the cell coordinator of #225 is not
//! built; §3.7: the fleet's one cell hosts the fleet tenant). An interrupted `init`
//! or `delete` is resumed by running the same command again; a tenant is
//! created once, so a second `create` of a name is refused, and an
//! interrupted creation stays `REGISTERING` until it is deleted (or, with
//! #225, finished by the coordinator).

use std::fmt::Write as _;
use std::time::Duration;

use clap::{Args, Subcommand};
use moonpool_core::{Providers, RandomProvider, TokioProviders};
use paros::client::Client;
use paros::client::bootstrap::control_journals;
use paros::client::fleet::{
    FleetRefusal, FleetSession, Interrupted, Run, Stage, Step, read_directory,
};
use paros::fleet::TenantState;
use paros::machine::ControlJournals;
use paros::system::Registry;
use paros::{JournalId, JournalIdentifier, NodeId, TenantId};
use serde_json::json;

use crate::Ending;
use crate::output::{Printer, note, record_text};

type ParosClient = Client<TokioProviders>;

/// How many tenant identifiers `tenant create` hands the library to try in turn
/// (a collision of random 64-bit ids is all but impossible: one redraw is
/// the realistic worst case).
const DRAWS: usize = 4;

/// `parosctl tenant`.
#[derive(Args, Debug)]
pub struct TenantArgs {
    /// How long a step interrupted by a moving leader is retried, in
    /// milliseconds.
    #[arg(long, default_value = "30000")]
    patience_ms: u64,
    #[command(subcommand)]
    command: TenantCommand,
}

#[derive(Subcommand, Debug)]
enum TenantCommand {
    /// Create a `users` tenant (`REGISTERING` in the fleet directory, hosted by the cell,
    /// then `READY`). A tenant is created once: a name the fleet tenant holds, in any
    /// state, is refused. The CLI never creates an `internal` tenant.
    Create {
        /// The tenant's name.
        name: String,
        /// What its journals survive: `az` (a zone, the default) or
        /// `region` (#252). Fixed at creation.
        #[arg(long, default_value = "az")]
        survives: paros::tenant::Survives,
    },
    /// Remove a `users` tenant (`REMOVING`, dropped by the cell, then
    /// removed). A re-run resumes.
    Delete {
        /// The tenant's name.
        name: String,
    },
    /// List the fleet directory's fleet, cells and tenants.
    List,
}

/// A random non-zero id.
pub fn nonzero(providers: &TokioProviders) -> u64 {
    loop {
        let id: u64 = providers.random().random();
        if id != 0 {
            return id;
        }
    }
}

/// A random leader seed: every run leads its own terms (#241).
pub fn leader_seed(providers: &TokioProviders) -> u128 {
    providers.random().random()
}

/// Whether `init` has claimed the cell control journal: someone leads it.
async fn initialized(client: &ParosClient, journals: &ControlJournals) -> Option<bool> {
    let state = client.journal_state(journals.cell, 0).await?;
    Some(state.leader.is_some())
}

/// A fleet session over `identifiers` writing both control journals as
/// leader uuids drawn from `seed`, folding the cell's journal over the genesis pool `servers`;
/// `None` when `identifiers` names no fleet journal.
pub fn session(
    client: &ParosClient,
    journals: ControlJournals,
    seed: u128,
    servers: &[u64],
) -> Option<FleetSession> {
    FleetSession::new(
        journals,
        seed,
        Registry::new(servers.iter().copied().map(NodeId)),
        client.tunables().checkpoint_policy(),
    )
}

/// A refusal's label and its meaning.
pub fn refusal_text(refusal: &FleetRefusal) -> String {
    match refusal {
        FleetRefusal::Unset => "unset: an id or an identifier of 0".into(),
        FleetRefusal::NotInitialized => "not_initialized: run parosctl init first".into(),
        FleetRefusal::CellMismatch { expected, found } => format!(
            "cell_mismatch: expected fleet={} cell={}, the cell records {}",
            expected.0,
            expected.1,
            found.map_or_else(
                || "nothing".to_string(),
                |f| format!("fleet={} cell={}", f.fleet_id, f.cell_id)
            )
        ),
        FleetRefusal::CellRemoving => "cell_removing".into(),
        FleetRefusal::IdTaken => "id_taken".into(),
        FleetRefusal::NameTaken { tenant, state } => format!(
            "name_taken: tenant {} is {}{}",
            tenant.0,
            state.as_str(),
            if *state == TenantState::Registering {
                " (an interrupted creation: delete it to create the name again)"
            } else {
                ""
            }
        ),
        FleetRefusal::OtherCell { node, cell_id } => format!(
            "other_cell: machine {} belongs to {}",
            node.0,
            if *cell_id == 0 {
                "another cell".to_string()
            } else {
                format!("cell {cell_id}")
            }
        ),
        FleetRefusal::InCellInit { node } => format!(
            "in_cell_init: machine {} promised in a cell init and has no cell: \
             finish that cell init, or wipe the machine",
            node.0
        ),
        FleetRefusal::Retired { node } => {
            format!("retired: machine {} was retired from this cell", node.0)
        }
        FleetRefusal::Removed { tenant } => {
            format!(
                "removed: tenant {} was removed while being created",
                tenant.0
            )
        }
    }
}

/// How an interrupted operation ends: ambiguous when a write may have
/// landed, unreachable when nothing was decided.
pub fn interrupted(stop: &Interrupted) -> Ending {
    note(&format!(
        "the operation was interrupted ({stop:?}): run the same command again, it resumes"
    ));
    match stop {
        Interrupted::NotWritten {
            outcome: paros::client::WriterOutcome::Ambiguous,
            ..
        } => Ending::Ambiguous,
        _ => Ending::Unreachable,
    }
}

/// The steps a run wrote, as labels.
pub fn steps(steps: &[Stage]) -> Vec<String> {
    steps.iter().map(|s| format!("{s:?}")).collect()
}

/// `parosctl tenant …`.
pub async fn run(
    providers: &TokioProviders,
    client: &ParosClient,
    servers: &[u64],
    out: &Printer,
    args: TenantArgs,
) -> Ending {
    let Some(journals) = control_journals(client).await else {
        note("no server named its cell's control journals: is the cell initialized?");
        return Ending::Unreachable;
    };
    let Some(fleet_journal) = journals.fleet else {
        note("the cell names no fleet journal");
        return Ending::Refused;
    };
    if let TenantCommand::List = args.command {
        return list(client, fleet_journal, out).await;
    }
    match initialized(client, &journals).await {
        Some(true) => {}
        Some(false) => {
            note("the cell control journal has no leader: run parosctl init first");
            return Ending::Refused;
        }
        None => {
            note("no server served the cell control journal's state");
            return Ending::Unreachable;
        }
    }
    let Some(mut fleet) = session(client, journals, leader_seed(providers), servers) else {
        note("the cell names no fleet journal");
        return Ending::Refused;
    };
    let patience = Duration::from_millis(args.patience_ms);
    match args.command {
        TenantCommand::Create { name, survives } => {
            // Random identifiers, the tenant's and its control journal's; the
            // library moves past one the fleet tenant holds already.
            let draws: Vec<JournalIdentifier> = (0..DRAWS)
                .map(|_| {
                    JournalIdentifier::new(
                        TenantId(nonzero(providers)),
                        JournalId(nonzero(providers)),
                    )
                })
                .collect();
            let mut fleet = fleet.with_survives(survives);
            let run = fleet
                .create_tenant(client, 0, name.as_bytes(), draws, patience)
                .await;
            report(out, "created", &name, run)
        }
        TenantCommand::Delete { name } => {
            let run = fleet
                .remove_tenant(client, 0, name.as_bytes(), patience)
                .await;
            match run.outcome {
                Step::Done { result: None, .. } => {
                    out.emit(
                        || format!("no tenant named {name}"),
                        || json!({ "outcome": "not_found", "name": name }),
                    );
                    Ending::Refused
                }
                Step::Done {
                    result: Some(tenant),
                    ..
                } => report(
                    out,
                    "deleted",
                    &name,
                    Run {
                        outcome: Step::Done {
                            result: tenant,
                            last: None,
                        },
                        steps: run.steps,
                    },
                ),
                Step::Refused(refusal) => refused(out, &refusal),
                Step::Interrupted(stop) => interrupted(&stop),
                Step::Advanced(_) => unreachable!("a run never ends advanced"),
            }
        }
        TenantCommand::List => unreachable!("listed above"),
    }
}

pub fn refused(out: &Printer, refusal: &FleetRefusal) -> Ending {
    let text = refusal_text(refusal);
    out.emit(
        || format!("refused: {text}"),
        || json!({ "outcome": "refused", "refusal": text }),
    );
    Ending::Refused
}

/// Print a create's or delete's run.
fn report(out: &Printer, done: &str, name: &str, run: Run<TenantId>) -> Ending {
    match run.outcome {
        Step::Done { result, .. } => {
            let already = run.steps.is_empty();
            out.emit(
                || {
                    format!(
                        "{} tenant={} name={name}{}",
                        if already { "unchanged" } else { done },
                        result.0,
                        if already {
                            String::new()
                        } else {
                            format!(" steps={}", steps(&run.steps).join(","))
                        }
                    )
                },
                || {
                    json!({
                        "outcome": if already { "unchanged" } else { done },
                        "tenant": result.0,
                        "name": name,
                        "steps": steps(&run.steps),
                    })
                },
            );
            Ending::Success
        }
        Step::Refused(refusal) => refused(out, &refusal),
        Step::Interrupted(stop) => interrupted(&stop),
        Step::Advanced(_) => unreachable!("a run never ends advanced"),
    }
}

/// `parosctl tenant list`: the fleet directory's fleet, its cells and every tenant — the
/// `internal` ones (the fleet tenant, the cell tenants) included, with their groups.
async fn list(client: &ParosClient, fleet: JournalIdentifier, out: &Printer) -> Ending {
    let directory = match read_directory(client, 0, fleet).await {
        Ok(directory) => directory,
        Err(outcome) => {
            note(&format!(
                "the fleet directory could not be read to its tail: {outcome:?}"
            ));
            return Ending::Unreachable;
        }
    };
    out.emit(
        || {
            let mut text = format!(
                "fleet={} fleet_control={}",
                directory.fleet()
                    .map_or_else(|| "none".to_string(), |f| f.to_string()),
                fleet
            );
            for (cell, entry) in directory.cells() {
                let _ = write!(text, "\ncell={cell} state={}", entry.state.as_str());
            }
            for (tenant, entry) in directory.tenants() {
                let _ = write!(
                    text,
                    "\ntenant={} control={} name={} groups={} cell={} state={}",
                    tenant.0,
                    JournalIdentifier::new(tenant, entry.control),
                    record_text(&entry.name),
                    entry.groups.label(),
                    entry.cell_id,
                    entry.state.as_str()
                );
            }
            text
        },
        || {
            json!({
                "fleet": directory.fleet(),
                "fleet_control": fleet.to_string(),
                "cells": directory.cells().map(|(cell, entry)| json!({
                    "cell": cell,
                    "state": entry.state.as_str(),
                    "cell_tenant": entry.control_tenant.0,
                })).collect::<Vec<_>>(),
                "tenants": directory.tenants().map(|(tenant, entry)| json!({
                    "tenant": tenant.0,
                    "control": JournalIdentifier::new(tenant, entry.control).to_string(),
                    "name": record_text(&entry.name),
                    "groups": entry.groups.iter().map(paros::fleet::Group::as_str).collect::<Vec<_>>(),
                    "cell": entry.cell_id,
                    "state": entry.state.as_str(),
                })).collect::<Vec<_>>(),
            })
        },
    );
    Ending::Success
}
