//! `parosctl tenant create|delete|list` (#229): the fleet operations of
//! `paros::client::fleet` against meta's directory and the cell's control
//! journal, and what `init` shares with them (the session, the outcomes'
//! endings and labels).
//!
//! The cell coordinator is whoever owns the cell control journal (claimed
//! at `init`), and in M9 it coordinates meta too (`docs/architecture.md`
//! §3.7: the fleet's one cell hosts meta): every operation here writes both
//! journals under its id. A crashed or interrupted operation is resumed by
//! running the same command again.

use std::fmt::Write as _;

use clap::{Args, Subcommand};
use moonpool_core::{Providers, RandomProvider, TokioProviders};
use paros::Read;
use paros::client::Client;
use paros::client::fleet::{FleetRefusal, FleetSession, Interrupted, Run, Stage, Step, read_meta};
use paros::machine::CELL_CONTROL;
use paros::system::Registry;
use paros::{NodeId, TenantId};
use serde_json::json;

use crate::Ending;
use crate::output::{Printer, note, record_text};

type ParosClient = Client<TokioProviders>;

/// How many tenant ids `tenant create` draws before it gives up on finding
/// a free one (a collision of random 64-bit ids is all but impossible: one
/// redraw is the realistic worst case).
const DRAWS: usize = 4;

/// `parosctl tenant`.
#[derive(Args, Debug)]
pub struct TenantArgs {
    #[command(subcommand)]
    command: TenantCommand,
}

#[derive(Subcommand, Debug)]
enum TenantCommand {
    /// Create a tenant (`REGISTERING` in meta, hosted by the cell, then
    /// `READY`). A re-run resumes an interrupted creation of the same name.
    Create {
        /// The tenant's name.
        name: String,
    },
    /// Remove a tenant (`REMOVING`, dropped by the cell, then removed). A
    /// re-run resumes.
    Delete {
        /// The tenant's name.
        name: String,
    },
    /// List meta's tenants and cells.
    List,
}

/// The cell coordinator: the owner of the cell control journal.
pub async fn coordinator(client: &ParosClient) -> Option<NodeId> {
    let read = Read {
        journal: CELL_CONTROL.journal.0,
        tenant: CELL_CONTROL.tenant.0,
        from_seq: 0,
        limit: 1,
        wait_ms: 0,
    };
    let state = client.read_any(&read, 0).await.outcome.state()?;
    state.owner.map(|owner| NodeId(owner.0))
}

/// A fleet session writing both control journals as `coordinator`, folding
/// the cell's journal over the genesis pool `servers`.
pub fn session(client: &ParosClient, coordinator: NodeId, servers: &[u64]) -> FleetSession {
    FleetSession::new(
        coordinator.0,
        coordinator,
        Registry::new(servers.iter().copied().map(NodeId)),
        client.tunables().checkpoint_policy(),
    )
}

/// A refusal's label and its meaning.
pub fn refusal_text(refusal: &FleetRefusal) -> String {
    match refusal {
        FleetRefusal::NoFleetId => "no_fleet_id: a fleet or cell id of 0".into(),
        FleetRefusal::CellIdUnknown => {
            "cell_id_unknown: the cell's id is recorded nowhere yet".into()
        }
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
        FleetRefusal::ReservedId => "reserved_id".into(),
        FleetRefusal::IdTaken => "id_taken".into(),
        FleetRefusal::NameBusy { tenant, state } => {
            format!("name_busy: tenant {} is {}", tenant.0, state.as_str())
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
    if let TenantCommand::List = args.command {
        return list(client, out).await;
    }
    let Some(coordinator) = coordinator(client).await else {
        note("the cell control journal has no owner: run parosctl init first");
        return Ending::Refused;
    };
    let mut fleet = session(client, coordinator, servers);
    match args.command {
        TenantCommand::Create { name } => {
            let mut run = None;
            for _ in 0..DRAWS {
                let draw = TenantId(
                    TenantId::FIRST_USER.0
                        + providers.random().random::<u64>() % (u64::MAX - TenantId::FIRST_USER.0),
                );
                let attempt = fleet.create_tenant(client, 0, name.as_bytes(), draw).await;
                if attempt.outcome != Step::Refused(FleetRefusal::IdTaken) {
                    run = Some(attempt);
                    break;
                }
            }
            let Some(run) = run else {
                note("every tenant id drawn was taken");
                return Ending::Refused;
            };
            report(out, "created", &name, run)
        }
        TenantCommand::Delete { name } => {
            let run = fleet.remove_tenant(client, 0, name.as_bytes()).await;
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

fn refused(out: &Printer, refusal: &FleetRefusal) -> Ending {
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

/// `parosctl tenant list`.
async fn list(client: &ParosClient, out: &Printer) -> Ending {
    let meta = match read_meta(client, 0).await {
        Ok(meta) => meta,
        Err(outcome) => {
            note(&format!("meta could not be read to its tail: {outcome:?}"));
            return Ending::Unreachable;
        }
    };
    out.emit(
        || {
            let mut text = format!(
                "fleet={}",
                meta.fleet()
                    .map_or_else(|| "none".to_string(), |f| f.to_string())
            );
            for (cell, entry) in meta.cells() {
                let _ = write!(text, "\ncell={cell} state={}", entry.state.as_str());
            }
            for (tenant, entry) in meta.tenants() {
                let _ = write!(
                    text,
                    "\ntenant={} name={} cell={} state={}",
                    tenant.0,
                    record_text(&entry.name),
                    entry.cell_id,
                    entry.state.as_str()
                );
            }
            text
        },
        || {
            json!({
                "fleet": meta.fleet(),
                "cells": meta.cells().map(|(cell, entry)| json!({
                    "cell": cell,
                    "state": entry.state.as_str(),
                })).collect::<Vec<_>>(),
                "tenants": meta.tenants().map(|(tenant, entry)| json!({
                    "tenant": tenant.0,
                    "name": record_text(&entry.name),
                    "cell": entry.cell_id,
                    "state": entry.state.as_str(),
                })).collect::<Vec<_>>(),
            })
        },
    );
    Ending::Success
}
