//! `parosctl tenant create|delete|list` (#229): the fleet's tenants,
//! through meta — `paros::client::fleet`'s resumable operations. A re-run
//! of an interrupted `create` or `delete` resumes it.

use std::time::Duration;

use clap::{Args, Subcommand};
use moonpool_core::{Providers, RandomProvider, TokioProviders};
use paros::client::Client;
use paros::client::fleet::{self, FleetStep, TenantCreation, TenantRemoval};
use paros::system::TenantState;
use paros::{AcceptorConfig, NodeId, QuorumSystem, TenantId};
use serde_json::json;

use crate::Ending;
use crate::output::{Printer, note};

/// `parosctl tenant`.
#[derive(Subcommand, Debug)]
pub enum TenantCommand {
    /// Create a tenant: registered in meta, hosted by the cell, then ready.
    Create(CreateArgs),
    /// Remove a tenant: removing in meta, unhosted by its cell, forgotten.
    Delete(DeleteArgs),
    /// List the fleet's tenants and cells, as meta records them.
    List,
}

/// What every tenant operation takes.
#[derive(Args, Debug)]
pub struct Operation {
    /// The client writing meta and the cell control journal.
    #[arg(long, env = "PAROSCTL_OWNER", default_value = "1")]
    owner: u64,
    /// How long an unavailable step is retried, in milliseconds.
    #[arg(long, default_value = "30000")]
    patience_ms: u64,
}

/// `parosctl tenant create`.
#[derive(Args, Debug)]
pub struct CreateArgs {
    /// The tenant's name.
    name: String,
    #[command(flatten)]
    operation: Operation,
}

/// `parosctl tenant delete`.
#[derive(Args, Debug)]
pub struct DeleteArgs {
    /// The tenant's name.
    name: String,
    #[command(flatten)]
    operation: Operation,
}

/// A tenant id in the user range, drawn at random.
fn draw(providers: &TokioProviders) -> TenantId {
    loop {
        let id = TenantId(providers.random().random());
        if id.is_user() {
            return id;
        }
    }
}

fn state_name(state: TenantState) -> &'static str {
    match state {
        TenantState::Registering => "registering",
        TenantState::Ready => "ready",
        TenantState::Removing => "removing",
        TenantState::UpdatingConfiguration => "updating_configuration",
        TenantState::Renaming => "renaming",
        TenantState::Error => "error",
    }
}

/// Report how an operation that did not finish ended.
fn unfinished(out: &Printer, verb: &str, step: &FleetStep) -> Ending {
    if let FleetStep::Refused(refusal) = step {
        out.emit(
            || format!("{verb} refused: {refusal:?}"),
            || json!({ "outcome": "refused", "refusal": format!("{refusal:?}") }),
        );
        return Ending::Refused;
    }
    note(&format!(
        "{verb} did not finish in time: run it again to resume"
    ));
    Ending::Unreachable
}

/// Run `command` against the cell through `client`.
pub async fn run(
    providers: &TokioProviders,
    client: &Client<TokioProviders>,
    out: &Printer,
    command: TenantCommand,
) -> Ending {
    match command {
        TenantCommand::Create(args) => create(providers, client, out, args).await,
        TenantCommand::Delete(args) => {
            let patience = Duration::from_millis(args.operation.patience_ms);
            let mut removal = TenantRemoval::new(args.operation.owner, args.name.into_bytes());
            match removal.run(client, 0, patience).await {
                FleetStep::Done(_) => {
                    let tenant = removal.tenant().map(|t| t.0);
                    out.emit(
                        || format!("tenant {} removed", tenant.unwrap_or_default()),
                        || json!({ "outcome": "removed", "tenant": tenant }),
                    );
                    Ending::Success
                }
                step => unfinished(out, "delete", &step),
            }
        }
        TenantCommand::List => list(client, out).await,
    }
}

/// `parosctl tenant create`: the tenant's control journal on the servers this command talks to (the
/// seeds), until the cell coordinator places it (#212).
async fn create(
    providers: &TokioProviders,
    client: &Client<TokioProviders>,
    out: &Printer,
    args: CreateArgs,
) -> Ending {
    let patience = Duration::from_millis(args.operation.patience_ms);
    // The tenant's control journal runs on the servers this command
    // talks to — the seeds — until the cell coordinator places it
    // (#212).
    let mut members: Vec<NodeId> = (0..client.server_count())
        .map(|i| NodeId(client.id_of(i)))
        .collect();
    members.sort_unstable();
    members.dedup();
    let control = AcceptorConfig::new(members, QuorumSystem::Majority);
    let mut creation = TenantCreation::new(
        args.operation.owner,
        args.name.into_bytes(),
        draw(providers),
        control,
    );
    let step = creation.run(client, 0, patience, || draw(providers)).await;
    match (step, creation.tenant()) {
        (FleetStep::Done(context), Some(tenant)) => {
            out.emit(
                || {
                    format!(
                        "tenant {} ready in cell {}{}",
                        tenant.0,
                        context.cell_id,
                        if creation.adopted() { " (resumed)" } else { "" }
                    )
                },
                || {
                    json!({
                        "outcome": "ready",
                        "tenant": tenant.0,
                        "cell": context.cell_id,
                        "fleet": context.fleet_id,
                        "resumed": creation.adopted(),
                    })
                },
            );
            Ending::Success
        }
        (step, _) => unfinished(out, "create", &step),
    }
}

/// `parosctl tenant list`: the fleet, its cells and its tenants, as meta records them.
async fn list(client: &Client<TokioProviders>, out: &Printer) -> Ending {
    let Some(meta) = fleet::load_meta(client, 0).await else {
        note("no server served meta");
        return Ending::Unreachable;
    };
    out.emit(
        || {
            let mut lines = vec![format!(
                "fleet {}",
                meta.fleet_id().map_or_else(|| "-".into(), |f| f.to_string())
            )];
            for (cell, entry) in meta.cells() {
                lines.push(format!("cell {cell} {:?}", entry.state));
            }
            for (tenant, entry) in meta.tenants() {
                lines.push(format!(
                    "tenant {} name={} cell={} state={}",
                    tenant.0,
                    String::from_utf8_lossy(&entry.name),
                    entry.cell_id,
                    state_name(entry.state)
                ));
            }
            lines.join("\n")
        },
        || {
            json!({
                "fleet": meta.fleet_id(),
                "cells": meta
                    .cells()
                    .map(|(cell, entry)| json!({ "cell": cell, "state": format!("{:?}", entry.state) }))
                    .collect::<Vec<_>>(),
                "tenants": meta
                    .tenants()
                    .map(|(tenant, entry)| json!({
                        "tenant": tenant.0,
                        "name": String::from_utf8_lossy(&entry.name),
                        "cell": entry.cell_id,
                        "state": state_name(entry.state),
                    }))
                    .collect::<Vec<_>>(),
            })
        },
    );
    Ending::Success
}
