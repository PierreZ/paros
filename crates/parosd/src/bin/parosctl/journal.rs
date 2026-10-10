//! `parosctl journal create|delete|list` (#210): a tenant's journals, over
//! `paros::client::journals`.
//!
//! The tenant is named; its id and its control journal come from the fleet
//! directory. A create or a delete is a request to the tenant coordinator
//! with an idempotency id drawn per run: the library re-sends the same id
//! until an answer decides it, so a request acts once. The coordinator draws
//! the journal's id and picks its members: a caller names a desired mode,
//! never members. A list folds the tenant's control journal.

use std::time::Duration;

use clap::{Args, Subcommand};
use moonpool_core::TokioProviders;
use moonpool_rpc::RpcHandle;
use paros::client::Client;
use paros::client::bootstrap::control_journals;
use paros::client::fleet::read_directory;
use paros::client::journals::{self, JournalAnswer, JournalOp, JournalRequest};
use paros::fleet::TenantState;
use paros::name::full_hex;
use paros::tenant::Desired;
use paros::{JournalIdentifier, TenantId, WriterMode};
use serde_json::json;

use crate::Ending;
use crate::fleet::nonzero;
use crate::output::{Printer, note, record_text};

type ParosClient = Client<TokioProviders>;

/// `parosctl journal`.
#[derive(Args, Debug)]
pub struct JournalArgs {
    /// How long a request is sent again while no coordinator decides it,
    /// in milliseconds.
    #[arg(long, default_value = "30000")]
    patience_ms: u64,
    #[command(subcommand)]
    command: JournalCommand,
}

#[derive(Subcommand, Debug)]
enum JournalCommand {
    /// Create a journal in a `READY` tenant. The coordinator draws its id
    /// and picks its members; a live journal of the same name is refused
    /// (`name_taken`).
    Create {
        /// The tenant's name.
        tenant: String,
        /// The journal's name.
        name: String,
        /// Who may write it: `single` (one fenced writer, the default) or
        /// `multi` (unfenced appends, #241). Fixed for the journal's life.
        #[arg(long, default_value = "single", value_parser = parse_mode)]
        mode: WriterMode,
        /// The desired mode: `single`, `double` (the default), `triple` or
        /// `grid:RxC`.
        #[arg(long, default_value = "double")]
        desired: Desired,
    },
    /// Delete a tenant's live journal by name: a tombstone, its id never
    /// used again.
    Delete {
        /// The tenant's name.
        tenant: String,
        /// The journal's name.
        name: String,
    },
    /// List a tenant's journals, deleted ones included.
    List {
        /// The tenant's name.
        tenant: String,
    },
}

/// A writer mode as given on the command line.
fn parse_mode(text: &str) -> Result<WriterMode, String> {
    match text {
        "single" => Ok(WriterMode::Single),
        "multi" => Ok(WriterMode::Multi),
        _ => Err("a mode is single or multi".into()),
    }
}

/// `parosctl journal …`.
pub async fn run(
    providers: &TokioProviders,
    rpc: &RpcHandle<TokioProviders>,
    client: &ParosClient,
    out: &Printer,
    args: JournalArgs,
) -> Ending {
    let Some(cell) = control_journals(client).await else {
        note("no server named its cell's control journals: is the cell initialized?");
        return Ending::Unreachable;
    };
    let (Some(fleet), Some(election)) = (cell.fleet, cell.election) else {
        note("the cell names no fleet journal or no election journal");
        return Ending::Refused;
    };
    let directory = match read_directory(client, 0, fleet).await {
        Ok(directory) => directory,
        Err(outcome) => {
            note(&format!(
                "the fleet directory could not be read to its tail: {outcome:?}"
            ));
            return Ending::Unreachable;
        }
    };
    let tenant_name = match &args.command {
        JournalCommand::Create { tenant, .. }
        | JournalCommand::Delete { tenant, .. }
        | JournalCommand::List { tenant } => tenant.clone(),
    };
    let Some((tenant, entry)) = directory.named(tenant_name.as_bytes()) else {
        out.emit(
            || format!("no tenant named {tenant_name}"),
            || json!({ "outcome": "unknown_tenant", "tenant": tenant_name }),
        );
        return Ending::Refused;
    };
    if entry.state != TenantState::Ready {
        out.emit(
            || format!("tenant {tenant_name} is {}", entry.state.as_str()),
            || json!({ "outcome": "not_ready", "tenant": tenant_name, "state": entry.state.as_str() }),
        );
        return Ending::Refused;
    }
    let control = entry.control;
    let op = match args.command {
        JournalCommand::List { .. } => return list(client, tenant, control, out).await,
        JournalCommand::Create {
            name,
            mode,
            desired,
            ..
        } => JournalOp::Create {
            name: name.into_bytes(),
            writer: mode,
            desired,
        },
        JournalCommand::Delete { name, .. } => JournalOp::Delete {
            name: name.into_bytes(),
        },
    };
    let request = JournalRequest {
        request: nonzero(providers),
        tenant,
        op,
    };
    let answer = journals::request(
        providers,
        rpc,
        client,
        election,
        &request,
        Duration::from_millis(args.patience_ms),
    )
    .await;
    report(out, tenant, &answer)
}

/// Print what a request came to.
fn report(out: &Printer, tenant: TenantId, answer: &JournalAnswer) -> Ending {
    let label = answer.as_str();
    match answer {
        JournalAnswer::Created { id, config } => {
            let journal = JournalIdentifier::new(tenant, *id);
            let members: Vec<u64> = config.members().iter().map(|m| m.0).collect();
            out.emit(
                || {
                    format!(
                        "created journal={} members={}",
                        hex(journal),
                        members
                            .iter()
                            .map(|m| full_hex(*m))
                            .collect::<Vec<_>>()
                            .join(",")
                    )
                },
                || json!({ "outcome": label, "journal": journal.to_string(), "members": members }),
            );
            Ending::Success
        }
        JournalAnswer::Deleted { id } => {
            let journal = JournalIdentifier::new(tenant, *id);
            out.emit(
                || format!("deleted journal={}", hex(journal)),
                || json!({ "outcome": label, "journal": journal.to_string() }),
            );
            Ending::Success
        }
        JournalAnswer::NameTaken { id } => {
            let journal = JournalIdentifier::new(tenant, *id);
            out.emit(
                || format!("refused: name_taken by journal={}", hex(journal)),
                || json!({ "outcome": label, "journal": journal.to_string() }),
            );
            Ending::Refused
        }
        JournalAnswer::NotCoordinator | JournalAnswer::Unavailable => {
            note("no coordinator decided the request: run the same command again");
            out.emit(
                || format!("undecided: {label}"),
                || json!({ "outcome": label }),
            );
            Ending::Ambiguous
        }
        _ => {
            out.emit(
                || format!("refused: {label}"),
                || json!({ "outcome": label }),
            );
            Ending::Refused
        }
    }
}

/// `parosctl journal list`: every journal the tenant created, live or a
/// tombstone.
async fn list(
    client: &ParosClient,
    tenant: TenantId,
    control: paros::JournalId,
    out: &Printer,
) -> Ending {
    let Some(fold) = journals::list(client, tenant, control).await else {
        note("the tenant's control journal could not be read to its tail");
        return Ending::Unreachable;
    };
    out.emit(
        || {
            let mut lines = vec![format!(
                "tenant={} control={}",
                full_hex(tenant.0),
                hex(JournalIdentifier::new(tenant, control))
            )];
            for (id, journal) in fold.journals() {
                lines.push(format!(
                    "journal={} name={} mode={} desired={} members={} state={}",
                    hex(JournalIdentifier::new(tenant, id)),
                    record_text(&journal.name),
                    mode_label(journal.writer),
                    journal.desired.label(),
                    journal
                        .config
                        .members()
                        .iter()
                        .map(|m| full_hex(m.0))
                        .collect::<Vec<_>>()
                        .join(","),
                    if journal.deleted_at.is_some() {
                        "deleted"
                    } else {
                        "live"
                    }
                ));
            }
            lines.join("\n")
        },
        || {
            json!({
                "tenant": tenant.0,
                "control": JournalIdentifier::new(tenant, control).to_string(),
                "journals": fold.journals().map(|(id, journal)| json!({
                    "journal": JournalIdentifier::new(tenant, id).to_string(),
                    "name": record_text(&journal.name),
                    "mode": mode_label(journal.writer),
                    "desired": journal.desired.label(),
                    "members": journal.config.members().iter().map(|m| m.0).collect::<Vec<_>>(),
                    "deleted": journal.deleted_at.is_some(),
                })).collect::<Vec<_>>(),
            })
        },
    );
    Ending::Success
}

/// A journal's ids, whole and in hex: `id:TENANT/JOURNAL` (#239 (names at
/// the edge)), the form every journal argument takes.
fn hex(journal: JournalIdentifier) -> String {
    format!(
        "id:{}/{}",
        full_hex(journal.tenant.0),
        full_hex(journal.journal.0)
    )
}

/// A writer mode's label.
fn mode_label(mode: WriterMode) -> &'static str {
    match mode {
        WriterMode::Single => "single",
        WriterMode::Multi => "multi",
    }
}
