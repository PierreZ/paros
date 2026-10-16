//! `parosctl journal create|delete|list` (#210): a tenant's journals, over
//! `paros::client::journals`; and `parosctl configure` (#214 (tenant
//! modes)), FDB's `configure`: a new desired mode for a journal or the
//! tenant's default, which the tenant coordinator applies.
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
use paros::tenant::{Desired, Durability, MAX_FAILURES_PER_PARTITION};
use paros::{JournalIdentifier, Names, TenantId, WriterMode};
use serde_json::json;

use crate::Ending;
use crate::fleet::nonzero;
use crate::labels::Labels;
use crate::output::{Printer, note, record_text, table};

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
        #[arg(long, default_value = "single", value_parser = parse_writers)]
        writers: WriterMode,
        /// Its durability; none named takes the tenant's default mode.
        #[command(flatten)]
        durability: DurabilityArgs,
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

/// A writer mode as given on the command line (`--writers`, #523).
fn parse_writers(text: &str) -> Result<WriterMode, String> {
    match text {
        "single" => Ok(WriterMode::Single),
        "multi" => Ok(WriterMode::Multi),
        _ => Err("writers is single or multi".into()),
    }
}

/// A journal's durability on the command line (§3.4, #523): each acceptor
/// partition has `2F+1` acceptors. A field not named takes its default
/// (`F=1`, `P=1`); none named is no mode at all.
#[derive(Args, Debug, Clone, Copy)]
pub struct DurabilityArgs {
    /// `F`: the failures each acceptor partition survives, 0 to 3 (default
    /// 1). Each acceptor partition has `2F+1` acceptors.
    #[arg(long, value_parser = clap::value_parser!(u32).range(0..=i64::from(MAX_FAILURES_PER_PARTITION)))]
    failures_per_partition: Option<u32>,
    /// `P`: the acceptor partitions a journal's writes spread over, at least
    /// 1 (default 1, the plain majority).
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..))]
    partitions: Option<u32>,
}

impl DurabilityArgs {
    /// The desired mode it names: `None` when it names neither field.
    ///
    /// # Panics
    ///
    /// Never: clap bounds both values.
    pub fn desired(self) -> Option<Desired> {
        if self.failures_per_partition.is_none() && self.partitions.is_none() {
            return None;
        }
        let durability = Durability::new(
            self.failures_per_partition
                .unwrap_or(Durability::DEFAULT.failures_per_partition),
            self.partitions.unwrap_or(Durability::DEFAULT.partitions),
        )
        .expect("clap bounds the durability");
        Some(Desired {
            durability,
            replicas: 0,
        })
    }
}

/// `parosctl configure` (#214 (tenant modes)): FDB's `configure`.
#[derive(Args, Debug)]
pub struct ConfigureArgs {
    /// The tenant's name.
    #[arg(long)]
    tenant: String,
    /// The journal whose mode changes; none changes the tenant's default,
    /// which a create that names no mode takes.
    #[arg(long)]
    journal: Option<String>,
    /// The new durability: a field not named takes its default.
    #[command(flatten)]
    durability: DurabilityArgs,
    /// The replicas per journal (recorded).
    #[arg(long)]
    replicas: Option<u32>,
    /// How long the request is sent again while no coordinator decides it,
    /// in milliseconds.
    #[arg(long, default_value = "30000")]
    patience_ms: u64,
}

/// `parosctl journal …`.
pub async fn run(
    providers: &TokioProviders,
    rpc: &RpcHandle<TokioProviders>,
    names: &Names,
    client: &ParosClient,
    labels: &Labels,
    out: &Printer,
    args: JournalArgs,
) -> Ending {
    let tenant_name = match &args.command {
        JournalCommand::Create { tenant, .. }
        | JournalCommand::Delete { tenant, .. }
        | JournalCommand::List { tenant } => tenant.clone(),
    };
    let (tenant, control, election) = match ready_tenant(client, out, &tenant_name).await {
        Ok(found) => found,
        Err(ending) => return ending,
    };
    let journal_name = match &args.command {
        JournalCommand::Create { name, .. } | JournalCommand::Delete { name, .. } => name.clone(),
        JournalCommand::List { .. } => String::new(),
    };
    let op = match args.command {
        JournalCommand::List { .. } => {
            return list(client, (&tenant_name, tenant, control), labels, out).await;
        }
        JournalCommand::Create {
            name,
            writers,
            durability,
            ..
        } => JournalOp::Create {
            name: name.into_bytes(),
            writer: writers,
            desired: durability.desired(),
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
        names,
        client,
        election,
        &request,
        Duration::from_millis(args.patience_ms),
    )
    .await;
    let full_name = format!("{tenant_name}/{journal_name}");
    report(out, (tenant, &full_name), labels, &answer)
}

/// The `READY` tenant named `tenant_name`, with its control journal, and the
/// cell's election journal, where the tenant coordinator publishes itself.
async fn ready_tenant(
    client: &ParosClient,
    out: &Printer,
    tenant_name: &str,
) -> Result<(TenantId, paros::JournalId, JournalIdentifier), Ending> {
    let Some(cell) = control_journals(client).await else {
        note("no server named its cell's control journals: is the cell initialized?");
        return Err(Ending::Unreachable);
    };
    let (Some(fleet), Some(election)) = (cell.fleet, cell.election) else {
        note("the cell names no fleet journal or no election journal");
        return Err(Ending::Refused);
    };
    let directory = match read_directory(client, 0, fleet).await {
        Ok(directory) => directory,
        Err(outcome) => {
            note(&format!(
                "the fleet directory could not be read to its tail: {outcome:?}"
            ));
            return Err(Ending::Unreachable);
        }
    };
    let Some((tenant, entry)) = directory.named(tenant_name.as_bytes()) else {
        out.emit(
            || format!("no tenant named {tenant_name}"),
            || json!({ "outcome": "unknown_tenant", "tenant": tenant_name }),
        );
        return Err(Ending::Refused);
    };
    if entry.state != TenantState::Ready {
        out.emit(
            || format!("tenant {tenant_name} is {}", entry.state.as_str()),
            || json!({ "outcome": "not_ready", "tenant": tenant_name, "state": entry.state.as_str() }),
        );
        return Err(Ending::Refused);
    }
    Ok((tenant, entry.control, election))
}

/// `parosctl configure` (#214 (tenant modes)): record a new desired mode
/// for a journal or the tenant's default. The answer says it is recorded;
/// the tenant coordinator then applies a journal's mode by reconfiguration
/// (`parosctl status` shows it degraded until then).
pub async fn configure(
    providers: &TokioProviders,
    rpc: &RpcHandle<TokioProviders>,
    names: &Names,
    client: &ParosClient,
    out: &Printer,
    args: ConfigureArgs,
) -> Ending {
    let Some(mut desired) = args
        .durability
        .desired()
        .or_else(|| args.replicas.map(|_| Desired::DEFAULT))
    else {
        note("name a mode: --failures-per-partition, --partitions or --replicas");
        return Ending::Refused;
    };
    desired.replicas = args.replicas.unwrap_or(0);
    let (tenant, _, election) = match ready_tenant(client, out, &args.tenant).await {
        Ok(found) => found,
        Err(ending) => return ending,
    };
    let request = JournalRequest {
        request: nonzero(providers),
        tenant,
        op: JournalOp::Configure {
            name: args.journal.as_ref().map(|j| j.clone().into_bytes()),
            desired,
        },
    };
    let answer = journals::request(
        providers,
        rpc,
        names,
        client,
        election,
        &request,
        Duration::from_millis(args.patience_ms),
    )
    .await;
    let target = args.journal.as_ref().map_or_else(
        || format!("{} (default)", args.tenant),
        |j| format!("{}/{j}", args.tenant),
    );
    let label = answer.as_str();
    match answer {
        JournalAnswer::Configured { .. } => {
            out.emit(
                || format!("configured {target}: {}", desired.label()),
                || json!({ "outcome": label, "target": target, "mode": desired.label() }),
            );
            Ending::Success
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
                || json!({ "outcome": label, "target": target }),
            );
            Ending::Refused
        }
    }
}

/// Print what a request for the journal `named` came to.
fn report(
    out: &Printer,
    (tenant, named): (TenantId, &str),
    labels: &Labels,
    answer: &JournalAnswer,
) -> Ending {
    let label = answer.as_str();
    match answer {
        JournalAnswer::Created { id, config } => {
            let journal = JournalIdentifier::new(tenant, *id);
            let members: Vec<u64> = config.members().iter().map(|m| m.0).collect();
            out.emit(
                || {
                    format!(
                        "created journal={named} members={}",
                        labels.machines(&members)
                    )
                },
                || json!({ "outcome": label, "journal": journal.to_string(), "members": members }),
            );
            Ending::Success
        }
        JournalAnswer::Deleted { id } => {
            let journal = JournalIdentifier::new(tenant, *id);
            out.emit(
                || format!("deleted journal={named}"),
                || json!({ "outcome": label, "journal": journal.to_string() }),
            );
            Ending::Success
        }
        JournalAnswer::NameTaken { id } => {
            let journal = JournalIdentifier::new(tenant, *id);
            out.emit(
                || format!("refused: name_taken: a live journal is named {named}"),
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
    (name, tenant, control): (&str, TenantId, paros::JournalId),
    labels: &Labels,
    out: &Printer,
) -> Ending {
    let Some(fold) = journals::list(client, tenant, control).await else {
        note("the tenant's control journal could not be read to its tail");
        return Ending::Unreachable;
    };
    out.emit(
        || {
            let rows: Vec<[String; 5]> = fold
                .journals()
                .map(|(_, journal)| {
                    let members: Vec<u64> = journal.config.members().iter().map(|m| m.0).collect();
                    [
                        format!("{name}/{}", record_text(&journal.name)),
                        mode_label(journal.writer).to_string(),
                        journal.desired.label(),
                        labels.machines(&members),
                        if journal.deleted_at.is_some() {
                            "deleted"
                        } else {
                            "live"
                        }
                        .to_string(),
                    ]
                })
                .collect();
            table(
                ["JOURNAL", "WRITER", "DESIRED", "ACCEPTORS", "STATE"],
                &rows,
            )
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

/// A writer mode's label.
fn mode_label(mode: WriterMode) -> &'static str {
    match mode {
        WriterMode::Single => "single",
        WriterMode::Multi => "multi",
    }
}
