//! `parosctl journal create|delete|list` (#210): a tenant's journals,
//! through the tenant's control journal — `paros::client::tenant`. A
//! created journal runs on the servers this command talks to (the seeds)
//! until the tenant coordinator places it (#212).

use clap::{Args, Subcommand};
use moonpool_core::{Providers, RandomProvider, TokioProviders};
use paros::client::Client;
use paros::client::tenant::{self, CreateJournalOutcome, DeleteJournalOutcome};
use paros::{AcceptorConfig, JournalId, NodeId, QuorumSystem, TenantId};
use serde_json::json;

use crate::Ending;
use crate::output::{Printer, note};

/// The most ids a `create` draws before it gives up (a random u64 is taken
/// by accident essentially never).
const ID_DRAWS: usize = 4;

/// `parosctl journal`.
#[derive(Subcommand, Debug)]
pub enum JournalCommand {
    /// Create a journal in a tenant.
    Create(CreateArgs),
    /// Delete a tenant's journal by name.
    Delete(DeleteArgs),
    /// List a tenant's journals, as its control journal records them.
    List(ListArgs),
}

/// Which tenant, and who writes its control journal.
#[derive(Args, Debug)]
pub struct TenantArg {
    /// The tenant: its name (resolved through meta), or its id.
    #[arg(long)]
    tenant: String,
    /// The client writing the tenant's control journal.
    #[arg(long, env = "PAROSCTL_OWNER", default_value = "1")]
    owner: u64,
}

/// `parosctl journal create`.
#[derive(Args, Debug)]
pub struct CreateArgs {
    /// The journal's name.
    name: String,
    #[command(flatten)]
    tenant: TenantArg,
}

/// `parosctl journal delete`.
#[derive(Args, Debug)]
pub struct DeleteArgs {
    /// The journal's name.
    name: String,
    #[command(flatten)]
    tenant: TenantArg,
}

/// `parosctl journal list`.
#[derive(Args, Debug)]
pub struct ListArgs {
    #[command(flatten)]
    tenant: TenantArg,
}

/// The tenant `arg` names: an id as given, a name through meta.
async fn tenant_of(client: &Client<TokioProviders>, arg: &TenantArg) -> Option<TenantId> {
    if let Ok(id) = arg.tenant.parse::<u64>() {
        return Some(TenantId(id));
    }
    tenant::resolve_tenant(client, arg.tenant.as_bytes(), 0).await
}

/// A journal id in the user range, drawn at random.
fn draw(providers: &TokioProviders) -> JournalId {
    loop {
        let id = JournalId(providers.random().random());
        if id.is_user() {
            return id;
        }
    }
}

/// Run `command` through `client`.
pub async fn run(
    providers: &TokioProviders,
    client: &Client<TokioProviders>,
    out: &Printer,
    command: JournalCommand,
) -> Ending {
    let arg = match &command {
        JournalCommand::Create(args) => &args.tenant,
        JournalCommand::Delete(args) => &args.tenant,
        JournalCommand::List(args) => &args.tenant,
    };
    let Some(tenant_id) = tenant_of(client, arg).await else {
        out.emit(
            || format!("no ready tenant named {}", arg.tenant),
            || json!({ "outcome": "refused", "refusal": "unknown_tenant" }),
        );
        return Ending::Refused;
    };
    match command {
        JournalCommand::Create(args) => create(providers, client, out, tenant_id, args).await,
        JournalCommand::Delete(args) => delete(client, out, tenant_id, args).await,
        JournalCommand::List(_) => list(client, out, tenant_id).await,
    }
}

/// `parosctl journal create`: the journal over the servers this command talks to, under a drawn id
/// (redrawn while taken).
async fn create(
    providers: &TokioProviders,
    client: &Client<TokioProviders>,
    out: &Printer,
    tenant_id: TenantId,
    args: CreateArgs,
) -> Ending {
    let mut members: Vec<NodeId> = (0..client.server_count())
        .map(|i| NodeId(client.id_of(i)))
        .collect();
    members.sort_unstable();
    members.dedup();
    let config = AcceptorConfig::new(members, QuorumSystem::Majority);
    for _ in 0..ID_DRAWS {
        let outcome = tenant::create_journal(
            client,
            args.tenant.owner,
            tenant_id,
            draw(providers),
            args.name.clone().into_bytes(),
            config.clone(),
            0,
        )
        .await;
        match outcome {
            CreateJournalOutcome::Created { journal } => {
                out.emit(
                    || format!("created journal {journal}"),
                    || {
                        json!({
                            "outcome": "created",
                            "tenant": journal.tenant.0,
                            "journal": journal.journal.0,
                        })
                    },
                );
                return Ending::Success;
            }
            CreateJournalOutcome::IdTaken => {}
            CreateJournalOutcome::NameTaken { winner } => {
                out.emit(
                    || format!("create refused: the name is journal {winner}'s"),
                    || {
                        json!({
                            "outcome": "refused",
                            "refusal": "name_taken",
                            "journal": winner.journal.0,
                        })
                    },
                );
                return Ending::Refused;
            }
            CreateJournalOutcome::Refused(refusal) => {
                out.emit(
                    || format!("create refused: {refusal:?}"),
                    || json!({ "outcome": "refused", "refusal": format!("{refusal:?}") }),
                );
                return Ending::Refused;
            }
            CreateJournalOutcome::Unavailable => {
                note("the tenant's control journal did not answer: run it again");
                return Ending::Unreachable;
            }
        }
    }
    note("every id drawn was taken");
    Ending::Refused
}

/// `parosctl journal delete`: tombstone the tenant's live journal by name.
async fn delete(
    client: &Client<TokioProviders>,
    out: &Printer,
    tenant_id: TenantId,
    args: DeleteArgs,
) -> Ending {
    match tenant::delete_journal(
        client,
        args.tenant.owner,
        tenant_id,
        args.name.as_bytes(),
        0,
    )
    .await
    {
        DeleteJournalOutcome::Deleted => {
            out.emit(
                || format!("deleted journal {}", args.name),
                || json!({ "outcome": "deleted" }),
            );
            Ending::Success
        }
        DeleteJournalOutcome::Unknown => {
            out.emit(
                || format!("no live journal named {}", args.name),
                || json!({ "outcome": "refused", "refusal": "unknown_journal" }),
            );
            Ending::Refused
        }
        DeleteJournalOutcome::Unavailable => {
            note("the tenant's control journal did not answer: run it again");
            Ending::Unreachable
        }
    }
}

/// `parosctl journal list`: the tenant's live journals, as its control journal records them.
async fn list(client: &Client<TokioProviders>, out: &Printer, tenant_id: TenantId) -> Ending {
    let Some(directory) = tenant::load_directory(client, tenant_id, 0).await else {
        note("no server served the tenant's control journal");
        return Ending::Unreachable;
    };
    let live: Vec<(JournalId, String)> = directory
        .journals()
        .filter(|(_, j)| j.deleted_at.is_none())
        .map(|(id, j)| (id, String::from_utf8_lossy(&j.name).into_owned()))
        .collect();
    out.emit(
        || {
            live.iter()
                .map(|(id, name)| format!("{}/{} {name}", tenant_id.0, id.0))
                .collect::<Vec<_>>()
                .join("\n")
        },
        || {
            json!({
                "tenant": tenant_id.0,
                "name": directory.name().map(String::from_utf8_lossy),
                "journals": live
                    .iter()
                    .map(|(id, name)| json!({ "journal": id.0, "name": name }))
                    .collect::<Vec<_>>(),
            })
        },
    );
    Ending::Success
}
