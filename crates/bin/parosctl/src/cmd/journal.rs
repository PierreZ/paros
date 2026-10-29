//! `parosctl journal create|delete|tag|list` (#210, #575 (key:value tags)):
//! a tenant's journals, over `paros::client::journals`; and `parosctl
//! configure` (#214 (tenant modes)), FDB's `configure`: a new desired mode
//! for a journal or the tenant's default, which the tenant coordinator
//! applies.
//!
//! The tenant is named; its id and its control journal come from the fleet
//! directory. A create or a delete is a request to the tenant coordinator
//! with an idempotency id drawn per run: the library re-sends the same id
//! until an answer decides it, so a request acts once. The coordinator draws
//! the journal's id and picks its members: a caller names a desired mode,
//! never members. A list folds the tenant's control journal.
//!
//! A journal carries user tags (#575 (key:value tags)): `create --tag` sets
//! them at birth, `tag` sets them again, whole. A list takes a selector
//! (`-l`) over the journal's tags and its tenant's (`tenant.<key>`).

use std::time::Duration;

use clap::{Args, Subcommand};
use moonpool_core::CompioProviders;
use moonpool_rpc::RpcHandle;
use paros::client::Client;
use paros::client::bootstrap::control_journals;
use paros::client::fleet::read_directory;
use paros::client::journals::{self, JournalAnswer, JournalOp, JournalRequest};
use paros::core::journal::{JournalIdentifier, TenantId, WriterMode};
use paros::model::address::Names;
use paros::model::fleet::TenantState;
use paros::model::tags::{Tags, UserTags};
use paros::model::tenant::{
    Desired, Durability, MAX_SURVIVE_MACHINES, StoreDurability, TenantRoles,
};
use serde_json::json;

use crate::cmd::tenant::nonzero;
use crate::support::connect::Ending;
use crate::support::labels::Labels;
use crate::support::output::{Printer, note, record_text, table};

type ParosClient = Client<CompioProviders>;

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
        /// How its stores commit a batch (#599): `ordered` (two syncs per
        /// commit, the default) or `batched` (one sync; a crash inside a
        /// commit can lose the last batch, which the journal repairs from
        /// its peers). Fixed for the journal's life.
        #[arg(long, default_value = "ordered", value_parser = parse_store_durability)]
        store_durability: StoreDurability,
        /// A user tag, `key:value`, repeated (#575). A `paros/` key is
        /// refused.
        #[arg(long = "tag")]
        tags: Vec<String>,
    },
    /// Set a journal's user tags, whole: the tags given replace the ones
    /// it has, and none clears them (#575).
    Tag {
        /// The tenant's name.
        tenant: String,
        /// The journal's name.
        name: String,
        /// The tags, `key:value` each.
        tags: Vec<String>,
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
        /// Only the journals whose tags match: `key`, `key:value`, comma
        /// separated, all of them; `tenant.<key>` reads the tenant's tags
        /// (#575).
        #[arg(short = 'l', long)]
        selector: Option<String>,
    },
}

impl JournalCommand {
    /// The tenant's name and the journal's (empty for a list).
    fn names(&self) -> (String, String) {
        match self {
            JournalCommand::Create { tenant, name, .. }
            | JournalCommand::Delete { tenant, name }
            | JournalCommand::Tag { tenant, name, .. } => (tenant.clone(), name.clone()),
            JournalCommand::List { tenant, .. } => (tenant.clone(), String::new()),
        }
    }
}

impl JournalArgs {
    /// Whether it is `journal tag`, which goes through the frontends when
    /// they are given (#575 (key:value tags)).
    pub fn is_tag(&self) -> bool {
        matches!(self.command, JournalCommand::Tag { .. })
    }
}

/// `parosctl journal tag` through the frontends (#575 (key:value tags)):
/// the tenant's own token sets its journal's user tags. One idempotency id
/// per run, sent to each frontend in turn until an answer decides it.
pub async fn tag_through_frontends(
    providers: &CompioProviders,
    frontends: &ParosClient,
    out: &Printer,
    args: JournalArgs,
) -> Ending {
    let JournalCommand::Tag { tenant, name, tags } = args.command else {
        unreachable!("only journal tag goes through the frontends");
    };
    let Ok(tags) = user_tags(&tags) else {
        return Ending::Refused;
    };
    let Ok(journal) = paros::model::name::JournalName::new(&tenant, &name) else {
        note(&format!("bad journal name {tenant}/{name}"));
        return Ending::Refused;
    };
    let answer = journals::tag_through(
        providers,
        frontends,
        (&journal, nonzero(providers)),
        &tags,
        Duration::from_millis(args.patience_ms),
    )
    .await;
    match answer {
        journals::FrontTagged::Denied(denial) => {
            out.emit(
                || format!("denied: {}", denial.as_str()),
                || json!({ "outcome": "denied", "denial": denial.as_str() }),
            );
            Ending::Refused
        }
        journals::FrontTagged::Answered(JournalAnswer::Tagged { id }) => {
            out.emit(
                || format!("tagged journal={journal}"),
                || json!({ "outcome": "tagged", "journal": journal.to_string(), "id": id.0 }),
            );
            Ending::Success
        }
        journals::FrontTagged::Answered(answer) if !answer.is_retryable() => {
            out.emit(
                || format!("refused: {}", answer.as_str()),
                || json!({ "outcome": answer.as_str() }),
            );
            Ending::Refused
        }
        _ => {
            note("no frontend decided the request: run the same command again");
            Ending::Ambiguous
        }
    }
}

/// A writer mode as given on the command line (`--writers`, #523).
fn parse_writers(text: &str) -> Result<WriterMode, String> {
    match text {
        "single" => Ok(WriterMode::Single),
        "multi" => Ok(WriterMode::Multi),
        _ => Err("writers is single or multi".into()),
    }
}

/// `--store-durability` (#599 (durability mode per journal)).
pub fn parse_store_durability(text: &str) -> Result<StoreDurability, String> {
    text.parse().map_err(|why: &str| why.to_string())
}

/// `--survive machines=M,zones=Z,regions=R` (§3.4, #748 (survive)): what a
/// journal must survive. It gives `machines`, 0 to 3. Zone and region
/// survival (`zones=1`, `regions=1`) come with #748 (survive); until then
/// only `0` is taken for them.
///
/// # Errors
///
/// A part that is not `key=number`, an unknown key, `machines` above 3, or
/// `zones` or `regions` other than 0.
pub fn parse_survive(text: &str) -> Result<u32, String> {
    let mut machines = Durability::DEFAULT.survive_machines;
    for part in text
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        let (key, value) = part
            .split_once('=')
            .ok_or("survive is machines=M,zones=Z,regions=R")?;
        let value: u32 = value
            .trim()
            .parse()
            .map_err(|_| "a survive value is a number")?;
        match key.trim() {
            "machines" if value <= MAX_SURVIVE_MACHINES => machines = value,
            "machines" => return Err(format!("machines is at most {MAX_SURVIVE_MACHINES}")),
            "zones" | "regions" if value == 0 => {}
            "zones" | "regions" => {
                return Err(format!("{key}={value} is not built yet (#748 (survive))"));
            }
            _ => {
                return Err(format!(
                    "survive names machines, zones and regions, not {key}"
                ));
            }
        }
    }
    Ok(machines)
}

/// A journal's durability on the command line (§3.4, #748 (survive)): each
/// acceptor partition has `2F+1` acceptors, `F` the `machines` of
/// `--survive`. A field not named takes its default (`machines=1`, `P=1`);
/// none named is no mode at all.
#[derive(Args, Debug, Clone, Copy)]
pub struct DurabilityArgs {
    /// What the journal must survive: `machines=M,zones=Z,regions=R`.
    /// `machines` is 0 to 3 (default 1), and each acceptor partition has
    /// `2M+1` acceptors. `zones` and `regions` take only 0 for now.
    #[arg(long = "survive", value_name = "machines=M,zones=Z,regions=R", value_parser = parse_survive)]
    survive_machines: Option<u32>,
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
        if self.survive_machines.is_none() && self.partitions.is_none() {
            return None;
        }
        let durability = Durability::new(
            self.survive_machines
                .unwrap_or(Durability::DEFAULT.survive_machines),
            self.partitions.unwrap_or(Durability::DEFAULT.partitions),
        )
        .expect("clap bounds the durability");
        Some(Desired {
            durability,
            ..Desired::DEFAULT
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
    /// The journal's replicas (#753 (per-journal proxies)): the coordinator
    /// books that many on `storage` machines. 0 is none.
    #[arg(long)]
    replicas: Option<u32>,
    /// The journal's proxy leaders (#753 (per-journal proxies)): the
    /// coordinator books that many on `stateless` machines. 0 runs every
    /// Phase 2 on the leader.
    #[arg(long)]
    proxies: Option<u32>,
    /// The journal's batchers (#145 (batchers), #753): the coordinator books
    /// that many on `stateless` machines for a multi-writer journal. 0 is
    /// none.
    #[arg(long)]
    batchers: Option<u32>,
    /// The tenant's frontends (#723 (frontend as a parosd role)): the
    /// coordinator books that many on the cell's `stateless` machines. Only
    /// without `--journal`.
    #[arg(long)]
    frontends: Option<u32>,
    /// How long the request is sent again while no coordinator decides it,
    /// in milliseconds.
    #[arg(long, default_value = "30000")]
    patience_ms: u64,
}

/// `parosctl journal …`.
pub async fn run(
    providers: &CompioProviders,
    rpc: &RpcHandle<CompioProviders>,
    names: &Names,
    client: &ParosClient,
    labels: &Labels,
    out: &Printer,
    args: JournalArgs,
) -> Ending {
    let (tenant_name, journal_name) = args.command.names();
    let (tenant, control, election, tenant_tags) =
        match ready_tenant(client, out, &tenant_name).await {
            Ok(found) => found,
            Err(ending) => return ending,
        };
    let op = match args.command {
        JournalCommand::List { selector, .. } => {
            return list(
                client,
                (&tenant_name, tenant, control),
                (selector.as_deref(), &tenant_tags),
                labels,
                out,
            )
            .await;
        }
        JournalCommand::Create {
            name,
            writers,
            durability,
            store_durability,
            tags,
            ..
        } => JournalOp::Create {
            name: name.into_bytes(),
            writer: writers,
            desired: durability.desired(),
            tags: match user_tags(&tags) {
                Ok(tags) => tags,
                Err(ending) => return ending,
            },
            store: store_durability,
        },
        JournalCommand::Tag { name, tags, .. } => JournalOp::SetTags {
            name: name.into_bytes(),
            tags: match user_tags(&tags) {
                Ok(tags) => tags,
                Err(ending) => return ending,
            },
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

/// The user tags given on the command line, or a refusal printed.
pub(crate) fn user_tags(texts: &[String]) -> Result<UserTags, Ending> {
    UserTags::parse(texts).map_err(|error| {
        note(&format!("refused: {}: {error}", error.label()));
        Ending::Refused
    })
}

/// The `READY` tenant named `tenant_name`, with its control journal, the
/// cell's election journal, where the tenant coordinator publishes itself,
/// and the tenant's tags (#575 (key:value tags)).
async fn ready_tenant(
    client: &ParosClient,
    out: &Printer,
    tenant_name: &str,
) -> Result<
    (
        TenantId,
        paros::core::journal::JournalId,
        JournalIdentifier,
        Tags,
    ),
    Ending,
> {
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
    Ok((tenant, entry.control, election, entry.all_tags()))
}

/// The role counts `args` names (#604 (role counts)), each count it does
/// not name kept from `held`, or a refusal printed.
fn role_counts(args: &ConfigureArgs, held: TenantRoles) -> Result<TenantRoles, Ending> {
    TenantRoles::new(args.frontends.unwrap_or(held.frontends)).map_err(|error| {
        note(&format!("refused: {error}"));
        Ending::Refused
    })
}

/// The scale counts `args` names (#753 (per-journal proxies)), each count
/// it does not name kept from `held`, or a refusal printed.
fn scale_counts(args: &ConfigureArgs, held: Desired) -> Result<Desired, Ending> {
    held.with_counts(
        args.replicas.unwrap_or(held.replicas),
        args.proxies.unwrap_or(held.proxies),
        args.batchers.unwrap_or(held.batchers),
    )
    .map_err(|error| {
        note(&format!("refused: {error}"));
        Ending::Refused
    })
}

/// The mode `journal` (the tenant default when `None`) holds now, and the
/// tenant's role counts, read from the tenant's control journal.
async fn held_mode(
    client: &ParosClient,
    (tenant, control): (TenantId, paros::core::journal::JournalId),
    journal: Option<&str>,
) -> Result<(Desired, TenantRoles), Ending> {
    let Some(fold) = journals::list(client, tenant, control).await else {
        note("the tenant's control journal could not be read to its tail");
        return Err(Ending::Unreachable);
    };
    let desired = match journal {
        None => fold.default_desired(),
        Some(name) => fold
            .named(name.as_bytes())
            .and_then(|id| fold.get(id))
            .map_or(Desired::DEFAULT, |journal| journal.desired),
    };
    Ok((desired, fold.roles()))
}

/// `parosctl configure` (#214 (tenant modes)): record a new desired mode
/// for a journal or the tenant's default. The answer says it is recorded;
/// the tenant coordinator then applies a journal's mode by reconfiguration
/// (`parosctl status` shows it degraded until then).
pub async fn configure(
    providers: &CompioProviders,
    rpc: &RpcHandle<CompioProviders>,
    names: &Names,
    client: &ParosClient,
    out: &Printer,
    args: ConfigureArgs,
) -> Ending {
    let named_roles = args.frontends.is_some();
    if named_roles && args.journal.is_some() {
        note("--frontends is the tenant default's: drop --journal");
        return Ending::Refused;
    }
    let named_counts = args.replicas.is_some() || args.proxies.is_some() || args.batchers.is_some();
    let named_mode = args.durability.desired();
    if named_mode.is_none() && !named_counts && !named_roles {
        note(
            "name a mode: --survive, --partitions, --replicas, --proxies, --batchers or --frontends",
        );
        return Ending::Refused;
    }
    let (tenant, control, election, _) = match ready_tenant(client, out, &args.tenant).await {
        Ok(found) => found,
        Err(ending) => return ending,
    };
    // A count or a mode not named keeps the target's: read the fold when
    // the command names only part of them.
    let current = if named_roles || named_mode.is_none() || named_counts {
        match held_mode(client, (tenant, control), args.journal.as_deref()).await {
            Ok(held) => Some(held),
            Err(ending) => return ending,
        }
    } else {
        None
    };
    let roles = if named_roles {
        let held = current.map(|(_, roles)| roles).unwrap_or_default();
        match role_counts(&args, held) {
            Ok(roles) => Some(roles),
            Err(ending) => return ending,
        }
    } else {
        None
    };
    let held = current.map_or(Desired::DEFAULT, |(desired, _)| desired);
    // A named durability keeps the counts held; a count not named keeps its
    // own.
    let mode = named_mode.map_or(held, |mode| Desired {
        durability: mode.durability,
        ..held
    });
    let desired = match scale_counts(&args, mode) {
        Ok(desired) => desired,
        Err(ending) => return ending,
    };
    let request = JournalRequest {
        request: nonzero(providers),
        tenant,
        op: JournalOp::Configure {
            name: args.journal.as_ref().map(|j| j.clone().into_bytes()),
            desired,
            roles,
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
            let roles_label = roles.map(|r| r.label());
            out.emit(
                || match &roles_label {
                    Some(r) => format!("configured {target}: {}, {r}", desired.label()),
                    None => format!("configured {target}: {}", desired.label()),
                },
                || json!({ "outcome": label, "target": target, "mode": desired.label(), "roles": roles_label }),
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
        JournalAnswer::Tagged { id } => {
            let journal = JournalIdentifier::new(tenant, *id);
            out.emit(
                || format!("tagged journal={named}"),
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
    (name, tenant, control): (&str, TenantId, paros::core::journal::JournalId),
    (selector, tenant_tags): (Option<&str>, &Tags),
    labels: &Labels,
    out: &Printer,
) -> Ending {
    let selector = match crate::support::view::selector(selector) {
        Ok(selector) => selector,
        Err(ending) => return ending,
    };
    let Some(fold) = journals::list(client, tenant, control).await else {
        note("the tenant's control journal could not be read to its tail");
        return Ending::Unreachable;
    };
    let chosen: Vec<_> = fold
        .journals()
        .filter(|(_, journal)| selector.matches(journal.tags.tags(), Some(tenant_tags)))
        .collect();
    out.emit(
        || {
            let rows: Vec<[String; 7]> = chosen
                .iter()
                .map(|(_, journal)| {
                    let members: Vec<u64> = journal.config.members().iter().map(|m| m.0).collect();
                    [
                        format!("{name}/{}", record_text(&journal.name)),
                        mode_label(journal.writer).to_string(),
                        journal.store.as_str().to_string(),
                        journal.desired.label(),
                        labels.machines(&members),
                        if journal.deleted_at.is_some() {
                            "deleted"
                        } else {
                            "live"
                        }
                        .to_string(),
                        if journal.tags.is_empty() {
                            "-".to_string()
                        } else {
                            journal.tags.tags().label()
                        },
                    ]
                })
                .collect();
            table(
                [
                    "JOURNAL", "WRITER", "STORE", "DESIRED", "ACCEPTORS", "STATE", "TAGS",
                ],
                &rows,
            )
        },
        || {
            json!({
                "tenant": tenant.0,
                "control": JournalIdentifier::new(tenant, control).to_string(),
                "journals": chosen.iter().map(|(id, journal)| json!({
                    "journal": JournalIdentifier::new(tenant, *id).to_string(),
                    "name": record_text(&journal.name),
                    "mode": mode_label(journal.writer),
                    "store_durability": journal.store.as_str(),
                    "desired": journal.desired.label(),
                    "members": journal.config.members().iter().map(|m| m.0).collect::<Vec<_>>(),
                    "deleted": journal.deleted_at.is_some(),
                    "tags": journal.tags.tags().iter().collect::<std::collections::BTreeMap<_, _>>(),
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
