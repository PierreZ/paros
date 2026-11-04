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
use paros::client::journals::{self, JournalAnswer, JournalOp, JournalRequest};
use paros::core::journal::{JournalIdentifier, TenantId, WriterMode};
use paros::model::address::{Address, Names};
use paros::model::fleet::TenantState;
use paros::model::tags::{Tags, UserTags};
use paros::model::tenant::{
    Desired, Durability, Layout, MAX_SURVIVE_MACHINES, StoreDurability, TenantRoles,
};
use serde_json::json;

use crate::cmd::tenant::nonzero;
use crate::support::cells;
use crate::support::connect::Ending;
use crate::support::labels::Labels;
use crate::support::output::{Printer, note, record_text, table, writer_mode_text};

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
        /// Where its jobs run; none named takes the tenant's default mode.
        #[command(flatten)]
        layout: LayoutArgs,
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

/// Where a journal's jobs run on the command line (§3.4, `--layout`): a
/// layout, then the scale counts that change it (#753 (per-journal
/// proxies)).
#[derive(Args, Debug, Clone, Copy, Default)]
pub struct LayoutArgs {
    /// Where the journal's jobs run: `colocated` (every job in the leader
    /// and acceptor processes) or `compartmentalized` (2 proxy leaders, 2
    /// replicas and, for a multi-writer journal, 2 batchers, unless a count
    /// below changes it).
    #[arg(long, value_parser = parse_layout)]
    layout: Option<Layout>,
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
}

impl LayoutArgs {
    /// Whether it names a layout or a count.
    fn named(&self) -> bool {
        self.layout.is_some()
            || self.replicas.is_some()
            || self.proxies.is_some()
            || self.batchers.is_some()
    }

    /// `held` with the layout it names, then each count it names; a count
    /// not named keeps the layout's, or `held`'s when no layout is named.
    ///
    /// # Errors
    ///
    /// `colocated` with a count that is not 0, or a count above the cap.
    fn apply(self, held: Desired) -> Result<Desired, String> {
        let counts = [self.replicas, self.proxies, self.batchers];
        if self.layout == Some(Layout::Colocated) && counts.iter().flatten().any(|&n| n > 0) {
            return Err("a colocated journal has no replica, proxy or batcher".into());
        }
        let base = self.layout.map_or(held, |layout| held.with_layout(layout));
        base.with_counts(
            self.replicas.unwrap_or(base.replicas),
            self.proxies.unwrap_or(base.proxies),
            self.batchers.unwrap_or(base.batchers),
        )
        .map_err(str::to_string)
    }
}

/// `--layout` (§3.4).
fn parse_layout(text: &str) -> Result<Layout, String> {
    text.parse().map_err(|why: &str| why.to_string())
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
    /// The new layout and scale counts: a count not named keeps its own.
    #[command(flatten)]
    layout: LayoutArgs,
    /// The tenant's frontends (#723 (frontend as a parosd role)): the
    /// coordinator books that many on the cell's `stateless` machines. Only
    /// without `--journal`. Named `--frontend-count`: the global
    /// `--frontends` names the frontends a call goes through.
    #[arg(long)]
    frontend_count: Option<u32>,
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
    let ReadyTenant {
        tenant,
        control,
        election,
        tags: tenant_tags,
        client: cell,
        elsewhere,
    } = match ready_tenant(client, out, &tenant_name).await {
        Ok(found) => found,
        Err(ending) => return ending,
    };
    let client = &cell;
    // The names of the tenant's cell's machines, when another cell hosts it.
    let mut labels = labels.clone();
    if let Some(endpoint) = elsewhere {
        let timeout = Duration::from_millis(args.patience_ms);
        labels.absorb(Labels::fetch_from((providers, rpc, names), &endpoint, timeout).await);
    }
    let labels = &labels;
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
            layout,
            store_durability,
            tags,
            ..
        } => JournalOp::Create {
            name: name.into_bytes(),
            writer: writers,
            desired: match create_desired(client, (tenant, control), durability, layout).await {
                Ok(desired) => desired,
                Err(ending) => return ending,
            },
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

/// The desired mode a create names (§3.4): `None` when it names no
/// durability and no layout, so the tenant's default mode applies whole. A
/// layout without a durability takes the tenant default's durability.
async fn create_desired(
    client: &ParosClient,
    target: (TenantId, paros::core::journal::JournalId),
    durability: DurabilityArgs,
    layout: LayoutArgs,
) -> Result<Option<Desired>, Ending> {
    let named = durability.desired();
    if !layout.named() {
        return Ok(named);
    }
    let base = match named {
        Some(desired) => desired,
        None => held_mode(client, target, None).await?.0,
    };
    scale_counts(&layout, base).map(Some)
}

/// The user tags given on the command line, or a refusal printed.
pub(crate) fn user_tags(texts: &[String]) -> Result<UserTags, Ending> {
    UserTags::parse(texts).map_err(|error| {
        note(&format!("refused: {}: {error}", error.label()));
        Ending::Refused
    })
}

/// The `READY` tenant named `tenant_name`, as [`ready_tenant`] finds it.
struct ReadyTenant {
    tenant: TenantId,
    control: paros::core::journal::JournalId,
    /// The election journal of its cell, where the tenant coordinator
    /// publishes itself.
    election: JournalIdentifier,
    /// Its tags (#575 (key:value tags)).
    tags: Tags,
    /// A client of its cell.
    client: ParosClient,
    /// Its cell's entry endpoint, when another cell than the servers' hosts
    /// it: where the names of that cell's machines are read.
    elsewhere: Option<Vec<Address>>,
}

/// The `READY` tenant named `tenant_name`, through the universe directory
/// of the servers' cell or of the cell its universe pointer names. A tenant
/// another cell hosts is reached through the entry endpoint the directory
/// records for that cell (#232 (multiple cells)).
async fn ready_tenant(
    client: &ParosClient,
    out: &Printer,
    tenant_name: &str,
) -> Result<ReadyTenant, Ending> {
    let universe = cells::universe(client).await?;
    let Some((tenant, entry)) = universe.state.named(tenant_name.as_bytes()) else {
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
    let cell = cells::tenant_cell(client, &universe, entry.cell_id)?;
    let own = entry.cell_id == universe.own.cell_id;
    let election = if own {
        universe.own.election
    } else {
        let mut election = None;
        for server in 0..cell.server_count() {
            election = cell
                .inspect_node(server)
                .await
                .as_ref()
                .and_then(paros::client::bootstrap::control_journals_of)
                .filter(|journals| journals.cell_id == entry.cell_id)
                .and_then(|journals| journals.election);
            if election.is_some() {
                break;
            }
        }
        election
    };
    let Some(election) = election else {
        note(&format!(
            "no machine of tenant {tenant_name}'s cell named its election journal"
        ));
        return Err(Ending::Unreachable);
    };
    Ok(ReadyTenant {
        tenant,
        control: entry.control,
        election,
        tags: entry.all_tags(),
        elsewhere: (!own).then(|| {
            universe
                .state
                .endpoint(entry.cell_id)
                .iter()
                .map(|(_, addr)| addr.clone())
                .collect()
        }),
        client: cell,
    })
}

/// The role counts `args` names (#604 (role counts)), each count it does
/// not name kept from `held`, or a refusal printed.
fn role_counts(args: &ConfigureArgs, held: TenantRoles) -> Result<TenantRoles, Ending> {
    TenantRoles::new(args.frontend_count.unwrap_or(held.frontends)).map_err(|error| {
        note(&format!("refused: {error}"));
        Ending::Refused
    })
}

/// The scale counts `args` names (#753 (per-journal proxies)), each count
/// it does not name kept from `held`, or a refusal printed.
fn scale_counts(args: &LayoutArgs, held: Desired) -> Result<Desired, Ending> {
    args.apply(held).map_err(|error| {
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
    let asked = match args.named() {
        Ok(asked) => asked,
        Err(error) => {
            note(error);
            return Ending::Refused;
        }
    };
    let ready = match ready_tenant(client, out, &args.tenant).await {
        Ok(found) => found,
        Err(ending) => return ending,
    };
    let (tenant, control, election, client) =
        (ready.tenant, ready.control, ready.election, &ready.client);
    // A count or a mode not named keeps the target's: read the fold when
    // the command names only part of them.
    let current = if asked.reads_held() {
        match held_mode(client, (tenant, control), args.journal.as_deref()).await {
            Ok(held) => Some(held),
            Err(ending) => return ending,
        }
    } else {
        None
    };
    let (desired, roles) = match args.wanted(&asked, current) {
        Ok(wanted) => wanted,
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
    configured(out, &args.target(), (desired, roles), &answer)
}

/// What a `configure` names: the tenant's role counts, a journal's scale
/// counts, its durability.
struct Named {
    roles: bool,
    counts: bool,
    mode: Option<Desired>,
}

impl Named {
    /// Whether the target's mode is read first: a count or a mode the
    /// command does not name keeps the one it holds.
    fn reads_held(&self) -> bool {
        self.roles || self.mode.is_none() || self.counts
    }
}

impl ConfigureArgs {
    /// What it names; refused when it names nothing, or the tenant-wide
    /// frontend count for one journal.
    fn named(&self) -> Result<Named, &'static str> {
        let roles = self.frontend_count.is_some();
        if roles && self.journal.is_some() {
            return Err("--frontend-count is the tenant default's: drop --journal");
        }
        let asked = Named {
            roles,
            counts: self.layout.named(),
            mode: self.durability.desired(),
        };
        if asked.mode.is_none() && !asked.counts && !asked.roles {
            return Err(
                "name a mode: --survive, --partitions, --layout, --replicas, --proxies, --batchers or --frontend-count",
            );
        }
        Ok(asked)
    }

    /// The mode and the role counts to record: what it names over what
    /// `current` holds (the defaults when nothing was read).
    fn wanted(
        &self,
        asked: &Named,
        current: Option<(Desired, TenantRoles)>,
    ) -> Result<(Desired, Option<TenantRoles>), Ending> {
        let roles = if asked.roles {
            let held = current.map(|(_, roles)| roles).unwrap_or_default();
            Some(role_counts(self, held)?)
        } else {
            None
        };
        let held = current.map_or(Desired::DEFAULT, |(desired, _)| desired);
        // A named durability keeps the counts held; a count not named keeps
        // its own.
        let mode = asked.mode.map_or(held, |mode| Desired {
            durability: mode.durability,
            ..held
        });
        Ok((scale_counts(&self.layout, mode)?, roles))
    }

    /// The journal it configures, or the tenant's default.
    fn target(&self) -> String {
        self.journal.as_ref().map_or_else(
            || format!("{} (default)", self.tenant),
            |j| format!("{}/{j}", self.tenant),
        )
    }
}

/// Print what configuring `target` came to.
fn configured(
    out: &Printer,
    target: &str,
    (desired, roles): (Desired, Option<TenantRoles>),
    answer: &JournalAnswer,
) -> Ending {
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
        JournalAnswer::NotCoordinator | JournalAnswer::Unavailable => undecided(out, label),
        _ => {
            out.emit(
                || format!("refused: {label}"),
                || json!({ "outcome": label, "target": target }),
            );
            Ending::Refused
        }
    }
}

/// A request no coordinator decided: ambiguous, run it again.
fn undecided(out: &Printer, label: &str) -> Ending {
    note("no coordinator decided the request: run the same command again");
    out.emit(
        || format!("undecided: {label}"),
        || json!({ "outcome": label }),
    );
    Ending::Ambiguous
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
        JournalAnswer::NotCoordinator | JournalAnswer::Unavailable => undecided(out, label),
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
            let rows: Vec<[String; 8]> = chosen
                .iter()
                .map(|(_, journal)| {
                    let members: Vec<u64> = journal.config.members().iter().map(|m| m.0).collect();
                    [
                        format!("{name}/{}", record_text(&journal.name)),
                        writer_mode_text(journal.writer).to_string(),
                        journal.store.as_str().to_string(),
                        journal.desired.layout().as_str().to_string(),
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
                    "JOURNAL", "WRITER", "STORE", "LAYOUT", "DESIRED", "ACCEPTORS", "STATE",
                    "TAGS",
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
                    "mode": writer_mode_text(journal.writer),
                    "store_durability": journal.store.as_str(),
                    "layout": journal.desired.layout().as_str(),
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

#[cfg(test)]
mod tests {
    use paros::client::journals::JournalAnswer;
    use paros::core::NodeId;
    use paros::core::journal::{JournalId, JournalIdentifier, TenantId, WriterMode};
    use paros::core::membership::{AcceptorConfig, QuorumSystem};
    use paros::wire::view::{MachineView, ViewReply};

    use super::{
        ConfigureArgs, Desired, Durability, DurabilityArgs, JournalCommand, Layout, LayoutArgs,
        TenantRoles, configured, parse_survive, report, writer_mode_text,
    };
    use crate::support::connect::Ending;
    use crate::support::labels::Labels;
    use crate::support::output::Printer;

    fn args(layout: Option<Layout>, counts: [Option<u32>; 3]) -> LayoutArgs {
        let [replicas, proxies, batchers] = counts;
        LayoutArgs {
            layout,
            replicas,
            proxies,
            batchers,
        }
    }

    #[test]
    fn compartmentalized_books_two_of_each_role() {
        let desired = args(Some(Layout::Compartmentalized), [None; 3])
            .apply(Desired::DEFAULT)
            .expect("placeable counts");
        assert_eq!(
            (desired.replicas, desired.proxies, desired.batchers),
            (2, 2, 2)
        );
    }

    #[test]
    fn a_named_count_changes_the_layout_count() {
        let desired = args(Some(Layout::Compartmentalized), [Some(0), Some(3), None])
            .apply(Desired::DEFAULT)
            .expect("placeable counts");
        assert_eq!(
            (desired.replicas, desired.proxies, desired.batchers),
            (0, 3, 2)
        );
    }

    #[test]
    fn without_a_layout_a_count_not_named_keeps_the_held_one() {
        let held = Desired::DEFAULT.with_counts(1, 2, 3).expect("counts");
        let desired = args(None, [None, Some(5), None])
            .apply(held)
            .expect("placeable counts");
        assert_eq!(
            (desired.replicas, desired.proxies, desired.batchers),
            (1, 5, 3)
        );
    }

    #[test]
    fn colocated_clears_the_held_counts_and_keeps_the_durability() {
        let held = Desired {
            durability: Durability::new(2, 1).expect("durability"),
            ..Desired::DEFAULT
        }
        .with_counts(1, 2, 3)
        .expect("counts");
        let desired = args(Some(Layout::Colocated), [None; 3])
            .apply(held)
            .expect("placeable counts");
        assert_eq!(desired.layout(), Layout::Colocated);
        assert_eq!(desired.durability, held.durability);
    }

    #[test]
    fn colocated_with_a_count_is_refused() {
        assert!(
            args(Some(Layout::Colocated), [None, Some(1), None])
                .apply(Desired::DEFAULT)
                .is_err()
        );
        // A count of 0 says the same thing as colocated.
        assert!(
            args(Some(Layout::Colocated), [Some(0), None, None])
                .apply(Desired::DEFAULT)
                .is_ok()
        );
    }

    #[test]
    fn a_count_above_the_cap_is_refused() {
        assert!(
            args(None, [Some(65), None, None])
                .apply(Desired::DEFAULT)
                .is_err()
        );
    }

    #[test]
    fn survive_reads_machines_and_takes_no_zone_or_region_yet() {
        assert_eq!(parse_survive("machines=2"), Ok(2));
        assert_eq!(parse_survive(" machines = 3 ,zones=0, regions=0,"), Ok(3));
        assert_eq!(parse_survive("machines=0"), Ok(0));
        assert_eq!(parse_survive(""), Ok(1), "the default");
        assert_eq!(parse_survive("zones=0"), Ok(1));
        assert_eq!(
            parse_survive("machines=4"),
            Err("machines is at most 3".to_string())
        );
        assert_eq!(
            parse_survive("zones=1"),
            Err("zones=1 is not built yet (#748 (survive))".to_string())
        );
        assert_eq!(
            parse_survive("regions=2"),
            Err("regions=2 is not built yet (#748 (survive))".to_string())
        );
        assert_eq!(
            parse_survive("racks=1"),
            Err("survive names machines, zones and regions, not racks".to_string())
        );
        assert_eq!(
            parse_survive("machines"),
            Err("survive is machines=M,zones=Z,regions=R".to_string())
        );
        assert_eq!(
            parse_survive("machines=x"),
            Err("a survive value is a number".to_string())
        );
    }

    #[test]
    fn a_durability_names_a_mode_only_when_it_names_a_field() {
        let durability = |survive_machines, partitions| DurabilityArgs {
            survive_machines,
            partitions,
        };
        assert_eq!(durability(None, None).desired(), None);
        let machines = durability(Some(2), None).desired().expect("a mode");
        assert_eq!(machines.durability, Durability::new(2, 1).expect("valid"));
        assert_eq!(
            (machines.replicas, machines.proxies, machines.batchers),
            (0, 0, 0)
        );
        let partitions = durability(None, Some(3)).desired().expect("a mode");
        assert_eq!(partitions.durability, Durability::new(1, 3).expect("valid"));
    }

    #[test]
    fn a_layout_is_named_by_any_of_its_fields() {
        assert!(!args(None, [None; 3]).named());
        assert!(args(Some(Layout::Compartmentalized), [None; 3]).named());
        assert!(args(None, [Some(1), None, None]).named());
        assert!(args(None, [None, Some(1), None]).named());
        assert!(args(None, [None, None, Some(1)]).named());
    }

    #[test]
    fn a_command_names_its_tenant_and_journal() {
        use clap::Parser as _;
        #[derive(clap::Parser)]
        struct Cli {
            #[command(subcommand)]
            command: JournalCommand,
        }
        let names = |line: &[&str]| {
            let mut words = vec!["journal"];
            words.extend_from_slice(line);
            Cli::try_parse_from(words).expect("valid").command.names()
        };
        assert_eq!(
            names(&["create", "acme", "orders"]),
            ("acme".into(), "orders".into())
        );
        assert_eq!(
            names(&["delete", "acme", "orders"]),
            ("acme".into(), "orders".into())
        );
        assert_eq!(
            names(&["tag", "acme", "orders"]),
            ("acme".into(), "orders".into())
        );
        assert_eq!(names(&["list", "acme"]), ("acme".into(), String::new()));
    }

    #[test]
    fn a_writer_mode_has_its_label() {
        assert_eq!(writer_mode_text(WriterMode::Single), "single");
        assert_eq!(writer_mode_text(WriterMode::Multi), "multi");
    }

    #[test]
    fn each_answer_prints_its_outcome_and_ends_by_it() {
        let labels = Labels::of(&ViewReply {
            machines: vec![
                MachineView {
                    node_id: 1,
                    name: "alpha".into(),
                    ..MachineView::default()
                },
                MachineView {
                    node_id: 2,
                    name: "beta".into(),
                    ..MachineView::default()
                },
            ],
            ..ViewReply::default()
        });
        let tenant = TenantId(10);
        let id = JournalId(11);
        let journal = JournalIdentifier::new(tenant, id).to_string();
        let config = AcceptorConfig::new(vec![NodeId(2), NodeId(1)], QuorumSystem::Majority);
        let cases = [
            (
                JournalAnswer::Created { id, config },
                Ending::Success,
                "created journal=orders members=[alpha,beta]".to_string(),
                serde_json::json!({ "outcome": "created", "journal": journal, "members": [1, 2] }),
            ),
            (
                JournalAnswer::Deleted { id },
                Ending::Success,
                "deleted journal=orders".to_string(),
                serde_json::json!({ "outcome": "deleted", "journal": journal }),
            ),
            (
                JournalAnswer::Tagged { id },
                Ending::Success,
                "tagged journal=orders".to_string(),
                serde_json::json!({ "outcome": "tagged", "journal": journal }),
            ),
            (
                JournalAnswer::NameTaken { id },
                Ending::Refused,
                "refused: name_taken: a live journal is named orders".to_string(),
                serde_json::json!({ "outcome": "name_taken", "journal": journal }),
            ),
            (
                JournalAnswer::NotCoordinator,
                Ending::Ambiguous,
                "undecided: not_coordinator".to_string(),
                serde_json::json!({ "outcome": "not_coordinator" }),
            ),
            (
                JournalAnswer::Unavailable,
                Ending::Ambiguous,
                "undecided: unavailable".to_string(),
                serde_json::json!({ "outcome": "unavailable" }),
            ),
            (
                JournalAnswer::Unplaceable,
                Ending::Refused,
                "refused: unplaceable".to_string(),
                serde_json::json!({ "outcome": "unplaceable" }),
            ),
        ];
        for (answer, ending, text, doc) in cases {
            let (people, machines) = (Printer::new(false), Printer::new(true));
            assert_eq!(
                report(&people, (tenant, "orders"), &labels, &answer),
                ending
            );
            assert_eq!(
                report(&machines, (tenant, "orders"), &labels, &answer),
                ending
            );
            assert_eq!(people.printed(), [text]);
            let printed: serde_json::Value =
                serde_json::from_str(&machines.printed()[0]).expect("JSON");
            assert_eq!(printed, doc);
        }
    }

    fn configure(line: &[&str]) -> ConfigureArgs {
        use clap::Parser as _;
        #[derive(clap::Parser)]
        struct Cli {
            #[command(flatten)]
            args: ConfigureArgs,
        }
        let mut words = vec!["configure", "--tenant", "acme"];
        words.extend_from_slice(line);
        Cli::try_parse_from(words).expect("valid").args
    }

    #[test]
    fn configure_names_a_mode_and_never_a_tenant_count_for_one_journal() {
        assert_eq!(
            configure(&[]).named().err(),
            Some(
                "name a mode: --survive, --partitions, --layout, --replicas, --proxies, \
                 --batchers or --frontend-count"
            )
        );
        assert_eq!(
            configure(&["--journal", "orders", "--frontend-count", "2"])
                .named()
                .err(),
            Some("--frontend-count is the tenant default's: drop --journal")
        );
        let durability = configure(&["--survive", "machines=2"])
            .named()
            .expect("named");
        assert!(
            !durability.reads_held(),
            "a durability alone needs nothing held"
        );
        for line in [
            &["--frontend-count", "2"][..],
            &["--proxies", "1"],
            &["--survive", "machines=2", "--proxies", "1"],
        ] {
            assert!(
                configure(line).named().expect("named").reads_held(),
                "{line:?}"
            );
        }
    }

    #[test]
    fn configure_keeps_what_it_does_not_name() {
        let held = (
            Desired {
                durability: Durability::new(2, 1).expect("valid"),
                proxies: 3,
                ..Desired::DEFAULT
            },
            TenantRoles::new(4).expect("valid"),
        );
        let wanted = |line: &[&str], current| {
            let args = configure(line);
            let asked = args.named().expect("named");
            args.wanted(&asked, current).expect("placeable")
        };
        let (mode, roles) = wanted(&["--survive", "machines=3"], Some(held));
        assert_eq!(mode.durability, Durability::new(3, 1).expect("valid"));
        assert_eq!(mode.proxies, 3, "the held count");
        assert_eq!(roles, None, "no role count named");
        let (mode, roles) = wanted(&["--frontend-count", "1"], Some(held));
        assert_eq!(mode, held.0, "the held mode");
        assert_eq!(roles, Some(TenantRoles::new(1).expect("valid")));
        let (mode, _) = wanted(&["--batchers", "2"], Some(held));
        assert_eq!((mode.proxies, mode.batchers), (3, 2));
        assert_eq!(mode.durability, held.0.durability);
        let (mode, roles) = wanted(&["--survive", "machines=0"], None);
        assert_eq!(mode.durability, Durability::new(0, 1).expect("valid"));
        assert_eq!((mode.proxies, roles), (0, None), "the defaults");
        let args = configure(&["--frontend-count", "65"]);
        let asked = args.named().expect("named");
        assert!(
            args.wanted(&asked, Some(held)).is_err(),
            "above the role cap"
        );
    }

    #[test]
    fn configure_names_its_target_and_prints_the_answer() {
        assert_eq!(configure(&["--proxies", "1"]).target(), "acme (default)");
        assert_eq!(
            configure(&["--journal", "orders", "--proxies", "1"]).target(),
            "acme/orders"
        );
        let mode = Desired::DEFAULT;
        let out = Printer::new(false);
        let done = JournalAnswer::Configured { id: None };
        assert_eq!(
            configured(&out, "acme (default)", (mode, None), &done),
            Ending::Success
        );
        let roles = Some(TenantRoles::new(2).expect("valid"));
        configured(&out, "acme (default)", (mode, roles), &done);
        assert_eq!(
            configured(
                &out,
                "acme/orders",
                (mode, None),
                &JournalAnswer::Unavailable
            ),
            Ending::Ambiguous
        );
        assert_eq!(
            configured(
                &out,
                "acme/orders",
                (mode, None),
                &JournalAnswer::UnknownJournal
            ),
            Ending::Refused
        );
        let label = mode.label();
        assert_eq!(
            out.printed(),
            [
                format!("configured acme (default): {label}"),
                format!("configured acme (default): {label}, 2 frontends"),
                "undecided: unavailable".to_string(),
                "refused: unknown_journal".to_string(),
            ]
        );
        let json = Printer::new(true);
        configured(&json, "acme/orders", (mode, roles), &done);
        configured(
            &json,
            "acme/orders",
            (mode, None),
            &JournalAnswer::UnknownTenant,
        );
        let docs: Vec<serde_json::Value> = json
            .printed()
            .iter()
            .map(|line| serde_json::from_str(line).expect("JSON"))
            .collect();
        assert_eq!(
            docs[0],
            serde_json::json!({
                "outcome": "configured",
                "target": "acme/orders",
                "mode": label,
                "roles": "2 frontends",
            })
        );
        assert_eq!(
            docs[1],
            serde_json::json!({ "outcome": "unknown_tenant", "target": "acme/orders" })
        );
    }
}
