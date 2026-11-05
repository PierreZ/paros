//! `parosctl tenant create|delete|tag|list` (#229): the fleet operations of
//! `paros::client::fleet` against the fleet directory and the cell's control
//! journal, and what `init` shares with them (the session, the outcomes'
//! endings and labels).
//!
//! No identifier is fixed (`docs/architecture/03-8-identifiers-checkpoints-recovery-security.md` §3.8): the cell's and the fleet tenant's
//! control journals are learned from the servers' `Inspect`
//! (`paros::client::bootstrap::cell_frames`). Every operation here claims
//! both journals under a leader uuid of its own (#241), drawn per run, so it
//! fences whatever run led them before (the cell coordinator of #225 is not
//! built; §3.7: the fleet's one cell hosts the fleet tenant). An interrupted `init`
//! or `delete` is resumed by running the same command again; a tenant is
//! created once, so a second `create` of a name is refused, and an
//! interrupted creation stays `REGISTERING` until it is deleted (or, with
//! #225, finished by the coordinator).

use std::collections::BTreeMap;
use std::time::Duration;

use clap::{Args, Subcommand};
use moonpool_core::{CompioProviders, LocalProviders, RandomProvider};
use paros::client::Client;
use paros::client::bootstrap::control_journals;
use paros::client::fleet::{FleetRefusal, FleetSession, Interrupted, Run, Stage, Step, TagTarget};
use paros::core::NodeId;
use paros::core::journal::{JournalId, JournalIdentifier, TenantId};
use paros::machine::ControlJournals;
use paros::model::fleet::TenantState;
use paros::model::registry::Registry;
use paros::wire::view::view_request::Query;
use paros::wire::view::{TenantQuery, UniverseQuery};
use serde_json::json;

use crate::support::connect::Ending;
use crate::support::labels::Labels;
use crate::support::output::{Printer, note, table};
use crate::support::view::{
    Asker, coordinator_text, indent, journal_label, machine_names, machine_state, reply_json,
    selected, selector, tag, tags_text, text, user_tags_text,
};

type ParosClient = Client<CompioProviders>;

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
    pub command: TenantCommand,
}

#[derive(Subcommand, Debug)]
pub enum TenantCommand {
    /// Create a `users` tenant (`REGISTERING` in the universe directory, hosted by the
    /// cell, then `READY`). A tenant is created once: a name the universe directory
    /// holds, in any state, is refused. The CLI never creates an `internal` tenant.
    Create {
        /// The tenant's name.
        name: String,
        /// What its journals survive: `az` (a zone, the default) or
        /// `region` (#252). Fixed at creation.
        #[arg(long, default_value = "az")]
        survives: paros::model::tenant::Survives,
        /// A user tag, `key:value`, repeated (#575 (key:value tags)). A
        /// `paros/` key is refused.
        #[arg(long = "tag")]
        tags: Vec<String>,
        /// The cell it lives in, by name: a `READY` cell of the universe.
        /// It writes an override entry. By default the tenant goes to the
        /// cell its bucket names (`universe status`).
        #[arg(long, conflicts_with = "cell_selector")]
        cell: Option<String>,
        /// Pick the cell by its tags, `key:value`, comma separated, all of
        /// them: of the `READY` cells that match and carry what the tenant
        /// survives, the one with the fewest tenants, then by name. It
        /// writes an override entry.
        #[arg(long)]
        cell_selector: Option<String>,
        /// Why the tenant has its own cell (`dedicated`, `canary`,
        /// `quarantine`, ...): the override's reason, for people. Needs
        /// `--cell` or `--cell-selector`.
        #[arg(long)]
        reason: Option<String>,
    },
    /// Set a `users` tenant's user tags, whole: the tags given replace the
    /// ones it has, and none clears them (#575).
    Tag {
        /// The tenant's name.
        name: String,
        /// The tags, `key:value` each.
        tags: Vec<String>,
    },
    /// Move a `users` tenant to another cell (#784 (coordinator handover)):
    /// the universe directory opens the move, the two cells move its
    /// journals while they serve, and the directory then names the new
    /// cell. A re-run resumes.
    Move {
        /// The tenant's name.
        name: String,
        /// The cell it moves to, by name: a `READY` cell of the universe.
        #[arg(long)]
        cell: String,
    },
    /// Remove a `users` tenant (`REMOVING`, dropped by the cell, then
    /// removed). A re-run resumes.
    Delete {
        /// The tenant's name.
        name: String,
    },
    /// List the universe's tenants, the internal ones included (#399).
    List {
        /// Only the tenants whose tags match: `key`, `key:value`, comma
        /// separated, all of them (#575).
        #[arg(short = 'l', long)]
        selector: Option<String>,
    },
    /// Show a tenant: its cell, state, journals, their acceptors and the
    /// machines they use (#399).
    Show {
        /// The tenant's name.
        name: String,
    },
}

/// A random non-zero id.
pub fn nonzero(providers: &CompioProviders) -> u64 {
    loop {
        let id: u64 = providers.random().random();
        if id != 0 {
            return id;
        }
    }
}

/// A random leader seed: every run leads its own terms (#241).
pub fn leader_seed(providers: &CompioProviders) -> u128 {
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
#[allow(clippy::too_many_lines)] // one arm per refusal
pub fn refusal_text(refusal: &FleetRefusal, labels: &Labels) -> String {
    match refusal {
        FleetRefusal::Unset => "unset: an id or an identifier of 0".into(),
        FleetRefusal::NotInitialized => "not_initialized: run parosctl init first".into(),
        FleetRefusal::NameRefused => {
            "name_refused: a tenant name holds no ':' (an internal tenant shows as <kind>:<name>)"
                .into()
        }
        FleetRefusal::Unknown => "unknown: no tenant or cell holds the name".into(),
        FleetRefusal::NotMovable { tenant } => format!(
            "not_movable: tenant {} is not READY, is internal, lives in that cell, or moves elsewhere now",
            tenant.0
        ),
        FleetRefusal::CellMismatch { expected, found } => format!(
            "cell_mismatch: expected universe={} cell={}, the cell records {}",
            labels.universe(expected.0),
            labels.cell(expected.1),
            found.map_or_else(
                || "nothing".to_string(),
                |f| format!(
                    "universe={} cell={}",
                    labels.universe(f.fleet_id),
                    labels.cell(f.cell_id)
                )
            )
        ),
        FleetRefusal::CellRemoving => "cell_removing".into(),
        FleetRefusal::CellNotReady { cell_id } => {
            format!(
                "cell_not_ready: cell {} is not READY",
                labels.cell(*cell_id)
            )
        }
        FleetRefusal::UnknownCell { cell_id } => {
            format!(
                "unknown_cell: the universe holds no cell {}",
                labels.cell(*cell_id)
            )
        }
        FleetRefusal::CellRemoved { cell_id } => format!(
            "cell_removed: cell {} was removed from the universe, and never comes back",
            labels.cell(*cell_id)
        ),
        FleetRefusal::CellNotEmpty { cell_id, tenant } => format!(
            "cell_not_empty: cell {} hosts tenant {}: delete it first",
            labels.cell(*cell_id),
            labels.tenant(*tenant)
        ),
        FleetRefusal::HostsUniverse { cell_id } => format!(
            "hosts_universe: cell {} hosts the universe tenant",
            labels.cell(*cell_id)
        ),
        FleetRefusal::NoEndpoint { cell_id } => format!(
            "no_endpoint: the universe records no machine of cell {}",
            labels.cell(*cell_id)
        ),
        FleetRefusal::NoFootprint { tenant } => format!(
            "no_footprint: the cell has too few free slots for tenant {}'s footprint \
             (it is marked ERROR: delete it to free its name)",
            labels.tenant(*tenant)
        ),
        FleetRefusal::NoCellMatches => "no_cell_matches: no READY cell matches the selector \
             and carries what the tenant survives"
            .into(),
        FleetRefusal::NoBucketCell { bucket } => {
            format!("no_bucket_cell: bucket {bucket} names no cell: add a cell first")
        }
        FleetRefusal::BucketNotEmpty { bucket, tenant } => format!(
            "bucket_not_empty: tenant {} lives in bucket {bucket} on another cell, \
             and tenants do not move between cells yet",
            labels.tenant(*tenant)
        ),
        FleetRefusal::CellHoldsBuckets { cell_id, buckets } => format!(
            "cell_holds_buckets: cell {} holds {buckets} buckets: \
             assign them to another cell first (universe buckets assign)",
            labels.cell(*cell_id)
        ),
        FleetRefusal::IdTaken => "id_taken".into(),
        FleetRefusal::NameTaken { tenant, state } => format!(
            "name_taken: tenant {} is {}{}",
            labels.tenant(*tenant),
            state.as_str(),
            if *state == TenantState::Registering {
                " (an interrupted creation: delete it to create the name again)"
            } else {
                ""
            }
        ),
        FleetRefusal::OtherCell { node, cell_id } => format!(
            "other_cell: machine {} belongs to {}",
            labels.machine(node.0),
            if *cell_id == 0 {
                "another cell".to_string()
            } else {
                format!("cell {}", labels.cell(*cell_id))
            }
        ),
        FleetRefusal::InCellInit { node } => format!(
            "in_cell_init: machine {} promised in a cell init and has no cell: \
             finish that cell init, or wipe the machine",
            labels.machine(node.0)
        ),
        FleetRefusal::Retired { node } => {
            format!(
                "retired: machine {} was retired from this cell",
                labels.machine(node.0)
            )
        }
        FleetRefusal::Removed { tenant } => {
            format!(
                "removed: tenant {} was removed while being created",
                labels.tenant(*tenant)
            )
        }
        FleetRefusal::RootKeyTaken => "root_key_taken: another root key, in force or \
             removed, has this key id or this label: generate a new key"
            .into(),
        FleetRefusal::UnknownRootKey => {
            "unknown_root_key: no root key in force has this label".into()
        }
        FleetRefusal::LastRootKey => "last_root_key: the universe keeps its last root key: \
             add the next key first"
            .into(),
        FleetRefusal::MalformedRootKey => {
            "malformed_root_key: not an ed25519 public key file".into()
        }
        FleetRefusal::Denied { node, denial } => format!(
            "{}: machine {} checks an admin token against its pinned root key \
             (PAROS_ROOT_PUBLIC_KEY): pass one with --token or PAROS_TOKEN",
            denial.as_str(),
            labels.machine(node.0)
        ),
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
        Interrupted::NotJoined { .. } => {
            note(
                "the cell coordinator has not joined the cell to the universe yet: \
                 nothing was lost, run the same command again",
            );
            Ending::Unreachable
        }
        Interrupted::Unpointed { .. } => {
            note(
                "the cell coordinator has not repointed the cell at the universe's machines yet: \
                 nothing was lost, run the same command again",
            );
            Ending::Unreachable
        }
        Interrupted::NotHosted { .. }
        | Interrupted::NotDropped { .. }
        | Interrupted::NotSettled { .. } => {
            note(
                "the cell coordinator has not written the cell's step for this tenant yet: \
                 nothing was lost, run the same command again",
            );
            Ending::Unreachable
        }
        _ => Ending::Unreachable,
    }
}

/// How a fleet run ends: `done` prints a finished run from its result and
/// the steps it wrote (none when nothing changed); a refusal or an
/// interruption ends the way every fleet command ends it.
pub fn finish<T>(
    out: &Printer,
    labels: &Labels,
    run: Run<T>,
    done: impl FnOnce(T, &[Stage]) -> Ending,
) -> Ending {
    match run.outcome {
        Step::Done { result, .. } => done(result, &run.steps),
        Step::Refused(refusal) => refused(out, &refusal, labels),
        Step::Interrupted(stop) => interrupted(&stop),
        Step::Advanced(_) => unreachable!("a run never ends advanced"),
    }
}

/// `done`, or `unchanged` when a run wrote no step.
pub fn outcome<'a>(written: &[Stage], done: &'a str) -> &'a str {
    if written.is_empty() {
        "unchanged"
    } else {
        done
    }
}

/// ` steps=A,B` for the steps a run wrote, or nothing when it wrote none.
pub fn steps_text(written: &[Stage]) -> String {
    if written.is_empty() {
        String::new()
    } else {
        format!(" steps={}", steps(written).join(","))
    }
}

/// The steps a run wrote, as labels.
pub fn steps(steps: &[Stage]) -> Vec<String> {
    steps.iter().map(|s| format!("{s:?}")).collect()
}

/// A fleet session over the servers' initialized cell, under a leader seed
/// of its own; else how the command ends.
pub async fn open_session(
    providers: &CompioProviders,
    client: &ParosClient,
    servers: &[u64],
) -> Result<FleetSession, Ending> {
    let Some(journals) = control_journals(client).await else {
        note("no server named its cell's control journals: is the cell initialized?");
        return Err(Ending::Unreachable);
    };
    if journals.fleet.is_none() {
        note("the cell names no fleet journal");
        return Err(Ending::Refused);
    }
    match initialized(client, &journals).await {
        Some(true) => {}
        Some(false) => {
            note("the cell control journal has no leader: run parosctl init first");
            return Err(Ending::Refused);
        }
        None => {
            note("no server served the cell control journal's state");
            return Err(Ending::Unreachable);
        }
    }
    session(client, journals, leader_seed(providers), servers).ok_or_else(|| {
        note("the cell names no fleet journal");
        Ending::Refused
    })
}

/// `parosctl tenant …`.
pub async fn run(
    providers: &CompioProviders,
    client: &ParosClient,
    servers: &[u64],
    labels: &Labels,
    out: &Printer,
    args: TenantArgs,
) -> Ending {
    let mut fleet = match open_session(providers, client, servers).await {
        Ok(fleet) => fleet,
        Err(ending) => return ending,
    };
    let patience = Duration::from_millis(args.patience_ms);
    match args.command {
        TenantCommand::Create {
            name,
            survives,
            tags,
            cell,
            cell_selector,
            reason,
        } => {
            let tags = match crate::cmd::journal::user_tags(&tags) {
                Ok(tags) => tags,
                Err(ending) => return ending,
            };
            fleet = match target(client, fleet, cell, cell_selector, reason).await {
                Ok(fleet) => fleet,
                Err(ending) => return ending,
            };
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
            let mut fleet = fleet.with_survives(survives).with_tags(tags);
            let run = fleet
                .create_tenant(client, 0, name.as_bytes(), draws, patience)
                .await;
            let placed = match &run.outcome {
                Step::Done { result, .. } => placed(&fleet, *result),
                _ => None,
            };
            report(out, "created", &name, run, labels, placed.as_ref())
        }
        TenantCommand::Delete { name } => {
            let run = fleet
                .remove_tenant(client, 0, name.as_bytes(), patience)
                .await;
            finish(out, labels, run, |removed, written| {
                let Some(tenant) = removed else {
                    out.emit(
                        || format!("no tenant named {name}"),
                        || json!({ "outcome": "not_found", "name": name }),
                    );
                    return Ending::Refused;
                };
                reported(out, ("deleted", &name, tenant), written, None)
            })
        }
        TenantCommand::Move { name, cell } => {
            move_to(client, &mut fleet, (out, labels), (&name, &cell), patience).await
        }
        TenantCommand::Tag { name, tags } => {
            set_tags(
                providers,
                client,
                (out, labels),
                (&mut fleet, patience),
                TagTarget::Tenant(name.as_bytes()),
                &name,
                &tags,
            )
            .await
        }
        TenantCommand::List { .. } | TenantCommand::Show { .. } => {
            unreachable!("the views are answered in crate::support::view")
        }
    }
}

/// `tenant move` (#784 (coordinator handover)): move tenant `name` to the
/// cell named `cell`.
async fn move_to(
    client: &ParosClient,
    fleet: &mut FleetSession,
    (out, labels): (&Printer, &Labels),
    (name, cell): (&str, &str),
    patience: Duration,
) -> Ending {
    let to = match cell_named(client, fleet, cell).await {
        Ok(to) => to,
        Err(ending) => return ending,
    };
    let run = fleet
        .move_tenant(client, 0, (name.as_bytes(), to), patience)
        .await;
    report(out, "moved", name, run, labels, None)
}

/// `tenant tag` and `cell tag` (#575 (key:value tags)): set `target`'s user
/// tags to `texts`, under an idempotency id drawn for this run.
pub async fn set_tags(
    providers: &CompioProviders,
    client: &ParosClient,
    (out, labels): (&Printer, &Labels),
    (fleet, patience): (&mut FleetSession, Duration),
    target: TagTarget<'_>,
    name: &str,
    texts: &[String],
) -> Ending {
    let tags = match crate::cmd::journal::user_tags(texts) {
        Ok(tags) => tags,
        Err(ending) => return ending,
    };
    let kind = match target {
        TagTarget::Tenant(_) => "tenant",
        TagTarget::Cell(_) => "cell",
    };
    let run = fleet
        .set_tags(client, 0, target, nonzero(providers), &tags, patience)
        .await;
    finish(out, labels, run, |result, _| {
        out.emit(
                || format!("tagged {kind}={name} tags={}", tags_label(&tags)),
                || json!({ "outcome": "tagged", kind: result, "name": name, "tags": tags_label(&tags) }),
            );
        Ending::Success
    })
}

/// User tags as `k:v,…`, or `-` for none.
pub fn tags_label(tags: &paros::model::tags::UserTags) -> String {
    if tags.is_empty() {
        "-".to_string()
    } else {
        tags.tags().label()
    }
}

pub fn refused(out: &Printer, refusal: &FleetRefusal, labels: &Labels) -> Ending {
    let text = refusal_text(refusal, labels);
    out.emit(
        || format!("refused: {text}"),
        || json!({ "outcome": "refused", "refusal": text }),
    );
    Ending::Refused
}

/// Where the directory placed `tenant` (#713 (bucket placement)): its
/// cell's name, and `bucket=N` or `override=REASON`.
fn placed(fleet: &FleetSession, tenant: TenantId) -> Option<(String, String)> {
    use paros::model::fleet::buckets::{Placement, bucket_of};
    let directory = fleet.directory();
    let entry = directory.tenant(tenant)?;
    let cell = String::from_utf8_lossy(directory.cell_name(entry.cell_id)).into_owned();
    let how = match &entry.placement {
        Placement::Bucket => format!("bucket={}", bucket_of(&entry.name)),
        Placement::Override { reason } => format!("override={reason:?}"),
    };
    Some((cell, how))
}

/// Print a create's or delete's run, with where a creation placed the
/// tenant.
fn report(
    out: &Printer,
    done: &str,
    name: &str,
    run: Run<TenantId>,
    labels: &Labels,
    placed: Option<&(String, String)>,
) -> Ending {
    finish(out, labels, run, |tenant, written| {
        reported(out, (done, name, tenant), written, placed)
    })
}

/// Print a finished create or delete of tenant `name`, which wrote
/// `written`.
fn reported(
    out: &Printer,
    (done, name, tenant): (&str, &str, TenantId),
    written: &[Stage],
    placed: Option<&(String, String)>,
) -> Ending {
    let outcome = outcome(written, done);
    let at = placed
        .map(|(cell, how)| format!(" cell={cell} {how}"))
        .unwrap_or_default();
    out.emit(
        || format!("{outcome} tenant={name}{at}{}", steps_text(written)),
        || {
            json!({
                "outcome": outcome,
                "tenant": tenant.0,
                "name": name,
                "cell": placed.map(|(cell, _)| cell),
                "placement": placed.map(|(_, how)| how),
                "steps": steps(written),
            })
        },
    );
    Ending::Success
}

/// `fleet` placing the tenant it creates (#713 (bucket placement), #672
/// (cell tag selector)): in the cell named `cell`, or the one `selector`
/// picks, by an override entry with `reason`; else by its bucket.
async fn target(
    client: &ParosClient,
    fleet: FleetSession,
    cell: Option<String>,
    selector: Option<String>,
    reason: Option<String>,
) -> Result<FleetSession, Ending> {
    if let Some(cell) = cell {
        let cell_id = cell_named(client, &fleet, &cell).await?;
        let reason = reason.unwrap_or_else(|| format!("cell {cell} named"));
        Ok(fleet.with_cell_for(cell_id, &reason))
    } else if let Some(text) = selector {
        let chosen = crate::support::view::selector(Some(&text))?;
        let reason = reason.unwrap_or_else(|| format!("selector {text}"));
        Ok(fleet.with_selector(chosen, &reason))
    } else if reason.is_some() {
        note("--reason names why a tenant has its own cell: give --cell or --cell-selector");
        Err(Ending::Refused)
    } else {
        Ok(fleet)
    }
}

/// The id of the `READY` cell named `cell` (#232 (multiple cells)), read
/// from the universe directory.
async fn cell_named(client: &ParosClient, fleet: &FleetSession, cell: &str) -> Result<u64, Ending> {
    let directory =
        match paros::client::fleet::read_directory(client, 0, fleet.directory_journal()).await {
            Ok(directory) => directory,
            Err(outcome) => {
                note(&format!("the universe directory was not read: {outcome:?}"));
                return Err(Ending::Unreachable);
            }
        };
    directory.cell_named(cell.as_bytes()).ok_or_else(|| {
        note(&format!("the universe holds no cell named {cell}"));
        Ending::Refused
    })
}

// `tenant list` and `tenant show`: views of the cell (#399).

/// `parosctl tenant list`: the universe's tenants, the internal ones (the
/// universe tenant, the cell tenants) included.
pub async fn tenant_list(asker: &Asker<'_>, out: &Printer, chosen: Option<&str>) -> Ending {
    let chosen = match selector(chosen) {
        Ok(chosen) => chosen,
        Err(ending) => return ending,
    };
    let mut reply = match asker.ask(Query::Universe(UniverseQuery {})).await {
        Ok(reply) => reply,
        Err(ending) => return ending,
    };
    reply.tenants.retain(|t| selected(&chosen, &t.tags));
    let cells: BTreeMap<u64, String> = reply
        .cells
        .iter()
        .map(|c| (c.cell_id, text(&c.name)))
        .collect();
    let rows: Vec<[String; 6]> = reply
        .tenants
        .iter()
        .map(|t| {
            [
                text(&t.name),
                t.kind.clone(),
                cells
                    .get(&t.cell_id)
                    .cloned()
                    .unwrap_or_else(|| "-".to_string()),
                t.state.clone(),
                t.survives.clone(),
                user_tags_text(&t.tags),
            ]
        })
        .collect();
    out.emit(
        || {
            table(
                ["TENANT", "KIND", "CELL", "STATE", "SURVIVES", "TAGS"],
                &rows,
            )
        },
        || reply_json(&reply),
    );
    Ending::Success
}

/// `parosctl tenant show <tenant>`.
pub async fn tenant_show(asker: &Asker<'_>, out: &Printer, name: &str) -> Ending {
    let reply = match asker
        .ask(Query::Tenant(TenantQuery {
            name: name.as_bytes().to_vec(),
        }))
        .await
    {
        Ok(reply) => reply,
        Err(ending) => return ending,
    };
    let Some(tenant) = reply.tenants.first() else {
        note("the cell answered no tenant");
        return Ending::Refused;
    };
    let names = machine_names(&reply);
    let members = |ids: &[u64]| {
        let mut listed: Vec<String> = ids
            .iter()
            .map(|id| names.get(id).cloned().unwrap_or_else(|| "-".to_string()))
            .collect();
        listed.sort();
        listed.join(",")
    };
    // A journal's matchmakers are not in the view yet: every journal reads
    // as plain Multi-Paxos, so the table has no column for them.
    let journal_rows: Vec<[String; 5]> = tenant
        .journals
        .iter()
        .map(|j| {
            [
                journal_label(j),
                j.writer.clone(),
                j.store_durability.clone(),
                j.desired.clone(),
                members(&j.acceptors),
            ]
        })
        .collect();
    let mut footprint: BTreeMap<u64, usize> = BTreeMap::new();
    for j in &tenant.journals {
        for id in &j.acceptors {
            *footprint.entry(*id).or_default() += 1;
        }
    }
    let machine_rows: Vec<[String; 4]> = reply
        .machines
        .iter()
        .map(|m| {
            [
                names[&m.node_id].clone(),
                tag(&m.tags, paros::model::tags::reserved::ZONE),
                machine_state(m),
                footprint.get(&m.node_id).copied().unwrap_or(0).to_string(),
            ]
        })
        .collect();
    out.emit(
        || {
            format!(
                "tenant:       {}\ncell:         {}\nstate:        {}\ntags:         {}\nsurvives:     {}\ncoordinator:  {}\njournals:\n{}\nmachines:\n{}",
                text(&tenant.name),
                text(&reply.cell_name),
                tenant.state,
                tags_text(&tenant.tags),
                tenant.survives,
                coordinator_text(&reply, &names),
                indent(&table(
                    ["JOURNAL", "WRITER", "STORE", "DESIRED", "ACCEPTORS"],
                    &journal_rows
                )),
                indent(&table(["MACHINE", "ZONE", "STATE", "ACCEPTORS"], &machine_rows)),
            )
        },
        || reply_json(&reply),
    );
    Ending::Success
}

#[cfg(test)]
mod tests {
    use paros::client::WriterOutcome;
    use paros::client::fleet::{FleetRefusal, Interrupted, Run, Stage, Step};
    use paros::core::NodeId;
    use paros::core::journal::{JournalId, JournalIdentifier, TenantId};
    use paros::model::authz::Denial;
    use paros::model::fleet::TenantState;
    use paros::model::registry::FleetRegistration;
    use paros::wire::view::{MachineView, TenantView, ViewReply};

    use super::{interrupted, nonzero, refusal_text, report};

    #[test]
    fn a_drawn_id_is_never_the_unset_zero() {
        let providers = moonpool_core::CompioProviders::new();
        let ids: Vec<u64> = (0..64).map(|_| nonzero(&providers)).collect();
        assert!(ids.iter().all(|&id| id != 0));
        assert!(
            ids.windows(2).any(|pair| pair[0] != pair[1]),
            "drawn, not fixed"
        );
    }
    use crate::support::connect::Ending;
    use crate::support::labels::Labels;
    use crate::support::output::{Printer, short};

    fn labels() -> Labels {
        Labels::of(&ViewReply {
            cell_id: 0xce11,
            cell_name: b"west".to_vec(),
            universe_id: 0xf1ee7,
            universe_name: b"prod".to_vec(),
            machines: vec![MachineView {
                node_id: 7,
                name: "alpha".into(),
                ..MachineView::default()
            }],
            tenants: vec![TenantView {
                tenant: 10,
                kind: "users".into(),
                name: b"acme".to_vec(),
                ..TenantView::default()
            }],
            ..ViewReply::default()
        })
    }

    fn text(refusal: &FleetRefusal) -> String {
        refusal_text(refusal, &labels())
    }

    fn journal() -> JournalIdentifier {
        JournalIdentifier::new(TenantId(1), JournalId(2))
    }

    #[test]
    fn every_refusal_starts_with_its_label() {
        let cell = 0xce11;
        let tenant = TenantId(10);
        let node = NodeId(7);
        let cases = [
            (FleetRefusal::Unset, "unset: "),
            (FleetRefusal::NotInitialized, "not_initialized: "),
            (FleetRefusal::NameRefused, "name_refused: "),
            (FleetRefusal::Unknown, "unknown: "),
            (FleetRefusal::CellRemoving, "cell_removing"),
            (
                FleetRefusal::CellNotReady { cell_id: cell },
                "cell_not_ready: cell west ",
            ),
            (
                FleetRefusal::UnknownCell { cell_id: cell },
                "unknown_cell: the universe holds no cell west",
            ),
            (
                FleetRefusal::CellRemoved { cell_id: cell },
                "cell_removed: cell west ",
            ),
            (
                FleetRefusal::CellNotEmpty {
                    cell_id: cell,
                    tenant,
                },
                "cell_not_empty: cell west hosts tenant acme: delete it first",
            ),
            (
                FleetRefusal::HostsUniverse { cell_id: cell },
                "hosts_universe: cell west ",
            ),
            (
                FleetRefusal::NoEndpoint { cell_id: cell },
                "no_endpoint: the universe records no machine of cell west",
            ),
            (
                FleetRefusal::NoFootprint { tenant },
                "no_footprint: the cell has too few free slots for tenant acme's",
            ),
            (FleetRefusal::NoCellMatches, "no_cell_matches: "),
            (
                FleetRefusal::NoBucketCell { bucket: 3 },
                "no_bucket_cell: bucket 3 names no cell",
            ),
            (
                FleetRefusal::BucketNotEmpty { bucket: 3, tenant },
                "bucket_not_empty: tenant acme lives in bucket 3 ",
            ),
            (
                FleetRefusal::CellHoldsBuckets {
                    cell_id: cell,
                    buckets: 5,
                },
                "cell_holds_buckets: cell west holds 5 buckets: ",
            ),
            (FleetRefusal::IdTaken, "id_taken"),
            (
                FleetRefusal::InCellInit { node },
                "in_cell_init: machine alpha promised",
            ),
            (
                FleetRefusal::Retired { node },
                "retired: machine alpha was retired",
            ),
            (
                FleetRefusal::Removed { tenant },
                "removed: tenant acme was removed",
            ),
            (FleetRefusal::RootKeyTaken, "root_key_taken: "),
            (FleetRefusal::UnknownRootKey, "unknown_root_key: "),
            (FleetRefusal::LastRootKey, "last_root_key: "),
            (FleetRefusal::MalformedRootKey, "malformed_root_key: "),
            (
                FleetRefusal::Denied {
                    node,
                    denial: Denial::Expired,
                },
                "expired: machine alpha checks an admin token",
            ),
        ];
        for (refusal, start) in cases {
            let found = text(&refusal);
            assert!(found.starts_with(start), "{refusal:?}: {found}");
        }
    }

    #[test]
    fn a_cell_mismatch_names_both_sides() {
        let nothing = FleetRefusal::CellMismatch {
            expected: (0xf1ee7, 0xce11),
            found: None,
        };
        assert_eq!(
            text(&nothing),
            "cell_mismatch: expected universe=prod cell=west, the cell records nothing"
        );
        let other = FleetRefusal::CellMismatch {
            expected: (0xf1ee7, 0xce11),
            found: Some(FleetRegistration {
                fleet_id: 5,
                cell_id: 6,
                version: 1,
            }),
        };
        assert_eq!(
            text(&other),
            format!(
                "cell_mismatch: expected universe=prod cell=west, the cell records universe={} cell={}",
                short(5),
                short(6)
            )
        );
    }

    #[test]
    fn an_interrupted_creation_says_how_to_free_its_name() {
        let held = |state| {
            text(&FleetRefusal::NameTaken {
                tenant: TenantId(10),
                state,
            })
        };
        assert_eq!(
            held(TenantState::Registering),
            "name_taken: tenant acme is REGISTERING (an interrupted creation: delete it to \
             create the name again)"
        );
        assert_eq!(held(TenantState::Ready), "name_taken: tenant acme is READY");
    }

    #[test]
    fn another_cell_is_named_when_known() {
        let other = |cell_id| {
            text(&FleetRefusal::OtherCell {
                node: NodeId(7),
                cell_id,
            })
        };
        assert_eq!(
            other(0),
            "other_cell: machine alpha belongs to another cell"
        );
        assert_eq!(
            other(0xce11),
            "other_cell: machine alpha belongs to cell west"
        );
    }

    #[test]
    fn an_interruption_is_ambiguous_only_when_a_write_may_have_landed() {
        let not_written = |outcome| Interrupted::NotWritten {
            journal: journal(),
            outcome,
        };
        assert_eq!(
            interrupted(&not_written(WriterOutcome::Ambiguous)),
            Ending::Ambiguous
        );
        assert_eq!(
            interrupted(&not_written(WriterOutcome::NotOwner)),
            Ending::Unreachable
        );
        for stop in [
            Interrupted::NotJoined { journal: journal() },
            Interrupted::Unpointed { journal: journal() },
            Interrupted::NotHosted {
                journal: journal(),
                tenant: TenantId(1),
            },
            Interrupted::NotDropped {
                journal: journal(),
                tenant: TenantId(1),
            },
            Interrupted::GoingRound { steps: 3 },
        ] {
            assert_eq!(interrupted(&stop), Ending::Unreachable, "{stop:?}");
        }
    }

    #[test]
    fn a_deleted_tenant_reads_deleted_with_its_steps() {
        let out = Printer::new(false);
        let steps = [Stage::TenantRemoving, Stage::RemoveTenant];
        assert_eq!(
            super::reported(&out, ("deleted", "acme", TenantId(10)), &steps, None),
            Ending::Success
        );
        assert_eq!(
            out.printed(),
            ["deleted tenant=acme steps=TenantRemoving,RemoveTenant"]
        );
        assert_eq!(super::outcome(&[], "deleted"), "unchanged");
        assert_eq!(super::steps_text(&[]), "");
    }

    #[test]
    fn a_run_reports_its_steps_its_placement_or_unchanged() {
        let done = |steps: Vec<Stage>| Run {
            outcome: Step::Done {
                result: TenantId(10),
                last: None,
            },
            steps,
        };
        let placed = ("west".to_string(), "bucket=3".to_string());
        let text = Printer::new(false);
        let steps = vec![Stage::RegisterTenant, Stage::TenantReady];
        assert_eq!(
            report(
                &text,
                "created",
                "acme",
                done(steps.clone()),
                &labels(),
                Some(&placed)
            ),
            Ending::Success
        );
        assert_eq!(
            report(&text, "created", "acme", done(Vec::new()), &labels(), None),
            Ending::Success
        );
        assert_eq!(
            text.printed(),
            [
                "created tenant=acme cell=west bucket=3 steps=RegisterTenant,TenantReady",
                "unchanged tenant=acme",
            ]
        );
        let json = Printer::new(true);
        report(
            &json,
            "created",
            "acme",
            done(steps),
            &labels(),
            Some(&placed),
        );
        let doc: serde_json::Value = serde_json::from_str(&json.printed()[0]).expect("JSON");
        assert_eq!(
            doc,
            serde_json::json!({
                "outcome": "created",
                "tenant": 10,
                "name": "acme",
                "cell": "west",
                "placement": "bucket=3",
                "steps": ["RegisterTenant", "TenantReady"],
            })
        );
        let refused = Run {
            outcome: Step::Refused(FleetRefusal::IdTaken),
            steps: Vec::new(),
        };
        let out = Printer::new(false);
        assert_eq!(
            report(&out, "created", "acme", refused, &labels(), None),
            Ending::Refused
        );
        assert_eq!(out.printed(), ["refused: id_taken"]);
        let stopped = Run {
            outcome: Step::Interrupted(Interrupted::NotWritten {
                journal: journal(),
                outcome: WriterOutcome::Ambiguous,
            }),
            steps: Vec::new(),
        };
        assert_eq!(
            report(&out, "created", "acme", stopped, &labels(), None),
            Ending::Ambiguous
        );
    }
}
