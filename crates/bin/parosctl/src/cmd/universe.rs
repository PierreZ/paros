//! `parosctl universe key list|add|remove` (#245 (Biscuit tokens)): the
//! universe entry's root public keys, which every entry role checks a token
//! against. `parosctl universe add-cell|remove-cell` (#232 (multiple
//! cells)): an idle cell (`parosctl init --idle`) joins the universe, and an
//! empty cell leaves it for good. `parosctl universe status` and
//! `parosctl universe buckets assign` (#713 (bucket placement)): where the
//! universe places a new tenant, and moving a range of buckets to a cell.
//!
//! A rotation is two commands: `key add` the next key, then, once the
//! tokens signed with the old one expired, `key remove` it. Each is one
//! write to the universe directory through `paros::client::fleet`, under a
//! leader uuid of its own; a re-run changes nothing. A key id and a label
//! are never reused, and the last key stays. Keys are named by their
//! labels, never by their ids (decided on 2026-10-10).

use std::path::Path;
use std::time::Duration;

use clap::{Args, Subcommand};
use moonpool_core::CompioProviders;
use paros::client::Client;
use paros::client::bootstrap::{control_journals, majority_cell};
use paros::client::fleet::{CellJoin, Step, read_directory};
use paros::model::address::Address;
use paros::model::fleet::buckets::{BucketRange, bucket_of};
use paros::model::fleet::{FleetDirectory, RootKeyEntry, TenantKind};
use paros_authz_biscuit::RootPublicKey;
use serde_json::json;

use crate::cmd::tenant::{interrupted, open_session, refused, steps};
use crate::support::connect::Ending;
use crate::support::labels::Labels;
use crate::support::output::{Printer, note, table};
use crate::support::view::Asker;

/// `parosctl universe`.
#[derive(Args, Debug)]
pub struct UniverseArgs {
    /// How long a step interrupted by a moving leader is retried, in
    /// milliseconds.
    #[arg(long, default_value = "30000")]
    patience_ms: u64,
    #[command(subcommand)]
    command: UniverseCommand,
}

#[derive(Subcommand, Debug)]
enum UniverseCommand {
    /// The universe entry's root public keys.
    Key {
        #[command(subcommand)]
        command: KeyCommand,
    },
    /// Join an idle cell (formed with `parosctl init --idle`) to the
    /// universe the servers' cell hosts (#232 (multiple cells)). A re-run
    /// resumes.
    AddCell {
        /// The idle cell's founding members, comma-separated `HOST:PORT`s,
        /// each by the address it advertises.
        #[arg(long, value_delimiter = ',', required = true)]
        members: Vec<String>,
        /// The cell's name, for people: unique among the universe's cells.
        #[arg(long)]
        name: String,
    },
    /// Remove a cell that hosts no tenant from the universe, for good: its
    /// id is never added again (#232 (multiple cells)). A re-run resumes.
    RemoveCell {
        /// The cell's name.
        name: String,
    },
    /// The universe's cells: their state, the buckets each holds, and how
    /// many tenants each hosts, by bucket and by override.
    Status,
    /// The bucket table: a tenant created without `--cell` goes to the
    /// cell its bucket names, `CRC16(name) mod 16384`.
    Buckets {
        #[command(subcommand)]
        command: BucketsCommand,
    },
}

#[derive(Subcommand, Debug)]
enum BucketsCommand {
    /// A tenant name's bucket, the cell that bucket names, and where the
    /// tenant lives when it exists.
    Show {
        /// A tenant name: show its bucket and the cell it names.
        name: String,
    },
    /// Give a range of buckets to a `READY` cell. Refused while a tenant
    /// placed by one of them lives in another cell: tenants do not move
    /// between cells yet. A re-run changes nothing.
    Assign {
        /// The buckets, `FIRST-LAST` (inclusive) or one `N`, below 16384.
        range: paros::model::fleet::buckets::BucketRange,
        /// The cell's name.
        cell: String,
    },
}

#[derive(Subcommand, Debug)]
enum KeyCommand {
    /// List the root public keys in force.
    List,
    /// Add a root public key: a `.public` file (or the public half of a
    /// `.private` one) from `parosctl key generate`.
    Add {
        /// The key file.
        file: std::path::PathBuf,
    },
    /// Remove the root key with this label. The last key stays.
    Remove {
        /// The key's label.
        label: String,
    },
}

/// The universe entry's record of the key in the file at `path`.
///
/// # Errors
///
/// The file cannot be read, or holds no Ed25519 key.
pub fn read_key(path: &Path) -> Result<RootKeyEntry, String> {
    let text = crate::cmd::key::read(path)?;
    key_entry(&text).map_err(|e| format!("{}: {e}", path.display()))
}

/// The universe entry of a `.public` (or `.private`) file's text.
///
/// # Errors
///
/// The text holds no Ed25519 key.
pub fn key_entry(text: &str) -> Result<RootKeyEntry, String> {
    let key = RootPublicKey::from_file(text).map_err(|e| e.to_string())?;
    Ok(RootKeyEntry {
        key_id: key.key_id(),
        label: key.label().to_string(),
        public_key: key.key_text(),
    })
}

/// `parosctl universe …`.
pub async fn run(
    asker: &Asker<'_>,
    client: &Client<CompioProviders>,
    servers: &[u64],
    out: &Printer,
    args: UniverseArgs,
) -> Ending {
    let providers = asker.providers;
    let patience = Duration::from_millis(args.patience_ms);
    let command = match args.command {
        UniverseCommand::Key { command } => command,
        UniverseCommand::AddCell { members, name } => {
            return add_cell(asker, client, servers, out, &members, &name, patience).await;
        }
        UniverseCommand::RemoveCell { name } => {
            return remove_cell(asker, client, servers, out, &name, patience).await;
        }
        UniverseCommand::Status => return status(client, out).await,
        UniverseCommand::Buckets {
            command: BucketsCommand::Show { name },
        } => return bucket_show(client, out, &name).await,
        UniverseCommand::Buckets {
            command: BucketsCommand::Assign { range, cell },
        } => return assign(asker, client, servers, out, (range, &cell), patience).await,
    };
    match command {
        KeyCommand::List => list(client, out).await,
        KeyCommand::Add { file } => {
            let key = match read_key(&file) {
                Ok(key) => key,
                Err(error) => {
                    note(&error);
                    return Ending::Refused;
                }
            };
            let mut fleet = match open_session(providers, client, servers).await {
                Ok(fleet) => fleet,
                Err(ending) => return ending,
            };
            let run = fleet.add_root_key(client, 0, &key, patience).await;
            match run.outcome {
                Step::Done { .. } => {
                    let outcome = if run.steps.is_empty() {
                        "unchanged"
                    } else {
                        "added"
                    };
                    out.emit(
                        || format!("{outcome} key={}", key.label),
                        || {
                            json!({
                                "outcome": outcome,
                                "label": key.label,
                                "key_id": key.key_id,
                                "steps": steps(&run.steps),
                            })
                        },
                    );
                    Ending::Success
                }
                Step::Refused(refusal) => refused(out, &refusal, &Labels::default()),
                Step::Interrupted(stop) => interrupted(&stop),
                Step::Advanced(_) => unreachable!("a run never ends advanced"),
            }
        }
        KeyCommand::Remove { label } => {
            let mut fleet = match open_session(providers, client, servers).await {
                Ok(fleet) => fleet,
                Err(ending) => return ending,
            };
            let run = fleet.remove_root_key(client, 0, &label, patience).await;
            match run.outcome {
                Step::Done { result: None, .. } => {
                    out.emit(
                        || format!("no root key in force is labelled {label}"),
                        || json!({ "outcome": "not_found", "label": label }),
                    );
                    Ending::Refused
                }
                Step::Done { .. } => {
                    out.emit(
                        || format!("removed key={label}"),
                        || {
                            json!({
                                "outcome": "removed",
                                "label": label,
                                "steps": steps(&run.steps),
                            })
                        },
                    );
                    Ending::Success
                }
                Step::Refused(refusal) => refused(out, &refusal, &Labels::default()),
                Step::Interrupted(stop) => interrupted(&stop),
                Step::Advanced(_) => unreachable!("a run never ends advanced"),
            }
        }
    }
}

/// `parosctl universe add-cell` (#232 (multiple cells)): learn the idle
/// cell from a majority of `members`, then run the join through
/// `paros::client::fleet`.
async fn add_cell(
    asker: &Asker<'_>,
    client: &Client<CompioProviders>,
    servers: &[u64],
    out: &Printer,
    members: &[String],
    name: &str,
    patience: Duration,
) -> Ending {
    if let Err(error) = paros::model::name::check_label(name) {
        note(&format!("bad cell name {name:?}: {error}"));
        return Ending::Refused;
    }
    let mut addrs = Vec::new();
    for member in members.iter().map(|m| m.trim()).filter(|m| !m.is_empty()) {
        match Address::parse(member) {
            Ok(addr) => addrs.push(addr),
            Err(error) => {
                note(&format!("bad member {member:?}: {error}"));
                return Ending::Refused;
            }
        }
    }
    let Some((journals, cell_servers)) = majority_cell(
        asker.providers,
        asker.rpc,
        asker.names,
        &addrs,
        asker.timeout,
    )
    .await
    else {
        note(
            "no cell answered inspect from a majority of the members: is it formed (init --idle)?",
        );
        return Ending::Unreachable;
    };
    let joining = CellJoin {
        journals,
        endpoint: cell_servers
            .iter()
            .map(|server| (server.id, server.address.clone()))
            .collect(),
        name: name.as_bytes().to_vec(),
    };
    let mut fleet = match open_session(asker.providers, client, servers).await {
        Ok(fleet) => fleet,
        Err(ending) => return ending,
    };
    // A cell that joined already is repointed when the universe cell's
    // machines moved (#349): the pointer names them where they are now.
    if let Err(stop) = fleet.read_universe_cell(client, 0).await {
        return interrupted(&stop);
    }
    let run = fleet
        .add_cell(
            asker.providers,
            asker.rpc,
            asker.names,
            client,
            0,
            &joining,
            patience,
        )
        .await;
    match run.outcome {
        Step::Done { result, .. } => {
            let outcome = if run.steps.is_empty() {
                "unchanged"
            } else {
                "added"
            };
            out.emit(
                || format!("{outcome} cell={name}"),
                || {
                    json!({
                        "outcome": outcome,
                        "cell": result,
                        "name": name,
                        "steps": steps(&run.steps),
                    })
                },
            );
            Ending::Success
        }
        Step::Refused(refusal) => refused(out, &refusal, &Labels::default()),
        Step::Interrupted(stop) => interrupted(&stop),
        Step::Advanced(_) => unreachable!("a run never ends advanced"),
    }
}

/// `parosctl universe remove-cell` (#232 (multiple cells)): the cell
/// named `name` leaves the universe, its id tombstoned.
async fn remove_cell(
    asker: &Asker<'_>,
    client: &Client<CompioProviders>,
    servers: &[u64],
    out: &Printer,
    name: &str,
    patience: Duration,
) -> Ending {
    let mut fleet = match open_session(asker.providers, client, servers).await {
        Ok(fleet) => fleet,
        Err(ending) => return ending,
    };
    let directory = match read_directory(client, 0, fleet.directory_journal()).await {
        Ok(directory) => directory,
        Err(outcome) => {
            note(&format!("the universe directory was not read: {outcome:?}"));
            return Ending::Unreachable;
        }
    };
    let Some(cell_id) = directory.cell_named(name.as_bytes()) else {
        out.emit(
            || format!("no cell named {name}"),
            || json!({ "outcome": "not_found", "name": name }),
        );
        return Ending::Refused;
    };
    let labels = Labels::from_directory(&directory);
    let run = fleet.remove_cell(client, 0, cell_id, patience).await;
    match run.outcome {
        Step::Done { .. } => {
            out.emit(
                || format!("removed cell={name}"),
                || {
                    json!({
                        "outcome": "removed",
                        "cell": cell_id,
                        "name": name,
                        "steps": steps(&run.steps),
                    })
                },
            );
            Ending::Success
        }
        Step::Refused(refusal) => refused(out, &refusal, &labels),
        Step::Interrupted(stop) => interrupted(&stop),
        Step::Advanced(_) => unreachable!("a run never ends advanced"),
    }
}

/// The universe directory, read as a reader that owns nothing.
async fn universe(client: &Client<CompioProviders>) -> Result<FleetDirectory, Ending> {
    let Some(journals) = control_journals(client).await else {
        note("no server named its cell's control journals: is the cell initialized?");
        return Err(Ending::Unreachable);
    };
    let Some(universe) = journals.fleet else {
        note("the cell names no universe journal");
        return Err(Ending::Refused);
    };
    read_directory(client, 0, universe)
        .await
        .map_err(|outcome| {
            note(&format!("the universe directory was not read: {outcome:?}"));
            Ending::Unreachable
        })
}

/// A cell's name, or `-` when it has none.
fn cell_label(directory: &FleetDirectory, cell_id: u64) -> String {
    match directory.cell_name(cell_id) {
        [] => "-".to_string(),
        name => String::from_utf8_lossy(name).into_owned(),
    }
}

/// `parosctl universe status` (#713 (bucket placement)): each cell's
/// state, its bucket ranges, and its tenants by bucket and by override.
async fn status(client: &Client<CompioProviders>, out: &Printer) -> Ending {
    let directory = match universe(client).await {
        Ok(directory) => directory,
        Err(ending) => return ending,
    };
    let runs = directory.buckets().runs();
    let cells: Vec<CellStatus> = directory
        .cells()
        .map(|(cell_id, cell)| {
            let ranges: Vec<String> = runs
                .iter()
                .filter(|(_, owner)| *owner == cell_id)
                .map(|(range, _)| range.to_string())
                .collect();
            let users = directory
                .tenants()
                .filter(|(_, t)| t.cell_id == cell_id && t.kind == TenantKind::Users);
            let (by_bucket, by_override) = users.fold((0, 0), |(b, o), (_, t)| {
                if t.placement.is_bucket() {
                    (b + 1, o)
                } else {
                    (b, o + 1)
                }
            });
            CellStatus {
                name: cell_label(&directory, cell_id),
                state: cell.state.as_str(),
                kind: cell.kind.as_str(),
                buckets: directory.buckets().count(cell_id),
                ranges,
                by_bucket,
                by_override,
                hosts_universe: directory.universe_cell() == Some(cell_id),
            }
        })
        .collect();
    let mut sorted = cells;
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    let name = String::from_utf8_lossy(directory.universe_name()).into_owned();
    out.emit(
        || {
            let rows: Vec<[String; 6]> = sorted
                .iter()
                .map(|c| {
                    [
                        if c.hosts_universe {
                            format!("{} (universe)", c.name)
                        } else {
                            c.name.clone()
                        },
                        c.state.to_string(),
                        c.kind.to_string(),
                        if c.ranges.is_empty() {
                            "-".to_string()
                        } else {
                            format!("{} ({})", c.ranges.join(","), c.buckets)
                        },
                        c.by_bucket.to_string(),
                        c.by_override.to_string(),
                    ]
                })
                .collect();
            format!(
                "universe={}\n{}",
                if name.is_empty() { "-" } else { &name },
                table(
                    [
                        "CELL",
                        "STATE",
                        "KIND",
                        "BUCKETS",
                        "BY BUCKET",
                        "BY OVERRIDE"
                    ],
                    &rows
                )
            )
        },
        || {
            json!({
                "universe": name,
                "cells": sorted.iter().map(|c| json!({
                    "name": c.name,
                    "state": c.state,
                    "kind": c.kind,
                    "hosts_universe": c.hosts_universe,
                    "buckets": c.buckets,
                    "ranges": c.ranges,
                    "tenants_by_bucket": c.by_bucket,
                    "tenants_by_override": c.by_override,
                })).collect::<Vec<_>>(),
            })
        },
    );
    Ending::Success
}

/// One row of `universe status`.
struct CellStatus {
    name: String,
    state: &'static str,
    kind: &'static str,
    buckets: usize,
    ranges: Vec<String>,
    by_bucket: usize,
    by_override: usize,
    hosts_universe: bool,
}

/// `parosctl universe buckets show NAME`: the bucket of a tenant name, the
/// cell it names, and where the tenant lives when it exists.
async fn bucket_show(client: &Client<CompioProviders>, out: &Printer, name: &str) -> Ending {
    let directory = match universe(client).await {
        Ok(directory) => directory,
        Err(ending) => return ending,
    };
    let bucket = bucket_of(name.as_bytes());
    let cell = directory
        .bucket_cell(bucket)
        .map_or_else(|| "-".to_string(), |id| cell_label(&directory, id));
    let tenant = directory.named(name.as_bytes()).map(|(_, entry)| {
        (
            cell_label(&directory, entry.cell_id),
            entry.placement.label(),
        )
    });
    out.emit(
        || {
            let lives = tenant.as_ref().map_or_else(
                || "tenant=- (not created)".to_string(),
                |(cell, how)| format!("tenant={name} lives in cell={cell}, placed by {how}"),
            );
            format!("name={name} bucket={bucket} bucket_cell={cell}\n{lives}")
        },
        || {
            json!({
                "name": name,
                "bucket": bucket,
                "bucket_cell": cell,
                "tenant": tenant.as_ref().map(|(cell, how)| json!({
                    "cell": cell,
                    "placement": how,
                })),
            })
        },
    );
    Ending::Success
}

/// `parosctl universe buckets assign RANGE CELL` (#713): the buckets of
/// `range` name the cell named `cell`.
async fn assign(
    asker: &Asker<'_>,
    client: &Client<CompioProviders>,
    servers: &[u64],
    out: &Printer,
    (range, cell): (BucketRange, &str),
    patience: Duration,
) -> Ending {
    let mut fleet = match open_session(asker.providers, client, servers).await {
        Ok(fleet) => fleet,
        Err(ending) => return ending,
    };
    let directory = match read_directory(client, 0, fleet.directory_journal()).await {
        Ok(directory) => directory,
        Err(outcome) => {
            note(&format!("the universe directory was not read: {outcome:?}"));
            return Ending::Unreachable;
        }
    };
    let Some(cell_id) = directory.cell_named(cell.as_bytes()) else {
        out.emit(
            || format!("no cell named {cell}"),
            || json!({ "outcome": "not_found", "name": cell }),
        );
        return Ending::Refused;
    };
    let labels = Labels::from_directory(&directory);
    let run = fleet
        .assign_buckets(client, 0, range, cell_id, patience)
        .await;
    match run.outcome {
        Step::Done { .. } => {
            let outcome = if run.steps.is_empty() {
                "unchanged"
            } else {
                "assigned"
            };
            out.emit(
                || format!("{outcome} buckets={range} ({}) cell={cell}", range.len()),
                || {
                    json!({
                        "outcome": outcome,
                        "buckets": range.to_string(),
                        "count": range.len(),
                        "cell": cell,
                        "steps": steps(&run.steps),
                    })
                },
            );
            Ending::Success
        }
        Step::Refused(refusal) => refused(out, &refusal, &labels),
        Step::Interrupted(stop) => interrupted(&stop),
        Step::Advanced(_) => unreachable!("a run never ends advanced"),
    }
}

/// `parosctl universe key list`: the keys in force, read as a reader that
/// owns nothing.
async fn list(client: &Client<CompioProviders>, out: &Printer) -> Ending {
    let directory = match universe(client).await {
        Ok(directory) => directory,
        Err(ending) => return ending,
    };
    let keys: Vec<&RootKeyEntry> = directory.root_keys().collect();
    out.emit(
        || {
            if keys.is_empty() {
                return "no root key: the universe trusts the network (init --root-public-key)"
                    .to_string();
            }
            keys.iter()
                .map(|key| format!("key={} {}", key.label, key.public_key))
                .collect::<Vec<_>>()
                .join("\n")
        },
        || {
            json!({
                "keys": keys
                    .iter()
                    .map(|key| json!({
                        "label": key.label,
                        "key_id": key.key_id,
                        "public_key": key.public_key,
                    }))
                    .collect::<Vec<_>>(),
            })
        },
    );
    Ending::Success
}
