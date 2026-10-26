//! `parosctl universe key list|add|remove` (#245 (Biscuit tokens)): the
//! universe entry's root public keys, which every entry role checks a token
//! against. `parosctl universe add-cell|remove-cell` (#232 (multiple
//! cells)): an idle cell (`parosctl init --idle`) joins the universe, and an
//! empty cell leaves it for good.
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
use paros::model::fleet::RootKeyEntry;
use paros_authz_biscuit::RootPublicKey;
use serde_json::json;

use crate::Ending;
use crate::fleet::{interrupted, open_session, refused, steps};
use crate::labels::Labels;
use crate::output::{Printer, note};
use crate::views::Asker;

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
    let text = crate::key::read(path)?;
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
        Step::Refused(refusal) => refused(out, &refusal, &Labels::default()),
        Step::Interrupted(stop) => interrupted(&stop),
        Step::Advanced(_) => unreachable!("a run never ends advanced"),
    }
}

/// `parosctl universe key list`: the keys in force, read as a reader that
/// owns nothing.
async fn list(client: &Client<CompioProviders>, out: &Printer) -> Ending {
    let Some(journals) = control_journals(client).await else {
        note("no server named its cell's control journals: is the cell initialized?");
        return Ending::Unreachable;
    };
    let Some(universe) = journals.fleet else {
        note("the cell names no universe journal");
        return Ending::Refused;
    };
    let directory = match read_directory(client, 0, universe).await {
        Ok(directory) => directory,
        Err(outcome) => {
            note(&format!("the universe directory was not read: {outcome:?}"));
            return Ending::Unreachable;
        }
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
