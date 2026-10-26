//! `parosctl store inspect <data-dir>` (#365): a machine's disk, offline.
//!
//! `parosd` exits 78 on a refusal (amnesia, a lost identity, a changed
//! class, an edited `Config`): do not restart it blindly. This command shows
//! what the disk holds, with no `parosd` running: the machine record, and
//! for every journal store its format marker, promise, chosen index, floor,
//! sealed journal state and faulty slots, and for every matchmaker set its
//! marker and scalars. `--records` adds every accepted slot.
//!
//! It is read-only: `paros::inspect` copies each store to a scratch
//! directory (`--scratch`, by default a fresh one under the system's
//! temporary directory, removed after) and opens the copy, because opening
//! a store recovers it, and recovery writes. Names come from the cell's
//! journals this disk holds (`paros::inspect::names`); an id no journal on
//! the disk names prints as short hex.

use std::fmt::Write as _;
use std::path::PathBuf;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use clap::{Args, Subcommand};
use moonpool_core::CompioStorageProvider;
use paros::core::Ballot;
use paros::core::Command;
use paros::core::Config;
use paros::core::Control;
use paros::core::QuorumSystem;
use paros::core::Slot;
use paros::core::WriterMode;
use paros::inspect::{
    DiskImage, JournalImage, SetImage, SetStoreImage, StoreImage, inspect, names,
};
use paros::machine::MachineRecord;
use paros::storage::journal::JournalStoreConfig;
use serde_json::{Value as Json, json};

use crate::Ending;
use crate::labels::Labels;
use crate::output::{Printer, note, record_text};

/// `parosctl store`.
#[derive(Args, Debug)]
pub struct StoreArgs {
    #[command(subcommand)]
    command: StoreCommand,
}

#[derive(Subcommand, Debug)]
enum StoreCommand {
    /// Read a machine's data directory without starting `parosd`: the
    /// record, every journal store and every matchmaker set. Never writes
    /// to it.
    Inspect(InspectArgs),
}

/// `parosctl store inspect`.
#[derive(Args, Debug)]
pub struct InspectArgs {
    /// The machine's data directory (`PAROS_DATA_DIR`).
    data_dir: PathBuf,
    /// Print every accepted slot of each journal store, and its records.
    #[arg(long)]
    records: bool,
    /// Where the copies the inspection opens go: an empty or missing
    /// directory with room for the largest store. By default a fresh
    /// directory under the system's temporary directory, removed after.
    #[arg(long)]
    scratch: Option<PathBuf>,
}

/// `parosctl store`.
pub fn run(out: &Printer, args: StoreArgs) -> Ending {
    match args.command {
        StoreCommand::Inspect(args) => match read(&args) {
            Ok(image) => print(out, &image, args.records),
            Err(error) => {
                note(&error);
                Ending::Refused
            }
        },
    }
}

/// Inspect the data directory on a runtime of its own, the scratch
/// directory removed after when it is ours.
fn read(args: &InspectArgs) -> Result<DiskImage, String> {
    let root = path_text(&args.data_dir)?;
    let (scratch, ours) = match &args.scratch {
        Some(dir) => (dir.clone(), false),
        None => (
            std::env::temp_dir().join(format!(
                "parosctl-store-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| d.as_nanos())
            )),
            true,
        ),
    };
    let scratch_root = path_text(&scratch)?;
    if scratch.starts_with(&args.data_dir) {
        return Err("the scratch directory lies inside the data directory".to_string());
    }
    let runtime = compio::runtime::Runtime::new().map_err(|e| format!("runtime: {e}"))?;
    let provider = CompioStorageProvider::new();
    let image = runtime.block_on(inspect(
        &provider,
        &root,
        // A store keeps the geometry it was created with, whatever this one
        // says (PierreZ/moonpool#335).
        JournalStoreConfig::default(),
        &provider,
        &scratch_root,
    ));
    if ours {
        let _ = std::fs::remove_dir_all(&scratch);
    }
    image
}

fn path_text(path: &std::path::Path) -> Result<String, String> {
    path.to_str()
        .map(|p| p.trim_end_matches('/').to_string())
        .filter(|p| !p.is_empty())
        .ok_or_else(|| format!("{}: not a UTF-8 path", path.display()))
}

/// Print the image: one answer per entity. Exit 3 when the record or a
/// store does not read.
fn print(out: &Printer, image: &DiskImage, records: bool) -> Ending {
    let mut labels = names(image)
        .map(|view| Labels::of(&view))
        .unwrap_or_default();
    if let Some(Ok(record)) = &image.record
        && !record.name.is_empty()
    {
        labels.name_machine(record.node_id.0, &record.name);
    }
    let mut whole = true;
    match &image.record {
        None => out.emit(
            || format!("record: none in {}", image.root),
            || json!({ "record": Json::Null }),
        ),
        Some(Err(error)) => {
            whole = false;
            out.emit(
                || format!("record: unreadable: {error}"),
                || json!({ "record": { "error": error } }),
            );
        }
        Some(Ok(record)) => print_record(out, &labels, record),
    }
    for journal in &image.journals {
        whole &= journal.store.is_ok();
        print_journal(out, &labels, journal, records);
    }
    for set in &image.matchmakers {
        whole &= set.store.is_ok();
        print_set(out, &labels, set);
    }
    if whole {
        Ending::Success
    } else {
        Ending::Refused
    }
}

fn print_record(out: &Printer, labels: &Labels, record: &MachineRecord) {
    let cell = record.cell_id();
    let standing = if record.formed().is_some() {
        "founding member"
    } else if record.admitted.is_some() {
        "admitted"
    } else {
        "idle"
    };
    out.emit(
        || {
            format!(
                "machine {}: class={} capacity={} zone={} rack={} cell={} standing={} \
                 created={}",
                labels.machine(record.node_id.0),
                record.class.as_str(),
                record.capacity,
                or_dash(&record.locality.zone),
                or_dash(&record.locality.rack),
                labels.cell(cell),
                standing,
                record.created.len(),
            )
        },
        || {
            json!({
                "record": {
                    "node_id": record.node_id.0,
                    "name": labels.machine(record.node_id.0),
                    "class": record.class.as_str(),
                    "capacity": record.capacity,
                    "zone": record.locality.zone,
                    "rack": record.locality.rack,
                    "cell_id": cell,
                    "cell": labels.cell(cell),
                    "standing": standing,
                    "promised": ballot_json(record.promised, labels),
                    "created": record.created.iter().map(|j| json!({
                        "tenant": j.tenant.0,
                        "journal": j.journal.0,
                        "name": labels.journal(*j),
                    })).collect::<Vec<_>>(),
                }
            })
        },
    );
}

fn print_journal(out: &Printer, labels: &Labels, image: &JournalImage, records: bool) {
    let name = labels.journal(image.journal);
    let store = match &image.store {
        Ok(store) => store,
        Err(error) => {
            out.emit(
                || format!("journal {name}: unreadable: {error}"),
                || {
                    json!({
                        "journal": journal_json(image, &name),
                        "error": error,
                    })
                },
            );
            return;
        }
    };
    out.emit(
        || {
            let mut text = format!(
                "journal {name}: {} promise={} chosen_index={} floor={} last_slot={} \
                 accepted={} faulty={} next_seq={} first_seq={}",
                marker_text(store.formatted.as_ref(), labels),
                ballot_text(store.promise, labels),
                slot_text(store.chosen),
                store.floor.0,
                store
                    .accepted
                    .last()
                    .map_or_else(|| "none".to_string(), |(s, _, _)| s.0.to_string()),
                store.accepted.len(),
                faulty_text(store, labels),
                store.sealed.next_seq.0,
                store.sealed.first_seq.0,
            );
            if records {
                for (slot, ballot, command) in &store.accepted {
                    text.push('\n');
                    text.push_str(&slot_text_line(*slot, *ballot, command, labels));
                }
            }
            text
        },
        || {
            let mut doc = json!({
                "journal": journal_json(image, &name),
                "formatted": store.formatted.as_ref().map(|c| config_json(c, labels)),
                "promise": ballot_json(store.promise, labels),
                "chosen_index": store.chosen.map(|s| s.0),
                "floor": store.floor.0,
                "sealed": {
                    "next_seq": store.sealed.next_seq.0,
                    "first_seq": store.sealed.first_seq.0,
                    "term": store.sealed.term,
                    "leader": store.sealed.leader.map(|l| l.to_string()),
                },
                "accepted": store.accepted.len(),
                "faulty": store.faulty.iter().map(|(s, b)| json!({
                    "slot": s.0,
                    "ballot": ballot_json(*b, labels),
                })).collect::<Vec<_>>(),
            });
            if records {
                doc["slots"] = store
                    .accepted
                    .iter()
                    .map(|(slot, ballot, command)| slot_json(*slot, *ballot, command, labels))
                    .collect();
            }
            doc
        },
    );
}

fn print_set(out: &Printer, labels: &Labels, image: &SetImage) {
    let tenant = labels.tenant(image.tenant);
    let store: &SetStoreImage = match &image.store {
        Ok(store) => store,
        Err(error) => {
            out.emit(
                || format!("matchmaker set {tenant}: unreadable: {error}"),
                || {
                    json!({
                        "matchmaker_set": { "tenant": image.tenant.0, "name": tenant },
                        "error": error,
                    })
                },
            );
            return;
        }
    };
    let scalars = &store.scalars;
    let members: Vec<u64> = scalars.members.iter().map(|m| m.0).collect();
    out.emit(
        || {
            format!(
                "matchmaker set {tenant}: {} generation={} phase={} members={} journals={} \
                 registrations={}",
                store.formatted.as_ref().map_or_else(
                    || "unformatted".to_string(),
                    |c| format!("formatted as {}", labels.machine(c.id.0))
                ),
                scalars.generation.0,
                format!("{:?}", scalars.phase).to_lowercase(),
                labels.machines(&members),
                scalars.journals.len(),
                store.registered.len(),
            )
        },
        || {
            json!({
                "matchmaker_set": { "tenant": image.tenant.0, "name": tenant },
                "formatted": store.formatted.as_ref().map(|c| json!({
                    "id": c.id.0,
                    "name": labels.machine(c.id.0),
                    "bootstrap": c.bootstrap.iter().map(|m| m.0).collect::<Vec<_>>(),
                })),
                "generation": scalars.generation.0,
                "phase": format!("{:?}", scalars.phase).to_lowercase(),
                "members": members,
                "journals": scalars.journals.iter().map(|(journal, s)| json!({
                    "journal": journal.0,
                    "name": labels.journal(paros::core::JournalIdentifier::new(image.tenant, *journal)),
                    "gc_watermark": ballot_json(s.gc_watermark, labels),
                })).collect::<Vec<_>>(),
                "registrations": store.registered.iter().map(|(journal, ballot)| json!({
                    "journal": journal.0,
                    "ballot": ballot_json(*ballot, labels),
                })).collect::<Vec<_>>(),
            })
        },
    );
}

fn journal_json(image: &JournalImage, name: &str) -> Json {
    json!({
        "tenant": image.journal.tenant.0,
        "journal": image.journal.journal.0,
        "name": name,
    })
}

/// The format marker, in words.
fn marker_text(config: Option<&Config>, labels: &Labels) -> String {
    config.map_or_else(
        || "unformatted".to_string(),
        |c| {
            format!(
                "formatted members={} quorum={} writers={}{}",
                labels.machines(&c.peers.iter().map(|p| p.0).collect::<Vec<_>>()),
                quorum_text(c.quorum_system),
                writers_text(c.writer_mode),
                if c.matchmakers.is_empty() {
                    String::new()
                } else {
                    format!(
                        " matchmakers={}",
                        labels.machines(&c.matchmakers.iter().map(|m| m.0).collect::<Vec<_>>())
                    )
                },
            )
        },
    )
}

fn config_json(config: &Config, labels: &Labels) -> Json {
    json!({
        "node": config.id.0,
        "members": config.peers.iter().map(|p| p.0).collect::<Vec<_>>(),
        "member_names": config.peers.iter().map(|p| labels.machine(p.0)).collect::<Vec<_>>(),
        "pool": config.nodes.iter().map(|p| p.0).collect::<Vec<_>>(),
        "quorum": quorum_text(config.quorum_system),
        "writers": writers_text(config.writer_mode),
        "matchmakers": config.matchmakers.iter().map(|m| m.0).collect::<Vec<_>>(),
        "proxies": config.proxy_count,
        "replicas": config.replica_count,
    })
}

fn quorum_text(quorum: QuorumSystem) -> String {
    match quorum {
        QuorumSystem::Majority => "majority".to_string(),
        QuorumSystem::Flexible { q1, q2 } => format!("flexible:{q1}:{q2}"),
        QuorumSystem::Grid { rows, cols, acks } if acks >= 1 && rows == 2 * acks - 1 => {
            format!("partitions:{}:{cols}", acks - 1)
        }
        QuorumSystem::Grid { rows, cols, acks } => format!("grid:{rows}x{cols}:{acks}"),
    }
}

fn writers_text(mode: WriterMode) -> &'static str {
    match mode {
        WriterMode::Single => "single",
        WriterMode::Multi => "multi",
    }
}

fn ballot_text(ballot: Ballot, labels: &Labels) -> String {
    if ballot == Ballot::default() {
        return "none".to_string();
    }
    format!("{}.{}", ballot.round, labels.machine(ballot.node.0))
}

fn ballot_json(ballot: Ballot, labels: &Labels) -> Json {
    json!({
        "round": ballot.round,
        "node": ballot.node.0,
        "name": labels.machine(ballot.node.0),
    })
}

fn slot_text(slot: Option<Slot>) -> String {
    slot.map_or_else(|| "none".to_string(), |s| s.0.to_string())
}

fn faulty_text(store: &StoreImage, labels: &Labels) -> String {
    if store.faulty.is_empty() {
        return "0".to_string();
    }
    let slots: Vec<String> = store
        .faulty
        .iter()
        .map(|(s, b)| format!("{}@{}", s.0, ballot_text(*b, labels)))
        .collect();
    format!("[{}]", slots.join(","))
}

/// One slot, as one line.
fn slot_text_line(slot: Slot, ballot: Ballot, command: &Command, labels: &Labels) -> String {
    let head = format!("  slot {} ballot={}", slot.0, ballot_text(ballot, labels));
    match command {
        Command::Write(entry) => {
            let mut text = format!(
                "{head} write leader={} seq={} records={}",
                entry.leader,
                entry.seq.0,
                entry.records.len()
            );
            for (at, record) in (entry.seq.0..).zip(&entry.records) {
                let _ = write!(text, "\n    {at}: {}", preview(&record.0));
            }
            text
        }
        Command::Control(Control::SetLeader { new, old }) => format!(
            "{head} set-leader new={new} old={}",
            old.map_or_else(|| "none".to_string(), |o| o.to_string())
        ),
        Command::Control(Control::Truncate { leader, up_to }) => {
            format!("{head} truncate leader={leader} up_to={}", up_to.0)
        }
        Command::Control(Control::Noop) => format!("{head} noop"),
    }
}

fn slot_json(slot: Slot, ballot: Ballot, command: &Command, labels: &Labels) -> Json {
    let command = match command {
        Command::Write(entry) => json!({
            "write": {
                "leader": entry.leader.to_string(),
                "seq": entry.seq.0,
                "records": entry.records.iter().map(|r| STANDARD.encode(&r.0)).collect::<Vec<_>>(),
            }
        }),
        Command::Control(Control::SetLeader { new, old }) => json!({
            "set_leader": { "new": new.to_string(), "old": old.map(|o| o.to_string()) }
        }),
        Command::Control(Control::Truncate { leader, up_to }) => json!({
            "truncate": { "leader": leader.to_string(), "up_to": up_to.0 }
        }),
        Command::Control(Control::Noop) => json!("noop"),
    };
    json!({ "slot": slot.0, "ballot": ballot_json(ballot, labels), "command": command })
}

/// A record's bytes for a line: its text when it is printable, else its
/// length; at most 64 characters.
fn preview(bytes: &[u8]) -> String {
    let text = record_text(bytes);
    if text.chars().all(|c| !c.is_control() || c == '\n') {
        let short: String = text.chars().take(64).collect();
        let more = if short.len() < text.len() { "…" } else { "" };
        format!("{short:?}{more}")
    } else {
        format!("({} bytes)", bytes.len())
    }
}

/// `text`, or `-` when it is empty.
fn or_dash(text: &str) -> &str {
    if text.is_empty() { "-" } else { text }
}
