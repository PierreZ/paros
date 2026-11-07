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
use paros::core::journal::{Command, Control};
use paros::core::{Ballot, Config, Slot};
use paros::inspect::{
    DiskImage, JournalImage, SetImage, SetStoreImage, StoreImage, inspect, names,
};
use paros::machine::MachineRecord;
use paros::storage::journal::JournalStoreConfig;
use serde_json::{Value as Json, json};

use crate::support::connect::Ending;
use crate::support::labels::Labels;
use crate::support::output::{Printer, note, quorum_text, record_text, writer_mode_text};

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
        "initial member"
    } else if record.admitted.is_some() {
        "admitted"
    } else {
        "idle"
    };
    out.emit(
        || {
            format!(
                "machine {}: class={} zone={} rack={} cell={} standing={} \
                 created={}",
                labels.machine(record.node_id.0),
                record.class.as_str(),
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
                    "roles": record.class.as_str(),
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
                    "name": labels.journal(paros::core::journal::JournalIdentifier::new(image.tenant, *journal)),
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
                writer_mode_text(c.writer_mode),
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
        "writers": writer_mode_text(config.writer_mode),
        "matchmakers": config.matchmakers.iter().map(|m| m.0).collect::<Vec<_>>(),
    })
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

#[cfg(test)]
mod tests {
    use paros::core::journal::{
        Command, Control, Entry, JournalId, JournalIdentifier, JournalState, LeaderUuid, Seq,
        TenantId, WriterMode,
    };
    use paros::core::matchmaker::{MatchmakerConfig, MatchmakerHardState};
    use paros::core::membership::{MatchmakerId, QuorumSystem};
    use paros::core::{Ballot, Config, NodeId, Slot, Value};
    use paros::inspect::{DiskImage, JournalImage, SetImage, SetStoreImage, StoreImage};
    use paros::machine::{Class, ControlJournals, Locality, MachineRecord};
    use paros::model::machine::Admission;
    use serde_json::{Value as Json, json};

    use super::{StoreArgs, preview, print, run};
    use crate::support::connect::Ending;
    use crate::support::output::{Printer, short};

    fn journal() -> JournalIdentifier {
        JournalIdentifier::new(TenantId(10), JournalId(11))
    }

    fn ballot(round: u64, node: u64) -> Ballot {
        Ballot {
            round,
            node: NodeId(node),
        }
    }

    fn record(admitted: bool) -> MachineRecord {
        MachineRecord {
            node_id: NodeId(1),
            class: Class::All,
            locality: Locality {
                zone: "z1".into(),
                rack: String::new(),
            },
            name: "alpha".into(),
            promised: ballot(2, 1),
            plan: None,
            admitted: admitted.then(|| Admission {
                cell: ControlJournals {
                    cell_id: 0xce11,
                    cell: JournalIdentifier::new(TenantId(98), JournalId(99)),
                    fleet: None,
                    election: None,
                },
                members: Vec::new(),
                initial_members: Vec::new(),
            }),
            created: vec![journal()],
        }
    }

    fn store() -> StoreImage {
        let mut config = Config::new(NodeId(1), journal());
        config.peers = vec![NodeId(1), NodeId(2)];
        config.nodes = config.peers.clone();
        config.quorum_system = QuorumSystem::partitions(1, 2);
        config.writer_mode = WriterMode::Single;
        config.matchmakers = vec![MatchmakerId(1)];
        StoreImage {
            formatted: Some(config),
            promise: ballot(4, 1),
            chosen: Some(Slot(2)),
            floor: Slot(1),
            sealed: JournalState {
                leader: Some(LeaderUuid(0xab)),
                term: 3,
                next_seq: Seq(9),
                first_seq: Seq(1),
            },
            faulty: vec![(Slot(5), ballot(3, 2))],
            accepted: vec![
                (
                    Slot(1),
                    ballot(4, 1),
                    Command::Write(Entry {
                        leader: LeaderUuid(0xab),
                        seq: Seq(7),
                        records: vec![Value(b"hello".to_vec()), Value(vec![0, 1, 2])],
                    }),
                ),
                (
                    Slot(2),
                    ballot(4, 1),
                    Command::Control(Control::SetLeader {
                        new: LeaderUuid(0xab),
                        old: None,
                    }),
                ),
                (
                    Slot(3),
                    ballot(4, 2),
                    Command::Control(Control::Truncate {
                        leader: LeaderUuid(0xab),
                        up_to: Seq(4),
                    }),
                ),
                (Slot(4), Ballot::default(), Command::Control(Control::Noop)),
            ],
        }
    }

    fn set() -> SetStoreImage {
        let mut scalars = MatchmakerHardState::default();
        scalars.members = vec![MatchmakerId(1), MatchmakerId(2)];
        SetStoreImage {
            formatted: Some(MatchmakerConfig {
                id: MatchmakerId(1),
                bootstrap: vec![MatchmakerId(1)],
            }),
            scalars,
            registered: vec![(JournalId(11), ballot(4, 1))],
        }
    }

    fn image() -> DiskImage {
        DiskImage {
            root: "/data".into(),
            record: Some(Ok(record(true))),
            journals: vec![JournalImage {
                journal: journal(),
                store: Ok(store()),
            }],
            matchmakers: vec![SetImage {
                tenant: TenantId(10),
                store: Ok(set()),
            }],
        }
    }

    #[test]
    fn a_whole_disk_prints_its_record_its_journals_and_its_sets() {
        let out = Printer::new(false);
        assert_eq!(print(&out, &image(), true), Ending::Success);
        let printed = out.printed();
        let id = format!("id:{}/{}", short(10), short(11));
        assert_eq!(
            printed[0],
            format!(
                "machine alpha: class=all zone=z1 rack=- cell={} standing=admitted created=1",
                short(0xce11)
            )
        );
        let leader = LeaderUuid(0xab).to_string();
        assert_eq!(
            printed[1],
            format!(
                "journal {id}: formatted members=[{two},alpha] quorum=partitions:1:2 \
                 writers=single matchmakers=[alpha] promise=4.alpha chosen_index=2 floor=1 \
                 last_slot=4 accepted=4 faulty=[5@3.{two}] next_seq=9 first_seq=1\n  \
                 slot 1 ballot=4.alpha write leader={leader} seq=7 records=2\n    \
                 7: \"hello\"\n    8: (3 bytes)\n  \
                 slot 2 ballot=4.alpha set-leader new={leader} old=none\n  \
                 slot 3 ballot=4.{two} truncate leader={leader} up_to=4\n  \
                 slot 4 ballot=none noop",
                two = short(2)
            )
        );
        assert_eq!(
            printed[2],
            format!(
                "matchmaker set {}: formatted as alpha generation=0 phase=fresh \
                 members=[{},alpha] journals=0 registrations=1",
                short(10),
                short(2)
            )
        );
        let without = Printer::new(false);
        print(&without, &image(), false);
        assert!(!without.printed()[1].contains('\n'), "no slot lines");
    }

    #[test]
    fn a_whole_disk_prints_each_part_as_json() {
        let out = Printer::new(true);
        assert_eq!(print(&out, &image(), true), Ending::Success);
        let docs: Vec<Json> = out
            .printed()
            .iter()
            .map(|line| serde_json::from_str(line).expect("JSON"))
            .collect();
        let record = &docs[0]["record"];
        assert_eq!(record["name"], "alpha");
        assert_eq!(record["standing"], "admitted");
        assert_eq!(record["cell_id"], 0xce11);
        assert_eq!(
            record["promised"],
            json!({ "round": 2, "node": 1, "name": "alpha" })
        );
        assert_eq!(record["created"][0]["journal"], 11);
        let journal = &docs[1];
        assert_eq!(journal["journal"]["tenant"], 10);
        assert_eq!(journal["formatted"]["quorum"], "partitions:1:2");
        assert_eq!(journal["formatted"]["writers"], "single");
        assert_eq!(journal["formatted"]["members"], json!([1, 2]));
        assert_eq!(journal["chosen_index"], 2);
        assert_eq!(journal["sealed"]["term"], 3);
        assert_eq!(journal["sealed"]["leader"], LeaderUuid(0xab).to_string());
        assert_eq!(journal["faulty"][0]["slot"], 5);
        let slots = journal["slots"].as_array().expect("slots");
        assert_eq!(
            slots[0]["command"]["write"]["records"],
            json!(["aGVsbG8=", "AAEC"])
        );
        assert_eq!(slots[1]["command"]["set_leader"]["old"], Json::Null);
        assert_eq!(slots[2]["command"]["truncate"]["up_to"], 4);
        assert_eq!(slots[3]["command"], "noop");
        assert_eq!(slots[3]["ballot"]["round"], 0);
        let set = &docs[2];
        assert_eq!(set["formatted"]["bootstrap"], json!([1]));
        assert_eq!(set["registrations"][0]["journal"], 11);
        let plain = Printer::new(true);
        print(&plain, &image(), false);
        let journal: Json = serde_json::from_str(&plain.printed()[1]).expect("JSON");
        assert!(journal.get("slots").is_none());
    }

    #[test]
    fn an_unreadable_part_makes_the_disk_refused() {
        let mut broken = image();
        broken.journals[0].store = Err("eio".into());
        let out = Printer::new(false);
        assert_eq!(print(&out, &broken, false), Ending::Refused);
        assert_eq!(
            out.printed()[1],
            format!("journal id:{}/{}: unreadable: eio", short(10), short(11))
        );
        let mut set = image();
        set.matchmakers[0].store = Err("torn".into());
        let out = Printer::new(true);
        assert_eq!(print(&out, &set, false), Ending::Refused);
        let doc: Json = serde_json::from_str(&out.printed()[2]).expect("JSON");
        assert_eq!(doc["error"], "torn");
        let mut record = image();
        record.record = Some(Err("bad".into()));
        let out = Printer::new(false);
        assert_eq!(print(&out, &record, false), Ending::Refused);
        assert_eq!(out.printed()[0], "record: unreadable: bad");
    }

    #[test]
    fn an_empty_disk_holds_no_record_and_reads_whole() {
        let mut empty = image();
        empty.record = None;
        empty.journals.clear();
        empty.matchmakers.clear();
        let out = Printer::new(false);
        assert_eq!(print(&out, &empty, false), Ending::Success);
        assert_eq!(out.printed(), ["record: none in /data"]);
        let mut idle = image();
        idle.record = Some(Ok(record(false)));
        let mut unformatted = store();
        unformatted.formatted = None;
        unformatted.faulty.clear();
        unformatted.chosen = None;
        unformatted.accepted.clear();
        unformatted.promise = Ballot::default();
        idle.journals[0].store = Ok(unformatted);
        let mut fresh = set();
        fresh.formatted = None;
        idle.matchmakers[0].store = Ok(fresh);
        let out = Printer::new(false);
        print(&out, &idle, false);
        let printed = out.printed();
        assert!(
            printed[0].contains(" cell=none standing=idle "),
            "{}",
            printed[0]
        );
        assert!(
            printed[1].ends_with(
                ": unformatted promise=none chosen_index=none floor=1 last_slot=none \
                 accepted=0 faulty=0 next_seq=9 first_seq=1"
            ),
            "{}",
            printed[1]
        );
        assert!(
            printed[2].contains(": unformatted generation="),
            "{}",
            printed[2]
        );
    }

    #[test]
    fn a_record_preview_is_quoted_text_cut_at_64_or_a_byte_count() {
        assert_eq!(preview(b"hi\nthere"), "\"hi\\nthere\"");
        let long = "a".repeat(70);
        assert_eq!(preview(long.as_bytes()), format!("{:?}…", "a".repeat(64)));
        assert_eq!(
            preview("a".repeat(64).as_bytes()),
            format!("{:?}", "a".repeat(64))
        );
        assert_eq!(preview(&[0x1b, b'x']), "(2 bytes)");
    }

    #[test]
    fn inspect_reads_an_empty_data_directory_and_refuses_a_scratch_inside_it() {
        use clap::Parser as _;
        #[derive(clap::Parser)]
        struct Cli {
            #[command(flatten)]
            store: StoreArgs,
        }
        let store = |out: &Printer, line: &[&str]| {
            let mut words = vec!["store"];
            words.extend_from_slice(line);
            run(out, Cli::try_parse_from(words).expect("valid").store)
        };
        let data = tempfile::tempdir().expect("tempdir");
        let at = data.path().to_str().expect("UTF-8");
        let out = Printer::new(false);
        assert_eq!(store(&out, &["inspect", at]), Ending::Success);
        assert_eq!(out.printed(), [format!("record: none in {at}")]);
        let scratch = tempfile::tempdir().expect("tempdir");
        let kept = scratch.path().join("copy");
        let at_slash = format!("{at}/");
        assert_eq!(
            store(
                &out,
                &[
                    "inspect",
                    &at_slash,
                    "--scratch",
                    kept.to_str().expect("UTF-8")
                ]
            ),
            Ending::Success
        );
        assert_eq!(
            out.printed()[1],
            format!("record: none in {at}"),
            "no trailing slash"
        );
        let inside = data.path().join("scratch");
        assert_eq!(
            store(
                &out,
                &["inspect", at, "--scratch", inside.to_str().expect("UTF-8")]
            ),
            Ending::Refused
        );
        assert_eq!(out.printed().len(), 2, "a refusal prints no answer");
    }
}
