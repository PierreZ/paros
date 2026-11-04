//! One function per `parosctl` command: parse nothing, decide nothing the
//! library decides — call `paros::client`, print what came back, and say
//! how it ended.

use std::time::Duration;

use clap::Args;
use moonpool_core::{CompioProviders, LocalProviders, RandomProvider};
use paros::client::bootstrap::control_journals_of;
use paros::client::{
    ClaimOutcome, Client, ReaderOutcome, ReconfigureOutcome, RetireOutcome, TruncateOutcome,
    WriteOptions, WriteOutcome, Writer, WriterOutcome,
};
use paros::core::Value;
use paros::core::journal::{JournalIdentifier, JournalView, LeaderUuid, Seq};
use paros::core::membership::QuorumSystem;
use paros::roles::frontend::Denial;
use paros::wire::common::Ballot;
use paros::wire::{
    InspectReply, RetireRequest, WireQuorumSystem, journal_state_from_proto,
    quorum_system_from_proto,
};
use serde_json::{Value as Json, json};

use crate::support::connect::Ending;
use crate::support::labels::Labels;
use crate::support::names::{JournalRef, NodeRef, Resolved};
use crate::support::output::{Printer, note, quorum_text, record_text, state_json, state_text};
use crate::support::view::Asker;

type ParosClient = Client<CompioProviders>;

/// Where the next call starts: the believed leader, or the first server.
fn start(client: &ParosClient) -> usize {
    client.leader().unwrap_or(0)
}

/// A leader uuid as given on the command line: 1 to 32 hex digits, never
/// the unset uuid.
fn parse_uuid(text: &str) -> Result<LeaderUuid, String> {
    let digits = text.strip_prefix("0x").unwrap_or(text);
    if digits.is_empty() || digits.len() > 32 {
        return Err("a leader uuid is 1 to 32 hex digits".into());
    }
    let uuid = u128::from_str_radix(digits, 16).map_err(|e| e.to_string())?;
    if uuid == 0 {
        return Err("the unset uuid (0) never leads".into());
    }
    Ok(LeaderUuid(uuid))
}

/// `--leader`, or a random uuid when absent: a run that names none leads a
/// term of its own (#241).
fn leader_or_drawn(providers: &CompioProviders, given: Option<LeaderUuid>) -> LeaderUuid {
    given.unwrap_or_else(|| {
        loop {
            let uuid: u128 = providers.random().random();
            if uuid != 0 {
                break LeaderUuid(uuid);
            }
        }
    })
}

/// `parosctl write`.
#[derive(Args, Debug)]
pub struct WriteArgs {
    /// The journal: its name, `TENANT/JOURNAL` or `paros://TENANT/JOURNAL`,
    /// or its ids, `id:TENANT/JOURNAL` in hex (a unique prefix of each).
    journal: JournalRef,
    /// The records, in order, each one argument.
    #[arg(required = true)]
    records: Vec<String>,
    /// The leader uuid to write under (hex); drawn at random when absent,
    /// so a run that names none claims a term of its own.
    #[arg(long, env = "PAROSCTL_LEADER", value_parser = parse_uuid)]
    leader: Option<LeaderUuid>,
    /// Override: write under `--leader` without claiming.
    #[arg(long, requires = "leader")]
    no_claim: bool,
    /// Override: write at this position instead of the tail.
    #[arg(long)]
    seq: Option<u64>,
    /// Write to a multi-writer journal (#241): no leader uuid, no claim,
    /// no position. A write whose answer never came is not sent again: it
    /// may land, and a second send may land twice.
    #[arg(long, conflicts_with_all = ["leader", "no_claim", "seq"])]
    multi: bool,
}

/// Where `journal` stands, read from any server.
async fn journal_state(client: &ParosClient, journal: JournalIdentifier) -> Option<JournalView> {
    client.journal_state(journal, start(client)).await
}

/// Who a writing command acts as: the journal, the leader uuid, and the
/// overrides of `parosctl write` (a truncation names no position).
struct Identity {
    journal: Resolved,
    leader: LeaderUuid,
    claim: bool,
    seq: Option<u64>,
}

/// Become the journal's writer: under `--leader` without a claim (at
/// `--seq`, or the tail a read finds) when `--no-claim` says so, or by a
/// claim — a read naming this uuid already is adopted, never re-claimed.
async fn become_writer(
    client: &ParosClient,
    out: &Printer,
    args: &Identity,
) -> Result<Writer, Ending> {
    let journal = args.journal.journal;
    let label = &args.journal.label;
    let mut writer = Writer::with_uuid(journal, args.leader);
    let at = |next: u64| JournalView {
        leader: Some(args.leader),
        next_seq: Seq(next),
        first_seq: Seq(0),
    };
    if !args.claim {
        let next = if let Some(seq) = args.seq {
            seq
        } else if let Some(state) = journal_state(client, journal).await {
            state.next_seq.0
        } else {
            note("no server served the journal's state");
            return Err(Ending::Unreachable);
        };
        writer.won(&at(next));
        return Ok(writer);
    }
    match writer.claim(client, start(client), false).await {
        ClaimOutcome::Won { state } => {
            note(&format!("claimed journal {label}: {}", state_text(&state)));
        }
        ClaimOutcome::Owned { .. } => {}
        ClaimOutcome::Lost { state } => return Err(refused(out, "claim lost", &state)),
        ClaimOutcome::WrongMode { state } => {
            return Err(refused(out, "multi-writer journal", &state));
        }
        ClaimOutcome::UnknownJournal => return Err(unknown_journal(label)),
        ClaimOutcome::Denied(denial) => return Err(denied(denial)),
        ClaimOutcome::Unread => {
            note("no server served the journal's state");
            return Err(Ending::Unreachable);
        }
        ClaimOutcome::Redirect { .. } => {
            note("no leader decided the claim");
            return Err(Ending::Unreachable);
        }
        ClaimOutcome::Malformed | ClaimOutcome::Ambiguous => {
            note("the claim's answer never came: it may have won");
            return Err(Ending::Ambiguous);
        }
    }
    if let (Some(seq), Some(_)) = (args.seq, writer.owned()) {
        writer.won(&at(seq));
    }
    Ok(writer)
}

/// `parosctl write`: become the writer, then write at the tail through the
/// library's writer session — redirects followed, an ambiguous answer
/// settled, a refusal for a stale position corrected and retried, a
/// superseded writer stopped.
pub async fn write(
    providers: &CompioProviders,
    client: &ParosClient,
    out: &Printer,
    args: WriteArgs,
) -> Ending {
    let journal = match crate::support::names::journal(client, &args.journal).await {
        Ok(journal) => journal,
        Err(ending) => return ending,
    };
    let cell = journal.client.clone();
    let client = &cell;
    if args.multi {
        return append(client, out, &journal, &args.records).await;
    }
    let identity = Identity {
        journal: journal.clone(),
        leader: leader_or_drawn(providers, args.leader),
        claim: !args.no_claim,
        seq: args.seq,
    };
    let mut writer = match become_writer(client, out, &identity).await {
        Ok(writer) => writer,
        Err(ending) => return ending,
    };
    let records: Vec<Value> = args
        .records
        .iter()
        .map(|r| Value(r.as_bytes().to_vec()))
        .collect();
    let retries = client.tunables().retry_budget;
    let mut attempt = 0;
    loop {
        let outcome = writer.write(client, records.clone(), start(client)).await;
        let tail = args.seq.is_none();
        if writes_again(&outcome, tail, writer.owned().is_some(), (attempt, retries)) {
            attempt += 1;
            continue;
        }
        return wrote(out, writer.fence(), &journal.label, &outcome);
    }
}

/// Whether a write at the `tail` is sent again after `outcome`: a stale
/// position, still the `owner`, `attempt` below the `retries` budget. The
/// refusal named the tail, and nothing was written: write again there.
fn writes_again(
    outcome: &WriterOutcome,
    tail: bool,
    owner: bool,
    (attempt, retries): (u32, u32),
) -> bool {
    matches!(outcome, WriterOutcome::Refused { .. }) && tail && owner && attempt < retries
}

/// Print what a single-writer write under `fence` came to.
fn wrote(out: &Printer, fence: LeaderUuid, label: &str, outcome: &WriterOutcome) -> Ending {
    match outcome {
        WriterOutcome::Written {
            seq,
            count,
            duplicate,
            resolved,
        } => {
            out.emit(
                || {
                    format!(
                        "written seq={seq} count={count} leader={fence}{}",
                        if *resolved { " (resolved)" } else { "" }
                    )
                },
                || {
                    json!({
                        "outcome": "written",
                        "seq": seq,
                        "count": count,
                        "duplicate": duplicate,
                        "resolved": resolved,
                        "leader": fence.to_string(),
                    })
                },
            );
            Ending::Success
        }
        WriterOutcome::Refused { state } => refused(out, "refused", state),
        WriterOutcome::Superseded { state } => refused(out, "superseded", state),
        WriterOutcome::Truncated { state } => refused(out, "truncated", state),
        WriterOutcome::WrongMode { state } => refused(out, "multi-writer journal", state),
        WriterOutcome::NotWritten { state } => refused(out, "not written", state),
        WriterOutcome::Denied(denial) => denied(*denial),
        WriterOutcome::NotOwner => {
            note("this writer leads no term of the journal");
            Ending::Refused
        }
        WriterOutcome::UnknownJournal => unknown_journal(label),
        WriterOutcome::TooLarge {
            max_records,
            max_bytes,
        } => too_large(out, *max_records, *max_bytes),
        WriterOutcome::Unavailable { leader } => {
            note(&format!(
                "no leader took the write (last hint: {})",
                leader.map_or_else(|| "none".to_string(), |l| l.to_string())
            ));
            Ending::Unreachable
        }
        WriterOutcome::Ambiguous => {
            note("the write's answer never came and could not be settled: it may land");
            Ending::Ambiguous
        }
    }
}

/// `parosctl write --multi`: one multi-writer write (#241), redirects
/// followed, never re-sent after an answer that never came.
async fn append(
    client: &ParosClient,
    out: &Printer,
    journal: &Resolved,
    records: &[String],
) -> Ending {
    let records = records.iter().map(|r| r.as_bytes().to_vec()).collect();
    let request = paros::client::multi::append_request(journal.journal, records);
    let report = client
        .write(&request, start(client), WriteOptions::default())
        .await;
    appended(out, &journal.label, &report.outcome)
}

/// Print what a multi-writer write came to.
fn appended(out: &Printer, label: &str, outcome: &WriteOutcome) -> Ending {
    match outcome {
        WriteOutcome::Written { seq, count, .. } => {
            out.emit(
                || format!("written seq={seq} count={count}"),
                || json!({ "outcome": "written", "seq": seq, "count": count }),
            );
            Ending::Success
        }
        WriteOutcome::WrongMode { state } => refused(out, "single-writer journal", state),
        WriteOutcome::Refused { state } | WriteOutcome::Truncated { state } => {
            refused(out, "refused", state)
        }
        WriteOutcome::TooLarge {
            max_records,
            max_bytes,
        } => too_large(out, *max_records, *max_bytes),
        WriteOutcome::UnknownJournal => unknown_journal(label),
        WriteOutcome::Denied(denial) => denied(*denial),
        WriteOutcome::Redirect { .. } => {
            note("no leader took the write");
            Ending::Unreachable
        }
        WriteOutcome::Busy => {
            note("the leader stayed busy: its acceptors fall behind; the write did not land");
            Ending::Unreachable
        }
        WriteOutcome::Malformed | WriteOutcome::Ambiguous => {
            note("the write's answer never came: it may land");
            Ending::Ambiguous
        }
    }
}

/// A write over a node's limits.
fn too_large(out: &Printer, max_records: u64, max_bytes: u64) -> Ending {
    out.emit(
        || format!("too large max_records={max_records} max_bytes={max_bytes}"),
        || {
            json!({
                "outcome": "too large",
                "max_records": max_records,
                "max_bytes": max_bytes,
            })
        },
    );
    Ending::Refused
}

fn refused(out: &Printer, what: &str, state: &JournalView) -> Ending {
    out.emit(
        || format!("{what} {}", state_text(state)),
        || json!({ "outcome": what, "state": state_json(state) }),
    );
    Ending::Refused
}

fn denied(denial: Denial) -> Ending {
    note(&format!(
        "the frontend denied the call: {}",
        denial.as_str()
    ));
    Ending::Refused
}

fn unknown_journal(label: &str) -> Ending {
    note(&format!("no server serves journal {label}"));
    Ending::Refused
}

/// `parosctl read`.
#[derive(Args, Debug)]
pub struct ReadArgs {
    /// The journal: its name, `TENANT/JOURNAL` or `paros://TENANT/JOURNAL`,
    /// or its ids, `id:TENANT/JOURNAL` in hex (a unique prefix of each).
    journal: JournalRef,
    /// The first position to read.
    #[arg(long, default_value = "0")]
    from: u64,
    /// At most this many records (0: up to the tail).
    #[arg(long, default_value = "0")]
    limit: u64,
    /// Wait at the tail up to this long for a record.
    #[arg(long, default_value = "0")]
    wait_ms: u64,
}

/// `parosctl read`: page from `--from` to the tail (or `--limit`), one line
/// per record; a truncated range is reported and skipped, never hidden.
pub async fn read(client: &ParosClient, out: &Printer, args: ReadArgs) -> Ending {
    let resolved = match crate::support::names::journal(client, &args.journal).await {
        Ok(journal) => journal,
        Err(ending) => return ending,
    };
    let client = &resolved.client;
    let journal = resolved.journal;
    let mut reader = paros::client::Reader::new(journal, args.from);
    let mut records: Vec<(u64, Vec<u8>)> = Vec::new();
    let mut gaps: Vec<(u64, u64)> = Vec::new();
    let state = loop {
        let left = if args.limit == 0 {
            0
        } else {
            args.limit - records.len() as u64
        };
        let request = reader.request(left, args.wait_ms);
        let report = client.read_any(&request, start(client)).await;
        match reader.absorb(report.outcome) {
            ReaderOutcome::Records {
                from,
                records: page,
                state,
            } => {
                let empty = page.is_empty();
                records.extend((from..).zip(page));
                let full = args.limit > 0 && records.len() as u64 >= args.limit;
                if empty || full || reader.at_tail(&state) {
                    break state;
                }
            }
            ReaderOutcome::Gap {
                from, resumed_at, ..
            } => {
                note(&format!(
                    "positions [{from}, {resumed_at}) were truncated; reading from {resumed_at}"
                ));
                gaps.push((from, resumed_at));
            }
            ReaderOutcome::Unavailable => {
                note("no server served the read");
                return Ending::Unreachable;
            }
            ReaderOutcome::UnknownJournal => return unknown_journal(&resolved.label),
            ReaderOutcome::Denied(denial) => return denied(denial),
        }
    };
    out.emit(
        || {
            records
                .iter()
                .map(|(seq, record)| format!("{seq}\t{}", record_text(record)))
                .chain(std::iter::once(format!("# {}", state_text(&state))))
                .collect::<Vec<_>>()
                .join("\n")
        },
        || {
            json!({
                "journal": journal.to_string(),
                "name": args.journal.to_string(),
                "records": records
                    .iter()
                    .map(|(seq, record)| json!({ "seq": seq, "data": record_text(record) }))
                    .collect::<Vec<_>>(),
                "gaps": gaps
                    .iter()
                    .map(|(from, to)| json!({ "from": from, "to": to }))
                    .collect::<Vec<_>>(),
                "state": state_json(&state),
            })
        },
    );
    Ending::Success
}

/// `parosctl tail`.
#[derive(Args, Debug)]
pub struct TailArgs {
    /// The journal: its name, `TENANT/JOURNAL` or `paros://TENANT/JOURNAL`,
    /// or its ids, `id:TENANT/JOURNAL` in hex (a unique prefix of each).
    journal: JournalRef,
    /// The first position to read.
    #[arg(long, default_value = "0")]
    from: u64,
    /// How long each read waits at the tail for a record.
    #[arg(long, default_value = "1000")]
    wait_ms: u64,
}

/// `parosctl tail`: follow the journal until interrupted, a line (or a JSON
/// document) per record as it lands; a truncation the reader falls behind
/// is reported and skipped.
pub async fn tail(client: &ParosClient, out: &Printer, args: TailArgs) -> Ending {
    let resolved = match crate::support::names::journal(client, &args.journal).await {
        Ok(journal) => journal,
        Err(ending) => return ending,
    };
    let client = &resolved.client;
    let journal = resolved.journal;
    let mut reader = paros::client::Reader::new(journal, args.from);
    let backoff = client.tunables().retry_backoff;
    let follow = async {
        loop {
            let request = reader.request(client.tunables().page_size, args.wait_ms);
            let report = client.read_any(&request, start(client)).await;
            match reader.absorb(report.outcome) {
                ReaderOutcome::Records { from, records, .. } => {
                    for (seq, record) in (from..).zip(records) {
                        out.emit(
                            || format!("{seq}\t{}", record_text(&record)),
                            || json!({ "seq": seq, "data": record_text(&record) }),
                        );
                    }
                }
                ReaderOutcome::Gap {
                    from, resumed_at, ..
                } => {
                    note(&format!(
                        "positions [{from}, {resumed_at}) were truncated; following from {resumed_at}"
                    ));
                    out.emit(
                        || format!("# gap {from}..{resumed_at}"),
                        || json!({ "gap": { "from": from, "to": resumed_at } }),
                    );
                }
                ReaderOutcome::Unavailable => {
                    note("no server served the read; retrying");
                    compio::time::sleep(backoff.max(Duration::from_millis(100))).await;
                }
                ReaderOutcome::UnknownJournal => return unknown_journal(&resolved.label),
                ReaderOutcome::Denied(denial) => return denied(denial),
            }
        }
    };
    moonpool_core::select! {
        ending = follow => ending,
        _ = compio::signal::ctrl_c() => Ending::Success,
    }
}

/// `parosctl truncate`.
#[derive(Args, Debug)]
pub struct TruncateArgs {
    /// The journal: its name, `TENANT/JOURNAL` or `paros://TENANT/JOURNAL`,
    /// or its ids, `id:TENANT/JOURNAL` in hex (a unique prefix of each).
    journal: JournalRef,
    /// Drop every record below this position.
    #[arg(long)]
    up_to: u64,
    /// The leader uuid to truncate under (hex); drawn at random when
    /// absent, so a run that names none claims a term of its own.
    #[arg(long, env = "PAROSCTL_LEADER", value_parser = parse_uuid)]
    leader: Option<LeaderUuid>,
    /// Override: truncate under `--leader` without claiming.
    #[arg(long, requires = "leader")]
    no_claim: bool,
    /// Truncate a multi-writer journal (#241): anyone may, with no leader
    /// uuid and no claim.
    #[arg(long, conflicts_with_all = ["leader", "no_claim"])]
    multi: bool,
}

/// `parosctl truncate`: become the writer exactly as `parosctl write` does
/// (a claim, or `--no-claim`), then ask the leader to raise the journal's
/// floor under that fence (#228). A superseded leader is refused and says so.
pub async fn truncate(
    providers: &CompioProviders,
    client: &ParosClient,
    out: &Printer,
    args: TruncateArgs,
) -> Ending {
    let resolved = match crate::support::names::journal(client, &args.journal).await {
        Ok(journal) => journal,
        Err(ending) => return ending,
    };
    let client = &resolved.client;
    let (outcome, superseded) = if args.multi {
        let request = paros::client::multi::open_truncate_request(resolved.journal, args.up_to);
        (client.truncate(&request, start(client)).await, false)
    } else {
        let identity = Identity {
            journal: resolved.clone(),
            leader: leader_or_drawn(providers, args.leader),
            claim: !args.no_claim,
            seq: None,
        };
        let mut writer = match become_writer(client, out, &identity).await {
            Ok(writer) => writer,
            Err(ending) => return ending,
        };
        let Some(outcome) = writer.truncate(client, args.up_to, start(client)).await else {
            note("this writer leads no term of the journal");
            return Ending::Refused;
        };
        (outcome, writer.owned().is_none())
    };
    truncated(out, &resolved.label, &outcome, superseded)
}

/// Print what a truncation came to; `superseded` when the writer that
/// asked for it lost its term.
fn truncated(out: &Printer, label: &str, outcome: &TruncateOutcome, superseded: bool) -> Ending {
    match outcome {
        TruncateOutcome::Applied { state } => {
            out.emit(
                || format!("truncated {}", state_text(state)),
                || json!({ "outcome": "truncated", "state": state_json(state) }),
            );
            Ending::Success
        }
        // The refusal named another writer: this one was superseded.
        TruncateOutcome::Refused { state } if superseded => refused(out, "superseded", state),
        TruncateOutcome::Refused { state } => refused(out, "refused", state),
        TruncateOutcome::WrongMode { state } => refused(out, "wrong mode", state),
        TruncateOutcome::UnknownJournal => unknown_journal(label),
        TruncateOutcome::Denied(denial) => denied(*denial),
        TruncateOutcome::Redirect { .. } => {
            note("no leader decided the truncation");
            Ending::Unreachable
        }
        TruncateOutcome::Malformed | TruncateOutcome::Ambiguous => {
            note("the truncation's answer never came: it may have been decided");
            Ending::Ambiguous
        }
    }
}

/// `parosctl set-leader`.
#[derive(Args, Debug)]
pub struct SetLeaderArgs {
    /// The journal: its name, `TENANT/JOURNAL` or `paros://TENANT/JOURNAL`,
    /// or its ids, `id:TENANT/JOURNAL` in hex (a unique prefix of each).
    journal: JournalRef,
    /// The leader uuid to install (hex); drawn at random when absent.
    #[arg(long, value_parser = parse_uuid)]
    new: Option<LeaderUuid>,
    /// The leader uuid the caller believes current (hex, or `none` for a
    /// journal no one leads yet); read from the journal when absent.
    #[arg(long, value_parser = parse_old)]
    old: Option<Old>,
}

/// `--old`: a uuid, or `none`.
#[derive(Clone, Copy, Debug)]
struct Old(Option<LeaderUuid>);

fn parse_old(text: &str) -> Result<Old, String> {
    if text == "none" {
        Ok(Old(None))
    } else {
        parse_uuid(text).map(|uuid| Old(Some(uuid)))
    }
}

/// `parosctl set-leader`: compare-and-set the journal's leader uuid —
/// against `--old`, or against the leader a read finds (#241).
pub async fn set_leader(
    providers: &CompioProviders,
    client: &ParosClient,
    out: &Printer,
    args: SetLeaderArgs,
) -> Ending {
    let resolved = match crate::support::names::journal(client, &args.journal).await {
        Ok(journal) => journal,
        Err(ending) => return ending,
    };
    let client = &resolved.client;
    let journal = resolved.journal;
    let new = leader_or_drawn(providers, args.new);
    let outcome = match args.old {
        Some(Old(old)) => client
            .set_leader(journal, new, old, start(client))
            .await
            .into(),
        None => client.claim(journal, new, start(client)).await,
    };
    swapped(out, &resolved.label, &outcome)
}

/// Print what a leader swap came to.
fn swapped(out: &Printer, label: &str, outcome: &ClaimOutcome) -> Ending {
    match outcome {
        ClaimOutcome::Won { state } => {
            out.emit(
                || format!("won {}", state_text(state)),
                || json!({ "outcome": "won", "state": state_json(state) }),
            );
            Ending::Success
        }
        ClaimOutcome::Owned { state } => {
            out.emit(
                || format!("owned {}", state_text(state)),
                || json!({ "outcome": "owned", "state": state_json(state) }),
            );
            Ending::Success
        }
        ClaimOutcome::Lost { state } => refused(out, "lost", state),
        ClaimOutcome::WrongMode { state } => refused(out, "multi-writer journal", state),
        ClaimOutcome::UnknownJournal => unknown_journal(label),
        ClaimOutcome::Denied(denial) => denied(*denial),
        ClaimOutcome::Unread | ClaimOutcome::Redirect { .. } => {
            note("no leader decided the swap");
            Ending::Unreachable
        }
        ClaimOutcome::Malformed | ClaimOutcome::Ambiguous => {
            note("the swap's answer never came: it may have won");
            Ending::Ambiguous
        }
    }
}

/// `parosctl inspect`.
#[derive(Args, Debug)]
pub struct InspectArgs {
    /// The journal to inspect: its name or its ids (`id:TENANT/JOURNAL`).
    /// Without it, each node is asked for its own facts alone: its id, its
    /// cell and the control journals (no identifier has a default, #243).
    #[arg(long)]
    journal: Option<JournalRef>,
}

fn ballot_text(ballot: Option<Ballot>, labels: &Labels) -> String {
    ballot.map_or_else(
        || "none".to_string(),
        |b| format!("{}.{}", b.round, labels.machine(b.node)),
    )
}

/// The servers' ids.
fn server_ids(client: &ParosClient) -> Vec<u64> {
    (0..client.server_count())
        .map(|s| client.id_of(s))
        .collect()
}

fn ballot_json(ballot: Option<Ballot>) -> Json {
    ballot.map_or(Json::Null, |b| json!({ "round": b.round, "node": b.node }))
}

fn inspect_quorum_text(reply: &InspectReply) -> String {
    let wire = WireQuorumSystem {
        quorum_system: reply.quorum_system,
        phase1_quorum: reply.phase1_quorum,
        phase2_quorum: reply.phase2_quorum,
        rows: reply.rows,
        cols: reply.cols,
    };
    quorum_system_from_proto(&wire).map_or_else(|_| "unknown".to_string(), quorum_text)
}

/// `parosctl inspect`: every server's view, in server order — of the
/// journal named, or of the node alone.
pub async fn inspect(
    (client, asker): (&ParosClient, &Asker<'_>),
    labels: &Labels,
    out: &Printer,
    args: InspectArgs,
) -> Ending {
    let Some(journal) = args.journal else {
        return inspect_nodes(client, labels, out).await;
    };
    let resolved = match crate::support::names::journal(client, &journal).await {
        Ok(journal) => journal,
        Err(ending) => return ending,
    };
    let (journal, client) = (resolved.journal, &resolved.client);
    // The names of the journal's cell's machines, when another cell serves it.
    let mut labels = labels.clone();
    if !resolved.elsewhere.is_empty() {
        labels.absorb(
            Labels::fetch_from(
                (asker.providers, asker.rpc, asker.names),
                &resolved.elsewhere,
                asker.timeout,
            )
            .await,
        );
    }
    let labels = &labels;
    let mut replies = Vec::new();
    for server in 0..client.server_count() {
        replies.push((client.id_of(server), client.inspect(server, journal).await));
    }
    let mut answered = false;
    for (id, reply) in replies {
        let Some(reply) = reply else {
            out.emit(
                || format!("machine {}: no answer", labels.machine(id)),
                || json!({ "node": id, "answered": false }),
            );
            continue;
        };
        answered = true;
        let state = journal_state_from_proto(reply.journal)
            .ok()
            .map(|s| s.view());
        out.emit(
            || {
                format!(
                    "machine {}: paxos_leader={} ballot={} members={} quorum={} chosen_index={} \
                     first_slot={} folded={} gc_watermark={} retirable={} \
                     matchmakers={}@{}{}",
                    labels.machine(id),
                    reply.leader,
                    ballot_text(reply.config_ballot, labels),
                    labels.machines(&reply.members),
                    inspect_quorum_text(&reply),
                    reply
                        .chosen_index
                        .map_or_else(|| "none".to_string(), |c| c.to_string()),
                    reply.first_slot,
                    reply.folded,
                    ballot_text(reply.gc_watermark, labels),
                    labels.machines(&reply.retirable),
                    labels.machines(&reply.matchmakers),
                    reply.matchmaker_generation,
                    state
                        .as_ref()
                        .map_or_else(String::new, |s| format!(" {}", state_text(s))),
                )
            },
            || {
                json!({
                    "node": id,
                    "answered": true,
                    "leader": reply.leader,
                    "ballot": ballot_json(reply.config_ballot),
                    "members": reply.members,
                    "quorum": inspect_quorum_text(&reply),
                    "chosen_index": reply.chosen_index,
                    "first_slot": reply.first_slot,
                    "folded": reply.folded,
                    "gc_watermark": ballot_json(reply.gc_watermark),
                    "retirable": reply.retirable,
                    "matchmakers": reply.matchmakers,
                    "matchmaker_generation": reply.matchmaker_generation,
                    "journal": state.as_ref().map(state_json),
                })
            },
        );
    }
    if answered {
        Ending::Success
    } else {
        Ending::Unreachable
    }
}

/// `parosctl inspect` without `--journal`: every server's own facts — its
/// id, its cell and the control journals it names.
async fn inspect_nodes(client: &ParosClient, labels: &Labels, out: &Printer) -> Ending {
    let mut replies = Vec::new();
    for server in 0..client.server_count() {
        replies.push((client.id_of(server), client.inspect_node(server).await));
    }
    // A cell that does not host the universe directory names it through its
    // universe pointer (#232 (multiple cells)).
    let pointed = match replies
        .iter()
        .filter_map(|(_, reply)| reply.as_ref().and_then(control_journals_of))
        .find(|journals| journals.fleet.is_none())
    {
        Some(own) => crate::support::cells::universe_label(client, &own).await,
        None => None,
    };
    let mut answered = false;
    for (id, reply) in replies {
        let Some(reply) = reply else {
            out.emit(
                || format!("machine {}: no answer", labels.machine(id)),
                || json!({ "node": id, "answered": false }),
            );
            continue;
        };
        answered = true;
        let journals = control_journals_of(&reply);
        let cell = journals.map(|j| j.cell.to_string());
        let fleet = journals.and_then(|j| j.fleet).map(|f| f.to_string());
        out.emit(
            || {
                let journal = |j: Option<JournalIdentifier>| {
                    j.map_or_else(|| "none".to_string(), |j| labels.journal(j))
                };
                format!(
                    "machine {}: cell={} control={} universe={}",
                    labels.machine(reply.node),
                    labels.cell(reply.cell_id),
                    journal(journals.map(|j| j.cell)),
                    match (journals.and_then(|j| j.fleet), &pointed) {
                        (None, Some(pointed)) => pointed.clone(),
                        (fleet, _) => journal(fleet),
                    },
                )
            },
            || {
                json!({
                    "node": reply.node,
                    "answered": true,
                    "cell": reply.cell_id,
                    "control": cell,
                    "fleet": fleet,
                })
            },
        );
    }
    if answered {
        Ending::Success
    } else {
        Ending::Unreachable
    }
}

/// `parosctl reconfigure`.
#[derive(Args, Debug)]
pub struct ReconfigureArgs {
    /// The journal to reconfigure, by name (`TENANT/JOURNAL`) or ids: a
    /// machine serves many, and none is the default (#439).
    #[arg(long)]
    journal: JournalRef,
    /// The new acceptor set, comma-separated machine names (or, for local
    /// debugging, a unique hex prefix of a server's id each).
    #[arg(long, value_delimiter = ',', required = true)]
    members: Vec<NodeRef>,
    /// Its quorum system: `majority`, `flexible:Q1:Q2` or
    /// `partitions:F:P` (`P` acceptor partitions of `2F+1` acceptors each,
    /// #532 (acceptor partitions)).
    #[arg(long, default_value = "majority", value_parser = parse_quorum)]
    quorum: QuorumSystem,
}

fn parse_quorum(s: &str) -> Result<QuorumSystem, String> {
    let bad = || format!("expected majority, flexible:Q1:Q2 or partitions:F:P, got {s:?}");
    if s == "majority" {
        return Ok(QuorumSystem::Majority);
    }
    if let Some(split) = s.strip_prefix("flexible:") {
        let (q1, q2) = split.split_once(':').ok_or_else(bad)?;
        return Ok(QuorumSystem::Flexible {
            q1: q1.parse().map_err(|_| bad())?,
            q2: q2.parse().map_err(|_| bad())?,
        });
    }
    if let Some(partitions) = s.strip_prefix("partitions:") {
        let (f, p) = partitions.split_once(':').ok_or_else(bad)?;
        let f: usize = f.parse().map_err(|_| bad())?;
        let p: usize = p.parse().map_err(|_| bad())?;
        if f > 3 || p == 0 {
            return Err(bad());
        }
        return Ok(QuorumSystem::partitions(f, p));
    }
    Err(bad())
}

/// `parosctl reconfigure`: the operator's call to change the acceptor set.
pub async fn reconfigure(
    client: &ParosClient,
    labels: &Labels,
    out: &Printer,
    args: ReconfigureArgs,
) -> Ending {
    let servers = server_ids(client);
    let mut members = Vec::new();
    for member in &args.members {
        match member.resolve(&servers, labels) {
            Ok(id) => members.push(id),
            Err(ending) => return ending,
        }
    }
    let journal = match crate::support::names::journal(client, &args.journal).await {
        Ok(journal) => journal,
        Err(ending) => return ending,
    };
    match client
        .reconfigure(journal.journal, &members, args.quorum, start(client))
        .await
    {
        ReconfigureOutcome::Started { round, leader } => {
            out.emit(
                || {
                    format!(
                        "started round={round} leader={}",
                        leader.map_or_else(|| "none".to_string(), |l| labels.machine(l))
                    )
                },
                || json!({ "outcome": "started", "round": round, "leader": leader }),
            );
            Ending::Success
        }
        ReconfigureOutcome::Refused { refusal, .. } => {
            out.emit(
                || format!("refused {refusal:?}"),
                || json!({ "outcome": "refused", "refusal": format!("{refusal:?}") }),
            );
            Ending::Refused
        }
        ReconfigureOutcome::UnknownJournal => {
            note(&format!(
                "no machine asked serves journal {}",
                journal.label
            ));
            Ending::Refused
        }
        ReconfigureOutcome::NotLeader { .. } | ReconfigureOutcome::Unrecognized { .. } => {
            note("no leader took the reconfiguration");
            Ending::Unreachable
        }
        ReconfigureOutcome::Ambiguous => {
            note("the reconfiguration's answer never came: it may have started");
            Ending::Ambiguous
        }
    }
}

/// `parosctl retire`.
#[derive(Args, Debug)]
pub struct RetireArgs {
    /// The machine to retire, by name (or a unique hex prefix of a
    /// server's id): it must be in `--servers`.
    #[arg(long)]
    node: NodeRef,
    /// The GC watermark, `ROUND.MACHINE` (the machine by name, or a unique
    /// hex prefix of a server's id); read from the leader's `inspect` of `--journal` when
    /// absent.
    #[arg(long, value_parser = parse_watermark)]
    gc_watermark: Option<(u64, NodeRef)>,
    /// The journal the machine leaves, by name or ids; without
    /// `--gc-watermark`, its leader reports the watermark. No identifier
    /// has a default (#243, #439).
    #[arg(long)]
    journal: JournalRef,
}

fn parse_watermark(s: &str) -> Result<(u64, NodeRef), String> {
    let (round, node) = s
        .split_once('.')
        .ok_or_else(|| format!("expected ROUND.NODE, got {s:?}"))?;
    Ok((
        round.parse().map_err(|e| format!("bad round: {e}"))?,
        node.parse().map_err(|e| format!("bad node: {e}"))?,
    ))
}

/// The effective GC watermark `journal`'s leader reports, read from every
/// server.
async fn leader_watermark(client: &ParosClient, journal: JournalIdentifier) -> Option<Ballot> {
    for server in 0..client.server_count() {
        if let Some(reply) = client.inspect(server, journal).await
            && reply.leader
            && reply.gc_watermark.is_some()
        {
            return reply.gc_watermark;
        }
    }
    None
}

/// `parosctl retire`: retire a node, carrying the GC watermark that proves
/// the cluster forgot every configuration naming it (the RPC's evidence).
pub async fn retire(
    client: &ParosClient,
    labels: &Labels,
    out: &Printer,
    args: RetireArgs,
) -> Ending {
    let servers = server_ids(client);
    let node = match args.node.resolve(&servers, labels) {
        Ok(node) => node,
        Err(ending) => return ending,
    };
    let Some(target) = client.index_of(node) else {
        note(&format!(
            "machine {} is not in --servers",
            labels.machine(node)
        ));
        return Ending::Refused;
    };
    let journal = match crate::support::names::journal(client, &args.journal).await {
        Ok(journal) => journal.journal,
        Err(ending) => return ending,
    };
    let watermark = if let Some((round, node)) = args.gc_watermark {
        match node.resolve(&servers, labels) {
            Ok(node) => Ballot { round, node },
            Err(ending) => return ending,
        }
    } else {
        let Some(watermark) = leader_watermark(client, journal).await else {
            note("no leader reports an effective GC watermark: nothing is retirable yet");
            return Ending::Refused;
        };
        watermark
    };
    let request = RetireRequest {
        gc_watermark: Some(watermark),
        tenant: journal.tenant.0,
        journal: journal.journal.0,
    };
    match client.retire(target, request).await {
        RetireOutcome::Retired => {
            out.emit(
                || format!("retired machine={}", labels.machine(node)),
                || json!({ "outcome": "retired", "node": node }),
            );
            Ending::Success
        }
        RetireOutcome::Refused(refusal) => {
            out.emit(
                || format!("refused {refusal:?}"),
                || json!({ "outcome": "refused", "refusal": format!("{refusal:?}") }),
            );
            Ending::Refused
        }
        RetireOutcome::Ambiguous => {
            note("the retirement's answer never came: the node may have retired");
            Ending::Ambiguous
        }
    }
}

#[cfg(test)]
mod tests {
    use moonpool_core::CompioProviders;
    use paros::core::journal::LeaderUuid;
    use paros::core::membership::QuorumSystem;
    use paros::wire::{InspectReply, quorum_system_to_proto};

    use paros::client::{ClaimOutcome, TruncateOutcome, WriteOutcome, WriterOutcome};
    use paros::core::journal::{JournalView, Seq};
    use paros::roles::frontend::Denial;

    use super::{
        appended, inspect_quorum_text, leader_or_drawn, parse_quorum, parse_uuid, parse_watermark,
        swapped, truncated, writes_again, wrote,
    };
    use crate::support::connect::Ending;
    use crate::support::output::Printer;

    /// A journal led by `0x…07`, at `[2, 9)`.
    fn view() -> JournalView {
        JournalView {
            leader: Some(LeaderUuid(7)),
            next_seq: Seq(9),
            first_seq: Seq(2),
        }
    }

    const STATE: &str = "leader=00000000000000000000000000000007 next_seq=9 first_seq=2";

    #[test]
    fn only_a_stale_position_at_the_tail_is_written_again() {
        let refused = WriterOutcome::Refused { state: view() };
        assert!(writes_again(&refused, true, true, (0, 3)));
        assert!(writes_again(&refused, true, true, (2, 3)));
        assert!(!writes_again(&refused, true, true, (3, 3)), "budget spent");
        assert!(!writes_again(&refused, false, true, (0, 3)), "--seq given");
        assert!(!writes_again(&refused, true, false, (0, 3)), "superseded");
        let superseded = WriterOutcome::Superseded { state: view() };
        assert!(!writes_again(&superseded, true, true, (0, 3)));
        assert!(!writes_again(&WriterOutcome::Ambiguous, true, true, (0, 3)));
    }

    #[test]
    fn a_single_writer_write_prints_its_fence() {
        let fence = LeaderUuid(0xab);
        for (resolved, tail) in [(false, ""), (true, " (resolved)")] {
            let out = Printer::new(false);
            let written = WriterOutcome::Written {
                seq: 4,
                count: 2,
                duplicate: false,
                resolved,
            };
            assert_eq!(wrote(&out, fence, "t/j", &written), Ending::Success);
            assert_eq!(
                out.printed(),
                vec![format!(
                    "written seq=4 count=2 leader=000000000000000000000000000000ab{tail}"
                )]
            );
        }
        let json = Printer::new(true);
        let written = WriterOutcome::Written {
            seq: 4,
            count: 2,
            duplicate: true,
            resolved: true,
        };
        assert_eq!(wrote(&json, fence, "t/j", &written), Ending::Success);
        let doc: serde_json::Value = serde_json::from_str(&json.printed()[0]).expect("json");
        assert_eq!(doc["outcome"], "written");
        assert_eq!(doc["seq"], 4);
        assert_eq!(doc["count"], 2);
        assert_eq!(doc["duplicate"], true);
        assert_eq!(doc["resolved"], true);
        assert_eq!(doc["leader"], "000000000000000000000000000000ab");
    }

    #[test]
    fn a_single_writer_refusal_names_its_kind() {
        let state = view();
        for (outcome, what) in [
            (WriterOutcome::Refused { state }, "refused"),
            (WriterOutcome::Superseded { state }, "superseded"),
            (WriterOutcome::Truncated { state }, "truncated"),
            (WriterOutcome::WrongMode { state }, "multi-writer journal"),
            (WriterOutcome::NotWritten { state }, "not written"),
        ] {
            let out = Printer::new(false);
            assert_eq!(wrote(&out, LeaderUuid(1), "t/j", &outcome), Ending::Refused);
            assert_eq!(out.printed(), vec![format!("{what} {STATE}")]);
        }
        let out = Printer::new(false);
        let too_large = WriterOutcome::TooLarge {
            max_records: 3,
            max_bytes: 10,
        };
        assert_eq!(
            wrote(&out, LeaderUuid(1), "t/j", &too_large),
            Ending::Refused
        );
        assert_eq!(out.printed(), vec!["too large max_records=3 max_bytes=10"]);
        for (outcome, ending) in [
            (WriterOutcome::Denied(Denial::Expired), Ending::Refused),
            (WriterOutcome::NotOwner, Ending::Refused),
            (WriterOutcome::UnknownJournal, Ending::Refused),
            (
                WriterOutcome::Unavailable { leader: None },
                Ending::Unreachable,
            ),
            (
                WriterOutcome::Unavailable { leader: Some(3) },
                Ending::Unreachable,
            ),
            (WriterOutcome::Ambiguous, Ending::Ambiguous),
        ] {
            let out = Printer::new(false);
            assert_eq!(wrote(&out, LeaderUuid(1), "t/j", &outcome), ending);
            assert!(out.printed().is_empty(), "only a note: {outcome:?}");
        }
    }

    #[test]
    fn a_multi_writer_write_prints_its_place() {
        let out = Printer::new(false);
        let written = WriteOutcome::Written {
            seq: 5,
            count: 1,
            duplicate: false,
        };
        assert_eq!(appended(&out, "t/j", &written), Ending::Success);
        assert_eq!(out.printed(), vec!["written seq=5 count=1"]);
        let state = view();
        for (outcome, what) in [
            (WriteOutcome::WrongMode { state }, "single-writer journal"),
            (WriteOutcome::Refused { state }, "refused"),
            (WriteOutcome::Truncated { state }, "refused"),
        ] {
            let out = Printer::new(false);
            assert_eq!(appended(&out, "t/j", &outcome), Ending::Refused);
            assert_eq!(out.printed(), vec![format!("{what} {STATE}")]);
        }
        let out = Printer::new(false);
        let too_large = WriteOutcome::TooLarge {
            max_records: 1,
            max_bytes: 2,
        };
        assert_eq!(appended(&out, "t/j", &too_large), Ending::Refused);
        assert_eq!(out.printed(), vec!["too large max_records=1 max_bytes=2"]);
        for (outcome, ending) in [
            (WriteOutcome::UnknownJournal, Ending::Refused),
            (WriteOutcome::Denied(Denial::Forbidden), Ending::Refused),
            (
                WriteOutcome::Redirect { leader: Some(1) },
                Ending::Unreachable,
            ),
            (WriteOutcome::Busy, Ending::Unreachable),
            (WriteOutcome::Malformed, Ending::Ambiguous),
            (WriteOutcome::Ambiguous, Ending::Ambiguous),
        ] {
            let out = Printer::new(false);
            assert_eq!(appended(&out, "t/j", &outcome), ending);
            assert!(out.printed().is_empty(), "only a note: {outcome:?}");
        }
    }

    #[test]
    fn a_truncation_refused_after_a_lost_term_is_superseded() {
        let state = view();
        let out = Printer::new(false);
        let applied = TruncateOutcome::Applied { state };
        assert_eq!(truncated(&out, "t/j", &applied, false), Ending::Success);
        assert_eq!(out.printed(), vec![format!("truncated {STATE}")]);
        for (outcome, superseded, what) in [
            (TruncateOutcome::Refused { state }, true, "superseded"),
            (TruncateOutcome::Refused { state }, false, "refused"),
            (TruncateOutcome::WrongMode { state }, true, "wrong mode"),
        ] {
            let out = Printer::new(false);
            assert_eq!(
                truncated(&out, "t/j", &outcome, superseded),
                Ending::Refused
            );
            assert_eq!(out.printed(), vec![format!("{what} {STATE}")]);
        }
        for (outcome, ending) in [
            (TruncateOutcome::UnknownJournal, Ending::Refused),
            (TruncateOutcome::Denied(Denial::Malformed), Ending::Refused),
            (
                TruncateOutcome::Redirect { leader: None },
                Ending::Unreachable,
            ),
            (TruncateOutcome::Malformed, Ending::Ambiguous),
            (TruncateOutcome::Ambiguous, Ending::Ambiguous),
        ] {
            let out = Printer::new(false);
            assert_eq!(truncated(&out, "t/j", &outcome, false), ending);
            assert!(out.printed().is_empty(), "only a note: {outcome:?}");
        }
    }

    #[test]
    fn a_leader_swap_prints_who_owns_the_journal() {
        let state = view();
        for (outcome, what, ending) in [
            (ClaimOutcome::Won { state }, "won", Ending::Success),
            (ClaimOutcome::Owned { state }, "owned", Ending::Success),
            (ClaimOutcome::Lost { state }, "lost", Ending::Refused),
            (
                ClaimOutcome::WrongMode { state },
                "multi-writer journal",
                Ending::Refused,
            ),
        ] {
            let out = Printer::new(false);
            assert_eq!(swapped(&out, "t/j", &outcome), ending);
            assert_eq!(out.printed(), vec![format!("{what} {STATE}")]);
        }
        let json = Printer::new(true);
        assert_eq!(
            swapped(&json, "t/j", &ClaimOutcome::Owned { state }),
            Ending::Success
        );
        let doc: serde_json::Value = serde_json::from_str(&json.printed()[0]).expect("json");
        assert_eq!(doc["outcome"], "owned");
        assert_eq!(doc["state"]["next_seq"], 9);
        for (outcome, ending) in [
            (ClaimOutcome::UnknownJournal, Ending::Refused),
            (ClaimOutcome::Denied(Denial::InvalidToken), Ending::Refused),
            (ClaimOutcome::Unread, Ending::Unreachable),
            (ClaimOutcome::Redirect { leader: None }, Ending::Unreachable),
            (ClaimOutcome::Malformed, Ending::Ambiguous),
            (ClaimOutcome::Ambiguous, Ending::Ambiguous),
        ] {
            let out = Printer::new(false);
            assert_eq!(swapped(&out, "t/j", &outcome), ending);
            assert!(out.printed().is_empty(), "only a note: {outcome:?}");
        }
    }

    /// The text `inspect` prints for a node that runs `quorum`.
    fn printed(quorum: QuorumSystem) -> String {
        let wire = quorum_system_to_proto(quorum);
        inspect_quorum_text(&InspectReply {
            quorum_system: wire.quorum_system,
            phase1_quorum: wire.phase1_quorum,
            phase2_quorum: wire.phase2_quorum,
            rows: wire.rows,
            cols: wire.cols,
            ..InspectReply::default()
        })
    }

    #[test]
    fn a_quorum_system_reads_as_it_is_written() {
        assert_eq!(parse_quorum("majority"), Ok(QuorumSystem::Majority));
        assert_eq!(
            parse_quorum("flexible:3:2"),
            Ok(QuorumSystem::Flexible { q1: 3, q2: 2 })
        );
        assert_eq!(
            parse_quorum("partitions:1:2"),
            Ok(QuorumSystem::partitions(1, 2))
        );
        assert_eq!(
            parse_quorum("partitions:3:1"),
            Ok(QuorumSystem::partitions(3, 1))
        );
        assert_eq!(
            parse_quorum("partitions:0:1"),
            Ok(QuorumSystem::partitions(0, 1))
        );
        for text in [
            "majority",
            "flexible:3:2",
            "partitions:1:2",
            "partitions:2:3",
        ] {
            let quorum = parse_quorum(text).expect("valid");
            assert_eq!(printed(quorum), text, "printed as typed");
        }
    }

    #[test]
    fn a_malformed_quorum_system_is_refused_by_name() {
        for text in [
            "",
            "Majority",
            "flexible:3",
            "flexible:a:2",
            "flexible:3:b",
            "partitions:1",
            "partitions:x:2",
            "partitions:1:y",
            "partitions:4:1",
            "partitions:1:0",
            "grid:3x3:2",
        ] {
            let refused = parse_quorum(text).expect_err(text);
            assert_eq!(
                refused,
                format!("expected majority, flexible:Q1:Q2 or partitions:F:P, got {text:?}")
            );
        }
    }

    #[test]
    fn a_grid_that_is_not_partitions_shows_its_shape() {
        let grid = QuorumSystem::Grid {
            rows: 3,
            cols: 3,
            acks: 1,
        };
        assert_eq!(printed(grid), "grid:3x3:1");
        let two_by_two = QuorumSystem::Grid {
            rows: 2,
            cols: 2,
            acks: 2,
        };
        assert_eq!(printed(two_by_two), "grid:2x2:2");
        let unknown = InspectReply {
            quorum_system: 99,
            ..InspectReply::default()
        };
        assert_eq!(inspect_quorum_text(&unknown), "unknown");
    }

    #[test]
    fn a_leader_uuid_is_1_to_32_hex_digits_and_never_zero() {
        assert_eq!(parse_uuid("ff"), Ok(LeaderUuid(0xff)));
        assert_eq!(parse_uuid("0x1A"), Ok(LeaderUuid(0x1a)));
        assert_eq!(
            parse_uuid(&"f".repeat(32)),
            Ok(LeaderUuid(u128::MAX)),
            "32 digits"
        );
        let length = Err("a leader uuid is 1 to 32 hex digits".to_string());
        assert_eq!(parse_uuid(""), length);
        assert_eq!(parse_uuid("0x"), length);
        assert_eq!(parse_uuid(&"1".repeat(33)), length);
        assert_eq!(
            parse_uuid("0"),
            Err("the unset uuid (0) never leads".to_string())
        );
        assert!(parse_uuid("xyz").is_err());
    }

    #[test]
    fn a_watermark_is_a_round_and_a_machine() {
        let (round, node) = parse_watermark("7.alpha").expect("valid");
        assert_eq!((round, node), (7, "alpha".parse().expect("a name")));
        assert_eq!(
            parse_watermark("7"),
            Err("expected ROUND.NODE, got \"7\"".to_string())
        );
        assert!(
            parse_watermark("x.alpha").is_err_and(|e| e.starts_with("bad round: ")),
            "a round is a number"
        );
        assert!(parse_watermark("7.").is_err_and(|e| e.starts_with("bad node: ")));
    }

    #[test]
    fn a_given_leader_is_kept_and_a_drawn_one_is_never_unset() {
        let providers = CompioProviders::new();
        assert_eq!(
            leader_or_drawn(&providers, Some(LeaderUuid(5))),
            LeaderUuid(5)
        );
        let drawn = leader_or_drawn(&providers, None);
        assert_ne!(drawn, LeaderUuid(0));
        assert_ne!(drawn, leader_or_drawn(&providers, None), "a fresh draw");
    }
}
