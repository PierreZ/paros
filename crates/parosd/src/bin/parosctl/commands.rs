//! One function per `parosctl` command: parse nothing, decide nothing the
//! library decides — call `paros::client`, print what came back, and say
//! how it ended.

use std::time::Duration;

use clap::Args;
use moonpool_core::{Providers, RandomProvider, TokioProviders};
use paros::client::bootstrap::control_journals_of;
use paros::client::{
    ClaimOutcome, Client, ReaderOutcome, ReconfigureOutcome, RetireOutcome, TruncateOutcome,
    WriteOptions, WriteOutcome, Writer, WriterOutcome,
};
use paros::name::Abbreviations;
use paros::wire::common::Ballot;
use paros::{
    InspectReply, JournalIdentifier, JournalView, LeaderUuid, QuorumSystem, RetireRequest, Seq,
    Value, WireQuorumSystem, journal_state_from_proto, quorum_system_from_proto,
};
use serde_json::{Value as Json, json};

use crate::Ending;
use crate::names::{JournalRef, NodeRef, Resolved};
use crate::output::{Printer, note, record_text, state_json, state_text};

type ParosClient = Client<TokioProviders>;

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
fn leader_or_drawn(providers: &TokioProviders, given: Option<LeaderUuid>) -> LeaderUuid {
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
    providers: &TokioProviders,
    client: &ParosClient,
    out: &Printer,
    args: WriteArgs,
) -> Ending {
    let journal = match crate::names::journal(client, &args.journal).await {
        Ok(journal) => journal,
        Err(ending) => return ending,
    };
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
                            "written seq={seq} count={count} leader={}{}",
                            writer.fence(),
                            if resolved { " (resolved)" } else { "" }
                        )
                    },
                    || {
                        json!({
                            "outcome": "written",
                            "seq": seq,
                            "count": count,
                            "duplicate": duplicate,
                            "resolved": resolved,
                            "leader": writer.fence().to_string(),
                        })
                    },
                );
                return Ending::Success;
            }
            // A stale position, still the owner: the refusal named the
            // tail, and nothing was written — write again there.
            WriterOutcome::Refused { .. }
                if args.seq.is_none() && writer.owned().is_some() && attempt < retries =>
            {
                attempt += 1;
            }
            WriterOutcome::Refused { state } => return refused(out, "refused", &state),
            WriterOutcome::Superseded { state } => return refused(out, "superseded", &state),
            WriterOutcome::Truncated { state } => return refused(out, "truncated", &state),
            WriterOutcome::WrongMode { state } => {
                return refused(out, "multi-writer journal", &state);
            }
            WriterOutcome::NotWritten { state } => return refused(out, "not written", &state),
            WriterOutcome::NotOwner => {
                note("this writer leads no term of the journal");
                return Ending::Refused;
            }
            WriterOutcome::UnknownJournal => return unknown_journal(&journal.label),
            WriterOutcome::TooLarge {
                max_records,
                max_bytes,
            } => {
                return too_large(out, max_records, max_bytes);
            }
            WriterOutcome::Unavailable { leader } => {
                note(&format!(
                    "no leader took the write (last hint: {})",
                    leader.map_or_else(|| "none".to_string(), |l| l.to_string())
                ));
                return Ending::Unreachable;
            }
            WriterOutcome::Ambiguous => {
                note("the write's answer never came and could not be settled: it may land");
                return Ending::Ambiguous;
            }
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
    match report.outcome {
        WriteOutcome::Written { seq, count, .. } => {
            out.emit(
                || format!("written seq={seq} count={count}"),
                || json!({ "outcome": "written", "seq": seq, "count": count }),
            );
            Ending::Success
        }
        WriteOutcome::WrongMode { state } => refused(out, "single-writer journal", &state),
        WriteOutcome::Refused { state } | WriteOutcome::Truncated { state } => {
            refused(out, "refused", &state)
        }
        WriteOutcome::TooLarge {
            max_records,
            max_bytes,
        } => too_large(out, max_records, max_bytes),
        WriteOutcome::UnknownJournal => unknown_journal(&journal.label),
        WriteOutcome::Redirect { .. } => {
            note("no leader took the write");
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
    let resolved = match crate::names::journal(client, &args.journal).await {
        Ok(journal) => journal,
        Err(ending) => return ending,
    };
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
    let resolved = match crate::names::journal(client, &args.journal).await {
        Ok(journal) => journal,
        Err(ending) => return ending,
    };
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
                    tokio::time::sleep(backoff.max(Duration::from_millis(100))).await;
                }
                ReaderOutcome::UnknownJournal => return unknown_journal(&resolved.label),
            }
        }
    };
    tokio::select! {
        ending = follow => ending,
        _ = tokio::signal::ctrl_c() => Ending::Success,
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
    providers: &TokioProviders,
    client: &ParosClient,
    out: &Printer,
    args: TruncateArgs,
) -> Ending {
    let resolved = match crate::names::journal(client, &args.journal).await {
        Ok(journal) => journal,
        Err(ending) => return ending,
    };
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
    match outcome {
        TruncateOutcome::Applied { state } => {
            out.emit(
                || format!("truncated {}", state_text(&state)),
                || json!({ "outcome": "truncated", "state": state_json(&state) }),
            );
            Ending::Success
        }
        // The refusal named another writer: this one was superseded.
        TruncateOutcome::Refused { state } if superseded => refused(out, "superseded", &state),
        TruncateOutcome::Refused { state } => refused(out, "refused", &state),
        TruncateOutcome::WrongMode { state } => refused(out, "wrong mode", &state),
        TruncateOutcome::UnknownJournal => unknown_journal(&resolved.label),
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
    providers: &TokioProviders,
    client: &ParosClient,
    out: &Printer,
    args: SetLeaderArgs,
) -> Ending {
    let resolved = match crate::names::journal(client, &args.journal).await {
        Ok(journal) => journal,
        Err(ending) => return ending,
    };
    let journal = resolved.journal;
    let new = leader_or_drawn(providers, args.new);
    let outcome = match args.old {
        Some(Old(old)) => client
            .set_leader(journal, new, old, start(client))
            .await
            .into(),
        None => client.claim(journal, new, start(client)).await,
    };
    match outcome {
        ClaimOutcome::Won { state } => {
            out.emit(
                || format!("won {}", state_text(&state)),
                || json!({ "outcome": "won", "state": state_json(&state) }),
            );
            Ending::Success
        }
        ClaimOutcome::Owned { state } => {
            out.emit(
                || format!("owned {}", state_text(&state)),
                || json!({ "outcome": "owned", "state": state_json(&state) }),
            );
            Ending::Success
        }
        ClaimOutcome::Lost { state } => refused(out, "lost", &state),
        ClaimOutcome::WrongMode { state } => refused(out, "multi-writer journal", &state),
        ClaimOutcome::UnknownJournal => unknown_journal(&resolved.label),
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

fn ballot_text(ballot: Option<Ballot>, ids: Abbreviations) -> String {
    ballot.map_or_else(
        || "none".to_string(),
        |b| format!("{}.{}", b.round, ids.id(b.node)),
    )
}

/// Node ids, abbreviated.
fn nodes_text(nodes: &[u64], ids: Abbreviations) -> String {
    let nodes: Vec<String> = nodes.iter().map(|n| ids.id(*n)).collect();
    format!("[{}]", nodes.join(","))
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

fn quorum_text(reply: &InspectReply) -> String {
    let wire = WireQuorumSystem {
        quorum_system: reply.quorum_system,
        phase1_quorum: reply.phase1_quorum,
        phase2_quorum: reply.phase2_quorum,
        rows: reply.rows,
        cols: reply.cols,
    };
    match quorum_system_from_proto(&wire) {
        Ok(QuorumSystem::Majority) => "majority".to_string(),
        Ok(QuorumSystem::Flexible { q1, q2 }) => format!("flexible:{q1}:{q2}"),
        Ok(QuorumSystem::Grid { rows, cols }) => format!("grid:{rows}x{cols}"),
        Err(_) => "unknown".to_string(),
    }
}

/// `parosctl inspect`: every server's view, in server order — of the
/// journal named, or of the node alone.
pub async fn inspect(client: &ParosClient, out: &Printer, args: InspectArgs) -> Ending {
    let Some(journal) = args.journal else {
        return inspect_nodes(client, out).await;
    };
    let journal = match crate::names::journal(client, &journal).await {
        Ok(journal) => journal.journal,
        Err(ending) => return ending,
    };
    let mut replies = Vec::new();
    for server in 0..client.server_count() {
        replies.push((client.id_of(server), client.inspect(server, journal).await));
    }
    // One abbreviation for every node id the listing prints.
    let ids = Abbreviations::new(
        server_ids(client)
            .into_iter()
            .chain(replies.iter().flat_map(|(_, reply)| {
                reply.iter().flat_map(|r| {
                    r.members
                        .iter()
                        .chain(&r.retirable)
                        .chain(&r.matchmakers)
                        .copied()
                        .chain(r.config_ballot.map(|b| b.node))
                        .chain(r.gc_watermark.map(|b| b.node))
                })
            })),
    );
    let mut answered = false;
    for (id, reply) in replies {
        let Some(reply) = reply else {
            out.emit(
                || format!("node {}: no answer", ids.id(id)),
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
                    "node {}: leader={} ballot={} members={} quorum={} chosen_index={} \
                     first_slot={} folded={} gc_watermark={} retirable={} \
                     matchmakers={}@{}{}",
                    ids.id(id),
                    reply.leader,
                    ballot_text(reply.config_ballot, ids),
                    nodes_text(&reply.members, ids),
                    quorum_text(&reply),
                    reply
                        .chosen_index
                        .map_or_else(|| "none".to_string(), |c| c.to_string()),
                    reply.first_slot,
                    reply.folded,
                    ballot_text(reply.gc_watermark, ids),
                    nodes_text(&reply.retirable, ids),
                    nodes_text(&reply.matchmakers, ids),
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
                    "quorum": quorum_text(&reply),
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
async fn inspect_nodes(client: &ParosClient, out: &Printer) -> Ending {
    let mut replies = Vec::new();
    for server in 0..client.server_count() {
        replies.push((client.id_of(server), client.inspect_node(server).await));
    }
    // One abbreviation for every id the listing prints.
    let ids = Abbreviations::new(
        server_ids(client)
            .into_iter()
            .chain(replies.iter().flat_map(|(_, reply)| {
                reply.iter().flat_map(|r| {
                    let journals = control_journals_of(r);
                    let cell = journals.map(|j| j.cell);
                    let fleet = journals.and_then(|j| j.fleet);
                    [r.node, r.cell_id].into_iter().chain(
                        cell.into_iter()
                            .chain(fleet)
                            .flat_map(|j| [j.tenant.0, j.journal.0]),
                    )
                })
            })),
    );
    let mut answered = false;
    for (id, reply) in replies {
        let Some(reply) = reply else {
            out.emit(
                || format!("node {}: no answer", ids.id(id)),
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
                let short = |j: Option<JournalIdentifier>| {
                    j.map_or_else(|| "none".to_string(), |j| format!("id:{}", ids.journal(j)))
                };
                format!(
                    "node {}: cell={} control={} universe={}",
                    ids.id(reply.node),
                    if reply.cell_id == 0 {
                        "none".to_string()
                    } else {
                        ids.id(reply.cell_id)
                    },
                    short(journals.map(|j| j.cell)),
                    short(journals.and_then(|j| j.fleet)),
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
    /// The new acceptor set, comma-separated node ids (hex, a unique prefix
    /// of a server's id each).
    #[arg(long, value_delimiter = ',', required = true)]
    members: Vec<NodeRef>,
    /// Its quorum system: `majority`, `flexible:Q1:Q2` or `grid:ROWSxCOLS`.
    #[arg(long, default_value = "majority", value_parser = parse_quorum)]
    quorum: QuorumSystem,
}

fn parse_quorum(s: &str) -> Result<QuorumSystem, String> {
    let bad = || format!("expected majority, flexible:Q1:Q2 or grid:ROWSxCOLS, got {s:?}");
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
    if let Some(grid) = s.strip_prefix("grid:") {
        let (rows, cols) = grid.split_once('x').ok_or_else(bad)?;
        return Ok(QuorumSystem::Grid {
            rows: rows.parse().map_err(|_| bad())?,
            cols: cols.parse().map_err(|_| bad())?,
        });
    }
    Err(bad())
}

/// `parosctl reconfigure`: the operator's call to change the acceptor set.
pub async fn reconfigure(client: &ParosClient, out: &Printer, args: ReconfigureArgs) -> Ending {
    let servers = server_ids(client);
    let mut members = Vec::new();
    for member in &args.members {
        match member.resolve(&servers) {
            Ok(id) => members.push(id),
            Err(ending) => return ending,
        }
    }
    let ids = Abbreviations::new(servers.iter().copied());
    match client
        .reconfigure(&members, args.quorum, start(client))
        .await
    {
        ReconfigureOutcome::Started { round, leader } => {
            out.emit(
                || {
                    format!(
                        "started round={round} leader={}",
                        leader.map_or_else(|| "none".to_string(), |l| ids.id(l))
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
    /// The node to retire (hex, a unique prefix of a server's id: it must
    /// be in `--servers`).
    #[arg(long)]
    node: NodeRef,
    /// The GC watermark, `ROUND.NODE` (the node in hex, a unique prefix of
    /// a server's id); read from the leader's `inspect` of `--journal` when
    /// absent.
    #[arg(long, value_parser = parse_watermark)]
    gc_watermark: Option<(u64, NodeRef)>,
    /// The journal whose leader reports the GC watermark, by name or ids
    /// (the journal the matchmakers serve); needed without
    /// `--gc-watermark`: no identifier has a default (#243).
    #[arg(long)]
    journal: Option<JournalRef>,
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
pub async fn retire(client: &ParosClient, out: &Printer, args: RetireArgs) -> Ending {
    let servers = server_ids(client);
    let ids = Abbreviations::new(servers.iter().copied());
    let node = match args.node.resolve(&servers) {
        Ok(node) => node,
        Err(ending) => return ending,
    };
    let Some(target) = client.index_of(node) else {
        note(&format!("node {} is not in --servers", ids.id(node)));
        return Ending::Refused;
    };
    let watermark = if let Some((round, node)) = args.gc_watermark {
        match node.resolve(&servers) {
            Ok(node) => Ballot { round, node },
            Err(ending) => return ending,
        }
    } else {
        let Some(journal) = args.journal else {
            note(
                "name the journal whose leader reports the watermark (--journal), or pass --gc-watermark",
            );
            return Ending::Refused;
        };
        let journal = match crate::names::journal(client, &journal).await {
            Ok(journal) => journal.journal,
            Err(ending) => return ending,
        };
        let Some(watermark) = leader_watermark(client, journal).await else {
            note("no leader reports an effective GC watermark: nothing is retirable yet");
            return Ending::Refused;
        };
        watermark
    };
    let request = RetireRequest {
        gc_watermark: Some(watermark),
    };
    match client.retire(target, request).await {
        RetireOutcome::Retired => {
            out.emit(
                || format!("retired node={}", ids.id(node)),
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
