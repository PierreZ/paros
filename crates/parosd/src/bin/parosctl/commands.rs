//! One function per `parosctl` command: parse nothing, decide nothing the
//! library decides — call `paros::client`, print what came back, and say
//! how it ended.

use std::time::Duration;

use clap::Args;
use moonpool_core::TokioProviders;
use paros::client::{
    ClaimOutcome, Client, ReaderOutcome, ReconfigureOutcome, RetireOutcome, TruncateOutcome,
    Writer, WriterOutcome,
};
use paros::wire::common::Ballot;
use paros::{
    ClientId, Generation, InspectReply, JournalId, JournalState, QuorumSystem, Read, RetireRequest,
    Seq, Value, WireQuorumSystem, journal_state_from_proto, quorum_system_from_proto,
};
use serde_json::{Value as Json, json};

use crate::Ending;
use crate::output::{Printer, note, record_text, state_json, state_text};

type ParosClient = Client<TokioProviders>;

/// Where the next call starts: the believed leader, or the first server.
fn start(client: &ParosClient) -> usize {
    client.leader().unwrap_or(0)
}

/// `parosctl write`.
#[derive(Args, Debug)]
pub struct WriteArgs {
    /// The journal.
    journal: u64,
    /// The records, in order, each one argument.
    #[arg(required = true)]
    records: Vec<String>,
    /// The client writing (its identity as the journal's owner).
    #[arg(long, env = "PAROSCTL_OWNER", default_value = "1")]
    owner: u64,
    /// Override: write under this generation instead of claiming.
    #[arg(long)]
    generation: Option<u64>,
    /// Override: write at this position instead of the tail.
    #[arg(long)]
    seq: Option<u64>,
}

/// Where `journal` stands, read from any server.
async fn journal_state(client: &ParosClient, journal: JournalId) -> Option<JournalState> {
    let read = Read {
        journal: journal.0,
        from_seq: 0,
        limit: 1,
        wait_ms: 0,
    };
    client.read_any(&read, start(client)).await.outcome.state()
}

/// Who a writing command acts as: the journal, the owner id, and the
/// overrides of `parosctl write` (a truncation names no position).
struct Identity {
    journal: u64,
    owner: u64,
    generation: Option<u64>,
    seq: Option<u64>,
}

/// Become the journal's writer: under `--generation` (at `--seq`, or the
/// tail a read finds) as given, or by a claim — a read naming this owner
/// already is adopted, never re-claimed.
async fn become_writer(
    client: &ParosClient,
    out: &Printer,
    args: &Identity,
) -> Result<Writer, Ending> {
    let journal = JournalId(args.journal);
    let mut writer = Writer::new(journal, args.owner);
    let at = |generation: u64, next: u64| JournalState {
        owner: Some(ClientId(args.owner)),
        generation: Generation(generation),
        next_seq: Seq(next),
        first_seq: Seq(0),
    };
    if let Some(generation) = args.generation {
        let next = if let Some(seq) = args.seq {
            seq
        } else if let Some(state) = journal_state(client, journal).await {
            state.next_seq.0
        } else {
            note("no server served the journal's state");
            return Err(Ending::Unreachable);
        };
        writer.won(&at(generation, next));
        return Ok(writer);
    }
    match writer.claim(client, start(client), false).await {
        ClaimOutcome::Won { state } => {
            note(&format!(
                "claimed journal {}: {}",
                journal.0,
                state_text(&state)
            ));
        }
        ClaimOutcome::Owned { .. } => {}
        ClaimOutcome::Lost { state } => return Err(refused(out, "claim lost", &state)),
        ClaimOutcome::UnknownJournal => return Err(unknown_journal(journal)),
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
    if let (Some(seq), Some(generation)) = (args.seq, writer.owned()) {
        writer.won(&at(generation, seq));
    }
    Ok(writer)
}

/// `parosctl write`: become the writer, then write at the tail through the
/// library's writer session — redirects followed, an ambiguous answer
/// settled, a refusal for a stale position corrected and retried, a
/// superseded writer stopped.
pub async fn write(client: &ParosClient, out: &Printer, args: WriteArgs) -> Ending {
    let journal = JournalId(args.journal);
    let identity = Identity {
        journal: args.journal,
        owner: args.owner,
        generation: args.generation,
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
                            "written seq={seq} count={count} generation={} owner={}{}",
                            writer.generation(),
                            args.owner,
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
                            "generation": writer.generation(),
                            "owner": args.owner,
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
            WriterOutcome::NotWritten { state } => return refused(out, "not written", &state),
            WriterOutcome::NotOwner => {
                note("this writer owns no generation of the journal");
                return Ending::Refused;
            }
            WriterOutcome::UnknownJournal => return unknown_journal(journal),
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

fn refused(out: &Printer, what: &str, state: &JournalState) -> Ending {
    out.emit(
        || format!("{what} {}", state_text(state)),
        || json!({ "outcome": what, "state": state_json(state) }),
    );
    Ending::Refused
}

fn unknown_journal(journal: JournalId) -> Ending {
    note(&format!("no server serves journal {}", journal.0));
    Ending::Refused
}

/// `parosctl read`.
#[derive(Args, Debug)]
pub struct ReadArgs {
    /// The journal.
    journal: u64,
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
    let journal = JournalId(args.journal);
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
            ReaderOutcome::UnknownJournal => return unknown_journal(journal),
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
                "journal": journal.0,
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
    /// The journal.
    journal: u64,
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
    let journal = JournalId(args.journal);
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
                ReaderOutcome::UnknownJournal => return unknown_journal(journal),
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
    /// The journal.
    journal: u64,
    /// Drop every record below this position.
    #[arg(long)]
    up_to: u64,
    /// The client truncating (its identity as the journal's owner).
    #[arg(long, env = "PAROSCTL_OWNER", default_value = "1")]
    owner: u64,
    /// Override: truncate under this generation instead of claiming.
    #[arg(long)]
    generation: Option<u64>,
}

/// `parosctl truncate`: become the writer exactly as `parosctl write` does
/// (a claim, or `--generation`), then ask the leader to raise the journal's
/// floor under that fence (#228). A stale owner is refused and says so.
pub async fn truncate(client: &ParosClient, out: &Printer, args: TruncateArgs) -> Ending {
    let journal = JournalId(args.journal);
    let identity = Identity {
        journal: args.journal,
        owner: args.owner,
        generation: args.generation,
        seq: None,
    };
    let mut writer = match become_writer(client, out, &identity).await {
        Ok(writer) => writer,
        Err(ending) => return ending,
    };
    let Some(outcome) = writer.truncate(client, args.up_to, start(client)).await else {
        note("this writer owns no generation of the journal");
        return Ending::Refused;
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
        TruncateOutcome::Refused { state } if writer.owned().is_none() => {
            refused(out, "superseded", &state)
        }
        TruncateOutcome::Refused { state } => refused(out, "refused", &state),
        TruncateOutcome::UnknownJournal => unknown_journal(journal),
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
    /// The journal.
    journal: u64,
    /// The client that should own the journal.
    #[arg(long)]
    owner: u64,
    /// The generation the caller believes current; read from the journal
    /// when absent.
    #[arg(long)]
    expected: Option<u64>,
}

/// `parosctl set-leader`: compare-and-swap the journal's writer — against
/// `--expected`, or against the generation a read finds.
pub async fn set_leader(client: &ParosClient, out: &Printer, args: SetLeaderArgs) -> Ending {
    let journal = JournalId(args.journal);
    let outcome = match args.expected {
        Some(expected) => client
            .set_leader(journal, expected, args.owner, start(client))
            .await
            .into(),
        None => client.claim(journal, args.owner, start(client), true).await,
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
        ClaimOutcome::UnknownJournal => unknown_journal(journal),
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
    /// The journal to inspect (0: each node's first journal).
    #[arg(long, default_value = "0")]
    journal: u64,
}

fn ballot_text(ballot: Option<Ballot>) -> String {
    ballot.map_or_else(|| "none".to_string(), |b| format!("{}.{}", b.round, b.node))
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

/// `parosctl inspect`: every server's view, in server order.
pub async fn inspect(client: &ParosClient, out: &Printer, args: InspectArgs) -> Ending {
    let mut answered = false;
    for server in 0..client.server_count() {
        let id = client.id_of(server);
        let Some(reply) = client.inspect(server, args.journal).await else {
            out.emit(
                || format!("node {id}: no answer"),
                || json!({ "node": id, "answered": false }),
            );
            continue;
        };
        answered = true;
        let state = journal_state_from_proto(reply.journal).ok();
        out.emit(
            || {
                format!(
                    "node {id}: leader={} ballot={} members={:?} quorum={} chosen_index={} \
                     first_slot={} folded={} gc_watermark={} retirable={:?} \
                     matchmakers={:?}@{}{}",
                    reply.leader,
                    ballot_text(reply.config_ballot),
                    reply.members,
                    quorum_text(&reply),
                    reply
                        .chosen_index
                        .map_or_else(|| "none".to_string(), |c| c.to_string()),
                    reply.first_slot,
                    reply.folded,
                    ballot_text(reply.gc_watermark),
                    reply.retirable,
                    reply.matchmakers,
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

/// `parosctl reconfigure`.
#[derive(Args, Debug)]
pub struct ReconfigureArgs {
    /// The new acceptor set, comma-separated node ids.
    #[arg(long, value_delimiter = ',', required = true)]
    members: Vec<u64>,
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
    match client
        .reconfigure(&args.members, args.quorum, start(client))
        .await
    {
        ReconfigureOutcome::Started { round, leader } => {
            out.emit(
                || {
                    format!(
                        "started round={round} leader={}",
                        leader.map_or_else(|| "none".to_string(), |l| l.to_string())
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
    /// The node to retire (its id, which must be in `--servers`).
    #[arg(long)]
    node: u64,
    /// The GC watermark, `ROUND.NODE`; read from the leader's `inspect`
    /// when absent.
    #[arg(long, value_parser = parse_ballot)]
    gc_watermark: Option<Ballot>,
}

fn parse_ballot(s: &str) -> Result<Ballot, String> {
    let (round, node) = s
        .split_once('.')
        .ok_or_else(|| format!("expected ROUND.NODE, got {s:?}"))?;
    Ok(Ballot {
        round: round.parse().map_err(|e| format!("bad round: {e}"))?,
        node: node.parse().map_err(|e| format!("bad node: {e}"))?,
    })
}

/// The effective GC watermark a leader reports, read from every server.
async fn leader_watermark(client: &ParosClient) -> Option<Ballot> {
    for server in 0..client.server_count() {
        if let Some(reply) = client.inspect(server, 0).await
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
    let Some(target) = client.index_of(args.node) else {
        note(&format!("node {} is not in --servers", args.node));
        return Ending::Refused;
    };
    let watermark = match args.gc_watermark {
        Some(watermark) => watermark,
        None => {
            if let Some(watermark) = leader_watermark(client).await {
                watermark
            } else {
                note("no leader reports an effective GC watermark: nothing is retirable yet");
                return Ending::Refused;
            }
        }
    };
    let request = RetireRequest {
        gc_watermark: Some(watermark),
    };
    match client.retire(target, request).await {
        RetireOutcome::Retired => {
            out.emit(
                || format!("retired node={}", args.node),
                || json!({ "outcome": "retired", "node": args.node }),
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
