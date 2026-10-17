//! `parosctl status [--tenant t] [--cell c]` (#191 (status),
//! `docs/architecture/03-6-status.md` §3.6): how the universe, the cell or one tenant
//! stands, in the spirit of `fdbcli status`.
//!
//! Three columns per journal: **desired** (the durability the tenant asked
//! for, its partition count and acceptor count, #523), **current** (the acceptors placed) and
//! **available** (the placed acceptors that are up and not leaving), with
//! the word the cell shows (healthy, degraded, unavailable). A word the cell
//! holds back (hysteresis) shows the word it judged now beside it.
//!
//! Everything comes from one or two views (`paros::client::views`); the
//! words and the counts are computed by the cell and by
//! `paros::status::summarize`, never here. Names only: hex ids are for
//! `--json`.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use clap::Args;
use paros::client::load::LoadOutcome;
use paros::client::views::{ViewOutcome, ask, request};
use paros::machine::load::usage_from_wire;
use paros::metrics::{CallKind, JournalUsage};
use paros::status::{Member, Summary, Word, member, summarize};
use paros::view::Scope;
use paros::wire::view::{self as wire, TenantQuery, UniverseQuery, view_request::Query};
use serde_json::{Value, json};

use crate::Ending;
use crate::output::{Printer, table};
use crate::views::{
    Asker, coordinator_text, indent, journal_label, machine_names, machine_state, reply_json, text,
};

/// `parosctl status`.
#[derive(Args, Debug)]
pub struct StatusArgs {
    /// One tenant: its journals and the machines they use.
    #[arg(long, conflicts_with = "cell")]
    tenant: Option<String>,
    /// The cell: refused unless the servers are its machines.
    #[arg(long)]
    cell: Option<String>,
}

/// `parosctl status`.
pub async fn run(asker: &Asker<'_>, out: &Printer, args: StatusArgs) -> Ending {
    match args.tenant {
        Some(tenant) => tenant_status(asker, out, &tenant).await,
        None => cell_status(asker, out, args.cell.as_deref()).await,
    }
}

/// The universe view when the servers' cell hosts the universe directory
/// and the scope may see it; `None` otherwise, and nothing is printed.
async fn universe(asker: &Asker<'_>) -> Option<wire::ViewReply> {
    if asker.scope != Scope::Admin {
        return None;
    }
    match ask(
        asker.providers,
        asker.rpc,
        asker.names,
        &asker.servers,
        0,
        &request(&asker.scope, Query::Universe(UniverseQuery {})),
        asker.timeout,
    )
    .await
    {
        ViewOutcome::Answered(reply) => Some(reply),
        ViewOutcome::Refused(_) | ViewOutcome::Unreachable => None,
    }
}

/// One journal's row: tenant, journal, desired, current, available, word.
fn journal_row(
    reply: &wire::ViewReply,
    tenant: &wire::TenantView,
    journal: &wire::JournalView,
) -> [String; 6] {
    let available = journal
        .acceptors
        .iter()
        .filter(|id| member(reply.machines.iter().find(|m| m.node_id == **id)) == Member::Serving)
        .count();
    [
        text(&tenant.name),
        journal_label(journal),
        journal.desired.clone(),
        journal.acceptors.len().to_string(),
        available.to_string(),
        word_text(journal),
    ]
}

/// The word the cell shows, and the word it judged now when it holds that
/// one back.
fn word_text(journal: &wire::JournalView) -> String {
    match (journal.word.as_str(), journal.observed.as_str()) {
        ("", _) => "-".to_string(),
        (word, observed) if word == observed || observed.is_empty() => word.to_string(),
        (word, observed) => format!("{word} (now {observed})"),
    }
}

/// The journals per word, in words.
fn words_text(summary: &Summary) -> String {
    let parts: Vec<String> = [Word::Healthy, Word::Degraded, Word::Unavailable]
        .into_iter()
        .map(|w| {
            format!(
                "{} {}",
                summary.words.get(&w).copied().unwrap_or(0),
                w.as_str()
            )
        })
        .collect();
    parts.join(", ")
}

/// The summary as JSON.
fn summary_json(summary: &Summary) -> Value {
    let slots: serde_json::Map<String, Value> = summary
        .slots
        .iter()
        .map(|(class, (booked, total))| {
            let slots = json!({
                "booked": booked,
                "free": total.saturating_sub(*booked),
                "total": total,
            });
            (class.clone(), slots)
        })
        .collect();
    let words: serde_json::Map<String, Value> = [Word::Healthy, Word::Degraded, Word::Unavailable]
        .into_iter()
        .map(|w| {
            let count = summary.words.get(&w).copied().unwrap_or(0);
            (w.as_str().to_string(), json!(count))
        })
        .collect();
    json!({
        "machines": summary.machines,
        "available": summary.available,
        "slots": slots,
        "words": words,
    })
}

const JOURNAL_HEADERS: [&str; 6] = [
    "TENANT",
    "JOURNAL",
    "DESIRED",
    "CURRENT",
    "AVAILABLE",
    "WORD",
];

/// `parosctl status [--cell c]`: the universe (when this cell hosts its
/// directory), the cell and every journal it holds.
async fn cell_status(asker: &Asker<'_>, out: &Printer, cell: Option<&str>) -> Ending {
    let reply = match asker.cell(cell).await {
        Ok(reply) => reply,
        Err(ending) => return ending,
    };
    let universe = universe(asker).await;
    let summary = summarize(&reply);
    let names = machine_names(&reply);
    let rows: Vec<[String; 6]> = reply
        .tenants
        .iter()
        .flat_map(|t| t.journals.iter().map(move |j| (t, j)))
        .map(|(t, j)| journal_row(&reply, t, j))
        .collect();
    let slot_rows: Vec<[String; 4]> = summary
        .slots
        .iter()
        .map(|(class, (booked, total))| {
            [
                class.clone(),
                booked.to_string(),
                total.saturating_sub(*booked).to_string(),
                total.to_string(),
            ]
        })
        .collect();
    out.emit(
        || {
            let mut out = String::new();
            if let Some(universe) = &universe {
                let users = universe.tenants.iter().filter(|t| t.kind == "users").count();
                let _ = writeln!(
                    out,
                    "universe:     {} ({} cells, {users} tenants)",
                    text(&universe.universe_name),
                    universe.cells.len(),
                );
            }
            let _ = write!(
                out,
                "cell:         {} ({})\ncoordinator:  {}\nmachines:     {} available of {}\nslots:\n{}\njournals:     {}\n{}",
                text(&reply.cell_name),
                if reply.cell_state.is_empty() { "-" } else { &reply.cell_state },
                coordinator_text(&reply, &names),
                summary.available,
                summary.machines,
                indent(&table(["CLASS", "BOOKED", "FREE", "TOTAL"], &slot_rows)),
                words_text(&summary),
                indent(&table(JOURNAL_HEADERS, &rows)),
            );
            out
        },
        || {
            let mut doc = reply_json(&reply);
            doc["status"] = summary_json(&summary);
            if let Some(universe) = &universe {
                doc["universe_view"] = reply_json(universe);
            }
            doc
        },
    );
    Ending::Success
}

/// `parosctl status --tenant t`: the tenant's journals and the machines
/// they use.
async fn tenant_status(asker: &Asker<'_>, out: &Printer, name: &str) -> Ending {
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
        crate::output::note("the cell answered no tenant");
        return Ending::Refused;
    };
    let summary = summarize(&reply);
    let names = machine_names(&reply);
    let usage = Usage::of(tenant, &asker.loads(&reply).await);
    let rows: Vec<[String; 6]> = tenant
        .journals
        .iter()
        .map(|j| journal_row(&reply, tenant, j))
        .collect();
    let mut footprint: BTreeMap<u64, usize> = BTreeMap::new();
    for id in tenant.journals.iter().flat_map(|j| &j.acceptors) {
        *footprint.entry(*id).or_default() += 1;
    }
    let machine_rows: Vec<[String; 4]> = reply
        .machines
        .iter()
        .map(|m| {
            [
                names[&m.node_id].clone(),
                crate::views::tag(&m.tags, paros::tags::reserved::ZONE),
                machine_state(m),
                footprint.get(&m.node_id).copied().unwrap_or(0).to_string(),
            ]
        })
        .collect();
    out.emit(
        || {
            format!(
                "tenant:       {} ({})\ncell:         {}\nsurvives:     {}\njournals:     {}\nscrub:        {}\n{}\nmachines:\n{}\n{}",
                text(&tenant.name),
                tenant.state,
                text(&reply.cell_name),
                if tenant.survives.is_empty() { "-" } else { &tenant.survives },
                words_text(&summary),
                scrub_text(tenant.scrub.as_ref()),
                indent(&table(JOURNAL_HEADERS, &rows)),
                indent(&table(["MACHINE", "DOMAIN", "STATE", "ACCEPTOR SLOTS"], &machine_rows)),
                usage.text(tenant),
            )
        },
        || {
            let mut doc = reply_json(&reply);
            doc["status"] = summary_json(&summary);
            doc["usage"] = usage.json(tenant);
            doc
        },
    );
    Ending::Success
}

/// The tenant scrubber's progress (#362 (background scrubbing)): the pass,
/// where it stands, this pass's findings and the findings over every pass.
fn scrub_text(scrub: Option<&wire::ScrubView>) -> String {
    let Some(scrub) = scrub else {
        return "no pass yet".to_string();
    };
    let at = if scrub.done {
        "done".to_string()
    } else if scrub.journal == 0 {
        "starting".to_string()
    } else {
        format!("at journal {:x} slot {}", scrub.journal, scrub.next_slot)
    };
    let late = if scrub.late { ", late" } else { "" };
    format!(
        "pass {} {at}{late}: {} records checked, {} damaged, {} repaired, {} mismatched; \
         {} damaged, {} repaired, {} mismatched in all",
        scrub.pass,
        scrub.checked,
        scrub.damaged,
        scrub.repaired,
        scrub.mismatched,
        scrub.damaged_total,
        scrub.repaired_total,
        scrub.mismatched_total,
    )
}

/// A tenant's usage (#366 (metrics)): what the machines that answered
/// `Load` counted per journal of the tenant, summed over the machines.
/// Each machine counts since its process started; a machine that did not
/// answer, or records no metrics, adds nothing and is counted out.
struct Usage {
    per_journal: BTreeMap<u64, JournalUsage>,
    /// The machines asked, and those that answered with metrics.
    asked: usize,
    metered: usize,
}

impl Usage {
    fn of(tenant: &wire::TenantView, loads: &BTreeMap<u64, LoadOutcome>) -> Self {
        let mut per_journal: BTreeMap<u64, JournalUsage> = BTreeMap::new();
        let mut metered = 0;
        for outcome in loads.values() {
            let LoadOutcome::Answered(ack) = outcome else {
                continue;
            };
            if !ack.metered {
                continue;
            }
            metered += 1;
            for row in ack.usage.iter().filter(|row| row.tenant == tenant.tenant) {
                let usage = usage_from_wire(row);
                per_journal
                    .entry(row.journal)
                    .and_modify(|sum| sum.add(&usage))
                    .or_insert(usage);
            }
        }
        Self {
            per_journal,
            asked: loads.len(),
            metered,
        }
    }

    /// The tenant's total, over every journal.
    fn total(&self) -> [u64; 6] {
        self.per_journal.values().fold([0; 6], |mut sum, usage| {
            for (at, call) in CallKind::ALL.into_iter().enumerate() {
                sum[at] += usage.calls_of(call);
            }
            sum[4] += usage.written_bytes;
            sum[5] += usage.read_bytes;
            sum
        })
    }

    fn text(&self, tenant: &wire::TenantView) -> String {
        let header = format!(
            "usage (since each machine started, {} of {} machines metered):",
            self.metered, self.asked
        );
        if self.metered == 0 {
            return format!("{header} no metrics");
        }
        let name_of = |id: u64| {
            tenant
                .journals
                .iter()
                .find(|j| j.journal == id)
                .map_or_else(|| crate::output::short(id), journal_label)
        };
        let row = |name: String, counts: [u64; 6]| {
            [
                name,
                counts[0].to_string(),
                counts[1].to_string(),
                counts[2].to_string(),
                counts[3].to_string(),
                bytes(counts[4]),
                bytes(counts[5]),
            ]
        };
        let mut rows: Vec<[String; 7]> = self
            .per_journal
            .iter()
            .map(|(id, usage)| {
                row(
                    name_of(*id),
                    [
                        usage.calls_of(CallKind::Write),
                        usage.calls_of(CallKind::Read),
                        usage.calls_of(CallKind::SetLeader),
                        usage.calls_of(CallKind::Truncate),
                        usage.written_bytes,
                        usage.read_bytes,
                    ],
                )
            })
            .collect();
        rows.push(row("(total)".to_string(), self.total()));
        format!(
            "{header}\n{}",
            indent(&table(
                [
                    "JOURNAL",
                    "WRITES",
                    "READS",
                    "SET-LEADER",
                    "TRUNCATE",
                    "WRITTEN",
                    "READ"
                ],
                &rows
            ))
        )
    }

    fn json(&self, tenant: &wire::TenantView) -> Value {
        let [writes, reads, set_leaders, truncates, written, read] = self.total();
        json!({
            "machines": self.asked,
            "metered": self.metered,
            "calls": {
                "write": writes,
                "read": reads,
                "set_leader": set_leaders,
                "truncate": truncates,
            },
            "written_bytes": written,
            "read_bytes": read,
            "journals": self.per_journal.iter().map(|(id, usage)| json!({
                "journal": id,
                "name": tenant.journals.iter().find(|j| j.journal == *id).map(|j| text(&j.name)),
                "calls": {
                    "write": usage.calls_of(CallKind::Write),
                    "read": usage.calls_of(CallKind::Read),
                    "set_leader": usage.calls_of(CallKind::SetLeader),
                    "truncate": usage.calls_of(CallKind::Truncate),
                },
                "written_bytes": usage.written_bytes,
                "read_bytes": usage.read_bytes,
            })).collect::<Vec<_>>(),
        })
    }
}

/// A byte count for people.
fn bytes(count: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    let mut value = count;
    let mut unit = 0;
    while value >= 10 * 1024 && unit + 1 < UNITS.len() {
        value /= 1024;
        unit += 1;
    }
    format!("{value} {}", UNITS[unit])
}
