//! `parosctl status [--tenant t] [--cell c]` (#191 (status),
//! `docs/architecture.md` §3.6): how the universe, the cell or one tenant
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
use paros::client::views::{ViewOutcome, ask, request};
use paros::status::{Member, Summary, Word, member, summarize};
use paros::view::Scope;
use paros::wire::view::{self as wire, TenantQuery, UniverseQuery, view_request::Query};
use serde_json::{Value, json};

use crate::Ending;
use crate::output::{Printer, table};
use crate::views::{
    Asker, coordinator_text, indent, journal_label, machine_names, reply_json, state, text,
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
                if m.failure_domain.is_empty() {
                    "-".to_string()
                } else {
                    m.failure_domain.clone()
                },
                state(m.up).to_string(),
                footprint.get(&m.node_id).copied().unwrap_or(0).to_string(),
            ]
        })
        .collect();
    out.emit(
        || {
            format!(
                "tenant:       {} ({})\ncell:         {}\nsurvives:     {}\njournals:     {}\n{}\nmachines:\n{}",
                text(&tenant.name),
                tenant.state,
                text(&reply.cell_name),
                if tenant.survives.is_empty() { "-" } else { &tenant.survives },
                words_text(&summary),
                indent(&table(JOURNAL_HEADERS, &rows)),
                indent(&table(["MACHINE", "DOMAIN", "STATE", "ACCEPTOR SLOTS"], &machine_rows)),
            )
        },
        || {
            let mut doc = reply_json(&reply);
            doc["status"] = summary_json(&summary);
            doc
        },
    );
    Ending::Success
}
