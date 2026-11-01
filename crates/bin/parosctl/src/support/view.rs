//! What the administrative views share (#399): `machine list|show`,
//! `cell list|show`, `tenant list|show` and `roles` (each in its noun's
//! module under `cmd`) are each one `View` request to one cell through
//! `paros::client::views`.
//!
//! The request picks the cell: `--servers` names it (the cell of the
//! servers asked), and `--cell` checks that the servers are that cell's.
//! `tenant show` asks the servers' cell, and a tenant that lives in another
//! cell is asked again at that cell's entry endpoint, which the refusal
//! names (#232 (multiple cells)). `cell list` and `tenant list` ask
//! the cell that hosts the universe tenant.
//!
//! The cell filters every answer by the caller's scope before it sends it;
//! the commands only print it. Until tokens (#245), every caller is an
//! admin and may narrow itself to one tenant with `--as-tenant`.
//!
//! Everything printed names an entity by its name (tables for people);
//! `--json` prints the answer as one document, ids included, for scripts.
//!
//! `machine list|show` also ask every machine of the answer for its `Load`
//! (#424), all at once: the busyness columns (FDB's `cpu`, `machine` and
//! `disk IO` of `status details`). A machine that does not answer in time
//! shows `no metrics`; one that answers without a value shows `-`.

use std::collections::BTreeMap;

use moonpool_core::CompioProviders;
use moonpool_rpc::RpcHandle;
use paros::client::load::{LoadOutcome, ask_all};
use paros::client::views::{ViewOutcome, ask, request};
use paros::model::address::{Address, Names};
use paros::model::view::Scope;
use paros::wire::view::{self as wire, CellQuery, view_request::Query};
use serde_json::{Value, json};

use crate::support::connect::Ending;
use crate::support::output::{note, record_text};

/// Where a view goes: the servers of one cell, the scope and the patience.
pub struct Asker<'a> {
    pub providers: &'a CompioProviders,
    pub rpc: &'a RpcHandle<CompioProviders>,
    pub names: &'a Names,
    pub servers: Vec<Address>,
    pub scope: Scope,
    pub timeout: std::time::Duration,
}

impl Asker<'_> {
    /// Ask `query`; the answer, or how the command ends. A tenant that
    /// lives in another cell is asked again at that cell's entry endpoint,
    /// which the refusal names (#232 (multiple cells)).
    pub(crate) async fn ask(&self, query: Query) -> Result<wire::ViewReply, Ending> {
        self.ask_followed(query).await.map(|(reply, _)| reply)
    }

    /// Ask `query` as [`Asker::ask`] does; the answer, and the servers of
    /// the cell that answered it.
    pub(crate) async fn ask_followed(
        &self,
        query: Query,
    ) -> Result<(wire::ViewReply, Vec<Address>), Ending> {
        match self.ask_at(&self.servers, query.clone()).await? {
            Ok(reply) => Ok((reply, self.servers.clone())),
            Err(refused) if refused.refusal == "other_cell" && !refused.endpoint.is_empty() => {
                let endpoint: Vec<Address> = refused
                    .endpoint
                    .iter()
                    .filter_map(|addr| Address::parse(addr).ok())
                    .collect();
                match self.ask_at(&endpoint, query).await? {
                    Ok(reply) => Ok((reply, endpoint)),
                    Err(refused) => {
                        note(&refusal_text(&refused));
                        Err(Ending::Refused)
                    }
                }
            }
            Err(refused) => {
                note(&refusal_text(&refused));
                Err(Ending::Refused)
            }
        }
    }

    /// Ask `query` of `servers`: the answer, the refusal, or how the command
    /// ends when none answers.
    pub(crate) async fn ask_at(
        &self,
        servers: &[Address],
        query: Query,
    ) -> Result<Result<wire::ViewReply, wire::ViewReply>, Ending> {
        let request = request(&self.scope, query);
        match ask(
            self.providers,
            self.rpc,
            self.names,
            servers,
            0,
            &request,
            self.timeout,
        )
        .await
        {
            ViewOutcome::Answered(reply) => Ok(Ok(sorted(reply))),
            ViewOutcome::Refused(reply) => Ok(Err(reply)),
            ViewOutcome::Unreachable => {
                note(
                    "no server answered the view: is the cell formed, and are these its founding members?",
                );
                Err(Ending::Unreachable)
            }
        }
    }

    /// Every machine of `reply` that has an address, asked for its `Load`.
    pub(crate) async fn loads(&self, reply: &wire::ViewReply) -> BTreeMap<u64, LoadOutcome> {
        let machines: Vec<(u64, Address)> = reply
            .machines
            .iter()
            .filter_map(|m| Address::parse(&m.addr).ok().map(|addr| (m.node_id, addr)))
            .collect();
        ask_all(
            self.providers,
            self.rpc,
            self.names,
            &machines,
            self.timeout,
        )
        .await
    }

    /// The cell view, checked to be cell `cell` when one is named.
    pub(crate) async fn cell(&self, cell: Option<&str>) -> Result<wire::ViewReply, Ending> {
        let reply = self.ask(Query::Cell(CellQuery {})).await?;
        if let Some(cell) = cell
            && reply.cell_name != cell.as_bytes()
        {
            note(&format!(
                "the servers are cell {}, not {cell}: pass --servers of cell {cell}",
                text(&reply.cell_name)
            ));
            return Err(Ending::Refused);
        }
        Ok(reply)
    }
}

/// `reply` with its machines, tenants and cells in name order, as people
/// read a table; a machine with no name sorts by its address.
fn sorted(mut reply: wire::ViewReply) -> wire::ViewReply {
    reply.machines.sort_by(|a, b| {
        let key = |m: &wire::MachineView| (m.name.is_empty(), m.name.clone(), m.addr.clone());
        key(a).cmp(&key(b))
    });
    reply.tenants.sort_by(|a, b| a.name.cmp(&b.name));
    reply.cells.sort_by(|a, b| a.name.cmp(&b.name));
    reply
}

/// A refusal in words.
pub fn refusal_text(reply: &wire::ViewReply) -> String {
    match reply.refusal.as_str() {
        "forbidden" => "forbidden: the scope does not cover this view".to_string(),
        "unknown_tenant" => "no tenant of that name lives in this cell".to_string(),
        "other_cell" => format!(
            "the tenant lives in cell {}: pass --servers of that cell",
            text(&reply.cell_name)
        ),
        "not_universe" => {
            "this cell does not host the universe directory: ask the cell that does".to_string()
        }
        "malformed" => "the cell found the request malformed".to_string(),
        other => format!("refused: {other}"),
    }
}

/// A name as text, `-` when empty.
pub fn text(name: &[u8]) -> String {
    if name.is_empty() {
        "-".to_string()
    } else {
        record_text(name)
    }
}

/// The machines of an answer, by id: their names (the address when a
/// machine has none, `-` when nothing names it).
pub fn machine_names(reply: &wire::ViewReply) -> BTreeMap<u64, String> {
    reply
        .machines
        .iter()
        .map(|m| {
            let name = if !m.name.is_empty() {
                m.name.clone()
            } else if !m.addr.is_empty() {
                m.addr.clone()
            } else {
                "-".to_string()
            };
            (m.node_id, name)
        })
        .collect()
}

/// The machine named `name` in `reply`; refused when none or several are.
pub fn find_machine<'a>(
    reply: &'a wire::ViewReply,
    name: &str,
) -> Result<&'a wire::MachineView, Ending> {
    let found: Vec<&wire::MachineView> = reply
        .machines
        .iter()
        .filter(|m| m.name == name || (m.name.is_empty() && m.addr == name))
        .collect();
    // A wiped machine comes back as a new one, often under the same name:
    // the one not retired is meant.
    let live: Vec<&wire::MachineView> = found
        .iter()
        .copied()
        .filter(|m| m.standing != "retired")
        .collect();
    match (found.as_slice(), live.as_slice()) {
        ([], _) => {
            note(&format!(
                "no machine named {name} in cell {}",
                text(&reply.cell_name)
            ));
            Err(Ending::Refused)
        }
        ([one], _) | (_, [one]) => Ok(one),
        _ => {
            note(&format!(
                "{} machines are named {name}: rename one (PAROS_NAME), or use --json to tell them apart",
                live.len()
            ));
            Err(Ending::Refused)
        }
    }
}

/// `up`, `down`, or `slow` for a machine the cell coordinator holds slow
/// (#361 (slow machines)).
pub fn state(machine: &paros::wire::view::MachineView) -> &'static str {
    match (machine.up, machine.slow) {
        (false, _) => "down",
        (true, true) => "slow",
        (true, false) => "up",
    }
}

/// A machine's state: `up`, `slow` (#361 (slow machines)) or `down`, and
/// `in maintenance` while a maintenance window over it or its zone is open
/// (#359).
pub fn machine_state(m: &wire::MachineView) -> String {
    if m.maintenance_ms > 0 {
        format!("{}, in maintenance", state(m))
    } else {
        state(m).to_string()
    }
}

/// `parosctl machine …`.
/// The value of the tag `key` (#575 (key:value tags)), or `-`.
pub fn tag(tags: &[paros::wire::common::Tag], key: &str) -> String {
    paros::model::tags::value_of(tags, key).map_or_else(|| "-".to_string(), str::to_string)
}

/// Every tag as `k:v`, comma-joined, or `-` (#575 (key:value tags)).
pub fn tags_text(tags: &[paros::wire::common::Tag]) -> String {
    if tags.is_empty() {
        "-".to_string()
    } else {
        paros::model::tags::Tags::from_wire(tags.to_vec())
            .map_or_else(|_| "?".to_string(), |t| t.label())
    }
}

/// The user tags only, as `k:v`, comma-joined, or `-`.
pub fn user_tags_text(tags: &[paros::wire::common::Tag]) -> String {
    let user: Vec<paros::wire::common::Tag> = tags
        .iter()
        .filter(|t| !paros::model::tags::is_reserved(&t.key))
        .cloned()
        .collect();
    tags_text(&user)
}

/// The selector `-l` gives, empty without one; else the command's end
/// (#575 (key:value tags)).
pub fn selector(text: Option<&str>) -> Result<paros::model::tags::Selector, Ending> {
    text.map(str::parse::<paros::model::tags::Selector>)
        .transpose()
        .map(Option::unwrap_or_default)
        .map_err(|error| {
            note(&format!("bad selector: {error}"));
            Ending::Refused
        })
}

/// Whether `tags` (a view's) match `selector`. Tags a view cannot parse
/// match no term.
pub fn selected(
    selector: &paros::model::tags::Selector,
    tags: &[paros::wire::common::Tag],
) -> bool {
    selector.is_empty()
        || paros::model::tags::Tags::from_wire(tags.to_vec())
            .is_ok_and(|tags| selector.matches(&tags, None))
}

/// Tags as a JSON object, key to value.
pub fn tags_json(tags: &[paros::wire::common::Tag]) -> Value {
    Value::Object(
        tags.iter()
            .map(|t| (t.key.clone(), Value::String(t.value.clone())))
            .collect(),
    )
}

/// Every line of `text`, indented two spaces.
pub fn indent(text: &str) -> String {
    text.lines()
        .map(|line| format!("  {line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The coordinator in words.
pub fn coordinator_text(reply: &wire::ViewReply, names: &BTreeMap<u64, String>) -> String {
    if reply.coordinator == 0 {
        return "none".to_string();
    }
    format!(
        "{} (term {})",
        names
            .get(&reply.coordinator)
            .cloned()
            .unwrap_or_else(|| "-".to_string()),
        reply.coordinator_term
    )
}

/// A journal's label: its name, or its kind for one the cell made.
pub fn journal_label(journal: &wire::JournalView) -> String {
    if journal.name.is_empty() {
        format!("({})", journal.kind)
    } else {
        record_text(&journal.name)
    }
}

/// A machine as JSON.
pub fn machine_json(m: &wire::MachineView) -> Value {
    json!({
        "node_id": m.node_id,
        "name": m.name,
        "addr": m.addr,
        "tags": tags_json(&m.tags),
        "booked": m.booked,
        "standing": m.standing,
        "up": m.up,
        "slow": m.slow,
        "maintenance_ms": m.maintenance_ms,
        "founder": m.founder,
        "incarnation": format!("{:016x}{:016x}", m.incarnation_high, m.incarnation_low),
        "bookings": m.bookings.iter().map(|b| json!({
            "booking": b.booking,
            "role": b.role,
            "tenant": b.tenant,
            "journal": b.journal,
            "set": b.set,
        })).collect::<Vec<_>>(),
    })
}

/// A whole answer as one JSON document: ids and names both.
pub fn reply_json(reply: &wire::ViewReply) -> Value {
    json!({
        "cell": { "id": reply.cell_id, "name": record_text(&reply.cell_name), "state": reply.cell_state },
        "universe": record_text(&reply.universe_name),
        "registry_at": reply.registry_at,
        "directory_at": reply.directory_at,
        "answered_by": reply.answered_by,
        "coordinator": { "node_id": reply.coordinator, "term": reply.coordinator_term },
        "machines": reply.machines.iter().map(machine_json).collect::<Vec<_>>(),
        "tenants": reply.tenants.iter().map(|t| json!({
            "tenant": t.tenant,
            "name": record_text(&t.name),
            "kind": t.kind,
            "state": t.state,
            "tags": tags_json(&t.tags),
            "survives": t.survives,
            "cell": t.cell_id,
            "journals": t.journals.iter().map(|j| json!({
                "journal": j.journal,
                "name": record_text(&j.name),
                "kind": j.kind,
                "writer": j.writer,
                "store_durability": j.store_durability,
                "desired": j.desired,
                "acceptors": j.acceptors,
                "matchmakers": j.matchmakers,
                "desired_acceptors": j.desired_acceptors,
                "observed": j.observed,
                "word": j.word,
                "tags": tags_json(&j.tags),
            })).collect::<Vec<_>>(),
            "scrub": t.scrub.as_ref().map(|s| json!({
                "pass": s.pass,
                "done": s.done,
                "late": s.late,
                "journal": s.journal,
                "next_slot": s.next_slot,
                "checked": s.checked,
                "damaged": s.damaged,
                "repaired": s.repaired,
                "mismatched": s.mismatched,
                "damaged_total": s.damaged_total,
                "repaired_total": s.repaired_total,
                "mismatched_total": s.mismatched_total,
            })),
        })).collect::<Vec<_>>(),
        "cells": reply.cells.iter().map(|c| json!({
            "cell": c.cell_id,
            "name": record_text(&c.name),
            "state": c.state,
            "tenants": c.tenants,
            "tags": tags_json(&c.tags),
        })).collect::<Vec<_>>(),
    })
}
