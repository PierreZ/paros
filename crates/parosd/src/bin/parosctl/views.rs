//! The administrative views (#399): `machine list|show`, `cell list|show`,
//! `tenant list|show` and `roles`, each one `View` request to one cell
//! through `paros::client::views`.
//!
//! The request picks the cell: `--servers` names it (the cell of the
//! servers asked), and `--cell` checks that the servers are that cell's.
//! `tenant show` asks the servers' cell, and a tenant that lives in another
//! cell is refused with that cell's name. `cell list` and `tenant list` ask
//! the cell that hosts the universe tenant.
//!
//! The cell filters every answer by the caller's scope before it sends it;
//! this module only prints it. Until tokens (#245), every caller is an
//! admin and may narrow itself to one tenant with `--as-tenant`.
//!
//! Everything printed names an entity by its name (tables for people);
//! `--json` prints the answer as one document, ids included, for scripts.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use clap::{Args, Subcommand};
use moonpool_core::TokioProviders;
use moonpool_rpc::RpcHandle;
use paros::client::views::{ViewOutcome, ask, request};
use paros::view::Scope;
use paros::wire::view::{self as wire, CellQuery, TenantQuery, UniverseQuery, view_request::Query};
use paros::{Address, Names};
use serde_json::{Value, json};

use crate::Ending;
use crate::output::{Printer, note, record_text, table};

/// Where a view goes: the servers of one cell, the scope and the patience.
pub struct Asker<'a> {
    pub providers: &'a TokioProviders,
    pub rpc: &'a RpcHandle<TokioProviders>,
    pub names: &'a Names,
    pub servers: Vec<Address>,
    pub scope: Scope,
    pub timeout: std::time::Duration,
}

impl Asker<'_> {
    /// Ask `query`; the answer, or how the command ends.
    async fn ask(&self, query: Query) -> Result<wire::ViewReply, Ending> {
        let request = request(&self.scope, query);
        match ask(
            self.providers,
            self.rpc,
            self.names,
            &self.servers,
            0,
            &request,
            self.timeout,
        )
        .await
        {
            ViewOutcome::Answered(reply) => Ok(reply),
            ViewOutcome::Refused(reply) => {
                note(&refusal_text(&reply));
                Err(Ending::Refused)
            }
            ViewOutcome::Unreachable => {
                note(
                    "no server answered the view: is the cell formed, and are these its founding members?",
                );
                Err(Ending::Unreachable)
            }
        }
    }

    /// The cell view, checked to be cell `cell` when one is named.
    async fn cell(&self, cell: Option<&str>) -> Result<wire::ViewReply, Ending> {
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

/// A refusal in words.
fn refusal_text(reply: &wire::ViewReply) -> String {
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
fn text(name: &[u8]) -> String {
    if name.is_empty() {
        "-".to_string()
    } else {
        record_text(name)
    }
}

/// `parosctl machine`.
#[derive(Args, Debug)]
pub struct MachineArgs {
    #[command(subcommand)]
    command: MachineCommand,
}

#[derive(Subcommand, Debug)]
enum MachineCommand {
    /// List the cell's machines: name, address, class, slots booked of
    /// total, failure domain, standing, up or down.
    List {
        /// The cell: refused unless the servers are its machines.
        #[arg(long)]
        cell: Option<String>,
    },
    /// Show one machine: its details, its bookings and the journals it
    /// serves.
    Show {
        /// The machine's name.
        machine: String,
    },
}

/// `parosctl roles`: who holds which role now.
#[derive(Args, Debug)]
pub struct RolesArgs {
    /// Every role of this cell (the servers' cell by default).
    #[arg(long, conflicts_with_all = ["tenant", "machine"])]
    cell: Option<String>,
    /// The roles that serve this tenant.
    #[arg(long, conflicts_with = "machine")]
    tenant: Option<String>,
    /// The roles this machine holds.
    #[arg(long)]
    machine: Option<String>,
}

/// The machines of an answer, by id: their names (the address when a
/// machine has none, `-` when nothing names it).
fn machine_names(reply: &wire::ViewReply) -> BTreeMap<u64, String> {
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
fn find_machine<'a>(
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

/// `up` or `down`.
fn state(up: bool) -> &'static str {
    if up { "up" } else { "down" }
}

/// `parosctl machine …`.
pub async fn machine(asker: &Asker<'_>, out: &Printer, args: MachineArgs) -> Ending {
    match args.command {
        MachineCommand::List { cell } => {
            let reply = match asker.cell(cell.as_deref()).await {
                Ok(reply) => reply,
                Err(ending) => return ending,
            };
            let rows: Vec<[String; 7]> = reply
                .machines
                .iter()
                .map(|m| {
                    [
                        machine_names(&reply)[&m.node_id].clone(),
                        m.addr.clone(),
                        m.class.clone(),
                        format!("{}/{}", m.booked, m.capacity),
                        if m.failure_domain.is_empty() {
                            "-".to_string()
                        } else {
                            m.failure_domain.clone()
                        },
                        m.standing.clone(),
                        state(m.up).to_string(),
                    ]
                })
                .collect();
            out.emit(
                || {
                    table(
                        [
                            "NAME", "ADDRESS", "CLASS", "SLOTS", "DOMAIN", "STANDING", "STATE",
                        ],
                        &rows,
                    )
                },
                || reply_json(&reply),
            );
            Ending::Success
        }
        MachineCommand::Show { machine } => {
            let reply = match asker.cell(None).await {
                Ok(reply) => reply,
                Err(ending) => return ending,
            };
            let m = match find_machine(&reply, &machine) {
                Ok(m) => m,
                Err(ending) => return ending,
            };
            let roles: Vec<[String; 4]> = roles(&reply)
                .into_iter()
                .filter(|r| r.holder == m.node_id)
                .map(|r| r.row(&reply))
                .collect();
            out.emit(
                || {
                    let mut text = format!(
                        "name:            {}\naddress:         {}\nclass:           {}\nslots:           {} booked of {}\nfailure domain:  {}\nstanding:        {}\nstate:           {}\nfounding member: {}\ncell:            {}",
                        machine_names(&reply)[&m.node_id],
                        m.addr,
                        m.class,
                        m.booked,
                        m.capacity,
                        if m.failure_domain.is_empty() { "-" } else { &m.failure_domain },
                        m.standing,
                        state(m.up),
                        if m.founder { "yes" } else { "no" },
                        self::text(&reply.cell_name),
                    );
                    let _ = write!(
                        text,
                        "\nroles:\n{}",
                        indent(&table(["ROLE", "HOLDER", "TENANT", "JOURNAL"], &roles))
                    );
                    text
                },
                || {
                    let mut doc = reply_json(&reply);
                    doc["machine"] = machine_json(m);
                    doc
                },
            );
            Ending::Success
        }
    }
}

/// Every line of `text`, indented two spaces.
fn indent(text: &str) -> String {
    text.lines()
        .map(|line| format!("  {line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// `parosctl cell list`: the universe's cells.
pub async fn cell_list(asker: &Asker<'_>, out: &Printer) -> Ending {
    let reply = match asker.ask(Query::Universe(UniverseQuery {})).await {
        Ok(reply) => reply,
        Err(ending) => return ending,
    };
    let rows: Vec<[String; 3]> = reply
        .cells
        .iter()
        .map(|c| [text(&c.name), c.state.clone(), c.tenants.to_string()])
        .collect();
    out.emit(
        || {
            format!(
                "universe {}\n{}",
                text(&reply.universe_name),
                table(["CELL", "STATE", "TENANTS"], &rows)
            )
        },
        || reply_json(&reply),
    );
    Ending::Success
}

/// `parosctl tenant list`: the universe's tenants, the internal ones (the
/// universe tenant, the cell tenants) included.
pub async fn tenant_list(asker: &Asker<'_>, out: &Printer) -> Ending {
    let reply = match asker.ask(Query::Universe(UniverseQuery {})).await {
        Ok(reply) => reply,
        Err(ending) => return ending,
    };
    let cells: BTreeMap<u64, String> = reply
        .cells
        .iter()
        .map(|c| (c.cell_id, text(&c.name)))
        .collect();
    let rows: Vec<[String; 6]> = reply
        .tenants
        .iter()
        .map(|t| {
            [
                text(&t.name),
                t.kind.clone(),
                cells
                    .get(&t.cell_id)
                    .cloned()
                    .unwrap_or_else(|| "-".to_string()),
                t.state.clone(),
                t.groups.clone(),
                t.survives.clone(),
            ]
        })
        .collect();
    out.emit(
        || {
            table(
                ["TENANT", "KIND", "CELL", "STATE", "GROUPS", "SURVIVES"],
                &rows,
            )
        },
        || reply_json(&reply),
    );
    Ending::Success
}

/// `parosctl cell show [<cell>]`: the cell's state, its coordinator, its
/// slots per class and its machines by standing.
pub async fn cell_show(asker: &Asker<'_>, out: &Printer, cell: Option<&str>) -> Ending {
    let reply = match asker.cell(cell).await {
        Ok(reply) => reply,
        Err(ending) => return ending,
    };
    let names = machine_names(&reply);
    let mut slots: BTreeMap<&str, (u64, u64)> = BTreeMap::new();
    let mut standing: BTreeMap<&str, usize> = BTreeMap::new();
    for m in &reply.machines {
        if m.standing != "retired" {
            let entry = slots.entry(m.class.as_str()).or_default();
            entry.0 += m.booked;
            entry.1 += m.capacity;
        }
        *standing.entry(m.standing.as_str()).or_default() += 1;
    }
    let down = reply.machines.iter().filter(|m| !m.up).count();
    let users = reply.tenants.iter().filter(|t| t.kind == "users").count();
    let slot_rows: Vec<[String; 4]> = slots
        .iter()
        .map(|(class, (booked, total))| {
            [
                (*class).to_string(),
                booked.to_string(),
                total.saturating_sub(*booked).to_string(),
                total.to_string(),
            ]
        })
        .collect();
    out.emit(
        || {
            format!(
                "cell:         {}\nuniverse:     {}\nstate:        {}\ncoordinator:  {}\nmachines:     {} ({}; {} down)\ntenants:      {users}\nslots:\n{}",
                text(&reply.cell_name),
                text(&reply.universe_name),
                if reply.cell_state.is_empty() { "-" } else { &reply.cell_state },
                coordinator_text(&reply, &names),
                reply.machines.len(),
                standing
                    .iter()
                    .map(|(s, n)| format!("{n} {s}"))
                    .collect::<Vec<_>>()
                    .join(", "),
                down,
                indent(&table(["CLASS", "BOOKED", "FREE", "TOTAL"], &slot_rows)),
            )
        },
        || reply_json(&reply),
    );
    Ending::Success
}

/// The coordinator in words.
fn coordinator_text(reply: &wire::ViewReply, names: &BTreeMap<u64, String>) -> String {
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

/// `parosctl tenant show <tenant>`.
pub async fn tenant_show(asker: &Asker<'_>, out: &Printer, name: &str) -> Ending {
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
        note("the cell answered no tenant");
        return Ending::Refused;
    };
    let names = machine_names(&reply);
    let members = |ids: &[u64]| {
        ids.iter()
            .map(|id| names.get(id).cloned().unwrap_or_else(|| "-".to_string()))
            .collect::<Vec<_>>()
            .join(",")
    };
    let journal_rows: Vec<[String; 5]> = tenant
        .journals
        .iter()
        .map(|j| {
            [
                journal_label(j),
                j.writer.clone(),
                j.desired.clone(),
                members(&j.acceptors),
                if j.matchmakers.is_empty() {
                    "-".to_string()
                } else {
                    members(&j.matchmakers)
                },
            ]
        })
        .collect();
    let mut footprint: BTreeMap<u64, usize> = BTreeMap::new();
    for j in &tenant.journals {
        for id in &j.acceptors {
            *footprint.entry(*id).or_default() += 1;
        }
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
                "tenant:       {}\ncell:         {}\nstate:        {}\ngroups:       {}\nsurvives:     {}\ncoordinator:  {}\njournals:\n{}\nmachines:\n{}",
                text(&tenant.name),
                text(&reply.cell_name),
                tenant.state,
                tenant.groups,
                tenant.survives,
                coordinator_text(&reply, &names),
                indent(&table(["JOURNAL", "WRITER", "DESIRED", "ACCEPTORS", "MATCHMAKERS"], &journal_rows)),
                indent(&table(["MACHINE", "DOMAIN", "STATE", "ACCEPTOR SLOTS"], &machine_rows)),
            )
        },
        || reply_json(&reply),
    );
    Ending::Success
}

/// A journal's label: its name, or its kind for one the cell made.
fn journal_label(journal: &wire::JournalView) -> String {
    if journal.name.is_empty() {
        format!("({})", journal.kind)
    } else {
        record_text(&journal.name)
    }
}

/// One role holder.
struct Holding {
    role: String,
    holder: u64,
    tenant: Vec<u8>,
    journal: String,
}

impl Holding {
    fn row(&self, reply: &wire::ViewReply) -> [String; 4] {
        let names = machine_names(reply);
        [
            self.role.clone(),
            names
                .get(&self.holder)
                .cloned()
                .unwrap_or_else(|| "-".to_string()),
            text(&self.tenant),
            self.journal.clone(),
        ]
    }
}

/// Every role holder an answer names: the coordinator, every journal's
/// acceptors and matchmakers, and every booking.
fn roles(reply: &wire::ViewReply) -> Vec<Holding> {
    let mut roles = Vec::new();
    if reply.coordinator != 0 {
        roles.push(Holding {
            role: "cell-coordinator".to_string(),
            holder: reply.coordinator,
            tenant: Vec::new(),
            journal: format!("term {}", reply.coordinator_term),
        });
        // The cell coordinator is the tenant coordinator of every tenant it
        // hosts until placement (#212, #225).
        for tenant in reply.tenants.iter().filter(|t| t.kind == "users") {
            roles.push(Holding {
                role: "tenant-coordinator".to_string(),
                holder: reply.coordinator,
                tenant: tenant.name.clone(),
                journal: "-".to_string(),
            });
        }
    }
    for tenant in &reply.tenants {
        for journal in &tenant.journals {
            for (role, ids) in [
                ("acceptor", &journal.acceptors),
                ("matchmaker", &journal.matchmakers),
            ] {
                roles.extend(ids.iter().map(|id| Holding {
                    role: role.to_string(),
                    holder: *id,
                    tenant: tenant.name.clone(),
                    journal: journal_label(journal),
                }));
            }
        }
    }
    let tenant_names: BTreeMap<u64, Vec<u8>> = reply
        .tenants
        .iter()
        .map(|t| (t.tenant, t.name.clone()))
        .collect();
    let journal_names: BTreeMap<(u64, u64), String> = reply
        .tenants
        .iter()
        .flat_map(|t| {
            t.journals
                .iter()
                .map(move |j| ((t.tenant, j.journal), journal_label(j)))
        })
        .collect();
    for machine in &reply.machines {
        for booking in &machine.bookings {
            roles.push(Holding {
                role: format!("booked:{}", booking.role),
                holder: machine.node_id,
                tenant: tenant_names
                    .get(&booking.tenant)
                    .cloned()
                    .unwrap_or_default(),
                journal: if booking.journal == 0 {
                    "(matchmaker set)".to_string()
                } else {
                    journal_names
                        .get(&(booking.tenant, booking.journal))
                        .cloned()
                        .unwrap_or_else(|| "-".to_string())
                },
            });
        }
    }
    roles
}

/// `parosctl roles`.
pub async fn roles_cmd(asker: &Asker<'_>, out: &Printer, args: RolesArgs) -> Ending {
    let reply = match &args.tenant {
        Some(tenant) => {
            asker
                .ask(Query::Tenant(TenantQuery {
                    name: tenant.as_bytes().to_vec(),
                }))
                .await
        }
        None => asker.cell(args.cell.as_deref()).await,
    };
    let reply = match reply {
        Ok(reply) => reply,
        Err(ending) => return ending,
    };
    let holder = match &args.machine {
        Some(name) => match find_machine(&reply, name) {
            Ok(m) => Some(m.node_id),
            Err(ending) => return ending,
        },
        None => None,
    };
    let rows: Vec<[String; 4]> = roles(&reply)
        .into_iter()
        .filter(|r| holder.is_none_or(|h| r.holder == h))
        .map(|r| r.row(&reply))
        .collect();
    out.emit(
        || table(["ROLE", "HOLDER", "TENANT", "JOURNAL"], &rows),
        || {
            let mut doc = reply_json(&reply);
            if let Some(holder) = holder {
                doc["holder"] = json!(holder);
            }
            doc
        },
    );
    Ending::Success
}

/// A machine as JSON.
fn machine_json(m: &wire::MachineView) -> Value {
    json!({
        "node_id": m.node_id,
        "name": m.name,
        "addr": m.addr,
        "class": m.class,
        "capacity": m.capacity,
        "booked": m.booked,
        "failure_domain": m.failure_domain,
        "standing": m.standing,
        "up": m.up,
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
fn reply_json(reply: &wire::ViewReply) -> Value {
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
            "groups": t.groups,
            "survives": t.survives,
            "cell": t.cell_id,
            "journals": t.journals.iter().map(|j| json!({
                "journal": j.journal,
                "name": record_text(&j.name),
                "kind": j.kind,
                "writer": j.writer,
                "desired": j.desired,
                "acceptors": j.acceptors,
                "matchmakers": j.matchmakers,
            })).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
        "cells": reply.cells.iter().map(|c| json!({
            "cell": c.cell_id,
            "name": record_text(&c.name),
            "state": c.state,
            "tenants": c.tenants,
        })).collect::<Vec<_>>(),
    })
}
