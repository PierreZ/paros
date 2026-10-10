//! **The administrative views** (#399, `docs/architecture.md` §3.6): what a
//! cell holds — its machines, its tenants and who holds which role — as one
//! cell answers it.
//!
//! A view is a request to **one** cell (`proto/view.proto`): a founding
//! member of that cell answers it from the cell's own journals
//! (`crate::machine::views`), and this module is the pure half of the
//! answer: [`CellFacts`] is what the member read, [`cell_view`] and
//! [`tenant_view`] build the answer, and [`authorize`] turns the caller's
//! claim into a [`Scope`]. The answer is filtered here, in the cell, before
//! it leaves: a caller never filters.
//!
//! | scope | sees |
//! |---|---|
//! | [`Scope::Admin`] | every view, every tenant: addresses, incarnations, standing, bookings, free slots |
//! | [`Scope::Tenant`] | its own tenant's view only: its journals, their acceptors, the coordinator, and of each machine they use its name, failure domain and up/down state |
//!
//! Every entity carries its name: what `parosctl` prints (hex ids are for
//! local debugging only). Pure and provider-free: no I/O, no randomness.

use paros_core::{JournalIdentifier, NodeId, TenantId, WriterMode};

use crate::Address;
use crate::fleet::{FleetDirectory, Group};
use crate::rpc::view as wire;
use crate::system::{NodeStanding, Registry};
use crate::tenant::TenantControl;

/// Who asks, as the cell decided it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Scope {
    /// Every detail of the cell (the `admin` role, #245: `view.detail`).
    Admin,
    /// The spread of one tenant over its cell, by the tenant's name (the
    /// `tenant` role, #245).
    Tenant(Vec<u8>),
}

impl Scope {
    /// The wire's form.
    #[must_use]
    pub fn to_wire(&self) -> wire::Scope {
        wire::Scope {
            kind: Some(match self {
                Scope::Admin => wire::scope::Kind::Admin(wire::AdminScope {}),
                Scope::Tenant(name) => wire::scope::Kind::Tenant(name.clone()),
            }),
        }
    }
}

/// The scope a caller's claim grants (#399). Until the frontend checks
/// tokens (#192), every caller is an admin and may narrow itself to one
/// tenant: the claim is the scope. #245 derives it from the caller's
/// Biscuit token instead (`view.detail` is admin, else the tenant its token
/// names), and nothing else changes.
///
/// # Errors
///
/// The claim names nothing, or an empty tenant name.
pub fn authorize(claim: Option<&wire::Scope>) -> Result<Scope, &'static str> {
    match claim.and_then(|scope| scope.kind.as_ref()) {
        Some(wire::scope::Kind::Admin(_)) => Ok(Scope::Admin),
        Some(wire::scope::Kind::Tenant(name)) if !name.is_empty() => {
            Ok(Scope::Tenant(name.clone()))
        }
        _ => Err("malformed"),
    }
}

/// What a founding member read of its cell to answer a view.
#[derive(Clone, Debug)]
pub struct CellFacts<'a> {
    /// The cell.
    pub cell_id: u64,
    /// The member answering.
    pub answered_by: NodeId,
    /// The founding members, at the address the plan names.
    pub founders: &'a [(NodeId, Address)],
    /// The cell tenant's control journal (the registry).
    pub control: JournalIdentifier,
    /// The cell's election journal.
    pub election: JournalIdentifier,
    /// The universe directory's journal, when this cell hosts it.
    pub universe: Option<JournalIdentifier>,
    /// The registry, folded to `registry.next_seq()`.
    pub registry: &'a Registry,
    /// The universe directory, when this cell hosts it and it was read.
    pub directory: Option<&'a FleetDirectory>,
    /// The control journals of the hosted tenants it read, folded.
    pub tenants: &'a [TenantControl],
    /// The cell coordinator now and its term, as the election journal
    /// names it.
    pub coordinator: Option<(u64, u64)>,
}

impl CellFacts<'_> {
    /// The founding members' ids.
    fn founder_ids(&self) -> Vec<u64> {
        self.founders.iter().map(|(id, _)| id.0).collect()
    }

    /// The answer's header: the cell, its names, where it was folded and
    /// who leads.
    fn header(&self) -> wire::ViewReply {
        let directory = self.directory;
        wire::ViewReply {
            cell_id: self.cell_id,
            cell_name: directory.map_or_else(Vec::new, |d| d.cell_name(self.cell_id).to_vec()),
            universe_name: directory.map_or_else(Vec::new, |d| d.universe_name().to_vec()),
            cell_state: directory
                .and_then(|d| d.cell(self.cell_id))
                .map_or_else(String::new, |cell| cell.state.as_str().to_string()),
            registry_at: self.registry.next_seq(),
            directory_at: directory.map_or(0, FleetDirectory::next_seq),
            answered_by: self.answered_by.0,
            coordinator: self.coordinator.map_or(0, |(node, _)| node),
            coordinator_term: self.coordinator.map_or(0, |(_, term)| term),
            ..wire::ViewReply::default()
        }
    }

    /// Every machine of the cell: the founding members, then every
    /// registered machine, in id order, with every detail.
    fn machines(&self) -> Vec<wire::MachineView> {
        let mut ids: Vec<NodeId> = self.founders.iter().map(|(id, _)| *id).collect();
        ids.extend(self.registry.nodes().map(|(id, _)| id));
        ids.sort_unstable();
        ids.dedup();
        ids.into_iter().map(|id| self.machine(id)).collect()
    }

    /// Machine `id`, with every detail.
    fn machine(&self, id: NodeId) -> wire::MachineView {
        let founder = self.founders.iter().find(|(f, _)| *f == id);
        let liveness = self.registry.liveness(id);
        let (high, low) = crate::machine::incarnation_halves(liveness.incarnation);
        let bookings = self
            .registry
            .bookings()
            .filter(|(_, booking)| booking.node == id)
            .map(|(booking, b)| {
                let (journal, set) = match b.target {
                    crate::system::BookingTarget::Journal(journal) => (journal.journal.0, 0),
                    crate::system::BookingTarget::Set { set, .. } => (0, set),
                };
                wire::BookingView {
                    booking,
                    role: b.role.as_str().to_string(),
                    tenant: b.target.tenant().0,
                    journal,
                    set,
                }
            })
            .collect();
        let base = wire::MachineView {
            node_id: id.0,
            up: liveness.up,
            incarnation_high: high,
            incarnation_low: low,
            founder: founder.is_some(),
            booked: self.registry.booked(id),
            bookings,
            ..wire::MachineView::default()
        };
        match self.registry.get(id) {
            Some(node) => wire::MachineView {
                name: node.name.clone(),
                addr: node.addr.clone(),
                class: node.class.as_str().to_string(),
                capacity: node.capacity,
                failure_domain: node.failure_domain.clone(),
                standing: standing(node.standing).to_string(),
                ..base
            },
            None => wire::MachineView {
                addr: founder.map_or_else(String::new, |(_, addr)| addr.to_string()),
                class: crate::machine::Class::Storage.as_str().to_string(),
                standing: "founding".to_string(),
                ..base
            },
        }
    }

    /// The internal tenants this cell holds: its cell tenant (the registry
    /// and the election journal) and, when it hosts it, the universe tenant
    /// (the universe directory). Their journals live on the founding
    /// members.
    fn internal_tenants(&self) -> Vec<wire::TenantView> {
        let founders = self.founder_ids();
        let journal = |id: JournalIdentifier, kind: &str, writer: WriterMode| wire::JournalView {
            journal: id.journal.0,
            name: Vec::new(),
            kind: kind.to_string(),
            writer: writer_label(writer).to_string(),
            desired: "founders".to_string(),
            acceptors: founders.clone(),
            matchmakers: Vec::new(),
        };
        let directory = self.directory;
        let mut cell = wire::TenantView {
            tenant: self.control.tenant.0,
            name: directory.map_or_else(Vec::new, |d| d.cell_name(self.cell_id).to_vec()),
            kind: "cell".to_string(),
            state: "ready".to_string(),
            groups: "internal,cell".to_string(),
            survives: String::new(),
            cell_id: self.cell_id,
            journals: vec![journal(self.control, "control", WriterMode::Single)],
        };
        if self.election.tenant == self.control.tenant {
            cell.journals
                .push(journal(self.election, "election", WriterMode::Multi));
        }
        let mut tenants = vec![cell];
        if let Some(universe) = self.universe {
            tenants.push(wire::TenantView {
                tenant: universe.tenant.0,
                name: directory.map_or_else(Vec::new, |d| d.universe_name().to_vec()),
                kind: "universe".to_string(),
                state: "ready".to_string(),
                groups: "internal,fleet".to_string(),
                survives: String::new(),
                cell_id: self.cell_id,
                journals: vec![journal(universe, "directory", WriterMode::Single)],
            });
        }
        tenants
    }

    /// The hosted tenant `tenant` (a `users` tenant): its control journal
    /// on the founding members and every live journal it created.
    fn hosted(&self, tenant: TenantId) -> Option<wire::TenantView> {
        let hosted = self.registry.hosted_tenant(tenant)?;
        let entry = self.directory.and_then(|d| d.tenant(tenant));
        let mut journals = vec![wire::JournalView {
            journal: hosted.control.0,
            name: Vec::new(),
            kind: "control".to_string(),
            writer: writer_label(WriterMode::Single).to_string(),
            desired: "founders".to_string(),
            acceptors: self.founder_ids(),
            matchmakers: Vec::new(),
        }];
        if let Some(control) = self.tenants.iter().find(|c| c.tenant() == tenant) {
            journals.extend(control.live().map(|(id, journal)| wire::JournalView {
                journal: id.0,
                name: journal.name.clone(),
                kind: "data".to_string(),
                writer: writer_label(journal.writer).to_string(),
                desired: journal.desired.label(),
                acceptors: journal.config.members().iter().map(|n| n.0).collect(),
                matchmakers: Vec::new(),
            }));
        }
        Some(wire::TenantView {
            tenant: tenant.0,
            name: hosted.name.clone(),
            kind: "users".to_string(),
            state: entry.map_or_else(|| "hosted".to_string(), |e| e.state.as_str().to_string()),
            groups: entry.map_or_else(|| "users".to_string(), |e| e.groups.label()),
            survives: hosted.survives.as_str().to_string(),
            cell_id: self.cell_id,
            journals,
        })
    }

    /// The hosted tenant named `name`, if this cell hosts one.
    fn hosted_named(&self, name: &[u8]) -> Option<TenantId> {
        self.registry
            .hosted()
            .find(|t| self.registry.hosted_tenant(*t).is_some_and(|h| h.name == name))
    }
}

/// A standing's label.
fn standing(standing: NodeStanding) -> &'static str {
    match standing {
        NodeStanding::Registered => "registered",
        NodeStanding::Draining => "draining",
        NodeStanding::Retired => "retired",
    }
}

/// A writer mode's label.
fn writer_label(mode: WriterMode) -> &'static str {
    match mode {
        WriterMode::Single => "single",
        WriterMode::Multi => "multi",
    }
}

/// A refusal: the reply that carries only why.
#[must_use]
pub fn refusal(why: &str) -> wire::ViewReply {
    wire::ViewReply {
        refusal: why.to_string(),
        ..wire::ViewReply::default()
    }
}

/// The cell view (`machine list`, `machine show`, `cell show`, `roles
/// --cell`, `roles --machine`): every machine, every tenant this cell holds
/// and every role holder. Admin only.
#[must_use]
pub fn cell_view(scope: &Scope, facts: &CellFacts<'_>) -> wire::ViewReply {
    if *scope != Scope::Admin {
        moonpool_assertions::reachable!("view: a tenant scope is refused the cell view");
        return refusal("forbidden");
    }
    let mut reply = facts.header();
    reply.machines = facts.machines();
    reply.tenants = facts.internal_tenants();
    reply
        .tenants
        .extend(facts.registry.hosted().filter_map(|t| facts.hosted(t)));
    reply
}

/// The tenant view (`tenant show`, `roles --tenant`): tenant `name`'s
/// journals, their acceptors and the machines they use. An admin sees every
/// detail of those machines; the tenant itself sees only their names,
/// failure domains and up/down state, and no other tenant's view.
#[must_use]
pub fn tenant_view(scope: &Scope, facts: &CellFacts<'_>, name: &[u8]) -> wire::ViewReply {
    if let Scope::Tenant(own) = scope
        && own.as_slice() != name
    {
        moonpool_assertions::reachable!("view: a view refused for a tenant the scope does not name");
        return refusal("forbidden");
    }
    let Some(tenant) = facts.hosted_named(name) else {
        // Not hosted here: the directory may know where it lives.
        let elsewhere = facts
            .directory
            .and_then(|d| d.named(name))
            .filter(|(_, entry)| entry.cell_id != facts.cell_id && entry.cell_id != 0);
        if let (Some((_, entry)), Some(directory)) = (elsewhere, facts.directory) {
            return wire::ViewReply {
                refusal: "other_cell".to_string(),
                cell_id: entry.cell_id,
                cell_name: directory.cell_name(entry.cell_id).to_vec(),
                ..wire::ViewReply::default()
            };
        }
        return refusal("unknown_tenant");
    };
    let Some(view) = facts.hosted(tenant) else {
        return refusal("unknown_tenant");
    };
    let mut used: Vec<u64> = view
        .journals
        .iter()
        .flat_map(|j| j.acceptors.iter().chain(&j.matchmakers).copied())
        .chain(facts.coordinator.map(|(node, _)| node))
        .collect();
    used.sort_unstable();
    used.dedup();
    let mut reply = facts.header();
    reply.machines = used
        .into_iter()
        .filter(|id| *id != 0)
        .map(|id| facts.machine(NodeId(id)))
        .collect();
    reply.tenants = vec![view];
    if let Scope::Tenant(_) = scope {
        moonpool_assertions::reachable!("view: a tenant scope sees its own spread");
        narrow(&mut reply);
    }
    reply
}

/// What a tenant scope may see of a tenant view: of each machine its id,
/// name, failure domain and up/down state; no address, no incarnation, no
/// capacity, no booking, no standing; and no other cell detail.
fn narrow(reply: &mut wire::ViewReply) {
    for machine in &mut reply.machines {
        *machine = wire::MachineView {
            node_id: machine.node_id,
            name: std::mem::take(&mut machine.name),
            failure_domain: std::mem::take(&mut machine.failure_domain),
            up: machine.up,
            ..wire::MachineView::default()
        };
    }
    reply.universe_name.clear();
    reply.cells.clear();
}

/// The universe view (`cell list`, `tenant list`): the universe directory's
/// cells and tenants. Admin only.
#[must_use]
pub fn universe_view(
    scope: &Scope,
    facts: &CellFacts<'_>,
    directory: &FleetDirectory,
) -> wire::ViewReply {
    if *scope != Scope::Admin {
        return refusal("forbidden");
    }
    let mut reply = facts.header();
    reply.cells = directory
        .cells()
        .map(|(cell_id, entry)| wire::CellView {
            cell_id,
            name: directory.cell_name(cell_id).to_vec(),
            state: entry.state.as_str().to_string(),
            tenants: directory
                .tenants()
                .filter(|(_, t)| t.cell_id == cell_id && t.groups.contains(Group::Users))
                .count() as u64,
        })
        .collect();
    reply.tenants = directory
        .tenants()
        .map(|(tenant, entry)| wire::TenantView {
            tenant: tenant.0,
            name: entry.name.clone(),
            kind: if entry.groups.contains(Group::Users) {
                "users"
            } else if entry.groups.contains(Group::Fleet) {
                "universe"
            } else {
                "cell"
            }
            .to_string(),
            state: entry.state.as_str().to_string(),
            groups: entry.groups.label(),
            survives: entry.survives.as_str().to_string(),
            cell_id: entry.cell_id,
            journals: Vec::new(),
        })
        .collect();
    reply
}

/// Whether `reply`, answered to a tenant scope for `name`, holds only what
/// that scope may see: the tenant itself, and no address, incarnation,
/// capacity, booking or standing. The simulation's oracle (#399).
#[must_use]
pub fn within_tenant_scope(reply: &wire::ViewReply, name: &[u8]) -> bool {
    reply.cells.is_empty()
        && reply.tenants.iter().all(|t| t.name == name)
        && reply.machines.iter().all(|m| {
            m.addr.is_empty()
                && m.incarnation_high == 0
                && m.incarnation_low == 0
                && m.capacity == 0
                && m.booked == 0
                && m.bookings.is_empty()
                && m.standing.is_empty()
                && m.class.is_empty()
        })
}

#[cfg(test)]
mod tests;
