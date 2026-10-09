//! The machine (#196, #216, #246): a machine's whole lifecycle
//! ([`run_machine`]: format, wait for a cell, serve it), and what an
//! uninitialized `parosd` does until `parosctl init` forms the cell.
//!
//! Every `parosd` starts the same way: with an identity minted at format —
//! its random `node_id` (#225), so a wiped disk is a new machine and "a wiped
//! identity never rejoins" holds by construction — and its rendezvous join
//! list, resolved to the cell's **seeds**. It never forms a cell on its own
//! (`docs/architecture.md` §3.1, `CockroachDB`'s `cockroach init`): it waits,
//! serving the machine contract (`proto/machine.proto`), until one `Init` is
//! sent to a seed that every seed's join list names.
//!
//! - **`Identify`** — who this machine is: its id, class, capacity, failure
//!   domain, address, and whether its own address is in its join list.
//! - **`Init`** (a seed's) — the cell step of `init`: identify every seed,
//!   mint the cell's id, record the plan as *pending* durably, form every
//!   other seed, then form this one last. A crash at any step is resumed by
//!   running `init` again: the pending plan is reused, never redrawn, and a
//!   seed already serving the cell counts as formed — but a seed with no
//!   plan that finds another seed serving a cell is refused (`cell_exists`):
//!   it is a new machine (a wiped volume), and never forms a second cell.
//!   Claiming the cell
//!   control journal with `SetLeader(expected_gen = 0)` is the caller's next
//!   step, on the formed cell (`parosctl init`).
//! - **`FormCell`** — join the cell a plan names: format its journals and
//!   record the plan durably ([`CellLedger::form`]); idempotent for the same
//!   plan. The machine then stops waiting, and its caller starts serving
//!   the plan's journals.
//!
//! The plan is a [`CellPlan`]: the cell's id, its bootstrap members (every
//! seed, by id and address) and the journals they serve from formation —
//! the cell tenant's control journal ([`CellPlan::control`]), the fleet
//! tenant's control journal ([`CellPlan::fleet`]: the fleet's one cell hosts
//! the fleet tenant in M9, #226) and the static assignment, every identifier
//! drawn at `init` (no identifier is fixed, `docs/architecture.md` §3.8)
//! that stands in for placement until M9 (#212). Its first coordinator, the
//! one that claims the cell control journal, is the lowest member id
//! ([`CellPlan::coordinator`]) until the cell coordinator of #225.
//!
//! Provider-generic like every driver here, so the simulation can run it
//! when the cell's bootstrap joins the campaign; today only `parosd` does
//! (#246). Durability is the caller's ([`MachineDisk`]): the library never
//! touches a path.

mod lifecycle;
mod record;
mod wait;

use std::collections::BTreeSet;
use std::net::SocketAddr;

use paros_core::{JournalId, JournalIdentifier, NodeId, TenantId};

pub use lifecycle::{MachineDisk, MachineError, MachineSettings, run_machine};
pub use record::{MachineRecord, PlanState, journal_config};
pub use wait::{CellLedger, wait_for_cell};

use crate::rpc::machine as wire;

/// The fleet's **control journals** as one cell knows them (§3.2, §3.8):
/// the cell's id, the cell tenant's control journal and, on the cell that
/// hosts it, the fleet tenant's. No control journal has a fixed id, so they
/// are learned — from the durable cell plan on a machine of the cell, from
/// any machine's node-only `Inspect` on a client — and handed on: the driver
/// is told which journals are control journals (they serve no user plane
/// and outlive a node's retirement), `Inspect` answers every client with
/// them, and a fleet operation ([`crate::client::fleet::FleetSession`])
/// writes them. One type for every holder (#243).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ControlJournals {
    /// The cell's id.
    pub cell_id: u64,
    /// The cell tenant's control journal: the registry and the capacity.
    pub cell: JournalIdentifier,
    /// The fleet tenant's control journal (the fleet directory), when this
    /// cell hosts the fleet tenant.
    pub fleet: Option<JournalIdentifier>,
}

/// A machine's class (FDB's process classes, `docs/architecture.md` §3.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    /// Anything with a durable store: acceptors, replicas, matchmakers.
    Storage,
    /// Proxies, proxy leaders, batchers, coordinators.
    Stateless,
}

impl Class {
    /// The class's name on the wire and in configuration.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Class::Storage => "storage",
            Class::Stateless => "stateless",
        }
    }
}

impl core::str::FromStr for Class {
    type Err = &'static str;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text {
            "storage" => Ok(Class::Storage),
            "stateless" => Ok(Class::Stateless),
            _ => Err("a class is `storage` or `stateless`"),
        }
    }
}

/// What a machine knows of itself before it has a cell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MachineFacts {
    /// Its identity, random, minted at format (#225).
    pub node_id: NodeId,
    /// Its class.
    pub class: Class,
    /// Its capacity, in the placement's units (opaque here).
    pub capacity: u64,
    /// Its failure domain (opaque here).
    pub failure_domain: String,
    /// The address it serves at, which its peers dial.
    pub addr: SocketAddr,
    /// Its rendezvous join list, resolved: the cell's seeds.
    pub seeds: Vec<SocketAddr>,
}

impl MachineFacts {
    /// Whether this machine is a seed: its own address is in its join list.
    #[must_use]
    pub fn is_seed(&self) -> bool {
        self.seeds.contains(&self.addr)
    }

    fn identify_ack(&self) -> wire::IdentifyAck {
        wire::IdentifyAck {
            node_id: self.node_id.0,
            class: self.class.as_str().into(),
            capacity: self.capacity,
            failure_domain: self.failure_domain.clone(),
            addr: self.addr.to_string(),
            seed: self.is_seed(),
        }
    }
}

/// A cell's bootstrap: what every seed records at formation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CellPlan {
    /// The cell's id, random, minted at `init` (#226).
    pub cell_id: u64,
    /// The bootstrap members — every seed — by id and address, in id order.
    pub members: Vec<(NodeId, SocketAddr)>,
    /// The cell tenant's control journal, its identifier drawn at `init`.
    pub control: JournalIdentifier,
    /// The fleet tenant's control journal when this cell hosts the fleet
    /// tenant (the fleet's first cell does, #226), its identifier drawn at
    /// `init`.
    pub fleet: Option<JournalIdentifier>,
    /// The journals every member serves from formation, in identifier order:
    /// [`CellPlan::control`], [`CellPlan::fleet`] and the static assignment.
    pub journals: Vec<JournalIdentifier>,
}

impl CellPlan {
    /// The first cell coordinator: the lowest member id (until #225's
    /// coordinator election).
    ///
    /// # Panics
    ///
    /// On a plan with no member (a [`CellPlan::check`]ed plan has one).
    #[must_use]
    pub fn coordinator(&self) -> NodeId {
        self.members
            .iter()
            .map(|(id, _)| *id)
            .min()
            .expect("a checked plan names a member")
    }

    /// Whether the plan is one a machine may form: a set cell id, at least
    /// one member, member ids and addresses unique, every journal identifier set
    /// and unique, the cell control journal among them.
    ///
    /// # Errors
    ///
    /// The first thing wrong with it.
    pub fn check(&self) -> Result<(), &'static str> {
        if self.cell_id == 0 {
            return Err("a cell id is non-zero");
        }
        if self.members.is_empty() {
            return Err("a cell has at least one member");
        }
        let ids: BTreeSet<NodeId> = self.members.iter().map(|(id, _)| *id).collect();
        let addrs: BTreeSet<SocketAddr> = self.members.iter().map(|(_, a)| *a).collect();
        if ids.len() != self.members.len() || addrs.len() != self.members.len() {
            return Err("a cell's members are unique by id and by address");
        }
        let served: BTreeSet<JournalIdentifier> = self.journals.iter().copied().collect();
        if served.len() != self.journals.len() || self.journals.iter().any(|j| !j.is_set()) {
            return Err("a cell's journals are set and unique");
        }
        if !self.control.is_set() || !served.contains(&self.control) {
            return Err("a cell serves its control journal");
        }
        if let Some(fleet) = self.fleet
            && (!served.contains(&fleet) || fleet.tenant == self.control.tenant)
        {
            return Err("the fleet tenant is served, under a tenant of its own");
        }
        Ok(())
    }

    /// The control journals the cell's machines know (see
    /// [`ControlJournals`]).
    #[must_use]
    pub fn control_journals(&self) -> ControlJournals {
        ControlJournals {
            cell_id: self.cell_id,
            cell: self.control,
            fleet: self.fleet,
        }
    }

    fn members_to_wire(&self) -> Vec<wire::Member> {
        self.members
            .iter()
            .map(|(id, addr)| wire::Member {
                node_id: id.0,
                addr: addr.to_string(),
            })
            .collect()
    }

    fn journals_to_wire(&self) -> Vec<wire::JournalIdentifier> {
        self.journals
            .iter()
            .map(|&journal| Self::identifier_to_wire(journal))
            .collect()
    }

    /// The plan a `FormCell` carries, normalized and checked.
    ///
    /// # Errors
    ///
    /// See [`CellPlan::from_wire`].
    pub fn from_form(form: &wire::FormCell) -> Result<Self, &'static str> {
        Self::from_wire(
            form.cell_id,
            &form.members,
            form.control.as_ref(),
            form.fleet.as_ref(),
            &form.journals,
        )
    }

    /// The plan an `InitAck` carries, normalized and checked.
    ///
    /// # Errors
    ///
    /// See [`CellPlan::from_wire`].
    pub fn from_init_ack(ack: &wire::InitAck) -> Result<Self, &'static str> {
        Self::from_wire(
            ack.cell_id,
            &ack.members,
            ack.control.as_ref(),
            ack.fleet.as_ref(),
            &ack.journals,
        )
    }

    /// A plan from its wire parts, normalized (members by id, journals by
    /// identifier) and checked.
    ///
    /// # Errors
    ///
    /// An address that does not parse, or a plan [`CellPlan::check`]
    /// refuses.
    fn from_wire(
        cell_id: u64,
        members: &[wire::Member],
        control: Option<&wire::JournalIdentifier>,
        fleet: Option<&wire::JournalIdentifier>,
        journals: &[wire::JournalIdentifier],
    ) -> Result<Self, &'static str> {
        let identifier = |f: &wire::JournalIdentifier| {
            JournalIdentifier::new(TenantId(f.tenant), JournalId(f.journal))
        };
        let mut members = members
            .iter()
            .map(|m| {
                m.addr
                    .parse()
                    .map(|addr| (NodeId(m.node_id), addr))
                    .map_err(|_| "a member's address is HOST:PORT, resolved")
            })
            .collect::<Result<Vec<_>, _>>()?;
        members.sort_unstable();
        let mut journals: Vec<JournalIdentifier> = journals
            .iter()
            .map(|f| JournalIdentifier::new(TenantId(f.tenant), JournalId(f.journal)))
            .collect();
        journals.sort_unstable();
        let plan = Self {
            cell_id,
            members,
            control: control.map_or(JournalIdentifier::UNSET, identifier),
            fleet: fleet.map(identifier).filter(|fleet| fleet.is_set()),
            journals,
        };
        plan.check()?;
        Ok(plan)
    }

    fn identifier_to_wire(journal: JournalIdentifier) -> wire::JournalIdentifier {
        wire::JournalIdentifier {
            tenant: journal.tenant.0,
            journal: journal.journal.0,
        }
    }

    fn form_request(&self) -> wire::FormCell {
        wire::FormCell {
            cell_id: self.cell_id,
            members: self.members_to_wire(),
            journals: self.journals_to_wire(),
            control: Some(Self::identifier_to_wire(self.control)),
            fleet: self.fleet.map(Self::identifier_to_wire),
        }
    }

    fn init_ack(&self) -> wire::InitAck {
        wire::InitAck {
            initialized: true,
            refusal: String::new(),
            cell_id: self.cell_id,
            members: self.members_to_wire(),
            journals: self.journals_to_wire(),
            coordinator: self.coordinator().0,
            control: Some(Self::identifier_to_wire(self.control)),
            fleet: self.fleet.map(Self::identifier_to_wire),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, 1], port))
    }

    fn identifier(tenant: u64, journal: u64) -> JournalIdentifier {
        JournalIdentifier::new(TenantId(tenant), JournalId(journal))
    }

    fn plan() -> CellPlan {
        CellPlan {
            cell_id: 7,
            members: vec![(NodeId(9), addr(1)), (NodeId(3), addr(2))],
            control: identifier(0x51, 0x52),
            fleet: Some(identifier(0x61, 0x62)),
            journals: vec![
                identifier(0x51, 0x52),
                identifier(0x61, 0x62),
                identifier(0x71, 0x72),
            ],
        }
    }

    #[test]
    fn a_plan_round_trips_normalized_and_names_its_coordinator() {
        let plan = plan();
        assert_eq!(plan.check(), Ok(()));
        assert_eq!(plan.coordinator(), NodeId(3));
        let back = CellPlan::from_form(&plan.form_request()).expect("a checked plan decodes");
        assert_eq!(
            back.members,
            vec![(NodeId(3), addr(2)), (NodeId(9), addr(1))]
        );
        assert_eq!(back.journals, plan.journals);
        assert_eq!(back.control_journals(), plan.control_journals());
        assert_eq!(CellPlan::from_init_ack(&plan.init_ack()), Ok(back));
    }

    #[test]
    fn a_malformed_plan_is_refused() {
        let mut no_cell = plan();
        no_cell.cell_id = 0;
        assert!(no_cell.check().is_err());
        let mut twice = plan();
        twice.members.push((NodeId(9), addr(3)));
        assert!(twice.check().is_err());
        let mut no_control = plan();
        no_control.journals.remove(0);
        assert!(no_control.check().is_err());
        let mut unset = plan();
        unset.control = JournalIdentifier::UNSET;
        assert!(unset.check().is_err());
        let mut shared = plan();
        shared.fleet = Some(identifier(0x51, 0x99));
        shared.journals.push(identifier(0x51, 0x99));
        assert!(
            shared.check().is_err(),
            "the fleet tenant has a tenant of its own"
        );
        assert!("cloud".parse::<Class>().is_err());
        assert_eq!("storage".parse::<Class>(), Ok(Class::Storage));
    }
}
