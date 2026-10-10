//! The machine (#196, #216, #246, #277): a machine's whole lifecycle
//! ([`run_machine`]: format, wait for a cell, serve it), and what an idle
//! `parosd` does until `cell init` forms its cell.
//!
//! Every `parosd` starts the same way: with an identity minted at format —
//! its random `node_id` (#225), so a wiped disk is a new machine and "a wiped
//! identity never rejoins" holds by construction. Its configuration names no
//! cell and no peer. It never forms a cell on its own
//! (`docs/architecture.md` §3.1): it waits, serving the machine contract
//! (`proto/machine.proto`), until a `cell init` that lists it forms it.
//!
//! **`cell init` is a single-decree Paxos on the cell plan** (decided on
//! 2026-10-09, #277): paros eats its own food, so the one-shot decision is
//! paros-core's [`Decree`](paros_core::Decree) over the shared
//! [`Proposer`](paros_core::proposer::Proposer) and
//! [`Acceptor`](paros_core::acceptor::Acceptor), the matchmaker handover's
//! machinery. The machine that receives `CellInit` is the proposer; every
//! listed machine is an acceptor. All of them must answer the ask, and a
//! majority of accepts chooses the plan (#246), so a founding member wiped
//! midway does not block it:
//!
//! - **`PrepareCell`** (ask, as a reservation) — a fresh random `init_id`,
//!   the ballot. Each machine answers who it is, promises to accept no plan
//!   under a lower ballot, and reports the plan it accepted, if any.
//! - **Adopt or draw.** A reported plan is finished instead of a new one
//!   (P2c): over the same addresses the two `init`s converge on one cell;
//!   over another list the receiver refuses (`other_cell_init`). A listed
//!   address that now hosts another machine than the plan names is a wiped
//!   one: the new machine never accepts the plan as the old one, and the
//!   others choose it while they are a majority (else `cell_lost`). With
//!   nothing reported the receiver draws the plan.
//! - **`FormCell`** (accept) — each machine accepts the plan unless it
//!   promised a higher ballot, and accepting is forming: it formats the
//!   plan's journals and records the plan durably ([`CellLedger::form`]),
//!   then serves them. A `FormCell` with no prior ask is both steps.
//!
//! A receiver that crashes midway leaves no lock: the next `cell init`, sent
//! to any listed machine, finds the accepted plan in its ask and finishes
//! it. A formed machine's vote is final, so it keeps answering both phases
//! from its record while it serves its cell ([`FormedCell`]).
//!
//! The plan is a [`CellPlan`]: the cell's id, its founding members by id and
//! address, and the journals they serve from formation — the cell tenant's
//! control journal ([`CellPlan::control`]), the fleet tenant's control
//! journal ([`CellPlan::fleet`]: the fleet's one cell hosts the fleet tenant
//! in M9, #226) and the static assignment, every identifier drawn at
//! `cell init` (no identifier is fixed, `docs/architecture.md` §3.8) that
//! stands in for placement until M9 (#212). The plan also names the cell's
//! first election journal ([`CellPlan::election`], #240): the founding
//! members campaign over it for the cell coordinator, which installs itself
//! as the control journal's leader (`coordinator.rs`).
//!
//! Provider-generic like every driver here: `parosd` runs it on a data
//! directory and the simulation on its simulated disk, each machine formed by
//! the workload's `init` (#246). Durability is the caller's ([`MachineDisk`]);
//! the record's write protocol and the formation's stores are
//! [`ProviderDisk`], over any storage provider, which both callers use.

mod admitted;
mod cell_init;
pub mod coordinator;
mod disk;
mod formed;
mod lifecycle;
mod record;
mod stores;
mod wait;

use std::collections::BTreeSet;
use std::net::SocketAddr;

use paros_core::{Ballot, Fingerprint, JournalId, JournalIdentifier, NodeId, TenantId};

pub use admitted::AdmittedMachine;
pub use disk::ProviderDisk;
pub use formed::FormedCell;
pub use lifecycle::{MachineAddresses, MachineError, MachineSettings, run_machine};
pub use record::{MachineRecord, journal_config};
pub use stores::AuditScope;
pub use wait::{CellLedger, Joined, wait_for_cell};

use crate::Address;
use crate::Names;
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
    /// The cell's election journal (#240): the multi-writer journal its
    /// coordinator is elected over, when known.
    pub election: Option<JournalIdentifier>,
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
    /// The address it advertises (`PAROS_ADVERTISE`, #257): what its peers
    /// and clients dial, a literal or a name resolved at dial time.
    pub addr: Address,
    /// The address it binds (`PAROS_LISTEN`, #257), never one read from the
    /// cell plan or the registry.
    pub listen: SocketAddr,
    /// How it resolves the addresses it dials.
    pub names: Names,
    /// The RPC incarnation of the runtime that serves it now (moonpool-rpc's
    /// `Incarnation`, new at every start): `0` until a runtime serves it
    /// ([`MachineFacts::serving`]). The registry records it, and the cell
    /// coordinator tells a reboot by it (#211).
    pub incarnation: u128,
}

/// The incarnation of the RPC runtime `rpc`, or `0` once it stopped.
pub(crate) fn incarnation_of<P: moonpool_core::Providers>(
    rpc: &moonpool_rpc::RpcHandle<P>,
) -> u128 {
    rpc.incarnation().map_or(0, moonpool_rpc::Incarnation::get)
}

/// A 128-bit incarnation's two wire halves, high then low.
#[must_use]
pub fn incarnation_halves(incarnation: u128) -> (u64, u64) {
    let high = u64::try_from(incarnation >> 64).unwrap_or(u64::MAX);
    let low = u64::try_from(incarnation & u128::from(u64::MAX)).unwrap_or(0);
    (high, low)
}

/// A 128-bit incarnation from its two wire halves.
#[must_use]
pub fn incarnation_from_halves(high: u64, low: u64) -> u128 {
    (u128::from(high) << 64) | u128::from(low)
}

impl MachineFacts {
    /// These facts as the runtime `rpc` serves them: its incarnation set.
    #[must_use]
    pub(crate) fn serving<P: moonpool_core::Providers>(
        &self,
        rpc: &moonpool_rpc::RpcHandle<P>,
    ) -> Self {
        let facts = Self {
            incarnation: incarnation_of(rpc),
            ..self.clone()
        };
        assert_eq!(facts.node_id, self.node_id, "serving keeps the identity");
        facts
    }

    /// Who this machine is, in cell `cell_id` (0 while it is idle).
    fn identify_ack(&self, cell_id: u64) -> wire::IdentifyAck {
        wire::IdentifyAck {
            node_id: self.node_id.0,
            class: self.class.as_str().into(),
            capacity: self.capacity,
            failure_domain: self.failure_domain.clone(),
            addr: self.addr.to_string(),
            cell_id,
            incarnation_high: incarnation_halves(self.incarnation).0,
            incarnation_low: incarnation_halves(self.incarnation).1,
        }
    }
}

/// A machine's admission into a cell (#216): what `cell add-machine`'s
/// `Admit` carries, and what the admitted machine records durably — the
/// cell, its control journals, and the cell's machines the caller knew at
/// admission, by id and address, in id order (the machine's cached registry
/// fold, `docs/architecture.md` §3.2). On every later start the machine
/// finds its cell from this record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Admission {
    /// The cell and its control journals.
    pub cell: ControlJournals,
    /// The cell's machines known at admission, by advertised address, in
    /// id order.
    pub members: Vec<(NodeId, Address)>,
}

impl Admission {
    /// Whether a machine may record this admission: a set cell id and cell
    /// control journal, the fleet tenant's (when named) set and under a
    /// tenant of its own, at least one known machine, unique by id and by
    /// address, in id order.
    ///
    /// # Errors
    ///
    /// The first thing wrong with it.
    pub fn check(&self) -> Result<(), &'static str> {
        if self.cell.cell_id == 0 || !self.cell.cell.is_set() {
            return Err("an admission names its cell and the cell control journal");
        }
        if let Some(fleet) = self.cell.fleet
            && (!fleet.is_set() || fleet.tenant == self.cell.cell.tenant)
        {
            return Err("the fleet tenant is set, under a tenant of its own");
        }
        if self.members.is_empty() {
            return Err("an admission names a machine of the cell");
        }
        let ids: BTreeSet<NodeId> = self.members.iter().map(|(id, _)| *id).collect();
        let addrs: BTreeSet<&Address> = self.members.iter().map(|(_, a)| a).collect();
        if ids.len() != self.members.len() || addrs.len() != self.members.len() {
            return Err("a cell's machines are unique by id and by address");
        }
        if !self.members.is_sorted() {
            return Err("an admission's machines are in id order");
        }
        Ok(())
    }

    /// The admission an `Admit` carries, normalized and checked.
    ///
    /// # Errors
    ///
    /// An address that does not parse, or an admission
    /// [`Admission::check`] refuses.
    pub fn from_wire(admit: &wire::Admit) -> Result<Self, &'static str> {
        let identifier = |f: &wire::JournalIdentifier| {
            JournalIdentifier::new(TenantId(f.tenant), JournalId(f.journal))
        };
        let mut members = admit
            .members
            .iter()
            .map(|m| {
                Address::parse(&m.addr)
                    .map(|addr| (NodeId(m.node_id), addr))
                    .map_err(|_| "a member's address is HOST:PORT")
            })
            .collect::<Result<Vec<_>, _>>()?;
        members.sort_unstable();
        let admission = Self {
            cell: ControlJournals {
                cell_id: admit.cell_id,
                cell: admit
                    .control
                    .as_ref()
                    .map_or(JournalIdentifier::UNSET, identifier),
                fleet: admit
                    .fleet
                    .as_ref()
                    .map(identifier)
                    .filter(|fleet| fleet.is_set()),
                election: admit
                    .election
                    .as_ref()
                    .map(identifier)
                    .filter(|election| election.is_set()),
            },
            members,
        };
        admission.check()?;
        Ok(admission)
    }

    /// The `Admit` that carries this admission.
    #[must_use]
    pub fn to_wire(&self) -> wire::Admit {
        wire::Admit {
            cell_id: self.cell.cell_id,
            control: Some(CellPlan::identifier_to_wire(self.cell.cell)),
            fleet: self.cell.fleet.map(CellPlan::identifier_to_wire),
            election: self.cell.election.map(CellPlan::identifier_to_wire),
            members: self
                .members
                .iter()
                .map(|(id, addr)| wire::Member {
                    node_id: id.0,
                    addr: addr.to_string(),
                })
                .collect(),
        }
    }
}

/// A cell's bootstrap: the value `cell init`'s decree chooses, and what
/// every founding member records at formation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CellPlan {
    /// The cell's id, random, minted at `cell init` (#226).
    pub cell_id: u64,
    /// The founding members — every machine `cell init` listed — by id and
    /// the address it was listed at (its advertised address, a literal or a
    /// name, #257), in id order.
    pub members: Vec<(NodeId, Address)>,
    /// The cell tenant's control journal, its identifier drawn at `init`.
    pub control: JournalIdentifier,
    /// The fleet tenant's control journal when this cell hosts the fleet
    /// tenant (the fleet's first cell does, #226), its identifier drawn at
    /// `init`.
    pub fleet: Option<JournalIdentifier>,
    /// The cell's first election journal (#240, decided on 2026-10-09): a
    /// multi-writer journal of the cell tenant, born on the founding members,
    /// over which they campaign for the cell coordinator.
    pub election: JournalIdentifier,
    /// The journals every member serves from formation, in identifier order:
    /// [`CellPlan::control`], [`CellPlan::election`], [`CellPlan::fleet`] and
    /// the static assignment.
    pub journals: Vec<JournalIdentifier>,
}

impl CellPlan {
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
        let addrs: BTreeSet<&Address> = self.members.iter().map(|(_, a)| a).collect();
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
        if !served.contains(&self.election)
            || self.election == self.control
            || self.election.tenant != self.control.tenant
        {
            return Err("a cell serves its election journal, in the cell tenant");
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
            election: Some(self.election),
        }
    }

    /// The static user journals: every journal the plan serves but the
    /// control and election journals.
    #[must_use]
    pub fn users(&self) -> Vec<JournalIdentifier> {
        self.journals
            .iter()
            .copied()
            .filter(|j| *j != self.control && *j != self.election && Some(*j) != self.fleet)
            .collect()
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
            form.election.as_ref(),
            &form.journals,
        )
    }

    /// The plan a `CellInitAck` carries, normalized and checked.
    ///
    /// # Errors
    ///
    /// See [`CellPlan::from_wire`].
    pub fn from_cell_init_ack(ack: &wire::CellInitAck) -> Result<Self, &'static str> {
        Self::from_wire(
            ack.cell_id,
            &ack.members,
            ack.control.as_ref(),
            ack.fleet.as_ref(),
            ack.election.as_ref(),
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
        election: Option<&wire::JournalIdentifier>,
        journals: &[wire::JournalIdentifier],
    ) -> Result<Self, &'static str> {
        let identifier = |f: &wire::JournalIdentifier| {
            JournalIdentifier::new(TenantId(f.tenant), JournalId(f.journal))
        };
        let mut members = members
            .iter()
            .map(|m| {
                Address::parse(&m.addr)
                    .map(|addr| (NodeId(m.node_id), addr))
                    .map_err(|_| "a member's address is HOST:PORT")
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
            election: election.map_or(JournalIdentifier::UNSET, identifier),
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

    /// The member addresses, as a set: what two `cell init`s compare.
    #[must_use]
    pub fn addrs(&self) -> BTreeSet<Address> {
        self.members.iter().map(|(_, addr)| addr.clone()).collect()
    }

    fn form_request(&self, ballot: Ballot) -> wire::FormCell {
        wire::FormCell {
            cell_id: self.cell_id,
            members: self.members_to_wire(),
            journals: self.journals_to_wire(),
            control: Some(Self::identifier_to_wire(self.control)),
            fleet: self.fleet.map(Self::identifier_to_wire),
            election: Some(Self::identifier_to_wire(self.election)),
            init: Some(ballot_to_wire(ballot)),
        }
    }

    fn cell_init_ack(&self) -> wire::CellInitAck {
        wire::CellInitAck {
            initialized: true,
            refusal: String::new(),
            cell_id: self.cell_id,
            members: self.members_to_wire(),
            journals: self.journals_to_wire(),
            control: Some(Self::identifier_to_wire(self.control)),
            fleet: self.fleet.map(Self::identifier_to_wire),
            election: Some(Self::identifier_to_wire(self.election)),
        }
    }
}

/// The identity a plan carries through the decree's Phase 2: an FNV-1a fold
/// over every field, in its normalized order. A plan is small and always
/// normalized, so its identity is its content.
impl Fingerprint for CellPlan {
    fn fingerprint(&self) -> u64 {
        const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        const PRIME: u64 = 0x0000_0100_0000_01b3;
        let fold = |hash: u64, bytes: &[u8]| {
            bytes
                .iter()
                .fold(hash, |h, b| (h ^ u64::from(*b)).wrapping_mul(PRIME))
        };
        let identifier = |hash: u64, journal: JournalIdentifier| {
            fold(
                fold(hash, &journal.tenant.0.to_le_bytes()),
                &journal.journal.0.to_le_bytes(),
            )
        };
        let mut hash = fold(OFFSET, &self.cell_id.to_le_bytes());
        for (id, addr) in &self.members {
            hash = fold(hash, &id.0.to_le_bytes());
            hash = fold(hash, addr.to_string().as_bytes());
        }
        hash = identifier(hash, self.control);
        hash = identifier(
            fold(hash, &[u8::from(self.fleet.is_some())]),
            self.fleet.unwrap_or(JournalIdentifier::UNSET),
        );
        hash = identifier(hash, self.election);
        for journal in &self.journals {
            hash = identifier(hash, *journal);
        }
        hash
    }
}

fn ballot_to_wire(ballot: Ballot) -> crate::rpc::common::Ballot {
    crate::rpc::common::Ballot {
        round: ballot.round,
        node: ballot.node.0,
    }
}

/// A ballot from the wire; an unset one decodes as `None` (the zero ballot
/// is no decree's: every `init_id` is non-zero).
fn ballot_from_wire(ballot: Option<&crate::rpc::common::Ballot>) -> Option<Ballot> {
    ballot
        .map(|b| Ballot {
            round: b.round,
            node: NodeId(b.node),
        })
        .filter(|b| b.round != 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> Address {
        Address::from(SocketAddr::from(([10, 0, 0, 1], port)))
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
            election: identifier(0x51, 0x53),
            journals: vec![
                identifier(0x51, 0x52),
                identifier(0x51, 0x53),
                identifier(0x61, 0x62),
                identifier(0x71, 0x72),
            ],
        }
    }

    #[test]
    fn a_plan_round_trips_normalized_and_names_its_election_journal() {
        let plan = plan();
        assert_eq!(plan.check(), Ok(()));
        assert_eq!(plan.users(), vec![identifier(0x71, 0x72)]);
        let mut foreign = plan.clone();
        foreign.election = identifier(0x61, 0x62);
        assert!(
            foreign.check().is_err(),
            "the election journal is the cell tenant's"
        );
        let ballot = Ballot {
            round: 5,
            node: NodeId(3),
        };
        let form = plan.form_request(ballot);
        assert_eq!(ballot_from_wire(form.init.as_ref()), Some(ballot));
        let back = CellPlan::from_form(&form).expect("a checked plan decodes");
        assert_eq!(
            back.members,
            vec![(NodeId(3), addr(2)), (NodeId(9), addr(1))]
        );
        assert_eq!(back.journals, plan.journals);
        assert_eq!(back.control_journals(), plan.control_journals());
        let again = CellPlan::from_form(&back.form_request(ballot)).expect("it decodes again");
        assert_eq!(again.fingerprint(), back.fingerprint());
        assert_eq!(
            CellPlan::from_cell_init_ack(&plan.cell_init_ack()),
            Ok(back)
        );
        let mut other = again.clone();
        other.cell_id = 8;
        assert_ne!(other.fingerprint(), again.fingerprint());
    }

    #[test]
    fn an_admission_round_trips_normalized_and_a_malformed_one_is_refused() {
        let admission = Admission {
            cell: plan().control_journals(),
            members: vec![(NodeId(3), addr(2)), (NodeId(9), addr(1))],
        };
        assert_eq!(admission.check(), Ok(()));
        let mut wire = admission.to_wire();
        wire.members.reverse();
        assert_eq!(Admission::from_wire(&wire), Ok(admission.clone()));
        let mut no_cell = admission.clone();
        no_cell.cell.cell_id = 0;
        assert!(no_cell.check().is_err());
        let mut unset = admission.clone();
        unset.cell.cell = JournalIdentifier::UNSET;
        assert!(unset.check().is_err());
        let mut shared = admission.clone();
        shared.cell.fleet = Some(identifier(0x51, 0x99));
        assert!(shared.check().is_err());
        let mut nobody = admission.clone();
        nobody.members.clear();
        assert!(nobody.check().is_err());
        let mut twice = admission.clone();
        twice.members.push((NodeId(11), addr(1)));
        assert!(twice.check().is_err());
        let mut unordered = admission;
        unordered.members.reverse();
        assert!(unordered.check().is_err());
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
