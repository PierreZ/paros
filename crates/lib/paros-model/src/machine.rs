//! The machine types every layer shares (#196, #216, #246, #277): a
//! machine's class and locality, the cell's control journals, an
//! admission, and the cell plan `cell init` chooses. The lifecycle that
//! uses them is in `paros_machine`.

mod book;
mod cache;
mod record;

use std::collections::BTreeSet;

use paros_core::journal::{JournalId, JournalIdentifier, TenantId};
use paros_core::types::Fingerprint;
use paros_core::{Ballot, NodeId};

pub use book::{address_book, cell_book, control_members, with_own};
pub use cache::{CacheSink, CachedRegistry, newer, starting_book};
pub use record::{MachineRecord, journal_config};

use crate::address::Address;
use crate::tags::{Tag, TagError, Tags, UserTags, reserved};
use paros_wire::machine as wire;

/// The fleet's **control journals** as one cell knows them (§3.2, §3.8):
/// the cell's id, the cell tenant's control journal and, on the cell that
/// hosts it, the fleet tenant's. No control journal has a fixed id, so they
/// are learned — from the durable cell plan on a machine of the cell, from
/// any machine's node-only `Inspect` on a client — and handed on: the driver
/// is told which journals are control journals (they serve no user plane
/// and outlive a node's retirement), `Inspect` answers every client with
/// them, and a fleet operation ([`paros_client::fleet::FleetSession`])
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

/// A machine's class (FDB's process classes, `docs/architecture/03-2-machines.md` §3.2):
/// the roles it takes. A role's own class ([`crate::registry::Role::class`])
/// is `Storage` or `Stateless`, never `All`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    /// Every role: the default of a machine.
    All,
    /// The roles with a durable store only: acceptors, replicas, matchmakers.
    Storage,
    /// The roles with no durable store only: frontends, proxy leaders,
    /// batchers, coordinators.
    Stateless,
}

impl Class {
    /// The class's name on the wire and in configuration.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Class::All => "all",
            Class::Storage => "storage",
            Class::Stateless => "stateless",
        }
    }

    /// Whether a machine of this class takes a role of `role` (a role's
    /// class, `Storage` or `Stateless`).
    #[must_use]
    pub fn takes(self, role: Class) -> bool {
        self == Class::All || self == role
    }

    /// Whether a machine of this class takes the roles with a durable store:
    /// it can be a founding member, and it hosts acceptors and matchmakers.
    #[must_use]
    pub fn stores(self) -> bool {
        self.takes(Class::Storage)
    }
}

impl core::str::FromStr for Class {
    type Err = &'static str;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text {
            "all" => Ok(Class::All),
            "storage" => Ok(Class::Storage),
            "stateless" => Ok(Class::Stateless),
            _ => Err("a class is `all`, `storage` or `stateless`"),
        }
    }
}

/// Where a machine stands (#575 (key:value tags)): its reserved tags
/// `paros/zone` (its failure domain, what placement spreads over) and
/// `paros/rack`, inside the zone. Both are opaque labels; an empty one is no
/// tag.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Locality {
    /// Its zone (`paros/zone`).
    pub zone: String,
    /// Its rack (`paros/rack`).
    pub rack: String,
}

impl Locality {
    /// A machine in zone `zone`, with no rack.
    #[must_use]
    pub fn zone(zone: impl Into<String>) -> Self {
        Self {
            zone: zone.into(),
            rack: String::new(),
        }
    }

    /// Whether both labels are valid tag values (#575): what a machine's
    /// configuration must hold.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        [&self.zone, &self.rack]
            .into_iter()
            .all(|label| Tag::new(reserved::ZONE, label.as_str()).is_ok())
    }
}

/// A machine's reserved tags (#575): the roles it takes (its class), its zone
/// and its rack.
///
/// # Panics
///
/// If the locality holds a label no tag value may hold (a programmer error:
/// a machine refuses such a configuration at start, [`Locality::is_valid`]).
#[must_use]
pub fn machine_tags(class: Class, locality: &Locality) -> Vec<Tag> {
    let mut tags = vec![Tag::reserved(reserved::ROLES, class.as_str())];
    if !locality.zone.is_empty() {
        tags.push(Tag::reserved(reserved::ZONE, locality.zone.clone()));
    }
    if !locality.rack.is_empty() {
        tags.push(Tag::reserved(reserved::RACK, locality.rack.clone()));
    }
    tags
}

/// A machine's class and locality from its reserved tags, and its user
/// tags (#575).
///
/// # Errors
///
/// No class, a class no machine has, a reserved key no machine carries, or
/// user tags over the cap.
pub fn parse_machine_tags(tags: Tags) -> Result<(Class, Locality, UserTags), TagError> {
    let (mut held, user) = tags.split()?;
    // A record written before the rename names the roles `paros/class`.
    let legacy = held.remove(reserved::CLASS);
    let class = held
        .remove(reserved::ROLES)
        .or(legacy)
        .and_then(|class| class.parse::<Class>().ok())
        .ok_or_else(|| TagError::UnknownReserved(reserved::ROLES.into()))?;
    let locality = Locality {
        zone: held.remove(reserved::ZONE).unwrap_or_default(),
        rack: held.remove(reserved::RACK).unwrap_or_default(),
    };
    match held.into_keys().next() {
        Some(key) => Err(TagError::UnknownReserved(key)),
        None => Ok((class, locality, user)),
    }
}

/// A machine's class and locality as its `IdentifyAck` names them (#575):
/// `None` when its tags are malformed, name no class, or carry a key no
/// machine reports.
#[must_use]
pub fn identity_tags(ack: &wire::IdentifyAck) -> Option<(Class, Locality)> {
    let tags = Tags::from_wire(ack.tags.clone()).ok()?;
    let (class, locality, user) = parse_machine_tags(tags).ok()?;
    user.is_empty().then_some((class, locality))
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

/// A machine's admission into a cell (#216): what `cell add-machine`'s
/// `Admit` carries, and what the admitted machine records durably — the
/// cell, its control journals, and the cell's machines the caller knew at
/// admission, by id and address, in id order (the machine's cached registry
/// fold, `docs/architecture/03-2-machines.md` §3.2). On every later start the machine
/// finds its cell from this record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Admission {
    /// The cell and its control journals.
    pub cell: ControlJournals,
    /// The cell's machines known at admission, by advertised address, in
    /// id order.
    pub members: Vec<(NodeId, Address)>,
    /// The cell's founding members, by id, in id order (#474): the
    /// registry's genesis pool, the machines that serve the control
    /// journals, and the cell's matchmakers ([`CellPlan::matchmakers`]).
    pub founders: Vec<NodeId>,
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
        if self.founders.is_empty() {
            return Err("an admission names the cell's founding members");
        }
        if !self.founders.windows(2).all(|w| w[0] < w[1]) {
            return Err("an admission's founding members are unique, in id order");
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
        let mut founders: Vec<NodeId> = admit.founders.iter().copied().map(NodeId).collect();
        founders.sort_unstable();
        founders.dedup();
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
            founders,
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
                    machine_interface: Vec::new(),
                    zone: String::new(),
                })
                .collect(),
            founders: self.founders.iter().map(|id| id.0).collect(),
            registry_position: 0,
            token: Vec::new(),
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
    /// [`CellPlan::control`], [`CellPlan::election`] and [`CellPlan::fleet`].
    pub journals: Vec<JournalIdentifier>,
}

impl CellPlan {
    /// The cell's matchmakers (#190): every founding member hosts every
    /// tenant's matchmaker set until placement (#212), named by its node id.
    ///
    /// # Panics
    ///
    /// Never: one matchmaker per member, by construction.
    #[must_use]
    pub fn matchmakers(&self) -> Vec<paros_core::membership::MatchmakerId> {
        let matchmakers: Vec<paros_core::membership::MatchmakerId> = self
            .members
            .iter()
            .map(|(id, _)| paros_core::membership::MatchmakerId(id.0))
            .collect();
        assert_eq!(
            matchmakers.len(),
            self.members.len(),
            "one matchmaker per founding member"
        );
        matchmakers
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

    fn members_to_wire(&self) -> Vec<wire::Member> {
        self.members
            .iter()
            .map(|(id, addr)| wire::Member {
                node_id: id.0,
                addr: addr.to_string(),
                machine_interface: Vec::new(),
                zone: String::new(),
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

    pub fn form_request(&self, ballot: Ballot) -> wire::FormCell {
        wire::FormCell {
            cell_id: self.cell_id,
            members: self.members_to_wire(),
            journals: self.journals_to_wire(),
            control: Some(Self::identifier_to_wire(self.control)),
            fleet: self.fleet.map(Self::identifier_to_wire),
            election: Some(Self::identifier_to_wire(self.election)),
            init: Some(ballot_to_wire(ballot)),
            token: Vec::new(),
        }
    }

    pub fn cell_init_ack(&self) -> wire::CellInitAck {
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

#[must_use]
pub fn ballot_to_wire(ballot: Ballot) -> paros_wire::common::Ballot {
    paros_wire::common::Ballot {
        round: ballot.round,
        node: ballot.node.0,
    }
}

/// A ballot from the wire; an unset one decodes as `None` (the zero ballot
/// is no decree's: every `init_id` is non-zero).
#[must_use]
pub fn ballot_from_wire(ballot: Option<&paros_wire::common::Ballot>) -> Option<Ballot> {
    ballot
        .map(|b| Ballot {
            round: b.round,
            node: NodeId(b.node),
        })
        .filter(|b| b.round != 0)
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

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
            ],
        }
    }

    #[test]
    fn a_plan_round_trips_normalized_and_names_its_election_journal() {
        let plan = plan();
        assert_eq!(plan.check(), Ok(()));
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
            founders: vec![NodeId(3)],
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
        let mut unordered = admission.clone();
        unordered.members.reverse();
        assert!(unordered.check().is_err());
        let mut founderless = admission;
        founderless.founders.clear();
        assert!(founderless.check().is_err());
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

    #[test]
    fn machine_tags_name_the_roles_and_read_the_old_key() {
        let tags = |list: &[&str]| Tags::of(list.iter().map(|t| t.parse().unwrap())).unwrap();
        let written = Tags::of(machine_tags(Class::All, &Locality::zone("zone-a"))).unwrap();
        assert_eq!(written.label(), "paros/roles:all,paros/zone:zone-a");
        let (class, locality, _) = parse_machine_tags(written).unwrap();
        assert_eq!((class, locality.zone.as_str()), (Class::All, "zone-a"));
        let (class, _, _) = parse_machine_tags(tags(&["paros/class:stateless"])).unwrap();
        assert_eq!(class, Class::Stateless, "a record before the rename");
        assert!(parse_machine_tags(tags(&["paros/zone:zone-a"])).is_err());
    }

    #[test]
    fn a_class_names_the_roles_a_machine_takes() {
        for class in [Class::All, Class::Storage, Class::Stateless] {
            assert_eq!(class.as_str().parse::<Class>(), Ok(class));
        }
        assert!(Class::All.takes(Class::Storage) && Class::All.takes(Class::Stateless));
        assert!(Class::Storage.takes(Class::Storage) && !Class::Storage.takes(Class::Stateless));
        assert!(
            Class::Stateless.takes(Class::Stateless) && !Class::Stateless.takes(Class::Storage)
        );
        assert!(Class::All.stores() && Class::Storage.stores() && !Class::Stateless.stores());
    }
}
