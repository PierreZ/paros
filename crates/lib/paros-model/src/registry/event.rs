//! What a registry record folded to ([`RegistryEvent`]), or why it changed nothing ([`RegistryRefusal`]).

use paros_core::NodeId;
use paros_core::journal::{JournalId, TenantId};

use crate::checkpoint::Folded;
use crate::machine::Class;
use crate::registry::command::{GrantKey, Slot};
use crate::registry::records::{BookingTarget, MaintenanceScope, Role};

// The cell control journal has no fixed identifier (§3.8): a deployment draws
// it and hands it to whoever folds it (`paros_roles::node::ControlPlan`).

/// What one registry record folded to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegistryEvent {
    /// A node joined the pool.
    Registered {
        /// The node.
        id: NodeId,
        /// Its address.
        addr: String,
        /// Its class.
        class: Class,
        /// Its capacity.
        capacity: u64,
    },
    /// A registered node registered again (a reboot): its address and
    /// capacity are the new registration's.
    Reregistered {
        /// The node.
        id: NodeId,
        /// Its address now.
        addr: String,
        /// Its capacity now.
        capacity: u64,
    },
    /// A node is draining.
    Draining {
        /// The node.
        id: NodeId,
    },
    /// A node left the pool for good, its live bookings with it.
    Retired {
        /// The node.
        id: NodeId,
    },
    /// A slot of `node` was booked.
    Booked {
        /// The booking's id.
        booking: u64,
        /// The node.
        node: NodeId,
        /// The slot's role.
        role: Role,
        /// What the slot is for: with the role, whether it weighs
        /// (`Booking::weighs`, #753 (per-journal proxies)).
        target: BookingTarget,
    },
    /// The cell coordinator recorded machine `id`'s admission (#511).
    MachineAdmitted {
        /// The machine.
        id: NodeId,
    },
    /// Machine `id`'s user tags changed (#575).
    Tagged {
        /// The machine.
        id: NodeId,
    },
    /// A tag request applied already: nothing changed (#575).
    TagRepeated {
        /// The request.
        request: u64,
    },
    /// The cell coordinator marked machine `id` down (#211).
    MachineDown {
        /// The machine.
        id: NodeId,
    },
    /// The cell coordinator judged machine `id` slow, or fast again (#361
    /// (slow machines)).
    MachineSlow {
        /// The machine.
        id: NodeId,
        /// Whether it is slow now.
        slow: bool,
    },
    /// The cell coordinator judged machine `id` low on space, or not any
    /// more (#360 (disk full)).
    MachineSpace {
        /// The machine.
        id: NodeId,
        /// Whether it is low on space now.
        low: bool,
    },
    /// The cell coordinator marked machine `id` up as `incarnation`
    /// (#211): `was_down` when it was marked down before.
    MachineUp {
        /// The machine.
        id: NodeId,
        /// The incarnation that answered.
        incarnation: u128,
        /// Whether the registry held it down.
        was_down: bool,
    },
    /// A maintenance window was opened, or ended (`for_ms == 0`), over
    /// `scope` (#359).
    Maintenance {
        /// The machine or failure domain.
        scope: MaintenanceScope,
        /// The window's length; `0` when it ended.
        for_ms: u64,
    },
    /// A booking was released.
    Released {
        /// The booking's id.
        booking: u64,
        /// The node whose slot it freed.
        node: NodeId,
    },
    /// The cell joined a fleet (#229).
    JoinedFleet {
        /// The fleet.
        fleet_id: u64,
        /// This cell's id.
        cell_id: u64,
    },
    /// The cell hosts a tenant (#229).
    TenantHosted {
        /// The tenant.
        tenant: TenantId,
        /// Its control journal's id (#210).
        control: JournalId,
    },
    /// The cell forgot a tenant (#229).
    TenantDropped {
        /// The tenant.
        tenant: TenantId,
    },
    /// The entry asked for what the cell already records: a re-run step of
    /// a fleet operation (#229).
    Unchanged,
    /// A grant booked every slot it names (#476).
    Granted {
        /// What it is recorded under.
        key: GrantKey,
        /// The slots booked.
        slots: Vec<Slot>,
    },
    /// A grant's writer found no room, and the refusal is recorded (#476).
    GrantRefused {
        /// What it is recorded under.
        key: GrantKey,
    },
    /// A checkpoint (#230): every position below `covers_up_to` is in the
    /// state this fold now holds — restored from it, or verified against it
    /// (the fold reports which through its audit, not here, so every fold
    /// of the same position folds the same event).
    Checkpoint {
        /// The checkpoint's horizon, the position of its run's `Begin`.
        covers_up_to: u64,
    },
    /// A checkpoint run's `Begin` or `Chunk` (#353): nothing changed until
    /// the run's `End`. Reported so every position of the registry folds to
    /// an event.
    CheckpointRun,
    /// The entry changed nothing.
    Refused(RegistryRefusal),
}

/// Why a registry entry changed nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegistryRefusal {
    /// Not exactly one decodable registry entry (or a checkpoint this fold
    /// cannot use).
    Malformed,
    /// The id is a retired one (ids are never reused).
    AlreadyKnown {
        /// The node named.
        id: NodeId,
    },
    /// A re-registration under another class: a machine's class is fixed by
    /// its first registration.
    ClassChanged {
        /// The node named.
        id: NodeId,
    },
    /// A drain, or a booking, of a node not registered and in the pool (a
    /// draining node takes no new work).
    NotRegistered {
        /// The node named.
        id: NodeId,
    },
    /// A retirement of a node that is not draining.
    NotDraining {
        /// The node named.
        id: NodeId,
    },
    /// A booking under an id that was ever booked (a live booking holds it,
    /// or one held it and was released): booking ids are never reused, and
    /// its writer redraws.
    BookingTaken {
        /// The id asked for.
        booking: u64,
    },
    /// A booking of a slot of the other class than the node's: a storage
    /// machine never takes stateless work, and the reverse.
    WrongClass {
        /// The node named.
        id: NodeId,
    },
    /// A booking of a node with no slot left.
    NoCapacity {
        /// The node named.
        id: NodeId,
    },
    /// A booking of a node that already holds a live booking for the same
    /// role of the same journal or set: one slot per role instance.
    AlreadyBooked {
        /// The node named.
        id: NodeId,
    },
    /// A liveness entry that changes nothing: `MachineDown` of a machine
    /// held down, or `MachineUp` of the incarnation held up (D6: changes
    /// only).
    LivenessUnchanged {
        /// The node named.
        id: NodeId,
    },
    /// A `MachineDown` naming another incarnation than the one the registry
    /// holds: a later incarnation answered since.
    StaleIncarnation {
        /// The node named.
        id: NodeId,
    },
    /// A liveness entry of a node that is not in the pool.
    NotInPool {
        /// The node named.
        id: NodeId,
    },
    /// A release of a booking that is not live.
    UnknownBooking {
        /// The id named.
        booking: u64,
    },
    /// A fleet entry naming another fleet (or cell) than the one the cell
    /// joined, or a tenant entry before the cell joined any (#229): a step
    /// of a fleet operation that talks to another fleet changes nothing.
    OtherFleet {
        /// The fleet the entry names.
        fleet_id: u64,
    },
    /// A fleet registration at a metadata version this fold does not
    /// understand (#229).
    Unsupported,
    /// A `HostTenant` of a tenant the cell dropped (#229): a dropped tenant
    /// is never hosted again.
    TenantDropped {
        /// The tenant named.
        tenant: TenantId,
    },
    /// A `HostTenant` of a tenant whose footprint the cell has not granted
    /// (#476): a cell hosts a tenant only once its footprint is booked.
    NoFootprint {
        /// The tenant named.
        tenant: TenantId,
    },
}

/// The event a [`Folded`] registry record is reported as: an entry's own
/// event, a checkpoint's, or a refusal for a record the fold cannot use.
/// `None` for a record the fold skipped (above a gap).
#[must_use]
pub fn registry_event(folded: Folded<RegistryEvent>) -> Option<RegistryEvent> {
    match folded {
        Folded::Entry(event) => Some(event),
        Folded::Checkpoint { covers_up_to, .. } => Some(RegistryEvent::Checkpoint { covers_up_to }),
        Folded::Unreadable(_) => Some(RegistryEvent::Refused(RegistryRefusal::Malformed)),
        Folded::Run => Some(RegistryEvent::CheckpointRun),
        Folded::Skipped => None,
    }
}

/// What one control-journal record folded to — the registry's or a
/// tenant control journal's event (#210) — as the driver reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SystemEvent {
    /// A cell control journal (registry) record.
    Registry(RegistryEvent),
    /// A tenant control journal record (#210).
    Tenant(crate::tenant::TenantEvent),
}
