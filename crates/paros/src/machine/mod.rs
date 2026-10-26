//! The machine (#196, #216, #246, #277): a machine's whole lifecycle
//! ([`run_machine`]: format, wait for a cell, serve it), and what an idle
//! `parosd` does until `cell init` forms its cell.
//!
//! Every `parosd` starts the same way: with an identity minted at format —
//! its random `node_id` (#225), so a wiped disk is a new machine and "a wiped
//! identity never rejoins" holds by construction. Its configuration names no
//! cell and no peer. It never forms a cell on its own
//! (`docs/architecture/03-1-cell-tenant-and-bootstrap.md` §3.1): it waits, serving the machine contract
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
//! in M9, #226), every identifier drawn at `cell init` (no identifier is
//! fixed, `docs/architecture/03-8-identifiers-checkpoints-recovery-security.md` §3.8). A tenant's journals are not in the
//! plan: they live in its own control journal (#210). The plan also names the cell's
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
mod cache;
mod capacity;
mod cell_init;
pub mod coordinator;
mod disk;
mod follow;
mod footprints;
mod formed;
mod lifecycle;
pub mod load;
mod matchmakers;
mod modes;
mod moves;
mod pin;
pub(crate) mod register;
mod resolve;
mod roles;
mod scrub;
mod sets;
pub mod slow;
mod stores;
mod tenants;
mod universe;
pub(crate) mod views;
mod wait;

use std::net::SocketAddr;

use paros_core::NodeId;

pub use admitted::AdmittedMachine;
pub use cache::spawn_writer;
pub use disk::ProviderDisk;
pub(crate) use disk::{read_all, write_all};
pub use formed::{FormationToken, FormedCell};
pub use lifecycle::{MachineAddresses, MachineError, MachineSettings, run_machine};
pub use pin::AdminPin;
pub use sets::{SET, set_of};
pub use stores::AuditScope;
pub use wait::{CellLedger, Joined, wait_for_cell};

pub use crate::model::machine::*;

use crate::Address;
use crate::Names;
use crate::model::tags::Tag;
use crate::rpc::machine as wire;

/// A machine of a cell as its node loop serves it (#474): a founding
/// member, or a machine `cell add-machine` admitted, with the cache its
/// follow of the registry writes (#211). Both serve journals: a founding
/// member its plan's and every journal placed on it, an admitted one every
/// journal placed on it.
#[derive(Clone, Debug)]
pub enum CellMachine {
    /// A founding member.
    Founded {
        /// The machine.
        cell: FormedCell,
        /// Its cached registry fold's writer: the node loop offers it each
        /// address book, which the decree's teacher reads.
        cache: CacheSink,
    },
    /// An admitted machine.
    Admitted {
        /// The machine.
        machine: AdmittedMachine,
        /// Its cached registry fold's writer.
        cache: CacheSink,
    },
}

impl CellMachine {
    /// The cell's control journals, as this machine knows them.
    #[must_use]
    pub fn control_journals(&self) -> ControlJournals {
        match self {
            CellMachine::Founded { cell, .. } => cell.control_journals(),
            CellMachine::Admitted { machine, .. } => machine.admission.cell,
        }
    }

    /// What the machine knows of itself.
    #[must_use]
    pub fn facts(&self) -> &MachineFacts {
        match self {
            CellMachine::Founded { cell, .. } => &cell.facts,
            CellMachine::Admitted { machine, .. } => &machine.facts,
        }
    }

    /// What the machine knows of itself, to update once it serves.
    pub(crate) fn facts_mut(&mut self) -> &mut MachineFacts {
        match self {
            CellMachine::Founded { cell, .. } => &mut cell.facts,
            CellMachine::Admitted { machine, .. } => &mut machine.facts,
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
    /// Its zone and rack (#575: reserved tags).
    pub locality: Locality,
    /// Its name, for people (#399): a label, never its identity.
    pub name: String,
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
    /// The encoded `InterfaceRef` of the [`MachineInterface`] group the
    /// runtime that serves it now registered (#404 (static vs dynamic
    /// endpoints)): empty while it serves none (idle, admitted, or before
    /// [`MachineFacts::with_interface`]). The registry records it, and the
    /// founding members' lanes dial `Deliver` through it.
    ///
    /// [`MachineInterface`]: crate::wire::methods::MachineInterface
    pub machine_interface: Vec<u8>,
    /// Its last full busyness window (#424), which every phase's `Load`
    /// answers from.
    pub load: load::LoadBoard,
    /// Its metrics and phase (#366 (metrics)): the settings' handle, shared
    /// by every phase and the node loop.
    pub metrics: crate::observe::metrics::Metrics,
}

/// The incarnation of the RPC runtime `rpc`, or `0` once it stopped.
pub(crate) fn incarnation_of<P: crate::ParosProviders>(rpc: &moonpool_rpc::RpcHandle<P>) -> u128 {
    rpc.incarnation().map_or(0, moonpool_rpc::Incarnation::get)
}

impl MachineFacts {
    /// These facts as the runtime `rpc` serves them: its incarnation set.
    #[must_use]
    pub(crate) fn serving<P: crate::ParosProviders>(
        &self,
        rpc: &moonpool_rpc::RpcHandle<P>,
    ) -> Self {
        let incarnation = incarnation_of(rpc);
        // A group belongs to one runtime: another runtime serves none yet.
        let machine_interface = if incarnation == self.incarnation {
            self.machine_interface.clone()
        } else {
            Vec::new()
        };
        let facts = Self {
            incarnation,
            machine_interface,
            ..self.clone()
        };
        assert_eq!(facts.node_id, self.node_id, "serving keeps the identity");
        facts
    }

    /// These facts with `group` as the machine interface of the runtime
    /// that serves them (#404 (static vs dynamic endpoints)).
    ///
    /// # Panics
    ///
    /// If `group` belongs to another runtime than the one these facts name
    /// (a programmer error: call [`MachineFacts::serving`] first).
    #[must_use]
    pub(crate) fn with_interface(
        &self,
        group: &moonpool_rpc::ServiceGroup<crate::rpc::methods::MachineInterface>,
    ) -> Self {
        let reference = group.interface_ref();
        assert_eq!(
            reference.endpoint().incarnation().get(),
            self.incarnation,
            "a machine interface is the serving runtime's"
        );
        assert!(self.incarnation != 0, "a served machine has an incarnation");
        Self {
            machine_interface: crate::rpc::published(&reference),
            ..self.clone()
        }
    }

    /// Who this machine is, in cell `cell_id` (0 while it is idle).
    fn identify_ack(&self, cell_id: u64) -> wire::IdentifyAck {
        wire::IdentifyAck {
            node_id: self.node_id.0,
            capacity: self.capacity,
            tags: machine_tags(self.class, &self.locality)
                .iter()
                .map(Tag::to_wire)
                .collect(),
            addr: self.addr.to_string(),
            name: self.name.clone(),
            cell_id,
            incarnation_high: incarnation_halves(self.incarnation).0,
            incarnation_low: incarnation_halves(self.incarnation).1,
            machine_interface: self.machine_interface.clone(),
            registry_position: 0,
        }
    }
}
