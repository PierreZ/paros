//! The machine that hosts a node (#474): what the node loop knows of it
//! ([`MachineFacts`], a founding member's [`FormedCell`]) and the port it
//! serves the machine's cell through ([`CellHost`]).
//!
//! The node loop is a role; the machine that runs it is a layer above. The
//! machine hands the loop a [`CellHost`], and the loop calls it once its RPC
//! runtime serves: the host starts what the machine answers beside the
//! journals (its views, its registration, the cell coordinator's campaign,
//! the cell decree). A node outside a machine has no host ([`NoHost`]).

use std::net::SocketAddr;
use std::sync::Arc;

use moonpool_rpc::RpcHandle;
use paros_core::{Ballot, NodeId};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::client::CallObserver;
use crate::model::machine::{
    CachedRegistry, CellPlan, Class, ControlJournals, Locality, incarnation_halves, machine_tags,
};
use crate::model::tags::Tag;
use crate::storage::RunError;
use crate::tunables::DriverTunables;
use crate::wire::machine as wire;
use crate::{Address, Names, ParosProviders};

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
    pub load: crate::model::load::LoadBoard,
    /// Its metrics and phase (#366 (metrics)): the settings' handle, shared
    /// by every phase and the node loop.
    pub metrics: crate::observe::metrics::Metrics,
}

/// The incarnation of the RPC runtime `rpc`, or `0` once it stopped.
pub fn incarnation_of<P: crate::ParosProviders>(rpc: &moonpool_rpc::RpcHandle<P>) -> u128 {
    rpc.incarnation().map_or(0, moonpool_rpc::Incarnation::get)
}

impl MachineFacts {
    /// These facts as the runtime `rpc` serves them: its incarnation set.
    #[must_use]
    pub fn serving<P: crate::ParosProviders>(&self, rpc: &moonpool_rpc::RpcHandle<P>) -> Self {
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
    pub fn with_interface(
        &self,
        group: &moonpool_rpc::ServiceGroup<crate::wire::methods::MachineInterface>,
    ) -> Self {
        let reference = group.interface_ref();
        assert_eq!(
            reference.endpoint().incarnation().get(),
            self.incarnation,
            "a machine interface is the serving runtime's"
        );
        assert!(self.incarnation != 0, "a served machine has an incarnation");
        Self {
            machine_interface: crate::net::published(&reference),
            ..self.clone()
        }
    }

    /// Who this machine is, in cell `cell_id` (0 while it is idle).
    #[must_use]
    pub fn identify_ack(&self, cell_id: u64) -> wire::IdentifyAck {
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

/// A machine of a cell: what it knows of itself, the plan it accepted and
/// the decree ballot it accepted it at — the record's commit point.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FormedCell {
    /// The machine.
    pub facts: MachineFacts,
    /// The plan it formed.
    pub plan: CellPlan,
    /// The ballot it accepted the plan at.
    pub ballot: Ballot,
    /// The cached registry fold its disk held at start (#211), if any.
    pub cached: Option<CachedRegistry>,
    /// The token of the `FormCell` that formed it in this incarnation (#245
    /// (Biscuit tokens)), in memory only: never in the record, so no bearer
    /// token rests on the disk. `None` after a restart.
    pub token: Option<FormationToken>,
}

/// The operator's `admin` token a machine was formed with, sent on by its
/// teacher (see the module doc). Its `Debug` never shows the bytes.
#[derive(Clone, PartialEq, Eq)]
pub struct FormationToken(Vec<u8>);

impl FormationToken {
    /// The token `bytes` (empty on a machine with no pin).
    #[must_use]
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// The token's bytes.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl std::fmt::Debug for FormationToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FormationToken({} bytes)", self.0.len())
    }
}

impl FormedCell {
    /// The control journals the cell's machines know (see
    /// [`ControlJournals`]).
    #[must_use]
    pub fn control_journals(&self) -> ControlJournals {
        self.plan.control_journals()
    }

    /// The founding members where this machine dials them at start: at the
    /// addresses its cached registry fold names (#211), else the plan's.
    /// The registry fold takes over once it passes the cache's position.
    ///
    /// # Panics
    ///
    /// Never: the assertion checks that every founding member keeps one
    /// address.
    #[must_use]
    pub fn founders(&self) -> Vec<(NodeId, Address)> {
        // The starting book lists the founding members first, then the
        // cache's others: the founders are its head.
        let mut founders =
            crate::model::machine::starting_book(&self.plan.members, self.cached.as_ref());
        founders.truncate(self.plan.members.len());
        assert_eq!(
            founders.len(),
            self.plan.members.len(),
            "every founding member keeps one address"
        );
        founders
    }
}

/// What a node's host started beside the journals: the receivers the node
/// loop watches.
#[derive(Debug, Default)]
pub struct Hosted {
    /// The books an admitted machine's cache is offered (#505 (stranded
    /// admitted machine)): its follow's folds, and a book its cell
    /// coordinator handed it when its fold was stuck.
    pub offered_books: Option<watch::Receiver<Option<CachedRegistry>>>,
    /// A founding member that came back at another address than the one
    /// its cell knows (#390): `false` until its registration ends.
    pub unregistered: Option<watch::Receiver<bool>>,
    /// Whether the cell coordinator's task ended (#560 (clean Ctrl-C
    /// shutdown)).
    pub coordinator_ended: Option<watch::Receiver<bool>>,
}

/// The machine a node loop serves journals for (#474): a founding member
/// or an admitted machine. The machine layer implements it.
pub trait CellHost: Clone {
    /// The cell's control journals, as this machine knows them.
    fn control_journals(&self) -> ControlJournals;

    /// What the machine knows of itself.
    fn facts(&self) -> &MachineFacts;

    /// What the machine knows of itself, to update once a runtime serves it.
    fn facts_mut(&mut self) -> &mut MachineFacts;

    /// The formed cell of a founding member, `None` on an admitted machine.
    fn formed(&self) -> Option<&FormedCell>;

    /// Start what the machine serves beside the journals on `rpc`, until
    /// `shutdown`; `stopping` is the clean stop the coordinator hands its
    /// term on at.
    ///
    /// # Errors
    ///
    /// When a service cannot be registered on `rpc`.
    fn serve<P: ParosProviders>(
        self,
        providers: &P,
        rpc: &RpcHandle<P>,
        tunables: &DriverTunables,
        observer: Option<Arc<dyn CallObserver>>,
        shutdown: (CancellationToken, CancellationToken),
    ) -> Result<Hosted, RunError>;
}

/// The host of a node outside a machine: there is none.
#[derive(Clone, Copy, Debug)]
pub enum NoHost {}

impl CellHost for NoHost {
    fn control_journals(&self) -> ControlJournals {
        match *self {}
    }

    fn facts(&self) -> &MachineFacts {
        match *self {}
    }

    fn facts_mut(&mut self) -> &mut MachineFacts {
        match *self {}
    }

    fn formed(&self) -> Option<&FormedCell> {
        match *self {}
    }

    fn serve<P: ParosProviders>(
        self,
        _: &P,
        _: &RpcHandle<P>,
        _: &DriverTunables,
        _: Option<Arc<dyn CallObserver>>,
        _: (CancellationToken, CancellationToken),
    ) -> Result<Hosted, RunError> {
        match self {}
    }
}
