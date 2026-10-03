//! The **system journals** (#189): the directory (the user tenant's control
//! journal) and the node registry (the cell tenant's control journal, #235).
//! A service must create and delete journals, and add
//! and retire nodes, while it runs; paros already has the right tool for
//! both — a replicated log — so both lists are journals of their own, and
//! every node learns them by reading them.
//!
//! This module is the one reading of their entries: the typed
//! [`SystemCommand`] a client writes (one record per position, framed by
//! [`SystemCommand::encode`]), and the two pure folds — [`Directory`] and
//! [`Registry`] — that every node, and every client reading back its own
//! request, runs over the chosen entries in position order. A fold is a function
//! of the log alone, so every reader that has folded a prefix agrees on it.
//!
//! **This is not an application** (#186): paros still decides nothing about
//! the bytes of a user journal. The system journals are paros's own control
//! plane, like the matchmaker registry; the core keeps their entries as
//! opaque as any other, and only this module and the driver read them.
//!
//! - **Directory.** A created journal's id is random (#226, #235): its
//!   creator draws it from the user range ([`JournalId::FIRST_USER`] and up)
//!   and this fold, the tenant's single writer of journal ids, checks it at
//!   apply. An id outside the user range or naming a journal the deployment
//!   was booted with (its *genesis* journals) folds to
//!   [`DirectoryRefusal::Reserved`]; an id the directory already created —
//!   deleted or not, ids are never reused — folds to
//!   [`DirectoryRefusal::IdTaken`], and the creator redraws. Never a log
//!   position: an id must not change when its tenant moves. Of two creates
//!   with one name the lower position wins; the other folds to
//!   [`DirectoryRefusal::NameTaken`], which its creator reads back. Names
//!   are opaque bytes.
//! - **Registry** (#211), keyed by `node_id` (random, minted at format). The
//!   node pool is the genesis pool plus every registered node not yet
//!   retired ([`Registry::pool`]). A node registers with its class
//!   (`storage` or `stateless`) and its capacity (role slots of its class);
//!   a reboot registers the same id again, updating address and capacity,
//!   never class. It is drained, then retired — an id is never reused, a
//!   retired one included. Capacity **bookings** (`BookCapacity`, written
//!   by the cell coordinator) are judged at apply: a slot of the node's own
//!   class only, never past its capacity. The registry is checkpointed with
//!   `paros::client::checkpoint` (#230): its state is the latest entry per
//!   `node_id` and the live bookings.
//!
//!   Not yet (follow-ups of #211): a machine registering itself at start
//!   and on a cadence (it needs the cell coordinator of #225 as the
//!   registry's single writer), the `InterfaceRef` of #216, and placement
//!   by booking (#212). The directory is not checkpointed yet (#229).
//!
//! Every malformed entry — a record that does not decode,
//! a configuration that does not admit its quorum system, a registry entry
//! in the directory — folds to a refusal, never a panic: the entries are
//! external input.

use std::collections::{BTreeMap, BTreeSet};

use paros_core::{AcceptorConfig, JournalId, JournalKey, NodeId, TenantId};
use prost::Message as _;

use crate::client::checkpoint::{Checkpointable, Folded};
pub use crate::machine::Class;

use crate::rpc::system as wire;
use crate::rpc::{config_from_proto, config_to_proto};

/// The directory: the journals created and deleted at runtime — the user
/// tenant's own control journal, which holds its journal names (#235,
/// `docs/architecture.md` §3.1). One user tenant today ([`TenantId::default`]);
/// tenant creation is #210.
pub const DIRECTORY: JournalKey = JournalKey::control(TenantId::FIRST_USER);

/// The node registry: the nodes registered, drained and retired at runtime —
/// the cell tenant's control journal (#235, §3.1).
pub const REGISTRY: JournalKey = JournalKey::control(TenantId::CELL);

/// Whether `journal` is one of the two system journals.
#[must_use]
pub fn is_system(journal: JournalKey) -> bool {
    journal == DIRECTORY || journal == REGISTRY
}

/// One system-journal entry, as a client appends it and a fold reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SystemCommand {
    /// Create journal `id` named `name` over the static configuration
    /// `config`.
    CreateJournal {
        /// The id its creator drew (user range); refused at apply when
        /// reserved or taken.
        id: JournalId,
        /// Opaque bytes; paros never interprets them.
        name: Vec<u8>,
        /// The journal's static acceptor configuration.
        config: AcceptorConfig,
    },
    /// Delete journal `id` (a tombstone, never a reuse).
    DeleteJournal {
        /// The journal to delete.
        id: JournalId,
    },
    /// Add node `id`, reachable at `addr`, to the pool as a machine of
    /// `class` with `capacity` role slots — or, for a node registered
    /// already, register it again (a reboot).
    RegisterNode {
        /// The node's identity (random, minted at format).
        id: NodeId,
        /// Its address.
        addr: String,
        /// Its class.
        class: Class,
        /// The role slots of its class it advertises.
        capacity: u64,
        /// Its failure domain (opaque; placement reads it).
        failure_domain: String,
    },
    /// Stop placing new work on node `id`.
    DrainNode {
        /// The node to drain.
        id: NodeId,
    },
    /// Remove drained node `id` from the pool for good.
    RetireNode {
        /// The node to retire.
        id: NodeId,
    },
    /// Book one `class` slot of `node` for `journal` (#211): written by the
    /// cell coordinator, the registry's single writer.
    BookCapacity {
        /// The id its writer drew; refused while a live booking holds it.
        booking: u64,
        /// The node.
        node: NodeId,
        /// The slot's class.
        class: Class,
        /// The journal the slot is for.
        journal: JournalKey,
    },
    /// Release a booking.
    ReleaseCapacity {
        /// The booking.
        booking: u64,
    },
}

impl SystemCommand {
    /// The record a client writes: exactly one per position.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        use wire::system_entry::Kind;
        let kind = match self {
            SystemCommand::CreateJournal { id, name, config } => {
                Kind::CreateJournal(wire::CreateJournal {
                    name: name.clone(),
                    config: Some(config_to_proto(config)),
                    id: id.0,
                })
            }
            SystemCommand::DeleteJournal { id } => {
                Kind::DeleteJournal(wire::DeleteJournal { id: id.0 })
            }
            SystemCommand::RegisterNode {
                id,
                addr,
                class,
                capacity,
                failure_domain,
            } => Kind::RegisterNode(wire::RegisterNode {
                id: id.0,
                addr: addr.clone(),
                failure_domain: failure_domain.clone(),
                class: class.as_str().into(),
                capacity: *capacity,
            }),
            SystemCommand::DrainNode { id } => Kind::DrainNode(wire::DrainNode { id: id.0 }),
            SystemCommand::RetireNode { id } => Kind::RetireNode(wire::RetireNode { id: id.0 }),
            SystemCommand::BookCapacity {
                booking,
                node,
                class,
                journal,
            } => Kind::BookCapacity(wire::BookCapacity {
                booking: *booking,
                node: node.0,
                class: class.as_str().into(),
                tenant: journal.tenant.0,
                journal: journal.journal.0,
            }),
            SystemCommand::ReleaseCapacity { booking } => {
                Kind::ReleaseCapacity(wire::ReleaseCapacity { booking: *booking })
            }
        };
        wire::SystemEntry { kind: Some(kind) }.encode_to_vec()
    }

    /// Read one record back.
    ///
    /// # Errors
    ///
    /// The record is not a system entry, names no kind, or carries a
    /// configuration that does not admit its quorum system.
    pub fn decode(record: &[u8]) -> Result<Self, &'static str> {
        use wire::system_entry::Kind;
        let entry = wire::SystemEntry::decode(record).map_err(|_| "not a system entry")?;
        Ok(match entry.kind.ok_or("a system entry names no kind")? {
            Kind::CreateJournal(create) => SystemCommand::CreateJournal {
                id: JournalId(create.id),
                name: create.name,
                config: config_from_proto(create.config)?
                    .ok_or("a created journal names no configuration")?,
            },
            Kind::DeleteJournal(delete) => SystemCommand::DeleteJournal {
                id: JournalId(delete.id),
            },
            Kind::RegisterNode(register) => SystemCommand::RegisterNode {
                id: NodeId(register.id),
                addr: register.addr,
                class: register.class.parse()?,
                capacity: register.capacity,
                failure_domain: register.failure_domain,
            },
            Kind::DrainNode(drain) => SystemCommand::DrainNode {
                id: NodeId(drain.id),
            },
            Kind::RetireNode(retire) => SystemCommand::RetireNode {
                id: NodeId(retire.id),
            },
            Kind::BookCapacity(book) => SystemCommand::BookCapacity {
                booking: book.booking,
                node: NodeId(book.node),
                class: book.class.parse()?,
                journal: JournalKey::new(TenantId(book.tenant), JournalId(book.journal)),
            },
            Kind::ReleaseCapacity(release) => SystemCommand::ReleaseCapacity {
                booking: release.booking,
            },
        })
    }
}

/// A journal the directory created.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreatedJournal {
    /// Its name.
    pub name: Vec<u8>,
    /// Its static acceptor configuration.
    pub config: AcceptorConfig,
    /// The position of the `DeleteJournal` that tombstoned it, if any.
    pub deleted_at: Option<u64>,
}

/// What one directory record folded to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DirectoryEvent {
    /// A journal was created with this id.
    Created {
        /// The id its creator drew.
        id: JournalId,
        /// Its name.
        name: Vec<u8>,
        /// Its configuration.
        config: AcceptorConfig,
    },
    /// A journal was tombstoned.
    Deleted {
        /// The deleted journal.
        id: JournalId,
    },
    /// The entry changed nothing.
    Refused(DirectoryRefusal),
}

/// Why a directory entry changed nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DirectoryRefusal {
    /// Not exactly one decodable directory entry.
    Malformed,
    /// The id is outside the user range, or a genesis journal's.
    Reserved {
        /// The id asked for.
        id: JournalId,
    },
    /// The directory already created this id (ids are never reused, a
    /// deleted one included): the creator redraws.
    IdTaken {
        /// The id asked for.
        id: JournalId,
    },
    /// A live journal already holds the name: the lower position won.
    NameTaken {
        /// The journal that holds it.
        winner: JournalId,
    },
    /// A delete of a journal the directory never created, or already deleted.
    UnknownJournal {
        /// The journal named.
        id: JournalId,
    },
}

/// The directory's fold: every journal created at runtime, by id, with its
/// tombstone. Genesis journals — the ones the deployment was booted with —
/// are reserved ids the directory never allocates and never deletes.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Directory {
    genesis: BTreeSet<JournalId>,
    journals: BTreeMap<JournalId, CreatedJournal>,
    /// Live names, to the journal holding each.
    names: BTreeMap<Vec<u8>, JournalId>,
    next_seq: u64,
}

impl Directory {
    /// An empty directory over the deployment's `genesis` journals.
    #[must_use]
    pub fn new(genesis: impl IntoIterator<Item = JournalId>) -> Self {
        Self {
            genesis: genesis.into_iter().collect(),
            ..Self::default()
        }
    }

    /// The next position this fold expects (one past the last folded one).
    #[must_use]
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Fold the record at position `seq` of the directory (in position order;
    /// a gap is simply skipped).
    ///
    /// # Panics
    ///
    /// If `seq` is below a position already folded: a fold is fed in log order,
    /// once (a programmer error of the caller, never an operating one).
    pub fn fold(&mut self, seq: u64, record: &[u8]) -> DirectoryEvent {
        assert!(
            seq >= self.next_seq,
            "the directory folds in position order"
        );
        self.next_seq = seq + 1;
        match SystemCommand::decode(record).ok() {
            Some(SystemCommand::CreateJournal { id, name, config }) => {
                if !id.is_user() || self.genesis.contains(&id) {
                    return DirectoryEvent::Refused(DirectoryRefusal::Reserved { id });
                }
                if self.journals.contains_key(&id) {
                    return DirectoryEvent::Refused(DirectoryRefusal::IdTaken { id });
                }
                if let Some(&winner) = self.names.get(&name) {
                    return DirectoryEvent::Refused(DirectoryRefusal::NameTaken { winner });
                }
                self.names.insert(name.clone(), id);
                self.journals.insert(
                    id,
                    CreatedJournal {
                        name: name.clone(),
                        config: config.clone(),
                        deleted_at: None,
                    },
                );
                DirectoryEvent::Created { id, name, config }
            }
            Some(SystemCommand::DeleteJournal { id }) => {
                let Some(created) = self
                    .journals
                    .get_mut(&id)
                    .filter(|c| c.deleted_at.is_none())
                else {
                    return DirectoryEvent::Refused(DirectoryRefusal::UnknownJournal { id });
                };
                created.deleted_at = Some(seq);
                self.names.remove(&created.name);
                DirectoryEvent::Deleted { id }
            }
            _ => DirectoryEvent::Refused(DirectoryRefusal::Malformed),
        }
    }

    /// The journal the directory created as `id`, deleted or not.
    #[must_use]
    pub fn get(&self, id: JournalId) -> Option<&CreatedJournal> {
        self.journals.get(&id)
    }

    /// Whether `id` was created and then deleted.
    #[must_use]
    pub fn is_deleted(&self, id: JournalId) -> bool {
        self.journals
            .get(&id)
            .is_some_and(|c| c.deleted_at.is_some())
    }

    /// Every journal created so far, deleted ones included, in id order.
    pub fn journals(&self) -> impl Iterator<Item = (JournalId, &CreatedJournal)> {
        self.journals.iter().map(|(id, c)| (*id, c))
    }
}

/// Where a registered node stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeStanding {
    /// In the pool.
    Registered,
    /// In the pool, taking no new work.
    Draining,
    /// Out of the pool for good.
    Retired,
}

impl NodeStanding {
    fn to_wire(self) -> u32 {
        match self {
            NodeStanding::Registered => 0,
            NodeStanding::Draining => 1,
            NodeStanding::Retired => 2,
        }
    }

    fn from_wire(standing: u32) -> Result<Self, &'static str> {
        match standing {
            0 => Ok(NodeStanding::Registered),
            1 => Ok(NodeStanding::Draining),
            2 => Ok(NodeStanding::Retired),
            _ => Err("a node standing is registered, draining or retired"),
        }
    }
}

/// A node the registry registered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisteredNode {
    /// Its address (the latest registration's).
    pub addr: String,
    /// Its failure domain.
    pub failure_domain: String,
    /// Its class, fixed by its first registration.
    pub class: Class,
    /// The role slots of its class it advertises (the latest registration's).
    pub capacity: u64,
    /// Where it stands.
    pub standing: NodeStanding,
    /// How many registrations it made: 1, plus one per re-registration (a
    /// reboot).
    pub registrations: u64,
}

/// One capacity booking: a role slot of `node` held for `journal`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Booking {
    /// The node whose slot it holds.
    pub node: NodeId,
    /// The class of the slot (always the node's).
    pub class: Class,
    /// The journal it was booked for.
    pub journal: JournalKey,
}

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
        /// The slot's class.
        class: Class,
    },
    /// A booking was released.
    Released {
        /// The booking's id.
        booking: u64,
        /// The node whose slot it freed.
        node: NodeId,
    },
    /// A checkpoint (#230): every position below `covers_up_to` is in the
    /// state this fold now holds — restored from it, or verified against it
    /// (the fold reports which through its audit, not here, so every fold
    /// of the same position folds the same event).
    Checkpoint {
        /// The checkpoint's horizon, its own position.
        covers_up_to: u64,
    },
    /// The entry changed nothing.
    Refused(RegistryRefusal),
}

/// Why a registry entry changed nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegistryRefusal {
    /// Not exactly one decodable registry entry (or a checkpoint this fold
    /// cannot use).
    Malformed,
    /// The id is a genesis node or a retired one (ids are never reused).
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
    /// A booking under an id a live booking holds: its writer redraws.
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
    /// A release of a booking that is not live.
    UnknownBooking {
        /// The id named.
        booking: u64,
    },
}

/// The registry's fold (#189, #211): the genesis pool the deployment was
/// booted with, every node registered at runtime keyed by its `node_id`
/// with where it stands, and every live capacity booking.
///
/// The fold enforces the placement's two rules at apply, where every
/// reader agrees on them: a booking takes a slot of the node's own class
/// ([`RegistryRefusal::WrongClass`]), and a node is never booked past its
/// capacity ([`RegistryRefusal::NoCapacity`]). It is
/// [`Checkpointable`]: its checkpoint is every runtime node and live
/// booking, never the history that made them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Registry {
    genesis: BTreeSet<NodeId>,
    nodes: BTreeMap<NodeId, RegisteredNode>,
    bookings: BTreeMap<u64, Booking>,
    next_seq: u64,
}

impl Registry {
    /// An empty registry over the deployment's `genesis` pool.
    #[must_use]
    pub fn new(genesis: impl IntoIterator<Item = NodeId>) -> Self {
        Self {
            genesis: genesis.into_iter().collect(),
            ..Self::default()
        }
    }

    /// The next position this fold expects.
    #[must_use]
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Fold the record at position `seq` of the registry (in position
    /// order). A checkpoint record is not an entry: fold through a
    /// [`Folder`](crate::client::checkpoint::Folder), which restores it.
    ///
    /// # Panics
    ///
    /// If `seq` is below a position already folded (see [`Directory::fold`]).
    pub fn fold(&mut self, seq: u64, record: &[u8]) -> RegistryEvent {
        assert!(seq >= self.next_seq, "the registry folds in position order");
        self.next_seq = seq + 1;
        match SystemCommand::decode(record).ok() {
            Some(SystemCommand::RegisterNode {
                id,
                addr,
                class,
                capacity,
                failure_domain,
            }) => self.register(id, addr, class, capacity, failure_domain),
            Some(SystemCommand::DrainNode { id }) => match self.nodes.get_mut(&id) {
                Some(node) if node.standing == NodeStanding::Registered => {
                    node.standing = NodeStanding::Draining;
                    RegistryEvent::Draining { id }
                }
                _ => RegistryEvent::Refused(RegistryRefusal::NotRegistered { id }),
            },
            Some(SystemCommand::RetireNode { id }) => match self.nodes.get_mut(&id) {
                Some(node) if node.standing == NodeStanding::Draining => {
                    node.standing = NodeStanding::Retired;
                    self.bookings.retain(|_, booking| booking.node != id);
                    RegistryEvent::Retired { id }
                }
                _ => RegistryEvent::Refused(RegistryRefusal::NotDraining { id }),
            },
            Some(SystemCommand::BookCapacity {
                booking,
                node,
                class,
                journal,
            }) => self.book(booking, node, class, journal),
            Some(SystemCommand::ReleaseCapacity { booking }) => {
                match self.bookings.remove(&booking) {
                    Some(held) => RegistryEvent::Released {
                        booking,
                        node: held.node,
                    },
                    None => RegistryEvent::Refused(RegistryRefusal::UnknownBooking { booking }),
                }
            }
            _ => RegistryEvent::Refused(RegistryRefusal::Malformed),
        }
    }

    fn register(
        &mut self,
        id: NodeId,
        addr: String,
        class: Class,
        capacity: u64,
        failure_domain: String,
    ) -> RegistryEvent {
        if self.genesis.contains(&id) {
            return RegistryEvent::Refused(RegistryRefusal::AlreadyKnown { id });
        }
        match self.nodes.get_mut(&id) {
            Some(node) if node.standing == NodeStanding::Retired => {
                RegistryEvent::Refused(RegistryRefusal::AlreadyKnown { id })
            }
            Some(node) if node.class != class => {
                RegistryEvent::Refused(RegistryRefusal::ClassChanged { id })
            }
            Some(node) => {
                node.addr.clone_from(&addr);
                node.capacity = capacity;
                node.failure_domain = failure_domain;
                node.registrations += 1;
                RegistryEvent::Reregistered { id, addr, capacity }
            }
            None => {
                self.nodes.insert(
                    id,
                    RegisteredNode {
                        addr: addr.clone(),
                        failure_domain,
                        class,
                        capacity,
                        standing: NodeStanding::Registered,
                        registrations: 1,
                    },
                );
                RegistryEvent::Registered {
                    id,
                    addr,
                    class,
                    capacity,
                }
            }
        }
    }

    fn book(
        &mut self,
        booking: u64,
        id: NodeId,
        class: Class,
        journal: JournalKey,
    ) -> RegistryEvent {
        if self.bookings.contains_key(&booking) {
            return RegistryEvent::Refused(RegistryRefusal::BookingTaken { booking });
        }
        let Some(node) = self
            .nodes
            .get(&id)
            .filter(|n| n.standing == NodeStanding::Registered)
        else {
            return RegistryEvent::Refused(RegistryRefusal::NotRegistered { id });
        };
        if node.class != class {
            return RegistryEvent::Refused(RegistryRefusal::WrongClass { id });
        }
        if self.booked(id) >= node.capacity {
            return RegistryEvent::Refused(RegistryRefusal::NoCapacity { id });
        }
        self.bookings.insert(
            booking,
            Booking {
                node: id,
                class,
                journal,
            },
        );
        RegistryEvent::Booked {
            booking,
            node: id,
            class,
        }
    }

    /// Whether `id` is in the pool: a genesis node, or registered and not
    /// retired.
    #[must_use]
    pub fn contains(&self, id: NodeId) -> bool {
        self.genesis.contains(&id)
            || self
                .nodes
                .get(&id)
                .is_some_and(|n| n.standing != NodeStanding::Retired)
    }

    /// The pool, sorted: the genesis nodes and every registered node not
    /// retired.
    #[must_use]
    pub fn pool(&self) -> Vec<NodeId> {
        let mut pool: BTreeSet<NodeId> = self.genesis.clone();
        pool.extend(
            self.nodes
                .iter()
                .filter(|(_, n)| n.standing != NodeStanding::Retired)
                .map(|(id, _)| *id),
        );
        pool.into_iter().collect()
    }

    /// The node registered as `id`, whatever its standing.
    #[must_use]
    pub fn get(&self, id: NodeId) -> Option<&RegisteredNode> {
        self.nodes.get(&id)
    }

    /// Every node registered at runtime, in id order.
    pub fn nodes(&self) -> impl Iterator<Item = (NodeId, &RegisteredNode)> {
        self.nodes.iter().map(|(id, n)| (*id, n))
    }

    /// The slots of `id` its live bookings hold.
    #[must_use]
    pub fn booked(&self, id: NodeId) -> u64 {
        self.bookings.values().filter(|b| b.node == id).count() as u64
    }

    /// The slots `id` has left: its capacity less its live bookings (`0` for
    /// a node that is not registered and in the pool, or over-booked after a
    /// re-registration lowered its capacity).
    #[must_use]
    pub fn available(&self, id: NodeId) -> u64 {
        self.nodes
            .get(&id)
            .filter(|n| n.standing == NodeStanding::Registered)
            .map_or(0, |n| n.capacity.saturating_sub(self.booked(id)))
    }

    /// The live booking `booking`.
    #[must_use]
    pub fn booking(&self, booking: u64) -> Option<&Booking> {
        self.bookings.get(&booking)
    }

    /// Every live booking, in id order.
    pub fn bookings(&self) -> impl Iterator<Item = (u64, &Booking)> {
        self.bookings.iter().map(|(id, b)| (*id, b))
    }

    fn state_to_wire(&self) -> wire::RegistryState {
        wire::RegistryState {
            nodes: self
                .nodes
                .iter()
                .map(|(id, n)| wire::RegisteredNodeState {
                    id: id.0,
                    addr: n.addr.clone(),
                    failure_domain: n.failure_domain.clone(),
                    class: n.class.as_str().into(),
                    capacity: n.capacity,
                    standing: n.standing.to_wire(),
                    registrations: n.registrations,
                })
                .collect(),
            bookings: self
                .bookings
                .iter()
                .map(|(id, b)| wire::BookingState {
                    booking: *id,
                    node: b.node.0,
                    class: b.class.as_str().into(),
                    tenant: b.journal.tenant.0,
                    journal: b.journal.journal.0,
                })
                .collect(),
        }
    }
}

impl Checkpointable for Registry {
    type Event = RegistryEvent;

    fn apply(&mut self, seq: u64, record: &[u8]) -> RegistryEvent {
        self.fold(seq, record)
    }

    fn checkpoint(&self) -> Vec<u8> {
        self.state_to_wire().encode_to_vec()
    }

    fn restore(&mut self, covers_up_to: u64, state: &[u8]) -> Result<(), &'static str> {
        let state =
            wire::RegistryState::decode(state).map_err(|_| "a registry state does not decode")?;
        let mut nodes = BTreeMap::new();
        for n in state.nodes {
            let id = NodeId(n.id);
            if self.genesis.contains(&id) {
                return Err("a registry state names a genesis node");
            }
            nodes.insert(
                id,
                RegisteredNode {
                    addr: n.addr,
                    failure_domain: n.failure_domain,
                    class: n.class.parse()?,
                    capacity: n.capacity,
                    standing: NodeStanding::from_wire(n.standing)?,
                    registrations: n.registrations,
                },
            );
        }
        let mut bookings = BTreeMap::new();
        for b in state.bookings {
            bookings.insert(
                b.booking,
                Booking {
                    node: NodeId(b.node),
                    class: b.class.parse()?,
                    journal: JournalKey::new(TenantId(b.tenant), JournalId(b.journal)),
                },
            );
        }
        self.nodes = nodes;
        self.bookings = bookings;
        self.next_seq = covers_up_to + 1;
        Ok(())
    }
}

/// The event a [`Folded`] registry record is reported as: an entry's own
/// event, a checkpoint's, or a refusal for a record the fold cannot use.
/// `None` for a record the fold skipped (above a gap) or is waiting on.
#[must_use]
pub fn registry_event(folded: Folded<RegistryEvent>) -> Option<RegistryEvent> {
    match folded {
        Folded::Entry(event) => Some(event),
        Folded::Checkpoint { covers_up_to, .. } => Some(RegistryEvent::Checkpoint { covers_up_to }),
        Folded::Unreadable(_) => Some(RegistryEvent::Refused(RegistryRefusal::Malformed)),
        Folded::NeedsRef(_) | Folded::Skipped => None,
    }
}

/// What one system-journal record folded to — the directory's or the
/// registry's event — as the driver reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SystemEvent {
    /// A directory record.
    Directory(DirectoryEvent),
    /// A registry record.
    Registry(RegistryEvent),
}

#[cfg(test)]
mod tests {
    use super::*;
    use paros_core::QuorumSystem;

    fn config(members: &[u64]) -> AcceptorConfig {
        AcceptorConfig::new(
            members.iter().copied().map(NodeId).collect(),
            QuorumSystem::Majority,
        )
    }

    fn create(id: u64, name: &[u8], members: &[u64]) -> Vec<u8> {
        SystemCommand::CreateJournal {
            id: JournalId(id),
            name: name.to_vec(),
            config: config(members),
        }
        .encode()
    }

    fn one(command: &SystemCommand) -> Vec<u8> {
        command.encode()
    }

    #[test]
    fn every_command_round_trips() {
        let commands = [
            SystemCommand::CreateJournal {
                id: JournalId(0x9e37_79b9),
                name: b"orders".to_vec(),
                config: config(&[0, 1, 2]),
            },
            SystemCommand::DeleteJournal { id: JournalId(131) },
            SystemCommand::RegisterNode {
                id: NodeId(100),
                addr: "10.0.5.1:4500".into(),
                class: Class::Storage,
                capacity: 3,
                failure_domain: "rack-a".into(),
            },
            SystemCommand::DrainNode { id: NodeId(100) },
            SystemCommand::RetireNode { id: NodeId(100) },
            SystemCommand::BookCapacity {
                booking: 7,
                node: NodeId(100),
                class: Class::Stateless,
                journal: JournalKey::new(TenantId(300), JournalId(400)),
            },
            SystemCommand::ReleaseCapacity { booking: 7 },
        ];
        for command in commands {
            assert_eq!(SystemCommand::decode(&command.encode()), Ok(command));
        }
        assert!(SystemCommand::decode(b"\xff\xff").is_err());
    }

    #[test]
    fn a_created_journal_takes_its_drawn_id_and_an_id_is_never_reused() {
        let genesis = JournalId::FIRST_USER;
        let mut dir = Directory::new([genesis]);
        // A genesis id and an id in the reserved range are refused.
        assert_eq!(
            dir.fold(0, &create(genesis.0, b"a", &[0, 1, 2])),
            DirectoryEvent::Refused(DirectoryRefusal::Reserved { id: genesis })
        );
        assert_eq!(
            dir.fold(1, &create(1, b"a", &[0, 1, 2])),
            DirectoryEvent::Refused(DirectoryRefusal::Reserved { id: JournalId(1) })
        );
        let drawn = JournalId(0xdead_beef);
        assert!(matches!(
            dir.fold(3, &create(drawn.0, b"a", &[0, 1, 2])),
            DirectoryEvent::Created { id, .. } if id == drawn
        ));
        // A second create of the same id is refused: the creator redraws.
        assert_eq!(
            dir.fold(4, &create(drawn.0, b"b", &[0, 1, 2])),
            DirectoryEvent::Refused(DirectoryRefusal::IdTaken { id: drawn })
        );
        assert_eq!(
            dir.fold(5, &one(&SystemCommand::DeleteJournal { id: drawn })),
            DirectoryEvent::Deleted { id: drawn }
        );
        assert!(dir.is_deleted(drawn));
        // A deleted journal is not deleted twice, its id is never reused,
        // and its name frees up for a new journal under a new id.
        assert_eq!(
            dir.fold(6, &one(&SystemCommand::DeleteJournal { id: drawn })),
            DirectoryEvent::Refused(DirectoryRefusal::UnknownJournal { id: drawn })
        );
        assert_eq!(
            dir.fold(7, &create(drawn.0, b"a", &[0, 1, 2])),
            DirectoryEvent::Refused(DirectoryRefusal::IdTaken { id: drawn })
        );
        assert!(matches!(
            dir.fold(8, &create(drawn.0 + 1, b"a", &[0, 1, 2])),
            DirectoryEvent::Created { id, .. } if id.0 == drawn.0 + 1
        ));
        assert!(dir.is_deleted(drawn), "the tombstone stays");
    }

    #[test]
    fn a_name_race_is_decided_by_position_order() {
        let mut dir = Directory::new([]);
        assert!(matches!(
            dir.fold(0, &create(300, b"x", &[0])),
            DirectoryEvent::Created {
                id: JournalId(300),
                ..
            }
        ));
        assert_eq!(
            dir.fold(1, &create(301, b"x", &[1])),
            DirectoryEvent::Refused(DirectoryRefusal::NameTaken {
                winner: JournalId(300)
            })
        );
    }

    #[test]
    fn malformed_records_change_nothing() {
        let mut dir = Directory::new([]);
        assert_eq!(
            dir.fold(0, b""),
            DirectoryEvent::Refused(DirectoryRefusal::Malformed)
        );
        assert_eq!(
            dir.fold(1, b"junk"),
            DirectoryEvent::Refused(DirectoryRefusal::Malformed)
        );
        assert_eq!(
            dir.fold(2, &one(&SystemCommand::DrainNode { id: NodeId(1) })),
            DirectoryEvent::Refused(DirectoryRefusal::Malformed),
            "a registry entry is not a directory entry"
        );
        // A grid that does not tile its membership is refused at decode.
        let bad = wire::SystemEntry {
            kind: Some(wire::system_entry::Kind::CreateJournal(
                wire::CreateJournal {
                    id: 300,
                    name: b"g".to_vec(),
                    config: Some(crate::rpc::common::AcceptorConfig {
                        members: vec![0, 1, 2],
                        quorum_system: crate::rpc::common::QuorumSystem::Grid.into(),
                        rows: 2,
                        cols: 2,
                        ..Default::default()
                    }),
                },
            )),
        }
        .encode_to_vec();
        assert_eq!(
            dir.fold(3, &bad),
            DirectoryEvent::Refused(DirectoryRefusal::Malformed)
        );
        assert_eq!(dir.journals().count(), 0);
    }

    #[test]
    fn the_pool_grows_by_registration_and_shrinks_by_retirement() {
        let mut reg = Registry::new([NodeId(0), NodeId(1)]);
        let register = |id: u64| register(id, Class::Storage, 1);
        assert_eq!(
            reg.fold(0, &register(0)),
            RegistryEvent::Refused(RegistryRefusal::AlreadyKnown { id: NodeId(0) })
        );
        assert!(matches!(
            reg.fold(1, &register(100)),
            RegistryEvent::Registered { .. }
        ));
        assert_eq!(reg.pool(), vec![NodeId(0), NodeId(1), NodeId(100)]);
        // Retiring needs a drain first.
        assert_eq!(
            reg.fold(2, &one(&SystemCommand::RetireNode { id: NodeId(100) })),
            RegistryEvent::Refused(RegistryRefusal::NotDraining { id: NodeId(100) })
        );
        assert_eq!(
            reg.fold(3, &one(&SystemCommand::DrainNode { id: NodeId(100) })),
            RegistryEvent::Draining { id: NodeId(100) }
        );
        assert!(
            reg.contains(NodeId(100)),
            "a draining node stays in the pool"
        );
        assert_eq!(
            reg.fold(4, &one(&SystemCommand::RetireNode { id: NodeId(100) })),
            RegistryEvent::Retired { id: NodeId(100) }
        );
        assert!(!reg.contains(NodeId(100)));
        assert_eq!(reg.pool(), vec![NodeId(0), NodeId(1)]);
        // A retired id is never registered again, a re-registration
        // included.
        assert_eq!(
            reg.fold(5, &register(100)),
            RegistryEvent::Refused(RegistryRefusal::AlreadyKnown { id: NodeId(100) })
        );
        // Genesis nodes are neither drained nor retired.
        assert_eq!(
            reg.fold(6, &one(&SystemCommand::DrainNode { id: NodeId(0) })),
            RegistryEvent::Refused(RegistryRefusal::NotRegistered { id: NodeId(0) })
        );
    }

    fn register(id: u64, class: Class, capacity: u64) -> Vec<u8> {
        one(&SystemCommand::RegisterNode {
            id: NodeId(id),
            addr: format!("n{id}"),
            class,
            capacity,
            failure_domain: String::new(),
        })
    }

    fn book(booking: u64, node: u64, class: Class) -> Vec<u8> {
        one(&SystemCommand::BookCapacity {
            booking,
            node: NodeId(node),
            class,
            journal: JournalKey::new(TenantId(300), JournalId(booking)),
        })
    }

    #[test]
    fn a_reboot_registers_again_under_the_same_class() {
        let mut reg = Registry::new([NodeId(0)]);
        assert!(matches!(
            reg.fold(0, &register(100, Class::Storage, 2)),
            RegistryEvent::Registered { .. }
        ));
        assert_eq!(
            reg.fold(1, &register(100, Class::Storage, 5)),
            RegistryEvent::Reregistered {
                id: NodeId(100),
                addr: "n100".into(),
                capacity: 5
            }
        );
        let node = reg.get(NodeId(100)).expect("registered");
        assert_eq!((node.capacity, node.registrations), (5, 2));
        assert_eq!(
            reg.fold(2, &register(100, Class::Stateless, 5)),
            RegistryEvent::Refused(RegistryRefusal::ClassChanged { id: NodeId(100) })
        );
    }

    #[test]
    fn a_booking_takes_a_slot_of_the_nodes_own_class_within_its_capacity() {
        let mut reg = Registry::new([NodeId(0)]);
        reg.fold(0, &register(100, Class::Storage, 2));
        reg.fold(1, &register(200, Class::Stateless, 1));
        // A storage machine never takes stateless work, and the reverse.
        assert_eq!(
            reg.fold(2, &book(1, 100, Class::Stateless)),
            RegistryEvent::Refused(RegistryRefusal::WrongClass { id: NodeId(100) })
        );
        assert_eq!(
            reg.fold(3, &book(1, 200, Class::Storage)),
            RegistryEvent::Refused(RegistryRefusal::WrongClass { id: NodeId(200) })
        );
        // A genesis node, or one never registered, is not bookable.
        assert_eq!(
            reg.fold(4, &book(1, 0, Class::Storage)),
            RegistryEvent::Refused(RegistryRefusal::NotRegistered { id: NodeId(0) })
        );
        assert!(matches!(
            reg.fold(5, &book(1, 100, Class::Storage)),
            RegistryEvent::Booked { booking: 1, .. }
        ));
        // A live booking id is booked at most once.
        assert_eq!(
            reg.fold(6, &book(1, 100, Class::Storage)),
            RegistryEvent::Refused(RegistryRefusal::BookingTaken { booking: 1 })
        );
        assert!(matches!(
            reg.fold(7, &book(2, 100, Class::Storage)),
            RegistryEvent::Booked { booking: 2, .. }
        ));
        assert_eq!(reg.available(NodeId(100)), 0);
        // Capacity is honoured.
        assert_eq!(
            reg.fold(8, &book(3, 100, Class::Storage)),
            RegistryEvent::Refused(RegistryRefusal::NoCapacity { id: NodeId(100) })
        );
        assert_eq!(
            reg.fold(9, &one(&SystemCommand::ReleaseCapacity { booking: 1 })),
            RegistryEvent::Released {
                booking: 1,
                node: NodeId(100)
            }
        );
        assert_eq!(
            reg.fold(10, &one(&SystemCommand::ReleaseCapacity { booking: 1 })),
            RegistryEvent::Refused(RegistryRefusal::UnknownBooking { booking: 1 })
        );
        assert_eq!(reg.available(NodeId(100)), 1);
        // A draining node takes no new work; a retired one drops its bookings.
        reg.fold(11, &one(&SystemCommand::DrainNode { id: NodeId(100) }));
        assert_eq!(
            reg.fold(12, &book(3, 100, Class::Storage)),
            RegistryEvent::Refused(RegistryRefusal::NotRegistered { id: NodeId(100) })
        );
        reg.fold(13, &one(&SystemCommand::RetireNode { id: NodeId(100) }));
        assert_eq!(reg.bookings().count(), 0);
    }

    #[test]
    fn a_registry_restored_from_its_checkpoint_is_the_registry_folded_whole() {
        use crate::client::checkpoint::{CheckpointRecord, Folded, Folder};
        let records = [
            register(100, Class::Storage, 2),
            register(200, Class::Stateless, 1),
            book(1, 100, Class::Storage),
            register(100, Class::Storage, 3),
            one(&SystemCommand::DrainNode { id: NodeId(200) }),
        ];
        let mut whole = Folder::new(Registry::new([NodeId(0)]));
        for (seq, record) in (0..).zip(&records) {
            assert!(matches!(whole.fold(seq, record), Some(Folded::Entry(_))));
        }
        let at = records.len() as u64;
        let checkpoint = CheckpointRecord::Inline {
            covers_up_to: at,
            chunks: vec![whole.state().checkpoint()],
        }
        .encode();
        // The fold that held every position verifies the checkpoint.
        assert_eq!(
            whole.fold(at, &checkpoint),
            Some(Folded::Checkpoint {
                covers_up_to: at,
                verified: Some(true)
            })
        );
        // A fold that jumped to the floor restores from it, and agrees.
        let mut restored = Folder::new(Registry::new([NodeId(0)]));
        restored.jump(at);
        assert!(!restored.is_whole());
        assert_eq!(
            restored.fold(at, &checkpoint),
            Some(Folded::Checkpoint {
                covers_up_to: at,
                verified: None
            })
        );
        assert!(restored.is_whole());
        assert_eq!(restored.state().checkpoint(), whole.state().checkpoint());
        let next = register(300, Class::Storage, 1);
        assert_eq!(whole.fold(at + 1, &next), restored.fold(at + 1, &next));
        assert_eq!(restored.state(), whole.state());
    }
}
