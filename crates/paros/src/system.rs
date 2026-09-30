//! The **system journals** (#189): the directory (journal 1) and the node
//! registry (journal 2). A service must create and delete journals, and add
//! and retire nodes, while it runs; paros already has the right tool for
//! both — a replicated log — so both lists are journals of their own, and
//! every node learns them by reading them.
//!
//! This module is the one reading of their entries: the typed
//! [`SystemCommand`] a client appends (one record per slot, framed by
//! [`SystemCommand::encode`]), and the two pure folds — [`Directory`] and
//! [`Registry`] — that every node, and every client reading back its own
//! request, runs over the chosen entries in LSN order. A fold is a function
//! of the log alone, so every reader that has folded a prefix agrees on it.
//!
//! **This is not an application** (#186): paros still decides nothing about
//! the bytes of a user journal. The system journals are paros's own control
//! plane, like the matchmaker registry; the core keeps their entries as
//! opaque as any other, and only this module and the driver read them.
//!
//! - **Directory.** A created journal's id is `128 + the LSN of its
//!   CreateJournal entry` ([`JournalId::FIRST_USER`] plus the slot): the log
//!   order is the allocator, so there is no counter and no race, and an id
//!   is never reused (every slot is used once; a deleted journal leaves a
//!   tombstone). Of two creates with one name the lower LSN wins; the other
//!   folds to [`DirectoryRefusal::NameTaken`], which its creator reads back.
//!   A slot whose id lands on a journal the deployment was booted with (its
//!   *genesis* journals) folds to [`DirectoryRefusal::Reserved`]. Names are
//!   opaque bytes.
//! - **Registry.** The node pool is the genesis pool plus every registered
//!   node not yet retired ([`Registry::pool`]). A node is registered once
//!   (an id is never reused, a retired one included), drained, then retired.
//!
//! Every malformed entry — a slot that is not exactly one decodable record,
//! a configuration that does not admit its quorum system, a registry entry
//! in the directory — folds to a refusal, never a panic: the entries are
//! external input.

use std::collections::{BTreeMap, BTreeSet};

use paros_core::{AcceptorConfig, JournalId, NodeId};
use prost::Message as _;

use crate::rpc::system as wire;
use crate::rpc::{config_from_proto, config_to_proto};

/// The directory: the journals created and deleted at runtime.
pub const DIRECTORY: JournalId = JournalId(1);

/// The node registry: the nodes registered, drained and retired at runtime.
pub const REGISTRY: JournalId = JournalId(2);

/// Whether `journal` is one of the two system journals.
#[must_use]
pub fn is_system(journal: JournalId) -> bool {
    journal == DIRECTORY || journal == REGISTRY
}

/// One system-journal entry, as a client appends it and a fold reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SystemCommand {
    /// Create a journal named `name` over the static configuration `config`.
    CreateJournal {
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
    /// Add node `id`, reachable at `addr`, to the pool.
    RegisterNode {
        /// The node's identity.
        id: NodeId,
        /// Its address.
        addr: String,
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
}

impl SystemCommand {
    /// The record a client appends: exactly one per slot.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        use wire::system_entry::Kind;
        let kind = match self {
            SystemCommand::CreateJournal { name, config } => {
                Kind::CreateJournal(wire::CreateJournal {
                    name: name.clone(),
                    config: Some(config_to_proto(config)),
                })
            }
            SystemCommand::DeleteJournal { id } => {
                Kind::DeleteJournal(wire::DeleteJournal { id: id.0 })
            }
            SystemCommand::RegisterNode {
                id,
                addr,
                failure_domain,
            } => Kind::RegisterNode(wire::RegisterNode {
                id: id.0,
                addr: addr.clone(),
                failure_domain: failure_domain.clone(),
            }),
            SystemCommand::DrainNode { id } => Kind::DrainNode(wire::DrainNode { id: id.0 }),
            SystemCommand::RetireNode { id } => Kind::RetireNode(wire::RetireNode { id: id.0 }),
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
                failure_domain: register.failure_domain,
            },
            Kind::DrainNode(drain) => SystemCommand::DrainNode {
                id: NodeId(drain.id),
            },
            Kind::RetireNode(retire) => SystemCommand::RetireNode {
                id: NodeId(retire.id),
            },
        })
    }

    /// Read a slot's records: a system slot holds exactly one entry.
    fn from_slot(records: &[Vec<u8>]) -> Option<Self> {
        match records {
            [record] => Self::decode(record).ok(),
            _ => None,
        }
    }
}

/// A journal the directory created.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreatedJournal {
    /// Its name.
    pub name: Vec<u8>,
    /// Its static acceptor configuration.
    pub config: AcceptorConfig,
    /// The LSN of the `DeleteJournal` that tombstoned it, if any.
    pub deleted_at: Option<u64>,
}

/// What one directory slot folded to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DirectoryEvent {
    /// A journal was created with this id.
    Created {
        /// `128 + the slot`.
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
    /// The id this slot allocates is a genesis journal's.
    Reserved {
        /// The id the slot would have allocated.
        id: JournalId,
    },
    /// A live journal already holds the name: the lower slot won.
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
    next_lsn: u64,
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

    /// The next LSN this fold expects (one past the last folded slot).
    #[must_use]
    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
    }

    /// Fold the entry at `lsn` (a chosen user slot of journal 1, in LSN
    /// order; holes are simply skipped) whose records are `records`.
    ///
    /// # Panics
    ///
    /// If `lsn` is below a slot already folded: a fold is fed in log order,
    /// once (a programmer error of the caller, never an operating one).
    pub fn fold(&mut self, lsn: u64, records: &[Vec<u8>]) -> DirectoryEvent {
        assert!(lsn >= self.next_lsn, "the directory folds in LSN order");
        self.next_lsn = lsn + 1;
        match SystemCommand::from_slot(records) {
            Some(SystemCommand::CreateJournal { name, config }) => {
                let Some(id) = JournalId::FIRST_USER.0.checked_add(lsn).map(JournalId) else {
                    return DirectoryEvent::Refused(DirectoryRefusal::Malformed);
                };
                if self.genesis.contains(&id) {
                    return DirectoryEvent::Refused(DirectoryRefusal::Reserved { id });
                }
                if let Some(&winner) = self.names.get(&name) {
                    return DirectoryEvent::Refused(DirectoryRefusal::NameTaken { winner });
                }
                assert!(
                    !self.journals.contains_key(&id),
                    "a slot allocates an id no earlier slot did"
                );
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
                created.deleted_at = Some(lsn);
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

/// A node the registry registered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisteredNode {
    /// Its address.
    pub addr: String,
    /// Its failure domain.
    pub failure_domain: String,
    /// Where it stands.
    pub standing: NodeStanding,
}

/// What one registry slot folded to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegistryEvent {
    /// A node joined the pool.
    Registered {
        /// The node.
        id: NodeId,
        /// Its address.
        addr: String,
    },
    /// A node is draining.
    Draining {
        /// The node.
        id: NodeId,
    },
    /// A node left the pool for good.
    Retired {
        /// The node.
        id: NodeId,
    },
    /// The entry changed nothing.
    Refused(RegistryRefusal),
}

/// Why a registry entry changed nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegistryRefusal {
    /// Not exactly one decodable registry entry.
    Malformed,
    /// The id is a genesis node or was registered before (ids are never
    /// reused, a retired one included).
    AlreadyKnown {
        /// The node named.
        id: NodeId,
    },
    /// A drain of a node not registered and in the pool.
    NotRegistered {
        /// The node named.
        id: NodeId,
    },
    /// A retirement of a node that is not draining.
    NotDraining {
        /// The node named.
        id: NodeId,
    },
}

/// The registry's fold: the genesis pool the deployment was booted with and
/// every node registered at runtime, with where it stands.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Registry {
    genesis: BTreeSet<NodeId>,
    nodes: BTreeMap<NodeId, RegisteredNode>,
    next_lsn: u64,
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

    /// The next LSN this fold expects.
    #[must_use]
    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
    }

    /// Fold the entry at `lsn` (a chosen user slot of journal 2, in LSN
    /// order).
    ///
    /// # Panics
    ///
    /// If `lsn` is below a slot already folded (see [`Directory::fold`]).
    pub fn fold(&mut self, lsn: u64, records: &[Vec<u8>]) -> RegistryEvent {
        assert!(lsn >= self.next_lsn, "the registry folds in LSN order");
        self.next_lsn = lsn + 1;
        match SystemCommand::from_slot(records) {
            Some(SystemCommand::RegisterNode {
                id,
                addr,
                failure_domain,
            }) => {
                if self.genesis.contains(&id) || self.nodes.contains_key(&id) {
                    return RegistryEvent::Refused(RegistryRefusal::AlreadyKnown { id });
                }
                self.nodes.insert(
                    id,
                    RegisteredNode {
                        addr: addr.clone(),
                        failure_domain,
                        standing: NodeStanding::Registered,
                    },
                );
                RegistryEvent::Registered { id, addr }
            }
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
                    RegistryEvent::Retired { id }
                }
                _ => RegistryEvent::Refused(RegistryRefusal::NotDraining { id }),
            },
            _ => RegistryEvent::Refused(RegistryRefusal::Malformed),
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
}

/// What one system-journal slot folded to — the directory's or the
/// registry's event — as the driver reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SystemEvent {
    /// A directory slot.
    Directory(DirectoryEvent),
    /// A registry slot.
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

    fn create(name: &[u8], members: &[u64]) -> Vec<Vec<u8>> {
        vec![
            SystemCommand::CreateJournal {
                name: name.to_vec(),
                config: config(members),
            }
            .encode(),
        ]
    }

    fn one(command: &SystemCommand) -> Vec<Vec<u8>> {
        vec![command.encode()]
    }

    #[test]
    fn every_command_round_trips() {
        let commands = [
            SystemCommand::CreateJournal {
                name: b"orders".to_vec(),
                config: config(&[0, 1, 2]),
            },
            SystemCommand::DeleteJournal { id: JournalId(131) },
            SystemCommand::RegisterNode {
                id: NodeId(100),
                addr: "10.0.5.1:4500".into(),
                failure_domain: "rack-a".into(),
            },
            SystemCommand::DrainNode { id: NodeId(100) },
            SystemCommand::RetireNode { id: NodeId(100) },
        ];
        for command in commands {
            assert_eq!(SystemCommand::decode(&command.encode()), Ok(command));
        }
        assert!(SystemCommand::decode(b"\xff\xff").is_err());
    }

    #[test]
    fn a_created_journal_is_named_by_its_slot_and_never_reused() {
        let mut dir = Directory::new([JournalId(128)]);
        // Slot 0 would allocate 128, a genesis journal.
        assert_eq!(
            dir.fold(0, &create(b"a", &[0, 1, 2])),
            DirectoryEvent::Refused(DirectoryRefusal::Reserved { id: JournalId(128) })
        );
        // Slot 3 (holes at 1 and 2 are skipped) allocates 131.
        assert!(matches!(
            dir.fold(3, &create(b"a", &[0, 1, 2])),
            DirectoryEvent::Created {
                id: JournalId(131),
                ..
            }
        ));
        assert_eq!(
            dir.fold(
                4,
                &one(&SystemCommand::DeleteJournal { id: JournalId(131) })
            ),
            DirectoryEvent::Deleted { id: JournalId(131) }
        );
        assert!(dir.is_deleted(JournalId(131)));
        // A deleted journal is not deleted twice, and its name frees up for a
        // new journal under a new id.
        assert_eq!(
            dir.fold(
                5,
                &one(&SystemCommand::DeleteJournal { id: JournalId(131) })
            ),
            DirectoryEvent::Refused(DirectoryRefusal::UnknownJournal { id: JournalId(131) })
        );
        assert!(matches!(
            dir.fold(6, &create(b"a", &[0, 1, 2])),
            DirectoryEvent::Created {
                id: JournalId(134),
                ..
            }
        ));
        assert!(dir.is_deleted(JournalId(131)), "the tombstone stays");
    }

    #[test]
    fn a_name_race_is_decided_by_slot_order() {
        let mut dir = Directory::new([]);
        assert!(matches!(
            dir.fold(0, &create(b"x", &[0])),
            DirectoryEvent::Created {
                id: JournalId(128),
                ..
            }
        ));
        assert_eq!(
            dir.fold(1, &create(b"x", &[1])),
            DirectoryEvent::Refused(DirectoryRefusal::NameTaken {
                winner: JournalId(128)
            })
        );
    }

    #[test]
    fn malformed_slots_change_nothing() {
        let mut dir = Directory::new([]);
        assert_eq!(
            dir.fold(0, &[]),
            DirectoryEvent::Refused(DirectoryRefusal::Malformed)
        );
        assert_eq!(
            dir.fold(1, &[b"junk".to_vec()]),
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
            dir.fold(3, &[bad]),
            DirectoryEvent::Refused(DirectoryRefusal::Malformed)
        );
        assert_eq!(dir.journals().count(), 0);
    }

    #[test]
    fn the_pool_grows_by_registration_and_shrinks_by_retirement() {
        let mut reg = Registry::new([NodeId(0), NodeId(1)]);
        let register = |id: u64| {
            one(&SystemCommand::RegisterNode {
                id: NodeId(id),
                addr: format!("n{id}"),
                failure_domain: String::new(),
            })
        };
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
        // A retired id is never registered again.
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
}
