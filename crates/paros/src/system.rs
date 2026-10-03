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
//! - **Registry.** The node pool is the genesis pool plus every registered
//!   node not yet retired ([`Registry::pool`]). A node is registered once
//!   (an id is never reused, a retired one included), drained, then retired.
//!
//! Every malformed entry — a record that does not decode,
//! a configuration that does not admit its quorum system, a registry entry
//! in the directory — folds to a refusal, never a panic: the entries are
//! external input.

use std::collections::{BTreeMap, BTreeSet};

use paros_core::{AcceptorConfig, JournalId, JournalKey, NodeId, TenantId};
use prost::Message as _;

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

/// What one registry record folded to.
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

    /// Fold the record at position `seq` of journal 2 (in position order).
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
