//! The **directory** (#189): a tenant's control journal's journal names —
//! the journals created and deleted at runtime inside one tenant (#235).

use std::collections::{BTreeMap, BTreeSet};

use paros_core::{AcceptorConfig, JournalId};

use super::SystemCommand;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::system as wire;
    use paros_core::{NodeId, QuorumSystem};
    use prost::Message as _;

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
}
