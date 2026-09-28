//! The node store's records and the in-memory image they fold into.
//!
//! One fold serves both directions: a live write stages a [`NodeRecord`]
//! and applies it to the image at once (what [`MemStorage`](crate::MemStorage)
//! does), and a boot replays the journal's records through the same
//! [`NodeImage::apply`] — so what a boot rebuilds is, record for record,
//! what the writes left. A damaged record goes through
//! [`NodeImage::apply_damaged`], the per-kind table:
//!
//! | Damaged record | Reaction | Why |
//! |---|---|---|
//! | `Accepted`, `Faulty` | the slot becomes **faulty** `(slot, ballot)` from the tag | CTRL's recoverable class: the vote's identity survived, its value did not — reported in the tri-state, repaired from peers, never "nothing accepted" |
//! | `ChosenIndex` | forgotten | the commit index is relaxed by contract: re-derivable after a crash |
//! | `Truncate` | forgotten | compaction is lazy and local; the records it would drop are still earlier in the log, so the store is the one it was before the truncation |
//! | `InstallSnapshot` | forgotten, and every later chosen index with it | the node is back below the floor with the log it had, and is sent the snapshot again; a later chosen index could claim slots the forgotten install covered |
//! | `SnapPoint` | forgotten: the previous point stays | a decided point is custody, re-recorded at the next marker |
//! | `SnapChunk` | the chunk is **faulty** | the recoverable class of #101: every peer holds the byte-identical chunk |
//! | `Begin`, `Sealed` (strict) | **crash** | the checkpoint the fold must trust has lost its floor or its ledger, and the history it summarised is gone |
//!
//! A damaged record inside a checkpoint the fold does *not* have to trust
//! never reaches this table: the plan skips that copy and reads the
//! originals (see `plan`).

use std::collections::BTreeMap;

use moonpool_journal::{EntryId, Tag};
use paros_core::{Ballot, ClientId, ClientSeq, Command, SessionEntry, Slot};
use serde::{Deserialize, Serialize};

use super::frame::{Framed, Kind, slot_identity, slot_tag, tag, words};
use crate::corruption::{CorruptionVerdict, IntegrityFault};
use crate::storage::{SNAP_CHUNK_BYTES, StorageError, StorageRecord, snap_chunk_count};

/// How many sealed-ledger records one checkpoint entry carries.
const SEALED_PER_ENTRY: usize = 512;

/// One durable write of the node store, as one journal entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum NodeRecord {
    /// `(ballot, command)` accepted — or learned — at `slot`: an upsert.
    Accepted {
        slot: Slot,
        ballot: Ballot,
        command: Command,
    },
    /// `slot`'s value is lost, its identity is not: a checkpoint's copy of
    /// a faulty entry.
    Faulty { slot: Slot, ballot: Ballot },
    /// The chosen index.
    ChosenIndex(Slot),
    /// Raise the floor to `first`, sealing the ledger it drops.
    Truncate {
        first: Slot,
        sealed: Vec<SessionEntry>,
    },
    /// A snapshot install at `chosen_index` (its ballot is the metadata's).
    InstallSnapshot {
        chosen_index: Slot,
        sessions: Vec<SessionEntry>,
    },
    /// A decided snapshot point: its length and each chunk's checksum.
    SnapPoint { at: Slot, len: u64, crcs: Vec<u32> },
    /// One chunk of the point at `at`.
    SnapChunk {
        at: Slot,
        chunk: u32,
        bytes: Vec<u8>,
    },
    /// Part of a checkpoint's sealed ledger.
    Sealed(Vec<SessionEntry>),
    /// A checkpoint opens: the floor and the chosen index, then the image.
    Begin {
        first: Slot,
        chosen_index: Option<Slot>,
    },
    /// A checkpoint closes.
    End,
}

impl Framed for NodeRecord {
    fn kind(&self) -> Kind {
        match self {
            NodeRecord::Accepted { .. } => Kind::Accepted,
            NodeRecord::Faulty { .. } => Kind::Faulty,
            NodeRecord::ChosenIndex(_) => Kind::ChosenIndex,
            NodeRecord::Truncate { .. } => Kind::Truncate,
            NodeRecord::InstallSnapshot { .. } => Kind::InstallSnapshot,
            NodeRecord::SnapPoint { .. } => Kind::SnapPoint,
            NodeRecord::SnapChunk { .. } => Kind::SnapChunk,
            NodeRecord::Sealed(_) => Kind::Sealed,
            NodeRecord::Begin { .. } => Kind::Begin,
            NodeRecord::End => Kind::End,
        }
    }

    fn tag(&self) -> Tag {
        match self {
            NodeRecord::Accepted { slot, ballot, .. } | NodeRecord::Faulty { slot, ballot } => {
                slot_tag(*slot, *ballot)
            }
            NodeRecord::SnapPoint { at, .. } => tag([at.0, 0, 0]),
            NodeRecord::SnapChunk { at, chunk, .. } => tag([at.0, u64::from(*chunk), 0]),
            _ => tag([0; 3]),
        }
    }
}

/// The retained decided snapshot point (#101).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SnapPoint {
    pub at: Slot,
    pub len: u64,
    pub crcs: Vec<u32>,
    /// Each chunk's bytes, `None` while it is lost (rotted, or never landed).
    pub chunks: Vec<Option<Vec<u8>>>,
}

impl SnapPoint {
    /// The point `blob` makes at `at`, every chunk present.
    pub(crate) fn of(at: Slot, blob: &[u8]) -> Self {
        let chunks: Vec<Option<Vec<u8>>> = blob
            .chunks(SNAP_CHUNK_BYTES)
            .map(|chunk| Some(chunk.to_vec()))
            .collect();
        debug_assert_eq!(chunks.len(), snap_chunk_count(blob.len()) as usize);
        Self {
            at,
            len: blob.len() as u64,
            crcs: blob.chunks(SNAP_CHUNK_BYTES).map(crc32c::crc32c).collect(),
            chunks,
        }
    }

    /// The expected length of chunk `chunk`, if the point has one.
    pub(crate) fn chunk_len(&self, chunk: u32) -> Option<usize> {
        let start = usize::try_from(chunk).ok()?.checked_mul(SNAP_CHUNK_BYTES)?;
        let len = usize::try_from(self.len).ok()?;
        (start < len).then(|| (len - start).min(SNAP_CHUNK_BYTES))
    }

    /// Whether `bytes` are exactly chunk `chunk` of this point.
    pub(crate) fn verifies(&self, chunk: u32, bytes: &[u8]) -> bool {
        let index = chunk as usize;
        self.chunk_len(chunk) == Some(bytes.len())
            && self.crcs.get(index) == Some(&crc32c::crc32c(bytes))
    }

    /// The chunks that are lost.
    pub(crate) fn lost(&self) -> impl Iterator<Item = u32> + '_ {
        self.chunks
            .iter()
            .enumerate()
            .filter(|(_, chunk)| chunk.is_none())
            .map(|(index, _)| u32::try_from(index).unwrap_or(u32::MAX))
    }
}

/// The node store's durable state, as the journal's records fold it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct NodeImage {
    pub chosen_index: Option<Slot>,
    /// The compaction floor.
    pub first: Slot,
    pub accepted: BTreeMap<Slot, (Ballot, Command)>,
    /// Slots whose value is lost and whose identity is not (CTRL).
    pub faulty: BTreeMap<Slot, Ballot>,
    pub sealed: BTreeMap<(ClientId, ClientSeq), Slot>,
    pub point: Option<SnapPoint>,
    /// Replay only: a damaged install was forgotten, so no later chosen
    /// index is believed until an intact install or checkpoint speaks.
    chosen_frozen: bool,
}

impl NodeImage {
    fn seal(&mut self, sealed: &[SessionEntry]) {
        for &(client, seq, slot) in sealed {
            self.sealed.entry((client, seq)).or_insert(slot);
        }
    }

    fn raise_floor(&mut self, first: Slot) {
        self.first = self.first.max(first);
        let floor = self.first;
        self.accepted = self.accepted.split_off(&floor);
        self.faulty = self.faulty.split_off(&floor);
    }

    /// Fold one intact record: the live write and the boot replay alike.
    pub(crate) fn apply(&mut self, record: &NodeRecord) {
        match record {
            NodeRecord::Accepted {
                slot,
                ballot,
                command,
            } => {
                self.faulty.remove(slot);
                self.accepted.insert(*slot, (*ballot, command.clone()));
            }
            NodeRecord::Faulty { slot, ballot } => {
                self.accepted.remove(slot);
                self.faulty.insert(*slot, *ballot);
            }
            NodeRecord::ChosenIndex(slot) => {
                if !self.chosen_frozen {
                    self.chosen_index = Some(*slot);
                }
            }
            NodeRecord::Truncate { first, sealed } => {
                self.seal(sealed);
                self.raise_floor(*first);
            }
            NodeRecord::InstallSnapshot {
                chosen_index,
                sessions,
            } => {
                self.chosen_frozen = false;
                self.seal(sessions);
                self.chosen_index = Some(*chosen_index);
                self.raise_floor(Slot(chosen_index.0 + 1));
            }
            NodeRecord::SnapPoint { at, len, crcs } => {
                self.point = Some(SnapPoint {
                    at: *at,
                    len: *len,
                    crcs: crcs.clone(),
                    chunks: vec![None; crcs.len()],
                });
            }
            NodeRecord::SnapChunk { at, chunk, bytes } => {
                if let Some(point) = self.point.as_mut()
                    && point.at == *at
                    && point.verifies(*chunk, bytes)
                {
                    point.chunks[*chunk as usize] = Some(bytes.clone());
                }
            }
            NodeRecord::Sealed(sealed) => self.seal(sealed),
            NodeRecord::Begin {
                first,
                chosen_index,
            } => {
                *self = NodeImage {
                    chosen_index: *chosen_index,
                    first: *first,
                    ..NodeImage::default()
                };
            }
            NodeRecord::End => {}
        }
    }

    /// Fold one damaged record (the module doc's table). `strict` says the
    /// fold is inside the checkpoint it has to trust.
    ///
    /// # Errors
    ///
    /// The crash verdict for a damaged checkpoint header or sealed ledger in
    /// strict mode, and for an entry of a kind this store never writes.
    pub(crate) fn apply_damaged(
        &mut self,
        id: &EntryId,
        kind: Option<Kind>,
        strict: bool,
    ) -> Result<(), StorageError> {
        let crash = |record| StorageError::Corruption {
            record,
            fault: IntegrityFault::ChecksumMismatch,
            verdict: CorruptionVerdict::Corrupted,
        };
        match kind {
            Some(Kind::Accepted | Kind::Faulty) => {
                let (slot, ballot) = slot_identity(&id.tag);
                self.accepted.remove(&slot);
                self.faulty.insert(slot, ballot);
                tracing::warn!(slot = slot.0, round = ballot.round, "journal_record_faulty");
            }
            Some(Kind::ChosenIndex | Kind::Truncate | Kind::SnapPoint) => {
                tracing::warn!(index = id.index, "journal_record_forgotten");
            }
            Some(Kind::InstallSnapshot) => {
                self.chosen_frozen = true;
                tracing::warn!(index = id.index, "journal_install_forgotten");
            }
            Some(Kind::SnapChunk) => {
                let [at, chunk, _] = words(&id.tag);
                if let Some(point) = self.point.as_mut()
                    && point.at == Slot(at)
                    && let Some(slot) = usize::try_from(chunk)
                        .ok()
                        .and_then(|chunk| point.chunks.get_mut(chunk))
                {
                    *slot = None;
                }
            }
            Some(Kind::End) => {}
            Some(Kind::Begin | Kind::Sealed) if strict => {
                return Err(crash(StorageRecord::Truncation));
            }
            // A damaged header or ledger of a checkpoint the plan did not
            // make the fold trust is skipped with its bracket; reaching here
            // would be a plan bug, and is treated as the rot it looks like.
            Some(Kind::Begin | Kind::Sealed) => return Err(crash(StorageRecord::Truncation)),
            Some(Kind::Register | Kind::Scalars | Kind::Install) | None => {
                return Err(crash(StorageRecord::Store));
            }
        }
        Ok(())
    }

    /// Close a replay: the floor bounds everything, and a chosen index never
    /// sits below the truncated prefix it implies.
    pub(crate) fn finish(&mut self) {
        self.chosen_frozen = false;
        let floor = self.first;
        self.raise_floor(floor);
        if let Some(below) = self.first.0.checked_sub(1)
            && self.chosen_index.is_none_or(|chosen| chosen.0 < below)
        {
            self.chosen_index = Some(Slot(below));
        }
    }

    /// The image re-emitted as a checkpoint's content (between `Begin` and
    /// `End`, which the caller adds).
    pub(crate) fn checkpoint(&self) -> Vec<NodeRecord> {
        let mut records = vec![NodeRecord::Begin {
            first: self.first,
            chosen_index: self.chosen_index,
        }];
        let sealed: Vec<SessionEntry> = self
            .sealed
            .iter()
            .map(|(&(client, seq), &slot)| (client, seq, slot))
            .collect();
        records.extend(
            sealed
                .chunks(SEALED_PER_ENTRY)
                .map(|part| NodeRecord::Sealed(part.to_vec())),
        );
        records.extend(self.accepted.iter().map(|(slot, (ballot, command))| {
            NodeRecord::Accepted {
                slot: *slot,
                ballot: *ballot,
                command: command.clone(),
            }
        }));
        records.extend(self.faulty.iter().map(|(slot, ballot)| NodeRecord::Faulty {
            slot: *slot,
            ballot: *ballot,
        }));
        if let Some(point) = &self.point {
            records.push(NodeRecord::SnapPoint {
                at: point.at,
                len: point.len,
                crcs: point.crcs.clone(),
            });
            // A lost chunk is left out: the fold finds it missing and it
            // stays faulty, exactly as it is now.
            records.extend(
                point
                    .chunks
                    .iter()
                    .enumerate()
                    .filter_map(|(index, bytes)| {
                        Some(NodeRecord::SnapChunk {
                            at: point.at,
                            chunk: u32::try_from(index).ok()?,
                            bytes: bytes.clone()?,
                        })
                    }),
            );
        }
        records.push(NodeRecord::End);
        records
    }
}

#[cfg(test)]
mod tests {
    use paros_core::{Entry, NodeId, Value};

    use super::*;

    fn ballot(round: u64) -> Ballot {
        Ballot {
            round,
            node: NodeId(1),
        }
    }

    fn user(byte: u8) -> Command {
        Command::User(Entry {
            client: ClientId(1),
            seq: ClientSeq(u64::from(byte)),
            value: Value(vec![byte]),
        })
    }

    #[test]
    fn a_checkpoint_folds_back_to_the_image_it_copies() {
        let mut image = NodeImage::default();
        for slot in 0..6 {
            image.apply(&NodeRecord::Accepted {
                slot: Slot(slot),
                ballot: ballot(2),
                command: user(u8::try_from(slot).expect("small")),
            });
        }
        image.apply(&NodeRecord::ChosenIndex(Slot(4)));
        image.apply(&NodeRecord::Truncate {
            first: Slot(2),
            sealed: vec![(ClientId(1), ClientSeq(0), Slot(0))],
        });
        image.apply(&NodeRecord::Faulty {
            slot: Slot(5),
            ballot: ballot(3),
        });
        let blob = vec![7u8; SNAP_CHUNK_BYTES * 2 + 3];
        let mut point = SnapPoint::of(Slot(3), &blob);
        point.chunks[1] = None;
        image.point = Some(point);

        let mut rebuilt = NodeImage::default();
        for record in image.checkpoint() {
            rebuilt.apply(&record);
        }
        rebuilt.finish();
        assert_eq!(rebuilt, image);
    }

    #[test]
    fn a_forgotten_install_freezes_the_chosen_index() {
        let mut image = NodeImage::default();
        image.apply(&NodeRecord::ChosenIndex(Slot(1)));
        let id = EntryId {
            index: 9,
            epoch: 0,
            tag: [0; moonpool_journal::TAG_SIZE],
        };
        image
            .apply_damaged(&id, Some(Kind::InstallSnapshot), false)
            .expect("an install is forgettable");
        image.apply(&NodeRecord::ChosenIndex(Slot(20)));
        image.finish();
        assert_eq!(image.chosen_index, Some(Slot(1)));
        assert_eq!(image.first, Slot(0));
    }
}
