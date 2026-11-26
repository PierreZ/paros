//! A [`Replica`] with the store of records it reads through its
//! [`ChosenLog`](super::super::ChosenLog) port, as a deployment holds the
//! two: a learn writes the record first, a drop of the prefix drops the
//! records below it.

use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};

use super::super::{LogRead, Replica};
use crate::contract::write::WriteOp;
use crate::journal::state::{JournalState, WriterMode};
use crate::journal::{Command, Entry, Seq};
use crate::types::{Ballot, NodeId, Slot};

/// The ballot every record of these tests is held at.
const BALLOT: Ballot = Ballot {
    round: 1,
    node: NodeId(1),
};

/// A replica and its records.
pub(super) struct Held {
    pub(super) replica: Replica,
    log: BTreeMap<Slot, (Ballot, Command)>,
}

impl Deref for Held {
    type Target = Replica;

    fn deref(&self) -> &Replica {
        &self.replica
    }
}

impl DerefMut for Held {
    fn deref_mut(&mut self) -> &mut Replica {
        &mut self.replica
    }
}

impl Held {
    /// The commands the replica holds chosen, in slot order, read from its
    /// records.
    pub(super) fn chosen_commands(&self) -> Vec<Command> {
        self.replica
            .chosen()
            .iter()
            .map(|slot| self.log[slot].1.clone())
            .collect()
    }

    /// [`Replica::from_boot`] over `records`, which the replica keeps.
    pub(super) fn boot(
        mode: WriterMode,
        chosen_index: Option<Slot>,
        floor: Slot,
        sealed: JournalState,
        records: &BTreeMap<Slot, (Ballot, Command)>,
    ) -> Self {
        Self {
            replica: Replica::from_boot(mode, chosen_index, floor, sealed, records),
            log: records.clone(),
        }
    }

    pub(super) fn learn(&mut self, slot: Slot, command: &Command) {
        self.log.insert(slot, (BALLOT, command.clone()));
        self.replica.learn(&self.log, slot, command);
    }

    pub(super) fn advance(&mut self, writes: &mut Vec<WriteOp>) -> Option<Slot> {
        self.replica.advance(&self.log, writes)
    }

    pub(super) fn read(&self, from: Seq, limit: usize, max_bytes: usize) -> LogRead {
        self.replica.read(&self.log, from, limit, max_bytes)
    }

    pub(super) fn trim_to(&mut self, point: Slot, sealed: JournalState) -> JournalState {
        self.log = self.log.split_off(&point);
        self.replica.trim_to(&self.log, point, sealed)
    }

    pub(super) fn truncate(&mut self, first: Slot) -> JournalState {
        let sealed = self.replica.truncate(&self.log, first);
        self.log = self.log.split_off(&first);
        sealed
    }

    pub(super) fn compaction_target(&self) -> Option<Slot> {
        self.replica.compaction_target(&self.log)
    }

    pub(super) fn compaction_floor(&self, up_to: Slot) -> Option<Slot> {
        self.replica.compaction_floor(&self.log, up_to)
    }

    pub(super) fn assert_compacted(&mut self) {
        self.replica.assert_compacted(&self.log);
    }

    pub(super) fn chosen_at(&self, slot: Slot) -> Option<&Command> {
        self.replica.chosen_at(&self.log, slot)
    }

    pub(super) fn accepted_at(&self, seq: Seq) -> Option<&Entry> {
        self.replica.accepted_at(&self.log, seq)
    }
}
