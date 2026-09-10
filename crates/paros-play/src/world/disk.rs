//! One node's durable state, and the application log behind it.
//!
//! [`Disk`] is the game's [`Storage`] implementation: the read-only recovery
//! port `paros-core` boots a [`ColocatedNode`](paros_core::ColocatedNode) from,
//! plus the write side the world applies a [`Ready`](paros_core::Ready) batch's
//! [`WriteOp`]s to. A crash drops the node and keeps the disk; a restart is
//! `ColocatedNode::new(&disk)` — which is why the disk lives in the world and
//! not inside the node.
//!
//! The `applied` log is the *application's* state, not paros's: the world
//! appends `Ready::committed` to it in slot order, exactly where a real driver
//! would hand each command to its state machine. Nothing in the engine reads
//! the bytes.

use std::collections::BTreeMap;

use paros_core::{
    AcceptorWrite, Ballot, ClientId, ClientSeq, Command, Config, HardState, SessionEntry, Slot,
    Storage, Value, WriteOp,
};

/// One node's disk.
#[derive(Clone, Debug)]
pub struct Disk {
    config: Config,
    hard_state: HardState,
    records: BTreeMap<Slot, (Ballot, Command)>,
    first_slot: Slot,
    sealed: BTreeMap<(ClientId, ClientSeq), Slot>,
    applied: Vec<(Slot, Command)>,
    /// The retained decided snapshot point, if any. Act III fills it through
    /// `Control::Snap`; today only [`WriteOp::InstallSnapshot`] writes it, and
    /// nothing reads the bytes.
    snapshot: Option<(Slot, Value)>,
}

impl Disk {
    /// A freshly formatted, empty disk for `config`.
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self {
            config,
            hard_state: HardState::default(),
            records: BTreeMap::new(),
            first_slot: Slot(0),
            sealed: BTreeMap::new(),
            applied: Vec::new(),
            snapshot: None,
        }
    }

    /// A disk pre-seeded with an accepted log and a promise — how a level puts
    /// a node's history in place before the player's first move.
    #[must_use]
    pub fn seeded(
        config: Config,
        promised: Ballot,
        records: BTreeMap<Slot, (Ballot, Command)>,
        chosen_index: Option<Slot>,
    ) -> Self {
        let mut disk = Self::new(config);
        disk.hard_state.max_promised_ballot = promised;
        disk.hard_state.chosen_index = chosen_index;
        disk.records = records;
        if let Some(upto) = chosen_index {
            disk.applied = disk
                .records
                .range(..=upto)
                .map(|(slot, (_, command))| (*slot, command.clone()))
                .collect();
        }
        disk
    }

    /// This node's static configuration.
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// The durable scalars.
    #[must_use]
    pub fn hard_state(&self) -> HardState {
        self.hard_state
    }

    /// The retained accepted records.
    #[must_use]
    pub fn records(&self) -> &BTreeMap<Slot, (Ballot, Command)> {
        &self.records
    }

    /// The compaction floor: the first slot still retained.
    #[must_use]
    pub fn floor(&self) -> Slot {
        self.first_slot
    }

    /// The application's log, in the order it was applied.
    #[must_use]
    pub fn applied(&self) -> &[(Slot, Command)] {
        &self.applied
    }

    /// The retained decided snapshot point, if any.
    #[must_use]
    pub fn snapshot_point(&self) -> Option<Slot> {
        self.snapshot.as_ref().map(|(at, _)| *at)
    }

    /// Apply one durable write, in the order the batch surfaced it.
    ///
    /// The world hands `Truncate` in *after* the application apply below, for
    /// the reason `paros::driver::ready` documents: a durable floor above the
    /// durable application prefix leaves a node whose apply stream can never be
    /// replayed.
    pub fn apply(&mut self, op: &WriteOp) {
        match op {
            WriteOp::Acceptor(AcceptorWrite::SetPromise(ballot)) => {
                self.hard_state.max_promised_ballot = *ballot;
            }
            WriteOp::Acceptor(AcceptorWrite::AppendAccepted {
                slot,
                ballot,
                value,
            }) => {
                self.records.insert(*slot, (*ballot, value.clone()));
            }
            WriteOp::SetChosenIndex(slot) => {
                self.hard_state.chosen_index = Some(*slot);
            }
            WriteOp::Truncate { first, sealed } => {
                self.seal(sealed);
                self.first_slot = self.first_slot.max(*first);
                self.records.retain(|slot, _| *slot >= self.first_slot);
            }
            WriteOp::InstallSnapshot {
                chosen_index,
                ballot,
                snapshot,
                sessions,
            } => {
                self.seal(sessions);
                self.hard_state.chosen_index = Some(*chosen_index);
                self.hard_state.max_promised_ballot =
                    self.hard_state.max_promised_ballot.max(*ballot);
                self.first_slot = self.first_slot.max(Slot(chosen_index.0 + 1));
                self.records.retain(|slot, _| *slot >= self.first_slot);
                self.snapshot = Some((*chosen_index, snapshot.clone()));
                // The application state the snapshot folds is opaque; what the
                // game can show is that everything up to the boundary is
                // covered, so the apply log is reset to it.
                self.applied.clear();
            }
        }
    }

    /// Hand one newly committed `(slot, command)` to the application.
    pub fn apply_committed(&mut self, slot: Slot, command: Command) {
        self.applied.push((slot, command));
    }

    /// Whether the application has applied `slot`.
    #[must_use]
    pub fn has_applied(&self, slot: Slot) -> bool {
        self.applied.iter().any(|(s, _)| *s == slot)
    }

    fn seal(&mut self, sealed: &[SessionEntry]) {
        for &(client, seq, slot) in sealed {
            self.sealed.entry((client, seq)).or_insert(slot);
        }
    }
}

impl Storage for Disk {
    fn initial_state(&self) -> (HardState, Config) {
        (self.hard_state, self.config.clone())
    }

    fn accepted(&self, slot: Slot) -> Option<(Ballot, Command)> {
        self.records.get(&slot).cloned()
    }

    fn first_slot(&self) -> Slot {
        self.first_slot
    }

    fn last_slot(&self) -> Slot {
        self.records.keys().next_back().copied().unwrap_or(Slot(0))
    }

    fn sealed_sessions(&self) -> Vec<SessionEntry> {
        self.sealed
            .iter()
            .map(|(&(client, seq), &slot)| (client, seq, slot))
            .collect()
    }
}
