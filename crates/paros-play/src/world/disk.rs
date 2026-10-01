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
//!
//! # Below the trim point
//!
//! Act III adds the jump a node makes when it asked a peer for slots that
//! peer has already trimmed: [`WriteOp::TrimmedTo`] raises the floor to the
//! peer's trim point and the chosen index to just below it, and seals the
//! at-most-once ledger for the prefix the node will never walk. No bytes
//! travel and the promise does not move. The applied log simply never sees
//! the slots below the point — they were decided and dropped before this
//! node got to them, and the decided `Truncate` that dropped them is what
//! every node agrees on.
//!
//! # Damage: a rotted record, and an erased disk
//!
//! Act IV adds the two ways a disk can lose something. [`Disk::corrupt`] rots
//! **one accepted record**: its value is gone and its identity — the slot and
//! the ballot it was accepted at — survives, which is exactly the tri-state a
//! boot scan classifies and reports through [`Storage::faulty_entries`]. The
//! record is never reported as "nothing accepted here": that misreport is what
//! lets a later ballot decide a second value for a slot that already has one.
//! A peer's `Accept` at a high enough ballot writes the value back and the
//! entry stops being faulty, which is the whole repair.
//!
//! [`Disk::wipe`] erases everything instead — the lost disk, not the clean
//! crash. What survives it is one bit outside the erased area: this identity
//! **was provisioned once**. A store that was provisioned and no longer
//! carries its own format marker is a node whose promise is gone, and the
//! engine refuses to boot it (see `World::restart`): nothing a peer can send
//! gives back a promise.

use std::collections::BTreeMap;

use paros_core::{
    AcceptorWrite, Ballot, ClientId, Command, Config, Generation, HardState, JournalState, Slot,
    Storage, WriteOp,
};

/// One node's disk.
#[derive(Clone, Debug)]
pub struct Disk {
    config: Config,
    hard_state: HardState,
    records: BTreeMap<Slot, (Ballot, Command)>,
    first_slot: Slot,
    /// The journal state sealed at the floor (#204): what the replica folds
    /// the retained log from.
    sealed: JournalState,
    applied: Vec<(Slot, Command)>,
    /// The **faulty** records: value lost, identity — the slot and the ballot
    /// it was accepted at — intact. Reported through
    /// [`Storage::faulty_entries`] at the next boot.
    faulty: BTreeMap<Slot, Ballot>,
    /// Whether this store carries its own format marker. A fresh disk in a
    /// level's setup does; [`Disk::wipe`] clears it.
    formatted: bool,
    /// Whether an operator ever provisioned this identity. It lives outside
    /// the area a wipe erases, because an operator remembers provisioning a
    /// node even when the node's disk no longer does.
    provisioned: bool,
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
            sealed: JournalState::default(),
            applied: Vec::new(),
            faulty: BTreeMap::new(),
            formatted: true,
            provisioned: true,
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

    /// The faulty records: the slots whose value is lost and whose identity
    /// survived.
    #[must_use]
    pub fn faulty(&self) -> &BTreeMap<Slot, Ballot> {
        &self.faulty
    }

    /// Whether this store carries its format marker.
    #[must_use]
    pub fn is_formatted(&self) -> bool {
        self.formatted
    }

    /// Whether an operator ever provisioned this identity.
    #[must_use]
    pub fn provisioned(&self) -> bool {
        self.provisioned
    }

    /// **Rot one accepted record**: drop its value and keep its identity.
    ///
    /// Returns `false` when the slot holds no readable record here. The next
    /// boot reads the entry back through [`Storage::faulty_entries`], so the
    /// node reports `faulty` for that slot rather than `have` — and never
    /// `none`, which is the misreport that would let a later ballot decide a
    /// second value for a slot a quorum may already have decided.
    pub fn corrupt(&mut self, slot: Slot) -> bool {
        let Some((ballot, _)) = self.records.remove(&slot) else {
            return false;
        };
        self.faulty.insert(slot, ballot);
        true
    }

    /// **Erase the disk**, keeping only the memory that this identity was
    /// provisioned once. The promise, the records, the application and the
    /// format marker all go.
    pub fn wipe(&mut self) {
        self.hard_state = HardState::default();
        self.records.clear();
        self.faulty.clear();
        self.sealed = JournalState::default();
        self.applied.clear();
        self.first_slot = Slot(0);
        self.formatted = false;
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
            })
            | WriteOp::Learned {
                slot,
                ballot,
                command: value,
            } => {
                // A record written over a faulty entry **is** the repair: the
                // value is readable again, so the slot leaves the faulty set
                // and the next boot reports `have` for it.
                self.faulty.remove(slot);
                self.records.insert(*slot, (*ballot, value.clone()));
            }
            WriteOp::SetChosenIndex(slot) => {
                self.hard_state.chosen_index = Some(*slot);
            }
            WriteOp::Truncate { first, sealed } => {
                self.seal(*first, *sealed);
                self.first_slot = self.first_slot.max(*first);
                self.records.retain(|slot, _| *slot >= self.first_slot);
                self.faulty.retain(|slot, _| *slot >= self.first_slot);
            }
            WriteOp::TrimmedTo { point, state } => {
                self.seal(*point, *state);
                let boundary = Slot(point.0.saturating_sub(1));
                if self.hard_state.chosen_index.is_none_or(|ci| ci < boundary) {
                    self.hard_state.chosen_index = Some(boundary);
                }
                self.first_slot = self.first_slot.max(*point);
                self.records.retain(|slot, _| *slot >= self.first_slot);
                self.faulty.retain(|slot, _| *slot >= self.first_slot);
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

    /// The state sealed at `floor` lands with it, never with a lower one.
    fn seal(&mut self, floor: Slot, sealed: JournalState) {
        if floor >= self.first_slot {
            self.sealed = sealed;
        }
    }

    /// Provision the journal to `owner` at generation 1, as if a
    /// `SetLeader` had been decided and trimmed before the level starts.
    /// The game teaches Paxos, not the claim (#204): a level's client is the
    /// journal's writer from its first move. Only a disk that has sealed
    /// nothing yet is provisioned.
    pub fn provision_owner(&mut self, owner: ClientId) {
        if self.sealed == JournalState::default() && self.first_slot == Slot(0) {
            self.sealed = JournalState {
                owner: Some(owner),
                generation: Generation(1),
                ..JournalState::default()
            };
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
        // A faulty entry is a record this node accepted: it bounds the scan
        // exactly like a readable one, it is simply unreadable.
        self.records
            .keys()
            .chain(self.faulty.keys())
            .max()
            .copied()
            .unwrap_or(Slot(0))
    }

    fn sealed_state(&self) -> JournalState {
        self.sealed
    }

    fn faulty_entries(&self) -> Vec<(Slot, Ballot)> {
        self.faulty
            .iter()
            .map(|(slot, ballot)| (*slot, *ballot))
            .collect()
    }
}
