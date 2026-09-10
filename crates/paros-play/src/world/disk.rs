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
//! # Snapshot custody
//!
//! Act III adds the other half of the storage seam: the **opaque application
//! snapshot**. [`Disk::snapshot`] serialises the applied log — that is this
//! game's whole application, so its state at a boundary *is* the list of
//! commands it executed — and [`Disk::record_snapshot`] retains it at a
//! decided [`Control::Snap`](paros_core::Control::Snap) point, which is what
//! a `Truncate` is later coupled to. A peer serving a below-floor node reads
//! those bytes back through [`Disk::snapshot`] and the receiver installs them
//! through [`WriteOp::InstallSnapshot`].
//!
//! The bytes are opaque *to paros*, not to the game: the game **is** the
//! application here, so it is the one party entitled to read them back. That
//! is exactly the split the doctrine asks for — the engine's driver half ships
//! and stores the blob without looking inside, and only the apply side, which
//! owns the state machine, decodes it.
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
//! engine refuses to boot it (see `World::restart`): a snapshot restores the
//! log, never a promise.

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
    /// The retained decided snapshot point and its opaque bytes. Written when
    /// a decided [`Control::Snap`](paros_core::Control::Snap) is applied
    /// ([`Disk::record_snapshot`]) and when a peer's snapshot is installed.
    snapshot: Option<(Slot, Value)>,
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
            sealed: BTreeMap::new(),
            applied: Vec::new(),
            snapshot: None,
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
        self.sealed.clear();
        self.applied.clear();
        self.snapshot = None;
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

    /// The retained decided snapshot point, if any.
    #[must_use]
    pub fn snapshot_point(&self) -> Option<Slot> {
        self.snapshot.as_ref().map(|(at, _)| *at)
    }

    /// The highest slot the application has executed — the boundary any
    /// snapshot this disk serves describes.
    #[must_use]
    pub fn applied_slot(&self) -> Option<Slot> {
        self.applied.last().map(|(slot, _)| *slot)
    }

    /// The **opaque application snapshot** at the current applied prefix: this
    /// game's application is the applied log, so its state is that list.
    ///
    /// The bytes travel through `paros-core` and the wire without anything
    /// looking inside — only [`Disk::apply`]'s install arm, which is the
    /// application side of the seam, decodes them again.
    #[must_use]
    pub fn snapshot(&self) -> Value {
        Value(serde_json::to_vec(&self.applied).unwrap_or_default())
    }

    /// Retain the snapshot at a decided
    /// [`Control::Snap`](paros_core::Control::Snap) point — the driver-side
    /// half of `NodeStorage::record_snapshot`, taken at exactly the instant of
    /// the contiguous walk that applied the marker.
    pub fn record_snapshot(&mut self, at: Slot) {
        self.snapshot = Some((at, self.snapshot()));
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
                self.seal(sealed);
                self.first_slot = self.first_slot.max(*first);
                self.records.retain(|slot, _| *slot >= self.first_slot);
                self.faulty.retain(|slot, _| *slot >= self.first_slot);
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
                self.faulty.retain(|slot, _| *slot >= self.first_slot);
                self.snapshot = Some((*chosen_index, snapshot.clone()));
                // The application side of the seam, and the only place the
                // bytes are read: the game's application state *is* the
                // applied log, so installing the snapshot means adopting the
                // serving peer's list wholesale. A blob the game cannot decode
                // leaves the prefix empty rather than half-applied — the same
                // "restore or nothing" contract a real `install_snapshot` has.
                self.applied = serde_json::from_slice(&snapshot.0).unwrap_or_default();
            }
        }
    }

    /// Hand one newly committed `(slot, command)` to the application.
    ///
    /// A decided [`Control::Snap`](paros_core::Control::Snap) also **retains**
    /// the snapshot, at the marker's own slot and at exactly this instant of
    /// the contiguous walk — which is what makes the retained bytes describe
    /// the boundary they advertise, and what a later `Truncate` is coupled to.
    pub fn apply_committed(&mut self, slot: Slot, command: Command) {
        let snap = matches!(command, Command::Control(paros_core::Control::Snap { .. }));
        self.applied.push((slot, command));
        if snap {
            self.record_snapshot(slot);
        }
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
        // A faulty entry is a record this node accepted: it bounds the scan
        // exactly like a readable one, it is simply unreadable.
        self.records
            .keys()
            .chain(self.faulty.keys())
            .max()
            .copied()
            .unwrap_or(Slot(0))
    }

    fn sealed_sessions(&self) -> Vec<SessionEntry> {
        self.sealed
            .iter()
            .map(|(&(client, seq), &slot)| (client, seq, slot))
            .collect()
    }

    fn faulty_entries(&self) -> Vec<(Slot, Ballot)> {
        self.faulty
            .iter()
            .map(|(slot, ballot)| (*slot, *ballot))
            .collect()
    }
}
