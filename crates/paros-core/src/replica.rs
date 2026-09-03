//! The **replica**: the chosen log and its application order, and nothing
//! else.
//!
//! A replica consumes one kind of fact — *slot `s` chose value `v`* — and
//! turns it into the contiguous applied prefix the application consumes,
//! never caring *why* a value was chosen (an accept quorum this node
//! counted, a `Commit` off the wire, a catch-up replay, a handoff's decided
//! tail). It owns:
//!
//! - the **chosen map** and the durable **chosen index** (the commit index:
//!   every slot at or below it is chosen and applied in order);
//! - the contiguous **walk** ([`Replica::advance`]) that surfaces newly
//!   applied entries, bounded per batch;
//! - the **at-most-once ledger** (#94): which `(client, seq)` applied at
//!   which slot, first slot wins, so a retry served across a partition and
//!   re-chosen at a second slot executes as a no-op cluster-wide;
//! - the **in-flight table** — the chosen-but-not-yet-applied window a
//!   client retry must be able to find its request in;
//! - the **application repair** cursor (Stage 8): the range the driver's
//!   boot replay could not walk, re-emitted in order as the values arrive.
//!
//! It knows nothing about ballots, promises, leadership, quorums or the
//! network. The one cross-component fact it consults is handed to it as
//! data: the acceptor's compaction floor (what is retained) and, for the
//! chosen/accepted coupling the walk asserts, the accepted log itself.
//! Durable changes are emitted as [`WriteOp`]s into the caller's batch.
//!
//! Hard `assert!`s throughout (AGENTS.md, *Assertion doctrine*).

use std::collections::{BTreeMap, BTreeSet};

use crate::node::LEADER_RECOVERY_BATCH;
use crate::types::{Ballot, ClientId, ClientSeq, Command, Control, Entry, SessionEntry, Slot};
use crate::write::WriteOp;

/// The replica: chosen log, applied prefix, dedup ledger. See the module doc.
#[derive(Clone, Debug)]
pub struct Replica {
    /// Every slot this node knows chosen, with its value — contiguous or not.
    chosen: BTreeMap<Slot, Command>,
    /// Highest contiguous chosen slot (the commit index), or `None` when
    /// nothing is chosen yet. Durable ([`WriteOp::SetChosenIndex`]).
    chosen_index: Option<Slot>,
    /// The walk released one bounded chunk and the next slot is already
    /// chosen: a deferred continuation the caller resumes after its batch.
    advance_pending: bool,
    /// The at-most-once ledger: `(client, seq) -> slot` for every identity
    /// applied, at its **first** slot — rebuilt on boot from the sealed
    /// records plus the retained log, so a restart suppresses exactly the
    /// slots the pre-restart apply did.
    applied_seq: BTreeMap<ClientId, BTreeMap<ClientSeq, Slot>>,
    /// Chosen-but-not-applied (or proposed-but-undecided) client identities
    /// by slot: what a retry finds between allocation and application.
    inflight: BTreeMap<(ClientId, ClientSeq), Slot>,
    /// Slots executed as no-ops because their identity applied earlier (#94).
    duplicate_slots: BTreeSet<Slot>,
    /// How many duplicates the walk suppressed this incarnation.
    duplicates_suppressed: u64,
    /// The open application repair's cursor: the next decided slot to
    /// re-emit, always inside the chosen prefix.
    app_repair: Option<Slot>,
    /// Newly applied `(slot, command)` pairs, in order, for the caller's
    /// `Ready` batch.
    committed: Vec<(Slot, Command)>,
}

impl Replica {
    /// Rebuild the replica from what a boot scan read back: the durable
    /// chosen index, the sealed session records, and the retained accepted
    /// log (every record at or below the chosen index carries the chosen
    /// value — the P2c chain).
    ///
    /// The ledger starts from the sealed records — the `(client, seq) ->
    /// slot` facts whose log records truncation (or a snapshot install)
    /// already dropped — and the walk over the retained log layers on top
    /// with first-slot-wins semantics. Sealed slots are always below the
    /// compaction floor, so the two sources never disagree; seeding sealed
    /// first is what keeps a restarted node's duplicate-suppression
    /// decisions identical to a node that held the whole log in memory.
    #[must_use]
    pub fn from_boot(
        chosen_index: Option<Slot>,
        sealed: impl IntoIterator<Item = SessionEntry>,
        records: &BTreeMap<Slot, (Ballot, Command)>,
    ) -> Self {
        let mut applied_seq: BTreeMap<ClientId, BTreeMap<ClientSeq, Slot>> = BTreeMap::new();
        for (client, seq, slot) in sealed {
            applied_seq.entry(client).or_default().insert(seq, slot);
        }
        let mut chosen = BTreeMap::new();
        let mut inflight = BTreeMap::new();
        let mut duplicate_slots = BTreeSet::new();
        for (slot, (_b, command)) in records {
            let is_chosen = chosen_index.is_some_and(|ci| *slot <= ci);
            if is_chosen {
                chosen.insert(*slot, command.clone());
                // Only client entries carry a `(client, seq)` dedup key; a
                // control command never dedups. Every executed seq is
                // recorded, not just the latest per client — and only at its
                // **first** (lowest) slot: a second chosen slot for the same
                // identity is the #94 duplicate, re-derived here exactly as
                // the live walk derived it.
                if let Command::User(entry) = command {
                    let seqs = applied_seq.entry(entry.client).or_default();
                    match seqs.get(&entry.seq) {
                        Some(&first) if first != *slot => {
                            duplicate_slots.insert(*slot);
                        }
                        _ => {
                            seqs.insert(entry.seq, *slot);
                        }
                    }
                }
            } else if let Command::User(entry) = command {
                inflight.insert((entry.client, entry.seq), *slot);
            }
        }
        Self {
            chosen,
            chosen_index,
            advance_pending: false,
            applied_seq,
            inflight,
            duplicate_slots,
            duplicates_suppressed: 0,
            app_repair: None,
            committed: Vec::new(),
        }
    }

    /// The replica's own cross-field invariants against the retention floor
    /// `floor` (the acceptor's compaction floor, handed in as data).
    ///
    /// # Panics
    ///
    /// Panics when a replica invariant is broken: a programmer error, never
    /// an operating condition.
    pub fn assert_invariants(&self, floor: Slot) {
        // A chosen first-unchosen slot is legal only as the explicit bounded
        // continuation left by the walk — an iff, split into its two
        // directions so a violation names the side that broke.
        if self.chosen.contains_key(&self.first_unchosen()) {
            assert!(
                self.advance_pending,
                "a chosen first-unchosen slot has a deferred prefix continuation"
            );
        }
        if self.advance_pending {
            assert!(
                self.chosen.contains_key(&self.first_unchosen()),
                "a deferred prefix continuation names a chosen first-unchosen slot"
            );
        }
        // The application repair cursor only ever points inside the chosen
        // prefix (there is nothing decided to re-emit past it).
        assert!(
            self.app_repair.is_none_or(|s| s < self.first_unchosen()),
            "the application repair cursor stays inside the chosen prefix"
        );
        assert!(
            self.chosen.keys().next().is_none_or(|s| *s >= floor),
            "no chosen record survives below the compaction floor"
        );
        assert!(
            self.duplicate_slots.first().is_none_or(|s| *s >= floor),
            "no duplicate marker survives below the compaction floor"
        );
        // Floor-bound structural check needing a full scan (`inflight` is
        // keyed by client identity, so its slots are unordered).
        assert!(
            self.inflight.values().all(|s| *s >= floor),
            "no in-flight dedup mapping survives below the compaction floor"
        );
    }

    // ---- reads --------------------------------------------------------------

    /// The durable chosen index (the commit index).
    #[must_use]
    pub fn chosen_index(&self) -> Option<Slot> {
        self.chosen_index
    }

    /// First slot not in the contiguous chosen prefix.
    #[must_use]
    pub fn first_unchosen(&self) -> Slot {
        match self.chosen_index {
            Some(s) => Slot(s.0 + 1),
            None => Slot(0),
        }
    }

    /// Every slot known chosen, contiguous or not.
    #[must_use]
    pub fn chosen(&self) -> &BTreeMap<Slot, Command> {
        &self.chosen
    }

    /// Whether `slot` is known chosen here.
    #[must_use]
    pub fn is_chosen(&self, slot: Slot) -> bool {
        self.chosen.contains_key(&slot)
    }

    /// The value chosen at `slot`, if known.
    #[must_use]
    pub fn chosen_at(&self, slot: Slot) -> Option<&Command> {
        self.chosen.get(&slot)
    }

    /// Whether the walk left a deferred continuation (see
    /// [`Replica::advance`]).
    #[must_use]
    pub fn advance_pending(&self) -> bool {
        self.advance_pending
    }

    /// The slot `(client, seq)` applied at, if it did.
    #[must_use]
    pub fn applied_at(&self, client: ClientId, seq: ClientSeq) -> Option<Slot> {
        self.applied_seq
            .get(&client)
            .and_then(|m| m.get(&seq))
            .copied()
    }

    /// The slot `(client, seq)` is in flight at, if it is.
    #[must_use]
    pub fn inflight_at(&self, client: ClientId, seq: ClientSeq) -> Option<Slot> {
        self.inflight.get(&(client, seq)).copied()
    }

    /// Whether `entry`'s `(client, seq)` identity is recorded in the applied
    /// ledger at a slot **other than** `slot` — the #94 duplicate test.
    #[must_use]
    pub fn applied_elsewhere(&self, entry: &Entry, slot: Slot) -> bool {
        self.applied_at(entry.client, entry.seq)
            .is_some_and(|first| first != slot)
    }

    /// The at-most-once ledger, whole.
    #[must_use]
    pub fn session_ledger(&self) -> &BTreeMap<ClientId, BTreeMap<ClientSeq, Slot>> {
        &self.applied_seq
    }

    /// Slots executed as no-ops because their identity applied earlier.
    #[must_use]
    pub fn duplicate_slots(&self) -> &BTreeSet<Slot> {
        &self.duplicate_slots
    }

    /// How many duplicates the walk suppressed this incarnation.
    #[must_use]
    pub fn duplicates_suppressed(&self) -> u64 {
        self.duplicates_suppressed
    }

    /// The open application repair's cursor, if any.
    #[must_use]
    pub fn app_repair(&self) -> Option<Slot> {
        self.app_repair
    }

    /// Newly applied entries this batch, in order.
    #[must_use]
    pub fn committed(&self) -> &[(Slot, Command)] {
        &self.committed
    }

    /// Drop the batch's applied entries (the caller consumed them).
    pub fn clear_committed(&mut self) {
        self.committed.clear();
    }

    /// The **chosen gap**, if this node holds one: `(hole, highest)` where
    /// `hole` is the first slot missing from the contiguous prefix and
    /// `highest` the highest slot above it already known chosen. `None` when
    /// the chosen set is contiguous. An open application repair's cursor is
    /// the hole while it is open.
    #[must_use]
    pub fn chosen_gap(&self) -> Option<(Slot, Slot)> {
        if let Some(hole) = self.app_repair {
            let highest = self
                .chosen
                .keys()
                .next_back()
                .copied()
                .unwrap_or(hole)
                .max(self.chosen_index.unwrap_or(hole));
            return Some((hole, highest));
        }
        let hole = self.first_unchosen();
        let highest = *self.chosen.range(hole..).next_back()?.0;
        Some((hole, highest))
    }

    /// The session records whose slots lie in `[from, to)` — what a
    /// truncation seals so a restart still recognizes them. Read from the
    /// *ledger*, not from the dropped chosen range: a duplicate slot's chosen
    /// command is a `User` entry whose ledger record points at its first
    /// slot, and sealing the duplicate's own slot would corrupt the ledger.
    #[must_use]
    pub fn seal(&self, from: Slot, to: Slot) -> Vec<SessionEntry> {
        self.applied_seq
            .iter()
            .flat_map(|(client, seqs)| {
                seqs.iter()
                    .filter(|entry| *entry.1 >= from && *entry.1 < to)
                    .map(|(&seq, &slot)| (*client, seq, slot))
            })
            .collect()
    }

    // ---- learning -------------------------------------------------------------

    /// Point the in-flight table at `slot` for `(client, seq)` — a fresh
    /// allocation, or a re-proposal of an inherited client entry.
    pub fn track_inflight(&mut self, client: ClientId, seq: ClientSeq, slot: Slot) {
        self.inflight.insert((client, seq), slot);
    }

    /// Learn `slot` chosen with `command`. The caller has already checked the
    /// slot is retained and not yet known chosen here.
    ///
    /// Re-points `inflight` at what this slot actually decided: whatever was
    /// in flight *for this slot* is dropped (the slot can no longer be the
    /// landing place of some other request), and the entry this slot did
    /// decide is mapped to it unless its identity already applied elsewhere
    /// (a #94 duplicate suppresses to a no-op at apply, so a retry must hit
    /// the ledger fast path instead of parking on it). A slot healed *below*
    /// the contiguous prefix never reaches the walk, so its ledger fold
    /// happens here, min-slot-wins.
    ///
    /// # Panics
    ///
    /// If `slot` is already known chosen.
    pub fn learn(&mut self, slot: Slot, command: &Command) {
        assert!(
            !self.chosen.contains_key(&slot),
            "a slot is learned chosen once"
        );
        self.chosen.insert(slot, command.clone());
        self.inflight.retain(|_, s| *s != slot);
        if let Command::User(entry) = command
            && !self.applied_elsewhere(entry, slot)
        {
            self.inflight.insert((entry.client, entry.seq), slot);
        }
        if slot < self.first_unchosen()
            && let Command::User(entry) = command
        {
            let seqs = self.applied_seq.entry(entry.client).or_default();
            match seqs.get(&entry.seq).copied() {
                Some(first) if first < slot => {
                    self.duplicate_slots.insert(slot);
                }
                Some(first) if first > slot => {
                    self.duplicate_slots.insert(first);
                    self.duplicate_slots.remove(&slot);
                    seqs.insert(entry.seq, slot);
                }
                _ => {
                    seqs.insert(entry.seq, slot);
                }
            }
        }
    }

    /// Walk the contiguous chosen prefix forward, surfacing each newly
    /// applied `(slot, command)` in order (no gaps), bounded per batch, and
    /// moving each identity's dedup state from "in flight" to "applied".
    /// Returns the highest `up_to` of any [`Control::Truncate`] the walk
    /// applied, for the caller to compact *after* the walk.
    ///
    /// `records` is the accepted log, consulted only for the coupling
    /// assertion that what the application is handed is what the
    /// authoritative record holds.
    ///
    /// # Panics
    ///
    /// If the walk's contiguity or the chosen/accepted coupling is broken.
    pub fn advance(
        &mut self,
        records: &BTreeMap<Slot, (Ballot, Command)>,
        writes: &mut Vec<WriteOp>,
    ) -> Option<Slot> {
        let mut next = self.first_unchosen();
        let mut advanced = 0_usize;
        let mut truncate_up_to: Option<Slot> = None;
        while advanced < LEADER_RECOVERY_BATCH
            && let Some(mut command) = self.chosen.get(&next).cloned()
        {
            // The walk is the *only* writer of `chosen_index`, and it
            // advances exactly one slot per iteration — the contiguity the
            // apply seam and the boot rebuild are built on.
            assert!(
                next == self.first_unchosen(),
                "the chosen prefix advances one slot at a time"
            );
            // The chosen/accepted coupling, per applied slot.
            assert!(
                records.get(&next).map(|(_, c)| c) == Some(&command),
                "an applied slot's accepted record carries the applied command"
            );
            self.chosen_index = Some(next);
            writes.push(WriteOp::SetChosenIndex(next));
            if let Command::Control(Control::Truncate { up_to }) = &command {
                truncate_up_to = Some(truncate_up_to.map_or(*up_to, |u| u.max(*up_to)));
            }
            self.inflight.retain(|_, s| *s != next);
            if let Command::User(entry) = &command {
                let seqs = self.applied_seq.entry(entry.client).or_default();
                match seqs.get(&entry.seq) {
                    // The #94 duplicate: execute the slot as a no-op. The
                    // decision reads only the replicated ledger, and the walk
                    // runs in slot order on every node, so first-slot-wins is
                    // cluster-wide deterministic.
                    Some(&first) if first != next => {
                        self.duplicate_slots.insert(next);
                        self.duplicates_suppressed += 1;
                        command = Command::Control(Control::Noop);
                    }
                    _ => {
                        seqs.insert(entry.seq, next);
                    }
                }
            }
            // While an application repair is open, the driver's application
            // sits below this walk's frontier: the repair pump re-emits every
            // decided slot in order from its cursor instead.
            if self.app_repair.is_none() {
                self.committed.push((next, command));
            }
            next = Slot(next.0 + 1);
            advanced += 1;
        }
        self.advance_pending = self.chosen.contains_key(&self.first_unchosen());
        // Postcondition: either the walk consumed the entire contiguous
        // chosen prefix, or exactly one bounded chunk was released.
        assert!(
            advanced == LEADER_RECOVERY_BATCH || !self.chosen.contains_key(&self.first_unchosen()),
            "the walk consumes or bounds the contiguous chosen prefix"
        );
        truncate_up_to
    }

    // ---- application repair ---------------------------------------------------

    /// Open an application repair from `from` (inside the chosen prefix).
    ///
    /// # Panics
    ///
    /// If `from` lies past the contiguous chosen prefix.
    pub fn open_app_repair(&mut self, from: Slot) {
        assert!(
            from <= self.first_unchosen(),
            "an application repair starts inside the chosen prefix"
        );
        if from >= self.first_unchosen() {
            return;
        }
        self.app_repair = Some(from);
    }

    /// Re-emit the next run of decided commands the open repair can serve:
    /// from the cursor, while each slot's value is present, bounded per
    /// batch. Stops at `floor` (only a snapshot heals below it) or at the
    /// first still-missing value. Closes the repair at the prefix frontier.
    pub fn pump_app_repair(&mut self, floor: Slot) {
        let Some(mut cursor) = self.app_repair else {
            return;
        };
        let end = self.first_unchosen();
        let mut emitted = 0_usize;
        while cursor < end && emitted < LEADER_RECOVERY_BATCH {
            if cursor < floor {
                break;
            }
            let Some(command) = self.chosen.get(&cursor).cloned() else {
                break;
            };
            let command = if self.duplicate_slots.contains(&cursor) {
                Command::Control(Control::Noop)
            } else {
                command
            };
            self.committed.push((cursor, command));
            cursor = Slot(cursor.0 + 1);
            emitted += 1;
        }
        self.app_repair = if cursor >= end { None } else { Some(cursor) };
    }

    // ---- log prefix drops -----------------------------------------------------

    /// Drop everything below `first` after a decided truncation: the walked
    /// prefix is applied, its ledger records were sealed by the caller.
    pub fn truncate(&mut self, first: Slot) {
        self.chosen = self.chosen.split_off(&first);
        self.advance_pending = self.chosen.contains_key(&self.first_unchosen());
        // The contiguous walk already handed every applied slot's `inflight`
        // entry over, except the below-prefix heals recorded while an
        // application repair is open — and a mapping into the truncated
        // prefix would answer a retry with a `Duplicate` whose commit can
        // never ack anyone.
        self.inflight.retain(|_, s| *s >= first);
        // The markers for the dropped prefix are spent: a restarted node
        // re-derives the set from the *retained* log only.
        self.duplicate_slots = self.duplicate_slots.split_off(&first);
    }

    /// Install a snapshot boundary: jump the chosen index to `chosen_index`,
    /// fold everything at or below it (its state is in the opaque bytes),
    /// close an application repair the boundary covers, and adopt the
    /// serving peer's session records for the folded prefix (`or_insert`:
    /// the prefixes agree cluster-wide).
    pub fn install(&mut self, chosen_index: Slot, sessions: &[SessionEntry]) {
        let first = Slot(chosen_index.0.saturating_add(1));
        self.chosen_index = Some(chosen_index);
        self.chosen = self.chosen.split_off(&first);
        self.duplicate_slots = self.duplicate_slots.split_off(&first);
        if self.app_repair.is_some_and(|cursor| cursor <= chosen_index) {
            self.app_repair = None;
        }
        self.advance_pending = self.chosen.contains_key(&self.first_unchosen());
        // The prefix jumped without the walk running, so nothing handed the
        // folded slots' `inflight` entries over. Drop them.
        self.inflight.retain(|_, s| *s >= first);
        for (client, seq, slot) in sessions {
            self.applied_seq
                .entry(*client)
                .or_default()
                .entry(*seq)
                .or_insert(*slot);
        }
    }
}
