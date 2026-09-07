//! **Phase 2**: the per-slot `Accept` rounds a leader streams, their vote
//! tallies, and the fair bounded page a re-send draws from.

use std::collections::{BTreeMap, BTreeSet};

use super::{Proposer, RESEND_BATCH};
use crate::membership::AcceptorConfig;
use crate::types::{Ballot, Fingerprint, Slot};

/// Volatile state of one in-flight per-slot Phase-2 (`Accept`) round.
#[derive(Clone, Debug)]
pub struct Round<Id, V> {
    /// The ballot this slot is being accepted under.
    pub(super) ballot: Ballot,
    /// The command being accepted for this slot.
    pub(super) command: V,
    /// Acceptors (incl. self) that have accepted this slot's command at `ballot`.
    pub(super) accepted_by: BTreeSet<Id>,
    /// The **column** this round was opened against
    /// ([`AcceptorConfig::column_of`] for the slot): the grid column its
    /// `Accept` was addressed to, the column a re-send addresses again, and
    /// the column its decision is judged by. `None` under a majority or a
    /// flexible split, which name no column.
    pub(super) column: Option<usize>,
}

impl<Id, V> Round<Id, V> {
    /// The ballot this slot is being accepted under.
    #[must_use]
    pub fn ballot(&self) -> Ballot {
        self.ballot
    }

    /// The command being accepted for this slot.
    #[must_use]
    pub fn command(&self) -> &V {
        &self.command
    }

    /// The column this round was opened against, `None` when the quorum
    /// system names no column.
    #[must_use]
    pub fn column(&self) -> Option<usize> {
        self.column
    }

    /// The acceptors (incl. self) whose accept at this round's ballot has
    /// been counted — what a Phase-2 re-send addresses the complement of.
    #[must_use]
    pub fn accepted_by(&self) -> &BTreeSet<Id> {
        &self.accepted_by
    }
}

/// One round of a re-send page ([`Proposer::resend_page`]): what its
/// `Accept` carries and the column it goes to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingAccept<V> {
    /// The round's slot.
    pub slot: Slot,
    /// The round's ballot.
    pub ballot: Ballot,
    /// The round's command.
    pub command: V,
    /// The column the round was opened against (`None`: no column).
    pub column: Option<usize>,
}

impl<Id: Copy + Ord, V> Proposer<Id, V> {
    // ---- the slot allocator -------------------------------------------------

    /// The next slot a fresh proposal is allocated at — the **allocator
    /// frontier**. Two nodes can only ever propose different commands at one
    /// `(slot, ballot)` if a successor rewinds it, which is why a handoff
    /// carries it explicitly and a receiver refuses one that moves it back.
    #[must_use]
    pub fn next_slot(&self) -> Slot {
        self.next_slot
    }

    /// Take the next slot and advance the frontier past it.
    pub fn allocate(&mut self) -> Slot {
        let slot = self.next_slot;
        self.next_slot = Slot(slot.0 + 1);
        slot
    }

    /// Install the frontier a fresh leadership starts allocating from: what a
    /// won Phase 1 derived from its quorum report, or what a handoff carried.
    pub fn set_next_slot(&mut self, slot: Slot) {
        self.next_slot = slot;
    }

    /// Raise the frontier to `slot` if it sits below — the monotone form an
    /// installed snapshot uses, whose boundary may sit above everything this
    /// node had.
    pub fn raise_next_slot(&mut self, slot: Slot) {
        self.next_slot = self.next_slot.max(slot);
    }
}

impl<Id: Copy + Ord, V: Clone + Fingerprint> Proposer<Id, V> {
    // ---- Phase 2 ------------------------------------------------------------

    /// Every in-flight Phase-2 round, keyed by slot.
    #[must_use]
    pub fn rounds(&self) -> &BTreeMap<Slot, Round<Id, V>> {
        &self.rounds
    }

    /// Open the Phase-2 round for `slot` at `ballot` against `column`, with
    /// `own_vote` as its first accept when the proposer is itself an
    /// addressee of that column and its promise allows the self-accept.
    /// `column` is what the configuration derived for the slot
    /// ([`AcceptorConfig::column_of`]) — `None` under a majority or a
    /// flexible split; the round remembers it so a re-send and the decision
    /// address and judge the same column.
    ///
    /// # Panics
    ///
    /// If a round is already open at `slot`: one round per slot per
    /// leadership — the allocator only hands out fresh slots, a recovery
    /// visits each inherited slot once, and a blocked slot is opened only by
    /// the probe that resolves it. A second round would let one
    /// `(slot, ballot)` carry two commands.
    pub fn open_round(
        &mut self,
        slot: Slot,
        ballot: Ballot,
        command: V,
        own_vote: Option<Id>,
        column: Option<usize>,
    ) {
        assert!(
            !self.rounds.contains_key(&slot),
            "a slot has at most one open Phase-2 round"
        );
        let mut accepted_by = BTreeSet::new();
        if let Some(me) = own_vote {
            accepted_by.insert(me);
        }
        self.rounds.insert(
            slot,
            Round {
                ballot,
                command,
                accepted_by,
                column,
            },
        );
    }

    /// The column the round at `slot` was opened against, if a round is
    /// open there — what the caller's addressee guard asks before folding
    /// an `Accepted` ([`AcceptorConfig::is_phase2_addressee`]).
    #[must_use]
    pub fn round_column(&self, slot: Slot) -> Option<Option<usize>> {
        self.rounds.get(&slot).map(Round::column)
    }

    /// Fold an `Accepted` from `from` into the round at `slot`: counted only
    /// for the round's own ballot and command fingerprint. Whether it
    /// counted. Whether `from` is an addressee of the round's column is the
    /// caller's guard; the decision ([`Proposer::decided`]) restates it.
    pub fn fold_accepted(&mut self, from: Id, ballot: Ballot, slot: Slot, vhash: u64) -> bool {
        let Some(round) = self.rounds.get_mut(&slot) else {
            return false;
        };
        if round.ballot != ballot || round.command.fingerprint() != vhash {
            return false;
        }
        round.accepted_by.insert(from);
        true
    }

    /// Whether the round at `slot` holds a Phase-2 quorum of `config` **in
    /// the round's column**: then its `(ballot, command)` is chosen. Under a
    /// grid the round was addressed to one column and only that full column
    /// decides it; a majority or a flexible split names no column and the
    /// whole membership tallies.
    ///
    /// # Panics
    ///
    /// If a vote behind a decision came from outside the round's column of
    /// `config`: the caller's guard refuses any other sender, restated here
    /// so the quorum predicate is never fed an id that could not have made a
    /// durable promise for this round.
    #[must_use]
    pub fn decided(&self, slot: Slot, config: &AcceptorConfig<Id>) -> Option<(Ballot, V)> {
        let round = self.rounds.get(&slot)?;
        if !config.has_phase2_quorum_in(&round.accepted_by, round.column) {
            return None;
        }
        assert!(
            round
                .accepted_by
                .iter()
                .all(|n| config.is_phase2_addressee(*n, round.column)),
            "every vote behind a decision comes from the round's column"
        );
        Some((round.ballot, round.command.clone()))
    }

    /// Close the round at `slot` (decided, or abandoned by a decision that
    /// arrived from elsewhere).
    pub fn close_round(&mut self, slot: Slot) {
        self.rounds.remove(&slot);
    }

    /// Drop every round below `first` (a compaction or a snapshot install
    /// folded those slots: they are chosen).
    pub fn retain_rounds_from(&mut self, first: Slot) {
        self.rounds.retain(|slot, _| *slot >= first);
    }

    /// The next fair page of rounds whose `Accept`s are to be re-sent: at
    /// most [`RESEND_BATCH`] rounds from the cursor up, wrapping
    /// around from the lowest round held, and the cursor advances past the
    /// page. Each entry carries the column the round was opened against, so
    /// the re-send addresses exactly the column the first send did.
    pub fn resend_page(&mut self) -> Vec<PendingAccept<V>> {
        // No round survives below the compaction floor (the cross-role
        // invariant `ColocatedNode::assert_invariants` pins), so a fresh cursor
        // starts at the bottom of the map and needs no floor handed in.
        let start = self.resend_cursor.unwrap_or(Slot(0));
        let page = |(s, r): (&Slot, &Round<Id, V>)| PendingAccept {
            slot: *s,
            ballot: r.ballot,
            command: r.command.clone(),
            column: r.column,
        };
        let mut pending: Vec<PendingAccept<V>> = self
            .rounds
            .range(start..)
            .take(RESEND_BATCH)
            .map(page)
            .collect();
        if pending.len() < RESEND_BATCH {
            let remaining = RESEND_BATCH - pending.len();
            pending.extend(self.rounds.range(..start).take(remaining).map(page));
        }
        self.resend_cursor = pending
            .last()
            .and_then(|p| p.slot.0.checked_add(1).map(Slot));
        pending
    }

    /// Whether a `Nack` for `ballot` at `slot` supersedes work this proposer
    /// has in flight: the open campaign at that ballot, or the open round at
    /// that slot and ballot.
    #[must_use]
    pub fn supersedes(&self, ballot: Ballot, slot: Slot) -> bool {
        self.election
            .as_ref()
            .is_some_and(|e| e.promises.ballot == ballot)
            || self.rounds.get(&slot).is_some_and(|r| r.ballot == ballot)
    }
}
