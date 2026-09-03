//! **Phase 2**: the per-slot `Accept` rounds a leader streams, their vote
//! tallies, and the fair bounded page a re-send draws from.

use std::collections::{BTreeMap, BTreeSet};

use super::{Proposer, RESEND_BATCH};
use crate::membership::AcceptorConfig;
use crate::types::{Ballot, Command, Fingerprint, NodeId, Slot};

/// Volatile state of one in-flight per-slot Phase-2 (`Accept`) round.
#[derive(Clone, Debug)]
pub struct Round {
    /// The ballot this slot is being accepted under.
    pub(super) ballot: Ballot,
    /// The command being accepted for this slot.
    pub(super) command: Command,
    /// Acceptors (incl. self) that have accepted this slot's command at `ballot`.
    pub(super) accepted_by: BTreeSet<NodeId>,
}

impl Round {
    /// The ballot this slot is being accepted under.
    #[must_use]
    pub fn ballot(&self) -> Ballot {
        self.ballot
    }

    /// The command being accepted for this slot.
    #[must_use]
    pub fn command(&self) -> &Command {
        &self.command
    }
}

impl Proposer {
    // ---- Phase 2 ------------------------------------------------------------

    /// Every in-flight Phase-2 round, keyed by slot.
    #[must_use]
    pub fn rounds(&self) -> &BTreeMap<Slot, Round> {
        &self.rounds
    }

    /// Open the Phase-2 round for `slot` at `ballot`, with `own_vote` as its
    /// first accept when the proposer is itself an acceptor of the ballot's
    /// configuration and its promise allows the self-accept.
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
        command: Command,
        own_vote: Option<NodeId>,
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
            },
        );
    }

    /// Fold an `Accepted` from `from` into the round at `slot`: counted only
    /// for the round's own ballot and command fingerprint. Whether it
    /// counted. Which configurations `from` belongs to is the caller's
    /// guard; the decision ([`Proposer::decided`]) counts members only.
    pub fn fold_accepted(&mut self, from: NodeId, ballot: Ballot, slot: Slot, vhash: u64) -> bool {
        let Some(round) = self.rounds.get_mut(&slot) else {
            return false;
        };
        if round.ballot != ballot || round.command.fingerprint() != vhash {
            return false;
        }
        round.accepted_by.insert(from);
        true
    }

    /// Whether the round at `slot` holds a Phase-2 quorum of `config`: then
    /// its `(ballot, command)` is chosen.
    ///
    /// # Panics
    ///
    /// If a vote behind a decision came from outside `config`: the caller's
    /// guard refuses any other sender, restated here so the quorum arithmetic
    /// is never fed an id that could not have made a durable promise.
    #[must_use]
    pub fn decided(&self, slot: Slot, config: &AcceptorConfig) -> Option<(Ballot, Command)> {
        let round = self.rounds.get(&slot)?;
        if !config.has_phase2_quorum(&round.accepted_by) {
            return None;
        }
        assert!(
            round.accepted_by.iter().all(|n| config.contains(*n)),
            "every vote behind a decision comes from a configured acceptor"
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
    /// page.
    pub fn resend_page(&mut self) -> Vec<(Slot, Ballot, Command)> {
        // No round survives below the compaction floor (the cross-role
        // invariant `RawNode::assert_invariants` pins), so a fresh cursor
        // starts at the bottom of the map and needs no floor handed in.
        let start = self.resend_cursor.unwrap_or(Slot(0));
        let mut pending: Vec<(Slot, Ballot, Command)> = self
            .rounds
            .range(start..)
            .take(RESEND_BATCH)
            .map(|(s, r)| (*s, r.ballot, r.command.clone()))
            .collect();
        if pending.len() < RESEND_BATCH {
            let remaining = RESEND_BATCH - pending.len();
            pending.extend(
                self.rounds
                    .range(..start)
                    .take(remaining)
                    .map(|(s, r)| (*s, r.ballot, r.command.clone())),
            );
        }
        self.resend_cursor = pending
            .last()
            .and_then(|(slot, _, _)| slot.0.checked_add(1).map(Slot));
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
