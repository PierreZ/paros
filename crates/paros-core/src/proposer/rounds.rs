//! **Phase 2**: the per-slot `Accept` rounds a leader streams, their vote
//! tallies, and the fair bounded page a re-send draws from — as a
//! **standalone tally**, [`Rounds`], that the [`Proposer`] embeds and
//! delegates to.
//!
//! Why standalone (#142, rung 0): a Phase-2 round is the one tally of the
//! proposer that another deployment runs *without* the rest of the role.
//! Compartmentalized Paxos's proxy leader (§3.1) fans a delegated `Accept`
//! out, folds the `Accepted`s and emits the `Commit` — a Phase-2 tally and
//! nothing else, no election, no recovery, no read fence — and the rule that
//! there is **no second Phase-2 kernel** in the crate (exactly as the
//! matchmaker-set decree reuses `Proposer` / `Acceptor` at slot zero rather
//! than growing a kernel of its own) means it must be *this* tally, embedded.
//! So the tally lives here as its own type with its own cursor, and the
//! proposer's Phase-2 surface is a delegation to it: the proposer's callers
//! and every existing test are unchanged, the decree kernel (which drives the
//! proposer at slot zero) is untouched, and the proxy leader of rung 1 is
//! a `Rounds` plus routing.
//!
//! What the tally deliberately does not know, like every role: who the
//! leader is, what a message looks like, which configuration is in force
//! (the caller hands the configuration in when it asks for a decision), or
//! what a value *means* — it needs only [`Fingerprint`], the "which value is
//! this" an `Accepted` reports.

use std::collections::{BTreeMap, BTreeSet};

use super::{Proposer, RESEND_BATCH};
use crate::membership::{AcceptorConfig, ProxyId};
use crate::types::{Ballot, Fingerprint, Slot};

/// **Who folds a round's `Accepted`s**: the opener itself, or a proxy leader
/// it delegated the round to (#142). An explicit state beside the counted
/// one, never a flag: the two answer every question differently — a
/// delegated round counts no vote here, decides nothing here, and is re-sent
/// as a re-delegation rather than a fan-out.
#[derive(Clone, Debug)]
pub enum Custody<Id> {
    /// The opener fans the `Accept` out and folds the `Accepted`s.
    Colocated {
        /// Acceptors (incl. self) that accepted the round's command at its
        /// ballot.
        accepted_by: BTreeSet<Id>,
    },
    /// A proxy fans out and folds; the opener learns the decision from the
    /// proxy's `Commit` and remembers only whom it asked and how often it
    /// asked again — the count a take-back is judged on.
    Delegated {
        /// The proxy the round was handed to.
        proxy: ProxyId,
        /// How many re-send pages have re-delegated this round without a
        /// decision arriving.
        redelegations: u64,
    },
}

/// Volatile state of one in-flight per-slot Phase-2 (`Accept`) round.
#[derive(Clone, Debug)]
pub struct Round<Id, V> {
    /// The ballot this slot is being accepted under.
    ballot: Ballot,
    /// The command being accepted for this slot.
    command: V,
    /// Who folds this round's `Accepted`s ([`Custody`]).
    custody: Custody<Id>,
    /// The **column** this round was opened against
    /// ([`AcceptorConfig::column_of`] for the slot): the grid column its
    /// `Accept` was addressed to, the column a re-send addresses again, and
    /// the column its decision is judged by. `None` under a majority or a
    /// flexible split, which name no column.
    column: Option<usize>,
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

    /// Who folds this round's `Accepted`s.
    #[must_use]
    pub fn custody(&self) -> &Custody<Id> {
        &self.custody
    }

    /// The proxy this round is delegated to, if it is.
    #[must_use]
    pub fn proxy(&self) -> Option<ProxyId> {
        match &self.custody {
            Custody::Delegated { proxy, .. } => Some(*proxy),
            Custody::Colocated { .. } => None,
        }
    }

    /// The acceptors (incl. self) whose accept at this round's ballot has
    /// been counted **here** — what a Phase-2 re-send addresses the
    /// complement of. `None` on a delegated round: its votes are the proxy's.
    #[must_use]
    pub fn accepted_by(&self) -> Option<&BTreeSet<Id>> {
        match &self.custody {
            Custody::Colocated { accepted_by } => Some(accepted_by),
            Custody::Delegated { .. } => None,
        }
    }
}

/// One round of a re-send page ([`Rounds::resend_page`]): what its
/// `Accept` carries, the column it goes to, and the proxy it goes through
/// when the round is delegated.
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
    /// The proxy the round is delegated to (`None`: colocated — the
    /// re-send fans out to the column itself).
    pub proxy: Option<ProxyId>,
}

/// The **Phase-2 tally**: every in-flight per-slot round and the fair cursor
/// a bounded re-send walks them with (see the module doc). Volatile, like
/// everything the proposer holds: it dies whole with the leadership that
/// streamed it ([`Rounds::clear`]).
///
/// Generic over the acceptor identity `Id` its quorums are counted in and
/// the value `V` its rounds carry, exactly as the [`Proposer`] is.
#[derive(Clone, Debug)]
pub struct Rounds<Id, V> {
    /// Per-slot in-flight rounds, keyed by slot.
    by_slot: BTreeMap<Slot, Round<Id, V>>,
    /// Fair cursor for bounded pending-`Accept` re-sends.
    resend_cursor: Option<Slot>,
}

impl<Id, V> Default for Rounds<Id, V> {
    fn default() -> Self {
        Self {
            by_slot: BTreeMap::new(),
            resend_cursor: None,
        }
    }
}

impl<Id, V> Rounds<Id, V> {
    /// A tally with no round open.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Every in-flight round, keyed by slot.
    #[must_use]
    pub fn by_slot(&self) -> &BTreeMap<Slot, Round<Id, V>> {
        &self.by_slot
    }

    /// Whether no round is open.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_slot.is_empty()
    }

    /// The column the round at `slot` was opened against, if a round is
    /// open there — what the caller's addressee guard asks before folding
    /// an `Accepted` ([`AcceptorConfig::is_phase2_addressee`]).
    #[must_use]
    pub fn column(&self, slot: Slot) -> Option<Option<usize>> {
        self.by_slot.get(&slot).map(Round::column)
    }

    /// Close the round at `slot` (decided, or abandoned by a decision that
    /// arrived from elsewhere).
    pub fn close(&mut self, slot: Slot) {
        self.by_slot.remove(&slot);
    }

    /// Drop every round below `first` (a compaction or a snapshot install
    /// folded those slots: they are chosen).
    pub fn retain_from(&mut self, first: Slot) {
        self.by_slot.retain(|slot, _| *slot >= first);
    }

    /// Drop every round and the re-send cursor: the tally dies whole with
    /// the leadership that streamed it.
    pub fn clear(&mut self) {
        self.by_slot.clear();
        self.resend_cursor = None;
    }

    /// Whether a round is open at `slot` **at `ballot`** — the round half of
    /// "does this `Nack` supersede work in flight" ([`Proposer::supersedes`]).
    #[must_use]
    pub fn is_open_at(&self, slot: Slot, ballot: Ballot) -> bool {
        self.by_slot.get(&slot).is_some_and(|r| r.ballot == ballot)
    }

    /// The delegated rounds re-delegated at least `after` times without a
    /// decision: what a leader **takes back** and runs colocated
    /// ([`Rounds::take_back`]). Liveness under a dead proxy is the opener's,
    /// and this is how it notices; the threshold is the caller's policy.
    #[must_use]
    pub fn stalled_delegations(&self, after: u64) -> Vec<Slot> {
        self.by_slot
            .iter()
            .filter(|(_, r)| {
                matches!(r.custody, Custody::Delegated { redelegations, .. } if redelegations >= after)
            })
            .map(|(s, _)| *s)
            .collect()
    }
}

impl<Id: Copy + Ord, V: Clone + Fingerprint> Rounds<Id, V> {
    /// Open the round for `slot` at `ballot` against `column`, with
    /// `own_vote` as its first accept when the opener is itself an
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
    pub fn open(
        &mut self,
        slot: Slot,
        ballot: Ballot,
        command: V,
        own_vote: Option<Id>,
        column: Option<usize>,
    ) {
        assert!(
            !self.by_slot.contains_key(&slot),
            "a slot has at most one open Phase-2 round"
        );
        let mut accepted_by = BTreeSet::new();
        if let Some(me) = own_vote {
            accepted_by.insert(me);
        }
        self.by_slot.insert(
            slot,
            Round {
                ballot,
                command,
                custody: Custody::Colocated { accepted_by },
                column,
            },
        );
    }

    /// Open the round for `slot` at `ballot` against `column` **delegated**
    /// to `proxy` (#142): the proxy fans the `Accept` out and folds the
    /// `Accepted`s, and this tally only remembers the round exists — so the
    /// allocator, the handoff tiling and the re-send page keep seeing it —
    /// until the proxy's `Commit` closes it or the opener takes it back.
    ///
    /// # Panics
    ///
    /// If a round is already open at `slot` (see [`Rounds::open`]).
    pub fn open_delegated(
        &mut self,
        slot: Slot,
        ballot: Ballot,
        command: V,
        column: Option<usize>,
        proxy: ProxyId,
    ) {
        assert!(
            !self.by_slot.contains_key(&slot),
            "a slot has at most one open Phase-2 round"
        );
        self.by_slot.insert(
            slot,
            Round {
                ballot,
                command,
                custody: Custody::Delegated {
                    proxy,
                    redelegations: 0,
                },
                column,
            },
        );
    }

    /// **Take a delegated round back**: the proxy did not decide it, so the
    /// opener folds it itself from here on, seeded with `own_vote` when the
    /// opener already accepted the round's command (its own acceptor record
    /// says so). Whether the round was delegated — a colocated round is left
    /// as it is. Always safe: a second fan-out of one `(slot, ballot,
    /// command)` is P2b-idempotent, so nothing the proxy may still do behind
    /// this can disagree with what the opener decides.
    pub fn take_back(&mut self, slot: Slot, own_vote: Option<Id>) -> bool {
        let Some(round) = self.by_slot.get_mut(&slot) else {
            return false;
        };
        if !matches!(round.custody, Custody::Delegated { .. }) {
            return false;
        }
        let mut accepted_by = BTreeSet::new();
        if let Some(me) = own_vote {
            accepted_by.insert(me);
        }
        round.custody = Custody::Colocated { accepted_by };
        true
    }

    /// Fold an `Accepted` from `from` into the round at `slot`: counted only
    /// for the round's own ballot and command fingerprint, and only on a
    /// **colocated** round — a delegated round's votes are the proxy's, and
    /// an `Accepted` that reaches the opener for one (a stray reply, a reply
    /// that outran a take-back) is not counted. Whether it counted. Whether
    /// `from` is an addressee of the round's column is the caller's guard;
    /// the decision ([`Rounds::decided`]) restates it.
    pub fn fold_accepted(&mut self, from: Id, ballot: Ballot, slot: Slot, vhash: u64) -> bool {
        let Some(round) = self.by_slot.get_mut(&slot) else {
            return false;
        };
        if round.ballot != ballot || round.command.fingerprint() != vhash {
            return false;
        }
        match &mut round.custody {
            Custody::Colocated { accepted_by } => {
                accepted_by.insert(from);
                true
            }
            Custody::Delegated { .. } => false,
        }
    }

    /// Whether the round at `slot` holds a Phase-2 quorum of `config` **in
    /// the round's column**: then its `(ballot, command)` is chosen. Under a
    /// grid the round was addressed to one column and only that full column
    /// decides it; a majority or a flexible split names no column and the
    /// whole membership tallies. A delegated round never decides here.
    ///
    /// # Panics
    ///
    /// If a vote behind a decision came from outside the round's column of
    /// `config`: the caller's guard refuses any other sender, restated here
    /// so the quorum predicate is never fed an id that could not have made a
    /// durable promise for this round.
    #[must_use]
    pub fn decided(&self, slot: Slot, config: &AcceptorConfig<Id>) -> Option<(Ballot, V)> {
        let round = self.by_slot.get(&slot)?;
        let accepted_by = round.accepted_by()?;
        if !config.has_phase2_quorum_in(accepted_by, round.column) {
            return None;
        }
        assert!(
            accepted_by
                .iter()
                .all(|n| config.is_phase2_addressee(*n, round.column)),
            "every vote behind a decision comes from the round's column"
        );
        Some((round.ballot, round.command.clone()))
    }

    /// The next fair page of rounds whose `Accept`s are to be re-sent: at
    /// most [`RESEND_BATCH`] rounds from the cursor up, wrapping
    /// around from the lowest round held, and the cursor advances past the
    /// page. Each entry carries the column the round was opened against, so
    /// the re-send addresses exactly the column the first send did, and the
    /// proxy a delegated round goes through — a re-send of a delegated round
    /// is a **re-delegation**, counted on the round so a stall is visible
    /// ([`Rounds::stalled_delegations`]).
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
            proxy: r.proxy(),
        };
        let mut pending: Vec<PendingAccept<V>> = self
            .by_slot
            .range(start..)
            .take(RESEND_BATCH)
            .map(page)
            .collect();
        if pending.len() < RESEND_BATCH {
            let remaining = RESEND_BATCH - pending.len();
            pending.extend(self.by_slot.range(..start).take(remaining).map(page));
        }
        for accept in &pending {
            if let Some(Round {
                custody: Custody::Delegated { redelegations, .. },
                ..
            }) = self.by_slot.get_mut(&accept.slot)
            {
                *redelegations = redelegations.saturating_add(1);
            }
        }
        self.resend_cursor = pending
            .last()
            .and_then(|p| p.slot.0.checked_add(1).map(Slot));
        pending
    }
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
    // ---- Phase 2: delegated to the embedded `Rounds` ------------------------

    /// Every in-flight Phase-2 round, keyed by slot.
    #[must_use]
    pub fn rounds(&self) -> &BTreeMap<Slot, Round<Id, V>> {
        self.rounds.by_slot()
    }

    /// Open the Phase-2 round for `slot` at `ballot` against `column`
    /// ([`Rounds::open`]).
    ///
    /// # Panics
    ///
    /// If a round is already open at `slot` (see [`Rounds::open`]).
    pub fn open_round(
        &mut self,
        slot: Slot,
        ballot: Ballot,
        command: V,
        own_vote: Option<Id>,
        column: Option<usize>,
    ) {
        self.rounds.open(slot, ballot, command, own_vote, column);
    }

    /// Open the Phase-2 round for `slot` at `ballot` against `column`,
    /// delegated to `proxy` ([`Rounds::open_delegated`]).
    ///
    /// # Panics
    ///
    /// If a round is already open at `slot` (see [`Rounds::open`]).
    pub fn open_delegated_round(
        &mut self,
        slot: Slot,
        ballot: Ballot,
        command: V,
        column: Option<usize>,
        proxy: ProxyId,
    ) {
        self.rounds
            .open_delegated(slot, ballot, command, column, proxy);
    }

    /// Take the delegated round at `slot` back into this proposer's own
    /// custody ([`Rounds::take_back`]). Whether it was delegated.
    pub fn take_back_round(&mut self, slot: Slot, own_vote: Option<Id>) -> bool {
        self.rounds.take_back(slot, own_vote)
    }

    /// The delegated rounds re-delegated at least `after` times without a
    /// decision ([`Rounds::stalled_delegations`]).
    #[must_use]
    pub fn stalled_delegations(&self, after: u64) -> Vec<Slot> {
        self.rounds.stalled_delegations(after)
    }

    /// The column the round at `slot` was opened against, if a round is
    /// open there ([`Rounds::column`]).
    #[must_use]
    pub fn round_column(&self, slot: Slot) -> Option<Option<usize>> {
        self.rounds.column(slot)
    }

    /// Fold an `Accepted` from `from` into the round at `slot`
    /// ([`Rounds::fold_accepted`]). Whether it counted.
    pub fn fold_accepted(&mut self, from: Id, ballot: Ballot, slot: Slot, vhash: u64) -> bool {
        self.rounds.fold_accepted(from, ballot, slot, vhash)
    }

    /// Whether the round at `slot` holds a Phase-2 quorum of `config` in the
    /// round's column: then its `(ballot, command)` is chosen
    /// ([`Rounds::decided`]).
    ///
    /// # Panics
    ///
    /// If a vote behind a decision came from outside the round's column
    /// (see [`Rounds::decided`]).
    #[must_use]
    pub fn decided(&self, slot: Slot, config: &AcceptorConfig<Id>) -> Option<(Ballot, V)> {
        self.rounds.decided(slot, config)
    }

    /// Close the round at `slot` ([`Rounds::close`]).
    pub fn close_round(&mut self, slot: Slot) {
        self.rounds.close(slot);
    }

    /// Drop every round below `first` ([`Rounds::retain_from`]).
    pub fn retain_rounds_from(&mut self, first: Slot) {
        self.rounds.retain_from(first);
    }

    /// The next fair page of rounds whose `Accept`s are to be re-sent
    /// ([`Rounds::resend_page`]).
    pub fn resend_page(&mut self) -> Vec<PendingAccept<V>> {
        self.rounds.resend_page()
    }

    /// Whether a `Nack` for `ballot` at `slot` supersedes work this proposer
    /// has in flight: the open campaign at that ballot, or the open round at
    /// that slot and ballot.
    #[must_use]
    pub fn supersedes(&self, ballot: Ballot, slot: Slot) -> bool {
        self.election
            .as_ref()
            .is_some_and(|e| e.promises.ballot == ballot)
            || self.rounds.is_open_at(slot, ballot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::QuorumSystem;
    use crate::types::{ClientId, ClientSeq, Command, Entry, NodeId, Value, command_fingerprint};

    fn ballot(round: u64, node: u64) -> Ballot {
        Ballot {
            round,
            node: NodeId(node),
        }
    }

    fn cmd(seq: u64) -> Command {
        Command::User(Entry {
            client: ClientId(1),
            seq: ClientSeq(seq),
            value: Value(seq.to_le_bytes().to_vec()),
        })
    }

    /// The tally stands on its own: opened, folded, decided and closed
    /// with no proposer around it — the shape the proxy leader embeds.
    #[test]
    fn a_standalone_tally_decides_on_a_configuration_quorum() {
        let config = AcceptorConfig::new(
            vec![NodeId(0), NodeId(1), NodeId(2)],
            QuorumSystem::Majority,
        );
        let mut rounds: Rounds<NodeId, Command> = Rounds::new();
        assert!(rounds.is_empty());
        rounds.open(Slot(5), ballot(1, 0), cmd(1), Some(NodeId(0)), None);
        assert_eq!(rounds.column(Slot(5)), Some(None));
        assert!(rounds.decided(Slot(5), &config).is_none());
        assert!(
            !rounds.fold_accepted(NodeId(1), ballot(1, 0), Slot(5), 0),
            "a vote for another value never counts"
        );
        assert!(
            !rounds.fold_accepted(
                NodeId(1),
                ballot(2, 0),
                Slot(5),
                command_fingerprint(&cmd(1))
            ),
            "a vote at another ballot never counts"
        );
        assert!(rounds.fold_accepted(
            NodeId(1),
            ballot(1, 0),
            Slot(5),
            command_fingerprint(&cmd(1))
        ));
        assert_eq!(
            rounds.decided(Slot(5), &config),
            Some((ballot(1, 0), cmd(1)))
        );
        assert!(rounds.is_open_at(Slot(5), ballot(1, 0)));
        assert!(!rounds.is_open_at(Slot(5), ballot(2, 0)));
        assert!(!rounds.is_open_at(Slot(6), ballot(1, 0)));
        rounds.close(Slot(5));
        assert!(rounds.is_empty());
        assert_eq!(rounds.column(Slot(5)), None);
    }

    /// The re-send cursor is the tally's own: a page walks from the cursor
    /// up, wraps around, and `clear` resets it with the rounds.
    #[test]
    fn the_resend_page_is_fair_and_dies_with_the_tally() {
        let mut rounds: Rounds<NodeId, Command> = Rounds::new();
        for slot in 0..3 {
            rounds.open(Slot(slot), ballot(1, 0), cmd(slot), None, None);
        }
        let first = rounds.resend_page();
        assert_eq!(
            first.iter().map(|p| p.slot).collect::<Vec<_>>(),
            vec![Slot(0), Slot(1), Slot(2)]
        );
        assert_eq!(first[0].column, None);
        rounds.retain_from(Slot(1));
        assert_eq!(rounds.by_slot().len(), 2);
        rounds.clear();
        assert!(rounds.is_empty());
        assert!(rounds.resend_page().is_empty());
    }

    /// A delegated round is remembered, never counted and never decided
    /// here; its re-send is a counted re-delegation, and a take-back turns
    /// it into an ordinary colocated round seeded with the opener's own vote.
    #[test]
    fn a_delegated_round_is_the_proxys_until_taken_back() {
        let config = AcceptorConfig::new(
            vec![NodeId(0), NodeId(1), NodeId(2)],
            QuorumSystem::Majority,
        );
        let mut rounds: Rounds<NodeId, Command> = Rounds::new();
        rounds.open_delegated(Slot(3), ballot(1, 0), cmd(3), None, ProxyId(1));
        let round = rounds.by_slot().get(&Slot(3)).expect("open");
        assert_eq!(round.proxy(), Some(ProxyId(1)));
        assert!(round.accepted_by().is_none());
        assert!(
            !rounds.fold_accepted(
                NodeId(1),
                ballot(1, 0),
                Slot(3),
                command_fingerprint(&cmd(3))
            ),
            "a delegated round counts no vote here"
        );
        assert!(rounds.decided(Slot(3), &config).is_none());
        assert!(rounds.is_open_at(Slot(3), ballot(1, 0)));
        assert!(rounds.stalled_delegations(1).is_empty());
        let page = rounds.resend_page();
        assert_eq!(page[0].proxy, Some(ProxyId(1)));
        assert_eq!(rounds.stalled_delegations(1), vec![Slot(3)]);
        assert!(rounds.stalled_delegations(2).is_empty());
        assert!(rounds.take_back(Slot(3), Some(NodeId(0))));
        assert!(
            !rounds.take_back(Slot(3), None),
            "a colocated round is not taken back"
        );
        assert!(rounds.stalled_delegations(0).is_empty());
        assert!(rounds.fold_accepted(
            NodeId(1),
            ballot(1, 0),
            Slot(3),
            command_fingerprint(&cmd(3))
        ));
        assert_eq!(
            rounds.decided(Slot(3), &config),
            Some((ballot(1, 0), cmd(3))),
            "the opener's own vote plus one is the majority"
        );
        assert_eq!(rounds.resend_page()[0].proxy, None);
    }
}
