//! The audit's view of a correlated outage's losses (#263,
//! `crate::world::outage`): which copies of a decided slot the journal said
//! were gone at their boots, and what the protocol must do about them.
//!
//! The storage ledger plans the loss; the audit learns each copy's loss from
//! the journal's own verdict at open ([`AuditState::note_copy_lost`]), and a
//! repaired copy from the durable accept that rewrites it. Once every planned
//! loss landed, the audit **recognizes** the shape the losses left, each a
//! `reach_once!`:
//!
//! - the slot's clean copies: none, exactly one, fewer than a quorum (CTRL's
//!   E1 family);
//! - every holder faulty while some member holds nothing: the bare-quorum
//!   tally `faulty, faulty, none`;
//! - the only clean copy on a node outside the configuration in force: the
//!   departed straggler.
//!
//! It then judges the outcome against the CTRL decision rule, derived here
//! independently of `paros-core` (`qualifying answers` over the decided
//! ballot's configuration): a slot is **recoverable** while a Phase-1
//! quorum of its members could qualify (a `none`, a clean copy, or a lost
//! one at a ballot no higher than the best clean copy). An unrecoverable
//! slot must be waited on: it is **never accepted again**, by anyone (a
//! no-op fill or a fabricated value would both be an accept). Convergence
//! excuses a journal holding one; a recoverable slot is still owed it.
//!
//! **Proved by mutation** (#263, 3,000-seed hunts each, every witness green
//! unmutated): letting a sub-quorum count of qualifying answers decide a
//! faulty slot (`slot_decidable` asking for any qualifying answer instead of
//! a Phase-1 quorum) goes red on 12 seeds, and an acceptor that reports its
//! faulty entries as nothing (`promise_page` dropping them) on 8. Both trip
//! this module's "an unrecoverable slot is never accepted again" and the
//! protocol's own value oracles ("a durable accept quorum never decides two
//! values for a slot"); witnesses 11861972872444187227 and
//! 7412779604769813589.
//!
//! **Not yet proved**: consulting only the newest prior configuration stays
//! green over 3,000 seeds, because the departed-straggler shape it needs (a
//! removed node holding the only clean copy) is never reached. A
//! reconfiguration lands only after the cluster's first decisions, which
//! come after the chaos window in which an outage may start.

use std::collections::{BTreeMap, BTreeSet};

use moonpool_sim::{assert_always, assert_reachable, assert_sometimes};
use paros::{Ballot, NodeId};

use super::state::AuditState;

/// One outage-planned loss of a slot (see the module doc).
#[derive(Debug, Default)]
struct Planned {
    /// Every acceptor holding the slot when the loss was planned.
    holders: BTreeSet<u64>,
    /// The holders whose copy the plan damages at their next boot.
    damaged: BTreeSet<u64>,
    /// The copies the journal reported lost and no accept has rewritten.
    lost: BTreeSet<u64>,
    /// The shape the losses left, once every planned loss landed.
    shape: Option<Shape>,
}

/// The shape a slot's landed losses left (see the module doc).
#[derive(Clone, Copy, Debug, Default)]
struct Shape {
    /// One clean copy was left: its recovery is owed.
    one_copy: bool,
    /// The one clean copy is on a node outside the configuration in force.
    straggler: bool,
    /// Every holder lost it while some member holds nothing.
    bare: bool,
}

/// The losses of one journal (see the module doc), a field of
/// [`AuditState`]. Its bools are a flag set, one sticky bit per gate, as
/// [`AuditState`]'s are.
#[derive(Debug, Default)]
#[allow(clippy::struct_excessive_bools)]
pub(super) struct Losses {
    planned: BTreeMap<u64, Planned>,
    /// Slots the CTRL rule says no leader can decide again: waited on, never
    /// accepted.
    unrecoverable: BTreeSet<u64>,
    no_clean_copy: bool,
    one_clean_copy: bool,
    below_quorum: bool,
    bare_seen: bool,
    straggler_seen: bool,
    one_copy_recovered: bool,
    straggler_recovered: bool,
}

impl Losses {
    /// Whether the journal holds a slot no leader can decide again: its
    /// convergence is excused.
    pub(super) fn has_unrecoverable(&self) -> bool {
        !self.unrecoverable.is_empty()
    }
}

impl AuditState {
    /// An outage planned to lose `slot` on `damaged` of its `holders`.
    pub(super) fn note_outage_loss(&mut self, slot: u64, holders: &[u64], damaged: &[u64]) {
        assert_always!(
            damaged.iter().all(|node| holders.contains(node)),
            "storage: an outage damages only holders of the slot",
            { "slot" => slot }
        );
        let planned = self.losses.planned.entry(slot).or_default();
        planned.holders.extend(holders.iter().copied());
        planned.damaged.extend(damaged.iter().copied());
    }

    /// The journal reported `node`'s copy of `slot` lost at its boot, as an
    /// outage planned.
    pub(super) fn note_copy_lost(&mut self, node: u64, slot: u64) {
        let Some(planned) = self.losses.planned.get_mut(&slot) else {
            assert_always!(
                false,
                "storage: a lost copy was planned by an outage",
                { "node" => node, "slot" => slot }
            );
            return;
        };
        planned.lost.insert(node);
        let down = self.down_for_good();
        let Some(planned) = self.losses.planned.get_mut(&slot) else {
            return;
        };
        // Every planned loss landed, or its holder is down for good (a
        // wipe takes the copy with the whole disk).
        if planned.shape.is_none()
            && planned
                .damaged
                .iter()
                .all(|node| planned.lost.contains(node) || down.contains(node))
        {
            planned.shape = Some(Shape::default());
            self.recognize_loss(slot);
        }
        self.judge_loss(slot);
    }

    /// Judge every planned loss again (a boot settled what it held).
    pub(super) fn reevaluate_losses(&mut self) {
        let slots: Vec<u64> = self.losses.planned.keys().copied().collect();
        for slot in slots {
            self.judge_loss(slot);
        }
    }

    /// Name `slot` unrecoverable once the CTRL rule says so (sticky: no
    /// clean copy comes back).
    fn judge_loss(&mut self, slot: u64) {
        if !self.loss_recoverable(slot) && self.losses.unrecoverable.insert(slot) {
            assert_reachable!("storage: a decided slot becomes unrecoverable");
        }
    }

    /// A durable accept of `slot` on `node`, carrying `vhash`: the
    /// unrecoverable slot's claim, and a lost copy rewritten.
    pub(super) fn loss_accepted(&mut self, node: u64, slot: u64, vhash: u64) {
        if self.losses.planned.is_empty() {
            return;
        }
        assert_always!(
            !self.losses.unrecoverable.contains(&slot),
            "storage: an unrecoverable slot is never accepted again",
            { "node" => node, "slot" => slot }
        );
        let decided = self.decided.get(&slot).map(|(_, _, decided)| *decided);
        let Some(planned) = self.losses.planned.get_mut(&slot) else {
            return;
        };
        if !planned.lost.remove(&node) || decided != Some(vhash) {
            return;
        }
        let shape = planned.shape.unwrap_or_default();
        if shape.one_copy {
            self.losses.one_copy_recovered = true;
        }
        if shape.straggler {
            self.losses.straggler_recovered = true;
        }
    }

    /// The outcome gates of the losses, once per run.
    pub(super) fn check_loss_gates(&self) {
        let losses = &self.losses;
        assert_sometimes!(
            losses.one_copy_recovered,
            "storage: a slot with one clean copy left recovers intact"
        );
        assert_sometimes!(
            losses.has_unrecoverable(),
            "storage: an unrecoverable slot is waited on to the end of the run"
        );
        assert_sometimes!(
            losses.planned.iter().any(|(slot, planned)| {
                planned.shape.is_some_and(|shape| shape.bare) && losses.unrecoverable.contains(slot)
            }),
            "storage: a bare-quorum tally refuses its no-op fill"
        );
        assert_sometimes!(
            losses.straggler_recovered,
            "storage: a departed straggler's slot is recovered through the prior configuration"
        );
    }

    /// The nodes down for good: they answer no Phase 1.
    fn down_for_good(&self) -> BTreeSet<u64> {
        self.storage_dead
            .iter()
            .chain(&self.wiped)
            .chain(&self.retired)
            .copied()
            .collect()
    }

    /// Every node whose disk may hold `slot`: the plan's holders, every
    /// node a durable accept of it was reported from, and every node with a
    /// commit carrying it in flight (a cut commit may land unreported, #264).
    /// The custody ledger the plan read misses a commit whose sync was cut,
    /// so the claims below never rest on it alone.
    fn holders_of(&self, slot: u64, planned: &Planned) -> BTreeSet<u64> {
        let mut holders = planned.holders.clone();
        for (_, nodes) in self
            .accept_sets
            .range((slot, 0, 0)..=(slot, u64::MAX, u64::MAX))
        {
            holders.extend(nodes.iter().copied());
        }
        holders.extend(
            self.in_flight
                .keys()
                .filter(|(_, at)| *at == slot)
                .map(|(node, _)| *node),
        );
        holders
    }

    /// `node`'s highest durable accept ballot at `slot`, if the tally still
    /// holds it.
    fn accept_ballot(&self, node: u64, slot: u64) -> Option<(u64, u64)> {
        self.accept_sets
            .range((slot, 0, 0)..=(slot, u64::MAX, u64::MAX))
            .filter(|(_, holders)| holders.contains(&node))
            .map(|((_, round, by), _)| (*round, *by))
            .max()
    }

    /// The CTRL rule over the decided ballot's configuration (see the
    /// module doc): an undecided slot, or one whose tally was pruned, is
    /// never claimed unrecoverable.
    fn loss_recoverable(&self, slot: u64) -> bool {
        let (Some(&(round, by, _)), Some(planned)) =
            (self.decided.get(&slot), self.losses.planned.get(&slot))
        else {
            return true;
        };
        let Some(config) = self.config_of(Ballot {
            round,
            node: NodeId(by),
        }) else {
            return true;
        };
        let down = self.down_for_good();
        let clean: Vec<u64> = self
            .holders_of(slot, planned)
            .into_iter()
            .filter(|node| !planned.lost.contains(node) && !down.contains(node))
            .collect();
        // A clean holder whose ballot the tally never heard (a commit in
        // flight that landed unreported, #264) may hold the best copy: no
        // claim rests on a ballot the audit does not know.
        let ballots: Vec<Option<(u64, u64)>> = clean
            .iter()
            .map(|node| self.accept_ballot(*node, slot))
            .collect();
        if ballots.iter().any(Option::is_none) {
            return true;
        }
        let threshold = ballots.into_iter().flatten().max();
        let qualifying: BTreeSet<NodeId> = config
            .members()
            .iter()
            .filter(|member| {
                let node = member.0;
                !down.contains(&node)
                    && (!planned.lost.contains(&node)
                        || self
                            .accept_ballot(node, slot)
                            .is_some_and(|ballot| Some(ballot) <= threshold))
            })
            .copied()
            .collect();
        config.has_phase1_quorum(&qualifying)
    }

    /// Recognize the shape the landed losses of `slot` left (see the module
    /// doc).
    fn recognize_loss(&mut self, slot: u64) {
        let Some(&(round, by, _)) = self.decided.get(&slot) else {
            return;
        };
        let Some(config) = self
            .config_of(Ballot {
                round,
                node: NodeId(by),
            })
            .cloned()
        else {
            return;
        };
        let in_force = self
            .leader_round
            .iter()
            .map(|(node, round)| (*round, *node))
            .max()
            .and_then(|(round, node)| {
                self.config_of(Ballot {
                    round,
                    node: NodeId(node),
                })
                .cloned()
            });
        let down = self.down_for_good();
        let Some(planned) = self.losses.planned.get(&slot) else {
            return;
        };
        let holders = self.holders_of(slot, planned);
        let clean: BTreeSet<NodeId> = holders
            .iter()
            .filter(|node| !planned.lost.contains(node) && !down.contains(node))
            .map(|node| NodeId(*node))
            .collect();
        let none_answer = config
            .members()
            .iter()
            .any(|member| !holders.contains(&member.0) && !down.contains(&member.0));
        let straggler = clean.len() == 1
            && in_force
                .as_ref()
                .is_some_and(|members| clean.iter().all(|node| !members.members().contains(node)));
        let bare = clean.is_empty() && none_answer;
        let below_quorum = !config.has_phase1_quorum(&clean);
        let losses = &mut self.losses;
        if clean.is_empty() {
            reach_once!(
                losses.no_clean_copy,
                "storage: a decided slot loses its last clean copy"
            );
        }
        if clean.len() == 1 {
            reach_once!(
                losses.one_clean_copy,
                "storage: a decided slot is left one clean copy"
            );
        }
        if below_quorum && !clean.is_empty() {
            reach_once!(
                losses.below_quorum,
                "storage: a decided slot is left fewer clean copies than a quorum"
            );
        }
        if bare {
            reach_once!(
                losses.bare_seen,
                "storage: a slot's Phase-1 tally is faulty, faulty, none"
            );
        }
        if straggler {
            reach_once!(
                losses.straggler_seen,
                "storage: a slot's only clean copy is on a node outside the configuration in force"
            );
        }
        if let Some(planned) = losses.planned.get_mut(&slot) {
            planned.shape = Some(Shape {
                one_copy: clean.len() == 1,
                straggler,
                bare,
            });
        }
    }
}
