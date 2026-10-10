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
//! one at a ballot no higher than the best clean copy). Liveness is also
//! excused while a configuration the latest campaign asked has no
//! qualifying quorum, the core's rule over every configuration in `H_b`. An unrecoverable
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
//! **Proved by mutation, the departed straggler** (#267): consulting only
//! the newest prior configuration (`slot_decidable` checking `prior.last()`
//! instead of every configuration in `H_b`) goes red in the sweep (3 of 157
//! seeds, 13 exploration bugs) and on 36 of 1,200 hunt seeds, on "a durable
//! accept quorum never decides two values for a slot" and "an accept at or
//! above a decided ballot carries the decided value" (witness
//! 9147512841470386050); it stayed green over 2,000 seeds before.
//! The members' durable chosen indexes used to cover the slot, so each
//! repaired its faulty record by catch-up, and the successor held it too,
//! so the newest configuration alone never had a quorum of `none` answers.
//! The scenario now rotates the set onto spares, aims the loss at a slot
//! most of the successor never held, and keeps the one clean holder down
//! last: a leader of the successor then sees its own configuration's
//! quorum answer `none` while the prior one's cannot qualify without the
//! straggler, and only the cross-configuration rule makes it wait.

use std::collections::{BTreeMap, BTreeSet};

use moonpool_sim::{assert_always, assert_reachable, assert_sometimes};
use paros::{AcceptorConfig, Ballot, NodeId};

use super::state::AuditState;

/// The CTRL threshold of a lost slot's clean copies.
enum Threshold {
    /// The best clean copy's ballot (`None`: no clean copy).
    Best(Option<(u64, u64)>),
    /// A clean holder's ballot the tally never heard.
    Unheard,
}

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
    /// Whether the journal's liveness is excused by an outage's losses: a
    /// slot no leader can decide again, or a lost slot whose every clean
    /// copy is out of reach.
    pub(super) fn loss_excuses_liveness(&self) -> bool {
        self.losses.has_unrecoverable()
            || self.losses.planned.keys().any(|slot| self.stranded(*slot))
            || self
                .losses
                .planned
                .keys()
                .any(|slot| self.blocked_in_asked(*slot))
    }

    /// Whether the decided `slot` an outage hit is frozen because some
    /// configuration the latest campaign asked ([`Self::asked_configurations`])
    /// has no qualifying Phase-1 quorum for it. The core's CTRL rule (#267)
    /// needs a qualifying quorum of **every** configuration in `H_b`, while
    /// [`Self::loss_recoverable`] judges only the decided ballot's: a
    /// re-proposal into a successor configuration, itself lost above the
    /// best clean copy, with another successor member down for good, blocks
    /// every repair although the decided configuration still qualifies
    /// (witness 4265196232306395188: slot 0 decided at `(3, 2)` in
    /// `{0, 1, 2}`, re-proposed at `(5, 4)` into `{2, 3, 4}` and lost on node
    /// 4, node 2 wiped; red on "every quorum-decided slot is applied by the
    /// end of the tail", green with this excuse). Like
    /// [`Self::stranded`], it excuses liveness only and is judged afresh each
    /// time it is asked.
    fn blocked_in_asked(&self, slot: u64) -> bool {
        let (Some(planned), true) = (
            self.losses.planned.get(&slot),
            self.decided.contains_key(&slot),
        ) else {
            return false;
        };
        let down = self.down_for_good();
        let clean: Vec<u64> = self
            .holders_of(slot, planned)
            .into_iter()
            .filter(|node| !planned.lost.contains(node) && !down.contains(node))
            .collect();
        // The escapes `loss_recoverable` takes: a clean holder serving the
        // slot from its chosen prefix, or a ballot the tally never heard.
        if clean.iter().any(|node| {
            self.decided_prefix
                .get(node)
                .is_some_and(|prefix| *prefix > slot)
        }) {
            return false;
        }
        let Threshold::Best(threshold) = self.clean_threshold(slot, &clean) else {
            return false;
        };
        self.asked_configurations().iter().any(|config| {
            !config.has_phase1_quorum(&self.qualifying(config, slot, planned, &down, threshold))
        })
    }

    /// Whether the decided `slot` an outage hit is frozen because its every
    /// clean copy is out of reach: each holder is a member of no
    /// configuration the latest campaign asked
    /// ([`Self::asked_configurations`]), so no Phase 1 asks it, and its
    /// chosen prefix does not cover the slot, so no catch-up is served from
    /// it. Without them the CTRL rule has no clean copy to qualify a lost one
    /// against, so only the reachable members holding nothing qualify, and
    /// they are short of a Phase-1 quorum of the decided ballot's
    /// configuration. The
    /// slot then freezes its journal exactly as an unrecoverable one does.
    /// This excuses
    /// liveness only: it is judged afresh each time it is asked (a later
    /// configuration naming the holder brings its copy back in reach), and
    /// it is never the unrecoverable claim, whose safety oracle a copy
    /// brought back in reach would trip (witness 1980540850679778313: slot
    /// 8 lost on every member of `{0, 1, 2}` and on node 4, the one clean
    /// copy on node 3, a spare no configuration ever named).
    fn stranded(&self, slot: u64) -> bool {
        let (Some(planned), Some(&(round, by, _))) =
            (self.losses.planned.get(&slot), self.decided.get(&slot))
        else {
            return false;
        };
        let Some(config) = self.config_of(Ballot {
            round,
            node: NodeId(by),
        }) else {
            return false;
        };
        let down = self.down_for_good();
        let asked = self.asked_configurations();
        let reachable = |node: u64| {
            asked
                .iter()
                .any(|config| config.members().contains(&NodeId(node)))
        };
        let holders = self.holders_of(slot, planned);
        let clean: Vec<u64> = holders
            .iter()
            .copied()
            .filter(|node| !planned.lost.contains(node) && !down.contains(node))
            .collect();
        let out_of_reach = !clean.is_empty()
            && clean.iter().all(|node| {
                !reachable(*node)
                    && self
                        .decided_prefix
                        .get(node)
                        .is_none_or(|prefix| *prefix <= slot)
            });
        if !out_of_reach {
            return false;
        }
        let holding_nothing: BTreeSet<NodeId> = config
            .members()
            .iter()
            .filter(|member| {
                !down.contains(&member.0) && !holders.contains(&member.0) && reachable(member.0)
            })
            .copied()
            .collect();
        !config.has_phase1_quorum(&holding_nothing)
    }

    /// The configurations the latest campaign's Phase 1 asked: its own and
    /// the prior ones its matchmaking closed with (`H_b`), the reach of any
    /// leader to come short of a reconfiguration naming more. A
    /// configuration GC forgot is outside it (witness 1980540850679778313,
    /// once aimed: the clean copy on node 1, removed from `{0, 1, 2}`, whose
    /// history the effective GC floor had dropped). With no matchmaking
    /// (plain Multi-Paxos), every configuration bound to a ballot.
    fn asked_configurations(&self) -> Vec<&AcceptorConfig> {
        let latest = self
            .prior
            .iter()
            .filter(|((owner, _, by), _)| owner == by)
            .max_by_key(|((_, round, by), _)| (*round, *by));
        match latest {
            Some(((_, round, by), prior)) => self
                .config_of(Ballot {
                    round: *round,
                    node: NodeId(*by),
                })
                .into_iter()
                .chain(prior)
                .collect(),
            None => self.bootstrap.iter().chain(self.configs.values()).collect(),
        }
    }

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
        if !self.losses.planned.contains_key(&slot) {
            assert_always!(
                false,
                "storage: a lost copy was planned by an outage",
                { "node" => node, "slot" => slot }
            );
            return;
        }
        self.land_loss(node, slot);
    }

    /// The journal reported `node`'s entries at `faulty` slots at its open,
    /// whatever damaged them. A faulty copy of a slot an outage planned to
    /// lose is lost all the same: the CTRL rule reads the answer, not its
    /// cause (witness 6839930092088337217: node 0's copy of slot 0 rotted
    /// at an earlier boot, the outage then took the other two, and the
    /// audit, hearing only of the outage's losses, owed the frozen slot a
    /// recovery no leader could make). A learner's copy is never one.
    pub(super) fn note_faulty_copies(&mut self, node: u64, faulty: &[u64]) {
        if self.replicas.contains(&node) {
            return;
        }
        for slot in faulty {
            if self
                .losses
                .planned
                .get(slot)
                .is_some_and(|planned| !planned.lost.contains(&node))
            {
                self.land_loss(node, *slot);
            }
        }
    }

    /// `node`'s copy of the planned `slot` is lost: recognize the shape once
    /// every planned loss landed, and judge the slot.
    fn land_loss(&mut self, node: u64, slot: u64) {
        let Some(planned) = self.losses.planned.get_mut(&slot) else {
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
        // A lost copy rewritten is a lost copy no longer.
        planned.lost.remove(&node);
        // A recovery is the decided value accepted again once the shape was
        // recognized, on any node: a leader's re-proposal reaches the
        // configuration in force, whose members may never have held the
        // slot, so a recovery is not only a lost copy rewritten.
        let Some(shape) = planned.shape else {
            return;
        };
        if decided != Some(vhash) {
            return;
        }
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
    /// so the claims below never rest on it alone. A replica's commits are
    /// in flight too, but a learner holds no vote and answers no Phase 1:
    /// its copy is never one CTRL can recover from, and since no durable
    /// accept is ever reported from it, its in-flight mark would stand for
    /// good (witness 10778422868787336488: every acceptor's copy of slot 0
    /// lost, the replica's mark kept the slot "recoverable" and the frozen
    /// journal unexcused).
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
                .filter(|(node, at)| *at == slot && !self.replicas.contains(node))
                .map(|(node, _)| *node),
        );
        holders
    }

    /// The ballot `node`'s lost copy of `slot` answers Phase 1 with: the
    /// identity its boot reported the faulty entry under, or, before any
    /// boot reported one, its highest durable accept the tally heard.
    fn lost_ballot(&self, node: u64, slot: u64) -> Option<(u64, u64)> {
        self.faulty_ballots
            .get(&(node, slot))
            .copied()
            .or_else(|| self.accept_ballot(node, slot))
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

    /// A boot of `node` recovered a clean record of `slot`: a lost copy
    /// rewritten (a commit that landed unreported) is a lost copy no
    /// longer, as in [`AuditState::loss_accepted`]'s live report. Without
    /// it, the claim moved to the rewritten holder's next boot, whose
    /// in-flight commits were already taken.
    pub(super) fn loss_rewritten_at_boot(&mut self, node: u64, slot: u64) {
        if let Some(planned) = self.losses.planned.get_mut(&slot) {
            planned.lost.remove(&node);
        }
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
        // A lost holder with a commit of the slot in flight may have
        // rewritten its copy durably, unreported (#264): no claim rests on
        // it (hunt seed 16212343262911210763: a leader's re-proposal of the
        // decided value landed unreported on a lost holder, which booted
        // with a clean copy at the higher ballot, and a later leader's
        // accepts of the decided value tripped the claim).
        if planned
            .lost
            .iter()
            .any(|node| !down.contains(node) && self.in_flight.contains_key(&(*node, slot)))
        {
            return true;
        }
        let clean: Vec<u64> = self
            .holders_of(slot, planned)
            .into_iter()
            .filter(|node| !planned.lost.contains(node) && !down.contains(node))
            .collect();
        // A clean holder that durably knows the slot decided (its chosen
        // index covers it) serves the record from its chosen prefix to any
        // peer whose own faulty chosen record left a hole (catch-up), and
        // re-replicates it when it leads — no tally needed (witness
        // 4059871466191551614: the clean copy at a ballot below the two
        // lost ones, a Phase-1 tally no leader could decide, the record
        // repaired from the clean holder's chosen prefix all the same).
        if clean.iter().any(|node| {
            self.decided_prefix
                .get(node)
                .is_some_and(|prefix| *prefix > slot)
        }) {
            return true;
        }
        // A clean holder whose ballot the tally never heard (a commit in
        // flight that landed unreported, #264) may hold the best copy: no
        // claim rests on a ballot the audit does not know.
        let Threshold::Best(threshold) = self.clean_threshold(slot, &clean) else {
            return true;
        };
        config.has_phase1_quorum(&self.qualifying(config, slot, planned, &down, threshold))
    }

    /// The best ballot among `slot`'s `clean` copies, the CTRL threshold a
    /// lost copy qualifies under, or [`Threshold::Unheard`] when a clean
    /// holder's ballot is one the tally never heard, or one a commit in
    /// flight may have raised unreported (#264): no claim rests on either.
    fn clean_threshold(&self, slot: u64, clean: &[u64]) -> Threshold {
        let ballots: Vec<Option<(u64, u64)>> = clean
            .iter()
            .map(|node| self.accept_ballot(*node, slot))
            .collect();
        // A clean holder with a commit of the slot in flight may have
        // landed a higher ballot unreported: its heard ballot is not its
        // durable one (witness: seed 17867571935699901919, a claim made at
        // the clean holder's old ballot while its re-accept was in flight).
        if ballots.iter().any(Option::is_none)
            || clean
                .iter()
                .any(|node| self.in_flight.contains_key(&(*node, slot)))
        {
            return Threshold::Unheard;
        }
        Threshold::Best(ballots.into_iter().flatten().max())
    }

    /// The members of `config` whose Phase-1 answer for `slot` qualifies
    /// under the CTRL rule: not down for good, and holding nothing, a clean
    /// copy, or a lost one at a ballot no higher than `threshold`.
    fn qualifying(
        &self,
        config: &AcceptorConfig,
        slot: u64,
        planned: &Planned,
        down: &BTreeSet<u64>,
        threshold: Option<(u64, u64)>,
    ) -> BTreeSet<NodeId> {
        config
            .members()
            .iter()
            .filter(|member| {
                let node = member.0;
                !down.contains(&node)
                    && (!planned.lost.contains(&node)
                        || self
                            .lost_ballot(node, slot)
                            .is_some_and(|ballot| Some(ballot) <= threshold))
            })
            .copied()
            .collect()
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
        // The configuration bound to the highest ballot the audit knows. A
        // leader's ballot with no configuration of its own falls back to the
        // bootstrap one in `config_of`, which still names every member the
        // operator removed, so the departed straggler was never recognized
        // (0 of 1,000 seeds, against 12% that planned its loss).
        let in_force = self
            .configs
            .values()
            .next_back()
            .or(self.bootstrap.as_ref())
            .cloned();
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
        // The departed straggler: the one clean copy is on a member of the
        // configuration the slot was decided under that the configuration
        // in force no longer names. A spare that learned the slot (never a
        // member) is outside too, but its copy is a learner's: CTRL's
        // cross-configuration Phase 1 never asks it.
        let straggler = clean.len() == 1
            && clean.iter().all(|node| config.members().contains(node))
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
