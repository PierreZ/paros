//! The **matchmaker reconfigurer** (#125): the sans-IO state machine that
//! replaces generation `g`'s matchmaker set by a chosen successor, driven by
//! the node driver (`paros::run_node`) over the matchmaker RPC contract.
//!
//! ```text
//! Idle
//!   | start(current = M_g, target)
//!   v
//! Stopping        Stop -> every member of M_g; wait for a quorum of StopB
//!   |             (a StopB naming a successor means someone already won: adopt)
//!   v
//! Bootstrapping   reconstruct (w = max watermark, H = union of histories >= w),
//!   |             Bootstrap{g+1, target, w, H} -> every member of the target;
//!   |             wait for ALL of them (a set is chosen only once fully initialized)
//!   v
//! Deciding        single-decree Paxos over M_g as acceptors (`Decree`: the shared
//!   |             `Proposer` + `Acceptor` roles over a one-slot log, a majority of M_g
//!   |             for both phases — the only quorum model paros
//!   |             supports for matchmakers): Phase 1 at a fresh ballot strictly above
//!   |             the stop quorum's decree promises (a rebooted node must never reuse
//!   |             a ballot of its earlier incarnation), P2c adopts a competing proposal
//!   |             already voted, Phase 2 with the selected value; a Nack reopens
//!   |             higher on the driver's next re-send
//!   v
//! Publishing      Chosen{g, successor} -> M_g ∪ successor; done once a quorum of
//!   |             each has learned it (re-sent to whoever has not answered; a member
//!   |             that already activated answers `Learned` again, so a lost ack is
//!   v             recovered; stragglers are told again by any node that meets them,
//! Idle            see `ColocatedNode::on_match_reply`)
//! ```
//!
//! Why this is safe: the frozen quorum's registries are immutable when
//! unioned, every completed registration of generation `g` reached a quorum
//! of `M_g` and so intersects the frozen quorum (Appendix B), and two
//! reconfigurers proposing incompatible successors are serialized by the
//! decree — the loser's Phase 1 sees the winner's vote and proposes it. The
//! successor metadata published is therefore always the *chosen* set, never
//! a proposal: `Chosen` is sent only from the `Publishing` phase, which is
//! entered only on a Phase-2 quorum.
//!
//! The reconfigurer holds no durable state. A crash loses the proposal, and
//! the next attempt (this node's or another's) re-runs the same steps: the
//! stop is idempotent, the bootstrap is keyed by the proposed set, and the
//! decree's votes are durable at the matchmakers, so the retry either adopts
//! what was already chosen or completes what was started. The one thing a
//! fresh incarnation must not do is reuse a decree ballot its predecessor
//! may have had a value accepted at: the `Stopped` replies carry each frozen
//! member's decree promise, and the decree opens strictly above their
//! maximum (the handover model checker, `crate::models::handover`, found the
//! reuse on its seed 103 — two values at one ballot).

use std::collections::{BTreeMap, BTreeSet};

use super::SuccessorDecree;
use super::{
    JournalRegistry, MatchmakerId, MatchmakerSet, PendingBootstrap, REGISTRY_PAGE,
    ReconfigureReply, ReconfigureRequest, raise_effective,
};
use crate::contract::limits::Limits;
use crate::decree::{AcceptFold, DecreePromise};
use crate::journal::JournalId;
use crate::membership::{AcceptorConfig, QuorumSystem};
use crate::types::{Ballot, NodeId};

/// Why [`MatchmakerReconfigurer::start`] refused a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartRefusal {
    /// A reconfiguration is already running here.
    Busy,
    /// The target names no matchmaker.
    Empty,
}

/// Where the reconfigurer stands.
#[derive(Clone, Debug)]
pub enum ReconfigurerPhase {
    /// Nothing running.
    Idle,
    /// Freezing the current generation.
    Stopping {
        /// The generation being replaced.
        old: MatchmakerSet,
        /// The proposed successor membership; `None` for a *finish*, which
        /// proposes the members that answered the freeze.
        target: Option<Vec<MatchmakerId>>,
        /// The frozen registries collected so far, every journal's per
        /// member (#190), from the members whose report is whole.
        acks: BTreeMap<MatchmakerId, BTreeMap<JournalId, JournalRegistry>>,
        /// The pages of a report still cut short (#439), per member: the
        /// journals so far and the cursor its next page starts after.
        partial: BTreeMap<MatchmakerId, (BTreeMap<JournalId, JournalRegistry>, JournalId)>,
        /// The highest decree ballot any frozen member reported promised:
        /// the decree opens strictly above it (see `decree_promised` on
        /// [`ReconfigureReply::Stopped`]).
        decree_floor: Ballot,
    },
    /// Handing the reconstruction to the proposed successor's members.
    Bootstrapping {
        /// The generation being replaced.
        old: MatchmakerSet,
        /// The reconstruction, addressed to every member of the proposed set.
        bootstrap: PendingBootstrap,
        /// Members that durably hold it whole.
        acks: BTreeSet<MatchmakerId>,
        /// The last journal each member reported it holds of a bootstrap
        /// still cut short (#439): its next page starts after it.
        held: BTreeMap<MatchmakerId, Option<JournalId>>,
        /// The stop quorum's decree floor, carried to the decree.
        decree_floor: Ballot,
    },
    /// Running the successor decree over the old generation.
    Deciding {
        /// The generation being replaced (the decree's acceptors).
        old: MatchmakerSet,
        /// The proposal this reconfigurer bootstrapped.
        bootstrap: PendingBootstrap,
        /// The single-decree proposal. Boxed: a decree carries the shared
        /// proposer role's whole tally surface, several times the size of
        /// any other phase's state.
        decree: Box<SuccessorDecree>,
    },
    /// Telling the old and new generations about the chosen successor.
    Publishing {
        /// The generation replaced.
        old: MatchmakerSet,
        /// The chosen successor.
        successor: MatchmakerSet,
        /// Old members that learned it.
        old_acks: BTreeSet<MatchmakerId>,
        /// New members that learned it.
        new_acks: BTreeSet<MatchmakerId>,
    },
}

/// What one reply did, returned by [`MatchmakerReconfigurer::on_reply`] so the
/// driver can report the transition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReconfigurerStep {
    /// Not for the running phase: nothing changed.
    Ignored,
    /// One more frozen registry; `remaining` more before the quorum.
    Stopped {
        /// Stop acks still needed.
        remaining: usize,
    },
    /// One more member holds the bootstrap; `remaining` more before all do.
    Bootstrapped {
        /// Members still missing.
        remaining: usize,
    },
    /// Every member holds the bootstrap: the decree opened at `ballot`.
    Deciding {
        /// The decree ballot.
        ballot: Ballot,
    },
    /// One more decree promise; `remaining` more before the Phase-1 quorum.
    Promised {
        /// Promises still missing.
        remaining: usize,
    },
    /// One more decree vote; `remaining` more before the successor is chosen.
    Accepted {
        /// Accepts still missing.
        remaining: usize,
    },
    /// One more member learned the chosen successor; `old_remaining` and
    /// `new_remaining` more before each set's quorum has.
    Published {
        /// Learners still missing in the replaced generation.
        old_remaining: usize,
        /// Learners still missing in the successor.
        new_remaining: usize,
    },
    /// A Phase-1 quorum holds: proposing `members` (P2c adopted a prior vote
    /// when `adopted`).
    Proposing {
        /// The decree ballot.
        ballot: Ballot,
        /// The proposed membership.
        members: Vec<MatchmakerId>,
        /// Whether a competing vote was adopted over the own proposal.
        adopted: bool,
    },
    /// A higher promise refused the decree at `ballot`; the next
    /// [`MatchmakerReconfigurer::resend`] reopens it above `promised` (the
    /// driver paces that retry).
    Preempted {
        /// The promise that refused.
        promised: Ballot,
        /// The fresh ballot.
        ballot: Ballot,
    },
    /// A Phase-2 quorum holds: `successor` is chosen and being published.
    Chosen {
        /// The chosen successor.
        successor: MatchmakerSet,
    },
    /// Publishing completed: back to idle.
    Done {
        /// The chosen successor.
        successor: MatchmakerSet,
    },
    /// The generation already has a chosen successor (learned from a frozen
    /// member, or from a member that moved on): aborted, and the driver
    /// should adopt `successor`.
    Superseded {
        /// The successor already chosen.
        successor: MatchmakerSet,
    },
}

/// One batch of a handover's outbound requests, and the compile-time gate
/// enforcing one batch in flight — the reconfigurer's [`crate::Ready`].
///
/// The guard holds the reconfigurer's unique borrow, so a second
/// [`MatchmakerReconfigurer::ready`] before [`ReconfigurerReady::advance`]
/// is a *compile* error, and a batch that is never advanced is a `#[must_use]`
/// warning rather than a handover that silently stalls. Before this the
/// queue was drained by a bare `mem::take`, which every other outbound
/// bucket in the crate had already stopped doing.
///
/// # Order
///
/// 1. Send [`ReconfigurerReady::requests`]. There is no durability step:
///    the reconfigurer holds no durable state of its own (a crash loses the
///    proposal and the next attempt re-runs the idempotent steps).
/// 2. Call [`ReconfigurerReady::advance`] to release the gate.
#[must_use = "a ReconfigurerReady must be processed and then advanced; dropping it silently skips a batch"]
pub struct ReconfigurerReady<'a> {
    reconfigurer: &'a mut MatchmakerReconfigurer,
}

impl ReconfigurerReady<'_> {
    /// The requests the running phase wants on the wire, in order.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn requests(&self) -> &[(MatchmakerId, ReconfigureRequest)] {
        let requests = &self.reconfigurer.pending;
        // Every request speaks for this reconfigurer, and a reconfigurer at
        // rest sends nothing.
        assert!(
            requests
                .iter()
                .all(|(_, r)| r.from() == self.reconfigurer.node),
            "a handover request names its own reconfigurer"
        );
        if !self.reconfigurer.is_busy() {
            assert!(requests.is_empty(), "an idle reconfigurer sends nothing");
        }
        requests
    }

    /// Acknowledge the batch: clears the queue and releases the unique
    /// borrow. Consumes `self` — the guard cannot be reused.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn advance(self) {
        self.reconfigurer.pending.clear();
        assert!(
            self.reconfigurer.pending.is_empty(),
            "an advanced batch has no request left"
        );
    }
}

/// Whether every journal's reconstructed history sits at or above its own
/// reconstructed watermark.
fn holds_nothing_below(registries: &BTreeMap<JournalId, JournalRegistry>) -> bool {
    registries.values().all(|registry| {
        registry
            .history
            .keys()
            .next()
            .is_none_or(|b| *b >= registry.gc_watermark)
    })
}

/// What [`MatchmakerReconfigurer::close_stop`] produced: the successor's
/// initial state, and how many ballots the frozen registries disagreed on
/// while it was unioned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reconstruction {
    /// The reconstruction handed to every proposed member.
    pub bootstrap: PendingBootstrap,
    /// Distinct ballots two frozen registries reported with *different*
    /// registrations. One ballot has one proposer and every matchmaker
    /// writes it once, so this is zero on a correct registry; the union
    /// keeps the first seen either way (it must pick one — the successor
    /// generation holds one record per ballot), and this counts what was
    /// dropped so the audit can judge the claim instead of the union
    /// silently narrowing the ledger, durably, forever.
    pub disagreements: u64,
}

/// The reconfigurer handle: one running handover at most, its outbound
/// requests drained by the driver through [`Self::ready`].
#[derive(Debug)]
pub struct MatchmakerReconfigurer {
    node: NodeId,
    /// The last decree round this node used (volatile; a fresh incarnation
    /// starts over, and the acceptors' promises push it up through Nacks).
    round: u64,
    phase: ReconfigurerPhase,
    pending: Vec<(MatchmakerId, ReconfigureRequest)>,
    /// Driver ticks since the running phase last made progress (see
    /// [`MatchmakerReconfigurer::stalled_for`]).
    elapsed: u64,
    /// The limits this reconfigurer runs under, fixed at construction
    /// ([`MatchmakerReconfigurer::new`]): the most journals one bootstrap
    /// page carries (#439), [`Limits::registry_page`].
    limits: Limits,
    /// The smallest page a frozen member cut its report at in this handover
    /// (#439): the bootstrap is paged no wider, so a successor meets pages
    /// no larger than the set's own members send.
    cut: Option<usize>,
}

impl MatchmakerReconfigurer {
    /// An idle reconfigurer for `node`, that pages its bootstraps by
    /// `limits.registry_page`.
    ///
    /// # Panics
    ///
    /// If `limits` breaks [`Limits::assert_valid`], or an assertion on its
    /// own invariants, preconditions or postconditions fails: a programmer
    /// error, never an operating condition.
    #[must_use]
    pub fn new(node: NodeId, limits: Limits) -> Self {
        limits.assert_valid();
        let reconfigurer = Self {
            node,
            round: 0,
            phase: ReconfigurerPhase::Idle,
            pending: Vec::new(),
            elapsed: 0,
            limits,
            cut: None,
        };
        assert!(!reconfigurer.is_busy(), "a fresh reconfigurer is idle");
        reconfigurer.assert_invariants();
        reconfigurer
    }

    /// The limits this reconfigurer runs under.
    #[must_use]
    pub fn limits(&self) -> Limits {
        self.limits
    }

    /// The handover's per-phase invariants: who may have answered, which
    /// generation each set is, and that an idle reconfigurer holds nothing.
    ///
    /// # Panics
    ///
    /// If a phase holds an ack from outside the set it tallies, a proposed
    /// successor is not the next generation, or an idle reconfigurer keeps
    /// work or a clock.
    pub fn assert_invariants(&self) {
        match &self.phase {
            ReconfigurerPhase::Idle => {
                assert!(
                    self.pending.is_empty(),
                    "an idle reconfigurer queues nothing"
                );
                assert!(self.elapsed == 0, "an idle reconfigurer's clock is stopped");
            }
            ReconfigurerPhase::Stopping {
                old,
                target,
                acks,
                partial,
                ..
            } => {
                assert!(
                    acks.keys().all(|m| old.contains(*m)),
                    "only members of the replaced generation acknowledge its freeze"
                );
                assert!(
                    partial
                        .keys()
                        .all(|m| old.contains(*m) && !acks.contains_key(m)),
                    "a report still cut short is a member's that is not whole"
                );
                assert!(
                    target.as_ref().is_none_or(|t| !t.is_empty()),
                    "a reconfiguration targets a non-empty set"
                );
            }
            ReconfigurerPhase::Bootstrapping {
                old,
                bootstrap,
                acks,
                held,
                ..
            } => {
                assert!(
                    bootstrap.complete,
                    "a bootstrap is paged out of a whole reconstruction"
                );
                assert!(
                    held.keys()
                        .all(|m| bootstrap.set.contains(*m) && !acks.contains(m)),
                    "a bootstrap still cut short is a member's that is not whole"
                );
                assert!(
                    bootstrap.set.generation == old.generation.next(),
                    "a bootstrap proposes the next generation"
                );
                assert!(
                    acks.iter().all(|m| bootstrap.set.contains(*m)),
                    "only members of the proposed set acknowledge its bootstrap"
                );
                assert!(
                    holds_nothing_below(&bootstrap.registries),
                    "a reconstruction holds nothing below its watermark"
                );
            }
            ReconfigurerPhase::Deciding {
                old,
                bootstrap,
                decree,
            } => {
                assert!(
                    bootstrap.set.generation == old.generation.next(),
                    "a decree decides the next generation"
                );
                assert!(
                    decree.ballot().node == self.node,
                    "a decree runs at this node's ballot"
                );
                assert!(
                    decree.ballot().round <= self.round,
                    "a decree's round was minted here"
                );
            }
            ReconfigurerPhase::Publishing {
                old,
                successor,
                old_acks,
                new_acks,
            } => {
                assert!(
                    successor.generation == old.generation.next(),
                    "a published successor is the next generation"
                );
                assert!(
                    old_acks.iter().all(|m| old.contains(*m)),
                    "only members of the replaced set count toward its quorum"
                );
                assert!(
                    new_acks.iter().all(|m| successor.contains(*m)),
                    "only members of the successor count toward its quorum"
                );
            }
        }
    }

    /// Where the reconfigurer stands.
    #[must_use]
    pub fn phase(&self) -> &ReconfigurerPhase {
        &self.phase
    }

    /// Whether a handover is running.
    #[must_use]
    pub fn is_busy(&self) -> bool {
        !matches!(self.phase, ReconfigurerPhase::Idle)
    }

    /// The generation being replaced, while a handover runs.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn old(&self) -> Option<&MatchmakerSet> {
        assert!(
            self.is_busy() != matches!(self.phase, ReconfigurerPhase::Idle),
            "busy is exactly not idle"
        );
        match &self.phase {
            ReconfigurerPhase::Idle => None,
            ReconfigurerPhase::Stopping { old, .. }
            | ReconfigurerPhase::Bootstrapping { old, .. }
            | ReconfigurerPhase::Deciding { old, .. }
            | ReconfigurerPhase::Publishing { old, .. } => Some(old),
        }
    }

    /// Drain the queued requests. Holds the unique borrow until
    /// [`ReconfigurerReady::advance`], so a second `ready()` before
    /// `advance()` is a compile error — the same gate
    /// [`crate::matchmaker::MatchmakerReady`] and [`crate::Ready`] put on their batches.
    pub fn ready(&mut self) -> ReconfigurerReady<'_> {
        ReconfigurerReady { reconfigurer: self }
    }

    /// Replace `current` (the set this node believes authoritative) by
    /// `target`: freeze `current` first. The target may equal the current
    /// membership — that is how a node finishes a handover whose reconfigurer
    /// died after freezing (the decree then adopts whatever was voted, or
    /// re-chooses the same members under a fresh generation).
    ///
    /// # Errors
    ///
    /// [`StartRefusal::Busy`] while a handover runs; [`StartRefusal::Empty`]
    /// for an empty target. Both sets — `current`, over which every freeze
    /// and decree quorum is drawn, and the target every later quorum is
    /// drawn from — admit the matchmaker quorum system by construction
    /// ([`MatchmakerSet::new`] is the only constructor and asserts it).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn start(
        &mut self,
        current: &MatchmakerSet,
        target: Vec<MatchmakerId>,
    ) -> Result<(), StartRefusal> {
        if self.is_busy() {
            return Err(StartRefusal::Busy);
        }
        if target.is_empty() {
            return Err(StartRefusal::Empty);
        }
        // Normalized through the one constructor, like every set.
        let target = MatchmakerSet::new(current.generation.next(), target)
            .members()
            .to_vec();
        assert!(!target.is_empty(), "a normalized target names a matchmaker");
        self.begin_stopping(current, Some(target));
        assert!(self.is_busy(), "a started handover is busy");
        Ok(())
    }

    /// Finish a handover whose reconfigurer died after freezing `current`:
    /// a frozen generation with no successor is a cluster that can elect no
    /// leader, so any node that meets one drives it to completion. The
    /// successor proposed is **the members that answer the freeze** — the
    /// only liveness this node can vouch for — so a member that died since
    /// (a lost registry, a machine gone) never blocks the bootstrap; the
    /// decree still adopts whatever an earlier reconfigurer got voted, and
    /// an operator can grow the set back afterwards.
    ///
    /// # Errors
    ///
    /// [`StartRefusal::Busy`] while a handover runs.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn finish(&mut self, current: &MatchmakerSet) -> Result<(), StartRefusal> {
        if self.is_busy() {
            return Err(StartRefusal::Busy);
        }
        self.begin_stopping(current, None);
        assert!(self.is_busy(), "a finishing handover is busy");
        assert!(
            self.old().is_some_and(|old| old == current),
            "a finish freezes the generation it was handed"
        );
        Ok(())
    }

    /// Enter the freeze of `current` toward `target` (`None`: the members
    /// that answer it) and send its first requests — the opening
    /// [`Self::start`] and [`Self::finish`] share.
    fn begin_stopping(&mut self, current: &MatchmakerSet, target: Option<Vec<MatchmakerId>>) {
        self.phase = ReconfigurerPhase::Stopping {
            old: current.clone(),
            target,
            acks: BTreeMap::new(),
            partial: BTreeMap::new(),
            decree_floor: Ballot::zero(),
        };
        self.elapsed = 0;
        self.cut = None;
        self.resend();
        assert!(
            matches!(self.phase, ReconfigurerPhase::Stopping { .. }),
            "a handover opens by freezing"
        );
        self.assert_invariants();
    }

    /// One driver tick while a handover runs: the running phase's stall
    /// clock advances. The core only *knows* that it has made no progress
    /// ([`Self::stalled_for`]); whether that is long enough to give up is
    /// the driver's decision ([`Self::abandon`]) — the timeout is policy,
    /// paced in the driver's own units, never a constant inside the state
    /// machine.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn tick(&mut self) {
        let elapsed = self.elapsed;
        if self.is_busy() {
            self.elapsed = self.elapsed.saturating_add(1);
        }
        assert!(self.elapsed >= elapsed, "a stall clock never runs backward");
        self.assert_invariants();
    }

    /// Driver ticks since the running phase last folded a reply that moved
    /// it (zero while idle).
    ///
    /// "Moved it" is exactly "the fold did not answer
    /// [`ReconfigurerStep::Ignored`]", and every phase counts the same way:
    /// an ack from a member that had already answered is a duplicate and
    /// changes nothing, while an ack that counts toward a quorum still
    /// short *is* progress and says how much is missing
    /// (`Stopped`/`Bootstrapped`/`Promised`/`Accepted`/`Published`). Both
    /// halves matter — a duplicate resetting the clock kept a dead phase
    /// alive forever, and a counted-but-short fold reported as `Ignored`
    /// had the driver abandon a decree that was progressing (review
    /// finding P4).
    #[must_use]
    pub fn stalled_for(&self) -> u64 {
        self.elapsed
    }

    /// Give up the running handover (back to `Idle`; `false` when nothing
    /// was running). A proposed member that will never answer its
    /// bootstrap, or an old member that will never vote, must not hold a
    /// `Busy` refusal forever — the next node to meet the frozen generation
    /// finishes it with the members that do answer. Abandoning is always
    /// safe: the reconfigurer holds no durable state, the freeze and the
    /// bootstrap are idempotent, and the decree's votes stay durable at the
    /// matchmakers.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn abandon(&mut self) -> bool {
        if !self.is_busy() {
            return false;
        }
        self.abort();
        assert!(!self.is_busy(), "an abandoned handover is idle");
        true
    }

    /// Whether the running freeze has the quorum it needs to be closed
    /// ([`Self::close_stop`]). `false` in every other phase.
    #[must_use]
    pub fn stop_quorum_reached(&self) -> bool {
        match &self.phase {
            ReconfigurerPhase::Stopping { old, acks, .. } => {
                old.has_quorum(&acks.keys().copied().collect())
            }
            _ => false,
        }
    }

    /// Close the freeze: reconstruct the successor's initial state from the
    /// members that answered so far and move to `Bootstrapping`, returning
    /// the reconstruction the driver must report. `None` when no freeze is
    /// running or the quorum is still short.
    ///
    /// **Closing is the driver's decision, not the quorum-completing ack's.**
    /// A quorum is the *floor* the reconstruction rests on; every further ack
    /// widens it, and — for a `finish`, whose proposal is "the members that
    /// answered" — widens the successor set itself. Closing on the ack that
    /// first reached the quorum made every finish propose exactly
    /// `quorum(M_g)`: a five-member set became three, then two, then one
    /// (review finding P5). Calling this from the same cadence that re-sends
    /// the `Stop` gives the stragglers one full round to arrive, and calling
    /// it late is always safe — the freeze is durable and idempotent.
    ///
    /// # Panics
    ///
    /// If the reconstruction breaks its own contract (a registration below
    /// the watermark it computed, a proposed successor that does not admit
    /// the matchmaker quorum system).
    pub fn close_stop(&mut self) -> Option<Reconstruction> {
        let ReconfigurerPhase::Stopping {
            old,
            target,
            acks,
            decree_floor,
            ..
        } = &mut self.phase
        else {
            return None;
        };
        if !old.has_quorum(&acks.keys().copied().collect()) {
            return None;
        }
        // The reconstruction (§5), per journal of the set (#190): the
        // maximum watermark, the union of every frozen registry at or above
        // it, and the maximum effective configuration. A ballot reported
        // twice carries one registration (the write-once ledger); the first
        // seen is kept.
        let mut registries: BTreeMap<JournalId, JournalRegistry> = BTreeMap::new();
        for frozen in acks.values() {
            for (journal, registry) in frozen {
                let held = registries.entry(*journal).or_default();
                held.gc_watermark = held.gc_watermark.max(registry.gc_watermark);
                // The effective configuration is a monotone scalar, not a
                // record: the maximum over the frozen members carries the
                // acceptor set in force into the successor generation even
                // when its own registration was collected long ago.
                if let Some((ballot, config)) = &registry.effective {
                    raise_effective(&mut held.effective, *ballot, config);
                }
            }
        }
        let mut disagreements: u64 = 0;
        for frozen in acks.values() {
            for (journal, registry) in frozen {
                let held = registries
                    .get_mut(journal)
                    .expect("every reported journal is held");
                let gc_watermark = held.gc_watermark;
                for (ballot, registration) in registry.history.range(gc_watermark..) {
                    match held.history.entry(*ballot) {
                        std::collections::btree_map::Entry::Vacant(slot) => {
                            slot.insert(registration.clone());
                        }
                        std::collections::btree_map::Entry::Occupied(slot) => {
                            if slot.get() != registration {
                                // Two frozen registries hold one ballot with
                                // different bytes. The successor generation
                                // can only carry one, so the first seen
                                // stays — but that *durably* narrows the
                                // ledger, and the leader's own union keeps
                                // both, so the count is reported rather than
                                // dropped (review finding S2c).
                                disagreements = disagreements.saturating_add(1);
                            }
                        }
                    }
                }
            }
        }
        registries.retain(|_, registry| !registry.is_blank());
        // A finish proposes every member that answered the freeze.
        let target = target
            .clone()
            .unwrap_or_else(|| acks.keys().copied().collect());
        let bootstrap = PendingBootstrap {
            set: MatchmakerSet::new(old.generation.next(), target),
            registries,
            complete: true,
        };
        assert!(
            holds_nothing_below(&bootstrap.registries),
            "a reconstruction holds nothing below its watermark"
        );
        // #190: every journal a frozen member reported is carried, so no
        // journal of the set loses its registry across the handover.
        // Mutation proof: a reconstruction that drops one journal trips this
        // assertion; with it off, the handover model checker fails on its
        // first seed ("an activated registry carries the complete
        // reconstruction").
        assert!(
            acks.values()
                .flat_map(BTreeMap::iter)
                .filter(|(_, registry)| !registry.is_blank())
                .all(|(journal, _)| bootstrap.registries.contains_key(journal)),
            "a reconstruction carries every reported journal"
        );
        assert!(
            bootstrap.set.generation == old.generation.next(),
            "a reconstruction proposes the next generation"
        );
        self.phase = ReconfigurerPhase::Bootstrapping {
            old: old.clone(),
            bootstrap: bootstrap.clone(),
            acks: BTreeSet::new(),
            held: BTreeMap::new(),
            decree_floor: *decree_floor,
        };
        self.elapsed = 0;
        self.resend();
        self.assert_invariants();
        Some(Reconstruction {
            bootstrap,
            disagreements,
        })
    }

    /// Re-queue the running phase's requests to every matchmaker that has not
    /// answered it. **Skipping a call is always safe** (a lost request or
    /// reply only stalls the handover until the next call), and a preempted
    /// decree is reopened at a fresh ballot only here — so the driver's
    /// cadence, not the core, paces dueling reconfigurers.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn resend(&mut self) {
        let me = self.node;
        let queued = self.pending.len();
        let mut queue = self.paged_requests();
        match &mut self.phase {
            ReconfigurerPhase::Idle
            | ReconfigurerPhase::Stopping { .. }
            | ReconfigurerPhase::Bootstrapping { .. } => {}
            ReconfigurerPhase::Deciding {
                old,
                bootstrap,
                decree,
            } => {
                if let Some(promised) = decree.preempted() {
                    // Reopen strictly above the promise that refused us.
                    self.round = self.round.max(promised.round).saturating_add(1);
                    **decree = successor_decree(
                        Ballot {
                            round: self.round,
                            node: me,
                        },
                        old,
                        bootstrap.set.members().to_vec(),
                    );
                }
                let ballot = decree.ballot();
                let generation = old.generation;
                let value = decree.value().cloned();
                for m in decree.unanswered() {
                    queue.push((
                        m,
                        match &value {
                            None => ReconfigureRequest::DecreePrepare {
                                from: me,
                                generation,
                                ballot,
                            },
                            Some(members) => ReconfigureRequest::DecreeAccept {
                                from: me,
                                generation,
                                ballot,
                                members: members.clone(),
                            },
                        },
                    ));
                }
            }
            ReconfigurerPhase::Publishing {
                old,
                successor,
                old_acks,
                new_acks,
            } => {
                let mut targets: Vec<MatchmakerId> = old
                    .members()
                    .iter()
                    .filter(|m| !old_acks.contains(m))
                    .chain(successor.members().iter().filter(|m| !new_acks.contains(m)))
                    .copied()
                    .collect();
                targets.sort_unstable();
                targets.dedup();
                for m in targets {
                    queue.push((
                        m,
                        ReconfigureRequest::Chosen {
                            from: me,
                            generation: old.generation,
                            successor: successor.clone(),
                        },
                    ));
                }
            }
        }
        self.pending.extend(queue);
        self.assert_requests_since(queued);
    }

    /// The most journals one bootstrap page carries (#439): the driver's
    /// page, no wider than the smallest page a frozen member cut its report
    /// at.
    fn bootstrap_page(&self) -> usize {
        let page = self.cut.map_or(self.limits.registry_page, |cut| {
            cut.min(self.limits.registry_page)
        });
        assert!(page > 0, "a bootstrap page carries a journal");
        assert!(
            page <= self.limits.registry_page,
            "a bootstrap page is no wider than the driver's"
        );
        page
    }

    /// The paged phases' re-sends (#439): each member not counted yet is
    /// asked for the page after the last one it answered.
    fn paged_requests(&self) -> Vec<(MatchmakerId, ReconfigureRequest)> {
        let me = self.node;
        let page = self.bootstrap_page();
        let mut queue = Vec::new();
        match &self.phase {
            ReconfigurerPhase::Stopping {
                old, acks, partial, ..
            } => {
                for m in old.members().iter().filter(|m| !acks.contains_key(m)) {
                    queue.push((
                        *m,
                        ReconfigureRequest::Stop {
                            from: me,
                            generation: old.generation,
                            after: partial.get(m).map(|(_, cursor)| *cursor),
                        },
                    ));
                }
            }
            ReconfigurerPhase::Bootstrapping {
                bootstrap,
                acks,
                held,
                ..
            } => {
                for m in bootstrap.set.members().iter().filter(|m| !acks.contains(m)) {
                    let after = held.get(m).copied().flatten();
                    queue.push((
                        *m,
                        ReconfigureRequest::Bootstrap {
                            from: me,
                            bootstrap: bootstrap.page(after, page),
                        },
                    ));
                }
            }
            ReconfigurerPhase::Idle
            | ReconfigurerPhase::Deciding { .. }
            | ReconfigurerPhase::Publishing { .. } => {}
        }
        // Only a member that has not answered whole is asked again.
        assert!(
            queue.iter().all(|(m, _)| match &self.phase {
                ReconfigurerPhase::Stopping { acks, .. } => !acks.contains_key(m),
                ReconfigurerPhase::Bootstrapping { acks, .. } => !acks.contains(m),
                _ => false,
            }),
            "a re-send asks only a member not counted yet"
        );
        queue
    }

    /// A re-send's postconditions: it only adds requests, each in this
    /// reconfigurer's name.
    fn assert_requests_since(&self, queued: usize) {
        assert!(
            self.pending.len() >= queued,
            "a re-send never drops a queued request"
        );
        assert!(
            self.pending[queued..]
                .iter()
                .all(|(_, r)| r.from() == self.node),
            "a re-sent request names its own reconfigurer"
        );
    }

    /// Fold one matchmaker's reply into the running phase.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition).
    pub fn on_reply(&mut self, reply: ReconfigureReply) -> ReconfigurerStep {
        let elapsed = self.elapsed;
        let step = self.fold_reply(reply);
        if !matches!(step, ReconfigurerStep::Ignored) {
            self.elapsed = 0;
        }
        // Only progress resets the stall clock; a duplicate never does.
        if matches!(step, ReconfigurerStep::Ignored) {
            assert!(
                self.elapsed == elapsed,
                "an ignored reply leaves the stall clock alone"
            );
        }
        if matches!(
            step,
            ReconfigurerStep::Done { .. } | ReconfigurerStep::Superseded { .. }
        ) {
            assert!(!self.is_busy(), "a finished or superseded handover is idle");
        }
        self.assert_invariants();
        step
    }

    #[allow(clippy::too_many_lines)]
    fn fold_reply(&mut self, reply: ReconfigureReply) -> ReconfigurerStep {
        let from = reply.matchmaker();
        // A matchmaker that moved on past the generation we are replacing:
        // its answer names the successor we must adopt, whatever phase we
        // are in.
        if let ReconfigureReply::Refused {
            current, successor, ..
        } = &reply
            && let Some(old) = self.old()
        {
            if current.generation > old.generation {
                let successor = current.clone();
                self.abort();
                return ReconfigurerStep::Superseded { successor };
            }
            if current.generation == old.generation
                && let Some(successor) = successor
            {
                let successor = successor.clone();
                self.abort();
                return ReconfigurerStep::Superseded { successor };
            }
            return ReconfigurerStep::Ignored;
        }
        match &mut self.phase {
            ReconfigurerPhase::Idle => ReconfigurerStep::Ignored,
            ReconfigurerPhase::Stopping {
                old,
                acks,
                partial,
                decree_floor,
                ..
            } => {
                let ReconfigureReply::Stopped {
                    generation,
                    after,
                    next,
                    registries,
                    successor,
                    decree_promised,
                    ..
                } = reply
                else {
                    return ReconfigurerStep::Ignored;
                };
                if generation != old.generation || !old.contains(from) {
                    return ReconfigurerStep::Ignored;
                }
                if let Some(successor) = successor {
                    self.abort();
                    return ReconfigurerStep::Superseded { successor };
                }
                if acks.contains_key(&from) {
                    // A member that already answered: the freeze is
                    // idempotent, so a re-sent `Stop` is answered again and
                    // the second copy moves nothing. Reporting it as
                    // progress reset the stall clock, which is how a phase
                    // whose remaining members were all dead stayed alive
                    // for the rest of a run.
                    return ReconfigurerStep::Ignored;
                }
                // A page counts only where it continues the member's report
                // (#439): a stale or duplicated page moves nothing. Wire
                // hygiene: at most a ceiling of journals, all above the
                // cursor, and a cut page continues after its last journal.
                let cursor = partial.get(&from).map(|(_, cursor)| *cursor);
                let well_formed = registries.len() <= REGISTRY_PAGE
                    && registries
                        .keys()
                        .all(|journal| after.is_none_or(|after| *journal > after))
                    && next.is_none_or(|next| registries.keys().next_back() == Some(&next));
                if after != cursor || !well_formed {
                    return ReconfigurerStep::Ignored;
                }
                let width = registries.len();
                let mut report = partial
                    .remove(&from)
                    .map(|(report, _)| report)
                    .unwrap_or_default();
                report.extend(registries);
                *decree_floor = (*decree_floor).max(decree_promised);
                if let Some(next) = next {
                    // A cut page is full: the member's page is its width.
                    assert!(width > 0, "a cut frozen page carries a journal");
                    self.cut = Some(self.cut.map_or(width, |cut| cut.min(width)));
                    // The report continues: ask its next page now.
                    partial.insert(from, (report, next));
                    self.pending.push((
                        from,
                        ReconfigureRequest::Stop {
                            from: self.node,
                            generation,
                            after: Some(next),
                        },
                    ));
                    return ReconfigurerStep::Stopped {
                        remaining: old.quorum_size().saturating_sub(acks.len()),
                    };
                }
                if after.is_some() {
                    probe!(
                        reachable,
                        "handover: a reconfigurer reads a frozen registry over several pages"
                    );
                }
                acks.insert(from, report);
                // The freeze does **not** close here. A quorum is what the
                // reconstruction *needs*, never what it should settle for:
                // closing on the ack that completed it made every `finish`
                // propose exactly `quorum(M_g)` members, ratcheting a set of
                // five to three to two to zero fault tolerance over a run
                // (review finding P5). The driver closes it on its own
                // cadence ([`Self::close_stop`]) once
                // [`Self::stop_quorum_reached`], so every ack that arrives
                // in between widens the reconstruction and a finish's
                // proposal.
                // A counted freeze: the member is in, and the decree floor
                // covers its decree promise.
                assert!(acks.contains_key(&from), "a counted freeze is recorded");
                assert!(
                    *decree_floor >= decree_promised,
                    "the decree floor covers every frozen promise"
                );
                let quorum = old.quorum_size();
                ReconfigurerStep::Stopped {
                    remaining: quorum.saturating_sub(acks.len()),
                }
            }
            ReconfigurerPhase::Bootstrapping {
                old,
                bootstrap,
                acks,
                held,
                decree_floor,
            } => {
                let ReconfigureReply::Bootstrapped {
                    set,
                    held: holds,
                    complete,
                    ..
                } = reply
                else {
                    return ReconfigurerStep::Ignored;
                };
                if set != bootstrap.set || !set.contains(from) || acks.contains(&from) {
                    return ReconfigurerStep::Ignored;
                }
                if !complete {
                    // The member holds the pages up to `holds` (#439): send
                    // the next one now. An answer that moved nothing is no
                    // progress; the driver's re-send cadence asks again.
                    if held.get(&from).copied().flatten() == holds && held.contains_key(&from) {
                        return ReconfigurerStep::Ignored;
                    }
                    held.insert(from, holds);
                    if holds.is_some() {
                        probe!(
                            reachable,
                            "handover: a reconfigurer sends a bootstrap over several pages"
                        );
                    }
                    // `bootstrap_page` borrows all of `self`; the phase is
                    // borrowed here, so read the two fields it reads.
                    let page = self.cut.map_or(self.limits.registry_page, |cut| {
                        cut.min(self.limits.registry_page)
                    });
                    self.pending.push((
                        from,
                        ReconfigureRequest::Bootstrap {
                            from: self.node,
                            bootstrap: bootstrap.page(holds, page),
                        },
                    ));
                    return ReconfigurerStep::Bootstrapped {
                        remaining: bootstrap.set.members().len() - acks.len(),
                    };
                }
                held.remove(&from);
                acks.insert(from);
                assert!(
                    acks.len() <= bootstrap.set.members().len(),
                    "only members of the proposed set acknowledge its bootstrap"
                );
                let remaining = bootstrap.set.members().len() - acks.len();
                if remaining > 0 {
                    return ReconfigurerStep::Bootstrapped { remaining };
                }
                // Every member holds the reconstruction: the set may now be
                // chosen. The decree opens at a fresh ballot of this node —
                // strictly above every round this incarnation used *and*
                // above the stop quorum's decree floor, so a rebooted node
                // never reuses a ballot its earlier incarnation may have had
                // a value accepted at (one ballot, one value).
                self.round = self.round.max(decree_floor.round).saturating_add(1);
                let ballot = Ballot {
                    round: self.round,
                    node: self.node,
                };
                // The decree opens strictly above every decree promise the stop
                // quorum reported, at this reconfigurer's own ballot.
                assert!(
                    ballot.round > decree_floor.round,
                    "a decree opens above the stop quorum's promises"
                );
                assert!(
                    ballot.node == self.node,
                    "a decree runs at this node's ballot"
                );
                let decree = Box::new(successor_decree(
                    ballot,
                    old,
                    bootstrap.set.members().to_vec(),
                ));
                self.phase = ReconfigurerPhase::Deciding {
                    old: old.clone(),
                    bootstrap: bootstrap.clone(),
                    decree,
                };
                self.resend();
                ReconfigurerStep::Deciding { ballot }
            }
            ReconfigurerPhase::Deciding { old, decree, .. } => {
                if !old.contains(from) {
                    return ReconfigurerStep::Ignored;
                }
                match reply {
                    ReconfigureReply::Promised {
                        generation,
                        ballot,
                        vote,
                        ..
                    } => {
                        if generation != old.generation || ballot != decree.ballot() {
                            return ReconfigurerStep::Ignored;
                        }
                        let members = match decree.on_promise(from, vote) {
                            DecreePromise::Ignored => return ReconfigurerStep::Ignored,
                            DecreePromise::Counted { remaining } => {
                                return ReconfigurerStep::Promised { remaining };
                            }
                            DecreePromise::Quorum(members) => members,
                        };
                        let adopted = decree.adopted_prior_vote();
                        // Phase 2 proposes the value P2c selected, at this ballot.
                        assert!(
                            decree.value() == Some(&members),
                            "the decree proposes the selected value"
                        );
                        assert!(
                            decree.ballot() == ballot,
                            "the decree proposes at its own ballot"
                        );
                        self.resend();
                        ReconfigurerStep::Proposing {
                            ballot,
                            members,
                            adopted,
                        }
                    }
                    ReconfigureReply::Accepted {
                        generation, ballot, ..
                    } => {
                        if generation != old.generation || ballot != decree.ballot() {
                            return ReconfigurerStep::Ignored;
                        }
                        let members = match decree.on_accepted(from) {
                            AcceptFold::Ignored => return ReconfigurerStep::Ignored,
                            AcceptFold::Counted { remaining } => {
                                return ReconfigurerStep::Accepted { remaining };
                            }
                            AcceptFold::Chosen(members) => members,
                        };
                        let successor = MatchmakerSet::new(old.generation.next(), members);
                        assert!(
                            successor.generation == old.generation.next(),
                            "a chosen successor is the next generation"
                        );
                        self.phase = ReconfigurerPhase::Publishing {
                            old: old.clone(),
                            successor: successor.clone(),
                            old_acks: BTreeSet::new(),
                            new_acks: BTreeSet::new(),
                        };
                        self.resend();
                        ReconfigurerStep::Chosen { successor }
                    }
                    ReconfigureReply::Nacked {
                        generation,
                        ballot,
                        promised,
                        ..
                    } => {
                        if generation != old.generation || ballot != decree.ballot() {
                            return ReconfigurerStep::Ignored;
                        }
                        let ballot = decree.ballot();
                        // A preempted decree is dead until the driver's next
                        // [`Self::resend`] reopens it, so a second refusal —
                        // the same Nack duplicated, or another member's —
                        // moves nothing and is `Ignored`: reporting it as
                        // `Preempted` again reset the stall clock on every
                        // refusal a dueling finisher sent, which is exactly
                        // the "duplicate keeps a dead phase alive" of review
                        // finding P4 (the handover model's claim 6 caught it
                        // on its first seed). The promise it carries still
                        // raises the floor the reopen clears.
                        let already_preempted = decree.preempted().is_some();
                        decree.on_nack(promised);
                        if already_preempted || decree.preempted().is_none() {
                            return ReconfigurerStep::Ignored;
                        }
                        // Preempted: the decree is reopened above the refusing
                        // promise only by the driver's next [`Self::resend`],
                        // so the driver paces the retry (a jittered backoff is
                        // what breaks a duel between finishers of one frozen
                        // generation — reopening here, on every Nack, kept
                        // five of them preempting each other past round 900).
                        ReconfigurerStep::Preempted { promised, ballot }
                    }
                    _ => ReconfigurerStep::Ignored,
                }
            }
            ReconfigurerPhase::Publishing {
                old,
                successor,
                old_acks,
                new_acks,
            } => {
                let ReconfigureReply::Learned { generation, at, .. } = reply else {
                    return ReconfigurerStep::Ignored;
                };
                if generation != old.generation {
                    return ReconfigurerStep::Ignored;
                }
                // A member of the *successor* counts toward the successor's
                // quorum only once it is actually at that generation.
                // Recording the chain link is what a departing member does;
                // counting it let a publication finish while no quorum of
                // the new set was serving it yet (review finding P7b). The
                // generation test is also what keeps the count idempotent
                // under a re-sent `Chosen` a member answers after
                // activating.
                let counted_old = old.contains(from) && old_acks.insert(from);
                let counted_new =
                    successor.contains(from) && at >= successor.generation && new_acks.insert(from);
                if !counted_old && !counted_new {
                    return ReconfigurerStep::Ignored;
                }
                // A counted learner is in the tally of the set it counts for.
                if counted_new {
                    assert!(
                        new_acks.contains(&from),
                        "a counted successor member is tallied"
                    );
                }
                if old.has_quorum(old_acks) && successor.has_quorum(new_acks) {
                    let successor = successor.clone();
                    self.phase = ReconfigurerPhase::Idle;
                    self.pending.clear();
                    return ReconfigurerStep::Done { successor };
                }
                ReconfigurerStep::Published {
                    old_remaining: old.remaining(old_acks),
                    new_remaining: successor.remaining(new_acks),
                }
            }
        }
    }

    /// Drop the running handover.
    fn abort(&mut self) {
        self.phase = ReconfigurerPhase::Idle;
        self.pending.clear();
        self.elapsed = 0;
        assert!(!self.is_busy(), "an aborted handover is idle");
        assert!(
            self.pending.is_empty(),
            "an aborted handover queues nothing"
        );
    }
}

/// The handover's decree (#125): the successor set, proposed at `ballot`
/// over the matchmakers of `old` under a majority — the only matchmaker
/// quorum paros supports.
///
/// # Panics
///
/// If the decree's acceptors are not exactly the set being replaced.
fn successor_decree(
    ballot: Ballot,
    old: &MatchmakerSet,
    proposal: Vec<MatchmakerId>,
) -> SuccessorDecree {
    let acceptors = AcceptorConfig::new(old.members().to_vec(), QuorumSystem::Majority);
    let decree = SuccessorDecree::new(ballot, acceptors, proposal);
    assert!(
        decree.acceptors().members() == old.members(),
        "a decree's acceptors are exactly the set being replaced"
    );
    assert!(
        decree.acceptors().quorum_system() == QuorumSystem::Majority,
        "a handover decree counts majorities only"
    );
    decree
}

#[cfg(test)]
mod tests;
