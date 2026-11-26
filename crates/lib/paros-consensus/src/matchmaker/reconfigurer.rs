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
//!   |             the round on disk (a rebooted node must never reuse a ballot of its
//!   |             earlier incarnation) and the stop quorum's decree promises, P2c adopts a competing proposal
//!   |             already voted, Phase 2 with the selected value; a Nack reopens
//!   |             higher on the driver's next re-send
//!   v
//! Publishing      Chosen{g, successor} -> M_g ∪ successor; done once a quorum of
//!   |             each has learned it (re-sent to whoever has not answered; a member
//!   |             that already activated answers `Learned` again, so a lost ack is
//!   v             recovered; the driver tells a straggler again when a node meets
//! Idle            one, paros-roles' `node/shared.rs` `republish`)
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
//! The reconfigurer's only durable state is its decree round. A crash loses
//! the proposal, and the next attempt (this node's or another's) re-runs the
//! same steps: the stop is idempotent, the bootstrap is keyed by the proposed
//! set, and the decree's votes are durable at the matchmakers, so the retry
//! either adopts what was already chosen or completes what was started. The
//! one thing a fresh incarnation must not do is reuse a decree ballot its
//! predecessor may have had a value accepted at. So the driver writes each
//! new round before the batch that carries it leaves
//! ([`ReconfigurerReady::round`]), and a new incarnation starts from the round
//! on disk ([`MatchmakerReconfigurer::new`]). The floor of the stop quorum
//! (each `Stopped` carries the member's decree promise) does not give this
//! alone: a `Stopped` carries no incarnation, so a reply sent to an earlier
//! incarnation can complete the stop quorum of a later one with a floor below
//! a round the earlier one used (#1229 (a stale `Stopped` reply reuses a
//! decree ballot), `specs/tla/experiments/stale-stopped/`). The decree still
//! opens above the floor, which skips rounds the matchmakers would refuse.

use std::collections::{BTreeMap, BTreeSet};

use super::{
    JournalRegistry, MatchmakerId, MatchmakerSet, PendingBootstrap, ReconfigureReply,
    ReconfigureRequest, raise_effective,
};
use super::{Retirement, SuccessorDecree};
use crate::decree::{AcceptFold, DecreePromise};
use crate::journal::chain::merge_chain;
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
        /// The frozen registries collected so far, one per member.
        acks: BTreeMap<MatchmakerId, JournalRegistry>,
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
        /// Members that durably hold it.
        acks: BTreeSet<MatchmakerId>,
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
/// 1. Write [`ReconfigurerReady::round`] to disk when it is `Some`, durably,
///    before anything of the batch leaves. The decree round is the
///    reconfigurer's only durable state; the rest of a handover is lost in a
///    crash, and the next attempt runs the idempotent steps again.
/// 2. Send [`ReconfigurerReady::requests`].
/// 3. Call [`ReconfigurerReady::advance`] to release the gate.
#[must_use = "a ReconfigurerReady must be processed and then advanced; a dropped one keeps its batch, which the next ready() returns again"]
pub struct ReconfigurerReady<'a> {
    reconfigurer: &'a mut MatchmakerReconfigurer,
}

impl ReconfigurerReady<'_> {
    /// The decree round the driver writes to disk before it sends the batch,
    /// or `None` when the batch opens no new round. At the next boot the
    /// driver gives the round on disk to [`MatchmakerReconfigurer::new`].
    ///
    /// A decree ballot has one value only if no two incarnations of a node
    /// open the decree at one round. The floor of the stop quorum does not
    /// give that: a `Stopped` reply carries no incarnation, so a reply sent to
    /// an earlier incarnation can complete the stop quorum of a later one with
    /// a floor below a round the earlier one used (#1229 (a stale `Stopped`
    /// reply reuses a decree ballot), `specs/tla/experiments/stale-stopped/`).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn round(&self) -> Option<u64> {
        let r = &*self.reconfigurer;
        // A round to write is the round the next decree request carries,
        // and it is above the round already on disk.
        if let Some(round) = r.unpersisted {
            assert!(
                round == r.round,
                "the round to write is the last round minted"
            );
            assert!(
                round > r.durable,
                "the round to write is above the round on disk"
            );
        } else {
            assert!(
                r.round == r.durable,
                "a batch with no round to write sends a round on disk"
            );
        }
        r.unpersisted
    }

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
        // Persist before send: no decree request carries a round above the
        // round that this batch writes or that is already on disk.
        let written = self
            .reconfigurer
            .unpersisted
            .unwrap_or(self.reconfigurer.durable);
        assert!(
            requests.iter().all(|(_, r)| match r {
                ReconfigureRequest::DecreePrepare { ballot, .. }
                | ReconfigureRequest::DecreeAccept { ballot, .. } => ballot.round <= written,
                _ => true,
            }),
            "a decree request carries a round that is on disk before it leaves"
        );
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
        let r = self.reconfigurer;
        r.pending.clear();
        // The driver wrote the round of this batch: it is on disk now.
        if let Some(round) = r.unpersisted.take() {
            assert!(round > r.durable, "the round on disk only rises");
            r.durable = round;
        }
        assert!(
            r.pending.is_empty(),
            "an advanced batch has no request left"
        );
        assert!(
            r.round == r.durable,
            "an advanced batch leaves its round on disk"
        );
    }
}

/// Mint the next decree round: strictly above `round` (the last round minted,
/// at or above the round on disk) and above `floor`, and mark it to be
/// written before the batch that carries it leaves.
fn mint(round: &mut u64, unpersisted: &mut Option<u64>, floor: u64) -> u64 {
    let before = *round;
    *round = (*round).max(floor).saturating_add(1);
    *unpersisted = Some(*round);
    assert!(
        *round > before,
        "a minted round is above every round minted before"
    );
    assert!(*round > floor, "a minted round is above the floor");
    *round
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
    /// The last decree round this node minted: the round on disk, or the
    /// round of the batch not yet advanced. It only rises, across
    /// incarnations too: [`MatchmakerReconfigurer::new`] starts from the
    /// round on disk.
    round: u64,
    /// The decree round on disk, as the driver last acknowledged it.
    durable: u64,
    /// A round minted since the last [`ReconfigurerReady::advance`], that the
    /// driver writes before it sends the batch ([`ReconfigurerReady::round`]).
    unpersisted: Option<u64>,
    phase: ReconfigurerPhase,
    pending: Vec<(MatchmakerId, ReconfigureRequest)>,
    /// Driver ticks since the running phase last made progress (see
    /// [`MatchmakerReconfigurer::stalled_for`]).
    elapsed: u64,
}

impl MatchmakerReconfigurer {
    /// An idle reconfigurer for `node`, that opens its next decree above
    /// `round`, the
    /// decree round on disk (0 when the node never wrote one).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or
    /// postconditions fails: a programmer error, never an operating
    /// condition.
    #[must_use]
    pub fn new(node: NodeId, round: u64) -> Self {
        let reconfigurer = Self {
            node,
            round,
            durable: round,
            unpersisted: None,
            phase: ReconfigurerPhase::Idle,
            pending: Vec::new(),
            elapsed: 0,
        };
        assert!(!reconfigurer.is_busy(), "a fresh reconfigurer is idle");
        assert!(
            reconfigurer.round == round,
            "a fresh reconfigurer starts from the round on disk"
        );
        reconfigurer.assert_invariants();
        reconfigurer
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
        self.assert_round();
        match &self.phase {
            ReconfigurerPhase::Idle => {
                assert!(
                    self.pending.is_empty(),
                    "an idle reconfigurer queues nothing"
                );
                assert!(self.elapsed == 0, "an idle reconfigurer's clock is stopped");
            }
            ReconfigurerPhase::Stopping {
                old, target, acks, ..
            } => {
                assert!(
                    acks.keys().all(|m| old.contains(*m)),
                    "only members of the replaced generation acknowledge its freeze"
                );
                assert!(
                    acks.values().all(JournalRegistry::holds_nothing_below),
                    "a counted freeze report holds nothing below its watermark"
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
                ..
            } => {
                assert!(
                    bootstrap.set.generation == old.generation.next(),
                    "a bootstrap proposes the next generation"
                );
                assert!(
                    acks.iter().all(|m| bootstrap.set.contains(*m)),
                    "only members of the proposed set acknowledge its bootstrap"
                );
                assert!(
                    bootstrap.registry.holds_nothing_below(),
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

    /// The minted round is on disk, or is the round the next batch writes.
    fn assert_round(&self) {
        assert!(
            self.round == self.unpersisted.unwrap_or(self.durable),
            "the minted round is on disk or waits to be written"
        );
        assert!(
            self.unpersisted.is_none_or(|round| round > self.durable),
            "a round waiting to be written is above the round on disk"
        );
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
    /// an operator can grow the set back afterwards. An even count of
    /// answerers is thinned to an odd one (`odd_answerers`): a set of two
    /// survives no loss and waits on both members at every bootstrap and
    /// election, where a set of one waits on one.
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
            decree_floor: Ballot::zero(),
        };
        self.elapsed = 0;
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
        // The reconstruction (§5): the maximum watermark, the union of
        // every frozen registry at or above it, and the maximum effective
        // configuration. A ballot reported twice carries one registration
        // (the write-once ledger); the first seen is kept.
        let mut registry = JournalRegistry::default();
        for frozen in acks.values() {
            registry.gc_watermark = registry.gc_watermark.max(frozen.gc_watermark);
            // The effective configuration is a monotone scalar, not a
            // record: the maximum over the frozen members carries the
            // acceptor set in force into the successor generation even
            // when its own registration was collected long ago.
            if let Some((ballot, config)) = &frozen.effective {
                raise_effective(&mut registry.effective, *ballot, config);
            }
            // The retirement records merge over the frozen members
            // (#1002 (retire fence gaps)): the later incarnation wins,
            // and at one incarnation a retirement wins. A node retires
            // only once every member of the old set holds it, so every
            // frozen quorum reports it.
            Retirement::merge_all(&mut registry.retired, &frozen.retired);
            // The chain of segments merges over the frozen members (R5):
            // a publish completed at a majority of the old set, which
            // meets every frozen quorum.
            merge_chain(&mut registry.segments, &frozen.segments);
        }
        let mut disagreements: u64 = 0;
        let gc_watermark = registry.gc_watermark;
        for frozen in acks.values() {
            for (ballot, registration) in frozen.history.range(gc_watermark..) {
                match registry.history.entry(*ballot) {
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
        // A finish proposes the members that answered the freeze, thinned to
        // an odd count (`odd_answerers`).
        let target = target
            .clone()
            .unwrap_or_else(|| odd_answerers(acks.keys().copied()));
        assert!(!target.is_empty(), "a reconstruction proposes a member");
        let bootstrap = PendingBootstrap {
            set: MatchmakerSet::new(old.generation.next(), target),
            registry,
        };
        assert!(
            bootstrap.registry.holds_nothing_below(),
            "a reconstruction holds nothing below its watermark"
        );
        // Every registration a frozen member reported at or above the
        // reconstructed floor is carried, so no registration a later Phase
        // 1 needs is lost across the handover.
        assert!(
            acks.values().all(|frozen| frozen
                .history
                .range(gc_watermark..)
                .all(|(ballot, _)| bootstrap.registry.history.contains_key(ballot))),
            "a reconstruction carries every reported registration above its floor"
        );
        assert!(
            bootstrap.set.generation == old.generation.next(),
            "a reconstruction proposes the next generation"
        );
        self.phase = ReconfigurerPhase::Bootstrapping {
            old: old.clone(),
            bootstrap: bootstrap.clone(),
            acks: BTreeSet::new(),
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
        let mut queue = self.freeze_and_bootstrap_requests();
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
                    let round = mint(&mut self.round, &mut self.unpersisted, promised.round);
                    **decree = successor_decree(
                        Ballot { round, node: me },
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

    /// The re-sends of the freeze and the bootstrap: each member not
    /// counted yet is asked again.
    fn freeze_and_bootstrap_requests(&self) -> Vec<(MatchmakerId, ReconfigureRequest)> {
        let me = self.node;
        let mut queue = Vec::new();
        match &self.phase {
            ReconfigurerPhase::Stopping { old, acks, .. } => {
                for m in old.members().iter().filter(|m| !acks.contains_key(m)) {
                    queue.push((
                        *m,
                        ReconfigureRequest::Stop {
                            from: me,
                            generation: old.generation,
                        },
                    ));
                }
            }
            ReconfigurerPhase::Bootstrapping {
                bootstrap, acks, ..
            } => {
                for m in bootstrap.set.members().iter().filter(|m| !acks.contains(m)) {
                    queue.push((
                        *m,
                        ReconfigureRequest::Bootstrap {
                            from: me,
                            bootstrap: bootstrap.clone(),
                        },
                    ));
                }
            }
            ReconfigurerPhase::Idle
            | ReconfigurerPhase::Deciding { .. }
            | ReconfigurerPhase::Publishing { .. } => {}
        }
        // Only a member that has not answered is asked again.
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
            ReconfigurerPhase::Stopping { .. } => self.fold_stopping(from, reply),
            ReconfigurerPhase::Bootstrapping { .. } => self.fold_bootstrapping(from, reply),
            ReconfigurerPhase::Deciding { .. } => self.fold_deciding(from, reply),
            ReconfigurerPhase::Publishing { .. } => self.fold_publishing(from, &reply),
        }
    }

    /// The frozen report of a member of the generation being replaced.
    fn fold_stopping(&mut self, from: MatchmakerId, reply: ReconfigureReply) -> ReconfigurerStep {
        let ReconfigurerPhase::Stopping {
            old,
            acks,
            decree_floor,
            ..
        } = &mut self.phase
        else {
            unreachable!("fold_stopping runs in the Stopping phase");
        };
        let ReconfigureReply::Stopped {
            generation,
            registry,
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
        // Wire hygiene: a report with a registration below its own
        // watermark moves nothing.
        if !registry.holds_nothing_below() {
            return ReconfigurerStep::Ignored;
        }
        *decree_floor = (*decree_floor).max(decree_promised);
        acks.insert(from, registry);
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

    /// A member of the proposed set holds the reconstruction.
    fn fold_bootstrapping(
        &mut self,
        from: MatchmakerId,
        reply: ReconfigureReply,
    ) -> ReconfigurerStep {
        let ReconfigurerPhase::Bootstrapping {
            old,
            bootstrap,
            acks,
            decree_floor,
        } = &mut self.phase
        else {
            unreachable!("fold_bootstrapping runs in the Bootstrapping phase");
        };
        let ReconfigureReply::Bootstrapped { set, .. } = reply else {
            return ReconfigurerStep::Ignored;
        };
        if set != bootstrap.set || !set.contains(from) || acks.contains(&from) {
            return ReconfigurerStep::Ignored;
        }
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
        // chosen. The decree opens at a fresh ballot of this node:
        // above every round on disk, so no incarnation of this node
        // reuses a ballot an earlier one may have had a value accepted
        // at (one ballot, one value), and above the stop quorum's
        // decree floor, so the matchmakers do not refuse it.
        let round = mint(&mut self.round, &mut self.unpersisted, decree_floor.round);
        let ballot = Ballot {
            round,
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

    /// A member of the old generation answers the successor decree.
    fn fold_deciding(&mut self, from: MatchmakerId, reply: ReconfigureReply) -> ReconfigurerStep {
        let ReconfigurerPhase::Deciding { old, decree, .. } = &mut self.phase else {
            unreachable!("fold_deciding runs in the Deciding phase");
        };
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

    /// A member learned the chosen successor.
    fn fold_publishing(
        &mut self,
        from: MatchmakerId,
        reply: &ReconfigureReply,
    ) -> ReconfigurerStep {
        let ReconfigurerPhase::Publishing {
            old,
            successor,
            old_acks,
            new_acks,
        } = &mut self.phase
        else {
            unreachable!("fold_publishing runs in the Publishing phase");
        };
        let ReconfigureReply::Learned { generation, at, .. } = *reply else {
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

/// The successor a finish proposes from the members that answered its
/// freeze, in id order: all of them when their count is odd, else all but
/// the last. A set of two survives no loss and waits on both members at
/// every bootstrap and election, where a set of one waits on one: the rule
/// the coordinator applies to every special set (#846 (set below 2F+1)),
/// applied to the one other maker of sets. Witness seed 6896547675157816584
/// on c377e64: a finish made the universe tenant's set `{a, b}` of three,
/// `b`'s machine was wiped, and the universe journal elected no leader for
/// the rest of the run (red on "resolve: every initial member answers
/// after chaos"). The coordinator grows the set back to its size afterwards.
/// No answerer gives no member: a finish closes only on a stop quorum, and
/// the close asserts its proposal names one.
pub(crate) fn odd_answerers(answerers: impl Iterator<Item = MatchmakerId>) -> Vec<MatchmakerId> {
    let mut members: Vec<MatchmakerId> = answerers.collect();
    let answered = members.len();
    // An even count drops its last answerer; no answerer drops nothing.
    if answered.is_multiple_of(2) && members.pop().is_some() {
        probe!(
            reachable,
            "handover: a finish thins an even count of answerers to an odd successor"
        );
    }
    assert!(
        members.is_empty() || members.len() % 2 == 1,
        "a finish proposes an odd set"
    );
    assert!(
        members.len() == answered || members.len() + 1 == answered,
        "a finish drops at most one answerer"
    );
    members
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
