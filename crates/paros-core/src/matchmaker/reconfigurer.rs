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
//! Deciding        single-decree Paxos over M_g as acceptors (`DecreeProposer`, a
//!   |             majority of M_g for both phases — the only quorum model paros
//!   |             supports for matchmakers): Phase 1 at a fresh ballot strictly above
//!   |             the stop quorum's decree promises (a rebooted node must never reuse
//!   |             a ballot of its earlier incarnation), P2c adopts a competing proposal
//!   |             already voted, Phase 2 with the selected value; a Nack reopens
//!   |             higher on the driver's next re-send
//!   v
//! Publishing      Chosen{g, successor} -> M_g ∪ successor; done once a quorum of
//!   |             each has learned it (stragglers are told again by any node that
//!   v             meets them, see `RawNode::on_match_reply`)
//! Idle
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
//! maximum (the handover model checker, `super::handover_model`, found the
//! reuse on its seed 103 — two values at one ballot).

use std::collections::{BTreeMap, BTreeSet};

use super::{
    MatchmakerId, MatchmakerSet, PendingBootstrap, ReconfigureReply, ReconfigureRequest,
    Registration,
};
use crate::membership::AcceptorConfig;
use crate::single_decree::{AcceptFold, DecreePhase, DecreeProposer, PromiseFold};
use crate::types::{Ballot, NodeId};

/// Why [`MatchmakerReconfigurer::start`] refused a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartRefusal {
    /// A reconfiguration is already running here.
    Busy,
    /// The target names no matchmaker.
    Empty,
    /// A set in the request does not admit the matchmaker quorum system
    /// ([`MatchmakerSet::is_well_formed`]) — the generation being replaced,
    /// or the target. Under the majority system only the *current* set can
    /// reach it in practice: [`MatchmakerSet::new`] sorts and deduplicates
    /// the target, and every non-empty sorted set admits a majority. The
    /// current set is not normalized here — it is what the caller believes
    /// authoritative — and a malformed one would install a `Stopping` phase
    /// whose quorum can never complete (an empty one panics the first time
    /// a quorum is drawn).
    Malformed,
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
        /// The frozen registries collected so far.
        acks: BTreeMap<MatchmakerId, (Ballot, BTreeMap<Ballot, Registration>)>,
        /// The highest decree ballot any frozen member reported promised:
        /// the decree opens strictly above it (see `decree_floor` on
        /// [`ReconfigureReply::Stopped`]).
        decree_floor: Ballot,
        /// The highest **effective configuration** any frozen member
        /// reported: the reconstruction carries the maximum, exactly as it
        /// carries the maximum watermark.
        effective: Option<(Ballot, AcceptorConfig)>,
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
        /// The single-decree proposer.
        proposer: DecreeProposer<MatchmakerId, Vec<MatchmakerId>>,
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
    #[must_use]
    pub fn requests(&self) -> &[(MatchmakerId, ReconfigureRequest)] {
        &self.reconfigurer.pending
    }

    /// Acknowledge the batch: clears the queue and releases the unique
    /// borrow. Consumes `self` — the guard cannot be reused.
    pub fn advance(self) {
        self.reconfigurer.pending.clear();
    }
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
}

impl MatchmakerReconfigurer {
    /// An idle reconfigurer for `node`.
    #[must_use]
    pub fn new(node: NodeId) -> Self {
        Self {
            node,
            round: 0,
            phase: ReconfigurerPhase::Idle,
            pending: Vec::new(),
            elapsed: 0,
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
    #[must_use]
    pub fn old(&self) -> Option<&MatchmakerSet> {
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
    /// [`crate::MatchmakerReady`] and [`crate::Ready`] put on their batches.
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
    /// for an empty target; [`StartRefusal::Malformed`] when `current` or the
    /// target does not admit the matchmaker quorum system.
    pub fn start(
        &mut self,
        current: &MatchmakerSet,
        target: Vec<MatchmakerId>,
    ) -> Result<(), StartRefusal> {
        if self.is_busy() {
            return Err(StartRefusal::Busy);
        }
        // The generation being replaced is a quorum system too: every freeze
        // and every decree quorum is drawn over it.
        if !current.is_well_formed() {
            return Err(StartRefusal::Malformed);
        }
        let target = MatchmakerSet::new(current.generation.next(), target);
        if target.members.is_empty() {
            return Err(StartRefusal::Empty);
        }
        // The successor must itself admit the quorum system every later
        // freeze, decree and registration quorum is drawn from.
        if !target.is_well_formed() {
            return Err(StartRefusal::Malformed);
        }
        let target = target.members;
        self.phase = ReconfigurerPhase::Stopping {
            old: current.clone(),
            target: Some(target),
            acks: BTreeMap::new(),
            decree_floor: Ballot::zero(),
            effective: None,
        };
        self.elapsed = 0;
        self.resend();
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
    /// [`StartRefusal::Busy`] while a handover runs;
    /// [`StartRefusal::Malformed`] when `current` does not admit the
    /// matchmaker quorum system.
    pub fn finish(&mut self, current: &MatchmakerSet) -> Result<(), StartRefusal> {
        if self.is_busy() {
            return Err(StartRefusal::Busy);
        }
        if !current.is_well_formed() {
            return Err(StartRefusal::Malformed);
        }
        self.phase = ReconfigurerPhase::Stopping {
            old: current.clone(),
            target: None,
            acks: BTreeMap::new(),
            decree_floor: Ballot::zero(),
            effective: None,
        };
        self.elapsed = 0;
        self.resend();
        Ok(())
    }

    /// One driver tick while a handover runs: the running phase's stall
    /// clock advances. The core only *knows* that it has made no progress
    /// ([`Self::stalled_for`]); whether that is long enough to give up is
    /// the driver's decision ([`Self::abandon`]) — the timeout is policy,
    /// paced in the driver's own units, never a constant inside the state
    /// machine.
    pub fn tick(&mut self) {
        if self.is_busy() {
            self.elapsed = self.elapsed.saturating_add(1);
        }
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
    pub fn abandon(&mut self) -> bool {
        if !self.is_busy() {
            return false;
        }
        self.abort();
        true
    }

    /// Whether the running freeze has the quorum it needs to be closed
    /// ([`Self::close_stop`]). `false` in every other phase.
    #[must_use]
    pub fn stop_quorum_reached(&self) -> bool {
        match &self.phase {
            ReconfigurerPhase::Stopping { old, acks, .. } => acks.len() >= old.quorum_size(),
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
    pub fn close_stop(&mut self) -> Option<PendingBootstrap> {
        let ReconfigurerPhase::Stopping {
            old,
            target,
            acks,
            decree_floor,
            effective,
        } = &mut self.phase
        else {
            return None;
        };
        if acks.len() < old.quorum_size() {
            return None;
        }
        // The reconstruction (§5): the maximum watermark, and the union of
        // every frozen registry at or above it. A ballot reported twice
        // carries one registration (the write-once ledger); the first seen
        // is kept.
        let gc_watermark = acks.values().map(|(w, _)| *w).max().unwrap_or_default();
        let mut history: BTreeMap<Ballot, Registration> = BTreeMap::new();
        for (_, registry) in acks.values() {
            for (ballot, registration) in registry {
                if *ballot >= gc_watermark {
                    history
                        .entry(*ballot)
                        .or_insert_with(|| registration.clone());
                }
            }
        }
        // A finish proposes every member that answered the freeze.
        let target = target
            .clone()
            .unwrap_or_else(|| acks.keys().copied().collect());
        let bootstrap = PendingBootstrap {
            set: MatchmakerSet::new(old.generation.next(), target),
            gc_watermark,
            history,
            effective: effective.clone(),
        };
        assert!(
            bootstrap
                .history
                .keys()
                .all(|b| *b >= bootstrap.gc_watermark),
            "a reconstruction holds nothing below its watermark"
        );
        // A proposed successor admits its quorum system: a `start` refused a
        // malformed target, and a `finish` proposes the members that
        // answered the freeze — a quorum of the old set, never fewer.
        assert!(
            bootstrap.set.is_well_formed(),
            "a proposed successor admits the matchmaker quorum system"
        );
        self.phase = ReconfigurerPhase::Bootstrapping {
            old: old.clone(),
            bootstrap: bootstrap.clone(),
            acks: BTreeSet::new(),
            decree_floor: *decree_floor,
        };
        self.elapsed = 0;
        self.resend();
        Some(bootstrap)
    }

    /// Re-queue the running phase's requests to every matchmaker that has not
    /// answered it. **Skipping a call is always safe** (a lost request or
    /// reply only stalls the handover until the next call), and a preempted
    /// decree is reopened at a fresh ballot only here — so the driver's
    /// cadence, not the core, paces dueling reconfigurers.
    pub fn resend(&mut self) {
        let me = self.node;
        let mut queue: Vec<(MatchmakerId, ReconfigureRequest)> = Vec::new();
        match &mut self.phase {
            ReconfigurerPhase::Idle => {}
            ReconfigurerPhase::Stopping { old, acks, .. } => {
                for m in old.members.iter().filter(|m| !acks.contains_key(m)) {
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
                for m in bootstrap.set.members.iter().filter(|m| !acks.contains(m)) {
                    queue.push((
                        *m,
                        ReconfigureRequest::Bootstrap {
                            from: me,
                            bootstrap: bootstrap.clone(),
                        },
                    ));
                }
            }
            ReconfigurerPhase::Deciding {
                old,
                bootstrap,
                proposer,
            } => {
                if let DecreePhase::Preempted(promised) = proposer.phase() {
                    // Reopen strictly above the promise that refused us.
                    self.round = self.round.max(promised.round).saturating_add(1);
                    *proposer = DecreeProposer::new(
                        Ballot {
                            round: self.round,
                            node: me,
                        },
                        old.members.iter().copied(),
                        bootstrap.set.members.clone(),
                    );
                }
                let ballot = proposer.ballot();
                let generation = old.generation;
                let value = proposer.value().cloned();
                for m in proposer.unanswered() {
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
                    .members
                    .iter()
                    .filter(|m| !old_acks.contains(m))
                    .chain(successor.members.iter().filter(|m| !new_acks.contains(m)))
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
    }

    /// Fold one matchmaker's reply into the running phase.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition).
    pub fn on_reply(&mut self, reply: ReconfigureReply) -> ReconfigurerStep {
        let step = self.fold_reply(reply);
        if !matches!(step, ReconfigurerStep::Ignored) {
            self.elapsed = 0;
        }
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
                decree_floor,
                effective,
                ..
            } => {
                let ReconfigureReply::Stopped {
                    generation,
                    gc_watermark,
                    history,
                    effective: reported,
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
                if acks.insert(from, (gc_watermark, history)).is_some() {
                    // A member that already answered: the freeze is
                    // idempotent, so a re-sent `Stop` is answered again and
                    // the second copy moves nothing. Reporting it as
                    // progress reset the stall clock, which is how a phase
                    // whose remaining members were all dead stayed alive
                    // for the rest of a run.
                    return ReconfigurerStep::Ignored;
                }
                *decree_floor = (*decree_floor).max(decree_promised);
                // The effective configuration is a monotone scalar, not a
                // record: the maximum over the frozen members carries the
                // acceptor set in force into the successor generation even
                // when its own registration was collected long ago.
                if let Some((ballot, config)) = reported
                    && effective.as_ref().is_none_or(|(held, _)| ballot > *held)
                {
                    *effective = Some((ballot, config));
                }
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
                let quorum = old.quorum_size();
                ReconfigurerStep::Stopped {
                    remaining: quorum.saturating_sub(acks.len()),
                }
            }
            ReconfigurerPhase::Bootstrapping {
                old,
                bootstrap,
                acks,
                decree_floor,
            } => {
                let ReconfigureReply::Bootstrapped { set, .. } = reply else {
                    return ReconfigurerStep::Ignored;
                };
                if set != bootstrap.set || !set.contains(from) || !acks.insert(from) {
                    return ReconfigurerStep::Ignored;
                }
                let remaining = bootstrap.set.members.len() - acks.len();
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
                let proposer = DecreeProposer::new(
                    ballot,
                    old.members.iter().copied(),
                    bootstrap.set.members.clone(),
                );
                self.phase = ReconfigurerPhase::Deciding {
                    old: old.clone(),
                    bootstrap: bootstrap.clone(),
                    proposer,
                };
                self.resend();
                ReconfigurerStep::Deciding { ballot }
            }
            ReconfigurerPhase::Deciding { old, proposer, .. } => {
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
                        if generation != old.generation || ballot != proposer.ballot() {
                            return ReconfigurerStep::Ignored;
                        }
                        let members = match proposer.on_promise(from, vote) {
                            PromiseFold::Ignored => return ReconfigurerStep::Ignored,
                            PromiseFold::Counted { remaining } => {
                                return ReconfigurerStep::Promised { remaining };
                            }
                            PromiseFold::Quorum(members) => members,
                        };
                        let adopted = proposer.adopted_prior_vote();
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
                        if generation != old.generation || ballot != proposer.ballot() {
                            return ReconfigurerStep::Ignored;
                        }
                        let members = match proposer.on_accepted(from) {
                            AcceptFold::Ignored => return ReconfigurerStep::Ignored,
                            AcceptFold::Counted { remaining } => {
                                return ReconfigurerStep::Accepted { remaining };
                            }
                            AcceptFold::Chosen(members) => members,
                        };
                        let successor = MatchmakerSet::new(old.generation.next(), members);
                        // The decree chose what some reconfigurer bootstrapped,
                        // and every bootstrap was a well-formed proposal.
                        assert!(
                            successor.is_well_formed(),
                            "a chosen successor admits the matchmaker quorum system"
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
                        if generation != old.generation || ballot != proposer.ballot() {
                            return ReconfigurerStep::Ignored;
                        }
                        let ballot = proposer.ballot();
                        proposer.on_nack(promised);
                        if !matches!(proposer.phase(), DecreePhase::Preempted(_)) {
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
                if old.has_quorum(old_acks) && successor.has_quorum(new_acks) {
                    let successor = successor.clone();
                    self.phase = ReconfigurerPhase::Idle;
                    self.pending.clear();
                    return ReconfigurerStep::Done { successor };
                }
                ReconfigurerStep::Published {
                    old_remaining: old.quorum_size().saturating_sub(old_acks.len()),
                    new_remaining: successor.quorum_size().saturating_sub(new_acks.len()),
                }
            }
        }
    }

    /// Drop the running handover.
    fn abort(&mut self) {
        self.phase = ReconfigurerPhase::Idle;
        self.pending.clear();
        self.elapsed = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::matchmaker::MatchmakerPhase;
    use crate::matchmaker::{Matchmaker, MatchmakerConfig, MatchmakerHardState, RegistryStorage};
    use crate::membership::MatchmakerGeneration;

    struct Empty;
    impl RegistryStorage for Empty {
        fn initial_state(&self) -> MatchmakerHardState {
            MatchmakerHardState::default()
        }
        fn registration(&self, _ballot: Ballot) -> Option<Registration> {
            None
        }
        fn registered_ballots(&self) -> Vec<Ballot> {
            Vec::new()
        }
    }

    fn ids(v: &[u64]) -> Vec<MatchmakerId> {
        v.iter().copied().map(MatchmakerId).collect()
    }

    /// Review finding P10: `start` and `finish` validated the target but not
    /// the set being replaced. An empty or unsorted `current` installed a
    /// `Stopping` phase whose quorum can never complete — and whose first
    /// quorum draw panics — from a value the caller believes authoritative.
    #[test]
    fn a_malformed_current_set_is_refused_by_start_and_finish() {
        let mut r = MatchmakerReconfigurer::new(NodeId(0));
        let empty = MatchmakerSet {
            generation: MatchmakerGeneration(0),
            members: Vec::new(),
        };
        assert_eq!(
            r.start(&empty, ids(&[0, 1, 2])),
            Err(StartRefusal::Malformed)
        );
        assert_eq!(r.finish(&empty), Err(StartRefusal::Malformed));
        let unsorted = MatchmakerSet {
            generation: MatchmakerGeneration(0),
            members: ids(&[2, 1, 0]),
        };
        assert_eq!(
            r.start(&unsorted, ids(&[0, 1, 2])),
            Err(StartRefusal::Malformed)
        );
        assert_eq!(r.finish(&unsorted), Err(StartRefusal::Malformed));
        assert!(!r.is_busy(), "a refusal starts nothing");
        // The same call over a well-formed current runs.
        let current = MatchmakerSet::new(MatchmakerGeneration(0), ids(&[0, 1, 2]));
        assert_eq!(r.start(&current, ids(&[0, 1, 3])), Ok(()));
        assert!(r.is_busy());
    }

    /// A pool of matchmakers, `0..n`, bootstrapped on `bootstrap`.
    fn pool(n: u64, bootstrap: &[u64]) -> Vec<Matchmaker> {
        (0..n)
            .map(|i| {
                Matchmaker::new(
                    &MatchmakerConfig {
                        id: MatchmakerId(i),
                        bootstrap: ids(bootstrap),
                    },
                    &Empty,
                )
            })
            .collect()
    }

    /// Deliver every queued request of `r` to the pool and fold the replies,
    /// returning the steps. `drop` names matchmakers that never answer.
    fn exchange(
        r: &mut MatchmakerReconfigurer,
        pool: &mut [Matchmaker],
        drop: &[u64],
    ) -> Vec<ReconfigurerStep> {
        let mut steps = Vec::new();
        let ready = r.ready();
        let requests = ready.requests().to_vec();
        ready.advance();
        for (to, request) in requests {
            if drop.contains(&to.0) {
                continue;
            }
            let mm = &mut pool[usize::try_from(to.0).expect("index")];
            mm.step_reconfigure(request);
            let ready = mm.ready();
            let replies = ready.reconfigure_replies().to_vec();
            ready.advance();
            for reply in replies {
                steps.push(r.on_reply(reply));
            }
        }
        // The driver's own cadence closes a completed freeze
        // ([`MatchmakerReconfigurer::close_stop`]); doing it here keeps
        // these tests the shape the driver has, one beat per call.
        r.close_stop();
        steps
    }

    fn set(g: u64, m: &[u64]) -> MatchmakerSet {
        MatchmakerSet::new(MatchmakerGeneration(g), ids(m))
    }

    /// The happy path: stop a quorum, bootstrap all of the successor, decide,
    /// publish — every matchmaker of both generations ends where it should.
    #[test]
    fn a_handover_replaces_one_matchmaker_by_a_spare() {
        let mut pool = pool(4, &[0, 1, 2]);
        let mut r = MatchmakerReconfigurer::new(NodeId(7));
        r.start(&set(0, &[0, 1, 2]), ids(&[0, 1, 3]))
            .expect("start");
        assert!(r.is_busy());
        let steps = exchange(&mut r, &mut pool, &[]);
        // Every member answers the freeze; the quorum-completing ack does
        // not close it, the beat that follows does — so the third answer is
        // still counted, not dropped on a phase that already moved on.
        assert_eq!(
            steps,
            vec![
                ReconfigurerStep::Stopped { remaining: 1 },
                ReconfigurerStep::Stopped { remaining: 0 },
                ReconfigurerStep::Stopped { remaining: 0 },
            ]
        );
        assert!(matches!(r.phase(), ReconfigurerPhase::Bootstrapping { .. }));
        let steps = exchange(&mut r, &mut pool, &[]);
        assert!(matches!(
            steps.last(),
            Some(ReconfigurerStep::Deciding { .. })
        ));
        let steps = exchange(&mut r, &mut pool, &[]);
        assert!(
            steps
                .iter()
                .any(|s| matches!(s, ReconfigurerStep::Proposing { adopted: false, .. }))
        );
        let steps = exchange(&mut r, &mut pool, &[]);
        assert!(steps.iter().any(|s| matches!(s, ReconfigurerStep::Chosen { successor } if *successor == set(1, &[0, 1, 3]))));
        let steps = exchange(&mut r, &mut pool, &[]);
        assert!(
            steps
                .iter()
                .any(|s| matches!(s, ReconfigurerStep::Done { .. }))
        );
        assert!(!r.is_busy());
        for i in [0, 1, 3] {
            assert_eq!(pool[i].phase(), MatchmakerPhase::Active);
            assert_eq!(pool[i].set(), set(1, &[0, 1, 3]));
        }
        assert_eq!(pool[2].phase(), MatchmakerPhase::Stopped);
        assert_eq!(pool[2].successor(), Some(&set(1, &[0, 1, 3])));
    }

    /// Review finding P4, at the reconfigurer: the stall clock moves on
    /// progress and only on progress. A re-sent `Stop` is answered again by
    /// a matchmaker that already froze — the freeze is idempotent — and that
    /// duplicate must be `Ignored`, or a phase whose remaining members are
    /// all dead resets its clock forever and is never abandoned. The
    /// counted-but-short direction is the same fold's other half: it reports
    /// how many acks are still missing.
    #[test]
    fn a_duplicate_ack_is_ignored_and_never_resets_the_stall_clock() {
        let mut pool = pool(5, &[0, 1, 2, 3, 4]);
        let mut r = MatchmakerReconfigurer::new(NodeId(7));
        r.start(&set(0, &[0, 1, 2, 3, 4]), ids(&[0, 1, 2]))
            .expect("start");
        // One freeze: counted, three short of the quorum of five.
        let steps = exchange(&mut r, &mut pool, &[1, 2, 3, 4]);
        assert_eq!(steps, vec![ReconfigurerStep::Stopped { remaining: 2 }]);
        assert_eq!(r.stalled_for(), 0);
        r.tick();
        r.tick();
        assert_eq!(r.stalled_for(), 2);
        // The same matchmaker answers a re-sent `Stop`: idempotent at the
        // registry, and nothing at all here.
        r.resend();
        let steps = exchange(&mut r, &mut pool, &[1, 2, 3, 4]);
        assert!(
            steps.is_empty(),
            "a frozen member is not re-asked: {steps:?}"
        );
        r.on_reply(ReconfigureReply::Stopped {
            matchmaker: MatchmakerId(0),
            generation: MatchmakerGeneration(0),
            gc_watermark: Ballot::zero(),
            history: BTreeMap::new(),
            effective: None,
            successor: None,
            decree_promised: Ballot::zero(),
        });
        assert_eq!(r.stalled_for(), 2, "a duplicate freeze ack is not progress");
        // A second, different member is: the clock restarts.
        r.resend();
        let steps = exchange(&mut r, &mut pool, &[0, 2, 3, 4]);
        assert_eq!(steps, vec![ReconfigurerStep::Stopped { remaining: 1 }]);
        assert_eq!(r.stalled_for(), 0);
    }

    /// The decree's own folds, through the reconfigurer: a promise and a
    /// vote that count toward a quorum still short are `Promised` /
    /// `Accepted` with what is missing, and the publication reports the
    /// learners each set still needs.
    #[test]
    fn a_short_decree_quorum_reports_what_is_missing() {
        let mut pool = pool(6, &[0, 1, 2, 3, 4]);
        let mut r = MatchmakerReconfigurer::new(NodeId(7));
        r.start(&set(0, &[0, 1, 2, 3, 4]), ids(&[0, 1, 5]))
            .expect("start");
        // Freeze a quorum, bootstrap every proposed member, open the decree.
        while !matches!(r.phase(), ReconfigurerPhase::Deciding { .. }) {
            let steps = exchange(&mut r, &mut pool, &[]);
            assert!(!steps.is_empty(), "the handover stalled: {:?}", r.phase());
        }
        // Phase 1 over five acceptors: the first two promises are short.
        let steps = exchange(&mut r, &mut pool, &[1, 2, 3, 4]);
        assert_eq!(steps, vec![ReconfigurerStep::Promised { remaining: 2 }]);
        r.resend();
        let steps = exchange(&mut r, &mut pool, &[0, 2, 3, 4]);
        assert_eq!(steps, vec![ReconfigurerStep::Promised { remaining: 1 }]);
        r.resend();
        let steps = exchange(&mut r, &mut pool, &[0, 1, 3, 4]);
        assert!(matches!(
            steps.as_slice(),
            [ReconfigurerStep::Proposing { .. }]
        ));
        // Phase 2, the same shape.
        let steps = exchange(&mut r, &mut pool, &[1, 2, 3, 4]);
        assert_eq!(steps, vec![ReconfigurerStep::Accepted { remaining: 2 }]);
        r.resend();
        let steps = exchange(&mut r, &mut pool, &[0, 2, 3, 4]);
        assert_eq!(steps, vec![ReconfigurerStep::Accepted { remaining: 1 }]);
        r.resend();
        let steps = exchange(&mut r, &mut pool, &[0, 1, 3, 4]);
        assert!(matches!(
            steps.as_slice(),
            [ReconfigurerStep::Chosen { .. }]
        ));
        // Publishing: one learner of each set at a time.
        let steps = exchange(&mut r, &mut pool, &[1, 2, 3, 4, 5]);
        assert_eq!(
            steps,
            vec![ReconfigurerStep::Published {
                old_remaining: 2,
                new_remaining: 1
            }]
        );
    }

    /// Review finding P7b: a member of the *successor* that only recorded
    /// the chain link is not serving the new generation, and counting it
    /// let a publication finish while no quorum of the new set answered for
    /// it. The generation the learner reports is what decides — which also
    /// makes the count idempotent under a re-sent `Chosen`.
    #[test]
    fn a_learner_that_only_recorded_does_not_count_for_the_successor() {
        let mut pool = pool(4, &[0, 1, 2]);
        let mut r = MatchmakerReconfigurer::new(NodeId(7));
        r.start(&set(0, &[0, 1, 2]), ids(&[0, 1, 3]))
            .expect("start");
        while !matches!(r.phase(), ReconfigurerPhase::Publishing { .. }) {
            let steps = exchange(&mut r, &mut pool, &[]);
            assert!(!steps.is_empty(), "the handover stalled: {:?}", r.phase());
            r.resend();
        }
        let learned = |m: u64, at: u64| ReconfigureReply::Learned {
            matchmaker: MatchmakerId(m),
            generation: MatchmakerGeneration(0),
            activated: at > 0,
            at: MatchmakerGeneration(at),
        };
        // Matchmaker 0 is in both sets but still at generation 0: it counts
        // for the generation it is leaving, never for the one it has not
        // joined.
        assert_eq!(
            r.on_reply(learned(0, 0)),
            ReconfigurerStep::Published {
                old_remaining: 1,
                new_remaining: 2
            }
        );
        assert_eq!(
            r.on_reply(learned(1, 1)),
            ReconfigurerStep::Published {
                old_remaining: 0,
                new_remaining: 1
            }
        );
        assert_eq!(
            r.on_reply(learned(1, 1)),
            ReconfigurerStep::Ignored,
            "a re-sent Chosen a member answers again changes nothing"
        );
        // Matchmaker 0 activates and answers again: now it counts.
        assert_eq!(
            r.on_reply(learned(0, 1)),
            ReconfigurerStep::Done {
                successor: set(1, &[0, 1, 3])
            }
        );
    }

    /// The replacement story: one old matchmaker never answers (lost for
    /// good), the quorum of the other two suffices for every step.
    #[test]
    fn a_dead_matchmaker_is_replaced_without_its_cooperation() {
        let mut pool = pool(4, &[0, 1, 2]);
        let mut r = MatchmakerReconfigurer::new(NodeId(7));
        r.start(&set(0, &[0, 1, 2]), ids(&[0, 1, 3]))
            .expect("start");
        for _ in 0..8 {
            exchange(&mut r, &mut pool, &[2]);
            r.resend();
        }
        assert!(!r.is_busy(), "the handover completed without matchmaker 2");
        assert_eq!(pool[3].set(), set(1, &[0, 1, 3]));
        assert_eq!(
            pool[2].phase(),
            MatchmakerPhase::Active,
            "never told, never stopped"
        );
    }

    /// The headline scenario: two reconfigurers propose incompatible
    /// successors; the decree serializes them and the loser adopts the
    /// winner, so exactly one set becomes authoritative for generation 1.
    #[test]
    fn concurrent_incompatible_proposals_are_serialized_by_the_decree() {
        let mut pool = pool(6, &[0, 1, 2]);
        let mut r1 = MatchmakerReconfigurer::new(NodeId(1));
        let mut r2 = MatchmakerReconfigurer::new(NodeId(2));
        r1.start(&set(0, &[0, 1, 2]), ids(&[0, 1, 3])).expect("r1");
        r2.start(&set(0, &[0, 1, 2]), ids(&[1, 2, 4])).expect("r2");
        // Both freeze and bootstrap.
        exchange(&mut r1, &mut pool, &[]);
        exchange(&mut r2, &mut pool, &[]);
        exchange(&mut r1, &mut pool, &[]);
        exchange(&mut r2, &mut pool, &[]);
        assert!(matches!(r1.phase(), ReconfigurerPhase::Deciding { .. }));
        assert!(matches!(r2.phase(), ReconfigurerPhase::Deciding { .. }));
        // R1 completes Phase 1 and Phase 2 first.
        exchange(&mut r1, &mut pool, &[]);
        let steps = exchange(&mut r1, &mut pool, &[]);
        assert!(
            steps
                .iter()
                .any(|s| matches!(s, ReconfigurerStep::Chosen { .. }))
        );
        // R2's Phase 1 at (1, 2) is nacked by promises at (1, 1)? No: (1,2) > (1,1),
        // so R2 is promised and *learns R1's vote* — P2c makes it propose R1's set.
        let steps = exchange(&mut r2, &mut pool, &[]);
        assert!(
            steps.iter().any(|s| matches!(s, ReconfigurerStep::Proposing { adopted: true, members, .. } if *members == ids(&[0, 1, 3]))),
            "the loser adopts the winner: {steps:?}"
        );
        for _ in 0..4 {
            exchange(&mut r1, &mut pool, &[]);
            exchange(&mut r2, &mut pool, &[]);
            r1.resend();
            r2.resend();
        }
        // One authoritative set for generation 1, everywhere.
        for i in [0, 1, 3] {
            assert_eq!(pool[i].set(), set(1, &[0, 1, 3]));
        }
        assert_eq!(
            pool[4].phase(),
            MatchmakerPhase::Inactive,
            "the losing proposal never activates"
        );
        assert_eq!(pool[2].successor(), Some(&set(1, &[0, 1, 3])));
    }

    /// A reconfigurer that starts after the generation was already replaced
    /// learns the successor from the frozen members and aborts.
    #[test]
    fn a_late_reconfigurer_adopts_the_successor_it_finds() {
        let mut pool = pool(5, &[0, 1, 2]);
        let mut r1 = MatchmakerReconfigurer::new(NodeId(1));
        r1.start(&set(0, &[0, 1, 2]), ids(&[0, 1, 3])).expect("r1");
        for _ in 0..8 {
            exchange(&mut r1, &mut pool, &[]);
            r1.resend();
        }
        assert!(!r1.is_busy());
        let mut r2 = MatchmakerReconfigurer::new(NodeId(2));
        r2.start(&set(0, &[0, 1, 2]), ids(&[2, 4])).expect("r2");
        let steps = exchange(&mut r2, &mut pool, &[]);
        assert!(
            steps.iter().any(|s| matches!(s, ReconfigurerStep::Superseded { successor } if *successor == set(1, &[0, 1, 3]))),
            "{steps:?}"
        );
        assert!(!r2.is_busy());
    }

    /// A Nack reopens the decree strictly above the refusing promise, on the
    /// next re-send.
    #[test]
    fn a_nacked_decree_reopens_above_the_refusing_promise() {
        let mut pool = pool(4, &[0, 1, 2]);
        let mut r = MatchmakerReconfigurer::new(NodeId(1));
        r.start(&set(0, &[0, 1, 2]), ids(&[0, 1, 3]))
            .expect("start");
        // The freeze completes with no decree promise anywhere...
        exchange(&mut r, &mut pool, &[]);
        // ...then a competing proposer promises the acceptors at a high
        // ballot before this reconfigurer's decree opens.
        for mm in pool.iter_mut().take(3) {
            mm.step_reconfigure(ReconfigureRequest::DecreePrepare {
                from: NodeId(9),
                generation: MatchmakerGeneration(0),
                ballot: Ballot {
                    round: 5,
                    node: NodeId(9),
                },
            });
            let ready = mm.ready();
            ready.advance();
        }
        exchange(&mut r, &mut pool, &[]);
        let steps = exchange(&mut r, &mut pool, &[]);
        assert!(
            steps.iter().any(
                |s| matches!(s, ReconfigurerStep::Preempted { promised, .. } if promised.round == 5)
            ),
            "{steps:?}"
        );
        // The reopen is the driver's: only a re-send moves above the promise.
        r.resend();
        assert!(matches!(
            r.phase(),
            ReconfigurerPhase::Deciding { proposer, .. } if proposer.ballot().round == 6
        ));
        for _ in 0..4 {
            exchange(&mut r, &mut pool, &[]);
            r.resend();
        }
        assert!(!r.is_busy());
        assert_eq!(pool[3].set(), set(1, &[0, 1, 3]));
    }

    /// A decree opens strictly above the promises the stop quorum reports:
    /// a fresh incarnation (round counter at zero) whose predecessor took
    /// promises at round 5 opens at 6 without a single Nack — the rule that
    /// keeps a rebooted node from reusing a ballot its earlier incarnation
    /// may have had a value accepted at (the handover model's seed 103).
    #[test]
    fn a_decree_opens_above_the_stop_quorums_promises() {
        let mut pool = pool(4, &[0, 1, 2]);
        // The earlier incarnation of node 1 promised the acceptors at round 5.
        for mm in pool.iter_mut().take(3) {
            mm.step_reconfigure(ReconfigureRequest::DecreePrepare {
                from: NodeId(1),
                generation: MatchmakerGeneration(0),
                ballot: Ballot {
                    round: 5,
                    node: NodeId(1),
                },
            });
            let ready = mm.ready();
            ready.advance();
        }
        // Its fresh incarnation starts from nothing.
        let mut r = MatchmakerReconfigurer::new(NodeId(1));
        r.start(&set(0, &[0, 1, 2]), ids(&[0, 1, 3]))
            .expect("start");
        exchange(&mut r, &mut pool, &[]);
        let steps = exchange(&mut r, &mut pool, &[]);
        assert!(
            steps
                .iter()
                .any(|s| matches!(s, ReconfigurerStep::Deciding { ballot } if ballot.round == 6)),
            "the decree opens above the reported promises: {steps:?}"
        );
        let steps = exchange(&mut r, &mut pool, &[]);
        assert!(
            !steps
                .iter()
                .any(|s| matches!(s, ReconfigurerStep::Preempted { .. })),
            "no Nack: {steps:?}"
        );
        for _ in 0..4 {
            exchange(&mut r, &mut pool, &[]);
            r.resend();
        }
        assert!(!r.is_busy());
        assert_eq!(pool[3].set(), set(1, &[0, 1, 3]));
    }
}
