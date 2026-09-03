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
use crate::single_decree::{DecreePhase, DecreeProposer};
use crate::types::{Ballot, NodeId};

/// Why [`MatchmakerReconfigurer::start`] refused a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartRefusal {
    /// A reconfiguration is already running here.
    Busy,
    /// The target names no matchmaker.
    Empty,
    /// The target does not admit the matchmaker quorum system
    /// ([`MatchmakerSet::is_well_formed`]).
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
    /// The quorum froze: reconstructed and bootstrapping.
    Bootstrapping {
        /// The reconstruction sent to the proposed members.
        bootstrap: PendingBootstrap,
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

/// The reconfigurer handle: one running handover at most, its outbound
/// requests drained by the driver through [`Self::take_requests`].
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

    /// Requests queued since the last drain, in order.
    pub fn take_requests(&mut self) -> Vec<(MatchmakerId, ReconfigureRequest)> {
        std::mem::take(&mut self.pending)
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
    /// for an empty target.
    pub fn start(
        &mut self,
        current: &MatchmakerSet,
        target: Vec<MatchmakerId>,
    ) -> Result<(), StartRefusal> {
        if self.is_busy() {
            return Err(StartRefusal::Busy);
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
    /// [`StartRefusal::Busy`] while a handover runs.
    pub fn finish(&mut self, current: &MatchmakerSet) -> Result<(), StartRefusal> {
        if self.is_busy() {
            return Err(StartRefusal::Busy);
        }
        self.phase = ReconfigurerPhase::Stopping {
            old: current.clone(),
            target: None,
            acks: BTreeMap::new(),
            decree_floor: Ballot::zero(),
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
                target,
                acks,
                decree_floor,
            } => {
                let ReconfigureReply::Stopped {
                    generation,
                    gc_watermark,
                    history,
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
                acks.insert(from, (gc_watermark, history));
                *decree_floor = (*decree_floor).max(decree_promised);
                let quorum = old.quorum_size();
                if acks.len() < quorum {
                    return ReconfigurerStep::Stopped {
                        remaining: quorum - acks.len(),
                    };
                }
                // The reconstruction (§5): the maximum watermark, and the
                // union of every frozen registry at or above it. A ballot
                // reported twice carries one registration (the write-once
                // ledger); the first seen is kept.
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
                // A finish proposes the members that answered the freeze.
                let target = target
                    .clone()
                    .unwrap_or_else(|| acks.keys().copied().collect());
                let bootstrap = PendingBootstrap {
                    set: MatchmakerSet::new(old.generation.next(), target),
                    gc_watermark,
                    history,
                };
                assert!(
                    bootstrap
                        .history
                        .keys()
                        .all(|b| *b >= bootstrap.gc_watermark),
                    "a reconstruction holds nothing below its watermark"
                );
                // A proposed successor admits its quorum system: a `start`
                // refused a malformed target, and a `finish` proposes the
                // members that answered the freeze — a quorum of the old
                // set, never fewer.
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
                self.resend();
                ReconfigurerStep::Bootstrapping { bootstrap }
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
                if set != bootstrap.set || !set.contains(from) {
                    return ReconfigurerStep::Ignored;
                }
                acks.insert(from);
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
                        let Some(members) = proposer.on_promise(from, vote) else {
                            return ReconfigurerStep::Ignored;
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
                        let Some(members) = proposer.on_accepted(from) else {
                            return ReconfigurerStep::Ignored;
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
                let ReconfigureReply::Learned { generation, .. } = reply else {
                    return ReconfigurerStep::Ignored;
                };
                if generation != old.generation {
                    return ReconfigurerStep::Ignored;
                }
                if old.contains(from) {
                    old_acks.insert(from);
                }
                if successor.contains(from) {
                    new_acks.insert(from);
                }
                if old_acks.len() >= old.quorum_size() && new_acks.len() >= successor.quorum_size()
                {
                    let successor = successor.clone();
                    self.phase = ReconfigurerPhase::Idle;
                    self.pending.clear();
                    return ReconfigurerStep::Done { successor };
                }
                ReconfigurerStep::Ignored
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
    use crate::matchmaker::{Matchmaker, MatchmakerConfig, MatchmakerHardState, RegistryStorage};
    use crate::matchmaker::{MatchmakerGeneration, MatchmakerPhase};

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
        for (to, request) in r.take_requests() {
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
        assert!(matches!(
            steps[0],
            ReconfigurerStep::Stopped { remaining: 1 }
        ));
        assert!(matches!(steps[1], ReconfigurerStep::Bootstrapping { .. }));
        // The third stop answer lands on a bootstrapping reconfigurer: ignored.
        assert!(matches!(steps[2], ReconfigurerStep::Ignored));
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
