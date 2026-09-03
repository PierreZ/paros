//! The matchmaker's **generation machine** (#125): the five-step handover
//! that replaces one matchmaker set by the next, seen from the matchmaker
//! side.
//!
//! `Stop` freezes a generation and reports its registry, `Bootstrap` holds a
//! proposed successor's reconstruction durably, `DecreePrepare` and
//! `DecreeAccept` are this matchmaker's acceptor half of the single-decree
//! Paxos instance that *chooses* the successor, and `Chosen` is the learner
//! notification that records the chain link or activates the pending
//! bootstrap. The reconfigurer half — the proposer that drives all five — is
//! [`super::MatchmakerReconfigurer`].
//!
//! Every arm is fenced by generation and phase, and a refusal names what this
//! matchmaker knows so a stale or ahead reconfigurer can adopt or abort.

use super::{
    Matchmaker, MatchmakerPhase, MatchmakerWriteOp, PendingBootstrap, ReconfigureReply,
    ReconfigureRequest, Registration,
};
use crate::membership::{MatchmakerGeneration, MatchmakerId, MatchmakerSet};
use crate::single_decree::DecreeAcceptor;
use crate::types::Ballot;
use std::collections::BTreeMap;

impl Matchmaker {
    /// Answer one reconfiguration message (the module doc's *Generations*).
    /// Every arm is fenced by generation and phase; a refusal names what this
    /// matchmaker knows so a stale or ahead reconfigurer can adopt or abort.
    ///
    /// # Panics
    ///
    /// If processing exposes a broken internal invariant.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(matchmaker = self.config.id.0, from = request.from().0)))]
    pub fn step_reconfigure(&mut self, request: ReconfigureRequest) {
        self.assert_invariants();
        let reply = match request {
            ReconfigureRequest::Stop { generation, .. } => self.on_stop(generation),
            ReconfigureRequest::Bootstrap { bootstrap, .. } => self.on_bootstrap(bootstrap),
            ReconfigureRequest::DecreePrepare {
                generation, ballot, ..
            } => self.on_decree_prepare(generation, ballot),
            ReconfigureRequest::DecreeAccept {
                generation,
                ballot,
                members,
                ..
            } => self.on_decree_accept(generation, ballot, members),
            ReconfigureRequest::Chosen {
                generation,
                successor,
                ..
            } => self.on_chosen(generation, successor),
        };
        self.pending_reconfigure_replies.push(reply);
        self.assert_invariants();
    }

    /// The refusal every arm falls back to: what this matchmaker is, where it
    /// stands, and the successor it knows of.
    fn refusal(&self) -> ReconfigureReply {
        ReconfigureReply::Refused {
            matchmaker: self.config.id,
            current: self.set(),
            phase: self.phase(),
            successor: self.hard_state.successor.clone(),
        }
    }

    /// `StopA`: freeze `generation` (durably, before the answer leaves) and
    /// report the registry the successor is reconstructed from.
    fn on_stop(&mut self, generation: MatchmakerGeneration) -> ReconfigureReply {
        let me = self.config.id;
        let current = self.set();
        let phase = self.phase();
        if current.generation != generation
            || !matches!(phase, MatchmakerPhase::Active | MatchmakerPhase::Stopped)
        {
            self.refusal()
        } else {
            if phase == MatchmakerPhase::Active {
                // The freeze, durable before the answer leaves: a
                // matchmaker that forgot it stopped and resumed
                // registering would break the reconstruction the
                // successor rests on.
                self.freeze();
            }
            ReconfigureReply::Stopped {
                matchmaker: me,
                generation,
                gc_watermark: self.hard_state.gc_watermark,
                history: self.history_from_watermark(),
                successor: self.hard_state.successor.clone(),
                decree_promised: self.hard_state.decree.promised,
            }
        }
    }

    /// Hold a proposed successor's reconstruction durably, pending its decree.
    fn on_bootstrap(&mut self, bootstrap: PendingBootstrap) -> ReconfigureReply {
        let me = self.config.id;
        let current = self.set();
        let set = bootstrap.set.clone();
        // Wire hygiene: a proposal this matchmaker is not in, one
        // that would not move it forward, or one that cannot admit
        // the quorum system is refused whole. So is a competing
        // proposal for a generation already settled here: once a
        // successor is recorded, nothing else at its generation can
        // ever be chosen, and storing it would keep a whole registry
        // copy durable forever. The refusal names the successor, so
        // the stale reconfigurer adopts it instead.
        let settled = self
            .hard_state
            .successor
            .as_ref()
            .is_some_and(|s| set.generation <= s.generation && set != *s);
        if !set.contains(me)
            || set.generation <= current.generation
            || !set.is_well_formed()
            || settled
        {
            self.refusal()
        } else {
            // Keyed by the proposed set: two reconfigurers may
            // bootstrap this matchmaker into two different proposed
            // successors of one generation, and only the chosen one
            // activates. Idempotent for the same set (a resent
            // bootstrap from the same reconstruction).
            if let Some(existing) = self.hard_state.pending.iter_mut().find(|p| p.set == set) {
                if *existing != bootstrap {
                    // A different reconstruction of the same
                    // proposal (two reconfigurers, two frozen
                    // quorums): the later one supersedes, whole.
                    *existing = bootstrap;
                    self.stage_scalars();
                }
            } else {
                self.hard_state.pending.push(bootstrap);
                self.stage_scalars();
            }
            ReconfigureReply::Bootstrapped {
                matchmaker: me,
                set,
            }
        }
    }

    /// Phase 1b of the successor decree.
    fn on_decree_prepare(
        &mut self,
        generation: MatchmakerGeneration,
        ballot: Ballot,
    ) -> ReconfigureReply {
        let me = self.config.id;
        let current = self.set();
        let phase = self.phase();
        if current.generation != generation
            || !matches!(phase, MatchmakerPhase::Active | MatchmakerPhase::Stopped)
        {
            self.refusal()
        } else {
            let before = self.hard_state.decree.promised;
            match self.hard_state.decree.prepare(ballot) {
                Ok(vote) => {
                    if self.hard_state.decree.promised != before {
                        self.stage_scalars();
                    }
                    ReconfigureReply::Promised {
                        matchmaker: me,
                        generation,
                        ballot,
                        vote,
                    }
                }
                Err(promised) => ReconfigureReply::Nacked {
                    matchmaker: me,
                    generation,
                    ballot,
                    promised,
                },
            }
        }
    }

    /// Phase 2b of the successor decree.
    fn on_decree_accept(
        &mut self,
        generation: MatchmakerGeneration,
        ballot: Ballot,
        members: Vec<MatchmakerId>,
    ) -> ReconfigureReply {
        let me = self.config.id;
        let current = self.set();
        let phase = self.phase();
        if current.generation != generation
            || !matches!(phase, MatchmakerPhase::Active | MatchmakerPhase::Stopped)
        {
            self.refusal()
        } else {
            let mut members = members;
            members.sort_unstable();
            members.dedup();
            match self.hard_state.decree.accept(ballot, members) {
                Ok(()) => {
                    self.stage_scalars();
                    ReconfigureReply::Accepted {
                        matchmaker: me,
                        generation,
                        ballot,
                    }
                }
                Err(promised) => ReconfigureReply::Nacked {
                    matchmaker: me,
                    generation,
                    ballot,
                    promised,
                },
            }
        }
    }

    /// The learner notification: record the chain link, activate a pending
    /// bootstrap, or refuse (see the *Trust boundary of `Chosen`* section of
    /// the module doc).
    fn on_chosen(
        &mut self,
        generation: MatchmakerGeneration,
        successor: MatchmakerSet,
    ) -> ReconfigureReply {
        let me = self.config.id;
        let current = self.set();
        let phase = self.phase();
        // A learner notification (see the type doc on
        // `ReconfigureRequest::Chosen`): the matchmaker does not
        // re-derive the decision, it applies what a proposer that
        // held the Phase-2 quorum tells it — after the wire checks
        // any learner makes (the generation chain and a set that
        // admits the quorum system).
        let successor = MatchmakerSet::new(successor.generation, successor.members);
        if successor.generation != generation.next() || !successor.is_well_formed() {
            self.refusal()
        } else if current.generation == generation
            && matches!(phase, MatchmakerPhase::Active | MatchmakerPhase::Stopped)
        {
            if self
                .hard_state
                .successor
                .as_ref()
                .is_some_and(|recorded| *recorded != successor)
            {
                // Two different successors for one generation: one of
                // the two publications is wrong. A learner cannot tell
                // which, but it can refuse to apply the second — the
                // refusal names the successor recorded, so the
                // contradiction reaches the sender (and any audit)
                // instead of disappearing into an ordinary `Learned`.
                self.refusal()
            } else {
                // A member of the succeeded generation: record the
                // chain link (freezing if it had not — once a
                // successor is chosen the generation is over), then
                // activate if it is also a member of the successor
                // holding its bootstrap.
                let mut changed = false;
                if self.hard_state.successor.is_none() {
                    if phase == MatchmakerPhase::Active {
                        self.freeze();
                    }
                    self.hard_state.successor = Some(successor.clone());
                    changed = true;
                }
                changed |= self.prune_settled_pending(&successor);
                if changed {
                    self.stage_scalars();
                }
                let activated = self.activate(&successor);
                ReconfigureReply::Learned {
                    matchmaker: me,
                    generation,
                    activated,
                }
            }
        } else if successor.generation > current.generation
            || matches!(phase, MatchmakerPhase::Inactive | MatchmakerPhase::Fresh)
        {
            // Not a member of the succeeded generation (a spare, or
            // frozen further back): only an activation can apply —
            // but the decision settles this matchmaker's losing
            // bootstraps either way.
            let pruned = self.prune_settled_pending(&successor);
            let activated = self.activate(&successor);
            if activated {
                ReconfigureReply::Learned {
                    matchmaker: me,
                    generation,
                    activated,
                }
            } else {
                if pruned {
                    self.stage_scalars();
                }
                self.refusal()
            }
        } else {
            self.refusal()
        }
    }

    /// Freeze the current generation (durable before any reply).
    fn freeze(&mut self) {
        let set = self.set();
        self.hard_state.generation = set.generation;
        self.hard_state.members = set.members;
        self.hard_state.phase = MatchmakerPhase::Stopped;
        self.stage_scalars();
    }

    /// Activate `successor` if this matchmaker is one of its members holding
    /// its pending bootstrap and stands strictly below it: the reconstructed
    /// registry replaces the current one whole, the watermark becomes the
    /// reconstructed one (never lower than the one held — the maximum over a
    /// frozen quorum that this matchmaker, if it was in it, contributed to),
    /// the decree record resets for the new generation, and every pending
    /// bootstrap at or below the new generation is dropped.
    fn activate(&mut self, successor: &MatchmakerSet) -> bool {
        if !successor.contains(self.config.id) || successor.generation <= self.set().generation {
            return false;
        }
        assert!(
            successor.is_well_formed(),
            "an activated matchmaker set admits the matchmaker quorum system"
        );
        let Some(index) = self
            .hard_state
            .pending
            .iter()
            .position(|p| p.set == *successor)
        else {
            return false;
        };
        let bootstrap = self.hard_state.pending.remove(index);
        // The activated watermark is the maximum of the reconstructed one
        // and this matchmaker's own. Both are legitimate floors over the
        // reconstructed registry: the reconstructed one is the maximum over
        // a frozen quorum of `M_g`, and the local one was raised only by a
        // `GarbageA` whose leader had established the §3.5 preconditions —
        // "everything below `i` may be forgotten" is a fact about the
        // cluster, not about the matchmaker that happened to hear it first,
        // so applying it to the reconstruction forgets nothing a future
        // Phase 1 can need. What the local floor can never do is *add*
        // knowledge: the registry installed is exactly the reconstructed
        // history at or above the higher floor, and nothing else.
        let local = self.hard_state.gc_watermark;
        let watermark = bootstrap.gc_watermark.max(local);
        let registry: BTreeMap<Ballot, Registration> = bootstrap
            .history
            .into_iter()
            .filter(|(b, _)| *b >= watermark)
            .collect();
        assert!(
            watermark >= bootstrap.gc_watermark && watermark >= local,
            "an activation never lowers either watermark it inherits"
        );
        assert!(
            registry.keys().all(|b| *b >= watermark),
            "an activated registry holds nothing below the activated watermark"
        );
        self.hard_state.generation = successor.generation;
        self.hard_state.members.clone_from(&successor.members);
        self.hard_state.phase = MatchmakerPhase::Active;
        self.hard_state.successor = None;
        self.hard_state.decree = DecreeAcceptor::default();
        self.hard_state.gc_watermark = watermark;
        self.hard_state
            .pending
            .retain(|p| p.set.generation > successor.generation);
        self.registry = registry.clone();
        // One write for the whole activation: a crash between "registry
        // replaced" and "scalars advanced" would boot a matchmaker answering
        // the wrong generation from the wrong registry.
        self.pending_writes
            .push(MatchmakerWriteOp::InstallRegistry {
                scalars: self.hard_state.clone(),
                registrations: registry,
            });
        true
    }

    /// Drop every pending bootstrap the chosen `successor` settles: a
    /// proposal at or below the successor's generation that is not the
    /// successor itself lost its decree and can never be activated. Keeping
    /// one is not free — the pending list holds a whole reconstructed
    /// registry per proposal and rides inside every later
    /// [`MatchmakerWriteOp::SetScalars`], so an unpruned loser makes every
    /// freeze, promise and vote of every later generation more expensive,
    /// forever. Returns whether anything was dropped.
    fn prune_settled_pending(&mut self, successor: &MatchmakerSet) -> bool {
        let before = self.hard_state.pending.len();
        self.hard_state
            .pending
            .retain(|p| p.set.generation > successor.generation || p.set == *successor);
        self.hard_state.pending.len() != before
    }

    /// Stage a whole-scalars write.
    fn stage_scalars(&mut self) {
        self.pending_writes
            .push(MatchmakerWriteOp::SetScalars(self.hard_state.clone()));
    }
}
