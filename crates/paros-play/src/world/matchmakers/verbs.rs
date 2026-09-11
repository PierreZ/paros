//! The operator's matchmaker-plane verbs, and the reads a goal makes of them:
//! crashing and restarting a matchmaker, asking a leader to change the acceptor
//! set, retiring a released acceptor on the evidence, driving a matchmaker-set
//! handover, and the three re-sends.

use paros_core::{
    AcceptorConfig, Ballot, ColocatedNode, MatchmakerId, NodeId, ReconfigureRefusal,
    ReconfigureResult, StartRefusal,
};

use crate::action::{ActionError, ActionErrorCode};
use crate::narration::{NarrationKind, many, who};
use crate::view::show_ballot;
use crate::world::drain::Paused;
use crate::world::matchmakers::{MatchmakerProcess, show_members, show_set, which};
use crate::world::{World, unknown_node};

impl World {
    /// The matchmakers this level deployed, in id order. Empty on a plain
    /// deployment, which is the whole of Act I to Act III.
    #[must_use]
    pub fn matchmakers(&self) -> &[MatchmakerProcess] {
        &self.matchmakers
    }

    /// The matchmaker with `id`, if this level deployed one.
    #[must_use]
    pub fn matchmaker(&self, id: MatchmakerId) -> Option<&MatchmakerProcess> {
        self.matchmakers.iter().find(|m| m.id() == id)
    }

    /// Whether an operator retired `id`: it answered the evidence, shut down,
    /// and it never comes back.
    #[must_use]
    pub fn retired(&self, id: NodeId) -> bool {
        self.index_of(id).is_some_and(|index| self.retired[index])
    }

    /// Every retire request the engine refused for want of evidence:
    /// `(node, the watermark the operator showed)`.
    #[must_use]
    pub fn refused_retires(&self) -> &[(NodeId, Ballot)] {
        &self.refused_retires
    }

    /// Every retirement that went through: `(node, the watermark it retired
    /// on)`. A goal reads it to insist the shutdown rested on a floor a
    /// leadership really made effective, rather than on a number.
    #[must_use]
    pub fn retirements(&self) -> &[(NodeId, Ballot)] {
        &self.retirements
    }

    /// Every handover a node gave up because it made no progress for the
    /// level's stall timeout, in the order they were given up. Nothing is lost
    /// when one is: the freeze and the votes are on the matchmakers' own
    /// disks, and the next node that meets the frozen generation finishes it.
    #[must_use]
    pub fn abandoned_handovers(&self) -> &[NodeId] {
        &self.abandoned_handovers
    }

    /// The garbage-collection floor `id`'s leadership made effective, and the
    /// acceptors it released. `None` until a matchmaker quorum has acked it.
    #[must_use]
    pub fn gc_effective(&self, id: NodeId) -> Option<(Ballot, Vec<NodeId>)> {
        self.node(id)
            .and_then(ColocatedNode::gc_effective)
            .map(|(watermark, retired)| (watermark, retired.to_vec()))
    }

    /// Whether some live node reports `watermark` as a floor its own
    /// leadership made effective.
    ///
    /// This is where a retirement's evidence comes from, and the only place it
    /// may come from. [`ColocatedNode::may_retire`] asks one question of the
    /// number it is handed — is it above every ballot that bound a
    /// configuration naming me? — and an operator who typed a large enough
    /// number would pass it. The core's own contract says the operator reads
    /// the watermark off a leader whose garbage collection reached a
    /// matchmaker quorum, so the engine makes the operator show that: a
    /// watermark no leadership reports is not evidence of anything.
    #[must_use]
    pub fn reports_gc_floor(&self, watermark: Ballot) -> bool {
        self.pool
            .iter()
            .copied()
            .any(|id| self.gc_effective(id).is_some_and(|(at, _)| at == watermark))
    }

    /// Every acceptor configuration a node was told about before it opened its
    /// Phase 1 — `H_b`, recorded when the matchmaking phase completed.
    #[must_use]
    pub fn prior_configurations(&self, id: NodeId) -> Vec<AcceptorConfig> {
        self.index_of(id)
            .map(|index| self.campaign_prior[index].clone())
            .unwrap_or_default()
    }

    pub(super) fn matchmaker_index(&self, id: MatchmakerId) -> Result<usize, ActionError> {
        self.matchmakers
            .iter()
            .position(|m| m.id() == id)
            .ok_or_else(|| {
                ActionError::new(
                    ActionErrorCode::UnknownParty,
                    format!("there is no matchmaker {} in this level", id.0),
                )
            })
    }

    /// Turn a player's membership and quorum system into a configuration the
    /// core will accept, or refuse it with a reason.
    ///
    /// [`AcceptorConfig::new`] **asserts** an empty membership and one that
    /// does not admit its quorum system, and an assert in wasm is an abort
    /// with no stack. So every one of those is checked here first, and the
    /// player reads a sentence.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the set is not one this cluster can run.
    pub fn compose(
        &self,
        members: &[u64],
        system: paros_core::QuorumSystem,
    ) -> Result<AcceptorConfig, ActionError> {
        if members.is_empty() {
            return Err(ActionError::new(
                ActionErrorCode::BadReach,
                "an acceptor set names at least one acceptor",
            ));
        }
        let mut ids: Vec<NodeId> = members.iter().map(|id| NodeId(*id)).collect();
        ids.sort_unstable();
        ids.dedup();
        for id in &ids {
            if self.index_of(*id).is_none() {
                return Err(unknown_node(*id));
            }
        }
        if !system.admits(ids.len()) {
            return Err(ActionError::new(
                ActionErrorCode::BadReach,
                format!(
                    "{} acceptors do not admit that quorum system: every configuration must have \
                     a Phase-1 quorum and a Phase-2 quorum that meet",
                    ids.len()
                ),
            ));
        }
        Ok(AcceptorConfig::new(ids, system))
    }

    /// Drop `id`'s volatile role; its registry survives.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn crash_matchmaker(&mut self, id: MatchmakerId) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.matchmaker_index(id)?;
        if !self.matchmakers[index].alive() {
            return Err(ActionError::new(
                ActionErrorCode::NodeCrashed,
                format!("matchmaker {} is already crashed", id.0),
            ));
        }
        let held = self.matchmakers[index].disk().registrations().len();
        let watermark = self.matchmakers[index].disk().hard_state().gc_watermark;
        self.matchmakers[index].crash();
        self.narrate(
            NarrationKind::Crash,
            format!(
                "{} crashes. Its registry keeps {} and the watermark {}. A matchmaker answers a \
                 candidate only after the write is on the disk, so everything it ever promised is \
                 still there.",
                which(id),
                many(held, "registration"),
                show_ballot(watermark)
            ),
        );
        Ok(())
    }

    /// Rebuild a crashed matchmaker from its registry.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn restart_matchmaker(&mut self, id: MatchmakerId) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.matchmaker_index(id)?;
        if self.matchmakers[index].alive() {
            return Err(ActionError::new(
                ActionErrorCode::NodeAlive,
                format!("matchmaker {} is already running", id.0),
            ));
        }
        self.matchmakers[index].reboot();
        let held = self.matchmakers[index].disk().registrations().len();
        let generation = self.matchmakers[index].disk().hard_state().generation;
        self.narrate(
            NarrationKind::Restart,
            format!(
                "{} restarts from its registry: {}, generation {}. The role is built from the \
                 disk and from nothing else.",
                which(id),
                many(held, "registration"),
                generation.0
            ),
        );
        Ok(())
    }

    /// A client asks the leader at `id` to put `config` in force.
    ///
    /// A reconfiguration is a **round change**: the leader moves to a fresh
    /// ballot registered with the new configuration, and leads again under it
    /// once the cross-configuration Phase 1 completes. Every refusal is an
    /// operating condition the caller retries, and a cluster with no
    /// matchmakers refuses every one of them.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn reconfigure(&mut self, id: NodeId, config: &AcceptorConfig) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.require_live(id)?;
        let members = show_members(config.members());
        let mark = self.narration.len();
        let requested = config.clone();
        let result = self.observe(id, move |world| {
            let out = world.nodes[index]
                .as_mut()
                .map(|node| node.reconfigure(&requested));
            world.pump(id);
            out
        });
        let text = match result {
            Some(ReconfigureResult::Started(ballot)) => format!(
                "A client asks {} to run with the acceptors {members}. The leader does not edit \
                 the set it has. It opens a fresh ballot, {}, and registers the new set with the \
                 matchmakers under that ballot. A configuration belongs to one ballot, so the \
                 only way to change it is a new ballot. The leader proposes nothing new until it \
                 holds that new ballot.",
                who(id),
                show_ballot(ballot)
            ),
            Some(ReconfigureResult::NotLeader(hint)) => {
                self.narration.truncate(mark);
                return Err(ActionError::new(
                    ActionErrorCode::NotLeader,
                    match hint {
                        Some(leader) => format!(
                            "node {} is not the leader; a reconfiguration goes to node {}",
                            id.0, leader.0
                        ),
                        None => format!(
                            "node {} is not the leader, and it does not know who is",
                            id.0
                        ),
                    },
                ));
            }
            Some(ReconfigureResult::Refused(refusal)) => {
                self.reconfigure_refused(id, refusal, &members);
                let opening = crate::narration::say(
                    NarrationKind::Reconfigure,
                    format!(
                        "A client asks {} to run with the acceptors {members}, and the leader \
                         refuses.",
                        who(id)
                    ),
                );
                self.narration.insert(mark, opening);
                return Ok(());
            }
            None => format!("{} is not running.", who(id)),
        };
        let opening = crate::narration::say(NarrationKind::Reconfigure, text);
        self.narration.insert(mark, opening);
        Ok(())
    }

    /// Say what a refused reconfiguration was refused for.
    fn reconfigure_refused(&mut self, id: NodeId, refusal: ReconfigureRefusal, members: &str) {
        let text = match refusal {
            ReconfigureRefusal::NoMatchmakers => format!(
                "{} names no matchmakers. Plain Multi-Paxos has one fixed acceptor set for the \
                 life of the cluster. There is nowhere to record a second set, so a later leader \
                 could not learn that {members} ever existed. The request is refused, and it is \
                 not queued.",
                who(id)
            ),
            ReconfigureRefusal::Unchanged => {
                format!("{members} is the acceptor set already in force.")
            }
            ReconfigureRefusal::UnknownMember => format!(
                "{members} names a node this deployment cannot reach. A new acceptor must be in \
                 the pool before it can be added to a configuration."
            ),
            ReconfigureRefusal::Malformed => format!(
                "{members} does not admit the quorum system asked for. Every configuration must \
                 have a Phase-1 quorum and a Phase-2 quorum that meet."
            ),
            ReconfigureRefusal::Unsettled => format!(
                "{} still has Phase-1 work open: a slot to settle, a damaged record to repair, or \
                 an application prefix to complete. A reconfiguration moves a settled leadership. \
                 Ask again once the recovery closes.",
                who(id)
            ),
            ReconfigureRefusal::RoundExhausted => {
                "there is no higher ballot left to move to.".to_string()
            }
        };
        self.narrate(NarrationKind::Reconfigure, text);
    }

    /// An operator asks `target` to retire, showing the effective watermark it
    /// read from `id`'s report.
    ///
    /// The evidence is what makes this safe. "I am not in the configuration in
    /// force" is a **belief** — it is volatile, and a reboot puts the node back
    /// on its bootstrap configuration — so a node that answered on that belief
    /// alone could shut down while a configuration it is still needed for is
    /// alive. A watermark strictly above every ballot a configuration naming
    /// this node was bound to is a **fact**: a matchmaker quorum durably
    /// refuses every campaign that could still ask for it.
    ///
    /// The number itself is not the fact. A watermark that no live node
    /// reports as a floor its own leadership made effective
    /// ([`World::reports_gc_floor`]) is refused before the target is asked
    /// anything, because the operator is supposed to read that number off a
    /// leader's report and nowhere else.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn retire(
        &mut self,
        id: NodeId,
        target: NodeId,
        watermark: Ballot,
    ) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        self.require_live(id)?;
        let index = self.require_live(target)?;
        let effective = self.gc_effective(id).map(|(ballot, _)| ballot);
        let evidenced = self.reports_gc_floor(watermark);
        if let Some(prompt) = self.may_retire_prompt(target, index, watermark, effective, evidenced)
        {
            self.narrate(
                NarrationKind::Gc,
                format!(
                    "An operator asks {} to shut down permanently. {} You answer for it.",
                    who(target),
                    prompt.question
                ),
            );
            self.prompt = Some(prompt);
            self.paused = Some(Paused::Retire { target, watermark });
            return Ok(());
        }
        if !evidenced {
            return Err(ActionError::new(
                ActionErrorCode::NoEvidence,
                format!(
                    "no leader reports the floor {}, so node {} is not asked to retire. An \
                     operator reads a floor from a leader that made it effective, and a \
                     matchmaker quorum wrote that floor to disk. A number from another source is \
                     not evidence.",
                    show_ballot(watermark),
                    target.0
                ),
            ));
        }
        self.retire_now(target, watermark);
        Ok(())
    }

    /// Honour or refuse the retirement, once nobody owes an answer for it.
    pub(in crate::world) fn retire_now(&mut self, target: NodeId, watermark: Ballot) {
        let Some(index) = self.index_of(target) else {
            return;
        };
        let evidenced = self.reports_gc_floor(watermark);
        let may = evidenced
            && self.nodes[index]
                .as_ref()
                .is_some_and(|node| node.may_retire(watermark));
        if !may {
            self.refused_retires.push((target, watermark));
            let text = if evidenced {
                format!(
                    "{} refuses to retire, and the reason is \"not collected\". The watermark it \
                     was shown, {}, is not above every ballot a configuration naming this node \
                     was bound to. An installed successor is not a collected predecessor. Some \
                     future leader may still need this node's Phase-1 promise, until a \
                     matchmaker quorum durably refuses those ballots.",
                    who(target),
                    show_ballot(watermark)
                )
            } else {
                format!(
                    "{} refuses to retire: no leader reports the floor {}. An operator reads a \
                     floor from a leader that made it effective. A number that no leader reports \
                     is not evidence. A node that stops on such a number can still hold the one \
                     promise that a later Phase 1 needs.",
                    who(target),
                    show_ballot(watermark)
                )
            };
            self.narrate(NarrationKind::Gc, text);
            return;
        }
        self.retirements.push((target, watermark));
        self.nodes[index] = None;
        self.armed_seams[index] = None;
        self.retired[index] = true;
        self.narrate(
            NarrationKind::Gc,
            format!(
                "{} retires. It is not in the acceptor set in force, and it does not lead. The \
                 watermark {} sits above every ballot a configuration naming it was bound to. No \
                 future leader can ask it for a promise, so it may stop permanently.",
                who(target),
                show_ballot(watermark)
            ),
        );
        self.settle();
    }

    /// A node drives a handover of the matchmaker set onto `members`.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn reconfigure_matchmakers(
        &mut self,
        id: NodeId,
        members: Vec<MatchmakerId>,
    ) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.require_live(id)?;
        let Some(current) = self
            .node(id)
            .and_then(ColocatedNode::matchmaker_set)
            .cloned()
        else {
            return Err(ActionError::new(
                ActionErrorCode::NoMatchmakers,
                format!(
                    "node {} names no matchmakers, so there is no matchmaker set to replace",
                    id.0
                ),
            ));
        };
        for member in &members {
            if self.matchmaker(*member).is_none() {
                return Err(ActionError::new(
                    ActionErrorCode::UnknownParty,
                    format!("there is no matchmaker {} in this level", member.0),
                ));
            }
        }
        let proposed = show_set(&members);
        match self.reconfigurers[index].start(&current, members) {
            Ok(()) => {}
            Err(StartRefusal::Busy) => {
                return Err(ActionError::new(
                    ActionErrorCode::HandoverBusy,
                    format!("node {} is already driving a handover", id.0),
                ));
            }
            Err(StartRefusal::Empty) => {
                return Err(ActionError::new(
                    ActionErrorCode::UnknownParty,
                    "a matchmaker set names at least one matchmaker".to_string(),
                ));
            }
        }
        self.narrate(
            NarrationKind::Generation,
            format!(
                "{} starts a handover of the matchmaker set: generation {} = {} is replaced by \
                 {}. The first step freezes the old generation. A frozen matchmaker registers \
                 nothing more, so the copy taken next cannot change while it is read.",
                who(id),
                current.generation.0,
                show_set(current.members()),
                proposed
            ),
        );
        self.queue_reconfigurer(index);
        Ok(())
    }

    /// Re-queue a candidate's open registration toward every matchmaker that
    /// has not answered. Skipping is always safe: a matchmaker answers a
    /// repeated request from its own registry, and a campaign that never
    /// completes is abandoned at the next election timeout.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn resend_matchmaking(&mut self, id: NodeId) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.require_live(id)?;
        self.narrate(
            NarrationKind::Matchmaking,
            format!(
                "{} asks the matchmakers that have not answered again. A matchmaker answers the \
                 same request the same way, and writes nothing a second time.",
                who(id)
            ),
        );
        self.observe(id, move |world| {
            if let Some(node) = world.nodes[index].as_mut() {
                node.resend_matchmaking();
            }
            world.pump(id);
        });
        Ok(())
    }

    /// Re-queue a leader's open garbage-collection request. Skipping is always
    /// safe: a floor that never rises costs unbounded histories, never safety.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn resend_gc(&mut self, id: NodeId) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.require_live(id)?;
        self.narrate(
            NarrationKind::Gc,
            format!(
                "{} asks the matchmakers that have not acked the floor again.",
                who(id)
            ),
        );
        self.observe(id, move |world| {
            if let Some(node) = world.nodes[index].as_mut() {
                node.resend_gc();
            }
            world.pump(id);
        });
        Ok(())
    }

    /// Re-issue a handover's current step, and close a freeze whose quorum has
    /// answered.
    ///
    /// Closing on this cadence rather than on the ack that completed the
    /// quorum is deliberate: a quorum is the floor the reconstruction rests on,
    /// and every straggler that arrives before the close widens it.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn resend_reconfigurer(&mut self, id: NodeId) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.require_live(id)?;
        if !self.reconfigurers[index].is_busy() {
            return Err(ActionError::new(
                ActionErrorCode::NoHandover,
                format!("node {} is not driving a handover", id.0),
            ));
        }
        self.reconfigurers[index].resend();
        self.close_freeze(index);
        self.queue_reconfigurer(index);
        Ok(())
    }
}
