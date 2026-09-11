//! The three questions the matchmaker plane raises, and the clones they are
//! judged on.

use paros_core::{
    Ballot, MatchOutcome, MatchReply, MatchRequest, MatchmakerId, NodeId, RegistrationKind,
};

use crate::prompt::{Prompt, PromptKind};
use crate::world::World;

impl World {
    /// The question a registration raises at a matchmaker whose generation is
    /// not the one the request addresses: serve it, or refuse it?
    ///
    /// Judged on a **clone of the matchmaker**: the clone is stepped with this
    /// very request and its own reply is read back, so the answer is the
    /// registry's, not a rule restated here.
    pub(super) fn generation_fence_prompt(
        &mut self,
        to: MatchmakerId,
        index: usize,
        request: &MatchRequest,
    ) -> Option<Prompt> {
        if !self.policy.manual.contains(&PromptKind::GenerationFence) {
            return None;
        }
        let role = self.matchmakers[index].role()?;
        let held = role.set().generation;
        let phase = role.phase();
        let mut clone = role.clone();
        clone.step(request.clone());
        let ready = clone.ready();
        let outcome = ready.replies().first().map(|reply| reply.outcome.clone());
        ready.advance();
        let refused = match outcome? {
            MatchOutcome::Registered { .. } => None,
            MatchOutcome::Refused(refusal) => Some(refusal),
        };
        let id = self.take_prompt_id();
        Some(Prompt::generation_fence(
            id,
            to,
            request.ballot,
            request.generation.0,
            held.0,
            phase,
            refused.as_ref(),
        ))
    }

    /// The question a matchmaker quorum's answer raises at a candidate whose
    /// belief is out of date: abandon the campaign, or carry on?
    ///
    /// Judged on a **clone of the candidate's own matchmaking phase**. The
    /// world drives a second [`paros_core::Matchmaking`] with the same
    /// answers the node is given — the role is the core's, but `ColocatedNode`
    /// hands no reference to its own, so this is a shadow rather than a clone
    /// of the node's. See the crate's report: it is the one prompt whose oracle
    /// is not read straight off the node.
    pub(super) fn stale_configuration_prompt(
        &mut self,
        to: NodeId,
        index: usize,
        reply: &MatchReply,
    ) -> Option<Prompt> {
        if !self.policy.manual.contains(&PromptKind::StaleConfiguration) {
            return None;
        }
        let node = self.nodes[index].as_ref()?;
        let matchmakers = node.matchmaker_set()?.clone();
        let (ballot, believed, kind) = node
            .matchmaking()
            .map(|(ballot, config, kind)| (ballot, config.clone(), kind))?;
        if reply.ballot != ballot || kind == RegistrationKind::Reconfiguration {
            return None;
        }
        let shadow = self.matchmaking_shadow[index].as_mut()?;
        let mut clone = shadow.clone();
        let (matchmaker, answer) =
            paros_core::matchmaking::RegisteredPage::from_reply(reply.clone());
        let Ok(page) = answer else {
            return None;
        };
        if clone.fold(matchmaker, page) != paros_core::matchmaking::MatchFold::Registered {
            return None;
        }
        if !clone.quorum_held(&matchmakers) {
            return None;
        }
        let effective = clone.stale_belief();
        let id = self.take_prompt_id();
        Some(Prompt::stale_configuration(
            id,
            to,
            ballot,
            believed.members(),
            effective
                .as_ref()
                .map(|(at, config)| (*at, config.members())),
        ))
    }

    /// The question an operator's retire request raises at the node it names.
    ///
    /// Judged by [`ColocatedNode::may_retire`](paros_core::ColocatedNode::may_retire)
    /// on the target itself: the call takes `&self` and changes nothing, so
    /// there is nothing to clone. `evidenced` is the half the core cannot
    /// answer — whether the number the operator showed is a floor a live
    /// leadership reports at all — and a request without it is refused
    /// whatever the target's own state says.
    pub(super) fn may_retire_prompt(
        &mut self,
        target: NodeId,
        index: usize,
        watermark: Ballot,
        effective: Option<Ballot>,
        evidenced: bool,
    ) -> Option<Prompt> {
        if !self.policy.manual.contains(&PromptKind::MayRetire) {
            return None;
        }
        let node = self.nodes[index].as_ref()?;
        let member = node.is_acceptor();
        let leader = node.is_leader();
        let may = evidenced && node.may_retire(watermark);
        let id = self.take_prompt_id();
        Some(Prompt::may_retire(
            id, target, watermark, effective, member, leader, may, evidenced,
        ))
    }
}
