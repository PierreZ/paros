//! The matchmaker set handover ([`Action::Handover`](super::Action::Handover)):
//! each node's driver holds a `MatchmakerReconfigurer`. A node starts a
//! handover to a target drawn from the matchmaker pool, finishes a frozen
//! generation it meets with no successor, adopts the set a handover chose,
//! and tells a matchmaker left behind the set it knows (the discovery rules of
//! `paros_consensus::models::handover`). The reconfigurer holds no
//! durable state: a crash of its node loses it.

use moonpool_walk::{Cx, sometimes};
use paros_consensus::matchmaker::{
    MatchOutcome, MatchRefusal, MatchReply, ReconfigureReply, ReconfigureRequest, ReconfigurerStep,
};
use paros_consensus::membership::{MatchmakerGeneration, MatchmakerId, MatchmakerSet};
use paros_consensus::{MatchmakerReconfigurer, NodeId};

use super::{Cell, Envelope, Party, Wire, reach};

/// The driver ticks a handover may stall before its node gives it up.
const ABANDON_TICKS: u64 = 12;

impl Cell {
    /// A fresh reconfigurer for node `id`.
    pub(super) fn reconfigurer(&self, id: NodeId) -> MatchmakerReconfigurer {
        MatchmakerReconfigurer::new(id, self.flavor.limits)
    }

    /// A node starts a handover from the set it believes to a target drawn
    /// from the matchmaker pool.
    pub(super) fn handover(&mut self, cx: &mut Cx<'_, Self>) {
        let live: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|site| site.live.is_some())
            .map(|site| site.id)
            .collect();
        let Some(&id) = cx.rng().pick(&live) else {
            return;
        };
        let node = self.node(id).live.as_ref().expect("a live node");
        let Some(set) = node.matchmaker_set().cloned() else {
            return;
        };
        let mut pool = self.matchmaker_ids();
        let size = if cx.rng().chance(1, 4) { 1 } else { 3 };
        while pool.len() > size {
            let out = Self::index(cx.rng().below(pool.len() as u64));
            pool.remove(out);
        }
        if pool == set.members() {
            return;
        }
        let index = Self::index(id.0);
        if self.reconfigurers[index].start(&set, pool).is_ok() {
            self.flush_reconfigurer(id, cx);
        }
    }

    /// Send the reconfigurer's batch of node `id`.
    pub(super) fn flush_reconfigurer(&mut self, id: NodeId, cx: &mut Cx<'_, Self>) {
        let ready = self.reconfigurers[Self::index(id.0)].ready();
        let requests = ready.requests().to_vec();
        ready.advance();
        for (to, request) in requests {
            cx.send(Envelope {
                to: Party::Matchmaker(to),
                msg: Wire::Reconfigure(request),
            });
        }
    }

    /// The driver's beat of node `id`'s handover: give up a stalled one,
    /// close a freeze whose quorum answered, re-send what is unanswered.
    pub(super) fn tick_reconfigurer(&mut self, id: NodeId, cx: &mut Cx<'_, Self>) {
        let reconfigurer = &mut self.reconfigurers[Self::index(id.0)];
        if !reconfigurer.is_busy() {
            return;
        }
        reconfigurer.tick();
        if reconfigurer.stalled_for() >= ABANDON_TICKS {
            let _ = reconfigurer.abandon();
            return;
        }
        let _ = reconfigurer.close_stop();
        reconfigurer.resend();
        self.flush_reconfigurer(id, cx);
    }

    /// A matchmaker answers a handover's request: step, persist, reply.
    pub(super) fn reconfigure_request(
        &mut self,
        id: MatchmakerId,
        request: ReconfigureRequest,
        cx: &mut Cx<'_, Self>,
    ) {
        let from = request.from();
        let Some(replies) = self.matchmaker_mut(id).reconfigure(request) else {
            return;
        };
        self.bind_registrations(id);
        for reply in replies {
            cx.send(Envelope {
                to: Party::Node(from),
                msg: Wire::ReconfigureReply(reply),
            });
        }
    }

    /// Node `id`'s reconfigurer folds a matchmaker's answer; the node adopts
    /// a set the handover chose.
    pub(super) fn reconfigure_reply(
        &mut self,
        id: NodeId,
        reply: ReconfigureReply,
        cx: &mut Cx<'_, Self>,
    ) {
        if self.node(id).live.is_none() {
            return;
        }
        let step = self.reconfigurers[Self::index(id.0)].on_reply(reply);
        sometimes!(
            matches!(step, ReconfigurerStep::Chosen { .. }),
            reach::HANDED_OVER
        );
        if let ReconfigurerStep::Chosen { successor }
        | ReconfigurerStep::Done { successor }
        | ReconfigurerStep::Superseded { successor } = &step
        {
            let node = self.node_mut(id).live.as_mut().expect("a live node");
            let _ = node.learn_matchmakers(successor);
            self.drain_node(id, cx);
        }
        self.flush_reconfigurer(id, cx);
    }

    /// The driver's discovery rules on a matchmaking answer to node `id`,
    /// before the node folds it: finish a frozen generation with no
    /// successor, and tell a matchmaker left behind the set the node knows.
    pub(super) fn discover(&mut self, id: NodeId, reply: &MatchReply, cx: &mut Cx<'_, Self>) {
        let MatchOutcome::Refused(refusal) = &reply.outcome else {
            return;
        };
        let Some(believed) = self
            .node(id)
            .live
            .as_ref()
            .and_then(|node| node.matchmaker_set().cloned())
        else {
            return;
        };
        let index = Self::index(id.0);
        match refusal {
            MatchRefusal::Stopped { successor: None } => {
                let reconfigurer = &mut self.reconfigurers[index];
                if reply.generation == believed.generation
                    && !reconfigurer.is_busy()
                    && reconfigurer.finish(&believed).is_ok()
                {
                    self.flush_reconfigurer(id, cx);
                }
            }
            MatchRefusal::Generation { current }
                if current.generation < believed.generation && believed.generation.0 > 0 =>
            {
                Self::republish(id, reply.matchmaker, &believed, cx);
            }
            MatchRefusal::Inactive if believed.generation.0 > 0 => {
                Self::republish(id, reply.matchmaker, &believed, cx);
            }
            _ => {}
        }
    }

    /// Tell matchmaker `to` the set `set` that node `from` knows was chosen.
    fn republish(from: NodeId, to: MatchmakerId, set: &MatchmakerSet, cx: &mut Cx<'_, Self>) {
        cx.send(Envelope {
            to: Party::Matchmaker(to),
            msg: Wire::Reconfigure(ReconfigureRequest::Chosen {
                from,
                generation: MatchmakerGeneration(set.generation.0 - 1),
                successor: set.clone(),
            }),
        });
    }
}
