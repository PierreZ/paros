//! The wire: what a matchmaker-plane message does when it lands, what the
//! world says about it, and the two decisions the **driver** owns rather than
//! an ack.
//!
//! Closing a freeze ([`paros_core::MatchmakerReconfigurer::close_stop`]) and
//! abandoning a stalled handover both happen on a **beat**, never on the reply
//! that completed a quorum: a straggler that arrives before the close widens
//! the reconstruction, exactly as `paros::run_node` has it.

use paros_core::{
    ColocatedNode, MatchOutcome, MatchReply, MatchRequest, MatchStep, MatchmakerId, MatchmakerSet,
    NodeId, ReconfigureReply, ReconfigureRequest, ReconfigurerStep, RegistrationKind,
};

use crate::narration::{NarrationKind, many, who};

use crate::view::show_ballot;
use crate::world::drain::Paused;
use crate::world::matchmakers::{show_members, show_set, which, why};
use crate::world::{Envelope, InFlight, Party, World};

impl World {
    /// One beat of a running handover: the stall clock advances, a freeze whose
    /// quorum answered is closed, and a phase that has made no progress for
    /// the handover stall timeout beats is abandoned.
    ///
    /// The timeout is the **driver's** policy, never a constant inside the
    /// state machine, and it is a level tunable here. Its floor is structural:
    /// a phase must get more beats than one round trip needs, or a handover
    /// that is simply slow is abandoned every time. Zero disables it, which is
    /// what a level that never wants a handover abandoned sets.
    pub(in crate::world) fn beat_reconfigurer(&mut self, index: usize) {
        if !self.reconfigurers[index].is_busy() {
            return;
        }
        self.reconfigurers[index].tick();
        self.close_freeze(index);
        let timeout = self.reconfigure_timeout_ticks;
        if timeout > 0 && self.reconfigurers[index].stalled_for() >= timeout {
            let id = self.pool[index];
            if self.reconfigurers[index].abandon() {
                self.abandoned_handovers.push(id);
                self.narrate(
                    NarrationKind::Generation,
                    format!(
                        "{} gives up the handover it was driving: it made no progress for \
                         {timeout} beats. Nothing is lost. The freeze and the votes are on the \
                         matchmakers' own disks, and the next node that meets the frozen \
                         generation finishes it.",
                        who(id)
                    ),
                );
            }
        }
        self.queue_reconfigurer(index);
    }

    /// Close a freeze whose quorum has answered.
    pub(super) fn close_freeze(&mut self, index: usize) {
        if !self.reconfigurers[index].stop_quorum_reached() {
            return;
        }
        let Some(reconstruction) = self.reconfigurers[index].close_stop() else {
            return;
        };
        let id = self.pool[index];
        self.narrate(
            NarrationKind::Generation,
            format!(
                "{} closes the freeze. A quorum of the old generation answered. It takes the \
                 highest watermark they reported, and the union of their registries above it: \
                 {}. Every registration that ever completed reached a quorum of the old \
                 generation. Any two quorums share a matchmaker, so nothing can be missing from \
                 that union.",
                who(id),
                many(reconstruction.bootstrap.history.len(), "registration")
            ),
        );
    }

    /// Keep the shadow matchmaking phase in step with the node's own.
    ///
    /// A candidate that opens a campaign gets a fresh instance of the core's
    /// [`paros_core::matchmaking::Matchmaking`] role, built from the very
    /// ballot, configuration and kind the node reports. A campaign that closed
    /// takes its shadow with it.
    ///
    /// A **fresh** campaign also drops the prior configurations the last one
    /// reported: `H_b` belongs to the campaign that completed, and a node that
    /// is registering again has not been told anything yet.
    pub(in crate::world) fn sync_matchmaking_shadow(&mut self, index: usize) {
        let open = self.nodes[index]
            .as_ref()
            .and_then(ColocatedNode::matchmaking)
            .map(|(ballot, config, kind)| (ballot, config.clone(), kind));
        match open {
            None => self.matchmaking_shadow[index] = None,
            Some((ballot, config, kind)) => {
                let stale = self.matchmaking_shadow[index]
                    .as_ref()
                    .is_none_or(|shadow| shadow.ballot() != ballot);
                if stale {
                    self.matchmaking_shadow[index] = Some(
                        paros_core::matchmaking::Matchmaking::new(ballot, config, kind),
                    );
                    self.campaign_prior[index].clear();
                }
            }
        }
    }

    /// Push a running handover's current step onto the wire.
    pub(in crate::world) fn queue_reconfigurer(&mut self, index: usize) {
        let id = self.pool[index];
        let ready = self.reconfigurers[index].ready();
        let requests: Vec<(MatchmakerId, ReconfigureRequest)> = ready.requests().to_vec();
        ready.advance();
        for (to, request) in requests {
            self.send(
                Party::Node(id),
                Party::Matchmaker(to),
                Envelope::Reconfigure(request),
            );
        }
    }

    /// Queue one message.
    pub(in crate::world) fn send(&mut self, from: Party, to: Party, envelope: Envelope) {
        self.wire.push(InFlight {
            id: self.next_message_id,
            from,
            to,
            envelope,
            sent_at: self.clock,
        });
        self.next_message_id += 1;
    }

    /// Deliver one message to a matchmaker.
    pub(in crate::world) fn deliver_to_matchmaker(&mut self, entry: InFlight) {
        let Party::Matchmaker(to) = entry.to else {
            return;
        };
        let Ok(index) = self.matchmaker_index(to) else {
            return;
        };
        if !self.matchmakers[index].alive() {
            self.narrate(
                NarrationKind::Info,
                format!(
                    "The message reaches {}, which is not running, so it is discarded.",
                    which(to)
                ),
            );
            return;
        }
        match entry.envelope {
            Envelope::Match(request) => {
                if let Some(prompt) = self.generation_fence_prompt(to, index, &request) {
                    self.narrate(
                        NarrationKind::Matchmaking,
                        format!(
                            "A registration reaches {}. {} You answer for it, and the real \
                             registry marks the answer.",
                            which(to),
                            prompt.question
                        ),
                    );
                    self.prompt = Some(prompt);
                    self.paused = Some(Paused::Registration {
                        from: entry.from,
                        to,
                        request: Box::new(request),
                    });
                    return;
                }
                self.register_now(entry.from, to, request);
            }
            Envelope::Gc(request) => {
                let ack = self.matchmakers[index].deliver_gc(request);
                if let Some(ack) = ack {
                    self.narrate(
                        NarrationKind::Gc,
                        format!(
                            "{} raises its watermark to {} and writes it down before it answers. \
                             From now on it refuses every campaign below that floor, so no later \
                             history can name a configuration it dropped.",
                            which(to),
                            show_ballot(ack.watermark)
                        ),
                    );
                    self.send(entry.to, entry.from, Envelope::GcAck(ack));
                }
            }
            Envelope::Reconfigure(request) => {
                let reply = self.matchmakers[index].deliver_reconfigure(request);
                if let Some(reply) = reply {
                    self.narrate_handover_reply(to, &reply);
                    self.send(entry.to, entry.from, Envelope::ReconfigureReply(reply));
                }
            }
            Envelope::Node(_)
            | Envelope::MatchReply(_)
            | Envelope::GcAck(_)
            | Envelope::ReconfigureReply(_) => {}
        }
    }

    /// Register for real, once nobody owes an answer for it.
    pub(in crate::world) fn register_now(
        &mut self,
        from: Party,
        to: MatchmakerId,
        request: MatchRequest,
    ) {
        let Ok(index) = self.matchmaker_index(to) else {
            return;
        };
        let ballot = request.ballot;
        let members = show_members(request.config.members());
        let reconfiguration = request.kind == RegistrationKind::Reconfiguration;
        let Some(reply) = self.matchmakers[index].deliver_match(request) else {
            return;
        };
        let text = match &reply.outcome {
            MatchOutcome::Registered { history, .. } => format!(
                "{} writes down that ballot {} runs with the acceptors {members}{}. It answers \
                 only after that write. It reports the {} it holds below that ballot. The write \
                 comes first: a registration it forgot would leave a later leader asking the \
                 wrong acceptors.",
                which(to),
                show_ballot(ballot),
                if reconfiguration {
                    " (an operator's change, and not a belief)"
                } else {
                    ""
                },
                many(history.len(), "configuration")
            ),
            MatchOutcome::Refused(refusal) => format!(
                "{} refuses ballot {}: {}.",
                which(to),
                show_ballot(ballot),
                why(refusal)
            ),
        };
        self.narrate(NarrationKind::Matchmaking, text);
        self.send(Party::Matchmaker(to), from, Envelope::MatchReply(reply));
    }

    /// Deliver one matchmaker answer to the node that asked.
    pub(in crate::world) fn deliver_matchmaker_reply(&mut self, entry: InFlight) {
        let Party::Node(to) = entry.to else {
            return;
        };
        let Some(index) = self.index_of(to) else {
            return;
        };
        if self.nodes[index].is_none() {
            self.narrate(
                NarrationKind::Info,
                format!(
                    "The answer reaches {}, which is not running, so it is discarded.",
                    who(to)
                ),
            );
            return;
        }
        match entry.envelope {
            Envelope::MatchReply(reply) => {
                if let Some(prompt) = self.stale_configuration_prompt(to, index, &reply) {
                    self.narrate(
                        NarrationKind::Matchmaking,
                        format!(
                            "The matchmakers have answered {}. {} You answer for it.",
                            who(to),
                            prompt.question
                        ),
                    );
                    self.prompt = Some(prompt);
                    self.paused = Some(Paused::MatchReply {
                        node: to,
                        reply: Box::new(reply),
                    });
                    return;
                }
                self.fold_match_reply(to, index, reply);
            }
            Envelope::GcAck(ack) => {
                let step = self.observe(to, move |world| {
                    let out = world.nodes[index].as_mut().map(|node| node.on_gc_ack(&ack));
                    world.pump(to);
                    out
                });
                if let Some(step) = step {
                    self.narrate_gc_step(to, &step);
                }
            }
            Envelope::ReconfigureReply(reply) => {
                let step = self.reconfigurers[index].on_reply(reply);
                self.narrate_reconfigurer_step(to, index, &step);
                // The freeze is **not** closed here. An ack that completes the
                // quorum only makes the close possible; the close itself waits
                // for a beat, so a straggler that arrives in between widens the
                // reconstruction. `paros::run_node` closes it under
                // `resend_due` for the same reason.
                self.queue_reconfigurer(index);
            }
            Envelope::Node(_) | Envelope::Match(_) | Envelope::Gc(_) | Envelope::Reconfigure(_) => {
            }
        }
    }

    /// Fold one matchmaker answer for real, once nobody owes an answer for it.
    pub(in crate::world) fn fold_match_reply(
        &mut self,
        to: NodeId,
        index: usize,
        reply: MatchReply,
    ) {
        // The shadow is fed exactly what the node is fed, and in the same
        // order, so the `StaleConfiguration` oracle sees the union the node
        // sees. "Exactly" includes the guards `ColocatedNode::on_match_reply`
        // applies before it folds anything: an answer addressed to another
        // node, an answer from a matchmaker outside the set this node believes
        // authoritative, and an answer for another generation are all
        // `Ignored` there, so the shadow must ignore them too. A shadow that
        // counted one of them would report a quorum the node does not hold.
        let addressed = reply.to == to
            && self.nodes[index]
                .as_ref()
                .and_then(ColocatedNode::matchmaker_set)
                .is_some_and(|set| {
                    set.contains(reply.matchmaker) && set.generation == reply.generation
                });
        if addressed
            && let Some(shadow) = self.matchmaking_shadow[index].as_mut()
            && shadow.ballot() == reply.ballot
        {
            let (matchmaker, answer) =
                paros_core::matchmaking::RegisteredPage::from_reply(reply.clone());
            if let Ok(page) = answer {
                shadow.fold(matchmaker, page);
            }
        }
        let step = self.observe(to, move |world| {
            let out = world.nodes[index]
                .as_mut()
                .map(|node| node.on_match_reply(reply));
            world.pump(to);
            out
        });
        if let Some(step) = step {
            if let MatchStep::Completed { prior, .. } = &step {
                self.campaign_prior[index].clone_from(prior);
            }
            self.narrate_match_step(to, &step);
        }
    }

    fn narrate_match_step(&mut self, id: NodeId, step: &MatchStep) {
        let text = match step {
            MatchStep::Ignored => return,
            MatchStep::Paged { next } => format!(
                "{} counts nothing yet: that matchmaker's answer is one page of several, and it \
                 asks for the rest from ballot {}.",
                who(id),
                show_ballot(*next)
            ),
            MatchStep::Registered { remaining } => format!(
                "{} counts one registration. {} more must answer before the quorum holds.",
                who(id),
                remaining
            ),
            MatchStep::Completed {
                prior, watermark, ..
            } => {
                let named = if prior.is_empty() {
                    "no configuration at all, so there is nothing for Phase 1 to recover"
                        .to_string()
                } else {
                    format!(
                        "{}: {}",
                        many(prior.len(), "earlier configuration"),
                        prior
                            .iter()
                            .map(|config| show_members(config.members()))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                };
                format!(
                    "A matchmaker quorum has answered {}. The union of their histories, above the \
                     watermark {}, names {named}. Phase 1 may open now, and not before. The \
                     matchmakers prove that no earlier ballot chose a value this candidate \
                     cannot see.",
                    who(id),
                    show_ballot(*watermark)
                )
            }
            MatchStep::Refused(refusal) => format!(
                "{} abandons its campaign: a matchmaker refused it, because {}. A refused \
                 registration does not become a leadership. The next campaign opens at a higher \
                 round.",
                who(id),
                why(refusal)
            ),
            MatchStep::Superseded { set } => format!(
                "{} learns that the matchmaker set has changed: generation {} = {} is \
                 authoritative now. It abandons this campaign and asks the new generation next \
                 time.",
                who(id),
                set.generation.0,
                show_set(set.members())
            ),
            MatchStep::StaleConfiguration { newest } => format!(
                "{} abandons its campaign and adopts the configuration registered at {}. Its own \
                 belief was out of date, because an operator changed the acceptor set while this \
                 node was not listening. A candidate elected under a superseded set would undo \
                 that change, and nobody asked for that.",
                who(id),
                show_ballot(*newest)
            ),
        };
        self.narrate(NarrationKind::Matchmaking, text);
    }

    fn narrate_gc_step(&mut self, id: NodeId, step: &paros_core::GcStep) {
        use paros_core::GcStep;
        let text = match step {
            GcStep::Ignored => return,
            GcStep::Acked { remaining } => format!(
                "{} counts one matchmaker's ack for the floor. {remaining} more must ack before \
                 the floor is in force.",
                who(id)
            ),
            GcStep::Effective { watermark, retired } => format!(
                "The floor {} is in force: a matchmaker quorum wrote it down. Every later \
                 campaign asks a quorum that shares a matchmaker with this one, so no later \
                 history names a configuration below the floor. That releases {}.",
                show_ballot(*watermark),
                if retired.is_empty() {
                    "no acceptor".to_string()
                } else {
                    format!("the acceptors {}", show_members(retired))
                }
            ),
        };
        self.narrate(NarrationKind::Gc, text);
    }

    fn narrate_handover_reply(&mut self, id: MatchmakerId, reply: &ReconfigureReply) {
        let text = match reply {
            ReconfigureReply::Stopped { history, .. } => format!(
                "{} freezes, durably, and hands over its {}. It registers nothing for this \
                 generation ever again. It stays alive: it still votes in the decree, and it \
                 still points a late candidate at the successor.",
                which(id),
                many(history.len(), "registration")
            ),
            ReconfigureReply::Bootstrapped { set, .. } => format!(
                "{} holds the proposed generation {} on its disk, marked pending. It does not \
                 serve it. A set can only be chosen once every member already holds the history \
                 it will be asked about.",
                which(id),
                set.generation.0
            ),
            ReconfigureReply::Promised { ballot, vote, .. } => format!(
                "{} promises {} in the one decree slot, and reports {}. This is Phase 1 of the \
                 single decree, over exactly the roles Act I used.",
                which(id),
                show_ballot(*ballot),
                match vote {
                    Some((at, members)) => format!(
                        "the vote it already holds: {} at {}",
                        show_set(members),
                        show_ballot(*at)
                    ),
                    None => "no vote at all".to_string(),
                }
            ),
            ReconfigureReply::Accepted { ballot, .. } => format!(
                "{} votes for the proposed set at {}, and writes the vote down before it answers. \
                 This is Phase 2 of the single decree.",
                which(id),
                show_ballot(*ballot)
            ),
            ReconfigureReply::Nacked { promised, .. } => format!(
                "{} refuses the decree ballot: it has promised {}.",
                which(id),
                show_ballot(*promised)
            ),
            ReconfigureReply::Learned { activated, at, .. } => {
                if *activated {
                    format!(
                        "{} activates generation {}: the bootstrap it held pending is live, and it \
                         serves matchmaking again.",
                        which(id),
                        at.0
                    )
                } else {
                    format!(
                        "{} records the successor. It stays frozen at generation {} and points \
                         late candidates at the new set.",
                        which(id),
                        at.0
                    )
                }
            }
            ReconfigureReply::Refused { current, .. } => format!(
                "{} refuses the step: it holds generation {}.",
                which(id),
                current.generation.0
            ),
        };
        self.narrate(NarrationKind::Generation, text);
    }

    fn narrate_reconfigurer_step(&mut self, id: NodeId, index: usize, step: &ReconfigurerStep) {
        let text = match step {
            ReconfigurerStep::Ignored => return,
            ReconfigurerStep::Stopped { remaining } => format!(
                "{} counts one freeze. {remaining} more before a quorum of the old generation is \
                 frozen.",
                who(id)
            ),
            ReconfigurerStep::Bootstrapped { remaining } => format!(
                "{} counts one member that holds the proposed set. {remaining} more must hold it: \
                 every member, not a quorum.",
                who(id)
            ),
            ReconfigurerStep::Deciding { ballot } => format!(
                "Every proposed member holds the bootstrap, so the decree opens at ballot {}. The \
                 acceptors of this decree are the matchmakers of the old generation, and the \
                 value is the new set. It is single-decree Paxos over one slot.",
                show_ballot(*ballot)
            ),
            ReconfigurerStep::Promised { remaining } => format!(
                "{} counts one decree promise. {remaining} more before Phase 1 of the decree is \
                 complete.",
                who(id)
            ),
            ReconfigurerStep::Proposing {
                ballot,
                members,
                adopted,
            } => format!(
                "Phase 1 of the decree is complete at {}. {} proposes {}{}",
                show_ballot(*ballot),
                who(id),
                show_set(members),
                if *adopted {
                    ". A matchmaker reported a set it had already voted for, so the value \
                     selection rule takes that set instead. Two operators cannot install two \
                     successors."
                } else {
                    ". No matchmaker reported an earlier vote, so this campaign proposes its own \
                     set."
                }
            ),
            ReconfigurerStep::Accepted { remaining } => format!(
                "{} counts one decree vote. {remaining} more before the set is chosen.",
                who(id)
            ),
            ReconfigurerStep::Chosen { successor } => format!(
                "Generation {} = {} is chosen: a quorum of the old generation voted for it at one \
                 ballot. From here there is exactly one successor, whichever node finishes the \
                 handover.",
                successor.generation.0,
                show_set(successor.members())
            ),
            ReconfigurerStep::Published {
                old_remaining,
                new_remaining,
            } => format!(
                "{} tells both generations. The old set still needs {old_remaining}, the new set \
                 {new_remaining}.",
                who(id)
            ),
            ReconfigurerStep::Done { successor } => {
                self.adopt_set(index, successor);
                format!(
                    "The handover is complete. Generation {} = {} serves matchmaking, and the \
                     members it replaced point every late candidate at it.",
                    successor.generation.0,
                    show_set(successor.members())
                )
            }
            ReconfigurerStep::Preempted { promised, ballot } => format!(
                "Another handover promised {} first, so this decree reopens at {}.",
                show_ballot(*promised),
                show_ballot(*ballot)
            ),
            ReconfigurerStep::Superseded { successor } => {
                self.adopt_set(index, successor);
                format!(
                    "This generation already has a successor: generation {} = {}. {} gives up and \
                     adopts it.",
                    successor.generation.0,
                    show_set(successor.members()),
                    who(id)
                )
            }
        };
        self.narrate(NarrationKind::Generation, text);
    }

    /// The driver's own half of a completed handover: the node that drove it
    /// adopts the set it published, exactly as `paros::run_node` does.
    fn adopt_set(&mut self, index: usize, set: &MatchmakerSet) {
        let id = self.pool[index];
        self.observe(id, move |world| {
            if let Some(node) = world.nodes[index].as_mut() {
                node.learn_matchmakers(set);
            }
            world.pump(id);
        });
    }
}
