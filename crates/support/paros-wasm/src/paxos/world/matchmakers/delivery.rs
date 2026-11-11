//! The wire: what a matchmaker-plane message does when it lands, what the
//! world says about it, and the two decisions the **driver** owns rather than
//! an ack.
//!
//! Closing a freeze ([`paros_core::MatchmakerReconfigurer::close_stop`]) and
//! abandoning a stalled handover both happen on a **beat**, never on the reply
//! that completed a quorum: a straggler that arrives before the close widens
//! the reconstruction, exactly as `paros::roles::node::run_node` has it.

use paros_core::matchmaker::{
    MatchOutcome, MatchReply, MatchRequest, ReconfigureReply, ReconfigureRequest, ReconfigurerStep,
};
use paros_core::membership::{MatchmakerId, MatchmakerSet};
use paros_core::node::MatchStep;
use paros_core::{ColocatedNode, NodeId};

use crate::paxos::narration::{NarrationKind, many, say, who};

use crate::paxos::view::show_ballot;
use crate::paxos::world::matchmakers::{show_members, show_set, which, why};
use crate::paxos::world::{Envelope, InFlight, Party, World};

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
    pub(in crate::paxos::world) fn beat_reconfigurer(&mut self, index: usize) {
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
                many(
                    reconstruction
                        .bootstrap
                        .registries
                        .values()
                        .map(|r| r.history.len())
                        .sum(),
                    "registration"
                )
            ),
        );
    }

    /// Drop the prior configurations a node's last campaign reported once it
    /// is registering again.
    ///
    /// `H_b` belongs to the campaign that completed, and a node whose
    /// matchmaking phase is open has not been told anything yet: the phase
    /// closes at the very step that hands `H_b` over
    /// ([`MatchStep::Completed`]), so an open one is always a fresh campaign.
    pub(in crate::paxos::world) fn forget_superseded_prior(&mut self, index: usize) {
        if self.nodes[index]
            .as_ref()
            .and_then(ColocatedNode::matchmaking_role)
            .is_some()
        {
            self.campaign_prior[index].clear();
        }
    }

    /// Push a running handover's current step onto the wire.
    pub(in crate::paxos::world) fn queue_reconfigurer(&mut self, index: usize) {
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
    pub(in crate::paxos::world) fn send(&mut self, from: Party, to: Party, envelope: Envelope) {
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
    pub(in crate::paxos::world) fn deliver_to_matchmaker(&mut self, entry: InFlight) {
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

    /// Hand the registration to the matchmaker.
    fn register_now(&mut self, from: Party, to: MatchmakerId, request: MatchRequest) {
        let Ok(index) = self.matchmaker_index(to) else {
            return;
        };
        let ballot = request.ballot;
        let members = show_members(request.config.members());
        let reconfiguration = request.purpose.is_reconfiguration();
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
            MatchOutcome::Probed { effective } => format!(
                "{} writes nothing. It only tells the node which configuration is in force: {}.",
                which(to),
                effective.as_ref().map_or_else(
                    || "no operator changed it, so the first one is".to_string(),
                    |(at, config)| format!(
                        "{}, from the change at ballot {}",
                        show_members(config.members()),
                        show_ballot(*at)
                    )
                )
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
    pub(in crate::paxos::world) fn deliver_matchmaker_reply(&mut self, entry: InFlight) {
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
                self.fold_match_reply(to, index, reply);
            }
            Envelope::GcAck(ack) => {
                let step = self.drive(to, index, move |node| node.on_gc_ack(&ack));
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
                // reconstruction. `paros::roles::node::run_node` closes it under
                // `resend_due` for the same reason.
                self.queue_reconfigurer(index);
            }
            Envelope::Node(_) | Envelope::Match(_) | Envelope::Gc(_) | Envelope::Reconfigure(_) => {
            }
        }
    }

    /// Fold one matchmaker answer. What the answer made of the matchmaking
    /// phase is said first, and what the node did about it after: a completed
    /// phase opens Phase 1 inside the same call.
    fn fold_match_reply(&mut self, to: NodeId, index: usize, reply: MatchReply) {
        let mark = self.narration.len();
        let step = self.drive(to, index, move |node| node.on_match_reply(reply));
        if let Some(step) = step {
            if let MatchStep::Completed { prior, .. } = &step {
                self.campaign_prior[index].clone_from(prior);
            }
            if let Some(text) = match_step_text(to, &step) {
                self.narration
                    .insert(mark, say(NarrationKind::Matchmaking, text));
            }
        }
    }

    fn narrate_gc_step(&mut self, id: NodeId, step: &paros_core::collector::GcStep) {
        use paros_core::collector::GcStep;
        let text = match step {
            GcStep::Ignored => return,
            GcStep::Acked { remaining } => format!(
                "{} counts one matchmaker's ack for the floor. {remaining} more must ack before \
                 the floor is in force.",
                who(id)
            ),
            GcStep::Frozen { generation } => format!(
                "{} meets a matchmaker set frozen at generation {} with no successor. The floor \
                 waits until a node finishes the handover.",
                who(id),
                generation.0
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
            ReconfigureReply::Stopped { registries, .. } => format!(
                "{} freezes, durably, and hands over its {}. It registers nothing for this \
                 generation ever again. It stays alive: it still votes in the decree, and it \
                 still points a late candidate at the successor.",
                which(id),
                many(
                    registries.values().map(|r| r.history.len()).sum(),
                    "registration"
                )
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
        // A finished handover: the node that drove it adopts the set (see
        // `adopt_set`), and then the step is narrated.
        if let ReconfigurerStep::Done { successor } | ReconfigurerStep::Superseded { successor } =
            step
        {
            self.adopt_set(index, successor);
        }
        if let Some(text) = reconfigurer_step_text(id, step) {
            self.narrate(NarrationKind::Generation, text);
        }
    }

    /// The driver's own half of a completed handover: the node that drove it
    /// adopts the set it published, exactly as `paros::roles::node::run_node` does.
    fn adopt_set(&mut self, index: usize, set: &MatchmakerSet) {
        let id = self.pool[index];
        self.drive(id, index, move |node| {
            node.learn_matchmakers(set);
        });
    }
}

/// What `id`'s matchmaking `step` says to the player; `None` for a step
/// that changed nothing.
fn match_step_text(id: NodeId, step: &MatchStep) -> Option<String> {
    let text = match step {
        MatchStep::Ignored => return None,
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
                "no configuration at all, so there is nothing for Phase 1 to recover".to_string()
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
        MatchStep::UnknownMember => format!(
            "{} abandons its campaign: the matchmakers name a node it has never heard of. It \
             waits until the node registry tells it about that node, and campaigns again.",
            who(id)
        ),
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
        MatchStep::ProbeAnswered => format!(
            "{} counts one answer to its question. It waits for a matchmaker quorum.",
            who(id)
        ),
        MatchStep::ProbeClosed {
            effective, member, ..
        } => format!(
            "{} knows which configuration is in force now{}. {}",
            who(id),
            effective.map_or_else(String::new, |at| format!(
                ": the change at ballot {}",
                show_ballot(at)
            )),
            if *member {
                "It is a member, so it starts a campaign."
            } else {
                "It is not a member, so it does not campaign."
            }
        ),
        MatchStep::ProbeLate { effective, member } => format!(
            "{} hears one more answer to a question it had already closed{}. {}",
            who(id),
            effective.map_or_else(String::new, |at| format!(
                ": the change at ballot {}",
                show_ballot(at)
            )),
            if *member {
                "It is a member after all, so it starts a campaign."
            } else {
                "It is still not a member."
            }
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
    Some(text)
}

/// What `id`'s handover `step` says to the player; `None` for a step that
/// changed nothing.
fn reconfigurer_step_text(id: NodeId, step: &ReconfigurerStep) -> Option<String> {
    let text = match step {
        ReconfigurerStep::Ignored => return None,
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
        ReconfigurerStep::Done { successor } => format!(
            "The handover is complete. Generation {} = {} serves matchmaking, and the \
             members it replaced point every late candidate at it.",
            successor.generation.0,
            show_set(successor.members())
        ),
        ReconfigurerStep::Preempted { promised, ballot } => format!(
            "Another handover promised {} first, so this decree reopens at {}.",
            show_ballot(*promised),
            show_ballot(*ballot)
        ),
        ReconfigurerStep::Superseded { successor } => format!(
            "This generation already has a successor: generation {} = {}. {} gives up and \
             adopts it.",
            successor.generation.0,
            show_set(successor.members()),
            who(id)
        ),
    };
    Some(text)
}

#[cfg(test)]
mod tests {
    use paros_core::Ballot;
    use paros_core::matchmaker::MatchRefusal;
    use paros_core::membership::{AcceptorConfig, MatchmakerGeneration, QuorumSystem};

    use super::*;

    fn ballot(round: u64, node: u64) -> Ballot {
        Ballot {
            round,
            node: NodeId(node),
        }
    }

    fn text(step: &MatchStep) -> String {
        match_step_text(NodeId(2), step).expect("a step that says something")
    }

    #[test]
    fn an_ignored_match_step_says_nothing() {
        assert_eq!(match_step_text(NodeId(2), &MatchStep::Ignored), None);
    }

    #[test]
    fn a_match_step_counts_what_the_candidate_heard() {
        assert_eq!(
            text(&MatchStep::Paged { next: ballot(3, 1) }),
            "node 2 counts nothing yet: that matchmaker's answer is one page of several, and \
             it asks for the rest from ballot 3.1."
        );
        assert_eq!(
            text(&MatchStep::Registered { remaining: 1 }),
            "node 2 counts one registration. 1 more must answer before the quorum holds."
        );
        assert_eq!(
            text(&MatchStep::ProbeAnswered),
            "node 2 counts one answer to its question. It waits for a matchmaker quorum."
        );
    }

    #[test]
    fn a_completed_matchmaking_names_the_earlier_configurations() {
        let completed = |prior| MatchStep::Completed {
            prior,
            watermark: ballot(1, 0),
            registered_by: 2,
            disagreements: 0,
        };
        assert_eq!(
            text(&completed(Vec::new())),
            "A matchmaker quorum has answered node 2. The union of their histories, above the \
             watermark 1.0, names no configuration at all, so there is nothing for Phase 1 to \
             recover. Phase 1 may open now, and not before. The matchmakers prove that no \
             earlier ballot chose a value this candidate cannot see."
        );
        let config = |ids: &[u64]| {
            AcceptorConfig::new(
                ids.iter().map(|id| NodeId(*id)).collect(),
                QuorumSystem::Majority,
            )
        };
        let two = text(&completed(vec![config(&[0, 1, 2]), config(&[1, 2, 3])]));
        assert!(
            two.contains("names 2 earlier configurations: {0, 1, 2}, {1, 2, 3}. Phase 1"),
            "{two}"
        );
    }

    #[test]
    fn an_abandoned_campaign_says_why() {
        assert_eq!(
            text(&MatchStep::UnknownMember),
            "node 2 abandons its campaign: the matchmakers name a node it has never heard of. \
             It waits until the node registry tells it about that node, and campaigns again."
        );
        assert_eq!(
            text(&MatchStep::Refused(MatchRefusal::Inactive)),
            "node 2 abandons its campaign: a matchmaker refused it, because it serves no \
             generation: it is a spare. A refused registration does not become a leadership. \
             The next campaign opens at a higher round."
        );
        let set = MatchmakerSet::new(
            MatchmakerGeneration(1),
            vec![MatchmakerId(0), MatchmakerId(3)],
        );
        assert_eq!(
            text(&MatchStep::Superseded { set }),
            "node 2 learns that the matchmaker set has changed: generation 1 = {0, 3} is \
             authoritative now. It abandons this campaign and asks the new generation next time."
        );
        assert!(
            text(&MatchStep::StaleConfiguration {
                newest: ballot(4, 1)
            })
            .starts_with(
                "node 2 abandons its campaign and adopts the configuration registered at 4.1."
            )
        );
    }

    #[test]
    fn a_probe_says_whether_the_node_campaigns() {
        let closed = |effective, member| MatchStep::ProbeClosed {
            effective,
            member,
            answered_by: 2,
        };
        assert_eq!(
            text(&closed(None, true)),
            "node 2 knows which configuration is in force now. It is a member, so it starts \
             a campaign."
        );
        assert_eq!(
            text(&closed(Some(ballot(3, 0)), false)),
            "node 2 knows which configuration is in force now: the change at ballot 3.0. It \
             is not a member, so it does not campaign."
        );
        assert_eq!(
            text(&MatchStep::ProbeLate {
                effective: None,
                member: false
            }),
            "node 2 hears one more answer to a question it had already closed. It is still \
             not a member."
        );
        assert_eq!(
            text(&MatchStep::ProbeLate {
                effective: Some(ballot(3, 0)),
                member: true
            }),
            "node 2 hears one more answer to a question it had already closed: the change at \
             ballot 3.0. It is a member after all, so it starts a campaign."
        );
    }

    fn handover(step: &ReconfigurerStep) -> String {
        reconfigurer_step_text(NodeId(2), step).expect("a step that says something")
    }

    #[test]
    fn a_handover_step_counts_what_the_driver_heard() {
        assert_eq!(
            reconfigurer_step_text(NodeId(2), &ReconfigurerStep::Ignored),
            None
        );
        assert_eq!(
            handover(&ReconfigurerStep::Stopped { remaining: 1 }),
            "node 2 counts one freeze. 1 more before a quorum of the old generation is frozen."
        );
        assert_eq!(
            handover(&ReconfigurerStep::Bootstrapped { remaining: 2 }),
            "node 2 counts one member that holds the proposed set. 2 more must hold it: every \
             member, not a quorum."
        );
        assert!(
            handover(&ReconfigurerStep::Deciding {
                ballot: ballot(1, 2)
            })
            .starts_with(
                "Every proposed member holds the bootstrap, so the decree opens at ballot 1.2."
            )
        );
        assert_eq!(
            handover(&ReconfigurerStep::Promised { remaining: 1 }),
            "node 2 counts one decree promise. 1 more before Phase 1 of the decree is complete."
        );
        assert_eq!(
            handover(&ReconfigurerStep::Accepted { remaining: 1 }),
            "node 2 counts one decree vote. 1 more before the set is chosen."
        );
        assert_eq!(
            handover(&ReconfigurerStep::Published {
                old_remaining: 1,
                new_remaining: 3
            }),
            "node 2 tells both generations. The old set still needs 1, the new set 3."
        );
        assert_eq!(
            handover(&ReconfigurerStep::Preempted {
                promised: ballot(2, 1),
                ballot: ballot(3, 2)
            }),
            "Another handover promised 2.1 first, so this decree reopens at 3.2."
        );
    }

    #[test]
    fn a_handover_proposal_says_whose_set_it_carries() {
        let proposing = |adopted| ReconfigurerStep::Proposing {
            ballot: ballot(1, 2),
            members: vec![MatchmakerId(0), MatchmakerId(3)],
            adopted,
        };
        assert_eq!(
            handover(&proposing(false)),
            "Phase 1 of the decree is complete at 1.2. node 2 proposes {0, 3}. No matchmaker \
             reported an earlier vote, so this campaign proposes its own set."
        );
        assert!(
            handover(&proposing(true)).ends_with("Two operators cannot install two successors.")
        );
    }

    #[test]
    fn a_finished_handover_names_the_successor() {
        let successor = MatchmakerSet::new(
            MatchmakerGeneration(1),
            vec![MatchmakerId(0), MatchmakerId(3)],
        );
        assert!(
            handover(&ReconfigurerStep::Chosen {
                successor: successor.clone()
            })
            .starts_with("Generation 1 = {0, 3} is chosen:")
        );
        assert!(
            handover(&ReconfigurerStep::Done {
                successor: successor.clone()
            })
            .starts_with("The handover is complete. Generation 1 = {0, 3} serves matchmaking")
        );
        assert_eq!(
            handover(&ReconfigurerStep::Superseded { successor }),
            "This generation already has a successor: generation 1 = {0, 3}. node 2 gives up \
             and adopts it."
        );
    }
}
