//! What a message carries, for the step-through (#811 (Paxos flavor pages)).
//!
//! A step that moves a message shows the message itself: its important
//! fields ("ballot 1.0", "slot 2", "value alpha") and one plain sentence in
//! the sender's voice ("I want to be the leader with ballot 1.0."). Both are
//! read off the real `paros-core` message on the wire, never off its label.
//!
//! The single-decree world has one slot only, so there a message names no
//! slot: `log` is false.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use paros_core::journal::{Command, Control};
use paros_core::matchmaker::{
    GcAck, MatchOutcome, MatchPurpose, MatchReply, MatchRequest, ReconfigureReply,
    ReconfigureRequest, RegistrationKind,
};
use paros_core::{Ballot, Message, Slot};
use serde::{Deserialize, Serialize};

use crate::paxos::view::{show_ballot, value_text};
use crate::paxos::world::matchmakers::{show_members, show_set, why};
use crate::paxos::world::{Envelope, InFlight};

/// How many votes a sentence lists before it says "and N more".
const LIST: usize = 3;

/// One field of a message: `ballot` = `1.0 (round 1, node 0)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Field {
    /// The field's name, in plain words: `ballot`, `slot`, `value`.
    pub name: String,
    /// Its value, as a reader reads it.
    pub value: String,
}

/// What a message says: its fields and one sentence in the sender's voice.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Content {
    /// The important fields, in the order a reader needs them.
    pub fields: Vec<Field>,
    /// The message as the sender would say it, in the first person.
    pub voice: String,
}

/// Read what `entry` carries. `log` is false in the single-decree world.
#[must_use]
pub fn content(entry: &InFlight, log: bool) -> Content {
    match &entry.envelope {
        Envelope::Node(message) => node_message(message, log),
        Envelope::Match(request) => match_request(request),
        Envelope::MatchReply(answer) => match_reply(answer),
        Envelope::Gc(request) => Content {
            fields: vec![field("forget below", ballot_text(request.watermark))],
            voice: format!(
                "My ballot {} is safe now. Forget the configurations below it.",
                show_ballot(request.watermark)
            ),
        },
        Envelope::GcAck(ack) => gc_ack(ack),
        Envelope::Reconfigure(request) => reconfigure(request),
        Envelope::ReconfigureReply(answer) => reconfigure_reply(answer),
    }
}

/// A candidate's matchmaking request: a probe or a registration.
fn match_request(request: &MatchRequest) -> Content {
    let mut fields = vec![
        field("ballot", ballot_text(request.ballot)),
        field("acceptors", show_members(request.config.members())),
        field("generation", request.generation.0.to_string()),
    ];
    let voice = match request.purpose {
        MatchPurpose::Probe => {
            fields.insert(0, field("asks", "which acceptors are in use"));
            "Which acceptors are in use now? Do not write anything down.".to_string()
        }
        MatchPurpose::Register(kind) => {
            let what = match kind {
                RegistrationKind::Belief => "write down",
                RegistrationKind::Reconfiguration => "write down a new configuration",
            };
            fields.insert(0, field("asks", what));
            format!(
                "I want to use ballot {} with acceptors {}. Write this down, and \
                     tell me which acceptors the lower ballots used.",
                show_ballot(request.ballot),
                show_members(request.config.members())
            )
        }
    };
    Content { fields, voice }
}

/// A matchmaker's answer to a matchmaking request.
#[allow(clippy::single_match_else)]
fn match_reply(answer: &MatchReply) -> Content {
    let ballot = show_ballot(answer.ballot);
    let mut fields = vec![field("ballot", ballot_text(answer.ballot))];
    let voice = match &answer.outcome {
        MatchOutcome::Registered {
            history, effective, ..
        } => {
            fields.insert(0, field("answer", "registered"));
            let older: Vec<String> = history
                .iter()
                .map(|(at, registration)| {
                    format!(
                        "{} → {}",
                        show_ballot(*at),
                        show_members(registration.config.members())
                    )
                })
                .collect();
            fields.push(field("lower ballots", list_or(&older, "none")));
            if let Some((at, config)) = effective {
                fields.push(field(
                    "in use",
                    format!(
                        "{} since {}",
                        show_members(config.members()),
                        show_ballot(*at)
                    ),
                ));
            }
            if history.is_empty() {
                format!("I wrote down ballot {ballot}. I hold no lower ballot.")
            } else {
                let used: Vec<String> = history
                    .iter()
                    .map(|(at, registration)| {
                        format!(
                            "ballot {} used acceptors {}",
                            show_ballot(*at),
                            show_members(registration.config.members())
                        )
                    })
                    .collect();
                format!(
                    "I wrote down ballot {ballot}. Before it, {}. Send Phase 1 \
                         to those acceptors too.",
                    short_list(&used)
                )
            }
        }
        MatchOutcome::Probed { effective } => {
            fields.insert(0, field("answer", "in use"));
            match effective {
                Some((at, config)) => {
                    fields.push(field(
                        "in use",
                        format!(
                            "{} since {}",
                            show_members(config.members()),
                            show_ballot(*at)
                        ),
                    ));
                    format!(
                        "Acceptors {} are in use since ballot {}.",
                        show_members(config.members()),
                        show_ballot(*at)
                    )
                }
                None => {
                    fields.push(field("in use", "the first configuration"));
                    "Nobody changed the acceptors. The first configuration is in use.".to_string()
                }
            }
        }
        MatchOutcome::Refused(refusal) => {
            fields.insert(0, field("answer", "refused"));
            format!("No. I refuse ballot {ballot}: {}.", why(refusal))
        }
    };
    Content { fields, voice }
}

/// A matchmaker's answer to a garbage-collection request.
fn gc_ack(ack: &GcAck) -> Content {
    let watermark = show_ballot(ack.watermark);
    Content {
        fields: vec![
            field("answer", if ack.applied { "done" } else { "refused" }),
            field("watermark", ballot_text(ack.watermark)),
        ],
        voice: if ack.applied {
            format!("Done. I forgot everything below ballot {watermark}.")
        } else {
            format!("No. My watermark stays at {watermark}.")
        },
    }
}

#[allow(clippy::too_many_lines, clippy::single_match_else)]
fn node_message(message: &Message, log: bool) -> Content {
    match message {
        Message::Prepare {
            ballot, from_slot, ..
        } => {
            let b = show_ballot(*ballot);
            let mut fields = vec![field("ballot", ballot_text(*ballot))];
            let voice = if log {
                fields.push(field("from slot", from_slot.0.to_string()));
                format!(
                    "I want to be the leader with ballot {b}. Promise that you will not \
                     accept a lower ballot from slot {} on, and tell me your votes.",
                    from_slot.0
                )
            } else {
                format!(
                    "I want to propose a value with ballot {b}. Promise that you will \
                     not accept a lower ballot, and tell me your vote."
                )
            };
            Content { fields, voice }
        }
        Message::Promise {
            ballot,
            from_slot,
            accepted,
            faulty,
            next_from_slot,
            ..
        } => {
            let b = show_ballot(*ballot);
            let mut fields = vec![field("ballot", ballot_text(*ballot))];
            let mut voice = format!("I promise ballot {b}.");
            if log {
                fields.push(field("from slot", from_slot.0.to_string()));
                let votes = votes(accepted);
                fields.push(field("votes", list_or(&votes, "none")));
                if votes.is_empty() {
                    let _ = write!(voice, " I have no vote from slot {} on.", from_slot.0);
                } else {
                    let _ = write!(voice, " My votes: {}.", short_list(&votes));
                }
            } else {
                match accepted.values().next() {
                    Some((at, command)) => {
                        let value = value_text(command);
                        fields.push(field("vote", format!("{value} at {}", show_ballot(*at))));
                        let _ = write!(
                            voice,
                            " I voted for {value} at ballot {} before.",
                            show_ballot(*at)
                        );
                    }
                    None => {
                        fields.push(field("vote", "none"));
                        voice.push_str(" I did not vote for a value yet.");
                    }
                }
            }
            if !faulty.is_empty() {
                fields.push(field("lost votes", faulty.len().to_string()));
                let _ = write!(
                    voice,
                    " My disk lost {}.",
                    plural(faulty.len(), "vote", "votes")
                );
            }
            if let Some(next) = next_from_slot {
                fields.push(field("more from slot", next.0.to_string()));
                let _ = write!(voice, " More votes follow from slot {}.", next.0);
            }
            Content { fields, voice }
        }
        Message::Accept {
            ballot,
            slot,
            command,
            leader,
            ..
        } => {
            let value = value_text(command);
            let mut fields = vec![field("ballot", ballot_text(*ballot))];
            if log {
                fields.push(field("slot", slot.0.to_string()));
            }
            fields.push(field("value", value.clone()));
            if *leader != ballot.node {
                fields.push(field("leader", format!("node {}", leader.0)));
            }
            let voice = if log {
                format!(
                    "Please vote for {} in slot {} at ballot {}.",
                    spoken(command),
                    slot.0,
                    show_ballot(*ballot)
                )
            } else {
                format!(
                    "Please vote for {} at ballot {}.",
                    spoken(command),
                    show_ballot(*ballot)
                )
            };
            Content { fields, voice }
        }
        Message::Accepted { ballot, slot, .. } => {
            let mut fields = vec![field("ballot", ballot_text(*ballot))];
            let voice = if log {
                fields.push(field("slot", slot.0.to_string()));
                format!(
                    "I voted for your value in slot {} at ballot {}. It is on my disk.",
                    slot.0,
                    show_ballot(*ballot)
                )
            } else {
                format!(
                    "I voted for your value at ballot {}. It is on my disk.",
                    show_ballot(*ballot)
                )
            };
            Content { fields, voice }
        }
        Message::Nack { ballot, slot, .. } => {
            let mut fields = vec![field("refused ballot", ballot_text(*ballot))];
            if log {
                fields.push(field("slot", slot.0.to_string()));
            }
            Content {
                fields,
                voice: format!(
                    "No. I promised a ballot higher than {}. Use a higher ballot.",
                    show_ballot(*ballot)
                ),
            }
        }
        Message::Commit {
            ballot,
            slot,
            command,
            ..
        } => {
            let mut fields = vec![field("ballot", ballot_text(*ballot))];
            if log {
                fields.push(field("slot", slot.0.to_string()));
            }
            fields.push(field("value", value_text(command)));
            let voice = if log {
                format!("Slot {} is chosen: {}.", slot.0, value_text(command))
            } else {
                format!("The chosen value is {}.", value_text(command))
            };
            Content { fields, voice }
        }
        Message::CatchUpRequest { from_slot, .. } => Content {
            fields: vec![field("from slot", from_slot.0.to_string())],
            voice: format!(
                "I am behind. Send me the chosen values from slot {} on.",
                from_slot.0
            ),
        },
        Message::CatchUpResponse { entries, .. } => {
            let chosen: Vec<String> = entries
                .iter()
                .map(|(slot, (_, command))| format!("slot {} = {}", slot.0, value_text(command)))
                .collect();
            Content {
                fields: vec![field("chosen", list_or(&chosen, "none"))],
                voice: if chosen.is_empty() {
                    "I know no chosen value after that slot.".to_string()
                } else {
                    format!("Here are the chosen values: {}.", short_list(&chosen))
                },
            }
        }
        Message::TrimmedTo { point, .. } => Content {
            fields: vec![field("log starts at", point.0.to_string())],
            voice: format!(
                "I removed the slots below {}. Get them from a snapshot.",
                point.0
            ),
        },
        Message::Relinquish {
            ballot,
            to,
            next_slot,
            decided,
            pending,
            ..
        } => Content {
            fields: vec![
                field("ballot", ballot_text(*ballot)),
                field("next leader", format!("node {}", to.0)),
                field("next slot", next_slot.0.to_string()),
                field("decided", decided.len().to_string()),
                field("not decided", pending.len().to_string()),
            ],
            voice: format!(
                "I stop being the leader. Take my ballot {} and go on from slot {}. \
                 You do not need Phase 1.",
                show_ballot(*ballot),
                next_slot.0
            ),
        },
        Message::Heartbeat { ballot, commit, .. } => {
            let mut fields = vec![field("ballot", ballot_text(*ballot))];
            let mut voice = format!(
                "I am still the leader with ballot {}.",
                show_ballot(*ballot)
            );
            if let Some(commit) = commit {
                fields.push(field("chosen up to", commit.0.to_string()));
                let _ = write!(voice, " The slots up to {} are chosen.", commit.0);
            }
            Content { fields, voice }
        }
        Message::HeartbeatAck { ballot, .. } => Content {
            fields: vec![field("ballot", ballot_text(*ballot))],
            voice: format!("I still follow ballot {}.", show_ballot(*ballot)),
        },
        Message::PreRead { ctx, .. } => Content {
            fields: vec![field("read", ctx.to_string())],
            voice: "Before I read: what is the highest slot you voted for?".to_string(),
        },
        Message::PreReadAck { ctx, watermark, .. } => Content {
            fields: vec![
                field("read", ctx.to_string()),
                field(
                    "voted up to",
                    watermark.map_or_else(|| "nothing".to_string(), |slot| slot.0.to_string()),
                ),
            ],
            voice: match watermark {
                Some(slot) => format!("I voted up to slot {}.", slot.0),
                None => "I did not vote for any slot.".to_string(),
            },
        },
        _ => Content {
            fields: Vec::new(),
            voice: String::new(),
        },
    }
}

fn reconfigure(request: &ReconfigureRequest) -> Content {
    match request {
        ReconfigureRequest::Stop { generation, .. } => Content {
            fields: vec![field("generation", generation.0.to_string())],
            voice: format!(
                "Stop serving generation {}, and give me what you hold.",
                generation.0
            ),
        },
        ReconfigureRequest::Bootstrap { bootstrap, .. } => Content {
            fields: vec![
                field("generation", bootstrap.set.generation.0.to_string()),
                field("matchmakers", show_set(bootstrap.set.members())),
            ],
            voice: format!(
                "Hold generation {} as the next one. Do not serve it yet.",
                bootstrap.set.generation.0
            ),
        },
        ReconfigureRequest::DecreePrepare { ballot, .. } => Content {
            fields: vec![field("ballot", ballot_text(*ballot))],
            voice: format!(
                "I want to choose the next matchmakers with ballot {}. Promise it.",
                show_ballot(*ballot)
            ),
        },
        ReconfigureRequest::DecreeAccept {
            ballot, members, ..
        } => Content {
            fields: vec![
                field("ballot", ballot_text(*ballot)),
                field("matchmakers", show_set(members)),
            ],
            voice: format!(
                "Please vote for matchmakers {} at ballot {}.",
                show_set(members),
                show_ballot(*ballot)
            ),
        },
        ReconfigureRequest::Chosen { successor, .. } => Content {
            fields: vec![
                field("generation", successor.generation.0.to_string()),
                field("matchmakers", show_set(successor.members())),
            ],
            voice: format!(
                "Generation {} with matchmakers {} is chosen.",
                successor.generation.0,
                show_set(successor.members())
            ),
        },
    }
}

#[allow(clippy::single_match_else)]
fn reconfigure_reply(answer: &ReconfigureReply) -> Content {
    match answer {
        ReconfigureReply::Stopped { generation, .. } => Content {
            fields: vec![field("generation", generation.0.to_string())],
            voice: format!(
                "I stopped generation {}. Here is what I hold.",
                generation.0
            ),
        },
        ReconfigureReply::Bootstrapped { set, .. } => Content {
            fields: vec![field("generation", set.generation.0.to_string())],
            voice: format!("I hold generation {} as the next one.", set.generation.0),
        },
        ReconfigureReply::Promised { ballot, vote, .. } => {
            let mut fields = vec![field("ballot", ballot_text(*ballot))];
            let voice = match vote {
                Some((at, members)) => {
                    fields.push(field(
                        "vote",
                        format!("{} at {}", show_set(members), show_ballot(*at)),
                    ));
                    format!(
                        "I promise ballot {}. I voted for matchmakers {} at ballot {} before.",
                        show_ballot(*ballot),
                        show_set(members),
                        show_ballot(*at)
                    )
                }
                None => {
                    fields.push(field("vote", "none"));
                    format!(
                        "I promise ballot {}. I did not vote yet.",
                        show_ballot(*ballot)
                    )
                }
            };
            Content { fields, voice }
        }
        ReconfigureReply::Accepted { ballot, .. } => Content {
            fields: vec![field("ballot", ballot_text(*ballot))],
            voice: format!("I voted at ballot {}.", show_ballot(*ballot)),
        },
        ReconfigureReply::Nacked { promised, .. } => Content {
            fields: vec![field("promised", ballot_text(*promised))],
            voice: format!(
                "No. I promised ballot {}. Use a higher ballot.",
                show_ballot(*promised)
            ),
        },
        ReconfigureReply::Learned { activated, at, .. } => Content {
            fields: vec![field("generation", at.0.to_string())],
            voice: if *activated {
                format!("I serve generation {} now.", at.0)
            } else {
                format!(
                    "I wrote down the next generation. I stay at generation {}.",
                    at.0
                )
            },
        },
        ReconfigureReply::Refused { current, .. } => Content {
            fields: vec![field("generation", current.generation.0.to_string())],
            voice: format!("No. I serve generation {}.", current.generation.0),
        },
    }
}

fn field(name: &str, value: impl Into<String>) -> Field {
    Field {
        name: name.to_string(),
        value: value.into(),
    }
}

/// `1.0 (round 1, node 0)`: a ballot with what its two numbers mean.
fn ballot_text(ballot: Ballot) -> String {
    format!(
        "{} (round {}, node {})",
        show_ballot(ballot),
        ballot.round,
        ballot.node.0
    )
}

/// `slot 2 = alpha at 1.0`, one per vote.
fn votes(accepted: &BTreeMap<Slot, (Ballot, Command)>) -> Vec<String> {
    accepted
        .iter()
        .map(|(slot, (at, command))| {
            format!(
                "slot {} = {} at {}",
                slot.0,
                value_text(command),
                show_ballot(*at)
            )
        })
        .collect()
}

/// A value in a sentence: a client value as written, a control command
/// in words.
fn spoken(command: &Command) -> String {
    match command {
        Command::Control(Control::Noop) => "the no-op entry".to_string(),
        _ => value_text(command),
    }
}

fn list_or(items: &[String], empty: &str) -> String {
    if items.is_empty() {
        empty.to_string()
    } else {
        items.join("; ")
    }
}

/// The first few items, then "and N more".
fn short_list(items: &[String]) -> String {
    let mut out = items
        .iter()
        .take(LIST)
        .cloned()
        .collect::<Vec<_>>()
        .join("; ");
    if items.len() > LIST {
        let _ = write!(out, " and {} more", items.len() - LIST);
    }
    out
}

fn plural(n: usize, one: &str, many: &str) -> String {
    if n == 1 {
        format!("1 {one}")
    } else {
        format!("{n} {many}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paros_core::NodeId;
    use paros_core::Value;
    use paros_core::journal::JournalId;
    use paros_core::journal::{Entry, LeaderUuid, Seq};
    use paros_core::matchmaker::{BootstrapPage, MatchRefusal, MatchmakerPhase, Registration};
    use paros_core::membership::{
        AcceptorConfig, MatchmakerGeneration, MatchmakerId, MatchmakerSet, QuorumSystem,
    };

    fn ballot(round: u64, node: u64) -> Ballot {
        Ballot {
            round,
            node: NodeId(node),
        }
    }

    fn write(value: &str) -> Command {
        Command::Write(Entry {
            leader: LeaderUuid(1),
            seq: Seq(0),
            records: vec![Value(value.as_bytes().to_vec())],
        })
    }

    #[test]
    fn a_prepare_asks_to_be_the_leader() {
        let message = Message::Prepare {
            reply_to: NodeId(0),
            ballot: ballot(1, 0),
            from_slot: Slot(3),
            config: None,
        };
        let log = node_message(&message, true);
        assert_eq!(log.fields[0], field("ballot", "1.0 (round 1, node 0)"));
        assert_eq!(log.fields[1], field("from slot", "3"));
        assert!(
            log.voice
                .starts_with("I want to be the leader with ballot 1.0.")
        );
        let decree = node_message(&message, false);
        assert_eq!(decree.fields.len(), 1, "one decree names no slot");
        assert!(!decree.voice.contains("slot"));
    }

    #[test]
    fn a_promise_reports_its_votes() {
        let mut accepted = BTreeMap::new();
        accepted.insert(Slot(0), (ballot(1, 0), write("alpha")));
        let message = Message::Promise {
            from: NodeId(1),
            ballot: ballot(2, 1),
            from_slot: Slot(0),
            accepted,
            faulty: BTreeMap::new(),
            next_from_slot: None,
        };
        let decree = node_message(&message, false);
        assert_eq!(decree.fields[1], field("vote", "alpha at 1.0"));
        assert_eq!(
            decree.voice,
            "I promise ballot 2.1. I voted for alpha at ballot 1.0 before."
        );
        let log = node_message(&message, true);
        assert_eq!(log.fields[2], field("votes", "slot 0 = alpha at 1.0"));
    }

    #[test]
    fn an_accept_names_the_value_and_a_commit_says_it_is_chosen() {
        let accept = Message::Accept {
            reply_to: paros_core::contract::Party::Node(NodeId(0)),
            leader: NodeId(0),
            ballot: ballot(1, 0),
            slot: Slot(2),
            command: write("beta"),
            config: None,
        };
        let content = node_message(&accept, true);
        assert_eq!(
            content.voice,
            "Please vote for beta in slot 2 at ballot 1.0."
        );
        assert!(content.fields.contains(&field("value", "beta")));
        let commit = Message::Commit {
            from: paros_core::contract::Party::Node(NodeId(0)),
            ballot: ballot(1, 0),
            slot: Slot(2),
            command: Command::Control(Control::Noop),
        };
        assert_eq!(node_message(&commit, true).voice, "Slot 2 is chosen: Noop.");
    }

    fn set(generation: u64, members: &[u64]) -> MatchmakerSet {
        MatchmakerSet::new(
            MatchmakerGeneration(generation),
            members.iter().map(|id| MatchmakerId(*id)).collect(),
        )
    }

    #[test]
    fn each_reconfigure_request_names_its_generation_or_ballot() {
        let from = NodeId(0);
        let stop = reconfigure(&ReconfigureRequest::Stop {
            from,
            generation: MatchmakerGeneration(3),
            after: None,
        });
        assert_eq!(stop.fields, vec![field("generation", "3")]);
        assert_eq!(
            stop.voice,
            "Stop serving generation 3, and give me what you hold."
        );
        let bootstrap = reconfigure(&ReconfigureRequest::Bootstrap {
            from,
            bootstrap: BootstrapPage {
                set: set(4, &[2, 1]),
                after: None,
                registries: BTreeMap::new(),
                last: true,
            },
        });
        assert_eq!(
            bootstrap.fields,
            vec![field("generation", "4"), field("matchmakers", "{1, 2}")]
        );
        assert_eq!(
            bootstrap.voice,
            "Hold generation 4 as the next one. Do not serve it yet."
        );
        let prepare = reconfigure(&ReconfigureRequest::DecreePrepare {
            from,
            generation: MatchmakerGeneration(3),
            ballot: ballot(2, 1),
        });
        assert_eq!(
            prepare.fields,
            vec![field("ballot", "2.1 (round 2, node 1)")]
        );
        assert_eq!(
            prepare.voice,
            "I want to choose the next matchmakers with ballot 2.1. Promise it."
        );
        let accept = reconfigure(&ReconfigureRequest::DecreeAccept {
            from,
            generation: MatchmakerGeneration(3),
            ballot: ballot(2, 1),
            members: vec![MatchmakerId(5), MatchmakerId(6)],
        });
        assert_eq!(
            accept.fields,
            vec![
                field("ballot", "2.1 (round 2, node 1)"),
                field("matchmakers", "{5, 6}")
            ]
        );
        assert_eq!(
            accept.voice,
            "Please vote for matchmakers {5, 6} at ballot 2.1."
        );
        let chosen = reconfigure(&ReconfigureRequest::Chosen {
            from,
            generation: MatchmakerGeneration(3),
            successor: set(4, &[5, 6]),
        });
        assert_eq!(
            chosen.fields,
            vec![field("generation", "4"), field("matchmakers", "{5, 6}")]
        );
        assert_eq!(
            chosen.voice,
            "Generation 4 with matchmakers {5, 6} is chosen."
        );
    }

    #[test]
    fn each_reconfigure_reply_speaks_for_its_matchmaker() {
        let matchmaker = MatchmakerId(1);
        let generation = MatchmakerGeneration(3);
        let stopped = reconfigure_reply(&ReconfigureReply::Stopped {
            matchmaker,
            generation,
            after: None,
            next: None,
            registries: BTreeMap::new(),
            successor: None,
            decree_promised: ballot(0, 0),
        });
        assert_eq!(stopped.fields, vec![field("generation", "3")]);
        assert_eq!(
            stopped.voice,
            "I stopped generation 3. Here is what I hold."
        );
        let bootstrapped = reconfigure_reply(&ReconfigureReply::Bootstrapped {
            matchmaker,
            set: set(4, &[1]),
            held: None,
            complete: true,
        });
        assert_eq!(bootstrapped.fields, vec![field("generation", "4")]);
        assert_eq!(bootstrapped.voice, "I hold generation 4 as the next one.");
        let voted = reconfigure_reply(&ReconfigureReply::Promised {
            matchmaker,
            generation,
            ballot: ballot(3, 2),
            vote: Some((ballot(2, 1), vec![MatchmakerId(5)])),
        });
        assert_eq!(
            voted.fields,
            vec![
                field("ballot", "3.2 (round 3, node 2)"),
                field("vote", "{5} at 2.1")
            ]
        );
        assert_eq!(
            voted.voice,
            "I promise ballot 3.2. I voted for matchmakers {5} at ballot 2.1 before."
        );
        let fresh = reconfigure_reply(&ReconfigureReply::Promised {
            matchmaker,
            generation,
            ballot: ballot(3, 2),
            vote: None,
        });
        assert_eq!(fresh.fields[1], field("vote", "none"));
        assert_eq!(fresh.voice, "I promise ballot 3.2. I did not vote yet.");
        let accepted = reconfigure_reply(&ReconfigureReply::Accepted {
            matchmaker,
            generation,
            ballot: ballot(3, 2),
        });
        assert_eq!(
            accepted.fields,
            vec![field("ballot", "3.2 (round 3, node 2)")]
        );
        assert_eq!(accepted.voice, "I voted at ballot 3.2.");
        let nacked = reconfigure_reply(&ReconfigureReply::Nacked {
            matchmaker,
            generation,
            ballot: ballot(3, 2),
            promised: ballot(4, 0),
        });
        assert_eq!(
            nacked.fields,
            vec![field("promised", "4.0 (round 4, node 0)")]
        );
        assert_eq!(
            nacked.voice,
            "No. I promised ballot 4.0. Use a higher ballot."
        );
        let serving = reconfigure_reply(&ReconfigureReply::Learned {
            matchmaker,
            generation,
            activated: true,
            at: MatchmakerGeneration(4),
        });
        assert_eq!(serving.fields, vec![field("generation", "4")]);
        assert_eq!(serving.voice, "I serve generation 4 now.");
        let waiting = reconfigure_reply(&ReconfigureReply::Learned {
            matchmaker,
            generation,
            activated: false,
            at: MatchmakerGeneration(3),
        });
        assert_eq!(
            waiting.voice,
            "I wrote down the next generation. I stay at generation 3."
        );
        let refused = reconfigure_reply(&ReconfigureReply::Refused {
            matchmaker,
            current: set(5, &[1]),
            phase: MatchmakerPhase::Fresh,
            successor: None,
        });
        assert_eq!(refused.fields, vec![field("generation", "5")]);
        assert_eq!(refused.voice, "No. I serve generation 5.");
    }

    #[test]
    fn a_promise_reports_lost_votes_and_a_next_page() {
        let mut faulty = BTreeMap::new();
        faulty.insert(Slot(1), ballot(1, 0));
        faulty.insert(Slot(2), ballot(1, 0));
        let message = Message::Promise {
            from: NodeId(1),
            ballot: ballot(2, 1),
            from_slot: Slot(0),
            accepted: BTreeMap::new(),
            faulty,
            next_from_slot: Some(Slot(9)),
        };
        let content = node_message(&message, true);
        assert!(content.fields.contains(&field("lost votes", "2")));
        assert!(content.fields.contains(&field("more from slot", "9")));
        assert!(content.voice.contains(" My disk lost 2 votes."));
        assert!(content.voice.ends_with(" More votes follow from slot 9."));
    }

    #[test]
    fn a_nack_names_its_slot_only_in_a_log() {
        let nack = Message::Nack {
            from: NodeId(1),
            ballot: ballot(1, 0),
            slot: Slot(4),
        };
        let log = node_message(&nack, true);
        assert_eq!(
            log.fields,
            vec![
                field("refused ballot", "1.0 (round 1, node 0)"),
                field("slot", "4")
            ]
        );
        assert_eq!(node_message(&nack, false).fields.len(), 1);
    }

    #[test]
    fn catch_up_and_trim_messages_say_what_they_carry() {
        let request = node_message(
            &Message::CatchUpRequest {
                from: NodeId(1),
                from_slot: Slot(3),
            },
            true,
        );
        assert_eq!(request.fields, vec![field("from slot", "3")]);
        assert_eq!(
            request.voice,
            "I am behind. Send me the chosen values from slot 3 on."
        );
        let empty = node_message(
            &Message::CatchUpResponse {
                from: NodeId(1),
                entries: BTreeMap::new(),
            },
            true,
        );
        assert_eq!(empty.fields, vec![field("chosen", "none")]);
        assert_eq!(empty.voice, "I know no chosen value after that slot.");
        let mut entries = BTreeMap::new();
        entries.insert(Slot(3), (ballot(1, 0), write("alpha")));
        let full = node_message(
            &Message::CatchUpResponse {
                from: NodeId(1),
                entries,
            },
            true,
        );
        assert_eq!(full.fields, vec![field("chosen", "slot 3 = alpha")]);
        assert_eq!(full.voice, "Here are the chosen values: slot 3 = alpha.");
        let trimmed = node_message(
            &Message::TrimmedTo {
                from: NodeId(1),
                point: Slot(5),
                state: paros_core::journal::JournalState::default(),
            },
            true,
        );
        assert_eq!(trimmed.fields, vec![field("log starts at", "5")]);
        assert_eq!(
            trimmed.voice,
            "I removed the slots below 5. Get them from a snapshot."
        );
    }

    #[test]
    fn a_relinquish_hands_on_the_ballot() {
        let mut decided = BTreeMap::new();
        decided.insert(Slot(0), (ballot(1, 0), write("alpha")));
        let content = node_message(
            &Message::Relinquish {
                from: NodeId(0),
                to: NodeId(2),
                ballot: ballot(1, 0),
                from_slot: Slot(0),
                next_slot: Slot(2),
                decided,
                pending: BTreeMap::new(),
                config: None,
                prior: Vec::new(),
            },
            true,
        );
        assert_eq!(
            content.fields,
            vec![
                field("ballot", "1.0 (round 1, node 0)"),
                field("next leader", "node 2"),
                field("next slot", "2"),
                field("decided", "1"),
                field("not decided", "0"),
            ]
        );
        assert_eq!(
            content.voice,
            "I stop being the leader. Take my ballot 1.0 and go on from slot 2. \
             You do not need Phase 1."
        );
    }

    #[test]
    fn heartbeats_and_pre_reads_speak_plainly() {
        let quiet = node_message(
            &Message::Heartbeat {
                from: NodeId(0),
                ballot: ballot(1, 0),
                commit: None,
                config: None,
                fence: None,
                gc_floor: None,
            },
            true,
        );
        assert_eq!(quiet.fields.len(), 1);
        assert_eq!(quiet.voice, "I am still the leader with ballot 1.0.");
        let committed = node_message(
            &Message::Heartbeat {
                from: NodeId(0),
                ballot: ballot(1, 0),
                commit: Some(Slot(4)),
                config: None,
                fence: None,
                gc_floor: None,
            },
            true,
        );
        assert_eq!(committed.fields[1], field("chosen up to", "4"));
        assert_eq!(
            committed.voice,
            "I am still the leader with ballot 1.0. The slots up to 4 are chosen."
        );
        let ack = node_message(
            &Message::HeartbeatAck {
                from: NodeId(1),
                ballot: ballot(1, 0),
                chosen: None,
                watermark: None,
            },
            true,
        );
        assert_eq!(ack.fields, vec![field("ballot", "1.0 (round 1, node 0)")]);
        assert_eq!(ack.voice, "I still follow ballot 1.0.");
        let pre = node_message(
            &Message::PreRead {
                reply_to: NodeId(7),
                ctx: 11,
            },
            true,
        );
        assert_eq!(pre.fields, vec![field("read", "11")]);
        assert_eq!(
            pre.voice,
            "Before I read: what is the highest slot you voted for?"
        );
        let voted = node_message(
            &Message::PreReadAck {
                from: NodeId(1),
                ctx: 11,
                watermark: Some(Slot(6)),
                config_since: None,
            },
            true,
        );
        assert_eq!(
            voted.fields,
            vec![field("read", "11"), field("voted up to", "6")]
        );
        assert_eq!(voted.voice, "I voted up to slot 6.");
        let none = node_message(
            &Message::PreReadAck {
                from: NodeId(1),
                ctx: 11,
                watermark: None,
                config_since: None,
            },
            true,
        );
        assert_eq!(none.fields[1], field("voted up to", "nothing"));
        assert_eq!(none.voice, "I did not vote for any slot.");
    }

    fn acceptors(ids: &[u64]) -> AcceptorConfig {
        AcceptorConfig::new(
            ids.iter().map(|id| NodeId(*id)).collect(),
            QuorumSystem::Majority,
        )
    }

    fn request(purpose: MatchPurpose) -> MatchRequest {
        MatchRequest {
            from: NodeId(0),
            journal: JournalId(1),
            ballot: ballot(2, 0),
            config: acceptors(&[0, 1, 2]),
            purpose,
            from_ballot: None,
            generation: MatchmakerGeneration(0),
        }
    }

    fn reply(outcome: MatchOutcome) -> MatchReply {
        MatchReply {
            matchmaker: MatchmakerId(1),
            journal: JournalId(1),
            to: NodeId(0),
            ballot: ballot(2, 0),
            generation: MatchmakerGeneration(0),
            outcome,
        }
    }

    #[test]
    fn a_match_request_says_whether_it_writes() {
        let probe = match_request(&request(MatchPurpose::Probe));
        assert_eq!(probe.fields[0], field("asks", "which acceptors are in use"));
        assert_eq!(
            probe.voice,
            "Which acceptors are in use now? Do not write anything down."
        );
        let belief = match_request(&request(MatchPurpose::Register(RegistrationKind::Belief)));
        assert_eq!(
            belief.fields,
            vec![
                field("asks", "write down"),
                field("ballot", "2.0 (round 2, node 0)"),
                field("acceptors", "{0, 1, 2}"),
                field("generation", "0"),
            ]
        );
        assert_eq!(
            belief.voice,
            "I want to use ballot 2.0 with acceptors {0, 1, 2}. Write this down, and \
             tell me which acceptors the lower ballots used."
        );
        let change = match_request(&request(MatchPurpose::Register(
            RegistrationKind::Reconfiguration,
        )));
        assert_eq!(
            change.fields[0],
            field("asks", "write down a new configuration")
        );
    }

    #[test]
    fn a_registered_reply_names_the_lower_ballots_and_the_set_in_use() {
        let registered = |history, effective| {
            match_reply(&reply(MatchOutcome::Registered {
                from_ballot: ballot(0, 0),
                history,
                next_from_ballot: None,
                gc_watermark: ballot(0, 0),
                effective,
            }))
        };
        let first = registered(BTreeMap::new(), None);
        assert_eq!(
            first.fields,
            vec![
                field("answer", "registered"),
                field("ballot", "2.0 (round 2, node 0)"),
                field("lower ballots", "none"),
            ]
        );
        assert_eq!(
            first.voice,
            "I wrote down ballot 2.0. I hold no lower ballot."
        );
        let mut history = BTreeMap::new();
        history.insert(
            ballot(1, 1),
            Registration {
                config: acceptors(&[1, 2, 3]),
                kind: RegistrationKind::Belief,
            },
        );
        let later = registered(history, Some((ballot(1, 1), acceptors(&[1, 2, 3]))));
        assert_eq!(
            later.fields[2..],
            [
                field("lower ballots", "1.1 → {1, 2, 3}"),
                field("in use", "{1, 2, 3} since 1.1"),
            ]
        );
        assert_eq!(
            later.voice,
            "I wrote down ballot 2.0. Before it, ballot 1.1 used acceptors {1, 2, 3}. Send \
             Phase 1 to those acceptors too."
        );
    }

    #[test]
    fn a_probe_reply_names_the_set_in_use() {
        let fresh = match_reply(&reply(MatchOutcome::Probed { effective: None }));
        assert_eq!(fresh.fields[0], field("answer", "in use"));
        assert_eq!(fresh.fields[2], field("in use", "the first configuration"));
        assert_eq!(
            fresh.voice,
            "Nobody changed the acceptors. The first configuration is in use."
        );
        let changed = match_reply(&reply(MatchOutcome::Probed {
            effective: Some((ballot(3, 1), acceptors(&[1, 4]))),
        }));
        assert_eq!(changed.fields[2], field("in use", "{1, 4} since 3.1"));
        assert_eq!(
            changed.voice,
            "Acceptors {1, 4} are in use since ballot 3.1."
        );
    }

    #[test]
    fn a_refused_reply_says_why() {
        let refused = |refusal| match_reply(&reply(MatchOutcome::Refused(refusal))).voice;
        let set = set(2, &[0, 3]);
        assert_eq!(
            refused(MatchRefusal::Stale {
                highest: ballot(4, 1)
            }),
            "No. I refuse ballot 2.0: it has already registered 4.1, and a ballot must be \
             above every registered one."
        );
        assert_eq!(
            refused(MatchRefusal::BelowWatermark {
                watermark: ballot(3, 0)
            }),
            "No. I refuse ballot 2.0: its watermark is 3.0, and nothing below a watermark \
             registers again."
        );
        assert_eq!(
            refused(MatchRefusal::Stopped {
                successor: Some(set.clone())
            }),
            "No. I refuse ballot 2.0: it is frozen, and its successor is generation 2 = {0, 3}."
        );
        assert_eq!(
            refused(MatchRefusal::Stopped { successor: None }),
            "No. I refuse ballot 2.0: it is frozen, and no successor is chosen yet."
        );
        assert_eq!(
            refused(MatchRefusal::Generation { current: set }),
            "No. I refuse ballot 2.0: it now serves generation 2 = {0, 3}."
        );
        assert_eq!(
            refused(MatchRefusal::Inactive),
            "No. I refuse ballot 2.0: it serves no generation: it is a spare."
        );
        let first = match_reply(&reply(MatchOutcome::Refused(MatchRefusal::Inactive)));
        assert_eq!(first.fields[0], field("answer", "refused"));
    }

    #[test]
    fn a_gc_ack_says_whether_it_forgot() {
        let ack = |applied| {
            gc_ack(&GcAck {
                matchmaker: MatchmakerId(1),
                journal: JournalId(1),
                generation: MatchmakerGeneration(0),
                applied,
                watermark: ballot(2, 0),
                newer: None,
                stopped: false,
            })
        };
        let done = ack(true);
        assert_eq!(
            done.fields,
            vec![
                field("answer", "done"),
                field("watermark", "2.0 (round 2, node 0)")
            ]
        );
        assert_eq!(done.voice, "Done. I forgot everything below ballot 2.0.");
        let refused = ack(false);
        assert_eq!(refused.fields[0], field("answer", "refused"));
        assert_eq!(refused.voice, "No. My watermark stays at 2.0.");
    }

    #[test]
    fn an_accept_names_its_leader_only_when_another_node_minted_the_ballot() {
        let accept = |leader| Message::Accept {
            reply_to: paros_core::contract::Party::Node(NodeId(0)),
            leader: NodeId(leader),
            ballot: ballot(1, 0),
            slot: Slot(2),
            command: write("beta"),
            config: None,
        };
        let own = node_message(&accept(0), true);
        assert!(own.fields.iter().all(|field| field.name != "leader"));
        let handed = node_message(&accept(3), true);
        assert_eq!(handed.fields.last(), Some(&field("leader", "node 3")));
    }

    #[test]
    fn a_long_list_is_cut() {
        let items: Vec<String> = (0..5).map(|i| i.to_string()).collect();
        assert_eq!(short_list(&items), "0; 1; 2 and 2 more");
        assert_eq!(list_or(&[], "none"), "none");
    }
}
