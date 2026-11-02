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
    MatchOutcome, MatchPurpose, ReconfigureReply, ReconfigureRequest, RegistrationKind,
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
#[allow(clippy::too_many_lines, clippy::single_match_else)]
pub fn content(entry: &InFlight, log: bool) -> Content {
    match &entry.envelope {
        Envelope::Node(message) => node_message(message, log),
        Envelope::Match(request) => {
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
        Envelope::MatchReply(answer) => {
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
                            "Nobody changed the acceptors. The first configuration is in use."
                                .to_string()
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
        Envelope::Gc(request) => Content {
            fields: vec![field("forget below", ballot_text(request.watermark))],
            voice: format!(
                "My ballot {} is safe now. Forget the configurations below it.",
                show_ballot(request.watermark)
            ),
        },
        Envelope::GcAck(ack) => {
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
        Envelope::Reconfigure(request) => reconfigure(request),
        Envelope::ReconfigureReply(answer) => reconfigure_reply(answer),
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
    use paros_core::journal::{Entry, LeaderUuid, Seq};

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

    #[test]
    fn a_long_list_is_cut() {
        let items: Vec<String> = (0..5).map(|i| i.to_string()).collect();
        assert_eq!(short_list(&items), "0; 1; 2 and 2 more");
        assert_eq!(list_or(&[], "none"), "none");
    }
}
