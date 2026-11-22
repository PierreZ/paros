//! The words of the seal's step-through: the name of each lane, message and
//! answer, and what a message carries and its sender says. Every word is
//! read off a role's message, a register change or an answer of the fold.

use paros_journal::register::{Ballot, Change};
use paros_journal::roles::{AcceptorOp, LeaderPhase, Message, ReplicaOp};
use paros_journal::segment::SegmentState;
use paros_journal::{Answer, Command, Fence, NodeId, Reply, ResolveAnswer, Write};

use super::{ACCEPTORS, PROPOSERS, Payload};
use crate::steps::{Field, LogCell, Stage};

pub(super) fn lane_of(node: NodeId) -> String {
    let n = node.0;
    if ACCEPTORS.contains(&n) {
        format!("a{n}")
    } else if PROPOSERS.contains(&n) {
        format!("p{n}")
    } else {
        format!("r{n}")
    }
}

/// The node of a lane id (`a1`, `p5`, `r7`).
///
/// # Panics
///
/// When the lane is not a node.
pub(super) fn node_of(lane: &str) -> NodeId {
    NodeId(
        lane[1..]
            .parse()
            .unwrap_or_else(|_| panic!("{lane} is not a node lane")),
    )
}

/// A lane in a sentence: `Acceptor 1`, `Proposer 6`, `The client`.
pub(super) fn party(lane: &str) -> String {
    match lane {
        "c" => "The client".to_string(),
        "ctl" => "The controller".to_string(),
        "m" => "The matchmakers".to_string(),
        id => {
            let name = match &id[..1] {
                "a" => "Acceptor",
                "p" => "Proposer",
                _ => "Replica",
            };
            format!("{name} {}", &id[1..])
        }
    }
}

pub(super) fn lower(text: &str) -> String {
    let mut chars = text.chars();
    chars.next().map_or_else(String::new, |first| {
        first.to_lowercase().chain(chars).collect()
    })
}

pub(super) fn ballot_text(ballot: Ballot) -> String {
    format!("{}.{}", ballot.round, ballot.node.0)
}

/// The name of a command: the payload of a write, `no-op`, `set leader A`.
pub(super) fn value_name(command: &Command) -> String {
    match command {
        Command::Write(write) => write
            .records
            .iter()
            .map(|record| String::from_utf8_lossy(&record.payload).into_owned())
            .collect::<Vec<_>>()
            .join("+"),
        Command::Noop => "no-op".to_string(),
        Command::SetLeader { .. } => "set leader A".to_string(),
        Command::Truncate { .. } => "truncate".to_string(),
        Command::Upgrade { .. } => "upgrade".to_string(),
    }
}

pub(super) fn cell(slot: u64, command: &Command, state: &str) -> LogCell {
    LogCell {
        slot,
        value: value_name(command),
        state: state.to_string(),
        control: !matches!(command, Command::Write(_)),
    }
}

pub(super) fn phase_text(phase: &LeaderPhase) -> String {
    match phase {
        LeaderPhase::Phase1 => "runs Phase 1".to_string(),
        LeaderPhase::Sealing { end } => format!("seals the segment before at slot {}", end.0),
        LeaderPhase::Open { start } => format!("writes, its segment from slot {}", start.0),
        LeaderPhase::Stopped => "stopped".to_string(),
    }
}

pub(super) fn state_text(state: SegmentState) -> &'static str {
    match state {
        SegmentState::Pending => "pending",
        SegmentState::InForce => "in force",
        SegmentState::Sealed => "sealed",
        SegmentState::Retired => "retired",
        SegmentState::Offloaded => "offloaded",
        SegmentState::Trimmed => "trimmed",
    }
}

pub(super) fn answer_text(answer: &Answer) -> String {
    match answer {
        Answer::Written { seq, .. } => format!("written at seq {}", seq.0),
        Answer::Duplicate { seq, .. } => format!("written before, at seq {}", seq.0),
        Answer::Below(_) => "Below, in an older segment".to_string(),
        Answer::LeaderSet(_) => "leader set".to_string(),
        Answer::Noop => "no-op".to_string(),
        other => format!("{other:?}"),
    }
}

/// What the client heard: the kind of the answer first.
pub(super) fn reply_text(reply: &Reply) -> String {
    match reply {
        Reply::Answer(Answer::Written { seq, .. }) => format!("Written at seq {}", seq.0),
        Reply::Answer(Answer::Duplicate { seq, .. }) => format!("Duplicate at seq {}", seq.0),
        Reply::Answer(Answer::Below(_)) => "Below: ask an older segment".to_string(),
        Reply::Answer(Answer::LeaderSet(_)) => "LeaderSet".to_string(),
        Reply::Answer(other) => format!("{other:?}"),
        Reply::Unknown => "Unknown".to_string(),
    }
}

pub(super) fn resolved_text(answer: ResolveAnswer) -> String {
    match answer {
        ResolveAnswer::Written { seq, .. } => format!("Written at seq {}", seq.0),
        ResolveAnswer::NotWritten => "NotWritten".to_string(),
        ResolveAnswer::Truncated => "Truncated".to_string(),
        ResolveAnswer::NotCovered => "NotCovered".to_string(),
    }
}

pub(super) fn change_text(change: &Change) -> String {
    match change {
        Change::Elect { .. } => "a new controller term".to_string(),
        Change::Reshape { opening, .. } => {
            let members: Vec<String> = opening.grid.columns()[0]
                .iter()
                .map(|node| node.0.to_string())
                .collect();
            format!(
                "the next segment, pending, with acceptors {} and replica {}",
                members.join(", "),
                opening.replicas[0].0
            )
        }
        Change::Seal { segment, end } => format!(
            "segment {} ends at slot {}, and segment {} is in force from slot {}",
            segment.0,
            end.0,
            segment.0 + 1,
            end.0
        ),
        Change::Boundary { segment, state } => format!(
            "the boundary state of segment {}: next_seq {}",
            segment.0, state.next_seq.0
        ),
        other => format!("{other:?}"),
    }
}

pub(super) fn acceptor_write(op: &AcceptorOp) -> String {
    match op {
        AcceptorOp::SetPromise(ballot) => format!("promise {}", ballot_text(*ballot)),
        AcceptorOp::AppendAccepted {
            slot,
            ballot,
            value,
        } => format!(
            "slot {}: vote {} at {}",
            slot.0,
            value_name(value),
            ballot_text(*ballot)
        ),
    }
}

pub(super) fn replica_write(op: &ReplicaOp) -> String {
    match op {
        ReplicaOp::Base { boundary, start } => format!(
            "base: next_seq {} from slot {}",
            boundary.next_seq.0, start.0
        ),
        ReplicaOp::Chosen { slot, command } => {
            format!("slot {}: chosen {}", slot.0, value_name(command))
        }
    }
}

pub(super) fn kind_of(payload: &Payload) -> &'static str {
    match payload {
        Payload::Role(Message::Prepare { .. }) => "Prepare",
        Payload::Role(Message::Promise { .. }) => "Promise",
        Payload::Role(Message::Nack { .. }) => "Nack",
        Payload::Role(Message::Accept { .. }) => "Accept",
        Payload::Role(Message::Accepted { .. }) => "Accepted",
        Payload::Role(Message::Commit { .. }) => "Commit",
        Payload::Role(Message::Applied { .. }) => "Applied",
        Payload::Role(Message::Pull { .. }) => "Pull",
        Payload::Write { .. } => "Write",
        Payload::Reply { .. } => "Reply",
        Payload::Change(Change::Reshape { .. }) => "Reshape",
        Payload::Change(Change::Seal { .. }) => "Seal",
        Payload::Change(Change::Boundary { .. }) => "Boundary",
        Payload::Change(_) => "Change",
        Payload::Done { .. } => "Done",
        Payload::Resolve { .. } => "Resolve",
        Payload::Resolved(_) => "Resolved",
    }
}

/// The colour family of a message on the page.
pub(super) fn phase_of(payload: &Payload) -> &'static str {
    match payload {
        Payload::Role(Message::Prepare { .. }) => "prepare",
        Payload::Role(Message::Promise { .. }) => "promise",
        Payload::Role(Message::Nack { .. }) => "nack",
        Payload::Role(Message::Accept { .. }) => "accept",
        Payload::Role(Message::Accepted { .. }) => "accepted",
        Payload::Role(Message::Commit { .. } | Message::Applied { .. }) => "commit",
        Payload::Role(Message::Pull { .. }) => "catchup",
        Payload::Write { .. } | Payload::Reply { .. } => "other",
        Payload::Change(_) | Payload::Done { .. } => "reconfigure",
        Payload::Resolve { .. } | Payload::Resolved(_) => "read",
    }
}

pub(super) fn is_reply(payload: &Payload) -> bool {
    matches!(
        payload,
        Payload::Role(
            Message::Promise { .. }
                | Message::Nack { .. }
                | Message::Accepted { .. }
                | Message::Applied { .. }
        ) | Payload::Reply { .. }
            | Payload::Done { .. }
            | Payload::Resolved(_)
    )
}

pub(super) fn label_of(payload: &Payload) -> String {
    match payload {
        Payload::Role(Message::Prepare { ballot, from }) => {
            format!("Prepare {} · from slot {}", ballot_text(*ballot), from.0)
        }
        Payload::Role(Message::Promise { ballot, page, .. }) => {
            let votes: Vec<String> = page
                .votes
                .iter()
                .map(|(slot, (_, command))| format!("{} at {}", value_name(command), slot.0))
                .collect();
            if votes.is_empty() {
                format!("Promise {} · no vote", ballot_text(*ballot))
            } else {
                format!("Promise {} · {}", ballot_text(*ballot), votes.join(", "))
            }
        }
        Payload::Role(Message::Nack { promised, .. }) => {
            format!("Nack · promised {}", ballot_text(*promised))
        }
        Payload::Role(Message::Accept {
            ballot,
            slot,
            command,
        }) => format!(
            "Accept {} · slot {} · {}",
            ballot_text(*ballot),
            slot.0,
            value_name(command)
        ),
        Payload::Role(Message::Accepted { ballot, slot }) => {
            format!("Accepted {} · slot {}", ballot_text(*ballot), slot.0)
        }
        Payload::Role(Message::Commit {
            segment,
            slot,
            command,
        }) => format!(
            "Commit · segment {} · slot {} · {}",
            segment.0,
            slot.0,
            value_name(command)
        ),
        Payload::Role(Message::Applied { slot, answer, .. }) => {
            format!("Applied · slot {} · {}", slot.0, answer_text(answer))
        }
        Payload::Role(Message::Pull { segment, from }) => {
            format!("Pull · segment {} · from slot {}", segment.0, from.0)
        }
        Payload::Write { command, .. } => match command {
            Command::Write(write) => match write.fence {
                Fence::Leader { expected_seq, .. } => {
                    format!("Write {} · seq {}", value_name(command), expected_seq.0)
                }
                Fence::Open => format!("Write {}", value_name(command)),
            },
            other => value_name(other),
        },
        Payload::Reply { reply, .. } => format!("Reply · {}", reply_text(reply)),
        Payload::Change(Change::Reshape { .. }) => "Reshape · segment 2".to_string(),
        Payload::Change(Change::Seal { segment, end }) => {
            format!("Seal · segment {} at slot {}", segment.0, end.0)
        }
        Payload::Change(Change::Boundary { segment, state }) => format!(
            "Boundary · segment {} · next_seq {}",
            segment.0, state.next_seq.0
        ),
        Payload::Change(other) => format!("{other:?}"),
        Payload::Done { refused: None, .. } => "Done".to_string(),
        Payload::Done { .. } => "Refused".to_string(),
        Payload::Resolve { write } => match write.fence {
            Fence::Leader { expected_seq, .. } => format!(
                "Resolve {} · seq {}",
                value_name(&Command::Write(write.clone())),
                expected_seq.0
            ),
            Fence::Open => "Resolve".to_string(),
        },
        Payload::Resolved(answer) => format!("Resolved · {}", resolved_text(*answer)),
    }
}

/// The stage a message opens: `None` continues the stage before it.
pub(super) fn stage_of(payload: &Payload) -> Option<Stage> {
    let stage = |title: String, about: &str| {
        Some(Stage {
            title,
            about: about.to_string(),
        })
    };
    match payload {
        Payload::Role(Message::Prepare { ballot, .. }) => stage(
            format!("Phase 1 · ballot {}", ballot_text(*ballot)),
            "The new leader asks every acceptor of every segment it reads for a promise and for its votes.",
        ),
        Payload::Write { command, .. } => stage(
            format!("Write {}", value_name(command)),
            "The leader puts the write in a slot, two of the three acceptors of its column vote, and the replica of the slot's segment applies it.",
        ),
        Payload::Change(Change::Reshape { .. }) => stage(
            "The reshape".to_string(),
            "The controller writes the next segment, pending, by compare-and-set on the register.",
        ),
        Payload::Change(Change::Seal { .. }) => stage(
            "The seal".to_string(),
            "The first leader of the new segment writes the end of the old one, once, by compare-and-set.",
        ),
        Payload::Change(Change::Boundary { .. }) => stage(
            "The boundary state".to_string(),
            "The replica that applied every slot of the sealed segment writes the journal state at the end into its record.",
        ),
        Payload::Resolve { .. } => stage(
            "Resolve".to_string(),
            "The client asks a replica of the old segment whether its write took its seq.",
        ),
        _ => None,
    }
}

/// What a message carries, and what its sender says.
pub(super) fn content(payload: &Payload) -> (Vec<Field>, String) {
    match payload {
        Payload::Role(message) => role_content(message),
        other => call_content(other),
    }
}

fn field(name: &str, value: String) -> Field {
    Field {
        name: name.to_string(),
        value,
    }
}

/// What a message between two roles carries.
/// What a message of Phase 1 carries.
fn phase1_content(message: &Message) -> (Vec<Field>, String) {
    match message {
        Message::Prepare { ballot, from } => (
            vec![
                field("ballot", ballot_text(*ballot)),
                field("from slot", from.0.to_string()),
            ],
            format!(
                "I lead with ballot {}. Promise me, and send me your votes from slot {}.",
                ballot_text(*ballot),
                from.0
            ),
        ),
        Message::Promise { ballot, page, .. } => {
            let votes: Vec<String> = page
                .votes
                .iter()
                .map(|(slot, (b, command))| {
                    format!(
                        "slot {}: {} at {}",
                        slot.0,
                        value_name(command),
                        ballot_text(*b)
                    )
                })
                .collect();
            let said = if votes.is_empty() {
                "I hold no vote there.".to_string()
            } else {
                format!("My votes: {}.", votes.join(", "))
            };
            (
                vec![
                    field("ballot", ballot_text(*ballot)),
                    field(
                        "votes",
                        if votes.is_empty() {
                            "none".to_string()
                        } else {
                            votes.join(", ")
                        },
                    ),
                ],
                format!("I promise ballot {}. {said}", ballot_text(*ballot)),
            )
        }
        Message::Nack { ballot, promised } => (
            vec![
                field("ballot", ballot_text(*ballot)),
                field("promised", ballot_text(*promised)),
            ],
            format!(
                "No: I promised ballot {}, above your {}.",
                ballot_text(*promised),
                ballot_text(*ballot)
            ),
        ),
        _ => unreachable!("a message of Phase 1"),
    }
}

fn role_content(message: &Message) -> (Vec<Field>, String) {
    match message {
        Message::Prepare { .. } | Message::Promise { .. } | Message::Nack { .. } => {
            phase1_content(message)
        }
        Message::Accept {
            ballot,
            slot,
            command,
        } => (
            vec![
                field("ballot", ballot_text(*ballot)),
                field("slot", slot.0.to_string()),
                field("value", value_name(command)),
            ],
            format!(
                "Vote for {} at slot {}, ballot {}.",
                value_name(command),
                slot.0,
                ballot_text(*ballot)
            ),
        ),
        Message::Accepted { ballot, slot } => (
            vec![
                field("ballot", ballot_text(*ballot)),
                field("slot", slot.0.to_string()),
            ],
            format!(
                "My vote for slot {} at ballot {} is on disk.",
                slot.0,
                ballot_text(*ballot)
            ),
        ),
        Message::Commit {
            segment,
            slot,
            command,
        } => (
            vec![
                field("segment", segment.0.to_string()),
                field("slot", slot.0.to_string()),
                field("value", value_name(command)),
            ],
            format!(
                "Slot {} of segment {} chose {}.",
                slot.0,
                segment.0,
                value_name(command)
            ),
        ),
        Message::Applied { slot, answer, .. } => (
            vec![
                field("slot", slot.0.to_string()),
                field("answer", answer_text(answer)),
            ],
            format!("I applied slot {}: {}.", slot.0, answer_text(answer)),
        ),
        Message::Pull { segment, from } => (
            vec![
                field("segment", segment.0.to_string()),
                field("from slot", from.0.to_string()),
            ],
            format!(
                "Send me the slots of segment {} from slot {}.",
                segment.0, from.0
            ),
        ),
    }
}

/// What a client call or a register call carries.
fn call_content(payload: &Payload) -> (Vec<Field>, String) {
    match payload {
        Payload::Role(message) => role_content(message),
        Payload::Write { command, .. } => {
            let seq = match command {
                Command::Write(Write {
                    fence: Fence::Leader { expected_seq, .. },
                    ..
                }) => expected_seq.0.to_string(),
                _ => "-".to_string(),
            };
            (
                vec![
                    field("value", value_name(command)),
                    field("expected_seq", seq.clone()),
                    field("leader", "A".to_string()),
                ],
                format!("Write {} at seq {seq}, as leader A.", value_name(command)),
            )
        }
        Payload::Reply { reply, .. } => (
            vec![field("answer", reply_text(reply))],
            match reply {
                Reply::Unknown => "I do not know what happened to your write.".to_string(),
                Reply::Answer(answer) => format!("Your write: {}.", answer_text(answer)),
            },
        ),
        Payload::Change(change) => (
            vec![field("change", change_text(change))],
            format!("Write this, if the guard holds: {}.", change_text(change)),
        ),
        Payload::Done { change, refused } => match refused {
            None => (
                vec![field("done", change_text(change))],
                format!("Done: {}.", change_text(change)),
            ),
            Some(why) => (
                vec![field("refused", format!("{why:?}"))],
                format!("Refused: {why:?}."),
            ),
        },
        Payload::Resolve { write } => {
            let seq = match write.fence {
                Fence::Leader { expected_seq, .. } => expected_seq.0.to_string(),
                Fence::Open => "-".to_string(),
            };
            let name = value_name(&Command::Write(write.clone()));
            (
                vec![
                    field("value", name.clone()),
                    field("expected_seq", seq.clone()),
                ],
                format!("Did {name} take seq {seq} in your segment?"),
            )
        }
        Payload::Resolved(answer) => (
            vec![field("answer", resolved_text(*answer))],
            match answer {
                ResolveAnswer::Written { seq, .. } => {
                    format!("Yes: it is written at seq {}.", seq.0)
                }
                other => format!("{}.", resolved_text(*other)),
            },
        ),
    }
}
