//! What the step-through says in plain words (#811 (Paxos flavor pages)).
//!
//! The engine's narration is precise and long: it is for a reader who already
//! knows Paxos. A page first shows one short sentence per step ("Acceptor 3
//! votes for alpha at ballot 1.5."), the phase the step belongs to, the state
//! of each lane as short lines, and what is chosen so far. The engine's notes
//! stay under the step as the details.
//!
//! Every word here is read off the world before and after the step: a lane's
//! promise, its votes, its role, what is chosen. Nothing is guessed from the
//! message alone.

use std::collections::BTreeSet;
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use crate::paxos::view::{
    AttemptView, MessageView, NodeFlavour, NodeView, PartyView, QuorumKindView, RoleView, WorldView,
};

/// The group of steps a step belongs to: "Phase 1 · ballot 1.5".
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stage {
    /// A short name, e.g. `Phase 1 · ballot 1.5`. Two steps with the same
    /// title next to each other are one group.
    pub title: String,
    /// One sentence on what the group does.
    pub about: String,
}

/// The ballot the empty world starts from: no promise yet.
const NO_BALLOT: &str = "0.0";

/// The stage of a message, from its family. `None` for a family that
/// continues the stage before it (a catch-up request, a refusal).
#[must_use]
pub fn stage_of(message: &MessageView, log: bool) -> Option<Stage> {
    let ballot = message.ballot.clone().unwrap_or_default();
    let (title, about) = match message.phase.as_str() {
        "prepare" | "promise" if log => (
            format!("Phase 1 · ballot {ballot}"),
            "Prepare and Promise. The candidate asks the nodes to promise its ballot, and \
             it needs promises from a quorum. Each promise reports the votes of the node. \
             The candidate also asks for the chosen slots that it does not have \
             (CatchUpRequest).",
        ),
        "prepare" | "promise" => (
            format!("Phase 1 · ballot {ballot}"),
            "Prepare and Promise. The proposer asks the acceptors to promise its ballot. \
             It needs promises from a quorum.",
        ),
        "heartbeat" => (
            "Heartbeat".to_string(),
            "The leader tells each node that it is alive and how far the log is chosen. A \
             node that is behind asks for the slots that it does not have (catch-up).",
        ),
        "read" => (
            "Read".to_string(),
            "A node asks a quorum for the highest slot that each one voted in. It answers \
             the read when it has applied every slot up to the highest one. No leader takes \
             part.",
        ),
        "accept" | "accepted" => (
            format!("Phase 2 · ballot {ballot}"),
            "Accept and Accepted. The proposer asks the acceptors to vote for one value. \
             When a quorum votes for it, the value is chosen.",
        ),
        // Single-decree Paxos has a proposer, not a leader (#953 (small
        // website fixes)).
        "commit" if log => (
            "Commit".to_string(),
            "The leader tells the other nodes which value is chosen.",
        ),
        "commit" => (
            "Commit".to_string(),
            "The proposer tells the acceptors which value is chosen.",
        ),
        "match" => (
            format!("Matchmakers · ballot {ballot}"),
            "The new leader writes down which acceptors its ballot uses, and it learns \
             which acceptors the older ballots used.",
        ),
        "gc" => (
            format!("Clean-up · ballot {ballot}"),
            "The matchmakers forget the configurations below this ballot.",
        ),
        "handoff" => (
            "Hand-off".to_string(),
            "The leader gives its ballot to the next leader. The next leader does not \
             run Phase 1.",
        ),
        _ => return None,
    };
    Some(Stage {
        title,
        about: about.to_string(),
    })
}

/// Give every step a stage: a message without its own continues the stage
/// before it, and an event opens the stage after it.
pub fn fill_stages(stages: &mut [Option<Stage>]) {
    let mut last: Option<Stage> = None;
    let mut pending = Vec::new();
    for i in 0..stages.len() {
        match stages[i].clone() {
            Some(stage) => {
                for j in pending.drain(..) {
                    stages[j] = Some(stage.clone());
                }
                last = Some(stage);
            }
            None => pending.push(i),
        }
    }
    // Steps at the end with no stage after them keep the one before.
    for j in pending {
        stages[j].clone_from(&last);
    }
}

/// The name of a party in a sentence: `Acceptor 3`, `Proposer 5`, `Node 1`,
/// `Matchmaker 0`.
#[must_use]
pub fn party_name(world: &WorldView, party: PartyView, id: u64) -> String {
    match party {
        PartyView::Matchmaker => format!("Matchmaker {id}"),
        PartyView::Node => match world.nodes.iter().find(|node| node.id == id) {
            Some(node) => match node.flavour {
                NodeFlavour::Proposer => format!("Proposer {id}"),
                NodeFlavour::Acceptor => format!("Acceptor {id}"),
                NodeFlavour::Colocated => format!("Node {id}"),
            },
            None => format!("Node {id}"),
        },
    }
}

/// What is chosen in the world, in the words of the step-through:
/// `alpha` for one decree, `slot 0 = alpha` for a log.
#[must_use]
pub fn chosen(world: &WorldView) -> Vec<String> {
    if let Some(chosen) = &world.chosen {
        return vec![chosen.value.clone()];
    }
    let known: BTreeSet<(u64, String)> = world
        .nodes
        .iter()
        .flat_map(|node| node.accepted.iter())
        .filter(|record| record.chosen)
        .map(|record| (record.slot, record.value.clone()))
        .collect();
    known
        .into_iter()
        .map(|(slot, value)| format!("slot {slot} = {value}"))
        .collect()
}

/// The lines under a lane's name: what a reader must see change.
#[must_use]
pub fn node_lines(node: &NodeView) -> Vec<String> {
    if node.retired {
        return vec!["retired".to_string()];
    }
    if !node.alive {
        return vec!["down".to_string()];
    }
    match node.flavour {
        NodeFlavour::Proposer => {
            let what = match node.attempt {
                None | Some(AttemptView::Idle) => return vec!["idle".to_string()],
                Some(AttemptView::Phase1) => "phase 1",
                Some(AttemptView::Phase2) => "phase 2",
                Some(AttemptView::Preempted) => "stopped",
                Some(AttemptView::Won) => "chosen",
            };
            let mut lines = Vec::new();
            if let Some(ballot) = &node.ballot {
                lines.push(format!("ballot {ballot}"));
            }
            lines.push(what.to_string());
            lines
        }
        NodeFlavour::Acceptor => {
            let mut lines = vec![promise_line(node)];
            match node.accepted.first() {
                Some(record) => {
                    lines.push(format!("vote {}", record.value));
                    lines.push(format!("at {}", record.ballot.clone().unwrap_or_default()));
                }
                None => lines.push("no vote".to_string()),
            }
            lines
        }
        NodeFlavour::Colocated => {
            let ballot = node.ballot.clone().unwrap_or_default();
            let first = match node.role {
                Some(RoleView::Leader) => format!("leader {ballot}"),
                Some(RoleView::Candidate) => format!("candidate {ballot}"),
                Some(RoleView::Follower) | None => promise_line(node),
            };
            // The slots are in the card's log strip (`Step::logs`).
            vec![first]
        }
    }
}

fn promise_line(node: &NodeView) -> String {
    match node.promised.as_deref() {
        None | Some(NO_BALLOT) => "no promise".to_string(),
        Some(promised) => format!("promise {promised}"),
    }
}

/// One short sentence on what a message step did, read off the world before
/// and after it.
#[must_use]
pub fn says_message(
    before: &WorldView,
    after: &WorldView,
    message: &MessageView,
    lost: bool,
) -> String {
    let from = party_name(after, message.from_party, message.from);
    let to = party_name(after, message.to_party, message.to);
    let ballot = message.ballot.clone().unwrap_or_default();
    if lost {
        return format!(
            "The {} from {from} to {to} is lost. {to} does not get it.",
            message.kind
        );
    }
    let node_before = node(before, message.to_party, message.to);
    let node_after = node(after, message.to_party, message.to);
    let mut out = match message.kind.as_str() {
        "Prepare" => prepare(&to, &ballot, node_before, node_after),
        "Promise" => {
            let mut out = format!("{to} gets a promise for ballot {ballot} from {from}.");
            let sender = node(after, message.from_party, message.from);
            if let Some(record) = sender
                .filter(|sender| sender.flavour == NodeFlavour::Acceptor)
                .and_then(|sender| sender.accepted.first())
            {
                let _ = write!(
                    out,
                    " The promise carries a vote for {} at ballot {}. A proposer must propose \
                     the value of the highest vote it hears of, not its own value.",
                    record.value,
                    record.ballot.clone().unwrap_or_default()
                );
            }
            out
        }
        "Accept" => accept(&to, &ballot, message.slot, node_before, node_after),
        "Accepted" => format!("{to} gets a vote from {from}."),
        "Nack" => format!(
            "{to} gets a refusal from {from}: {from} promised a ballot higher than {ballot}."
        ),
        "Commit" => commit(after, &to, message.slot, node_after),
        "CatchUpRequest" => {
            format!("{from} asks {to} for the chosen slots that {from} does not have yet.")
        }
        "CatchUpResponse" => {
            format!("{to} gets the chosen slots that it did not have from {from}.")
        }
        "TrimmedTo" => format!(
            "{to} learns that {from} dropped the slots below slot {}. {to} jumps to that slot.",
            message.slot.unwrap_or_default()
        ),
        "Heartbeat" => {
            format!("{to} hears from the leader, {from}. The beat says how far the log is chosen.")
        }
        "HeartbeatAck" => format!("{to} gets an answer to its beat from {from}."),
        "PreRead" => format!("{from} asks {to} for the highest slot that {to} voted in."),
        "PreReadAck" => match message.slot {
            Some(slot) => format!("{to} learns that {from} voted up to slot {slot}."),
            None => format!("{to} learns that {from} has no vote."),
        },
        "MatchRequest" => {
            let wrote = matchmaker_changed(before, after, message.to_party, message.to);
            if wrote {
                format!("{to} writes down the acceptors of ballot {ballot}, then answers {from}.")
            } else {
                format!("{to} tells {from} which acceptors are in use now.")
            }
        }
        "MatchReply" | "GcAck" => format!("{to} gets an answer from {from}."),
        "GcRequest" => format!("{to} forgets the configurations below ballot {ballot}."),
        "Relinquish" => {
            format!("{to} takes over ballot {ballot} from {from}. It does not run Phase 1.")
        }
        other => format!("{to} gets {other} from {from}."),
    };
    push_changes(&mut out, before, after, Some(message.kind.as_str()));
    out
}

/// One short sentence on what an event step did: its label, then what
/// changed.
#[must_use]
pub fn says_event(before: &WorldView, after: &WorldView, label: &str) -> String {
    let mut chars = label.chars();
    let mut out: String = match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => String::new(),
    };
    if !out.is_empty() {
        out.push('.');
    }
    push_changes(&mut out, before, after, None);
    out
}

fn prepare(to: &str, ballot: &str, before: Option<&NodeView>, after: Option<&NodeView>) -> String {
    let (Some(before), Some(after)) = (before, after) else {
        return format!("{to} gets Prepare {ballot}.");
    };
    if before.promised != after.promised {
        let mut out =
            format!("{to} promises ballot {ballot}. From now on, it refuses every lower ballot.");
        if after.flavour == NodeFlavour::Acceptor {
            match after.accepted.first() {
                Some(record) => {
                    let _ = write!(
                        out,
                        " It reports its vote: {} at ballot {}.",
                        record.value,
                        record.ballot.clone().unwrap_or_default()
                    );
                }
                None => out.push_str(" It has no vote to report."),
            }
        }
        out
    } else if after.promised.as_deref() == Some(ballot) {
        format!("{to} already promised ballot {ballot}. It answers again.")
    } else {
        format!(
            "{to} refuses ballot {ballot}: it promised ballot {}, which is higher.",
            after.promised.clone().unwrap_or_default()
        )
    }
}

fn accept(
    to: &str,
    ballot: &str,
    slot: Option<u64>,
    before: Option<&NodeView>,
    after: Option<&NodeView>,
) -> String {
    let (Some(before), Some(after)) = (before, after) else {
        return format!("{to} gets Accept {ballot}.");
    };
    let new_vote = after.accepted.iter().find(|record| {
        record.ballot.as_deref() == Some(ballot)
            && slot.is_none_or(|slot| record.slot == slot)
            && !before
                .accepted
                .iter()
                .any(|old| old.slot == record.slot && old.ballot == record.ballot)
    });
    match new_vote {
        Some(record) if after.flavour == NodeFlavour::Acceptor => {
            format!("{to} votes for {} at ballot {ballot}.", record.value)
        }
        Some(record) if record.control.as_deref() == Some("noop") => format!(
            "{to} votes for a no-op in slot {} at ballot {ballot}. No promise reported a vote \
             for this slot, so the new leader fills it with a no-op. Then the slots after it \
             can apply.",
            record.slot
        ),
        Some(record) => format!(
            "{to} votes for {} in slot {} at ballot {ballot}.",
            record.value, record.slot
        ),
        None => format!(
            "{to} does not vote: it promised ballot {}, which is higher than {ballot}.",
            after.promised.clone().unwrap_or_default()
        ),
    }
}

fn commit(world: &WorldView, to: &str, slot: Option<u64>, node: Option<&NodeView>) -> String {
    if let Some(chosen) = &world.chosen {
        return format!("{to} learns that {} is chosen.", chosen.value);
    }
    let value = node.and_then(|node| {
        node.accepted
            .iter()
            .find(|record| Some(record.slot) == slot && record.chosen)
    });
    match (slot, value) {
        (Some(slot), Some(record)) => {
            format!("{to} learns that slot {slot} = {} is chosen.", record.value)
        }
        (Some(slot), None) => format!("{to} learns the value of slot {slot}."),
        (None, _) => format!("{to} learns the chosen value."),
    }
}

/// "a majority" or "a quorum": how many acceptors a phase needs here.
fn quorum_words(world: &WorldView) -> &'static str {
    match quorum_kind(world) {
        Some(QuorumKindView::Majority) | None => "a majority",
        Some(_) => "a quorum",
    }
}

/// Add what changed in the world: a role, an attempt, a chosen value.
/// `kind` is the message the step delivered, if any.
fn push_changes(out: &mut String, before: &WorldView, after: &WorldView, kind: Option<&str>) {
    let quorum = quorum_words(after);
    for node in &after.nodes {
        let Some(old) = before.nodes.iter().find(|old| old.id == node.id) else {
            continue;
        };
        let name = party_name(after, PartyView::Node, node.id);
        let ballot = node.ballot.clone().unwrap_or_default();
        if old.attempt != node.attempt && node.alive {
            let said = match node.attempt {
                Some(AttemptView::Phase1) => format!(
                    "{name} starts Phase 1 at ballot {ballot}: it sends Prepare to every acceptor."
                ),
                Some(AttemptView::Phase2) => format!(
                    "{name} has promises from {quorum} of the acceptors, so Phase 1 is done. It \
                     starts Phase 2: it asks every acceptor to vote."
                ),
                Some(AttemptView::Preempted) => {
                    format!("{name} stops: an acceptor promised a higher ballot.")
                }
                // The chosen value is said below, once for the world.
                Some(AttemptView::Won | AttemptView::Idle) | None => String::new(),
            };
            push(out, &said);
        }
        if old.role != node.role && node.alive {
            let said = match node.role {
                Some(RoleView::Candidate) => {
                    format!("{name} wants to lead at ballot {ballot}. It starts Phase 1.")
                }
                Some(RoleView::Leader) if kind == Some("Promise") => format!(
                    "{name} has promises from {quorum} of the nodes, so Phase 1 is done. It is \
                     now the leader at ballot {ballot}."
                ),
                Some(RoleView::Leader) => format!("{name} is now the leader at ballot {ballot}."),
                Some(RoleView::Follower) | None if old.role == Some(RoleView::Leader) => {
                    format!("{name} is not the leader now.")
                }
                Some(RoleView::Follower) | None => String::new(),
            };
            push(out, &said);
        }
        push_lifecycle(out, &name, old, node);
        if old.alive && !node.alive && node.retired {
            push(
                out,
                &format!(
                    "{name} shuts down for good. No future leader can need its Phase-1 answer."
                ),
            );
        } else if old.alive && !node.alive {
            push(
                out,
                &format!(
                    "The disk of {name} keeps its promise and its votes. It forgets the rest."
                ),
            );
        }
    }
    push_reads(out, before, after);
    let was = chosen(before);
    let new: Vec<String> = chosen(after)
        .into_iter()
        .filter(|value| !was.contains(value))
        .collect();
    if !new.is_empty() {
        let mut chars = quorum.chars();
        let who: String = chars
            .next()
            .map(|first| first.to_uppercase().chain(chars).collect())
            .unwrap_or_default();
        push(
            out,
            &format!(
                "{who} of the acceptors voted for it, so {} is chosen. No later ballot can \
                 change it.",
                new.join(" and ")
            ),
        );
    }
}

/// What a restart, a wipe or a truncation changed on `node`.
fn push_lifecycle(out: &mut String, name: &str, old: &NodeView, node: &NodeView) {
    if !old.alive && node.alive {
        push(
            out,
            &format!("{name} reads its promise and its votes back from its disk."),
        );
    }
    if old.alive
        && node.alive
        && promise_line(old) != "no promise"
        && promise_line(node) == "no promise"
    {
        push(
            out,
            &format!("{name} forgot its promise and its votes: its disk is empty."),
        );
    }
    if node.alive && node.floor.unwrap_or(0) > old.floor.unwrap_or(0) {
        push(
            out,
            &format!(
                "{name} drops every slot below slot {}.",
                node.floor.unwrap_or(0)
            ),
        );
    }
}

/// The reads that a node answered in this step.
fn push_reads(out: &mut String, before: &WorldView, after: &WorldView) {
    for client in &after.clients {
        for read in client.reads.iter().filter(|read| read.served) {
            let new = before
                .clients
                .iter()
                .find(|old| old.id == client.id)
                .is_none_or(|old| !old.reads.iter().any(|r| r.ctx == read.ctx && r.served));
            if new {
                let name = party_name(after, PartyView::Node, read.node);
                push(
                    out,
                    &match read.index {
                        Some(index) => format!(
                            "{name} answers the read: it applied every slot up to slot {index}."
                        ),
                        None => format!("{name} answers the read: no slot is chosen yet."),
                    },
                );
            }
        }
    }
}

fn push(out: &mut String, sentence: &str) {
    if sentence.is_empty() {
        return;
    }
    if !out.is_empty() {
        out.push(' ');
    }
    out.push_str(sentence);
}

fn quorum_kind(world: &WorldView) -> Option<QuorumKindView> {
    world
        .nodes
        .iter()
        .find(|node| node.flavour != NodeFlavour::Proposer)
        .map(|node| node.quorum.kind)
}

fn node(world: &WorldView, party: PartyView, id: u64) -> Option<&NodeView> {
    match party {
        PartyView::Node => world.nodes.iter().find(|node| node.id == id),
        PartyView::Matchmaker => None,
    }
}

fn matchmaker_changed(before: &WorldView, after: &WorldView, party: PartyView, id: u64) -> bool {
    if party != PartyView::Matchmaker {
        return false;
    }
    let count = |world: &WorldView| {
        world
            .matchmakers
            .iter()
            .find(|matchmaker| matchmaker.id == id)
            .map_or(0, |matchmaker| matchmaker.registrations.len())
    };
    count(before) != count(after)
}
