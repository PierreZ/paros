//! The game explains what just happened, in Paxos.
//!
//! Every player action produces a list of [`NarrationEvent`]s, and every one of
//! them is **derived from the transition the engine just made** — never from a
//! script attached to a level, and never from a trace read back. The two
//! sources are the message that arrived and the *diff* of the node's own role
//! accessors across the call into it: a promise that rose, a record that
//! appeared, a chosen prefix that grew, a role that changed, and the messages
//! the batch put on the wire. If the core did not do it, nothing here says it
//! did.
//!
//! The vocabulary is the protocol's, with the concrete numbers in it — ballots
//! as `round.node`, slots by number, values by their text — because the point
//! of the game is to leave the player able to say *why* a node did what it did,
//! not which method was called.
//!
//! Narration never changes the world: it reads the same accessors the view
//! does, allocates strings, and pushes them on a buffer the [`crate::Game`]
//! drains once per action.

use std::collections::{BTreeMap, BTreeSet};

use paros_core::{
    Ballot, ColocatedNode, Command, LeadershipOrigin, Message, NodeId, NodeRole, QuorumSystem,
    Slot, proposer::Round,
};
use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::view::{NarrationView, show_ballot, show_command};

/// What a narration line is about. The frontend colours the caption by this,
/// and the log panel groups by it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, TS)]
#[serde(rename_all = "snake_case")]
pub enum NarrationKind {
    /// Something happened that is not one of the families below: a message
    /// arrived, a client asked, the world was set up.
    Info,
    /// An acceptor raised its durable promise, and what it reported with it.
    Promise,
    /// A message was refused: a ballot was fenced out.
    Nack,
    /// A value was proposed, or a vote was cast and recorded.
    Accept,
    /// A slot became chosen — the decision itself.
    Chosen,
    /// The contiguous prefix grew: the application executed a command.
    Applied,
    /// A campaign opened, or a role changed.
    Election,
    /// A candidate won: there is a leader.
    Leader,
    /// A beat, or the ack that answers one.
    Heartbeat,
    /// A node lost its volatile state.
    Crash,
    /// A node came back from its disk.
    Restart,
    /// A linearizable read opened, or was served.
    Read,
    /// A log prefix was dropped: a `Truncate` decided, or a floor that rose.
    Truncate,
    /// A snapshot point was recorded, offered, or installed.
    Snapshot,
    /// A client asked for something.
    Client,
    /// A candidate registered a configuration with the matchmakers, or learned
    /// what came before it.
    Matchmaking,
    /// The acceptor set changed: a reconfiguration opened, completed, or was
    /// refused.
    Reconfigure,
    /// A garbage-collection floor moved, or an acceptor was retired.
    Gc,
    /// The matchmaker set itself changed: a generation was frozen, decided, or
    /// activated.
    Generation,
    /// The rule the player's answer would have broken.
    Violation,
    /// The level's goal is reached.
    Goal,
}

/// One narration line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NarrationEvent {
    /// What it is about.
    pub kind: NarrationKind,
    /// The sentence, with this transition's own numbers in it.
    pub text: String,
}

impl NarrationEvent {
    /// A line of `kind`.
    pub fn new(kind: NarrationKind, text: impl Into<String>) -> Self {
        Self {
            kind,
            text: text.into(),
        }
    }

    /// Render it for the browser.
    #[must_use]
    pub fn view(&self) -> NarrationView {
        NarrationView {
            kind: self.kind,
            text: self.text.clone(),
        }
    }
}

/// Shorthand for a line of `kind`.
pub(crate) fn say(kind: NarrationKind, text: impl Into<String>) -> NarrationEvent {
    NarrationEvent::new(kind, text)
}

/// How the narration names a node. The game's nodes are numbered, and every
/// prompt and error says "node 1", so the narration does too.
pub(crate) fn who(id: NodeId) -> String {
    format!("node {}", id.0)
}

/// "nothing", or "slot 3" — the two ways a watermark reads.
fn at(slot: Option<Slot>) -> String {
    slot.map_or_else(|| "nothing".to_string(), |s| format!("slot {}", s.0))
}

/// "1 record", "2 records" — the game says things out loud, so it counts out
/// loud too.
pub(crate) fn many(count: usize, noun: &str) -> String {
    if count == 1 {
        format!("{count} {noun}")
    } else {
        format!("{count} {noun}s")
    }
}

/// "acceptor 1", "acceptors 1 and 2", "acceptors 1, 2 and 3".
pub(crate) fn list_nodes(ids: impl IntoIterator<Item = NodeId>) -> String {
    let ids: Vec<String> = ids.into_iter().map(|id| id.0.to_string()).collect();
    match ids.len() {
        0 => "nobody".to_string(),
        1 => format!("acceptor {}", ids[0]),
        _ => {
            let (last, rest) = ids.split_last().expect("a non-empty list");
            format!("acceptors {} and {last}", rest.join(", "))
        }
    }
}

/// What a **Phase-1** quorum is here, in one clause — the configuration's own
/// answer, never a threshold this crate computed. Under a majority it is a
/// count; under a flexible split it is the split's own `q1`; under a grid it
/// is a shape, and no count describes it.
pub(crate) fn phase1_note(system: QuorumSystem, members: usize) -> String {
    match system {
        QuorumSystem::Majority => format!(
            "A Phase-1 quorum here is {} of {members}.",
            system.phase1_quorum_size(members)
        ),
        QuorumSystem::Flexible { q1, q2 } => format!(
            "A Phase-1 quorum here is {q1} of {members}. Phase 2 takes only {q2}. The two \
             numbers must add up to more than {members}. Nothing else is required of either."
        ),
        QuorumSystem::Grid { rows, cols } => format!(
            "A Phase-1 quorum here is one whole row of the {rows} by {cols} grid. It must be \
             those named acceptors, and not any {cols} of them."
        ),
    }
}

/// What a **Phase-2** quorum is here, in one clause. See [`phase1_note`].
pub(crate) fn phase2_note(system: QuorumSystem, members: usize) -> String {
    match system {
        QuorumSystem::Majority => format!(
            "A Phase-2 quorum here is {} of {members}.",
            system.phase2_quorum_size(members)
        ),
        QuorumSystem::Flexible { q1, q2 } => format!(
            "A Phase-2 quorum here is {q2} of {members}. Two Phase-2 quorums do not have to \
             share an acceptor. Safety needs only one thing: every Phase-1 quorum of {q1} meets \
             every Phase-2 quorum."
        ),
        QuorumSystem::Grid { rows, cols } => format!(
            "A Phase-2 quorum here is one whole column of the {rows} by {cols} grid. A column \
             holds {rows} named acceptors. Every row meets every column in exactly one acceptor."
        ),
    }
}

// ---- the log world's snapshot ----------------------------------------------

/// Everything the narration diffs across a call into a node: the role
/// accessors, read once before and once after.
///
/// A crashed node's snapshot is [`NodeSnapshot::gone`], so "the node is not
/// there any more" is itself a diff the narration can describe.
#[derive(Clone, Debug, Default)]
pub(crate) struct NodeSnapshot {
    alive: bool,
    promised: Ballot,
    records: BTreeMap<Slot, (Ballot, Command)>,
    chosen: BTreeMap<Slot, Command>,
    chosen_index: Option<Slot>,
    first_unchosen: Slot,
    role: Option<NodeRole>,
    ballot: Ballot,
    /// Per open Phase-2 round: how many acceptors have voted so far. The
    /// decision's own tally, read one step before it completes.
    votes: BTreeMap<Slot, usize>,
    gap: Option<(Slot, Slot)>,
    recovery_remaining: usize,
    /// Where this node's leadership came from: won at its own ballot, or
    /// handed over by the node that minted one. The two produce very
    /// different sentences, and only the node knows which it is.
    origin: LeadershipOrigin,
}

impl NodeSnapshot {
    /// The snapshot of a node that is not running.
    pub(crate) fn gone() -> Self {
        Self::default()
    }

    /// Read every accessor the narration diffs.
    pub(crate) fn capture(node: Option<&ColocatedNode>) -> Self {
        let Some(node) = node else {
            return Self::gone();
        };
        Self {
            alive: true,
            promised: node.acceptor().promised(),
            records: node.acceptor().records().clone(),
            chosen: node.replica().chosen().clone(),
            chosen_index: node.replica().chosen_index(),
            first_unchosen: node.replica().first_unchosen(),
            role: Some(node.role()),
            ballot: node.ballot(),
            votes: node
                .proposer()
                .rounds()
                .iter()
                .map(|(slot, round)| (*slot, round.accepted_by().len()))
                .collect(),
            gap: node.replica().chosen_gap(),
            recovery_remaining: node.proposer().recovery_remaining(),
            origin: node.leadership_origin(),
        }
    }

    /// How many accepted records this node holds — what a `Promise` reports.
    fn record_count(&self) -> usize {
        self.records.len()
    }

    /// What it holds for `slot`, if anything.
    fn record(&self, slot: Slot) -> Option<&(Ballot, Command)> {
        self.records.get(&slot)
    }
}

// ---- what the node knew when the message landed -----------------------------

/// The first line of a delivery: what arrived, and what the node knew when it
/// did. The rest of the story is the diff.
#[must_use]
#[allow(clippy::too_many_lines)]
pub(crate) fn receipt(to: NodeId, message: &Message, before: &NodeSnapshot) -> NarrationEvent {
    let node = who(to);
    let p = show_ballot(before.promised);
    let text = match message {
        Message::Prepare {
            ballot, from_slot, ..
        } => format!(
            "{node} receives Prepare {} for every slot from {}. Its promise was {p}. It holds \
             {} to report.",
            show_ballot(*ballot),
            from_slot.0,
            many(before.record_count(), "accepted record")
        ),
        Message::Promise {
            from,
            ballot,
            accepted,
            ..
        } => format!(
            "{node} receives a Promise for ballot {} from node {}. The Promise reports the {} \
             it holds at or above the slot the Prepare asked about.",
            show_ballot(*ballot),
            from.0,
            many(accepted.len(), "accepted value")
        ),
        Message::Accept {
            ballot,
            slot,
            command,
            ..
        } => {
            let held = before.record(*slot).map_or_else(
                || format!("nothing at slot {}", slot.0),
                |(at, value)| {
                    format!(
                        "{} at ballot {} for slot {}",
                        show_command(value),
                        show_ballot(*at),
                        slot.0
                    )
                },
            );
            format!(
                "{node} receives Accept {} for slot {}, carrying {}. Its promise was {p}. It \
                 holds {held}.",
                show_ballot(*ballot),
                slot.0,
                show_command(command)
            )
        }
        Message::Accepted {
            from, ballot, slot, ..
        } => {
            let standing = if before.chosen.contains_key(slot) {
                format!(
                    "Slot {} is already chosen here: one more vote changes nothing.",
                    slot.0
                )
            } else {
                match before.votes.get(slot) {
                    Some(votes) => format!(
                        "Its round for that slot held {} before this one.",
                        many(*votes, "vote")
                    ),
                    None => "It has no open round for that slot, so no tally counts the vote."
                        .to_string(),
                }
            };
            format!(
                "{node} receives an Accepted for slot {} at {} from node {}. {standing}",
                slot.0,
                show_ballot(*ballot),
                from.0
            )
        }
        Message::Nack { from, ballot, slot } => format!(
            "{node} receives a Nack from node {}: its ballot {} was refused at slot {}.",
            from.0,
            show_ballot(*ballot),
            slot.0
        ),
        Message::Commit {
            ballot,
            slot,
            command,
            ..
        } => {
            let held = before.record(*slot).map_or_else(
                || "nothing".to_string(),
                |(at, value)| format!("{} at {}", show_command(value), show_ballot(*at)),
            );
            format!(
                "{node} receives a Commit: slot {} is chosen as {} at ballot {}. It held {held} \
                 there.",
                slot.0,
                show_command(command),
                show_ballot(*ballot)
            )
        }
        Message::Heartbeat {
            from,
            ballot,
            commit,
            seq,
            ..
        } => format!(
            "{node} receives beat #{seq} from node {} at ballot {}, carrying commit {}. Its own \
             applied prefix ends at {}.",
            from.0,
            show_ballot(*ballot),
            at(*commit),
            at(before.chosen_index)
        ),
        Message::HeartbeatAck {
            from, ballot, seq, ..
        } => format!(
            "{node} receives an ack for beat #{seq} at {} from node {}.",
            show_ballot(*ballot),
            from.0
        ),
        Message::CatchUpRequest { from, from_slot } => format!(
            "{node} receives a catch-up request from node {}: it is missing everything from slot \
             {}.",
            from.0, from_slot.0
        ),
        Message::CatchUpResponse { from, entries } => format!(
            "{node} receives a catch-up from node {} carrying {} decided slot(s). Its applied \
             prefix ends at {}.",
            from.0,
            entries.len(),
            at(before.chosen_index)
        ),
        Message::InstallSnapshot {
            from,
            chosen_index,
            ballot,
            ..
        } => format!(
            "{node} receives a snapshot from node {} covering everything up to slot {}, taken \
             under ballot {}.",
            from.0,
            chosen_index.0,
            show_ballot(*ballot)
        ),
        Message::Relinquish {
            from,
            ballot,
            next_slot,
            decided,
            pending,
            ..
        } => format!(
            "{node} receives a hand-off from node {}. It carries ballot {}, the frontier at slot \
             {}, and the tail below the frontier: {} already chosen and {} still in flight. Its \
             own promise was {p}.",
            from.0,
            show_ballot(*ballot),
            next_slot.0,
            many(decided.len(), "slot"),
            many(pending.len(), "slot")
        ),
        Message::PreRead { reply_to, ctx } => format!(
            "{node} receives one question from node {} for read #{ctx}: what is the highest slot \
             you have voted in? An answer writes nothing down and moves no promise.",
            reply_to.0
        ),
        Message::PreReadAck {
            from,
            ctx,
            watermark,
            ..
        } => format!(
            "{node} hears node {}'s answer to read #{ctx}: it has voted up to {}.",
            from.0,
            at(*watermark)
        ),
        _ => format!("{node} receives a message."),
    };
    say(NarrationKind::Info, text)
}

// ---- what the node did about it ---------------------------------------------

/// Every message a batch put on the wire, as the narration reads them.
pub(crate) type Sent = [(NodeId, Message)];

/// The story of one call into a node: the diff of its role accessors, plus the
/// messages its batches sent.
///
/// Nothing here is authored per level and nothing is a guess: a sentence is
/// emitted only when the corresponding accessor actually moved, or the
/// corresponding message actually left.
#[must_use]
#[allow(clippy::too_many_lines)]
pub(crate) fn describe(
    id: NodeId,
    before: &NodeSnapshot,
    after: &NodeSnapshot,
    sent: &Sent,
    system: QuorumSystem,
    members: usize,
) -> Vec<NarrationEvent> {
    let mut out = Vec::new();
    let node = who(id);

    if before.alive && !after.alive {
        out.push(say(
            NarrationKind::Crash,
            format!(
                "{node} stops here, inside the batch. Only the part of the batch that the \
                 durability seam let through survives."
            ),
        ));
    }

    // ---- role -------------------------------------------------------------
    if before.role != after.role
        && let Some(role) = after.role
    {
        let b = show_ballot(after.ballot);
        out.push(match role {
            NodeRole::Candidate => say(
                NarrationKind::Election,
                format!("{node} becomes a candidate at ballot {b}: it wants the whole log suffix."),
            ),
            NodeRole::Leader => match after.origin {
                LeadershipOrigin::Handoff { from } => say(
                    NarrationKind::Leader,
                    format!(
                        "{node} takes ballot {b} over from node {}. It runs no Phase 1 of its \
                         own. Node {} already holds the promises for {b}, and it told {node} \
                         where the frontier is and what sits below it. That is the gain: a \
                         leader change without a round trip.",
                        from.0, from.0
                    ),
                ),
                LeadershipOrigin::Elected => say(
                    NarrationKind::Leader,
                    format!(
                        "{node} is the leader at ballot {b}. A promise quorum answered its \
                         Prepare. No ballot below {b} can decide anything now, and {node} may \
                         run Phase 2 alone."
                    ),
                ),
            },
            NodeRole::Follower => say(
                NarrationKind::Election,
                match before.origin {
                    LeadershipOrigin::Handoff { .. } if before.role == Some(NodeRole::Leader) => {
                        format!(
                            "{node} gives up the leadership it was handed. It skipped Phase 1, \
                             so it never recovered the slots below the frontier it inherited. It \
                             waited long enough for those slots to arrive by ordinary \
                             replication. An election runs Phase 1, and that is the fallback."
                        )
                    }
                    _ => format!(
                        "{node} becomes a follower again. Its rounds are volatile, so they go \
                         with the leadership. Nobody re-sends what it had in flight."
                    ),
                },
            ),
        });
    }

    // ---- the acceptor's two writes ----------------------------------------
    if after.promised > before.promised {
        let reported = sent.iter().find_map(|(_, m)| match m {
            Message::Promise { accepted, .. } => Some(accepted.len()),
            _ => None,
        });
        let raised = format!(
            "{node} raises its durable promise from {} to {}",
            show_ballot(before.promised),
            show_ballot(after.promised)
        );
        out.push(say(
            NarrationKind::Promise,
            match reported {
                Some(0) => format!(
                    "{raised}, and reports what it accepted: nothing. Every ballot below {} is \
                     fenced out here from now on.",
                    show_ballot(after.promised)
                ),
                Some(n) => format!(
                    "{raised}, and reports the {} it accepted. A new leader must re-propose that \
                     report. It must not overwrite it.",
                    many(n, "value")
                ),
                None => format!("{raised}. The promise is durable before anything leaves."),
            },
        ));
    }

    for (slot, (ballot, command)) in &after.records {
        if before.record(*slot) == Some(&(*ballot, command.clone())) {
            continue;
        }
        let text = match before.record(*slot) {
            Some((held_at, held)) => format!(
                "{node} replaces its record for slot {}. It held {} at ballot {}, and it now \
                 holds {} at ballot {}. The record at the higher ballot replaces the record at \
                 the lower one, and that rule makes a restart safe.",
                slot.0,
                show_command(held),
                show_ballot(*held_at),
                show_command(command),
                show_ballot(*ballot)
            ),
            None => format!(
                "{node} records slot {} = {} at ballot {}. The vote is durable before the \
                 Accepted that reports it leaves.",
                slot.0,
                show_command(command),
                show_ballot(*ballot)
            ),
        };
        out.push(say(NarrationKind::Accept, text));
    }

    // ---- what it sent ------------------------------------------------------
    out.extend(describe_sent(id, after, sent, system));

    // ---- what it decided ---------------------------------------------------
    for (slot, command) in &after.chosen {
        if before.chosen.contains_key(slot) {
            continue;
        }
        let ballot = after.records.get(slot).map_or(after.ballot, |(b, _)| *b);
        let text = if let Some(votes) = before.votes.get(slot) {
            format!(
                "Slot {} is chosen. {} of the {members} acceptors voted for {} at ballot {}. {} \
                 This decision is final. No later ballot can decide this slot differently.",
                slot.0,
                votes + 1,
                show_command(command),
                show_ballot(ballot),
                phase2_note(system, members)
            )
        } else {
            format!(
                "{node} learns that slot {} is chosen as {} at ballot {}.",
                slot.0,
                show_command(command),
                show_ballot(ballot)
            )
        };
        out.push(say(NarrationKind::Chosen, text));
    }

    // ---- what it applied ---------------------------------------------------
    if after.chosen_index != before.chosen_index {
        let from = before.first_unchosen;
        let applied: Vec<String> = after
            .chosen
            .range(from..=after.chosen_index.unwrap_or(from))
            .map(|(slot, command)| format!("slot {} = {}", slot.0, show_command(command)))
            .collect();
        let waited = after
            .chosen
            .range(from..=after.chosen_index.unwrap_or(from))
            .any(|(slot, _)| before.chosen.contains_key(slot));
        let mut text = format!(
            "{node} applies {}. Its contiguous prefix now ends at {}.",
            applied.join(", "),
            at(after.chosen_index)
        );
        if waited {
            text.push_str(
                " Some of those slots were chosen earlier and could not be applied. A state \
                 machine executes in log order, so a hole in front of them held them back.",
            );
        }
        out.push(say(NarrationKind::Applied, text));
    }

    // ---- the hole ----------------------------------------------------------
    if before.gap.is_none()
        && let Some((hole, highest)) = after.gap
    {
        out.push(say(
            NarrationKind::Info,
            format!(
                "{node} now knows slot {} is chosen while slot {} is not. Its applied prefix \
                 stops below the hole. No catch-up can help, because nobody holds slot {} to \
                 replay.",
                highest.0, hole.0, hole.0
            ),
        ));
    }
    if before.gap.is_some() && after.gap.is_none() {
        out.push(say(
            NarrationKind::Info,
            format!("{node} has no hole left: its chosen prefix is contiguous again."),
        ));
    }
    if after.recovery_remaining > 0 && after.recovery_remaining != before.recovery_remaining {
        out.push(say(
            NarrationKind::Election,
            format!(
                "{node} still has {} of its inherited suffix to settle before it may stream \
                 anything new.",
                many(after.recovery_remaining, "slot")
            ),
        ));
    }
    out
}

/// The messages the batches put on the wire, grouped by what they are.
#[allow(clippy::too_many_lines)]
fn describe_sent(
    id: NodeId,
    after: &NodeSnapshot,
    sent: &Sent,
    system: QuorumSystem,
) -> Vec<NarrationEvent> {
    let mut out = Vec::new();
    let node = who(id);

    if let Some((ballot, from_slot)) = sent.iter().find_map(|(_, m)| match m {
        Message::Prepare {
            ballot, from_slot, ..
        } => Some((*ballot, *from_slot)),
        _ => None,
    }) {
        let peers = sent
            .iter()
            .filter(|(_, m)| matches!(m, Message::Prepare { .. }))
            .count();
        out.push(say(
            NarrationKind::Election,
            format!(
                "{node} campaigns at ballot {}. It sends one Prepare to {}, and it claims every \
                 slot from {} at once. Phase 1 asks about no value. It asks what the acceptors \
                 have already accepted.",
                show_ballot(ballot),
                many(peers, "peer"),
                from_slot.0
            ),
        ));
    }

    if let Some(ballot) = sent.iter().find_map(|(_, m)| match m {
        Message::Nack { ballot, .. } => Some(*ballot),
        _ => None,
    }) {
        out.push(say(
            NarrationKind::Nack,
            format!(
                "{node} refuses ballot {}. It has promised {}, and a promise is the only fence \
                 Paxos has.",
                show_ballot(ballot),
                show_ballot(after.promised)
            ),
        ));
    }

    let accepts: BTreeMap<Slot, (Ballot, Command, usize)> =
        sent.iter().fold(BTreeMap::new(), |mut acc, (_, m)| {
            if let Message::Accept {
                ballot,
                slot,
                command,
                ..
            } = m
            {
                let entry = acc
                    .entry(*slot)
                    .or_insert_with(|| (*ballot, command.clone(), 0));
                entry.2 += 1;
            }
            acc
        });
    for (slot, (ballot, command, fan_out)) in accepts {
        let noop = matches!(command, Command::Control(paros_core::Control::Noop));
        if let Some(column) = system.column_of(slot) {
            let voters: Vec<NodeId> = sent
                .iter()
                .filter(|(_, m)| matches!(m, Message::Accept { slot: at, .. } if *at == slot))
                .map(|(to, _)| *to)
                .collect();
            out.push(say(
                NarrationKind::Accept,
                format!(
                    "{node} proposes {} for slot {} at ballot {}, to column {column}: {}. The \
                     column is not on the wire. The column is slot {} modulo the number of \
                     columns, so every node that re-proposes this slot derives the same column.",
                    show_command(&command),
                    slot.0,
                    show_ballot(ballot),
                    list_nodes(voters),
                    slot.0
                ),
            ));
            continue;
        }
        out.push(say(
            NarrationKind::Accept,
            if noop {
                format!(
                    "{node} proposes a Noop for slot {} at ballot {ballot_text}, to {}. Its \
                     promise quorum reported nothing for that slot. Quorum intersection makes \
                     that silence safe to act on: an acceptor that promised would have reported \
                     any value already chosen there.",
                    slot.0,
                    many(fan_out, "acceptor"),
                    ballot_text = show_ballot(ballot)
                )
            } else {
                format!(
                    "{node} proposes {} for slot {} at ballot {}, to {}. No second Phase 1 is \
                     needed. The ballot it holds already covers the whole log suffix.",
                    show_command(&command),
                    slot.0,
                    show_ballot(ballot),
                    many(fan_out, "acceptor")
                )
            },
        ));
    }

    if let Some(seq) = sent.iter().find_map(|(_, m)| match m {
        Message::Heartbeat { seq, .. } => Some(*seq),
        _ => None,
    }) {
        let peers = sent
            .iter()
            .filter(|(_, m)| matches!(m, Message::Heartbeat { .. }))
            .count();
        out.push(say(
            NarrationKind::Heartbeat,
            format!(
                "{node} sends beat #{seq} to {}, and the beat carries its commit index. The beat \
                 costs nothing extra. The commit watermark travels on a message the leader sends \
                 anyway.",
                many(peers, "peer")
            ),
        ));
    }
    if let Some(seq) = sent.iter().find_map(|(_, m)| match m {
        Message::HeartbeatAck { seq, .. } => Some(*seq),
        _ => None,
    }) {
        out.push(say(
            NarrationKind::Heartbeat,
            format!(
                "{node} acks beat #{seq}. That ack is the only proof a leader has that it still \
                 leads."
            ),
        ));
    }
    if let Some(from_slot) = sent.iter().find_map(|(_, m)| match m {
        Message::CatchUpRequest { from_slot, .. } => Some(*from_slot),
        _ => None,
    }) {
        let peers = sent
            .iter()
            .filter(|(_, m)| matches!(m, Message::CatchUpRequest { .. }))
            .count();
        out.push(say(
            NarrationKind::Info,
            format!(
                "{node} asks {} for any slot from {} it may have missed. It does not claim to be \
                 behind. A decision reaches a follower only as a commit watermark. A \
                 watermark that it did not receive looks the same as a watermark that was never \
                 set.",
                many(peers, "peer"),
                from_slot.0
            ),
        ));
    }
    if let Some(count) = sent.iter().find_map(|(_, m)| match m {
        Message::CatchUpResponse { entries, .. } => Some(entries.len()),
        _ => None,
    }) {
        out.push(say(
            NarrationKind::Info,
            format!(
                "{node} replays {} to the peer that asked.",
                many(count, "decided slot")
            ),
        ));
    }
    out
}

/// The per-round vote tally a decision's narration reads.
///
/// Kept beside the snapshot because the Act I world holds its roles directly
/// rather than through a `ColocatedNode`.
pub(crate) fn votes_of<Id: Copy + Ord, V>(
    rounds: &BTreeMap<Slot, Round<Id, V>>,
    slot: Slot,
) -> BTreeSet<Id> {
    rounds
        .get(&slot)
        .map(|round| round.accepted_by().clone())
        .unwrap_or_default()
}
