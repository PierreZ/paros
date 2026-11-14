//! The `Ready` inspector of the site's sans-IO page (#951 (live Engineering
//! pages)): three [`ColocatedNode`]s of `paros-core`, a wire the reader
//! drives by hand, and, after each input, the [`paros_core::Ready`] batches
//! that the input produced, bucket by bucket, in the order a driver must do
//! them.
//!
//! The lab is a driver, the smallest one: it persists each batch's writes to
//! the game's [`Disk`], puts each message on the wire, and calls
//! [`paros_core::Ready::advance`] and [`ColocatedNode::advance_recovery`]
//! until the node is quiet. It holds no protocol logic: every word it shows
//! is read off a real `WriteOp`, `Message`, committed slot or `ReadState`.

use paros_core::contract::AcceptorWrite;
use paros_core::journal::{Entry, Seq};
use paros_core::membership::QuorumSystem;
use paros_core::node::ProposeResult;
use paros_core::quorum_read::ReadState;
use paros_core::{ColocatedNode, Config, Limits, Message, NodeId, Value, WriteOp};
use serde::{Deserialize, Serialize};

use crate::paxos::view::{show_ballot, show_command, show_role};
use crate::paxos::world::disk::Disk;
use crate::paxos::world::{ClientId, Envelope, InFlight, Party};

/// How many nodes the lab runs.
pub const NODES: u64 = 3;

/// How many batches one input may drain before the lab stops it: a bound no
/// input reaches.
const DRAIN_BUDGET: usize = 64;

/// The one client of the lab: the journal's writer from the start.
const CLIENT: ClientId = ClientId(0);

/// One input the reader gives a node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Input {
    /// The election clock of `node` runs out: one `tick()` with a timeout
    /// of one tick.
    Timeout {
        /// The node.
        node: u64,
    },
    /// One `tick()` of `node`'s clock.
    Tick {
        /// The node.
        node: u64,
    },
    /// Deliver the wire message `id` to its addressee: one `step()`.
    Deliver {
        /// The message's id on the wire.
        id: u64,
    },
    /// Lose the wire message `id`: no node sees it.
    Drop {
        /// The message's id on the wire.
        id: u64,
    },
    /// The client writes `text` at `node`: one `propose()`.
    Write {
        /// The node the client asks.
        node: u64,
        /// The record.
        text: String,
    },
}

/// One message on the wire, as the page shows it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireView {
    /// The id an input names.
    pub id: u64,
    /// The sender.
    pub from: u64,
    /// The addressee.
    pub to: u64,
    /// The `Message` variant, e.g. `Promise`.
    pub kind: String,
    /// The render family, e.g. `promise`.
    pub phase: String,
    /// One line with its fields.
    pub summary: String,
}

/// One durable write of a batch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteView {
    /// The `WriteOp` variant, e.g. `SetPromise`.
    pub kind: String,
    /// What it writes.
    pub detail: String,
    /// Whether the driver must sync before it sends (`WriteOp::needs_sync`).
    pub needs_sync: bool,
}

/// One `Ready` batch, copied out bucket by bucket.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadyView {
    /// Bucket 1: `writes()`, persisted first.
    pub writes: Vec<WriteView>,
    /// Whether the driver syncs this batch: one write needs it.
    pub sync: bool,
    /// Bucket 2: `messages()`, sent after the writes are durable, one entry
    /// per addressee.
    pub messages: Vec<WireView>,
    /// Bucket 3: `committed()`, the slots now chosen and durable.
    pub committed: Vec<String>,
    /// Bucket 4: `read_states()`, the reads that may now be served.
    pub read_states: Vec<String>,
}

impl ReadyView {
    fn is_empty(&self) -> bool {
        self.writes.is_empty()
            && self.messages.is_empty()
            && self.committed.is_empty()
            && self.read_states.is_empty()
    }
}

/// What one input did.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepView {
    /// The input, in words.
    pub input: String,
    /// The node that took it, if a node did.
    pub node: Option<u64>,
    /// What the core answered at once, when the input is a call with an
    /// answer (`propose`).
    pub answer: Option<String>,
    /// The batches the input produced, in order. Empty when it changed
    /// nothing that a driver must do.
    pub readies: Vec<ReadyView>,
}

/// One node, as the page shows it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LabNodeView {
    /// The node's id.
    pub id: u64,
    /// `follower`, `candidate` or `leader`.
    pub role: String,
    /// The promise on its disk, as `round.node`.
    pub promise: String,
    /// How many accepted records its disk holds.
    pub records: usize,
    /// The chosen prefix it knows, as the next unchosen slot.
    pub chosen: u64,
}

/// The whole lab after an input.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LabView {
    /// The nodes.
    pub nodes: Vec<LabNodeView>,
    /// The messages on the wire, oldest first.
    pub wire: Vec<WireView>,
    /// What the last input did.
    pub last: Option<StepView>,
    /// Why the last input was refused, when it was.
    pub error: Option<String>,
}

/// Three nodes, their disks and the wire between them.
pub struct ReadyLab {
    nodes: Vec<ColocatedNode>,
    disks: Vec<Disk>,
    wire: Vec<InFlight>,
    next_id: u64,
    next_seq: u64,
    clock: u64,
    last: Option<StepView>,
    error: Option<String>,
}

impl Default for ReadyLab {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadyLab {
    /// Three fresh nodes over a majority quorum, each on an empty disk, and
    /// an empty wire. The client is the journal's writer from the start.
    #[must_use]
    pub fn new() -> Self {
        let peers: Vec<NodeId> = (0..NODES).map(NodeId).collect();
        let disks: Vec<Disk> = peers
            .iter()
            .map(|id| {
                let mut disk = Disk::new(Config {
                    peers: peers.clone(),
                    quorum_system: QuorumSystem::Majority,
                    ..Config::new(*id, paros_core::journal::JournalIdentifier::UNSET)
                });
                disk.provision_owner(CLIENT);
                disk
            })
            .collect();
        let nodes = disks
            .iter()
            .map(|disk| ColocatedNode::new(disk, Limits::DEFAULT))
            .collect();
        Self {
            nodes,
            disks,
            wire: Vec::new(),
            next_id: 1,
            next_seq: 0,
            clock: 0,
            last: None,
            error: None,
        }
    }

    /// Give the lab one input.
    ///
    /// # Errors
    ///
    /// A node or a message that does not exist, or a write with no text:
    /// nothing changes.
    pub fn act(&mut self, input: &Input) -> Result<(), String> {
        self.error = None;
        let step = match input {
            Input::Timeout { node } => {
                let n = self.node(*node)?;
                self.nodes[n].set_election_timeout(1);
                self.nodes[n].tick();
                self.step(
                    format!("tick() at node {node}, its election clock runs out"),
                    n,
                    None,
                )
            }
            Input::Tick { node } => {
                let n = self.node(*node)?;
                self.nodes[n].tick();
                self.step(format!("tick() at node {node}"), n, None)
            }
            Input::Deliver { id } => {
                let at = self.wire_index(*id)?;
                let entry = self.wire.remove(at);
                let Party::Node(to) = entry.to else {
                    return Err("the lab has no matchmaker".into());
                };
                let Some(message) = entry.message().cloned() else {
                    return Err("the lab carries node messages only".into());
                };
                let n = self.node(to.0)?;
                let kind = view_of(&entry).kind;
                self.nodes[n].step(message);
                self.step(format!("step({kind}) at node {}", to.0), n, None)
            }
            Input::Drop { id } => {
                let at = self.wire_index(*id)?;
                let entry = self.wire.remove(at);
                StepView {
                    input: format!("the network loses {} #{id}", view_of(&entry).kind),
                    node: None,
                    answer: None,
                    readies: Vec::new(),
                }
            }
            Input::Write { node, text } => {
                let n = self.node(*node)?;
                if text.trim().is_empty() {
                    return Err("a write carries some text".into());
                }
                let entry = Entry {
                    leader: CLIENT.uuid(),
                    seq: Seq(self.next_seq),
                    records: vec![Value(text.trim().as_bytes().to_vec())],
                };
                let answer = match self.nodes[n].propose(entry) {
                    ProposeResult::Accepted(slot) => {
                        self.next_seq += 1;
                        format!("Accepted(slot {})", slot.0)
                    }
                    ProposeResult::NotLeader(Some(leader)) => {
                        format!("NotLeader: try node {}", leader.0)
                    }
                    ProposeResult::NotLeader(None) => "NotLeader: no leader known".into(),
                };
                self.step(
                    format!("propose(\"{}\") at node {node}", text.trim()),
                    n,
                    Some(answer),
                )
            }
        };
        self.last = Some(step);
        Ok(())
    }

    /// Give the lab an input, and keep a refusal for the view.
    pub fn act_or_note(&mut self, input: &Input) {
        if let Err(error) = self.act(input) {
            self.error = Some(error);
        }
    }

    /// The lab as the page shows it.
    #[must_use]
    pub fn view(&self) -> LabView {
        LabView {
            nodes: self
                .nodes
                .iter()
                .zip(&self.disks)
                .map(|(node, disk)| LabNodeView {
                    id: node.config().id.0,
                    role: format!("{:?}", show_role(node.role())).to_lowercase(),
                    promise: show_ballot(disk.hard_state().max_promised_ballot),
                    records: disk.records().len(),
                    chosen: node.replica().chosen_index().map_or(0, |s| s.0 + 1),
                })
                .collect(),
            wire: self.wire.iter().map(view_of).collect(),
            last: self.last.clone(),
            error: self.error.clone(),
        }
    }

    fn node(&self, id: u64) -> Result<usize, String> {
        usize::try_from(id)
            .ok()
            .filter(|n| *n < self.nodes.len())
            .ok_or_else(|| format!("no node {id}: name 0 to {}", NODES - 1))
    }

    fn wire_index(&self, id: u64) -> Result<usize, String> {
        self.wire
            .iter()
            .position(|m| m.id == id)
            .ok_or_else(|| format!("no message {id} on the wire"))
    }

    /// Drain node `n` until it is quiet: each `Ready` copied out, its writes
    /// persisted, its messages put on the wire, then `advance()` and
    /// `advance_recovery()`.
    fn step(&mut self, input: String, n: usize, answer: Option<String>) -> StepView {
        let mut readies = Vec::new();
        for _ in 0..DRAIN_BUDGET {
            let ready = self.drain(n);
            if ready.is_empty() {
                break;
            }
            readies.push(ready);
        }
        StepView {
            input,
            node: Some(self.nodes[n].config().id.0),
            answer,
            readies,
        }
    }

    fn drain(&mut self, n: usize) -> ReadyView {
        let node = &mut self.nodes[n];
        let me = node.config().id;
        let pool: Vec<NodeId> = node.config().pool().to_vec();
        let ready = node.ready();
        let writes: Vec<WriteOp> = ready.writes().to_vec();
        let messages: Vec<(NodeId, Message)> = ready
            .messages()
            .iter()
            .flat_map(|(audience, message)| {
                audience
                    .resolve(&pool, me)
                    .into_iter()
                    .map(move |to| (to, message.clone()))
            })
            .collect();
        let committed: Vec<String> = ready
            .committed()
            .iter()
            .map(|(slot, ballot, command, outcome)| {
                format!(
                    "slot {} = {} at {}: {:?}",
                    slot.0,
                    show_command(command),
                    show_ballot(*ballot),
                    outcome
                )
            })
            .collect();
        let read_states: Vec<String> = ready.read_states().iter().map(read_text).collect();
        ready.advance();
        // Bucket 1: persist, in order.
        for op in &writes {
            self.disks[n].apply(op);
        }
        // Bucket 2: send, after the writes are durable.
        let mut sent = Vec::new();
        for (to, message) in messages {
            let entry = InFlight {
                id: self.next_id,
                from: Party::Node(me),
                to: Party::Node(to),
                envelope: Envelope::Node(message),
                sent_at: self.clock,
            };
            self.next_id += 1;
            sent.push(view_of(&entry));
            self.wire.push(entry);
        }
        self.clock += 1;
        self.nodes[n].advance_recovery();
        ReadyView {
            sync: writes.iter().any(WriteOp::needs_sync),
            writes: writes.iter().map(write_view).collect(),
            messages: sent,
            committed,
            read_states,
        }
    }
}

/// The page's view of one wire entry.
fn view_of(entry: &InFlight) -> WireView {
    let (Party::Node(from), Party::Node(to)) = (entry.from, entry.to) else {
        unreachable!("the lab carries node messages only");
    };
    let Some(message) = entry.message() else {
        unreachable!("the lab carries node messages only");
    };
    let view = crate::paxos::view::message_view(
        entry.id,
        from.0,
        to.0,
        entry.sent_at,
        message,
        QuorumSystem::Majority,
    );
    WireView {
        id: entry.id,
        from: from.0,
        to: to.0,
        kind: view.kind,
        phase: view.phase,
        summary: view.summary,
    }
}

/// One `WriteOp`, in words.
fn write_view(op: &WriteOp) -> WriteView {
    let (kind, detail) = match op {
        WriteOp::Acceptor(AcceptorWrite::SetPromise(ballot)) => {
            ("SetPromise", format!("promise {}", show_ballot(*ballot)))
        }
        WriteOp::Acceptor(AcceptorWrite::AppendAccepted {
            slot,
            ballot,
            value,
        }) => (
            "AppendAccepted",
            format!(
                "slot {} = {} at {}",
                slot.0,
                show_command(value),
                show_ballot(*ballot)
            ),
        ),
        WriteOp::SetChosenIndex(slot) => {
            ("SetChosenIndex", format!("chosen up to slot {}", slot.0))
        }
        WriteOp::Learned { slot, .. } => ("Learned", format!("slot {}", slot.0)),
        WriteOp::Truncate { first, .. } => ("Truncate", format!("drop below slot {}", first.0)),
        WriteOp::TrimmedTo { point, .. } => ("TrimmedTo", format!("jump to slot {}", point.0)),
        WriteOp::RegistrationFence(ballot) => (
            "RegistrationFence",
            format!("registered at {}", show_ballot(*ballot)),
        ),
        WriteOp::RetireMark(mark) => (
            "RetireMark",
            format!(
                "incarnation {}, {}",
                mark.incarnation,
                if mark.checked {
                    "checked"
                } else {
                    "not checked"
                }
            ),
        ),
    };
    WriteView {
        kind: kind.into(),
        detail,
        needs_sync: op.needs_sync(),
    }
}

/// One `ReadState`, in words.
fn read_text(state: &ReadState) -> String {
    format!("{state:?}")
}

/// The first slot a command is chosen at, for the tests.
#[cfg(test)]
fn chosen_text(view: &LabView, node: usize) -> u64 {
    view.nodes[node].chosen
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deliver_all(lab: &mut ReadyLab) {
        for _ in 0..200 {
            let Some(id) = lab.view().wire.first().map(|m| m.id) else {
                return;
            };
            lab.act(&Input::Deliver { id })
                .expect("a wire message delivers");
        }
        panic!("the wire never went quiet");
    }

    #[test]
    fn a_timeout_writes_the_promise_before_it_sends_a_prepare() {
        let mut lab = ReadyLab::new();
        lab.act(&Input::Timeout { node: 0 }).expect("node 0 exists");
        let view = lab.view();
        let step = view.last.expect("the input ran");
        let first = &step.readies[0];
        assert!(
            first
                .writes
                .iter()
                .any(|w| w.kind == "SetPromise" && w.needs_sync),
            "a campaign makes its own promise durable: {first:?}"
        );
        assert!(first.sync, "a promise needs a sync");
        let prepares = first
            .messages
            .iter()
            .filter(|m| m.kind == "Prepare")
            .count();
        assert_eq!(prepares, 2, "one Prepare per peer: {:?}", first.messages);
        assert_eq!(view.wire.len(), first.messages.len());
        assert_eq!(
            view.nodes[0].promise,
            first.writes[0].detail.replace("promise ", "")
        );
    }

    #[test]
    fn a_write_is_chosen_after_its_accepteds_and_committed_in_a_later_ready() {
        let mut lab = ReadyLab::new();
        lab.act(&Input::Timeout { node: 0 }).expect("node 0 exists");
        deliver_all(&mut lab);
        assert_eq!(lab.view().nodes[0].role, "leader");
        lab.act(&Input::Write {
            node: 0,
            text: "alpha".into(),
        })
        .expect("a write");
        let step = lab.view().last.expect("ran");
        assert_eq!(step.answer.as_deref(), Some("Accepted(slot 0)"));
        let first = &step.readies[0];
        assert!(
            first.writes.iter().any(|w| w.kind == "AppendAccepted"),
            "the leader votes for its own value: {first:?}"
        );
        assert!(first.messages.iter().any(|m| m.kind == "Accept"));
        assert!(first.committed.is_empty(), "nothing is chosen yet");
        deliver_all(&mut lab);
        let view = lab.view();
        assert!(view.nodes.iter().all(|n| n.records >= 1));
        assert_eq!(chosen_text(&view, 0), 1, "slot 0 is chosen at the leader");
    }

    #[test]
    fn a_follower_answers_not_leader() {
        let mut lab = ReadyLab::new();
        lab.act(&Input::Write {
            node: 1,
            text: "alpha".into(),
        })
        .expect("a write");
        let step = lab.view().last.expect("ran");
        assert!(step.answer.expect("an answer").starts_with("NotLeader"));
        assert!(step.readies.is_empty());
    }

    #[test]
    fn a_lost_message_leaves_the_wire_and_reaches_nobody() {
        let mut lab = ReadyLab::new();
        lab.act(&Input::Timeout { node: 0 }).expect("node 0 exists");
        let before = lab.view().wire.len();
        let id = lab.view().wire[0].id;
        lab.act(&Input::Drop { id }).expect("on the wire");
        assert_eq!(lab.view().wire.len(), before - 1);
        assert!(lab.act(&Input::Deliver { id }).is_err(), "it is gone");
        assert!(lab.act(&Input::Tick { node: 9 }).is_err(), "no node 9");
    }
}
