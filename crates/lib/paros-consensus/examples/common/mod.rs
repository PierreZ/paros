//! The helpers the examples share: the values, the printers, the node of
//! the log lessons (`multi_paxos`, `acceptor_grid`), and the toy cluster of the
//! examples that drive `ColocatedNode` ([`cluster::Cluster`]). Each example
//! uses a part of them.
//!
//! This is Cargo's place for code shared by examples: a file
//! `examples/common.rs` would be built as an example of its own.

#![allow(dead_code)]

pub mod cluster;

use std::collections::BTreeMap;

use paros_consensus::acceptor::{AcceptOutcome, Acceptor, PrepareOutcome};
use paros_consensus::journal::{Command, Control, Entry, LeaderUuid, Seq};
use paros_consensus::proposer::Proposer;
use paros_consensus::replica::Replica;
use paros_consensus::{Ballot, NodeId, Slot, Value, WriteOp};

/// A ballot prints as `round.node`, so `7.8` is round 7 of node 8.
pub fn ballot(round: u64, node: NodeId) -> Ballot {
    Ballot { round, node }
}

/// A client command: some bytes the client wants applied, tagged with
/// `(client, seq)` so the log can tell a retry from a new request and
/// execute each request at most once.
pub fn command(text: &str, seq: u64) -> Command {
    Command::Write(Entry {
        leader: LeaderUuid(1),
        seq: Seq(seq),
        records: vec![Value::from(text.as_bytes().to_vec())],
    })
}

/// The command that does nothing. A new leader chooses it in a slot nobody
/// reported a value for, so the log has no hole.
pub fn noop() -> Command {
    Command::Control(Control::Noop)
}

/// A command as the trace prints it: the text of a write, or the control.
pub fn show(command: &Command) -> String {
    match command {
        Command::Write(entry) => format!("{:?}", String::from_utf8_lossy(&entry.records[0])),
        Command::Control(control) => format!("{control:?}"),
    }
}

/// What a promise reports: every record the acceptor holds in the range
/// the `Prepare` asked about, one `slot = value @ballot` each. Empty means
/// "I accepted nothing there", which is information too.
pub fn show_records(records: &BTreeMap<Slot, (Ballot, Command)>) -> String {
    if records.is_empty() {
        return "nothing accepted in the suffix".to_string();
    }
    let each: Vec<String> = records
        .iter()
        .map(|(s, (b, c))| format!("slot {} = {} @{b}", s.0, show(c)))
        .collect();
    format!("accepted: {}", each.join(", "))
}

/// One node of the log lessons: the three roles and its disk. The
/// [`Acceptor`] (the promise and the accepted records) and the [`Replica`]
/// (the chosen log) own durable state and push every change into `disk`; a
/// real driver fsyncs it before any reply leaves the node. The
/// [`Proposer`] owns *only* volatile state: leadership dies whole with the
/// process, which is why a restart constructs a fresh one and boots as a
/// follower. `accepts_seen` counts the `Accept`s the node was asked to vote
/// on, which `acceptor_grid` compares across its columns.
pub struct Node {
    pub id: NodeId,
    pub acceptor: Acceptor<Command>,
    pub proposer: Proposer<NodeId, Command>,
    pub replica: Replica,
    pub disk: Vec<WriteOp>,
    pub alive: bool,
    pub accepts_seen: usize,
}

impl Node {
    /// A node with an empty disk.
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            acceptor: Acceptor::new(Ballot::zero(), BTreeMap::new(), Slot(0), BTreeMap::new()),
            proposer: Proposer::new(),
            replica: Replica::from_boot(
                paros_consensus::journal::WriterMode::Single,
                None,
                Slot(0),
                paros_consensus::journal::JournalState::default(),
                &BTreeMap::new(),
            ),
            disk: Vec::new(),
            alive: true,
            accepts_seen: 0,
        }
    }

    /// Phase 1b, the log-shaped promise: vow to accept nothing below
    /// `ballot` in any slot at or after `from_slot`, and report **every**
    /// record accepted in that range — the new leader's P2c input, one
    /// entry per slot. A refusal (`Err`) carries the higher ballot this
    /// acceptor already promised, so the candidate learns it was outrun.
    /// (The reply is one page; a long log would be paged with a cursor,
    /// which a short log never needs.)
    pub fn on_prepare(
        &mut self,
        ballot: Ballot,
        from_slot: Slot,
    ) -> Result<BTreeMap<Slot, (Ballot, Command)>, Ballot> {
        match self.acceptor.prepare(ballot, from_slot, &mut self.disk) {
            PrepareOutcome::Promised { .. } => Ok(self
                .acceptor
                .promise_page(from_slot, paros_consensus::contract::limits::PROMISE_BATCH)
                .accepted),
            PrepareOutcome::Refused | PrepareOutcome::BelowFloor => Err(self.acceptor.promised()),
        }
    }

    /// Phase 2b, exactly as in the single-decree example, at one slot: the
    /// acceptor accepts unless it has promised a higher ballot, and writes
    /// the record to disk before answering.
    pub fn on_accept(
        &mut self,
        ballot: Ballot,
        slot: Slot,
        command: Command,
    ) -> Result<(), Ballot> {
        self.accepts_seen += 1;
        match self.acceptor.admit(ballot, slot) {
            AcceptOutcome::Admitted => {
                self.acceptor.set_promise(ballot, &mut self.disk);
                self.acceptor
                    .record_accepted(slot, ballot, command, &mut self.disk);
                Ok(())
            }
            AcceptOutcome::Refused | AcceptOutcome::BelowFloor => Err(self.acceptor.promised()),
        }
    }

    /// The learner: `command` was chosen at `slot`, either decided by this
    /// node's own tally or announced by a `Commit`. Two things happen.
    /// First, the chosen value becomes the acceptor's *authoritative* record
    /// for the slot — an overwrite, so a stale lower-ballot accept left by a
    /// failed ballot is replaced and this node's disk agrees with the log it
    /// serves (catch-up requests and the next boot both read it back). That
    /// is local repair, not what keeps Paxos safe; `multi_paxos` says which
    /// is which. Second, the replica files the value and walks its applied
    /// prefix forward. Chosen is not yet applied: the walk applies in slot
    /// order, so a slot chosen ahead of a hole waits for the hole to close.
    pub fn learn_chosen(&mut self, slot: Slot, at: Ballot, command: &Command) {
        if let Some(known) = self.replica.chosen_at(&self.acceptor, slot) {
            // Learning a slot twice must bring the same value: that is
            // Paxos safety, observed at one node.
            assert_eq!(known, command, "a slot is chosen once");
            return;
        }
        if at > self.acceptor.promised() {
            self.acceptor.set_promise(at, &mut self.disk);
        }
        self.acceptor
            .record_accepted(slot, at, command.clone(), &mut self.disk);
        self.replica.learn(&self.acceptor, slot, command);
        self.replica.advance(&self.acceptor, &mut self.disk);
    }

    /// The applied log: every chosen value from slot 0 up to the first slot
    /// not yet chosen, in slot order. A chosen slot beyond a hole is not in
    /// it.
    pub fn log(&self) -> Vec<String> {
        let end = self.replica.first_unchosen();
        self.replica
            .chosen()
            .range(..end)
            .map(|slot| {
                let command = self
                    .replica
                    .chosen_at(&self.acceptor, *slot)
                    .expect("a chosen slot holds its record");
                show(command)
            })
            .collect()
    }
}

/// The node `id` of `cluster`.
pub fn node(cluster: &mut [Node], id: NodeId) -> &mut Node {
    cluster
        .iter_mut()
        .find(|n| n.id == id)
        .expect("a known node")
}

/// The learner's message: `command` is chosen at `slot`, under ballot
/// `at`. The leader assembled the quorum, exactly as a Paxos learner does.
/// Only the live nodes in `reach` hear it, which is how a lesson leaves a
/// chosen value that nobody but a dead leader knows about.
pub fn commit(cluster: &mut [Node], slot: Slot, at: Ballot, command: &Command, reach: &[NodeId]) {
    for peer in reach {
        let peer = node(cluster, *peer);
        if peer.alive && !peer.replica.is_chosen(slot) {
            peer.learn_chosen(slot, at, command);
        }
    }
}

/// Every live node applied the same log, `expected`.
pub fn assert_same_log(cluster: &[Node], expected: &[&str]) {
    for node in cluster.iter().filter(|n| n.alive) {
        assert_eq!(
            node.log(),
            expected,
            "node {} applied a different log",
            node.id.0
        );
    }
}
