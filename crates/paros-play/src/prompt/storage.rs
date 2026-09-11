//! The disk's questions: the order a batch reaches it, the promise a snapshot
//! leaves behind, the CTRL case a damaged record puts a slot in, and the boot
//! an erased disk earns.

use std::collections::BTreeMap;

use paros_core::{Ballot, Command, NodeId, Slot};

use crate::view::{show_ballot, show_command};

use super::{Choice, Prompt, PromptKind};

impl Prompt {
    /// A drained batch holds both durable writes and outbound messages.
    ///
    /// There is nothing to compute: the answer is always "sync first". It is
    /// the persist-before-send edge, and it is the whole reason the `Ready`
    /// handshake exists.
    #[must_use]
    pub fn persist_order(id: u64, node: NodeId, writes: usize, messages: usize) -> Self {
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "send_first".to_string(),
            format!(
                "A send first is the classic loss of data in Paxos. This batch holds {writes} \
                 durable write(s) and {messages} message(s), and every message is a *claim \
                 about the writes*. A Promise says that the promise of the node is now durable \
                 at that ballot. An Accepted says that the node holds the value on disk. If you \
                 send the messages and the node crashes before the disk write, the node \
                 reboots without a promise that it published. It can also reboot without a vote \
                 that a proposer counted toward a majority. It can then accept a lower ballot \
                 that it refused, \
                 and either fault gives one slot two values. Write the batch to disk, then \
                 send it."
            ),
        );
        Self {
            id,
            kind: PromptKind::PersistOrder,
            node: node.0,
            question: "This batch holds writes and messages. Which goes first?".to_string(),
            state_summary: vec![
                format!("durable writes in the batch: {writes}"),
                format!("messages in the batch: {messages}"),
            ],
            choices: vec![
                Choice::new("sync_first", "Write the batch to disk, then send"),
                Choice::new("send_first", "Send, then write the batch to disk"),
            ],
            expected: "sync_first".to_string(),
            explanations,
            feedback: None,
        }
    }

    /// A peer's snapshot arrived at a node stranded below the cluster's floor.
    /// What is its durable promise afterwards?
    ///
    /// Judged on a **clone of the acceptor**, driven exactly as
    /// `ColocatedNode::on_install_snapshot` drives the real one: raise the
    /// promise to the snapshot's ballot only if that ballot is higher, then
    /// [`paros_core::acceptor::Acceptor::install`]. `promised` is what the
    /// clone holds afterwards, and the offered ballots are matched against it
    /// — so the answer is the core's, not a comparison restated here.
    ///
    /// The two choices are the two concrete ballots that differ: the higher of
    /// the pair, and the other one. When the snapshot's ballot is the lower,
    /// picking it is the mistake the whole level exists for — a snapshot
    /// restores the log, never a promise, and a node that forgot a promise it
    /// had already made is free to vote for a ballot it had sworn to refuse.
    #[must_use]
    pub fn snapshot_promise(
        id: u64,
        node: NodeId,
        at: Slot,
        snapshot_ballot: Ballot,
        held: Ballot,
        promised: Ballot,
    ) -> Self {
        let sb = show_ballot(snapshot_ballot);
        let hb = show_ballot(held);
        let higher = held.max(snapshot_ballot);
        let lower = held.min(snapshot_ballot);
        let expected = if promised == higher {
            "higher"
        } else {
            "lower"
        };
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "lower".to_string(),
            format!(
                "This answer lowers the durable promise of this node to {}. A node must not \
                 take back a promise. When it promised {hb}, it told a proposer that every \
                 lower ballot was finished here, and that proposer possibly chose a value from \
                 that answer. A snapshot restores the *log*: the values, the prefix and the \
                 state of the application. It says nothing about promises, and the peer that \
                 sent it does not know what this node promised. Always take the higher of the \
                 two ballots. For the same reason, a node whose disk was *erased* cannot \
                 rejoin: a snapshot cannot give back a promise that the node no longer holds.",
                show_ballot(lower)
            ),
        );
        Self {
            id,
            kind: PromptKind::SnapshotPromise,
            node: node.0,
            question: format!(
                "A snapshot arrived. It covers every slot up to slot {}, and a node took it \
                 under ballot {sb}. You promised {hb}. What is your promise now?",
                at.0
            ),
            state_summary: vec![
                format!("my own durable promise: {hb}"),
                format!("the ballot of the snapshot: {sb}"),
                format!("the snapshot covers every slot up to slot {}", at.0),
            ],
            choices: vec![
                Choice::new("higher", format!("Promise {}", show_ballot(higher))),
                Choice::new("lower", format!("Promise {}", show_ballot(lower))),
            ],
            expected: expected.to_string(),
            explanations,
            feedback: None,
        }
    }

    /// A leader's repair probe holds `slot`, whose value one acceptor lost.
    /// Which of the three CTRL cases is this, and what may be re-proposed?
    ///
    /// Judged on a **clone of the proposer**: the arriving `Promise` is folded
    /// through
    /// [`fold_probe_promise`](paros_core::proposer::Proposer::fold_probe_promise)
    /// and the probe is resolved through
    /// [`resolve_probe`](paros_core::proposer::Proposer::resolve_probe). A
    /// decision carrying a value is Case 1, a decision carrying none is Case
    /// 2, and no decision at all is Case 3.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn repair_verdict(
        id: u64,
        node: NodeId,
        slot: Slot,
        reported: Option<&Command>,
        faulty_at: Option<Ballot>,
        expected: RepairCase,
    ) -> Self {
        let value = reported.map(show_command);
        let rotted = faulty_at.map_or_else(
            || "a peer reports no damage".to_string(),
            |ballot| {
                format!(
                    "a peer lost its value for slot {}, which it accepted at ballot {}",
                    slot.0,
                    show_ballot(ballot)
                )
            },
        );
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "case1".to_string(),
            match &value {
                Some(value) => format!(
                    "This is the case where the probe re-proposes {value}. You read this text \
                     because you selected another answer."
                ),
                None => format!(
                    "There is no value to re-propose. No acceptor reported a value for slot {}. \
                     The acceptor that voted there lost the value, and every acceptor that \
                     answered reports no vote there.",
                    slot.0
                ),
            },
        );
        explanations.insert(
            "case2".to_string(),
            match &value {
                Some(value) => format!(
                    "A Noop here decides a *different* value at a slot that an earlier ballot \
                     possibly chose. A promise reported {value} for slot {}, at a ballot at or \
                     above the damaged record. This ballot may put only that reported value in \
                     slot {}.",
                    slot.0, slot.0
                ),
                None => format!(
                    "A Noop here can overwrite a chosen value. A Noop is safe only after a full \
                     Phase-1 quorum answers and no answer can hide a chosen value. One acceptor \
                     answers \"I voted in that slot and I do not know the value any more\", and \
                     that answer hides what a Noop overwrites. Slot {} stays undecided until \
                     enough of the other acceptors answer.",
                    slot.0
                ),
            },
        );
        explanations.insert(
            "case3".to_string(),
            match &value {
                Some(value) => format!(
                    "A wait gains nothing now. The reports hold {value} for slot {}, accepted \
                     at a ballot at or above the damaged record. A value chosen at or below \
                     that ballot is the same value. A value chosen above it left a record on \
                     a member of the quorum that answered. Re-propose the value, and the \
                     damaged acceptor writes it back when it votes.",
                    slot.0
                ),
                None => format!(
                    "A wait gains nothing now. Enough acceptors answered, so no chosen value \
                     can hide behind the damage. A full Phase-1 quorum reported nothing, or a \
                     record no higher than the record that the probe holds. While you wait, \
                     slot {} stays a hole in the prefix of every node.",
                    slot.0
                ),
            },
        );
        let expected_id = match expected {
            RepairCase::ReproposeReported => "case1",
            RepairCase::FillNoop => "case2",
            RepairCase::Wait => "case3",
        };
        Self {
            id,
            kind: PromptKind::RepairVerdict,
            node: node.0,
            question: format!("What may this ballot put in slot {}?", slot.0),
            state_summary: vec![
                rotted,
                format!(
                    "the highest value that any answer reports for slot {}: {}",
                    slot.0,
                    value.clone().unwrap_or_else(|| "none".to_string())
                ),
            ],
            choices: vec![
                Choice::new(
                    "case1",
                    match &value {
                        Some(value) => format!("Re-propose {value}"),
                        None => "Re-propose the reported value".to_string(),
                    },
                ),
                Choice::new("case2", "Decide a Noop"),
                Choice::new("case3", "Wait for more answers"),
            ],
            expected: expected_id.to_string(),
            explanations,
            feedback: None,
        }
    }

    /// A node whose disk was erased asks to come back. Boot it fresh, or
    /// refuse?
    ///
    /// **The answer is a constant, and deliberately so.** There is no role to
    /// clone: the store carries no promise to read back, which is the whole
    /// problem. What decides it is the operator's own record that this
    /// identity was provisioned once, and the library refuses such a boot
    /// outright rather than taking a branch in a state machine. The `refuse`
    /// side therefore carries no explanation and the `boot_fresh` side carries
    /// the whole story.
    #[must_use]
    pub fn wiped_rejoin(id: u64, node: NodeId, promised: Ballot) -> Self {
        let held = show_ballot(promised);
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "boot_fresh".to_string(),
            format!(
                "This answer lets one slot get two values. A fresh boot puts this node back \
                 in the pool with an empty promise, and the node promised {held} before. It now \
                 answers a ballot below {held}, because it holds no record of that promise, and \
                 it votes for the value of that ballot. A proposer already ran Phase 1 at \
                 {held}. That proposer learned that this acceptor held nothing newer, and it \
                 possibly chose a value from that answer. A quorum of this node and the \
                 acceptors at the older ballot then chooses a second value for one slot. A \
                 snapshot does not repair that, because a snapshot restores the log and not a \
                 promise, and no peer knows what this node promised. Refuse the boot: the \
                 cluster changes its acceptor set instead, and that change leaves this identity \
                 out of every quorum."
            ),
        );
        Self {
            id,
            kind: PromptKind::WipedRejoin,
            node: node.0,
            question: format!(
                "The disk of node {} is empty, and the node was a member. Boot it fresh, or \
                 refuse the boot?",
                node.0
            ),
            state_summary: vec![
                format!("the last promise that this node made: {held}"),
                "the disk now holds nothing".to_string(),
                "an operator provisioned this identity once".to_string(),
            ],
            choices: vec![
                Choice::new("refuse", "Refuse the boot"),
                Choice::new("boot_fresh", "Boot the node fresh, as a new node"),
            ],
            expected: "refuse".to_string(),
            explanations,
            feedback: None,
        }
    }
}

impl Prompt {}

/// Which CTRL case a repair probe's answers put a faulty slot in — the shape
/// of [`paros_core::proposer::Proposer::resolve_probe`]'s answer, named for
/// the player.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RepairCase {
    /// Case 1: a value was reported, and this ballot re-proposes it.
    ReproposeReported,
    /// Case 2: a full Phase-1 quorum of qualifying answers reported no value
    /// at all, so the slot may be decided as a `Noop`.
    FillNoop,
    /// Case 3: not enough answers yet. The slot stays undecided.
    Wait,
}
