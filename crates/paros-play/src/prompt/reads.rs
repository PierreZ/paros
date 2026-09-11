//! The two read questions: a read-index round's leadership proof, and a
//! leaderless read's watermark.

use std::collections::BTreeMap;

use paros_core::{NodeId, Slot};

use super::{Choice, Prompt, PromptKind};

impl Prompt {
    /// A read at `ctx` captured `index`; an ack just arrived. Serve, or wait?
    ///
    /// Judged by [`paros_core::proposer::Proposer::confirm_reads`] on a clone,
    /// after crediting this ack: a read confirms only once a **Phase-2 quorum**
    /// has acked a beat broadcast at or after the read began *and* the applied
    /// prefix covers the captured index.
    ///
    /// `acks` is the tally **this node's own vote included**. A read round is
    /// seeded with the leader itself, because a leader is an acceptor of its
    /// own configuration and its own state is the first evidence it has. The
    /// card used to print the peer acks alone, and a player who read "1" and
    /// waited for a third was marked wrong for waiting.
    #[must_use]
    // Every argument is one line of the card, and bundling them would only
    // rename them.
    #[allow(clippy::too_many_arguments)]
    pub fn read_serve(
        id: u64,
        node: NodeId,
        ctx: u64,
        index: Option<Slot>,
        acks: usize,
        members: usize,
        chosen_index: Option<Slot>,
        confirmed: bool,
    ) -> Self {
        let at = index.map_or_else(
            || "the empty prefix".to_string(),
            |s| format!("slot {}", s.0),
        );
        let applied =
            chosen_index.map_or_else(|| "nothing".to_string(), |s| format!("slot {}", s.0));
        let expected = if confirmed { "serve" } else { "wait" };
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "serve".to_string(),
            format!(
                "This answer gives the client whatever this node holds now. The acks in hand \
                 are {acks} of {members}, and that count includes the vote of this node. They \
                 must make a Phase-2 quorum of the configuration of this ballot, for a beat \
                 sent at or after the read started. The applied prefix ({applied}) must also \
                 cover {at}. One of those two conditions does not hold. A leader cannot separate \"my followers are slow\" from \"another \
                 node replaced me and commits without me\". A read on that state gives the \
                 client a value older than a write that the cluster acknowledged to another \
                 client. Wait for the acks of the quorum, because they are the proof and they \
                 need no log write."
            ),
        );
        Self {
            id,
            kind: PromptKind::ReadServe,
            node: node.0,
            question: format!(
                "The read of the client (#{ctx}) captured {at}. Serve it, or \
                 wait?"
            ),
            state_summary: vec![
                format!("read index captured: {at}"),
                format!("the acks, with the vote of this node: {acks} of {members}"),
                format!("the applied prefix ends at: {applied}"),
            ],
            choices: vec![
                Choice::new("serve", format!("Serve the read at {at}")),
                Choice::new("wait", "Wait for the ack quorum"),
            ],
            expected: expected.to_string(),
            explanations,
            feedback: None,
        }
    }

    /// A quorum read's row has answered: the highest slot any of them has
    /// voted in is `watermark`, and this node has applied up to `applied`.
    ///
    /// Judged on a clone of the node's own
    /// [`QuorumReads`](paros_core::quorum_read::QuorumReads), through
    /// [`serve`](paros_core::quorum_read::QuorumReads::serve) with the
    /// replica's own `covers`: `served` is what the clone did.
    #[must_use]
    pub fn quorum_read_serve(
        id: u64,
        node: NodeId,
        ctx: u64,
        watermark: Option<Slot>,
        applied: Option<Slot>,
        answered: usize,
        served: bool,
    ) -> Self {
        let high =
            watermark.map_or_else(|| "nothing at all".to_string(), |s| format!("slot {}", s.0));
        let here = applied.map_or_else(|| "nothing".to_string(), |s| format!("slot {}", s.0));
        let expected = if served { "serve" } else { "wait" };
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "serve".to_string(),
            format!(
                "This answer gives the client a state older than a write that it already has. \
                 The highest vote of the row is {high}, and this node applied {here}, so the \
                 prefix does not reach the watermark. One acceptor in that row voted in a slot \
                 that this node did not execute. A write acknowledged before the read started \
                 is possibly that slot. Wait: ordinary replication brings the slot here, \
                 and this node answers the read when the prefix covers it."
            ),
        );
        explanations.insert(
            "wait".to_string(),
            format!(
                "A wait gains nothing here, and no leader takes part. This node applied \
                 {here}, and that prefix already covers the highest vote of the row ({high}). A \
                 Phase-2 quorum chose every write acknowledged before this read started, and \
                 that quorum meets the row that this read asked. The maximum of the row is \
                 therefore at or above that write, and this prefix is at or above the maximum."
            ),
        );
        Self {
            id,
            kind: PromptKind::QuorumReadServe,
            node: node.0,
            question: format!("The row has answered read #{ctx}. Serve it, or wait?"),
            state_summary: vec![
                format!("the acceptors that answered: {answered}"),
                format!("the highest slot that any of them voted in: {high}"),
                format!("this node applied: {here}"),
            ],
            choices: vec![
                Choice::new("serve", format!("Serve the read at {high}")),
                Choice::new("wait", "Wait, because the prefix does not reach it"),
            ],
            expected: expected.to_string(),
            explanations,
            feedback: None,
        }
    }
}
