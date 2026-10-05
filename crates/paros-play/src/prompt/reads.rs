//! The read question: a leaderless read's watermark.

use std::collections::BTreeMap;

use paros_core::{NodeId, Slot};

use super::{Choice, Prompt, PromptKind};
use crate::narration;

impl Prompt {
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
        let here = narration::at(applied);
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
