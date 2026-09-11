//! The replica's questions: whether a slot that just became chosen applies
//! now, and which of the three honest answers a client's retry earns.

use std::collections::BTreeMap;

use paros_core::{NodeId, Slot};

use super::{Choice, Prompt, PromptKind};

impl Prompt {
    /// A message just made `slot` chosen at this node. Apply it, or hold?
    ///
    /// `applies_now` is the core's own answer, computed by the caller on a
    /// **clone of the replica**: learn the slot, run the contiguous walk, and
    /// see whether the walk surfaced this slot as committed. The rule it
    /// embodies is the contiguity one — the applied prefix has no holes, so a
    /// slot is executed exactly when the walk reaches it — but the answer
    /// comes from [`paros_core::replica::Replica::advance`], not from a
    /// comparison restated here.
    #[must_use]
    pub fn replica_apply(
        id: u64,
        node: NodeId,
        slot: Slot,
        chosen_index: Option<Slot>,
        first_unchosen: Slot,
        applies_now: bool,
    ) -> Self {
        let at = chosen_index.map_or_else(|| "nothing".to_string(), |s| format!("slot {}", s.0));
        let expected = if applies_now { "apply" } else { "hold" };
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "apply".to_string(),
            format!(
                "This answer puts two nodes in different states. Slot {} is chosen, but slot \
                 {} is not, and the state machine must execute the commands in log order. If \
                 you apply {} now, you pass over {}, and you cannot undo that, because the \
                 application already took the effect of the later command. Hold the slot: it \
                 stays recorded as chosen, and the walk applies it when the hole below it \
                 closes.",
                slot.0, first_unchosen.0, slot.0, first_unchosen.0
            ),
        );
        explanations.insert(
            "hold".to_string(),
            format!(
                "A hold stops the log for no reason. Slot {} is the first slot that the \
                 prefix misses, and the prefix ends at {at}. An apply extends the contiguous \
                 prefix by one slot, and possibly by more. The slots above it that were \
                 already chosen become contiguous as well.",
                slot.0
            ),
        );
        Self {
            id,
            kind: PromptKind::ReplicaApply,
            node: node.0,
            question: format!("Slot {} is chosen here. Apply it now?", slot.0),
            state_summary: vec![
                format!("applied prefix ends at: {at}"),
                format!("first unchosen slot: {}", first_unchosen.0),
                format!("the newly chosen slot: {}", slot.0),
            ],
            choices: vec![
                Choice::new("apply", format!("Apply slot {}", slot.0)),
                Choice::new("hold", "Hold the slot, because the prefix has a hole"),
            ],
            expected: expected.to_string(),
            explanations,
            feedback: None,
        }
    }

    /// A client retried a write. Which of the three honest answers is this?
    ///
    /// Judged on a **clone of the replica**, through the two ledgers the core
    /// itself consults in that order:
    /// [`applied_at`](paros_core::replica::Replica::applied_at) says the
    /// command is inside this node's applied prefix, so the ack may name its
    /// slot; [`inflight_at`](paros_core::replica::Replica::inflight_at) says
    /// it is chosen or in flight at a slot but not executed here yet, so the
    /// client waits on **that** slot; neither says the node has never seen it,
    /// and it takes a fresh one.
    ///
    /// The two tables move together, and that is the whole lesson: ack from
    /// the wrong one and the client is told a write is durable that no node
    /// has applied, or — worse — the retry misses both and the command is
    /// executed twice.
    #[must_use]
    pub fn ack_write(
        id: u64,
        node: NodeId,
        client: u64,
        seq: u64,
        applied_at: Option<Slot>,
        inflight_at: Option<Slot>,
        chosen_index: Option<Slot>,
    ) -> Self {
        let applied =
            chosen_index.map_or_else(|| "nothing".to_string(), |s| format!("slot {}", s.0));
        let expected = match (applied_at, inflight_at) {
            (Some(_), _) => "acked",
            (None, Some(_)) => "inflight",
            (None, None) => "fresh",
        };
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "acked".to_string(),
            format!(
                "This answer is the classic early ack. The applied prefix of this node ends \
                 at {applied}, and no slot in it carries write #{seq} for client {client}. The \
                 ack tells the client that its write is durable and readable. The client then \
                 reads at this same node and does not find the write, because \"chosen\" is not \
                 \"applied\". No node executes a slot decided above a hole until the hole \
                 closes."
            ),
        );
        explanations.insert(
            "inflight".to_string(),
            match applied_at {
                Some(slot) => format!(
                    "This answer makes the client wait for a result that it already has. Write \
                     #{seq} is applied here, at slot {}. If a later duplicate of it sits chosen \
                     and unapplied above, that duplicate executes as a no-op, and the reply \
                     never goes out.",
                    slot.0
                ),
                None => format!(
                    "There is no slot to hold the reply on. This node has no record of write \
                     #{seq} in either table: it is not applied, and it is not in flight."
                ),
            },
        );
        explanations.insert(
            "fresh".to_string(),
            match (applied_at, inflight_at) {
                (Some(slot), _) => format!(
                    "This answer executes the command of the client a *second* time. Write \
                     #{seq} is applied here, at slot {}. At-most-once execution exists to stop \
                     that result, and a second execution is worse than an early ack.",
                    slot.0
                ),
                (None, Some(slot)) => format!(
                    "This answer puts the same command in the log twice. Write #{seq} is chosen \
                     at slot {}, or it is still in flight there, and this node has not executed \
                     it yet. The two dedup tables must move together for that reason. If the \
                     command left the in-flight table before the applied table received it, a \
                     retry in that window would miss both tables.",
                    slot.0
                ),
                (None, None) => String::new(),
            },
        );
        let mut choices = vec![
            Choice::new("acked", "Ack the write, because this node applied it"),
            Choice::new(
                "inflight",
                "Hold the reply on the slot that it is in flight at",
            ),
            Choice::new("fresh", "Give the write the next free slot"),
        ];
        choices.retain(|choice| !choice.id.is_empty());
        Self {
            id,
            kind: PromptKind::AckWrite,
            node: node.0,
            question: format!(
                "Client {client} asks again for its write #{seq}. What do you answer?"
            ),
            state_summary: vec![
                format!("the applied prefix ends at: {applied}"),
                format!(
                    "the applied table says: {}",
                    applied_at.map_or_else(
                        || "nothing for this write".to_string(),
                        |s| format!("applied at slot {}", s.0)
                    )
                ),
                format!(
                    "the in-flight table says: {}",
                    inflight_at.map_or_else(
                        || "nothing for this write".to_string(),
                        |s| format!("in flight at slot {}", s.0)
                    )
                ),
            ],
            choices,
            expected: expected.to_string(),
            explanations,
            feedback: None,
        }
    }
}

impl Prompt {}
