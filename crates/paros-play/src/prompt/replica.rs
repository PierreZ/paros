//! The replica's questions: whether a slot that just became chosen applies
//! now, and what the journal answers a client's retry with.

use std::collections::BTreeMap;

use paros_core::{Entry, JournalState, NodeId, Outcome, Slot};

use super::{Choice, Prompt, PromptKind};
use crate::narration;

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
        let at = narration::at(chosen_index);
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

    /// A client retried a write. What does the journal answer it with?
    ///
    /// Judged on a **clone of the replica**, by the journal state machine
    /// its fold runs ([`JournalState::apply`], #204), over the state this
    /// node has folded and the write it holds at the retried position.
    /// There is no dedup table to consult: a retry is the same write sent
    /// again, and the log answers it. Position taken by exactly this write:
    /// a duplicate, acked, nothing moves. Position still ahead of the fold:
    /// nothing can be said yet — the retry takes a slot and waits for the
    /// fold to reach it, by which time the original has folded below it.
    /// Position free and next: the retry is the write that fills it.
    /// Anything else: refused, with the state that says why.
    #[must_use]
    pub fn ack_write(
        id: u64,
        node: NodeId,
        entry: &Entry,
        seq: u64,
        outcome: &Outcome,
        folded: JournalState,
        chosen_index: Option<Slot>,
    ) -> Self {
        let applied = narration::at(chosen_index);
        let position = entry.seq.0;
        let next = folded.next_seq.0;
        let expected = match outcome {
            Outcome::Duplicate { .. } => "acked",
            Outcome::Accepted { .. } => "fresh",
            _ if position > next => "inflight",
            _ => "refused",
        };
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "acked".to_string(),
            if position >= next {
                format!(
                    "This answer is an early ack. The fold of this node stops before position \
                     {position} (its next position is {next}), so nothing in the applied \
                     journal carries write #{seq} yet. An ack now would promise the client a \
                     write that a read at this node cannot find: \"chosen\" is not \"applied\"."
                )
            } else {
                format!(
                    "Position {position} is folded, but not with this write: another write holds \
                     it, or this one was sent under a superseded generation. Acking it would \
                     tell the client its records are where someone else's are."
                )
            },
        );
        explanations.insert(
            "inflight".to_string(),
            if position < next {
                format!(
                    "The fold has already reached position {position}: the journal can answer \
                     now, and it does not need the client to wait."
                )
            } else {
                format!(
                    "Position {position} is the journal's next position, so the fold can \
                     answer already: nothing below it is missing."
                )
            },
        );
        explanations.insert(
            "fresh".to_string(),
            format!(
                "A write is accepted only at the journal's next position, by its current \
                 writer, and the fold's next position is {next}, not {position}. This retry \
                 cannot be the write that fills a position now."
            ),
        );
        explanations.insert(
            "refused".to_string(),
            format!(
                "The journal does not refuse this retry: position {position} holds exactly \
                 this write, or is still the writer's to fill."
            ),
        );
        let choices = vec![
            Choice::new(
                "acked",
                "Ack it: the journal already holds this write at its position",
            ),
            Choice::new(
                "inflight",
                "Not yet: the fold has not reached its position, so the retry waits for its slot",
            ),
            Choice::new(
                "fresh",
                "Accept it: its position is the next one, and it fills it",
            ),
            Choice::new(
                "refused",
                "Refuse it: another write holds its position, or its writer was superseded",
            ),
        ];
        Self {
            id,
            kind: PromptKind::AckWrite,
            node: node.0,
            question: format!(
                "Client {} asks again for its write #{seq}, at position {position}. What does \
                 the journal answer, as far as this node has folded it?",
                crate::world::ClientId::of(entry.leader).map_or(0, |client| client.0)
            ),
            state_summary: vec![
                format!("the applied prefix ends at: {applied}"),
                format!("the journal's next position: {next}"),
                format!("the journal's first position: {}", folded.first_seq.0),
                format!("the journal's writer: {}", writer_text(&folded)),
            ],
            choices,
            expected: expected.to_string(),
            explanations,
            feedback: None,
        }
    }
}

impl Prompt {}

/// The journal's writer as a prompt names it: the level's client that leads
/// it, or nobody.
fn writer_text(folded: &paros_core::JournalState) -> String {
    folded
        .leader
        .and_then(crate::world::ClientId::of)
        .map_or_else(
            || "nobody".to_string(),
            |client| format!("client {}", client.0),
        )
}
