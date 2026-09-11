//! The acceptor's own questions: the two vote rules, and the record a `Commit`
//! contradicts.
//!
//! **One rule governs the two votes**: refuse anything below the promise held,
//! admit anything at or above it. Each answer is computed on a clone of
//! [`paros_core::acceptor::Acceptor`].

use std::collections::BTreeMap;

use paros_core::acceptor::{AcceptOutcome, PrepareOutcome};
use paros_core::{Ballot, Command, NodeId, Slot};

use crate::view::{show_ballot, show_command};

use super::{Choice, Prompt, PromptKind};

impl Prompt {
    /// `Prepare(ballot)` arrived at an acceptor whose promise is `promised`.
    ///
    /// Judged by [`paros_core::acceptor::Acceptor::prepare`] on a clone: a
    /// promise is raised (or re-affirmed) for any ballot **not below** the one
    /// held, and refused below it — or refused, without touching the promise,
    /// when the range starts below the compaction floor.
    #[must_use]
    pub fn acceptor_prepare(
        id: u64,
        node: NodeId,
        ballot: Ballot,
        from_slot: Slot,
        promised: Ballot,
        floor: Slot,
        outcome: PrepareOutcome,
    ) -> Self {
        let b = show_ballot(ballot);
        let p = show_ballot(promised);
        let expected = match outcome {
            PrepareOutcome::Promised { .. } => "promise",
            PrepareOutcome::Refused | PrepareOutcome::BelowFloor => "nack",
        };
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "promise".to_string(),
            match outcome {
                PrepareOutcome::BelowFloor => format!(
                    "A promise here lets the candidate propose a new value into slots that \
                     are already chosen. You would promise ballot {b} for the slots from {}, \
                     but this acceptor truncated everything below slot {}. A Promise reports \
                     the values that it accepted in the range that it covers, and those values \
                     are gone. The candidate would read the silence as \"nothing was ever \
                     accepted here\". Refuse the Prepare: a candidate this far behind must \
                     recover the compacted prefix another way.",
                    from_slot.0, floor.0
                ),
                _ => format!(
                    "A promise here lets one slot get two values. Ballot {b} is *below* the \
                     promise {p} that this acceptor holds. A promise is the only fence in \
                     Paxos. After this acceptor promised {p}, its report to the proposer of \
                     {p} covered every lower ballot for the last time. An answer to {b} now \
                     cancels that report, and ballot {b} can collect a majority and choose a \
                     second value for the same slot. One rule answers both questions: refuse \
                     every ballot below the promise that you hold."
                ),
            },
        );
        explanations.insert(
            "nack".to_string(),
            format!(
                "A refusal here costs the cluster an election that it could win. Ballot {b} \
                 is not below the promise {p} that this acceptor holds, so the refusal is not \
                 unsafe. It is a liveness fault. An *equal* ballot is not a special case. Only \
                 one proposer mints a ballot, so ballot {b} twice is that one proposer that \
                 asks again, and the answer does not change. The acceptor must promise, raise \
                 its durable promise to {b} *before* the reply goes out, and report every \
                 value that it accepted from slot {} up.",
                from_slot.0
            ),
        );
        Self {
            id,
            kind: PromptKind::AcceptorPrepare,
            node: node.0,
            question: format!("A Prepare at ballot {b} arrived. Promise, or Nack?"),
            state_summary: vec![
                format!("promised ballot: {p}"),
                format!("the Prepare's ballot: {b}"),
                format!("it covers the slots from {}", from_slot.0),
                format!("compaction floor: slot {}", floor.0),
            ],
            choices: vec![
                Choice::new("promise", format!("Promise {b}")),
                Choice::new("nack", format!("Nack, because I promised {p}")),
            ],
            expected: expected.to_string(),
            explanations,
            feedback: None,
        }
    }

    /// `Accept(ballot, slot)` arrived at an acceptor whose promise is
    /// `promised`.
    ///
    /// Judged by [`paros_core::acceptor::Acceptor::admit`], which needs no
    /// clone at all — it takes `&self`. The rule is the same one the `Prepare`
    /// side uses: anything **below** the promise is refused, and a ballot at
    /// or above it is admitted. An equal ballot is the proposer that already
    /// holds this acceptor's promise coming back for its vote.
    #[must_use]
    pub fn acceptor_accept(
        id: u64,
        node: NodeId,
        ballot: Ballot,
        slot: Slot,
        command: &Command,
        promised: Ballot,
        outcome: AcceptOutcome,
    ) -> Self {
        let b = show_ballot(ballot);
        let p = show_ballot(promised);
        let v = show_command(command);
        let expected = match outcome {
            AcceptOutcome::Admitted => "accept",
            AcceptOutcome::Refused | AcceptOutcome::BelowFloor => "nack",
        };
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "accept".to_string(),
            format!(
                "A vote here moves a second value one step nearer to a majority at slot {}. \
                 You would vote for {v} at ballot {b} while this acceptor holds a promise at \
                 {p}. The proposer of {p} used that promise as its fence when it ran its \
                 value-selection rule. It learned that this acceptor held nothing newer, and it \
                 possibly chose a value from that report. Refuse the Accept, and tell {b} which \
                 ballot refused it.",
                slot.0
            ),
        );
        explanations.insert(
            "nack".to_string(),
            format!(
                "A refusal here is a liveness fault. Ballot {b} is not below the promise {p}, \
                 so this vote is safe. A proposer that ran Phase 1 at {b} and got the promise \
                 of this acceptor may have its vote in Phase 2 at the same ballot. Only one \
                 proposer mints a ballot, so an equal ballot is that same proposer again. \
                 Accept the value, and look at the two writes and their order. The acceptor \
                 writes the promise at {b} first, and the record for slot {} second. The record \
                 must not reach the disk above the promise that covers it.",
                slot.0
            ),
        );
        Self {
            id,
            kind: PromptKind::AcceptorAccept,
            node: node.0,
            question: format!(
                "An Accept at ballot {b} for slot {} arrived, and it carries {v}. Accept, or \
                 Nack?",
                slot.0
            ),
            state_summary: vec![
                format!("promised ballot: {p}"),
                format!("the Accept's ballot: {b}"),
                format!("slot: {}", slot.0),
                format!("value: {v}"),
            ],
            choices: vec![
                Choice::new("accept", format!("Accept {v} at {b}")),
                Choice::new("nack", format!("Nack, because I promised {p}")),
            ],
            expected: expected.to_string(),
            explanations,
            feedback: None,
        }
    }

    /// The cluster says `slot` holds `chosen` at `ballot` — through a `Commit`
    /// or a catch-up replay — while this acceptor's record says `held` at a
    /// lower ballot.
    ///
    /// **The answer is a constant, and deliberately so.** There is no clone to
    /// ask: the core has no "keep the old record" state to be in.
    /// [`paros_core::acceptor::Acceptor::record_accepted`] is an upsert by
    /// slot, and the prompt is only ever raised when what arrived was decided
    /// at a *strictly higher* ballot than the record held, so the choosing
    /// ballot always wins and the answer is always `take`. What the prompt
    /// teaches is the consequence of the other answer, which is why the
    /// `keep` branch carries the whole stale-accept-resurrection story and
    /// `take` carries none.
    #[must_use]
    pub fn commit_overwrite(
        id: u64,
        node: NodeId,
        slot: Slot,
        ballot: Ballot,
        chosen: &Command,
        held_at: Ballot,
        held: &Command,
    ) -> Self {
        let b = show_ballot(ballot);
        let hb = show_ballot(held_at);
        let cv = show_command(chosen);
        let hv = show_command(held);
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "keep".to_string(),
            format!(
                "This answer keeps a record that the cluster already contradicted. {cv} is \
                 *chosen* for slot {}, a quorum decided it at ballot {b}, and {hb} is below \
                 that ballot. If the stale record stays on disk, a restart reads it back as the \
                 accepted value of this acceptor. The next promise quorum can then report {hv} \
                 as the highest accepted value, and a new leader re-proposes it over the chosen \
                 {cv}. That fault is the stale-accept resurrection, and the overwrite makes a \
                 restart safe.",
                slot.0
            ),
        );
        Self {
            id,
            kind: PromptKind::CommitOverwrite,
            node: node.0,
            question: format!(
                "The cluster says that slot {} holds {cv}, decided at {b}. Your record says \
                 {hv} at {hb}. Which record stays on disk?",
                slot.0
            ),
            state_summary: vec![
                format!("slot: {}", slot.0),
                format!("my own record: {hv} at ballot {hb}"),
                format!("the message that arrived: {cv} chosen at ballot {b}"),
            ],
            choices: vec![
                Choice::new("take", format!("Overwrite with {cv} at {b}")),
                Choice::new("keep", format!("Keep {hv} at {hb}")),
            ],
            expected: "take".to_string(),
            explanations,
            feedback: None,
        }
    }
}
