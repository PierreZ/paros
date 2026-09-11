//! The proposer's questions: which value a fresh ballot may carry, what a
//! recovery does with one slot, and which column of a grid takes a slot.

use std::collections::BTreeMap;

use paros_core::proposer::RecoveryStep;
use paros_core::{Ballot, Command, NodeId, Slot};

use crate::view::{show_ballot, show_command};

use super::{Choice, Prompt, PromptKind};

impl Prompt {
    /// Phase 1 just completed at `ballot`. Which value goes in the `Accept`?
    ///
    /// Judged by [`paros_core::proposer::Proposer::close_phase1`] on a clone:
    /// the highest-ballot value any promise reported wins, and the proposer's
    /// own value is set aside. This is P2c.
    #[must_use]
    pub fn proposer_value(
        id: u64,
        node: NodeId,
        ballot: Ballot,
        own: &Command,
        reported: Option<&(Ballot, Command)>,
    ) -> Self {
        let b = show_ballot(ballot);
        let mine = show_command(own);
        let mut choices = vec![Choice::new("own", format!("Propose my own {mine}"))];
        let mut explanations = BTreeMap::new();
        let expected = if let Some((at, value)) = reported {
            let rv = show_command(value);
            let ra = show_ballot(*at);
            choices.push(Choice::new(
                "reported",
                format!("Propose {rv}, reported at {ra}"),
            ));
            explanations.insert(
                "own".to_string(),
                format!(
                    "This answer can put two chosen values in slot 0. A promise reported {rv} \
                     accepted at ballot {ra}. You cannot separate that report from \"{rv} is \
                     already chosen\", because a majority of accepts at {ra} shares an acceptor \
                     with your promise majority. If you propose {mine} and the cluster already \
                     chose {rv}, slot 0 holds two different chosen values, and Paxos guarantees \
                     that this cannot occur. Adopt the value reported at the highest ballot. \
                     Your own value waits for a later slot or a later ballot.",
                ),
            );
            explanations.insert("reported".to_string(), String::new());
            "reported"
        } else {
            explanations.insert("own".to_string(), String::new());
            "own"
        };
        let mut state_summary = vec![
            format!("won ballot: {b}"),
            format!("the value of my client: {mine}"),
        ];
        state_summary.push(match reported {
            Some((at, value)) => format!(
                "the highest report from the promise quorum: {} at ballot {}",
                show_command(value),
                show_ballot(*at)
            ),
            None => "the promise quorum reported no accepted value".to_string(),
        });
        Self {
            id,
            kind: PromptKind::ProposerValue,
            node: node.0,
            question: format!("Phase 1 at ballot {b} is complete. Which value goes in the Accept?"),
            state_summary,
            choices,
            expected: expected.to_string(),
            explanations,
            feedback: None,
        }
    }

    /// A fresh leadership is about to recover `slot`.
    ///
    /// Judged by [`paros_core::proposer::Proposer::recovery_next`] on a clone
    /// of the proposer: `Recovered` means re-propose, `Fill` means decide a
    /// `Noop`, and `Undescribed` — a recovery inherited through a cooperative
    /// handoff, which ran no Phase 1 — means skip.
    #[must_use]
    pub fn leader_recovery(
        id: u64,
        node: NodeId,
        ballot: Ballot,
        slot: Slot,
        step: &RecoveryStep<Command>,
    ) -> Self {
        let b = show_ballot(ballot);
        let expected = match step {
            RecoveryStep::Recovered(_) => "repropose",
            RecoveryStep::Fill => "fill_noop",
            RecoveryStep::Undescribed => "skip",
        };
        let reported = match step {
            RecoveryStep::Recovered(command) => format!(
                "the promise quorum reported {} for slot {}",
                show_command(command),
                slot.0
            ),
            RecoveryStep::Fill => {
                format!("the promise quorum reported nothing for slot {}", slot.0)
            }
            RecoveryStep::Undescribed => format!(
                "the handoff from the last leader did not describe slot {}",
                slot.0
            ),
        };
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "repropose".to_string(),
            format!(
                "There is no value to re-propose here. No acceptor reported anything for slot \
                 {}. The next command of your client would be a *new* proposal at a slot below \
                 the frontier that you give fresh commands. That frontier comes from the \
                 accepted log, so a restart passes over the slot again.",
                slot.0
            ),
        );
        explanations.insert(
            "fill_noop".to_string(),
            match step {
                RecoveryStep::Recovered(command) => format!(
                    "A Noop here decides a *different* value at a slot that an earlier ballot \
                     possibly chose, and that is the double-choose. A promise reported {} for \
                     slot {}. The value-selection rule applies to each slot, and this slot has \
                     a value.",
                    show_command(command),
                    slot.0
                ),
                _ => format!(
                    "A Noop here has no permission behind it. Slot {} came from a cooperative \
                     handoff, not from a Phase 1. A handoff runs no Prepare, so no quorum \
                     report supports the claim \"no node chose anything here\", and a Noop fill \
                     needs that report. Skip the slot: the successor re-proposes only the slots \
                     that the last leader described, and an ordinary election covers the rest.",
                    slot.0
                ),
            },
        );
        explanations.insert(
            "skip".to_string(),
            match step {
                RecoveryStep::Recovered(command) => format!(
                    "A skip loses {}. No node proposes slot {} again, and the contiguous \
                     chosen prefix of every node stops one slot below it, permanently. \
                     Re-propose the reported value under your own ballot.",
                    show_command(command),
                    slot.0
                ),
                _ => format!(
                    "A skip of slot {} makes the permanent gap. A new proposal always takes the \
                     frontier, and a restart computes the frontier from the accepted log, so no \
                     node proposes this slot again. The chosen prefix then stops one slot below \
                     it on every node, and the reads stop above it. Catch-up cannot help, \
                     because every node holds the same prefix. Your promise quorum reported \
                     nothing here, and quorum intersection makes that silence a permission. \
                     Some member of *every* majority reports a value that is already chosen. \
                     Fill the slot with a Noop.",
                    slot.0
                ),
            },
        );
        Self {
            id,
            kind: PromptKind::LeaderRecovery,
            node: node.0,
            question: format!("You won ballot {b}. What happens to slot {}?", slot.0),
            state_summary: vec![format!("won ballot: {b}"), reported],
            choices: vec![
                Choice::new("repropose", "Re-propose the reported value"),
                Choice::new("fill_noop", "Fill the slot with a Noop"),
                Choice::new("skip", "Do not touch the slot"),
            ],
            expected: expected.to_string(),
            explanations,
            feedback: None,
        }
    }

    /// A grid leader is about to propose into `slot`. Which column takes it?
    ///
    /// Judged by [`paros_core::AcceptorConfig::column_of`] on the
    /// configuration in force: the column is `slot % cols`, a pure function of
    /// the slot. `expected` is that answer, and `columns` is how many the grid
    /// has.
    ///
    /// Every wrong choice here is **safe** — every full column of a grid is a
    /// Phase-2 quorum, and every row meets every column — so the explanation
    /// is about who else has to reach the same answer, not about a value being
    /// lost.
    #[must_use]
    pub fn grid_column(id: u64, node: NodeId, slot: Slot, columns: usize, expected: usize) -> Self {
        let mut choices = Vec::new();
        let mut explanations = BTreeMap::new();
        for column in 0..columns {
            choices.push(Choice::new(
                &format!("column_{column}"),
                format!("Column {column}"),
            ));
            if column == expected {
                continue;
            }
            explanations.insert(
                format!("column_{column}"),
                format!(
                    "This answer breaks the agreement about *which* column takes the slot. \
                     Column {column} is a correct Phase-2 quorum, because every full column of \
                     this grid is one and every row meets every column. No message carries the \
                     column. This leader can stop, and another node then re-proposes slot {}. \
                     This leader can also restart and send its own Accept again. Each of them \
                     computes the column from the slot alone, and each gets column {expected}, \
                     because the rule is the slot {} modulo {columns} columns. Select the column \
                     that the rule gives, and every leader addresses the same acceptors.",
                    slot.0, slot.0
                ),
            );
        }
        Self {
            id,
            kind: PromptKind::GridColumn,
            node: node.0,
            question: format!("Slot {} goes to which column?", slot.0),
            state_summary: vec![
                format!("the slot in the proposal: {}", slot.0),
                format!("the columns in this grid: {columns}"),
                "a Phase-2 quorum here is one full column".to_string(),
            ],
            choices,
            expected: format!("column_{expected}"),
            explanations,
            feedback: None,
        }
    }
}
