//! The excuse tally (#738 (count excuses, the baseline)): how often each
//! oracle excuse and each silent skip switches a check off.
//!
//! `assert_reachable!` only proves that an excuse fired once. This module
//! counts, per run, every excuse by name and every journal it excused, so a
//! sweep report shows which excuse hides the most. The counts live in
//! moonpool's assertion slots, because those are shared memory: the
//! sweep's forked workers and explored timelines add to the same counts.
//! Each count is an `assert_always_or_unreachable!(true, ..)`: it never
//! fails and is never a coverage gate, so a count that stays at zero
//! fails nothing. A run notes each excuse once, and each `(excuse,
//! journal)` pair once ([`note`]); the run itself is counted once by
//! [`note_run`], the denominator of the table.
//!
//! The runner reads the slots back by their messages ([`Excuse::ALL`],
//! [`Excuse::runs_slot`], [`Excuse::journals_slot`], [`RUNS_SLOT`]).
//! An explored timeline is a run of its own; a note made before its fork
//! point is counted on the parent only.

use std::collections::BTreeSet;
use std::sync::PoisonError;

use moonpool_sim::{StateHandle, assert_always_or_unreachable};
use paros::core::journal::JournalIdentifier;

/// One oracle excuse or silent skip.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Excuse {
    /// A slot no leader can decide again (`Losses::has_unrecoverable`).
    Unrecoverable,
    /// A lost slot whose every clean copy is out of reach (`stranded`).
    Stranded,
    /// A lost slot an asked configuration cannot recover (`blocked_in_asked`).
    BlockedInAsked,
    /// A slot faulty copies freeze (`frozen_by_faulty`).
    FrozenByFaulty,
    /// Wipes left no configuration a Phase-1 quorum (`wipes_excuse_liveness`).
    WipesNoQuorum,
    /// A node down for good from a storage fault (`excused_from_convergence`).
    NodeStorageDead,
    /// A wiped node (`excused_from_convergence`).
    NodeWiped,
    /// A retired node (`excused_from_convergence`).
    NodeRetired,
    /// A node that stopped the journal (`excused_from_convergence`).
    NodeLeftPool,
    /// The chain oracle: a sibling client ended the run first.
    ChainEndedBySibling,
    /// The chain oracle: an unavailable run that storage faults explain.
    ChainStorageFaults,
    /// The heal oracle: the cell cannot hold the journal's shape
    /// (`truth::excuse`, from the sim's ground truth, #739).
    HealCannotHold,
    /// The heal oracle: a maintenance window covers a member.
    HealMaintenance,
    /// The heal oracle: the tenant's matchmaker set is lost (`set_lost`).
    HealSetLost,
    /// The convergence oracle: the tenant's matchmaker set is lost.
    ConvergeSetLost,
    /// Skip: `journals_heal` returns when the coordinators wait past the tail.
    SkipHealReplacePastTail,
    /// Skip: the heal oracle accepts every unhealthy journal of a lost cell.
    SkipHealCellLost,
    /// Skip: `worlds_converge` returns when no cell formed.
    SkipConvergeNoCell,
    /// Skip: `worlds_converge` returns when the cell is lost.
    SkipConvergeCellLost,
}

impl Excuse {
    /// Every excuse, in the order of the report's table.
    pub const ALL: [Excuse; 19] = [
        Excuse::Unrecoverable,
        Excuse::Stranded,
        Excuse::BlockedInAsked,
        Excuse::FrozenByFaulty,
        Excuse::WipesNoQuorum,
        Excuse::NodeStorageDead,
        Excuse::NodeWiped,
        Excuse::NodeRetired,
        Excuse::NodeLeftPool,
        Excuse::ChainEndedBySibling,
        Excuse::ChainStorageFaults,
        Excuse::HealCannotHold,
        Excuse::HealMaintenance,
        Excuse::HealSetLost,
        Excuse::ConvergeSetLost,
        Excuse::SkipHealReplacePastTail,
        Excuse::SkipHealCellLost,
        Excuse::SkipConvergeNoCell,
        Excuse::SkipConvergeCellLost,
    ];

    /// The excuse's name in the report.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Excuse::Unrecoverable => "loss: unrecoverable slot",
            Excuse::Stranded => "loss: stranded slot",
            Excuse::BlockedInAsked => "loss: blocked in asked",
            Excuse::FrozenByFaulty => "loss: frozen by faulty",
            Excuse::WipesNoQuorum => "wipes: no Phase-1 quorum",
            Excuse::NodeStorageDead => "node: storage dead",
            Excuse::NodeWiped => "node: wiped",
            Excuse::NodeRetired => "node: retired",
            Excuse::NodeLeftPool => "node: left pool",
            Excuse::ChainEndedBySibling => "chain: ended by sibling",
            Excuse::ChainStorageFaults => "chain: storage faults explain",
            Excuse::HealCannotHold => "heal: cell cannot hold shape",
            Excuse::HealMaintenance => "heal: maintenance window",
            Excuse::HealSetLost => "heal: matchmaker set lost",
            Excuse::ConvergeSetLost => "converge: matchmaker set lost",
            Excuse::SkipHealReplacePastTail => "skip heal: re-place past tail",
            Excuse::SkipHealCellLost => "skip heal: cell lost",
            Excuse::SkipConvergeNoCell => "skip converge: no cell",
            Excuse::SkipConvergeCellLost => "skip converge: cell lost",
        }
    }

    /// The slot that counts the runs where the excuse fired.
    #[must_use]
    pub fn runs_slot(self) -> &'static str {
        match self {
            Excuse::Unrecoverable => "excuse runs: loss: unrecoverable slot",
            Excuse::Stranded => "excuse runs: loss: stranded slot",
            Excuse::BlockedInAsked => "excuse runs: loss: blocked in asked",
            Excuse::FrozenByFaulty => "excuse runs: loss: frozen by faulty",
            Excuse::WipesNoQuorum => "excuse runs: wipes: no Phase-1 quorum",
            Excuse::NodeStorageDead => "excuse runs: node: storage dead",
            Excuse::NodeWiped => "excuse runs: node: wiped",
            Excuse::NodeRetired => "excuse runs: node: retired",
            Excuse::NodeLeftPool => "excuse runs: node: left pool",
            Excuse::ChainEndedBySibling => "excuse runs: chain: ended by sibling",
            Excuse::ChainStorageFaults => "excuse runs: chain: storage faults explain",
            Excuse::HealCannotHold => "excuse runs: heal: cell cannot hold shape",
            Excuse::HealMaintenance => "excuse runs: heal: maintenance window",
            Excuse::HealSetLost => "excuse runs: heal: matchmaker set lost",
            Excuse::ConvergeSetLost => "excuse runs: converge: matchmaker set lost",
            Excuse::SkipHealReplacePastTail => "excuse runs: skip heal: re-place past tail",
            Excuse::SkipHealCellLost => "excuse runs: skip heal: cell lost",
            Excuse::SkipConvergeNoCell => "excuse runs: skip converge: no cell",
            Excuse::SkipConvergeCellLost => "excuse runs: skip converge: cell lost",
        }
    }

    /// The slot that counts the journals the excuse excused, one per run
    /// and journal. A skip that judges no journal has none.
    #[must_use]
    pub fn journals_slot(self) -> Option<&'static str> {
        Some(match self {
            Excuse::Unrecoverable => "excuse journals: loss: unrecoverable slot",
            Excuse::Stranded => "excuse journals: loss: stranded slot",
            Excuse::BlockedInAsked => "excuse journals: loss: blocked in asked",
            Excuse::FrozenByFaulty => "excuse journals: loss: frozen by faulty",
            Excuse::WipesNoQuorum => "excuse journals: wipes: no Phase-1 quorum",
            Excuse::NodeStorageDead => "excuse journals: node: storage dead",
            Excuse::NodeWiped => "excuse journals: node: wiped",
            Excuse::NodeRetired => "excuse journals: node: retired",
            Excuse::NodeLeftPool => "excuse journals: node: left pool",
            Excuse::ChainEndedBySibling => "excuse journals: chain: ended by sibling",
            Excuse::ChainStorageFaults => "excuse journals: chain: storage faults explain",
            Excuse::HealCannotHold => "excuse journals: heal: cell cannot hold shape",
            Excuse::HealMaintenance => "excuse journals: heal: maintenance window",
            Excuse::HealSetLost => "excuse journals: heal: matchmaker set lost",
            Excuse::ConvergeSetLost => "excuse journals: converge: matchmaker set lost",
            Excuse::SkipHealReplacePastTail
            | Excuse::SkipHealCellLost
            | Excuse::SkipConvergeNoCell
            | Excuse::SkipConvergeCellLost => return None,
        })
    }
}

/// The slot that counts the runs judged: the table's denominator.
pub const RUNS_SLOT: &str = "excuse runs: a run is judged";

const EXCUSE_BOARD_KEY: &str = "paros-sim-excuse-board";

/// What this run noted so far: each excuse and each `(excuse, journal)`
/// pair is counted once per run.
#[derive(Default)]
struct ExcuseBoard {
    runs: BTreeSet<Excuse>,
    journals: BTreeSet<(Excuse, JournalIdentifier)>,
}

/// `excuse` switched a check off in this run, on `journal` when it judges
/// one.
pub(crate) fn note(state: &StateHandle, excuse: Excuse, journal: Option<JournalIdentifier>) {
    let board = crate::state::published(state, EXCUSE_BOARD_KEY, ExcuseBoard::default);
    let mut board = board.lock().unwrap_or_else(PoisonError::into_inner);
    if board.runs.insert(excuse) {
        assert_always_or_unreachable!(true, excuse.runs_slot());
    }
    if let (Some(journal), Some(slot)) = (journal, excuse.journals_slot())
        && board.journals.insert((excuse, journal))
    {
        assert_always_or_unreachable!(true, slot);
    }
}

/// [`note`] for every excuse of `excuses`, on `journal`.
pub(crate) fn note_all(state: &StateHandle, excuses: &[Excuse], journal: JournalIdentifier) {
    for excuse in excuses {
        note(state, *excuse, Some(journal));
    }
}

/// This run is judged: counted once, by client 0's check.
pub(crate) fn note_run() {
    assert_always_or_unreachable!(true, RUNS_SLOT);
}

#[cfg(test)]
mod tests {
    use super::{Excuse, RUNS_SLOT};
    use std::collections::BTreeSet;

    #[test]
    fn every_slot_is_distinct_and_fits_its_name() {
        let mut seen = BTreeSet::new();
        assert!(seen.insert(RUNS_SLOT));
        for excuse in Excuse::ALL {
            for slot in [Some(excuse.runs_slot()), excuse.journals_slot()]
                .into_iter()
                .flatten()
            {
                // A slot is named by its first 63 bytes.
                assert!(slot.len() <= 63, "{slot:?} is too long");
                assert!(slot.ends_with(excuse.name()), "{slot:?}");
                assert!(seen.insert(slot), "{slot:?} twice");
            }
        }
    }
}
