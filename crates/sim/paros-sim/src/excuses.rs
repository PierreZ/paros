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
    /// A boot misses a persisted record that no corruption crash named: the
    /// node's corruption crash since its last boot explains it (#912
    /// (excuses too wide)).
    LossCorruptionCrash,
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
    /// A control journal's history: an attempt logged, or answered, after
    /// client 0 searched it, so the search judged it unknown or not at all
    /// (#912 (excuses too wide)).
    HistoryAfterSearch,
    /// The chain oracle: an unavailable run that storage faults explain.
    ChainStorageFaults,
    /// The heal oracle: the cell cannot hold the journal's shape
    /// (`truth::excuse`, from the sim's ground truth, #739).
    HealCannotHold,
    /// The heal oracle: a maintenance window covers a member.
    HealMaintenance,
    /// The heal and placement oracles: a `batched` journal lost its last
    /// batch on a configuration that cannot repair it (#599).
    HealBatchLost,
    /// The heal and placement oracles: a journal's move has no machine with
    /// disk room to go to (#985 (placement counts store disk)).
    HealNoRoom,
    /// The heal oracle: the tenant's matchmaker set is lost (`set_lost`).
    HealSetLost,
    /// The convergence oracle: the tenant's matchmaker set is lost.
    ConvergeSetLost,
    /// Skip: the heal oracle does not judge a journal with a gone member
    /// when the coordinators wait past the heal window before they
    /// re-place it (#909 (oracles that do not judge)).
    SkipHealReplacePastTail,
    /// Skip: the heal oracle accepts every unhealthy journal of a lost cell,
    /// and a cell that answers no view.
    SkipHealCellLost,
    /// Skip: `worlds_converge` returns when no cell formed.
    SkipConvergeNoCell,
    /// Skip: `worlds_converge` returns when the cell is lost.
    SkipConvergeCellLost,
    /// Skip: `final_check` runs none of the cell's tail checks (election,
    /// placements, sets, controls, heal, leads, live machines, resolve)
    /// when the cell is lost (#909 (oracles that do not judge)).
    SkipTailCellLost,
    /// Skip: `final_fleet` checks neither the directory nor the tenants
    /// when the cell is lost.
    SkipFleetCellLost,
    /// Skip: an operation an operator stopped in need not end when the
    /// cell is lost.
    SkipOperationCellLost,
    /// Skip: a tail check whose wait ran out passes because the cell was
    /// lost after the tail checks started.
    SkipLateCellLost,
    /// Skip: the follow oracle of a moved tenant ends with no verdict,
    /// because a machine folded the tombstone of the journal it follows
    /// (#1003 (uncounted skips from fix-main)).
    SkipFollowDeleted,
    /// Skip: a boot whose every open ended on a transient I/O or sync fault
    /// is not judged against the damage the injector planned for it
    /// (`injector::gave_verdict`, #1003 (uncounted skips from fix-main)).
    SkipInjectionNoVerdict,
    /// The Promise-content oracle: a reported record lies below the pruned
    /// tally (#458 (core oracles)).
    PromiseTallyPruned,
    /// The Promise-content oracle: the batch that carried the Promise
    /// learned a chosen record after the page was built.
    PromiseLateLearn,
    /// The Accept-record oracle: a higher promise kept the leader from
    /// recording its round.
    AcceptPromiseMoved,
    /// The Accept-record oracle: the round's slot lies below the pruned
    /// tally.
    AcceptTallyPruned,
    /// Skip: an admission at a position below what a resume at a
    /// checkpoint forgot is not judged.
    SkipAdmissionBelowResume,
}

impl Excuse {
    /// Every excuse, in the order of the report's table.
    pub const ALL: [Excuse; 34] = [
        Excuse::Unrecoverable,
        Excuse::Stranded,
        Excuse::BlockedInAsked,
        Excuse::FrozenByFaulty,
        Excuse::WipesNoQuorum,
        Excuse::LossCorruptionCrash,
        Excuse::NodeStorageDead,
        Excuse::NodeWiped,
        Excuse::NodeRetired,
        Excuse::NodeLeftPool,
        Excuse::ChainEndedBySibling,
        Excuse::ChainStorageFaults,
        Excuse::HistoryAfterSearch,
        Excuse::HealCannotHold,
        Excuse::HealMaintenance,
        Excuse::HealBatchLost,
        Excuse::HealNoRoom,
        Excuse::HealSetLost,
        Excuse::ConvergeSetLost,
        Excuse::SkipHealReplacePastTail,
        Excuse::SkipHealCellLost,
        Excuse::SkipConvergeNoCell,
        Excuse::SkipConvergeCellLost,
        Excuse::SkipTailCellLost,
        Excuse::SkipFleetCellLost,
        Excuse::SkipOperationCellLost,
        Excuse::SkipLateCellLost,
        Excuse::SkipFollowDeleted,
        Excuse::SkipInjectionNoVerdict,
        Excuse::PromiseTallyPruned,
        Excuse::PromiseLateLearn,
        Excuse::AcceptPromiseMoved,
        Excuse::AcceptTallyPruned,
        Excuse::SkipAdmissionBelowResume,
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
            Excuse::LossCorruptionCrash => "loss: corruption crash",
            Excuse::NodeStorageDead => "node: storage dead",
            Excuse::NodeWiped => "node: wiped",
            Excuse::NodeRetired => "node: retired",
            Excuse::NodeLeftPool => "node: left pool",
            Excuse::ChainEndedBySibling => "chain: ended by sibling",
            Excuse::ChainStorageFaults => "chain: storage faults explain",
            Excuse::HistoryAfterSearch => "history: call after search",
            Excuse::HealCannotHold => "heal: cell cannot hold shape",
            Excuse::HealMaintenance => "heal: maintenance window",
            Excuse::HealBatchLost => "heal: batch lost for good",
            Excuse::HealNoRoom => "heal: no machine with disk room",
            Excuse::HealSetLost => "heal: matchmaker set lost",
            Excuse::ConvergeSetLost => "converge: matchmaker set lost",
            Excuse::SkipHealReplacePastTail => "skip heal: re-place past tail",
            Excuse::SkipHealCellLost => "skip heal: cell lost",
            Excuse::SkipConvergeNoCell => "skip converge: no cell",
            Excuse::SkipConvergeCellLost => "skip converge: cell lost",
            Excuse::SkipTailCellLost => "skip tail: cell lost",
            Excuse::SkipFleetCellLost => "skip fleet: cell lost",
            Excuse::SkipOperationCellLost => "skip operation: cell lost",
            Excuse::SkipLateCellLost => "skip late: cell lost",
            Excuse::SkipFollowDeleted => "skip follow: journal deleted",
            Excuse::SkipInjectionNoVerdict => "skip injection: no verdict",
            Excuse::PromiseTallyPruned => "promise: tally pruned",
            Excuse::PromiseLateLearn => "promise: record learned later",
            Excuse::AcceptPromiseMoved => "accept: promise moved past",
            Excuse::AcceptTallyPruned => "accept: tally pruned",
            Excuse::SkipAdmissionBelowResume => "skip admission: below resume",
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
            Excuse::LossCorruptionCrash => "excuse runs: loss: corruption crash",
            Excuse::NodeStorageDead => "excuse runs: node: storage dead",
            Excuse::NodeWiped => "excuse runs: node: wiped",
            Excuse::NodeRetired => "excuse runs: node: retired",
            Excuse::NodeLeftPool => "excuse runs: node: left pool",
            Excuse::ChainEndedBySibling => "excuse runs: chain: ended by sibling",
            Excuse::ChainStorageFaults => "excuse runs: chain: storage faults explain",
            Excuse::HistoryAfterSearch => "excuse runs: history: call after search",
            Excuse::HealCannotHold => "excuse runs: heal: cell cannot hold shape",
            Excuse::HealMaintenance => "excuse runs: heal: maintenance window",
            Excuse::HealBatchLost => "excuse runs: heal: batch lost for good",
            Excuse::HealNoRoom => "excuse runs: heal: no machine with disk room",
            Excuse::HealSetLost => "excuse runs: heal: matchmaker set lost",
            Excuse::ConvergeSetLost => "excuse runs: converge: matchmaker set lost",
            Excuse::SkipHealReplacePastTail => "excuse runs: skip heal: re-place past tail",
            Excuse::SkipHealCellLost => "excuse runs: skip heal: cell lost",
            Excuse::SkipConvergeNoCell => "excuse runs: skip converge: no cell",
            Excuse::SkipConvergeCellLost => "excuse runs: skip converge: cell lost",
            Excuse::SkipTailCellLost => "excuse runs: skip tail: cell lost",
            Excuse::SkipFleetCellLost => "excuse runs: skip fleet: cell lost",
            Excuse::SkipOperationCellLost => "excuse runs: skip operation: cell lost",
            Excuse::SkipLateCellLost => "excuse runs: skip late: cell lost",
            Excuse::SkipFollowDeleted => "excuse runs: skip follow: journal deleted",
            Excuse::SkipInjectionNoVerdict => "excuse runs: skip injection: no verdict",
            Excuse::PromiseTallyPruned => "excuse runs: promise: tally pruned",
            Excuse::PromiseLateLearn => "excuse runs: promise: record learned later",
            Excuse::AcceptPromiseMoved => "excuse runs: accept: promise moved past",
            Excuse::AcceptTallyPruned => "excuse runs: accept: tally pruned",
            Excuse::SkipAdmissionBelowResume => "excuse runs: skip admission: below resume",
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
            Excuse::LossCorruptionCrash => "excuse journals: loss: corruption crash",
            Excuse::NodeStorageDead => "excuse journals: node: storage dead",
            Excuse::NodeWiped => "excuse journals: node: wiped",
            Excuse::NodeRetired => "excuse journals: node: retired",
            Excuse::NodeLeftPool => "excuse journals: node: left pool",
            Excuse::ChainEndedBySibling => "excuse journals: chain: ended by sibling",
            Excuse::ChainStorageFaults => "excuse journals: chain: storage faults explain",
            Excuse::HistoryAfterSearch => "excuse journals: history: call after search",
            Excuse::HealCannotHold => "excuse journals: heal: cell cannot hold shape",
            Excuse::HealMaintenance => "excuse journals: heal: maintenance window",
            Excuse::HealBatchLost => "excuse journals: heal: batch lost for good",
            Excuse::HealNoRoom => "excuse journals: heal: no machine with disk room",
            Excuse::HealSetLost => "excuse journals: heal: matchmaker set lost",
            Excuse::ConvergeSetLost => "excuse journals: converge: matchmaker set lost",
            Excuse::SkipHealReplacePastTail => "excuse journals: skip heal: re-place past tail",
            Excuse::SkipFollowDeleted => "excuse journals: skip follow: journal deleted",
            Excuse::SkipInjectionNoVerdict => "excuse journals: skip injection: no verdict",
            Excuse::PromiseTallyPruned => "excuse journals: promise: tally pruned",
            Excuse::PromiseLateLearn => "excuse journals: promise: record learned later",
            Excuse::AcceptPromiseMoved => "excuse journals: accept: promise moved past",
            Excuse::AcceptTallyPruned => "excuse journals: accept: tally pruned",
            Excuse::SkipHealCellLost
            | Excuse::SkipTailCellLost
            | Excuse::SkipFleetCellLost
            | Excuse::SkipOperationCellLost
            | Excuse::SkipLateCellLost
            | Excuse::SkipAdmissionBelowResume
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

/// Whether the cell is lost (`crate::machine::cell_lost`), noting `excuse`
/// when it is (#909 (oracles that do not judge)): every check that a lost
/// cell switches off asks through here, so each skip site is counted.
pub(crate) fn cell_lost(state: &StateHandle, excuse: Excuse) -> bool {
    let lost = crate::machine::cell_lost(state);
    if lost {
        note(state, excuse, None);
    }
    lost
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
