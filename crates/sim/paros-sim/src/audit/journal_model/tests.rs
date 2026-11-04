//! The journal model's oracles on hand-built logs: each log is decided by
//! the real journal state machine (`JournalState::apply`), so an honest log
//! passes every check; a test then breaks one verdict and the check that
//! owns it must go red.

use std::collections::BTreeMap;

use crate::audit::testing::red;
use paros::core::Value;
use paros::core::journal::{
    Command, Control, Entry, JournalState, JournalView, LeaderUuid, Outcome, Seq, WriterMode,
    command_fingerprint,
};

use super::{AckedWrite, JournalModel, record_hash};

const A: LeaderUuid = LeaderUuid(7);
const B: LeaderUuid = LeaderUuid(9);

/// One journal, decided slot by slot by the real state machine and folded
/// into the model as one node reports it.
struct Log {
    mode: WriterMode,
    state: JournalState,
    accepted: BTreeMap<u64, Entry>,
    model: JournalModel,
    slot: u64,
}

impl Log {
    fn new(mode: WriterMode) -> Self {
        let mut model = JournalModel::default();
        model.settle(mode);
        Self {
            mode,
            state: JournalState::default(),
            accepted: BTreeMap::new(),
            model,
            slot: 0,
        }
    }

    /// Decide `command` at the next slot and fold its honest verdict.
    fn decide(&mut self, command: &Command) -> (u64, Outcome) {
        let accepted = &self.accepted;
        let outcome = self
            .state
            .apply(self.mode, command, |seq| accepted.get(&seq.0));
        if let (Outcome::Accepted { seq, .. }, Command::Write(entry)) = (&outcome, command) {
            self.accepted.insert(seq.0, entry.clone());
        }
        let slot = self.next_slot();
        self.fold(0, slot, command, &outcome);
        (slot, outcome)
    }

    /// Fold `outcome` at `slot`, as `node` reports it (honest or not).
    fn fold(&mut self, node: u64, slot: u64, command: &Command, outcome: &Outcome) {
        let reported = (*outcome != Outcome::Noop).then_some(outcome);
        self.model
            .applied(node, slot, command, command_fingerprint(command), reported);
    }

    fn next_slot(&mut self) -> u64 {
        let slot = self.slot;
        self.slot += 1;
        slot
    }
}

fn write(leader: LeaderUuid, seq: u64, records: &[&[u8]]) -> Command {
    Command::Write(Entry {
        leader,
        seq: Seq(seq),
        records: records.iter().map(|r| Value(r.to_vec())).collect(),
    })
}

fn set_leader(new: LeaderUuid, old: Option<LeaderUuid>) -> Command {
    Command::Control(Control::SetLeader { new, old })
}

fn truncate(leader: LeaderUuid, up_to: u64) -> Command {
    Command::Control(Control::Truncate {
        leader,
        up_to: Seq(up_to),
    })
}

fn view(leader: Option<LeaderUuid>, next: u64, first: u64) -> JournalView {
    JournalView {
        leader,
        next_seq: Seq(next),
        first_seq: Seq(first),
    }
}

/// A single-writer log that reaches every verdict kind once.
fn honest_single() -> Log {
    let mut log = Log::new(WriterMode::Single);
    assert!(matches!(
        log.decide(&set_leader(A, None)).1,
        Outcome::Leader(_)
    ));
    assert!(matches!(
        log.decide(&write(A, 0, &[b"x", b"y"])).1,
        Outcome::Accepted { .. }
    ));
    assert!(matches!(
        log.decide(&write(A, 0, &[b"x", b"y"])).1,
        Outcome::Duplicate { .. }
    ));
    assert!(matches!(
        log.decide(&write(A, 5, &[b"z"])).1,
        Outcome::Refused(_)
    ));
    assert!(matches!(
        log.decide(&write(A, 1, &[b"q"])).1,
        Outcome::Refused(_)
    ));
    assert!(matches!(log.decide(&truncate(A, 1)).1, Outcome::Trimmed(_)));
    assert!(matches!(
        log.decide(&write(A, 0, &[b"x"])).1,
        Outcome::Truncated(_)
    ));
    assert!(matches!(
        log.decide(&set_leader(B, Some(A))).1,
        Outcome::Leader(_)
    ));
    assert!(matches!(
        log.decide(&set_leader(A, Some(A))).1,
        Outcome::LeaderRefused(_)
    ));
    assert!(matches!(
        log.decide(&truncate(A, 2)).1,
        Outcome::TruncateRefused(_)
    ));
    assert!(matches!(
        log.decide(&write(A, 2, &[b"w"])).1,
        Outcome::Refused(_)
    ));
    assert!(matches!(
        log.decide(&write(LeaderUuid::UNSET, 2, &[b"w"])).1,
        Outcome::WrongMode(_)
    ));
    assert!(matches!(
        log.decide(&write(B, 2, &[b"w"])).1,
        Outcome::Accepted { .. }
    ));
    log.decide(&Command::Control(Control::Noop));
    log
}

#[test]
fn an_honest_single_writer_log_passes_every_check() {
    assert!(!red(|| honest_single().model.check()));
}

#[test]
fn an_honest_multi_writer_log_passes_every_check() {
    assert!(!red(|| {
        let mut log = Log::new(WriterMode::Multi);
        let unset = LeaderUuid::UNSET;
        assert!(matches!(
            log.decide(&write(unset, 0, &[b"x"])).1,
            Outcome::Accepted { .. }
        ));
        assert!(matches!(
            log.decide(&write(unset, 0, &[b"x"])).1,
            Outcome::Accepted { .. }
        ));
        assert!(matches!(
            log.decide(&write(unset, 0, &[])).1,
            Outcome::Refused(_)
        ));
        assert!(matches!(
            log.decide(&truncate(unset, 1)).1,
            Outcome::Trimmed(_)
        ));
        assert!(matches!(
            log.decide(&set_leader(A, None)).1,
            Outcome::WrongMode(_)
        ));
        assert!(matches!(
            log.decide(&write(A, 0, &[b"y"])).1,
            Outcome::WrongMode(_)
        ));
        log.model.check();
    }));
}

#[test]
fn the_mode_is_settled_before_any_verdict() {
    let mut log = honest_single();
    assert_eq!(log.model.mode(), WriterMode::Single);
    assert!(red(|| log.model.settle(WriterMode::Multi)));
    assert!(!red(|| JournalModel::default().settle(WriterMode::Multi)));
}

#[test]
fn two_nodes_that_judge_one_slot_differently_are_red() {
    let mut log = Log::new(WriterMode::Single);
    let command = set_leader(A, None);
    let (slot, outcome) = log.decide(&command);
    assert!(!red(|| log.fold(1, slot, &command, &outcome)));
    let other = Outcome::LeaderRefused(view(None, 0, 0));
    assert!(red(|| log.fold(2, slot, &command, &other)));
}

#[test]
fn a_verdict_that_does_not_match_its_command_is_red() {
    let mut log = Log::new(WriterMode::Single);
    let slot = log.next_slot();
    let accepted = Outcome::Accepted {
        seq: Seq(0),
        count: 1,
    };
    assert!(red(|| log.fold(0, slot, &set_leader(A, None), &accepted)));
}

#[test]
fn a_won_set_leader_installs_its_new_uuid() {
    let mut log = Log::new(WriterMode::Single);
    let slot = log.next_slot();
    let wrong = Outcome::Leader(view(Some(B), 0, 0));
    assert!(red(|| log.fold(0, slot, &set_leader(A, None), &wrong)));
}

#[test]
fn a_set_leader_loses_only_against_another_leader() {
    let mut log = Log::new(WriterMode::Single);
    let slot = log.next_slot();
    // The journal had no leader and the call named none: it must win.
    let lost = Outcome::LeaderRefused(view(None, 0, 0));
    assert!(red(|| log.fold(0, slot, &set_leader(A, None), &lost)));
}

#[test]
fn a_write_accepted_at_other_positions_than_it_asked_is_red() {
    let mut log = Log::new(WriterMode::Single);
    log.decide(&set_leader(A, None));
    let slot = log.next_slot();
    let elsewhere = Outcome::Accepted {
        seq: Seq(3),
        count: 1,
    };
    assert!(red(|| log.fold(0, slot, &write(A, 0, &[b"x"]), &elsewhere)));
}

#[test]
fn a_position_accepted_twice_is_red() {
    let mut log = Log::new(WriterMode::Single);
    log.decide(&set_leader(A, None));
    log.decide(&write(A, 0, &[b"x", b"y"]));
    let slot = log.next_slot();
    let again = Outcome::Accepted {
        seq: Seq(1),
        count: 1,
    };
    assert!(red(|| log.fold(0, slot, &write(A, 1, &[b"z"]), &again)));
}

#[test]
fn a_write_answered_truncated_above_first_seq_is_red() {
    let mut log = Log::new(WriterMode::Single);
    let slot = log.next_slot();
    let truncated = Outcome::Truncated(view(Some(A), 4, 1));
    assert!(red(|| log.fold(0, slot, &write(A, 2, &[b"x"]), &truncated)));
}

#[test]
fn a_duplicate_is_acked_where_it_names_with_the_bytes_accepted_there() {
    let mut log = Log::new(WriterMode::Single);
    log.decide(&set_leader(A, None));
    log.decide(&write(A, 0, &[b"x"]));
    let slot = log.next_slot();
    let moved = Outcome::Duplicate {
        seq: Seq(1),
        count: 1,
    };
    assert!(red(|| log.fold(0, slot, &write(A, 0, &[b"x"]), &moved)));
    let slot = log.next_slot();
    let other_bytes = Outcome::Duplicate {
        seq: Seq(0),
        count: 1,
    };
    assert!(red(|| log.fold(
        0,
        slot,
        &write(A, 0, &[b"q"]),
        &other_bytes
    )));
}

#[test]
fn a_duplicate_acked_mid_batch_or_over_other_bytes_is_red_at_check() {
    let mut log = Log::new(WriterMode::Single);
    log.decide(&set_leader(A, None));
    log.decide(&write(A, 0, &[b"x", b"y"]));
    let slot = log.next_slot();
    let mid = Outcome::Duplicate {
        seq: Seq(1),
        count: 1,
    };
    log.fold(0, slot, &write(A, 1, &[b"y"]), &mid);
    assert!(red(|| log.model.judge_duplicates()));
    let mut log = Log::new(WriterMode::Single);
    log.decide(&set_leader(A, None));
    log.decide(&write(A, 0, &[b"x"]));
    // A retry that the model holds no batch for (its batch is unknown),
    // over a position whose record differs.
    log.model
        .duplicates
        .insert(9, (0, 1, vec![record_hash(b"q")]));
    assert!(red(|| log.model.judge_duplicates()));
}

#[test]
fn a_multi_writer_journal_refuses_only_an_empty_write_and_accepts_unfenced() {
    let mut log = Log::new(WriterMode::Multi);
    let unset = LeaderUuid::UNSET;
    let slot = log.next_slot();
    let refused = Outcome::Refused(view(None, 0, 0));
    assert!(red(|| log.fold(
        0,
        slot,
        &write(unset, 0, &[b"x"]),
        &refused
    )));
    let slot = log.next_slot();
    let fenced = Outcome::Accepted {
        seq: Seq(0),
        count: 1,
    };
    // The fenced write is also of the wrong mode: red either way.
    assert!(red(|| log.fold(0, slot, &write(A, 0, &[b"x"]), &fenced)));
}

#[test]
fn an_in_order_write_from_the_leader_is_refused_only_when_empty() {
    let mut log = Log::new(WriterMode::Single);
    log.decide(&set_leader(A, None));
    let slot = log.next_slot();
    let refused = Outcome::Refused(view(Some(A), 0, 0));
    assert!(red(|| log.fold(0, slot, &write(A, 0, &[b"x"]), &refused)));
    assert!(!red(|| {
        let slot = log.next_slot();
        log.fold(0, slot, &write(A, 0, &[]), &refused);
    }));
}

#[test]
fn a_call_of_the_wrong_mode_is_refused_as_such_and_only_then() {
    let mut log = Log::new(WriterMode::Single);
    let slot = log.next_slot();
    let accepted = Outcome::Accepted {
        seq: Seq(0),
        count: 1,
    };
    let unfenced = write(LeaderUuid::UNSET, 0, &[b"x"]);
    assert!(red(|| log.fold(0, slot, &unfenced, &accepted)));
    let slot = log.next_slot();
    let wrong = Outcome::WrongMode(view(None, 0, 0));
    assert!(red(|| log.fold(0, slot, &write(A, 0, &[b"x"]), &wrong)));
    assert!(!log.model.of_wrong_mode(&Command::Control(Control::Noop)));
    assert!(!log.model.of_wrong_mode(&set_leader(A, None)));
    assert!(log.model.of_wrong_mode(&truncate(LeaderUuid::UNSET, 1)));
}

#[test]
fn a_truncate_is_judged_against_its_fence() {
    let mut log = Log::new(WriterMode::Single);
    let slot = log.next_slot();
    let trimmed_foreign = Outcome::Trimmed(view(Some(B), 3, 1));
    assert!(red(|| log.fold(0, slot, &truncate(A, 1), &trimmed_foreign)));
    let slot = log.next_slot();
    let refused_owner = Outcome::TruncateRefused(view(Some(A), 3, 0));
    assert!(red(|| log.fold(0, slot, &truncate(A, 1), &refused_owner)));
    let slot = log.next_slot();
    let short = Outcome::Trimmed(view(Some(A), 3, 1));
    assert!(red(|| log.fold(0, slot, &truncate(A, 2), &short)));
}

#[test]
fn a_multi_writer_truncation_applies_from_anyone() {
    let mut log = Log::new(WriterMode::Multi);
    let slot = log.next_slot();
    let refused = Outcome::TruncateRefused(view(None, 3, 0));
    assert!(red(|| log.fold(
        0,
        slot,
        &truncate(LeaderUuid::UNSET, 1),
        &refused
    )));
}

#[test]
fn a_call_is_answered_with_the_verdict_its_slot_applied_to() {
    let mut log = Log::new(WriterMode::Single);
    let (slot, outcome) = log.decide(&set_leader(A, None));
    assert!(!red(|| log.model.answered(0, slot, &outcome)));
    let other = Outcome::LeaderRefused(view(None, 0, 0));
    assert!(red(|| log.model.answered(0, slot, &other)));
    // A slot the model has not folded is only noted.
    assert!(!red(|| log.model.answered(0, 99, &other)));
}

#[test]
fn a_superseded_writer_answered_its_fence_is_noted() {
    let mut log = honest_single();
    // Slot 10: A's write refused by B's generation.
    let refused = log.model.slots[&10].outcome.clone();
    assert!(!log.model.superseded_write_answered);
    log.model.answered(0, 10, &refused);
    assert!(log.model.superseded_write_answered);
}

#[test]
fn positions_that_are_not_dense_in_slot_order_are_red_at_check() {
    let mut log = Log::new(WriterMode::Single);
    log.decide(&set_leader(A, None));
    log.decide(&write(A, 0, &[b"x"]));
    let slot = log.next_slot();
    let jump = Outcome::Accepted {
        seq: Seq(4),
        count: 1,
    };
    log.fold(0, slot, &write(A, 4, &[b"y"]), &jump);
    assert!(red(|| log.model.check()));
}

#[test]
fn a_write_accepted_under_a_uuid_never_won_is_red_at_check() {
    let mut log = Log::new(WriterMode::Single);
    let slot = log.next_slot();
    let accepted = Outcome::Accepted {
        seq: Seq(0),
        count: 1,
    };
    log.fold(0, slot, &write(A, 0, &[b"x"]), &accepted);
    assert!(red(|| log.model.check()));
}

#[test]
fn a_write_accepted_from_a_superseded_writer_is_red_at_check() {
    let mut log = Log::new(WriterMode::Single);
    log.decide(&set_leader(A, None));
    log.decide(&set_leader(B, Some(A)));
    let slot = log.next_slot();
    let accepted = Outcome::Accepted {
        seq: Seq(0),
        count: 1,
    };
    log.fold(0, slot, &write(A, 0, &[b"x"]), &accepted);
    assert!(red(|| log.model.check()));
}

#[test]
fn a_verdict_that_names_another_leader_than_the_one_in_force_is_red_at_check() {
    let mut log = Log::new(WriterMode::Single);
    log.decide(&set_leader(A, None));
    let slot = log.next_slot();
    let refused = Outcome::Refused(view(Some(B), 0, 0));
    log.fold(0, slot, &write(B, 0, &[b"x"]), &refused);
    assert!(red(|| log.model.check()));
}

#[test]
fn a_set_leader_that_wins_over_another_than_the_leader_in_force_is_red_at_check() {
    let mut log = Log::new(WriterMode::Single);
    log.decide(&set_leader(A, None));
    let slot = log.next_slot();
    let won = Outcome::Leader(view(Some(B), 0, 0));
    log.fold(0, slot, &set_leader(B, None), &won);
    assert!(red(|| log.model.check()));
    let mut log = Log::new(WriterMode::Single);
    let slot = log.next_slot();
    let own = Outcome::Leader(view(Some(A), 0, 0));
    log.fold(0, slot, &set_leader(A, Some(A)), &own);
    assert!(red(|| log.model.check()));
}

#[test]
fn first_seq_moving_backwards_is_red_at_check() {
    let mut log = Log::new(WriterMode::Single);
    let slot = log.next_slot();
    log.fold(
        0,
        slot,
        &truncate(A, 2),
        &Outcome::Trimmed(view(Some(A), 3, 2)),
    );
    let slot = log.next_slot();
    log.fold(
        0,
        slot,
        &write(A, 0, &[b"x"]),
        &Outcome::Truncated(view(Some(A), 3, 0)),
    );
    assert!(red(|| log.model.check()));
}

#[test]
fn a_truncation_that_trims_past_its_clamped_ask_is_red_at_check() {
    let mut log = Log::new(WriterMode::Single);
    log.decide(&set_leader(A, None));
    log.decide(&write(A, 0, &[b"x", b"y", b"z"]));
    let slot = log.next_slot();
    log.fold(
        0,
        slot,
        &truncate(A, 1),
        &Outcome::Trimmed(view(Some(A), 3, 2)),
    );
    assert!(red(|| log.model.check()));
    // The same verdict behind a slot no node reported: the floor before it
    // is unknown, so the upper bound is not judged.
    let mut log = Log::new(WriterMode::Single);
    log.decide(&set_leader(A, None));
    log.decide(&write(A, 0, &[b"x", b"y", b"z"]));
    log.next_slot();
    let slot = log.next_slot();
    log.fold(
        0,
        slot,
        &truncate(A, 1),
        &Outcome::Trimmed(view(Some(A), 3, 2)),
    );
    assert!(!red(|| log.model.check()));
}

#[test]
fn a_multi_writer_verdict_that_names_a_leader_is_red_at_check() {
    let mut log = Log::new(WriterMode::Multi);
    let slot = log.next_slot();
    let refused = Outcome::Refused(view(Some(A), 0, 0));
    log.fold(0, slot, &write(LeaderUuid::UNSET, 0, &[]), &refused);
    assert!(red(|| log.model.check()));
}

#[test]
fn a_truncate_accepted_from_another_than_the_writer_in_force_is_red_at_check() {
    let mut log = Log::new(WriterMode::Single);
    log.decide(&set_leader(A, None));
    let slot = log.next_slot();
    // Judged against a view that names B, but A is in force.
    log.fold(
        0,
        slot,
        &truncate(B, 0),
        &Outcome::Trimmed(view(Some(B), 0, 0)),
    );
    assert!(red(|| log.model.check()));
}

#[test]
fn an_acknowledged_write_is_judged_by_its_bytes() {
    let log = honest_single();
    let own = AckedWrite {
        seq: 0,
        count: 2,
        records: vec![record_hash(b"x"), record_hash(b"y")],
    };
    assert!(!red(|| assert!(log.model.judge_acked(&own))));
    let other = AckedWrite {
        records: vec![record_hash(b"x"), record_hash(b"q")],
        ..own.clone()
    };
    assert!(red(|| {
        log.model.judge_acked(&other);
    }));
    let short = AckedWrite {
        count: 3,
        ..own.clone()
    };
    assert!(red(|| {
        log.model.judge_acked(&short);
    }));
    let unknown = AckedWrite {
        seq: 40,
        count: 1,
        records: vec![1],
    };
    assert!(!red(|| assert!(!log.model.judge_acked(&unknown))));
}

#[test]
fn an_acknowledged_write_with_other_bytes_is_red_at_check() {
    let mut log = honest_single();
    log.model.note_acked(AckedWrite {
        seq: 2,
        count: 1,
        records: vec![record_hash(b"not w")],
    });
    assert!(red(|| log.model.check()));
}

#[test]
fn the_model_answers_what_it_folded() {
    let log = honest_single();
    assert!(log.model.judged_any());
    assert!(!JournalModel::default().judged_any());
    assert_eq!(log.model.record_at(0), Some(record_hash(b"x")));
    assert_eq!(log.model.record_at(2), Some(record_hash(b"w")));
    assert_eq!(log.model.record_at(3), None);
    assert_eq!(log.model.next_seq(), 3);
    assert_eq!(log.model.released(), 1);
    // Slot 1 holds [0, 2), slot 12 holds [2, 3).
    assert_eq!(log.model.end_below(1), None);
    assert_eq!(log.model.end_below(2), Some(2));
    assert_eq!(log.model.end_below(12), Some(2));
    assert_eq!(log.model.end_below(13), Some(3));
    assert_eq!(log.model.slot_holding(0), Some(1));
    assert_eq!(log.model.slot_holding(1), Some(1));
    assert_eq!(log.model.slot_holding(2), Some(12));
    assert_eq!(log.model.slot_holding(3), None);
    let leaders: Vec<_> = log.model.set_leaders().collect();
    assert_eq!(leaders, vec![(0, A, true), (7, B, true), (8, A, false)]);
    assert!(log.model.noops.contains(&13));
}

#[test]
fn an_honest_log_raises_every_coverage_flag_its_verdicts_reach() {
    let log = honest_single();
    let m = &log.model;
    assert!(m.accepted_any && m.duplicate_any && m.won_any && m.lost_any);
    assert!(m.trimmed_any && m.truncated_any && m.wrong_mode_any);
    assert!(m.gap_refused_any && m.ahead_refused_any && m.fenced_any);
    assert!(!m.reinstated_any);
    let mut log = honest_single();
    log.decide(&set_leader(A, Some(B)));
    assert!(log.model.reinstated_any, "A leads again");
}

#[test]
fn hides_verdict_and_unanswered_verdict_follow_the_noops() {
    let log = honest_single();
    assert!(!log.model.hides_verdict(None, 4));
    assert!(!log.model.hides_verdict(Some(3), 4));
    assert!(log.model.hides_verdict(Some(3), 6));
    let mut model = JournalModel::default();
    model.noops.insert(4);
    assert!(!model.hides_verdict(Some(3), 5));
    let refused = super::SlotFact {
        write: None,
        truncate: None,
        set_leader: None,
        outcome: Outcome::Refused(view(Some(A), 2, 1)),
    };
    let mut floor = None;
    assert!(model.unanswered_verdict(3, &refused, &mut floor));
    assert_eq!(floor, Some(1));
    model.answered.insert(3);
    assert!(!model.unanswered_verdict(3, &refused, &mut floor));
    let leader = super::SlotFact {
        outcome: Outcome::Leader(view(Some(A), 2, 1)),
        ..refused
    };
    assert!(!model.unanswered_verdict(5, &leader, &mut floor));
}

#[test]
fn revealed_is_the_whole_view_except_for_accepts_duplicates_and_noops() {
    let v = view(Some(A), 2, 1);
    for outcome in [
        Outcome::Refused(v),
        Outcome::Truncated(v),
        Outcome::Leader(v),
        Outcome::LeaderRefused(v),
        Outcome::Trimmed(v),
        Outcome::TruncateRefused(v),
        Outcome::WrongMode(v),
    ] {
        assert_eq!(super::revealed(&outcome), Some(v));
    }
    let accepted = Outcome::Accepted {
        seq: Seq(0),
        count: 1,
    };
    let duplicate = Outcome::Duplicate {
        seq: Seq(0),
        count: 1,
    };
    for outcome in [accepted, duplicate, Outcome::Noop] {
        assert_eq!(super::revealed(&outcome), None);
    }
}
