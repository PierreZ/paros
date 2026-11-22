//! The whole journal walks (#1445): every seed passes every oracle, the
//! campaign reaches every site, and each oracle catches its mutant.

use journal_sim::campaign::{campaign, try_campaign};
use journal_sim::whole::{
    ACKED, AGREE, ALERTS, ANSWER, FLIGHT, HEAL_HELD, ONCE, SEALED, WholeMutant, reach, walk,
};

/// Two journals on nine machines in three zones, with their controllers,
/// retrying writers, machine and zone outages, commit faults and a lossy
/// network.
#[test]
fn the_whole_journal_walk() {
    campaign(&walk(WholeMutant::None, 150, 6000), reach::ALL);
}

fn caught(mutant: WholeMutant, oracle: &str) {
    let text = try_campaign(&walk(mutant, 200, 1500), &[]).expect_err("a seed of the mutant fails");
    assert!(
        text.contains(oracle),
        "{mutant:?} failed on another check:\n{text}"
    );
}

#[test]
fn flight_catches_a_disk_that_lies_about_a_commit() {
    caught(WholeMutant::LyingDisk, FLIGHT);
}

#[test]
fn agree_catches_a_machine_that_loses_its_disk() {
    caught(WholeMutant::WipeDisk, AGREE);
}

#[test]
fn sealed_catches_a_commit_at_the_end_of_a_sealed_segment() {
    caught(WholeMutant::CommitAtEnd, SEALED);
}

#[test]
fn answer_catches_a_taken_write_taken_as_written() {
    caught(WholeMutant::TakenAsWritten, ANSWER);
}

#[test]
fn once_catches_an_acked_write_sent_again_at_the_next_position() {
    caught(WholeMutant::WriteTwice, ONCE);
}

#[test]
fn acked_catches_an_ack_kept_at_another_position() {
    caught(WholeMutant::AckOffByOne, ACKED);
}

#[test]
fn alerts_catches_a_machine_kept_down_in_the_view() {
    caught(WholeMutant::StaleLiveness, ALERTS);
}

#[test]
fn heal_held_catches_a_copy_reported_when_it_starts() {
    caught(WholeMutant::EarlyCopied, HEAL_HELD);
}
