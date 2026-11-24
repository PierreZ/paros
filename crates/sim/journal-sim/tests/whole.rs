//! The whole journal walks (#1445): every seed passes every oracle, the
//! campaign reaches every site, and each oracle catches its mutant.

use journal_sim::campaign::{campaign, try_sites};
use journal_sim::whole::{
    ACKED, AGAIN, AGREE, ALERTS, ANSWER, BOUNDED_IN_FLIGHT, CONTROLLER_STOPPED, DISK, FLIGHT,
    HEAL_HELD, ONCE, SEALED, STOPPED, WholeMutant, reach, walk,
};
use moonpool_assertions::AssertKind;

/// Two journals on nine machines in three zones, with their controllers,
/// retrying writers, machine and zone outages, commit faults and a lossy
/// network.
#[test]
fn the_whole_journal_walk() {
    campaign(&walk(WholeMutant::None, 150, 6000), reach::ALL);
}

/// A seed of `mutant` fails on `oracle`. A mutant breaks the contract of
/// the code, so a seed may fail first on another check: a disk wiped on
/// every replica of a segment loses a slot for good, so the retire stalls
/// before a false agreement forms. The claim is that the oracle sees the
/// planted bug, so the walk replays seeds 1 to 200 in order until one fails
/// on it (Pierre, 2026-11-23). Each seed it passes over on another check
/// must pass with no mutant: the failure is the mutant's, never the walk's.
fn caught(mutant: WholeMutant, oracle: &str) {
    let walk_of = |m| walk(m, 1, 1500);
    let (planted, clean) = (walk_of(mutant), walk_of(WholeMutant::None));
    let mut others = Vec::new();
    for seed in 1..=200 {
        let Err(failure) = planted.replay(seed) else {
            continue;
        };
        // A seed that fails with the mutant passes without it: the mutant
        // made the failure.
        if let Err(failure) = clean.replay(seed) {
            panic!("seed {seed} fails with no mutant: {failure}");
        }
        let text = failure.to_string();
        if text.contains(oracle) {
            return;
        }
        others.push(text);
    }
    panic!("no seed of {mutant:?} fails on {oracle}; other failures: {others:#?}");
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

#[test]
fn answer_catches_a_below_inside_the_segment() {
    caught(WholeMutant::BelowInSegment, ANSWER);
}

#[test]
fn disk_catches_a_corrupt_copy() {
    caught(WholeMutant::CorruptCopy, DISK);
}

#[test]
fn again_catches_a_register_that_refuses_a_change_asked_again() {
    caught(WholeMutant::RefuseAgain, AGAIN);
}

#[test]
fn stopped_catches_a_register_that_refuses_a_first_seal() {
    caught(WholeMutant::RefuseSeal, STOPPED);
}

#[test]
fn controller_stopped_catches_a_read_with_a_forged_term() {
    caught(WholeMutant::ForgedTerm, CONTROLLER_STOPPED);
}

#[test]
fn bounded_in_flight_catches_a_token_for_every_journal() {
    caught(WholeMutant::NoToken, BOUNDED_IN_FLIGHT);
}

/// The whole walk reaches every `probe!` of the controller's driver and of
/// its signals.
#[test]
fn the_whole_walk_reaches_every_probe_of_the_driver_and_the_signals() {
    let probes: Vec<&str> = paros_journal_control::driver::PROBES
        .iter()
        .chain(paros_journal_control::signals::PROBES)
        .copied()
        .collect();
    try_sites(
        AssertKind::Reachable,
        || {
            walk(WholeMutant::None, 150, 6000)
                .try_run()
                .map_err(|error| error.to_string())
        },
        &probes,
    )
    .unwrap_or_else(|error| panic!("{error}"));
}
