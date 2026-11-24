//! The driver walks: every seed passes every oracle, the campaign reaches
//! every site, and each oracle catches its mutant.

use journal_sim::backend::LISTED;
use journal_sim::campaign::{campaign, try_campaign};
use journal_sim::host::{AGREE, ANSWER, FLIGHT, HostMutant, READ, reach, walk};

/// Three processes on the generic driver, with commit faults, crashes and
/// a lossy network.
#[test]
fn the_driver_walk() {
    campaign(&walk(HostMutant::None, 200, 3000), reach::ALL);
}

fn caught(mutant: HostMutant, oracle: &str) {
    let text = try_campaign(&walk(mutant, 300, 1000), &[]).expect_err("a seed of the mutant fails");
    assert!(
        text.contains(oracle),
        "{mutant:?} failed on another check:\n{text}"
    );
}

#[test]
fn flight_catches_a_disk_that_lies_about_a_commit() {
    caught(HostMutant::LyingDisk, FLIGHT);
}

#[test]
fn agree_catches_a_process_that_loses_its_disk() {
    caught(HostMutant::WipeDisk, AGREE);
}

#[test]
fn answer_catches_a_taken_write_taken_as_written() {
    caught(HostMutant::TakenAsWritten, ANSWER);
}

#[test]
fn read_catches_a_reader_that_shifts_its_page() {
    caught(HostMutant::PageShift, READ);
}

#[test]
fn listed_catches_a_list_that_hides_a_role() {
    caught(HostMutant::HiddenRole, LISTED);
}
