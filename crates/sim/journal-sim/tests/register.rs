//! The register walks: every seed passes every oracle, the campaign
//! reaches every site, and each oracle catches its mutant.

use journal_sim::campaign::{campaign, try_campaign};
use journal_sim::register::{ACTED, CARRIED, RegisterMutant, Shape, reach, walk};

/// The seeds of each walk in CI.
const SEEDS: u64 = 200;

/// The walk of the traced shape: the model's operations on one record.
#[test]
fn the_traced_register_walk() {
    campaign(
        &walk(Shape::TRACED, RegisterMutant::None, SEEDS, 120),
        reach::ALL,
    );
}

/// Any change, retries, two handovers, restarts.
#[test]
fn the_wide_register_walk() {
    let sites: Vec<&str> = reach::ALL.iter().chain(reach::WIDE).copied().collect();
    campaign(&walk(Shape::WIDE, RegisterMutant::None, SEEDS, 400), &sites);
}

fn caught(shape: Shape, mutant: RegisterMutant, oracle: &str) {
    let text =
        try_campaign(&walk(shape, mutant, 300, 400), &[]).expect_err("a seed of the mutant fails");
    assert!(
        text.contains(oracle),
        "{mutant:?} failed on another check:\n{text}"
    );
}

#[test]
fn carried_catches_a_reply_sent_before_its_write_is_durable() {
    caught(Shape::WIDE, RegisterMutant::SendBeforePersist, CARRIED);
}

#[test]
fn acted_catches_a_step_that_acts_on_a_read() {
    caught(Shape::TRACED, RegisterMutant::ActOnRead, ACTED);
}
