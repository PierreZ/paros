//! The heal walk (#1314 (re-replication of a sealed segment's replica
//! set)): every seed passes every oracle and settles, the campaign reaches
//! every site, and each oracle catches its mutant.

use journal_sim::campaign::try_campaign;
use journal_sim::heal::{COMPARED, CONVERGES, HELD, HealMutant, reach, walk};

/// The seeds of the walk in CI.
const SEEDS: u64 = 3000;
/// The chaos steps of each seed.
const STEPS: u64 = 300;

#[test]
fn the_heal_walk() {
    try_campaign(&walk(HealMutant::None, SEEDS, STEPS), reach::ALL)
        .unwrap_or_else(|error| panic!("{error}"));
}

fn caught(mutant: HealMutant, oracle: &str) {
    let text =
        try_campaign(&walk(mutant, SEEDS, STEPS), &[]).expect_err("a seed of the mutant fails");
    assert!(
        text.contains(oracle),
        "{mutant:?} failed on another check:\n{text}"
    );
}

#[test]
fn held_catches_a_copy_reported_before_it_is_complete() {
    caught(HealMutant::EarlyReport, HELD);
}

#[test]
fn held_catches_a_lower_copy_taken() {
    caught(HealMutant::AnyCopy, HELD);
}

#[test]
fn held_catches_a_heal_written_with_no_compare() {
    caught(HealMutant::NoCompare, HELD);
}

#[test]
fn held_catches_a_heal_written_with_no_term_check() {
    caught(HealMutant::NoTermCheck, HELD);
}

#[test]
fn compared_catches_a_heal_compared_with_no_count_of_heals() {
    caught(HealMutant::NoHealCount, COMPARED);
}

#[test]
fn converges_catches_a_heal_never_sent_again() {
    caught(HealMutant::NoResend, CONVERGES);
}

#[test]
fn converges_catches_an_orphan_copy_kept() {
    caught(HealMutant::KeepOrphans, CONVERGES);
}
