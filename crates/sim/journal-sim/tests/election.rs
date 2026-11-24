//! The election walks: every seed passes every oracle, the campaign reaches
//! every site, and each oracle catches its mutant.

use journal_sim::campaign::{campaign, try_campaign};
use journal_sim::election::{ElectionMutant, FRESH, LEAD_TRUE, MULTI, TERMS_GROW, reach, walk};

/// Three actors elect over one journal: lost answers, crashes, hand-ons and a
/// user that sets the multi-writer mode.
#[test]
fn the_election_walk() {
    campaign(&walk(ElectionMutant::None, 300, 300), reach::ALL);
}

/// The trace control is a bug that only the model sees: the oracles pass it.
#[test]
fn the_oracles_pass_a_skipped_hand_number() {
    campaign(&walk(ElectionMutant::HandSkip, 300, 300), &[]);
}

fn caught(mutant: ElectionMutant, oracle: &str) {
    let text = try_campaign(&walk(mutant, 300, 300), &[]).expect_err("a seed of the mutant fails");
    assert!(
        text.contains(oracle),
        "{mutant:?} failed on another check:\n{text}"
    );
}

#[test]
fn fresh_catches_draws_that_repeat() {
    caught(ElectionMutant::DrawsRepeat, FRESH);
}

#[test]
fn terms_grow_catches_pages_that_strip_term_records() {
    caught(ElectionMutant::PageDrops, TERMS_GROW);
}

#[test]
fn lead_true_catches_a_write_that_vanishes() {
    caught(ElectionMutant::WriteVanishes, LEAD_TRUE);
}

#[test]
fn multi_catches_a_journal_that_hides_its_mode() {
    caught(ElectionMutant::LaxMulti, MULTI);
}
