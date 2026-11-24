//! The election walks: every seed passes every oracle, the campaign reaches
//! every site, and each oracle catches its mutant.

use journal_sim::campaign::campaign;
use journal_sim::election::{
    ACTS, AGREE, ElectionMutant, FENCE, ONCE, ONE_HOLDER, SETTLED, TRUTH, reach, walk,
};

/// Three members, a lossy mailbox, crashes, early expiries, checkpoints and
/// trims.
#[test]
fn the_election_walk() {
    campaign(&walk(ElectionMutant::None, 300), reach::ALL);
}

/// The first seed that fails with the mutant names the oracle, and every
/// seed that fails with it passes without it: the mutant made the failure.
fn caught(mutant: ElectionMutant, oracle: &str) {
    let (planted, clean) = (walk(mutant, 1), walk(ElectionMutant::None, 1));
    let mut failures = Vec::new();
    for seed in 1..=300 {
        if let Err(failure) = planted.replay(seed) {
            if let Err(failure) = clean.replay(seed) {
                panic!("seed {seed} fails with no mutant: {failure}");
            }
            let text = failure.to_string();
            if text.contains(oracle) {
                return;
            }
            failures.push(text);
        }
    }
    assert!(
        !failures.is_empty(),
        "{mutant:?}: no seed of the mutant fails"
    );
    panic!(
        "{mutant:?}: no seed fails on {oracle}, the first failure:\n{}",
        failures[0]
    );
}

#[test]
fn agree_catches_a_late_page() {
    caught(ElectionMutant::PageLate, AGREE);
}

#[test]
fn truth_catches_a_skipped_expiry() {
    caught(ElectionMutant::SkipExpire, TRUTH);
}

#[test]
fn once_catches_a_resend_numbered_again() {
    caught(ElectionMutant::Renumber, ONCE);
}

#[test]
fn acts_catches_an_unguarded_act() {
    caught(ElectionMutant::Unguarded, ACTS);
}

#[test]
fn settled_catches_a_result_filed_under_the_next_act() {
    caught(ElectionMutant::SwapTxn, SETTLED);
}

#[test]
fn one_holder_catches_an_order_with_the_latch_s_term() {
    caught(ElectionMutant::AnyTerm, ONE_HOLDER);
}

#[test]
fn fence_catches_a_receiver_with_no_fence() {
    caught(ElectionMutant::NoFence, FENCE);
}
