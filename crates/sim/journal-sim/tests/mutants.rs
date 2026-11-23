//! Each oracle of the fold walk catches its mutant: a bug planted in the
//! driver, never in the code.

use journal_sim::campaign::try_campaign;
use journal_sim::fold::{FOLD, FoldModel, FoldMutant, REFOLD, RESOLVE, Shape, VIEW};
use moonpool_walk::Walk;

fn caught(mutant: FoldMutant, oracle: &str) {
    let walk = Walk::<FoldModel>::new()
        .seeds(300)
        .chaos_steps(120)
        .env_prefix(None)
        .model(move |cx| FoldModel::build(Shape::WIDE, mutant, cx));
    let text = try_campaign(&walk, &[]).expect_err("a seed of the mutant fails");
    assert!(
        text.contains(oracle),
        "{mutant:?} failed on another check:\n{text}"
    );
}

#[test]
fn view_catches_a_stale_boundary() {
    caught(FoldMutant::StaleBoundary, VIEW);
}

#[test]
fn fold_catches_a_lost_log() {
    caught(FoldMutant::LostLog, FOLD);
}

#[test]
fn refold_catches_a_short_refold() {
    caught(FoldMutant::RefoldShort, REFOLD);
}

#[test]
fn resolve_catches_a_resolve_at_the_segment_in_force() {
    caught(FoldMutant::ResolveInForce, RESOLVE);
}

#[test]
fn fold_catches_a_below_in_the_segment_in_force() {
    caught(FoldMutant::BelowJudged, FOLD);
}

/// The trace control passes every oracle.
#[test]
fn the_oracles_pass_a_below_in_the_segment_in_force() {
    let walk = Walk::<FoldModel>::new()
        .seeds(100)
        .chaos_steps(120)
        .env_prefix(None)
        .model(|cx| FoldModel::build(Shape::TRACED, FoldMutant::BelowInSegment, cx));
    try_campaign(&walk, &[]).expect("the oracles pass the trace control");
}
