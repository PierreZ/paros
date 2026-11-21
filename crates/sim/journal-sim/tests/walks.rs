//! The fold walks: every seed passes every oracle, and the campaign
//! reaches every site.

use journal_sim::campaign::{campaign, try_campaign};
use journal_sim::fold::{FoldModel, FoldMutant, Shape, reach};
use moonpool_walk::Walk;

/// The seeds of each walk in CI.
const SEEDS: u64 = 200;

fn walk(shape: Shape) -> Walk<FoldModel> {
    Walk::<FoldModel>::new()
        .seeds(SEEDS)
        .chaos_steps(120)
        .model(move |cx| FoldModel::build(shape, FoldMutant::None, cx))
}

/// The walk of the traced shape: one record a write.
#[test]
fn the_traced_fold_walk() {
    campaign(&walk(Shape::TRACED), reach::ALL);
}

/// The walk with batches, empty writes and `Upgrade`.
#[test]
fn the_wide_fold_walk() {
    let sites: Vec<&str> = reach::ALL.iter().chain(reach::WIDE).copied().collect();
    campaign(&walk(Shape::WIDE), &sites);
}

#[test]
fn a_long_site_name_is_refused() {
    let long = "fold walk: a site name that runs on past the sixty-three bytes of a slot";
    let failure = try_campaign(&walk(Shape::TRACED).seeds(1), &[long]).expect_err("refused");
    assert!(failure.contains("longer than"), "{failure}");
}
