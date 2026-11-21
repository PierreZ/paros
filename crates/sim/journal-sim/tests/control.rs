//! The grid controller's decisions (#1261 (grid controller decision
//! tests), `docs/journal/06-controller.md` §J6.2, tests 1, 2, 5 and 6):
//! every small view passes every oracle and gives the reference's decision,
//! the sweep reaches every probe of `decide`, and each oracle catches its
//! planted mutant.

use std::collections::BTreeSet;

use journal_sim::campaign::try_sites;
use journal_sim::control::{
    BOUNDED_RESHAPE_RATE, ControlMutant, FENCED, IDEMPOTENT, MONOTONE, NO_STALE_PLAN,
    NO_SUSPECT_REPLACED, NO_UNSAFE_PLAN, SAME_AS_REFERENCE, SHAPES, SMALL_SHAPES, check, views,
};
use moonpool_assertions::AssertKind;
use paros_journal_control::PROBES;

/// Every view of the sweep, behind `mutant`: the number of views, and the
/// first failure of each oracle that failed.
fn sweep(shapes: &[(u8, u64)], mutant: ControlMutant) -> (u64, Vec<String>) {
    let mut count = 0;
    let mut failures: Vec<String> = Vec::new();
    views(shapes, |case| {
        count += 1;
        if let Err(text) = check(case, mutant) {
            let oracle = text.split(':').next().unwrap_or_default().to_owned();
            if !failures.iter().any(|f| f.starts_with(&oracle)) {
                failures.push(text);
            }
        }
    });
    (count, failures)
}

/// Every small view passes every oracle, and the sweep reaches every probe
/// of `decide`, with no excused probe.
#[test]
fn every_small_view_passes_and_every_probe_is_reached() {
    try_sites(
        AssertKind::Reachable,
        || {
            let (count, failures) = sweep(SHAPES, ControlMutant::None);
            if failures.is_empty() {
                Ok(format!("{count} views"))
            } else {
                Err(failures.join("\n\n"))
            }
        },
        PROBES,
    )
    .unwrap_or_else(|error| panic!("{error}"));
}

fn caught(mutant: ControlMutant, oracle: &str) {
    let (_, failures) = sweep(SMALL_SHAPES, mutant);
    let names: BTreeSet<&str> = failures
        .iter()
        .map(|f| f.split(':').next().unwrap_or_default())
        .collect();
    assert!(
        names.contains(oracle),
        "{mutant:?} is not caught by {oracle:?}; the oracles that failed: {names:?}"
    );
}

#[test]
fn no_unsafe_plan_catches_a_controller_that_ignores_the_zones() {
    caught(ControlMutant::IgnoreZones, NO_UNSAFE_PLAN);
}

#[test]
fn bounded_reshape_rate_catches_a_controller_that_ignores_the_interval() {
    caught(ControlMutant::IgnoreInterval, BOUNDED_RESHAPE_RATE);
}

#[test]
fn fenced_catches_a_controller_that_takes_the_register_term() {
    caught(ControlMutant::IgnoreTerm, FENCED);
}

#[test]
fn no_stale_plan_catches_a_controller_that_ignores_the_age() {
    caught(ControlMutant::IgnoreAge, NO_STALE_PLAN);
}

#[test]
fn no_suspect_replaced_catches_a_suspect_read_as_down() {
    caught(ControlMutant::SuspectAsDown, NO_SUSPECT_REPLACED);
}

#[test]
fn idempotent_catches_a_controller_that_ignores_the_reshape_guard() {
    caught(ControlMutant::IgnoreReshaping, IDEMPOTENT);
}

#[test]
fn same_as_reference_catches_a_leader_read_inverted() {
    caught(ControlMutant::InvertLeader, SAME_AS_REFERENCE);
}

#[test]
fn monotone_catches_a_stop_held_as_unplaceable() {
    caught(ControlMutant::StoppedAsUnplaceable, MONOTONE);
}
