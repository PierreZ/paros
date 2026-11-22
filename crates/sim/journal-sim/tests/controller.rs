//! The controller walks (#1261 (grid controller decision tests), §J6.2
//! test 3): every seed passes every oracle and settles, the campaign
//! reaches every site and every `probe!` of the controller role, and each
//! oracle catches its mutant.

use journal_sim::campaign::{try_campaign, try_sites};
use journal_sim::controller::{
    BOUNDED_IN_FLIGHT, BOUNDED_RESHAPE_RATE, CONVERGES, COST_BOUND, ControllerMutant, FENCED,
    NO_OSCILLATION, Shape, reach, walk,
};
use moonpool_assertions::AssertKind;
use paros_journal_control::role;

/// The seeds of each walk in CI.
const SEEDS: u64 = 300;
/// The chaos steps of each seed.
const STEPS: u64 = 300;

fn sites(extra: &[&'static str]) -> Vec<&'static str> {
    reach::ALL.iter().chain(extra).copied().collect()
}

#[test]
fn the_traced_controller_walk() {
    try_campaign(
        &walk(Shape::TRACED, ControllerMutant::None, SEEDS, STEPS),
        &sites(&[]),
    )
    .unwrap_or_else(|error| panic!("{error}"));
}

#[test]
fn the_wide_controller_walk() {
    try_campaign(
        &walk(Shape::WIDE, ControllerMutant::None, SEEDS, STEPS),
        &sites(reach::WIDE),
    )
    .unwrap_or_else(|error| panic!("{error}"));
}

#[test]
fn the_many_journals_walk() {
    let many: Vec<&str> = [
        reach::OPENED,
        reach::SEALED,
        reach::RETIRED,
        reach::FAILED,
        reach::NO_TOKEN,
        reach::SHARED_DOWN,
    ]
    .to_vec();
    try_campaign(
        &walk(Shape::MANY, ControllerMutant::None, SEEDS, STEPS),
        &many,
    )
    .unwrap_or_else(|error| panic!("{error}"));
}

/// The walks reach every `probe!` of the controller role.
#[test]
fn the_walks_reach_every_probe_of_the_role() {
    try_sites(
        AssertKind::Reachable,
        || {
            for shape in [Shape::TRACED, Shape::WIDE, Shape::MANY] {
                walk(shape, ControllerMutant::None, 100, STEPS)
                    .try_run()
                    .map_err(|error| error.to_string())?;
            }
            Ok("the walks passed")
        },
        role::PROBES,
    )
    .unwrap_or_else(|error| panic!("{error}"));
}

fn caught(shape: Shape, mutant: ControllerMutant, oracle: &str) {
    let text = try_campaign(&walk(shape, mutant, SEEDS, STEPS), &[])
        .expect_err("a seed of the mutant fails");
    assert!(
        text.contains(oracle),
        "{mutant:?} failed on another check:\n{text}"
    );
}

#[test]
fn fenced_catches_a_write_over_a_stale_value() {
    caught(Shape::WIDE, ControllerMutant::WriteOnStale, FENCED);
}

#[test]
fn converges_catches_a_call_never_sent_again() {
    caught(Shape::TRACED, ControllerMutant::NoResend, CONVERGES);
}

#[test]
fn no_oscillation_catches_a_failure_forgotten() {
    caught(Shape::WIDE, ControllerMutant::ForgetDown, NO_OSCILLATION);
}

#[test]
fn cost_bound_catches_a_live_member_reported_down() {
    caught(Shape::WIDE, ControllerMutant::UpAsDown, COST_BOUND);
}

#[test]
fn bounded_reshape_rate_catches_no_interval() {
    caught(
        Shape::WIDE,
        ControllerMutant::NoInterval,
        BOUNDED_RESHAPE_RATE,
    );
}

#[test]
fn bounded_in_flight_catches_a_token_always_free() {
    caught(Shape::MANY, ControllerMutant::NoToken, BOUNDED_IN_FLIGHT);
}
