//! The roles walks: every seed passes every oracle, the campaign reaches
//! every site, and each oracle catches its mutant.

use journal_sim::campaign::campaign;
use journal_sim::roles::{
    AGREE, BOUNDARY, COMPACT, FLOOR, FOLD, PROXY, READ, REPAIRED, RESOLVE, RETIRE, RolesMutant,
    SEALED, STAMPED, Shape, TRIM, VIEW, reach, walk,
};

/// The seeds of each walk in CI.
const SEEDS: u64 = 200;

/// One column, three acceptors: the default grid.
#[test]
fn the_narrow_roles_walk() {
    campaign(
        &walk(Shape::NARROW, RolesMutant::None, SEEDS, 3000),
        reach::ALL,
    );
}

/// Two columns and promises in pages.
#[test]
fn the_wide_roles_walk() {
    let sites: Vec<&str> = reach::ALL.iter().chain(reach::WIDE).copied().collect();
    campaign(&walk(Shape::WIDE, RolesMutant::None, SEEDS, 3000), &sites);
}

/// Some seed of `mutant` fails on `oracle`. A planted bug can break more
/// than one oracle; each seed stops at the first it breaks.
fn caught(mutant: RolesMutant, oracle: &str) {
    let walk = walk(Shape::NARROW, mutant, 1, 1000);
    let mut failures = Vec::new();
    for seed in 1..=300 {
        if let Err(failure) = walk.replay(seed) {
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
fn agree_catches_an_acceptor_that_loses_its_disk() {
    caught(RolesMutant::WipeDisk, AGREE);
}

#[test]
fn sealed_catches_a_leader_that_binds_a_stale_grid() {
    caught(RolesMutant::StaleBinding, SEALED);
}

#[test]
fn fold_catches_a_duplicate_answered_taken() {
    caught(RolesMutant::DuplicateAsTaken, FOLD);
}

#[test]
fn view_catches_a_replica_that_takes_a_stale_base() {
    caught(RolesMutant::StaleBase, VIEW);
}

#[test]
fn boundary_catches_a_stale_boundary_state() {
    caught(RolesMutant::StaleBoundary, BOUNDARY);
}

#[test]
fn resolve_catches_a_resolve_at_the_segment_in_force() {
    caught(RolesMutant::ResolveInForce, RESOLVE);
}

#[test]
fn retire_catches_a_leader_that_counts_a_lagging_replica() {
    caught(RolesMutant::HoldsLie, RETIRE);
}

#[test]
fn proxy_catches_a_proxy_that_counts_forged_votes() {
    caught(RolesMutant::ForgedAccepted, PROXY);
}

#[test]
fn read_catches_a_read_with_no_fence() {
    caught(RolesMutant::ReadNoFence, READ);
}

#[test]
fn trim_catches_a_trim_on_the_intent_to_truncate() {
    caught(RolesMutant::TrimIntent, TRIM);
}

#[test]
fn floor_catches_a_reader_that_takes_the_floor_of_the_holder() {
    caught(RolesMutant::FloorFromHolder, FLOOR);
}

#[test]
fn compact_catches_a_leader_that_drops_votes_on_one_report() {
    caught(RolesMutant::ForgedCompacted, COMPACT);
}

#[test]
fn agree_catches_a_phase_one_that_hides_dropped_votes() {
    caught(RolesMutant::DroppedHidden, AGREE);
}

#[test]
fn stamped_catches_an_accept_that_lost_its_leader_time() {
    caught(RolesMutant::UnstampedAccept, STAMPED);
}

#[test]
fn repaired_catches_a_repair_that_writes_another_command() {
    caught(RolesMutant::RepairForged, REPAIRED);
}
