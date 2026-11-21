//! The roles walks: every seed passes every oracle, the campaign reaches
//! every site, and each oracle catches its mutant.

use journal_sim::campaign::{campaign, try_campaign};
use journal_sim::roles::{
    AGREE, BOUNDARY, FOLD, RESOLVE, RolesMutant, SEALED, Shape, VIEW, reach, walk,
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

fn caught(mutant: RolesMutant, oracle: &str) {
    let text = try_campaign(&walk(Shape::NARROW, mutant, 300, 400), &[])
        .expect_err("a seed of the mutant fails");
    assert!(
        text.contains(oracle),
        "{mutant:?} failed on another check:\n{text}"
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
