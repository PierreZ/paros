use super::*;

fn nodes(ids: &[u64]) -> Vec<NodeId> {
    ids.iter().copied().map(NodeId).collect()
}

fn grid(f: u8, columns: &[&[u64]]) -> Grid {
    Grid::new(f, columns.iter().map(|c| nodes(c)).collect()).expect("a v1 grid")
}

fn zone(name: &str) -> FailureDomain {
    FailureDomain::new("r", name)
}

fn zones(pairs: &[(u64, &str)]) -> BTreeMap<NodeId, FailureDomain> {
    pairs.iter().map(|(n, z)| (NodeId(*n), zone(z))).collect()
}

fn alive(up: &[u64], down: &[u64]) -> BTreeMap<NodeId, Liveness> {
    up.iter()
        .map(|n| (NodeId(*n), Liveness::Up))
        .chain(down.iter().map(|n| (NodeId(*n), Liveness::Down)))
        .collect()
}

#[test]
fn the_zone_rule_of_a_grid_is_the_rule_of_each_column() {
    let g = grid(1, &[&[1, 2, 3], &[4, 5, 6]]);
    assert_eq!(grid_zones(&g, &BTreeMap::new()), GridZones::Unlabeled);
    assert_eq!(
        grid_zones(&g, &zones(&[(1, "a")])),
        GridZones::PartlyLabeled(NodeId(2))
    );
    let rows = zones(&[(1, "a"), (2, "b"), (3, "c"), (4, "a"), (5, "b"), (6, "c")]);
    assert_eq!(grid_zones(&g, &rows), GridZones::Holds);
    // Two members of the second column in one zone: losing that zone
    // leaves the column one member, no majority.
    let two = zones(&[(1, "a"), (2, "b"), (3, "c"), (4, "a"), (5, "a"), (6, "c")]);
    assert!(matches!(
        grid_zones(&g, &two),
        GridZones::Breaks(ZoneRule::LosesQuorum(_))
    ));
}

#[test]
fn a_heal_replaces_only_the_members_that_are_down() {
    let g = grid(1, &[&[1, 2, 3], &[4, 5, 6]]);
    let live = alive(&[1, 3, 4, 5, 6, 7, 8], &[2]);
    let healed = heal_grid(&g, &live, &nodes(&[7, 8]), &BTreeMap::new()).expect("a placement");
    assert_eq!(healed.columns(), &[nodes(&[1, 7, 3]), nodes(&[4, 5, 6])]);
}

#[test]
fn a_heal_backtracks_to_meet_the_zone_rule() {
    let g = grid(1, &[&[1, 2, 3]]);
    let live = alive(&[1, 3, 7, 8], &[2]);
    // 7 is in zone a with 1: the first try commits alone, 8 in zone b holds.
    let labels = zones(&[(1, "a"), (2, "b"), (3, "c"), (7, "a"), (8, "b")]);
    let healed = heal_grid(&g, &live, &nodes(&[7, 8]), &labels).expect("a placement");
    assert_eq!(healed.columns(), &[nodes(&[1, 8, 3])]);
}

#[test]
fn a_lost_zone_of_three_has_no_placement() {
    let g = grid(1, &[&[1, 2, 3]]);
    let live = alive(&[1, 3, 7, 8], &[2]);
    let labels = zones(&[(1, "a"), (2, "b"), (3, "c"), (7, "a"), (8, "c")]);
    assert_eq!(heal_grid(&g, &live, &nodes(&[7, 8]), &labels), None);
}

#[test]
fn a_heal_takes_no_candidate_that_is_not_up_or_already_a_member() {
    let g = grid(1, &[&[1, 2, 3]]);
    let mut live = alive(&[1, 3, 8], &[2, 7]);
    live.insert(NodeId(9), Liveness::Suspect);
    assert_eq!(
        heal_grid(&g, &live, &nodes(&[7, 9, 1, 3]), &BTreeMap::new()),
        None
    );
    let healed = heal_grid(&g, &live, &nodes(&[7, 9, 8, 8]), &BTreeMap::new()).expect("8");
    assert_eq!(healed.columns(), &[nodes(&[1, 8, 3])]);
}

#[test]
fn a_heal_of_a_replica_set_keeps_its_order_and_size() {
    let live = alive(&[11, 13, 20, 21], &[12]);
    let none = BTreeMap::new();
    assert_eq!(
        heal_replicas(&nodes(&[11, 12, 13]), &live, &nodes(&[11, 20, 21]), &none),
        Some(nodes(&[11, 20, 13]))
    );
    assert_eq!(
        heal_replicas(&nodes(&[11, 12, 13]), &live, &nodes(&[13]), &none),
        None
    );
    assert_eq!(
        heal_replicas(&nodes(&[11, 13]), &live, &[], &none),
        Some(nodes(&[11, 13]))
    );
}

#[test]
fn a_heal_of_a_replica_set_meets_the_zone_rule() {
    let live = alive(&[11, 13, 20, 21], &[12]);
    let labels = zones(&[(11, "a"), (12, "b"), (13, "c"), (20, "a"), (21, "b")]);
    assert_eq!(
        heal_replicas(&nodes(&[11, 12, 13]), &live, &nodes(&[20, 21]), &labels),
        Some(nodes(&[11, 21, 13])),
        "the first candidate is in a zone that holds a replica already"
    );
    assert_eq!(
        heal_replicas(&nodes(&[11, 12, 13]), &live, &nodes(&[20]), &labels),
        None,
        "no candidate keeps one replica in each zone: the heal waits"
    );
    assert!(
        matches!(
            set_zones(&nodes(&[11, 20, 13]), &labels),
            GridZones::Breaks(_)
        ),
        "two replicas of three in one zone"
    );
    assert_eq!(set_zones(&nodes(&[11, 21, 13]), &labels), GridZones::Holds);
    assert_eq!(
        set_zones(&nodes(&[11, 22]), &labels),
        GridZones::PartlyLabeled(NodeId(22))
    );
    assert_eq!(set_zones(&nodes(&[30, 31]), &labels), GridZones::Unlabeled);
}
