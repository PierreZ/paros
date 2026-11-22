use super::*;

fn nodes(ids: &[u64]) -> Vec<NodeId> {
    ids.iter().copied().map(NodeId).collect()
}

fn grid(f: u8, columns: &[&[u64]]) -> Grid {
    Grid::new(f, columns.iter().map(|c| nodes(c)).collect()).expect("a v1 grid")
}

/// Every subset of `members`.
fn subsets(members: &[NodeId]) -> Vec<BTreeSet<NodeId>> {
    (0..1u32 << members.len())
        .map(|bits| {
            members
                .iter()
                .enumerate()
                .filter(|(i, _)| bits & (1 << i) != 0)
                .map(|(_, m)| *m)
                .collect()
        })
        .collect()
}

#[test]
fn a_spare_is_the_live_count_minus_a_majority_of_the_column() {
    for g in [
        grid(1, &[&[1, 2, 3]]),
        grid(2, &[&[1, 2, 3, 4, 5]]),
        grid(1, &[&[1, 2, 3], &[4, 5, 6]]),
    ] {
        let members: Vec<NodeId> = g.columns().iter().flatten().copied().collect();
        for live in subsets(&members) {
            let spares = column_spares(&g, &live);
            for (column, spare) in g.columns().iter().zip(&spares) {
                let count = column.iter().filter(|m| live.contains(m)).count();
                let expected = i64::try_from(count).unwrap() - i64::from(g.f()) - 1;
                assert_eq!(spare.spare(), expected, "{live:?}");
            }
        }
    }
}

#[test]
fn the_states_follow_the_table_of_j6_2() {
    let g = grid(1, &[&[1, 2, 3], &[4, 5, 6]]);
    let members: Vec<NodeId> = g.columns().iter().flatten().copied().collect();
    let mut seen = BTreeSet::new();
    for live in subsets(&members) {
        let per_column: Vec<usize> = g
            .columns()
            .iter()
            .map(|c| c.iter().filter(|m| live.contains(m)).count())
            .collect();
        for leader in [false, true] {
            for placeable in [false, true] {
                let state = grid_state(&g, &live, leader, placeable);
                seen.insert(state);
                let expected = if per_column.iter().all(|n| *n == 3) {
                    GridState::Healthy
                } else if per_column.iter().all(|n| *n >= 2) {
                    if placeable {
                        GridState::AtRisk
                    } else {
                        GridState::AtRiskUnplaceable
                    }
                } else if per_column.iter().all(|n| *n < 2) {
                    GridState::Stopped
                } else if leader {
                    GridState::WritesDegraded
                } else {
                    GridState::ElectionBlocked
                };
                assert_eq!(state, expected, "{live:?} {leader} {placeable}");
            }
        }
    }
    assert_eq!(seen.len(), 6, "every v1 state is reached");
}

#[test]
fn electable_and_writable_ask_the_column_quorums() {
    let g = grid(1, &[&[1, 2, 3], &[4, 5, 6]]);
    let live: BTreeSet<NodeId> = nodes(&[1, 2, 4]).into_iter().collect();
    assert!(!electable(&g, &live));
    assert!(writable(&g, &live));
    let live: BTreeSet<NodeId> = nodes(&[1, 2, 4, 5]).into_iter().collect();
    assert!(electable(&g, &live));
    assert!(writable(&g, &BTreeSet::from([NodeId(1), NodeId(2)])));
    assert!(!writable(&g, &BTreeSet::from([NodeId(1), NodeId(4)])));
}
