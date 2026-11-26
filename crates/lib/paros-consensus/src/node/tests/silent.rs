//! The observe-only evidence of a silent member and of a lost window: the
//! missed `CheckQuorum` windows of each member, the `QuorumWindowLost` flag,
//! and the `ticks` and `is_settled` accessors that a journal controller
//! reads (#1564 (journal controller, version 1)).

use super::*;

/// Tick the leader `nodes[0]` once and deliver its beats to every node but
/// `cut`, so the window closes held without the ack of `cut`.
fn beat_without(nodes: &mut [ColocatedNode], cut: Option<NodeId>) {
    nodes[0].tick();
    let q = drain(&mut nodes[0]);
    deliver_filtered(nodes, q, |to, _| Some(to) != cut);
}

#[test]
fn a_leader_counts_the_windows_a_silent_member_missed() {
    let mut nodes = cluster::<3>();
    make_leader(&mut nodes, 0);
    nodes[0].set_election_timeout(2);
    // Let the windows that node 2 may still have acked close first.
    for _ in 0..2 {
        beat_without(&mut nodes, Some(NodeId(2)));
    }
    let held_before = nodes[0].counters().quorum_windows_held;
    let missed_before = nodes[0]
        .missed_windows()
        .iter()
        .find(|(id, _)| *id == NodeId(2))
        .map_or(0, |(_, w)| *w);
    // Node 2 hears nothing: each window closes with the ack of node 1 only.
    for _ in 0..6 {
        beat_without(&mut nodes, Some(NodeId(2)));
        assert!(nodes[0].is_leader(), "a quorum still acks the leader");
    }
    let missed = nodes[0].missed_windows();
    assert_eq!(missed.len(), 1, "only node 2 missed: {missed:?}");
    let (who, windows) = missed[0];
    assert_eq!(who, NodeId(2));
    let closed = nodes[0].counters().quorum_windows_held - held_before;
    assert!(closed >= 2, "windows closed: {closed}");
    assert_eq!(
        windows - missed_before,
        closed,
        "node 2 missed exactly each window that closed"
    );
    // Node 2 is back: its next ack forgets the count.
    for _ in 0..4 {
        beat_without(&mut nodes, None);
    }
    assert!(
        nodes[0].missed_windows().is_empty(),
        "an ack forgets the count: {:?}",
        nodes[0].missed_windows()
    );
    // A follower reports no count: the count is the leader's.
    assert!(
        nodes[1].missed_windows().is_empty(),
        "a follower counts no missed window"
    );
}

#[test]
fn a_leader_that_loses_its_window_says_so_until_it_leads_again() {
    let mut nodes = cluster::<3>();
    make_leader(&mut nodes, 0);
    assert!(!nodes[0].quorum_window_lost());
    nodes[0].set_election_timeout(3);
    for _ in 0..10 {
        nodes[0].tick();
        let _ = drain(&mut nodes[0]);
        if !nodes[0].is_leader() {
            break;
        }
    }
    assert!(!nodes[0].is_leader(), "an isolated leader steps down");
    assert!(nodes[0].quorum_window_lost(), "it reports the lost window");
    assert!(
        nodes[0].missed_windows().is_empty(),
        "the count died with it"
    );
    assert!(!nodes[1].quorum_window_lost(), "a follower lost no window");
    // It wins again: the flag is cleared.
    make_leader(&mut nodes, 0);
    assert!(!nodes[0].quorum_window_lost(), "a new leadership clears it");
}

#[test]
fn a_leader_that_steps_down_for_another_reason_lost_no_window() {
    let mut nodes = cluster::<3>();
    make_leader(&mut nodes, 0);
    nodes[0].set_election_timeout(2);
    for _ in 0..4 {
        beat_without(&mut nodes, Some(NodeId(2)));
    }
    assert!(
        !nodes[0].missed_windows().is_empty(),
        "node 2 missed windows"
    );
    nodes[0].step_down();
    assert!(!nodes[0].is_leader(), "the leader stepped down");
    assert!(
        !nodes[0].quorum_window_lost(),
        "a step-down that no lost window caused sets no flag"
    );
    // A new leadership starts with no count: the count died with the old.
    make_leader(&mut nodes, 0);
    assert!(
        nodes[0].missed_windows().is_empty(),
        "a new leadership inherits no count"
    );
}

#[test]
fn ticks_count_each_tick_and_only_a_settled_leader_is_settled() {
    let mut nodes = cluster::<3>();
    let before = nodes[1].ticks();
    nodes[1].tick();
    nodes[1].tick();
    assert_eq!(nodes[1].ticks(), before + 2);
    assert!(!nodes[1].is_settled(), "a follower is not a settled leader");
    make_leader(&mut nodes, 0);
    assert!(nodes[0].is_settled(), "an empty recovery leaves it settled");
    assert!(!nodes[1].is_settled());
}
