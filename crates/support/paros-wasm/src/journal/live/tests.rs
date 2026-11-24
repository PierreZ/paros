//! The page's scenario, played as the reader would: the same steps as the
//! facade example `full_journal`.

use super::{Live, Move};

/// Step until `done` holds of the view, at most `ticks` ticks.
fn until(live: &mut Live, ticks: u32, what: &str, done: impl Fn(&super::View) -> bool) {
    for _ in 0..ticks {
        if done(&live.view()) {
            return;
        }
        live.step(1);
    }
    panic!(
        "{what}: not reached in {ticks} ticks; events: {:#?}",
        live.view().events
    );
}

fn leader_writes(view: &super::View, segment: u64) -> bool {
    view.hosts.iter().flat_map(|h| &h.roles).any(|r| {
        r.name == format!("leader L{segment}")
            && r.detail.as_deref().is_some_and(|d| d.starts_with("writes"))
    })
}

#[test]
fn the_page_scenario_runs() {
    let mut live = Live::new();
    until(&mut live, 400, "L1 writes", |v| leader_writes(v, 1));
    until(&mut live, 400, "SetLeader", |v| !v.busy);
    for _ in 0..3 {
        live.act(&Move::Append);
    }
    until(&mut live, 400, "three writes", |v| v.written == 3);
    live.act(&Move::Read);
    until(&mut live, 400, "a read", |v| v.read.is_some());
    assert_eq!(
        live.view().read,
        Some(vec![
            "record 0".into(),
            "record 1".into(),
            "record 2".into()
        ])
    );

    live.act(&Move::Crash { host: 3 });
    live.act(&Move::Append);
    until(&mut live, 400, "a write with host 3 down", |v| {
        v.written == 4
    });
    live.act(&Move::Restart { host: 3 });
    until(&mut live, 400, "host 3 serves again", |v| {
        v.hosts[2].roles.iter().all(|r| r.detail.is_some())
    });

    live.act(&Move::Roll);
    until(&mut live, 600, "L2 writes", |v| leader_writes(v, 2));
    let view = live.view();
    assert_eq!(
        view.segments[0].state,
        "sealed".to_string(),
        "{:#?}",
        view.segments
    );
    live.act(&Move::Append);
    until(&mut live, 400, "a write in segment 2", |v| v.written == 5);
    until(&mut live, 400, "segment 1 retires", |v| {
        v.segments[0].state == "retired"
    });

    live.act(&Move::Read);
    until(&mut live, 600, "a whole read", |v| {
        !v.reading && v.read.as_ref().is_some_and(|r| r.len() == 5)
    });
    let read = live.view().read.expect("a read");
    assert_eq!(
        read,
        (0..5).map(|i| format!("record {i}")).collect::<Vec<_>>()
    );

    // Segment 1 is retired and segment 2 in force: the register opens
    // segment 3. A second roll at once is refused: segment 2 is not
    // retired (and segment 3 not in force) until L3 seals segment 2.
    live.act(&Move::Roll);
    assert!(
        live.view()
            .events
            .iter()
            .take(2)
            .any(|e| e.ends_with("The controller opens segment 3.")),
        "{:#?}",
        live.view().events
    );
    live.act(&Move::Roll);
    assert!(
        live.view()
            .events
            .iter()
            .take(2)
            .any(|e| e.ends_with("Roll refused by the register: Unretired(SegmentId(2)).")),
        "{:#?}",
        live.view().events
    );
}

/// The register's value of the view: the segments and their states.
fn segments(view: &super::View) -> Vec<(u64, String)> {
    view.segments
        .iter()
        .map(|s| (s.id, s.state.clone()))
        .collect()
}

#[test]
fn a_crash_of_matchmaker_2_keeps_the_register() {
    let mut live = Live::new();
    until(&mut live, 400, "L1 writes", |v| leader_writes(v, 1));
    until(&mut live, 400, "SetLeader", |v| !v.busy);
    // Host 2 runs matchmaker 2 and acceptor 2. Matchmakers 1 and 3 are a
    // majority, and matchmaker 3 may hold no write yet.
    live.act(&Move::Crash { host: 2 });
    assert!(live.view().register, "{:#?}", live.view().matchmakers);
    assert_eq!(segments(&live.view()), vec![(1, "in force".to_string())]);
    for _ in 0..2 {
        live.act(&Move::Append);
        until(&mut live, 400, "a write with host 2 down", |v| !v.busy);
    }
    assert_eq!(live.view().written, 2);
    live.act(&Move::Read);
    until(&mut live, 400, "a read with host 2 down", |v| !v.reading);
    assert_eq!(live.view().read.map(|r| r.len()), Some(2));
}

#[test]
fn a_roll_moves_the_writes_off_a_crashed_leader() {
    let mut live = Live::new();
    until(&mut live, 400, "L1 writes", |v| leader_writes(v, 1));
    until(&mut live, 400, "SetLeader", |v| !v.busy);
    live.act(&Move::Append);
    until(&mut live, 400, "a first write", |v| v.written == 1);
    // Host 1 runs L1, matchmaker 1, acceptor 1 and replica 11.
    live.act(&Move::Crash { host: 1 });
    assert!(live.view().register, "{:#?}", live.view().matchmakers);
    live.act(&Move::Append);
    live.step(200);
    assert_eq!(live.view().written, 1, "the leader of segment 1 is down");
    // The roll places L2 on host 2: it seals segment 1 and writes.
    live.act(&Move::Roll);
    until(&mut live, 600, "L2 writes", |v| leader_writes(v, 2));
    until(&mut live, 600, "the write that waited", |v| v.written == 2);
    live.act(&Move::Read);
    until(&mut live, 600, "a whole read", |v| !v.reading);
    assert_eq!(
        live.view().read,
        Some(vec!["record 0".to_string(), "record 1".to_string()])
    );
}

#[test]
fn a_write_waits_while_no_column_holds_a_quorum() {
    let mut live = Live::new();
    until(&mut live, 400, "L1 writes", |v| leader_writes(v, 1));
    until(&mut live, 400, "SetLeader", |v| !v.busy);
    // Both columns lose two acceptors: no column holds F + 1. Two of the
    // three replicas of segment 1 and two matchmakers are down too.
    for host in [2, 3, 5, 6] {
        live.act(&Move::Crash { host });
    }
    live.act(&Move::Append);
    live.step(200);
    assert_eq!(live.view().written, 0, "no column has F + 1 acceptors up");
    // Hosts 3 and 5 bring back one acceptor of each column (each column
    // holds F + 1 again, whichever the leader proposed the write in),
    // replicas 12 and 13, and matchmaker 3 (a majority with matchmaker 1).
    live.act(&Move::Restart { host: 3 });
    live.act(&Move::Restart { host: 5 });
    until(&mut live, 600, "the write after a restart", |v| {
        v.written == 1
    });
}

#[test]
fn every_move_on_every_host_trips_no_assertion() {
    // A short walk over the moves, the same each run.
    let moves = [
        Move::Append,
        Move::Crash { host: 1 },
        Move::Append,
        Move::Read,
        Move::Restart { host: 1 },
        Move::Roll,
        Move::Crash { host: 2 },
        Move::Append,
        Move::Restart { host: 2 },
        Move::Crash { host: 4 },
        Move::Roll,
        Move::Read,
        Move::Restart { host: 4 },
        Move::Append,
    ];
    let mut live = Live::new();
    for chosen in &moves {
        live.act(chosen);
        live.step(40);
    }
    let _ = live.view();
}
