//! **The driver of a journal's controller: signals, a heal and a takeover.**
//!
//! Run it: `cargo run -p paros-journal-control --example driver`
//!
//! A [`ControlDriver`] runs the [`Controller`](paros_journal_control::Controller)
//! of one journal. The program gives it three things: the time of each call
//! (the driver reads no clock), the [`Report`]s that the processes of the
//! journal publish, and two ports. [`ControlRegister`] reaches the
//! journal's matchmaker register; [`Cell`] is the host's placement and the
//! cell's rate limiter. Here both ports are written by hand in this file.
//!
//! # What the run shows
//!
//! 1. The journal was created with no election: segment 1 on the grid 1,
//!    2, 3, with the replicas 11, 12, 13. Controller C1 finds no holder,
//!    campaigns and wins term 1. Controller C2 sees a live holder and waits.
//! 2. Process 2 stops reporting. It is suspect, then down. Once the
//!    minimum interval has passed, C1 heals: a reshape that puts the first
//!    candidate, 20, in the place of 2. The journal holds the cell's token
//!    while the new segment is pending.
//! 3. The process of C1 stops reporting. C2 takes the term once C1 is
//!    down.
//!
//! Each step asserts its result, so CI runs this example as a test. The
//! rules are in `docs/journal/06-controller.md` (§J6.2, "The driver as
//! built").

use std::collections::BTreeSet;

use paros_consensus::membership::MatchmakerGeneration;
use paros_journal::register::{Change, RegisterValue};
use paros_journal::{Grid, NodeId, OpeningCall, RELEASE};
use paros_journal_control::{
    Action, Cell, ControlConfig, ControlDriver, ControlRegister, FailureDomain, MatchmakerId,
    MatchmakerSet, Outcome, ProcessId, Report, Tunables, Windows,
};

const C1: NodeId = NodeId(100);
const C2: NodeId = NodeId(200);
/// The host's numbers, in its unit of time (each is **Open** in §J6).
const CONFIG: ControlConfig = ControlConfig {
    tunables: Tunables {
        stale_after: 20,
        min_reshape_interval: 30,
        stall_window: 1_000,
    },
    windows: Windows {
        suspect_after: 5,
        down_after: 15,
    },
};

/// The journal's register: one value in memory.
struct Register(RegisterValue);

impl ControlRegister for Register {
    fn read(&mut self) -> Option<RegisterValue> {
        Some(self.0.clone())
    }

    fn run(&mut self, change: &Change) -> Outcome {
        match self.0.apply(change) {
            Ok(value) => {
                self.0 = value.clone();
                Outcome::Done(value)
            }
            Err(refusal) => Outcome::Refused(refusal),
        }
    }
}

/// The placement: node `n` runs on process `n`; three candidates; one token.
struct Placement {
    token: bool,
}

impl Cell for Placement {
    fn process_of(&self, node: NodeId) -> Option<ProcessId> {
        Some(ProcessId(node.0))
    }

    fn zone(&self, _process: ProcessId) -> Option<FailureDomain> {
        None
    }

    fn candidates(&mut self, _value: &RegisterValue) -> Vec<NodeId> {
        vec![NodeId(20), NodeId(21), NodeId(22)]
    }

    fn matchmakers(&self) -> (MatchmakerSet, BTreeSet<MatchmakerId>) {
        let ids: Vec<MatchmakerId> = (1..=3).map(MatchmakerId).collect();
        (
            MatchmakerSet::new(MatchmakerGeneration(0), ids.clone()),
            ids.into_iter().collect(),
        )
    }

    fn token_free(&self) -> bool {
        true
    }

    fn take_token(&mut self) {
        self.token = true;
    }

    fn give_back(&mut self) {
        self.token = false;
    }
}

/// Every process but `silent` reports to both drivers at `now`.
fn report(drivers: &mut [&mut ControlDriver], now: u64, silent: &[u64]) {
    for process in [1, 2, 3, 11, 12, 13, 20, 21, 22, C1.0, C2.0] {
        if silent.contains(&process) {
            continue;
        }
        for driver in drivers.iter_mut() {
            driver.publish(
                now,
                Report::Beat {
                    process: ProcessId(process),
                },
            );
        }
    }
}

fn main() {
    let first = OpeningCall {
        term: 0,
        expected: None,
        grid: Grid::new(1, vec![vec![NodeId(1), NodeId(2), NodeId(3)]]).expect("a v1 grid"),
        replicas: vec![NodeId(11), NodeId(12), NodeId(13)],
    };
    let created = RegisterValue::new()
        .apply(&Change::Reshape {
            opening: first,
            release: RELEASE,
        })
        .expect("the journal's creation opens segment 1");
    let mut register = Register(created);
    let mut cell = Placement { token: false };
    let mut c1 = ControlDriver::new(C1, ProcessId(C1.0), CONFIG, 0);
    let mut c2 = ControlDriver::new(C2, ProcessId(C2.0), CONFIG, 0);

    // 1. No holder: C1 campaigns and wins term 1; C2 sees it live.
    report(&mut [&mut c1, &mut c2], 0, &[]);
    let polled = c1.poll(0, &mut register, &mut cell);
    assert_eq!(c1.controller().term(), Some(1));
    assert_eq!(polled.decision.map(|d| d.action), Some(Action::Idle));
    c2.poll(0, &mut register, &mut cell);
    assert_eq!(c2.controller().term(), None, "the holder is live");
    println!("1. C1 holds term 1; C2 waits");

    // 2. Process 2 goes silent: down at 15, healed once the minimum interval
    //    since the driver first saw segment 1 (at 0) has passed.
    let mut healed = None;
    for now in (5..=40).step_by(5) {
        report(&mut [&mut c1, &mut c2], now, &[2]);
        let polled = c1.poll(now, &mut register, &mut cell);
        if let Some(Action::Reshape(opening)) = polled.decision.map(|d| d.action) {
            healed = Some((now, opening));
            break;
        }
    }
    let (at, opening) = healed.expect("a heal");
    assert_eq!(at, 30, "the heal waits for the minimum interval");
    assert_eq!(
        opening.grid.columns(),
        &[vec![NodeId(1), NodeId(20), NodeId(3)]]
    );
    assert!(
        cell.token,
        "the journal holds the token while segment 2 is pending"
    );
    println!(
        "2. at {at}, C1 heals the grid to {:?}",
        opening.grid.columns()
    );

    // 3. C1's process goes silent: C2 takes the term once C1 is down.
    let mut takeover = None;
    for now in (35..=60).step_by(5) {
        report(&mut [&mut c2], now, &[2, C1.0]);
        c2.poll(now, &mut register, &mut cell);
        if c2.controller().term().is_some() {
            takeover = Some(now);
            break;
        }
    }
    assert_eq!(takeover, Some(45), "C1 was last heard at 30, down at 45");
    assert_eq!(c2.controller().term(), Some(2));
    println!("3. at 45, C2 takes term 2 from C1");
}
