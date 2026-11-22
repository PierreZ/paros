//! **The grid controller of one journal: its election, a heal, a lost reply
//! and a takeover.**
//!
//! Run it: `cargo run -p paros-journal-control --example controller`
//!
//! A [`Controller`] runs no register operation itself. It gives its driver a
//! [`Change`] to run on the journal's matchmaker register (an [`Ask`]), and
//! the driver gives back the [`Outcome`]. Here the driver is this file: it
//! applies each change to a [`RegisterValue`] by hand, and it builds each
//! [`View`] from signals that it owns. The register's guards decide whether
//! a change is safe; the controller only decides when to ask.
//!
//! # What the run shows
//!
//! 1. Controller C1 campaigns and wins term 1. The driver opens segment 1
//!    on the grid 1, 2, 3 (the bootstrap of the journal).
//! 2. With every member up, C1 plans nothing.
//! 3. Member 2 goes down. C1 plans the heal: a reshape call that puts the
//!    first candidate, 20, in the place of 2.
//! 4. The reply is lost. C1 gives the same call again, and the register
//!    answers it.
//! 5. Controller C2 campaigns and wins term 2. C1 reads a view of term 2
//!    and stops: a deposed controller plans nothing more.
//!
//! Each step asserts its result, so CI runs this example as a test. The
//! rules are in `docs/journal/06-controller.md` (§J6, §J6.1).

use std::collections::{BTreeMap, BTreeSet};

use paros_consensus::membership::MatchmakerGeneration;
use paros_journal::register::{Change, RegisterValue};
use paros_journal::{Grid, NodeId, OpeningCall, RELEASE, SegmentId};
use paros_journal_control::{
    Action, Ask, Controller, FailureDomain, Liveness, MatchmakerId, MatchmakerSet, Outcome,
    Standing, Tunables, View,
};

const C1: NodeId = NodeId(100);
const C2: NodeId = NodeId(200);

/// The numbers of §J6 are **Open**: the host gives them. These are the
/// example's own.
const TUNABLES: Tunables = Tunables {
    stale_after: 10,
    min_reshape_interval: 50,
    stall_window: 100,
};

/// The signals the driver gathers for a view: liveness, zones, candidates
/// and the matchmakers.
struct Signals {
    liveness: BTreeMap<NodeId, Liveness>,
    zones: BTreeMap<NodeId, FailureDomain>,
    candidates: Vec<NodeId>,
    matchmakers: MatchmakerSet,
    matchmakers_live: BTreeSet<MatchmakerId>,
    repairs: BTreeSet<SegmentId>,
}

impl Signals {
    /// Nodes 1 to 30 up but `down`, and three live matchmakers.
    fn with_down(down: &[u64]) -> Self {
        let ids: Vec<MatchmakerId> = (1..=3).map(MatchmakerId).collect();
        Self {
            liveness: (1..=30)
                .map(|n| {
                    let live = if down.contains(&n) {
                        Liveness::Down
                    } else {
                        Liveness::Up
                    };
                    (NodeId(n), live)
                })
                .collect(),
            zones: BTreeMap::new(),
            candidates: vec![NodeId(20), NodeId(21), NodeId(22)],
            matchmakers: MatchmakerSet::new(MatchmakerGeneration(0), ids.clone()),
            matchmakers_live: ids.into_iter().collect(),
            repairs: BTreeSet::new(),
        }
    }

    /// The view of `me` at `term`, on `register`, with fresh signals.
    fn view<'a>(&'a self, me: NodeId, term: u64, register: &'a RegisterValue) -> View<'a> {
        View {
            me,
            term,
            register,
            liveness: &self.liveness,
            zones: &self.zones,
            candidates: &self.candidates,
            leader_live: true,
            matchmakers: &self.matchmakers,
            matchmakers_live: &self.matchmakers_live,
            repairs_pending: &self.repairs,
            data_lost: false,
            signal_age: 0,
            since_last_reshape: None,
            seal_age: None,
            token_free: true,
        }
    }
}

/// The driver's one register operation: apply `change`, or give the refusal.
fn run(register: &mut RegisterValue, change: &Change) -> Outcome {
    match register.apply(change) {
        Ok(next) => {
            *register = next;
            Outcome::Done(register.clone())
        }
        Err(refusal) => Outcome::Refused(refusal),
    }
}

/// The grid of the newest segment of the register.
fn newest_grid(register: &RegisterValue) -> Vec<Vec<NodeId>> {
    let (_, record) = register.segments().iter().next_back().expect("a segment");
    record.opening().grid.columns().to_vec()
}

fn main() {
    let mut register = RegisterValue::new();

    // 1. C1 campaigns and wins term 1; the bootstrap opens segment 1.
    let mut c1 = Controller::new(C1);
    let elect = c1.campaign();
    assert_eq!(c1.standing(), &Standing::Campaigning);
    let outcome = run(&mut register, &elect);
    c1.elected(&outcome);
    assert_eq!(c1.term(), Some(1), "C1 holds term 1");
    println!("1. C1 holds term {}", c1.term().expect("a term"));
    let bootstrap = Change::Reshape {
        opening: OpeningCall {
            term: 1,
            expected: None,
            grid: Grid::new(1, vec![vec![NodeId(1), NodeId(2), NodeId(3)]]).expect("a v1 grid"),
            replicas: vec![NodeId(11), NodeId(12), NodeId(13)],
        },
        release: RELEASE,
    };
    assert!(matches!(run(&mut register, &bootstrap), Outcome::Done(_)));
    println!("   segment 1 opens on {:?}", newest_grid(&register));

    // 2. Every member up: nothing to do.
    let healthy = Signals::with_down(&[]);
    let (ask, decision) = c1.step(&healthy.view(C1, 1, &register), &TUNABLES);
    assert_eq!(ask, Ask::Nothing);
    assert_eq!(decision.map(|d| d.action), Some(Action::Idle));
    println!("2. every member up: C1 plans nothing");

    // 3. Member 2 down: the heal is a reshape call.
    let degraded = Signals::with_down(&[2]);
    let (ask, _) = c1.step(&degraded.view(C1, 1, &register), &TUNABLES);
    let Ask::Call(call) = ask else {
        panic!("a heal is a reshape call, got {ask:?}");
    };
    let Change::Reshape { opening, .. } = &call else {
        panic!("a heal is a reshape, got {call:?}");
    };
    let opening = opening.clone();
    assert_eq!(
        opening.grid.columns(),
        [vec![NodeId(1), NodeId(20), NodeId(3)]],
        "20 takes the place of 2; the survivors stay"
    );
    println!(
        "3. member 2 down: C1 calls reshape to {:?}",
        opening.grid.columns()
    );

    // 4. The reply is lost: the same call again, and the register answers.
    let _lost = run(&mut register, &call);
    c1.reshaped(&opening, &Outcome::NoAnswer);
    let (again, _) = c1.step(&degraded.view(C1, 1, &register), &TUNABLES);
    assert_eq!(again, Ask::Call(call.clone()), "the call in flight, again");
    let outcome = run(&mut register, &call);
    assert!(
        matches!(outcome, Outcome::Done(_)),
        "the register answers a call it took with ok"
    );
    c1.reshaped(&opening, &outcome);
    assert_eq!(c1.call(), None, "no call in flight");
    println!(
        "4. reply lost; sent again and answered: segment 2 on {:?}",
        newest_grid(&register)
    );

    // 5. C2 wins term 2; C1 reads it and stops.
    let mut c2 = Controller::new(C2);
    let elect = c2.campaign();
    let outcome = run(&mut register, &elect);
    c2.elected(&outcome);
    assert_eq!(c2.term(), Some(2), "C2 holds term 2");
    let (ask, decision) = c1.step(&degraded.view(C1, 1, &register), &TUNABLES);
    assert_eq!(ask, Ask::Nothing);
    assert_eq!(decision.map(|d| d.action), Some(Action::Stop));
    assert_eq!(
        c1.standing(),
        &Standing::Idle,
        "a deposed controller is idle"
    );
    println!("5. C2 holds term 2; C1 reads it and stops");
}
