use std::collections::{BTreeMap, BTreeSet};

use paros_consensus::membership::MatchmakerGeneration;
use paros_journal::register::ChangeRefusal;
use paros_journal::segment::OpeningCall;
use paros_journal::{BoundaryState, Grid, Leader, RELEASE, Slot};

use super::*;
use crate::decide::{Action, Alert, Wait};

const ME: NodeId = NodeId(100);
const OTHER: NodeId = NodeId(200);
const CONFIG: ControlConfig = ControlConfig {
    tunables: Tunables {
        stale_after: 40,
        min_reshape_interval: 50,
        stall_window: 1_000,
    },
    windows: Windows {
        suspect_after: 10,
        down_after: 30,
    },
};

fn nodes(ids: &[u64]) -> Vec<NodeId> {
    ids.iter().copied().map(NodeId).collect()
}

fn apply(value: &RegisterValue, change: &Change) -> RegisterValue {
    value.apply(change).expect("the register takes the change")
}

/// Segment 1 on 1, 2, 3 with replicas 11, 12, 13, opened by `holder` in
/// term 1.
fn one_segment(holder: NodeId) -> RegisterValue {
    let value = apply(&RegisterValue::new(), &Change::Elect { holder });
    apply(
        &value,
        &Change::Reshape {
            opening: OpeningCall {
                term: 1,
                expected: None,
                grid: Grid::new(1, vec![nodes(&[1, 2, 3])]).expect("a v1 grid"),
                replicas: nodes(&[11, 12, 13]),
            },
            release: RELEASE,
        },
    )
}

/// `one_segment`, then segment 2 on 4, 5, 6 with replicas 14, 15, 16, and
/// segment 1 sealed, with its boundary, and retired.
fn retired(holder: NodeId) -> RegisterValue {
    let value = apply(
        &one_segment(holder),
        &Change::Reshape {
            opening: OpeningCall {
                term: 1,
                expected: Some(SegmentId(1)),
                grid: Grid::new(1, vec![nodes(&[4, 5, 6])]).expect("a v1 grid"),
                replicas: nodes(&[14, 15, 16]),
            },
            release: RELEASE,
        },
    );
    [
        Change::Seal {
            segment: SegmentId(1),
            end: Slot(3),
        },
        Change::Boundary {
            segment: SegmentId(1),
            state: BoundaryState::fresh(Leader::Unset),
        },
        Change::Retire {
            segment: SegmentId(1),
            holders: 2,
            unrepaired_damage: false,
        },
    ]
    .iter()
    .fold(value, |value, change| apply(&value, change))
}

/// A register in memory that may lose the answer of the next operations.
struct Register {
    value: Option<RegisterValue>,
    /// Answers to lose: the change applies, the answer is lost.
    lose: usize,
    ran: Vec<Change>,
}

impl Register {
    fn new(value: RegisterValue) -> Self {
        Self {
            value: Some(value),
            lose: 0,
            ran: Vec::new(),
        }
    }
}

impl ControlRegister for Register {
    fn read(&mut self) -> Option<RegisterValue> {
        self.value.clone()
    }

    fn run(&mut self, change: &Change) -> Outcome {
        self.ran.push(change.clone());
        let value = self.value.as_ref().expect("a register");
        let outcome = match value.apply(change) {
            Ok(next) => {
                self.value = Some(next.clone());
                Outcome::Done(next)
            }
            Err(refusal) => Outcome::Refused(refusal),
        };
        if self.lose > 0 {
            self.lose -= 1;
            return Outcome::NoAnswer;
        }
        outcome
    }
}

/// A cell where node `n` runs on process `n`, with one token.
struct TestCell {
    candidates: Vec<NodeId>,
    zones: BTreeMap<ProcessId, FailureDomain>,
    token_free: bool,
    held: bool,
    takes: usize,
    gives: usize,
    unknown: BTreeSet<NodeId>,
}

impl TestCell {
    fn new() -> Self {
        Self {
            candidates: nodes(&[20, 21, 22]),
            zones: BTreeMap::new(),
            token_free: true,
            held: false,
            takes: 0,
            gives: 0,
            unknown: BTreeSet::new(),
        }
    }
}

impl Cell for TestCell {
    fn process_of(&self, node: NodeId) -> Option<ProcessId> {
        (!self.unknown.contains(&node)).then_some(ProcessId(node.0))
    }

    fn zone(&self, process: ProcessId) -> Option<FailureDomain> {
        self.zones.get(&process).cloned()
    }

    fn candidates(&mut self, _value: &RegisterValue) -> Vec<NodeId> {
        self.candidates.clone()
    }

    fn matchmakers(&self) -> (MatchmakerSet, BTreeSet<MatchmakerId>) {
        let ids: Vec<MatchmakerId> = (1..=3).map(MatchmakerId).collect();
        (
            MatchmakerSet::new(MatchmakerGeneration(0), ids.clone()),
            ids.into_iter().collect(),
        )
    }

    fn token_free(&self) -> bool {
        self.held || self.token_free
    }

    fn take_token(&mut self) {
        assert!(self.token_free(), "a token is taken only when free");
        self.held = true;
        self.takes += 1;
    }

    fn give_back(&mut self) {
        self.held = false;
        self.gives += 1;
    }
}

fn driver(now: u64) -> ControlDriver {
    ControlDriver::new(ME, ProcessId(ME.0), CONFIG, now)
}

/// Every process of 1..=30 but `down`, and both controllers, report at
/// `now`.
fn beat_all(driver: &mut ControlDriver, now: u64, down: &[u64]) {
    for n in (1..=30).chain([ME.0, OTHER.0]) {
        if !down.contains(&n) {
            driver.publish(
                now,
                Report::Beat {
                    process: ProcessId(n),
                },
            );
        }
    }
}

#[test]
fn a_driver_with_no_holder_campaigns_and_a_healthy_journal_asks_nothing() {
    let mut register = Register::new(RegisterValue::new());
    let mut cell = TestCell::new();
    let mut d = driver(0);
    let polled = d.poll(0, &mut register, &mut cell);
    assert_eq!(register.ran, vec![Change::Elect { holder: ME }]);
    assert_eq!(d.controller().term(), Some(1));
    // An empty register: the journal's creation opens the first segment.
    assert_eq!(
        polled.decision.map(|d| d.action),
        Some(Action::Wait(Wait::NoSegment))
    );
    assert_eq!(polled.sends, Vec::<Ask>::new());
    assert_eq!((cell.takes, cell.gives), (0, 1));
}

#[test]
fn a_restarted_holder_campaigns_again() {
    let mut register = Register::new(one_segment(ME));
    let mut cell = TestCell::new();
    let mut d = driver(0);
    beat_all(&mut d, 0, &[]);
    d.poll(0, &mut register, &mut cell);
    assert_eq!(register.ran, vec![Change::Elect { holder: ME }]);
    assert_eq!(d.controller().term(), Some(2));
}

#[test]
fn a_live_holder_keeps_its_term_and_a_down_one_is_taken_over() {
    let mut register = Register::new(one_segment(OTHER));
    let mut cell = TestCell::new();
    let mut d = driver(0);
    // The holder is up, then suspect: no campaign.
    for now in [0, 15, 29] {
        beat_all(&mut d, now, &[OTHER.0]);
        if now == 0 {
            d.publish(
                0,
                Report::Beat {
                    process: ProcessId(OTHER.0),
                },
            );
        }
        let polled = d.poll(now, &mut register, &mut cell);
        assert_eq!(polled, Polled::default());
        assert_eq!(register.ran, Vec::<Change>::new());
    }
    // Not heard for the down window: a takeover.
    beat_all(&mut d, 30, &[OTHER.0]);
    d.poll(30, &mut register, &mut cell);
    assert_eq!(register.ran, vec![Change::Elect { holder: ME }]);
    assert_eq!(d.controller().term(), Some(2));
}

#[test]
fn a_holder_with_no_known_process_is_taken_over() {
    let mut register = Register::new(one_segment(OTHER));
    let mut cell = TestCell::new();
    cell.unknown.insert(OTHER);
    let mut d = driver(0);
    d.poll(0, &mut register, &mut cell);
    assert_eq!(register.ran, vec![Change::Elect { holder: ME }]);
}

#[test]
fn a_lost_campaign_leaves_the_driver_idle() {
    let mut register = Register::new(RegisterValue::new());
    register.lose = 1;
    let mut cell = TestCell::new();
    let mut d = driver(0);
    let polled = d.poll(0, &mut register, &mut cell);
    assert_eq!(polled, Polled::default());
    assert_eq!(d.controller().standing(), &Standing::Idle);
    // The write took place: the register names this controller, which
    // campaigns again at its next step and wins.
    d.poll(1, &mut register, &mut cell);
    assert_eq!(d.controller().term(), Some(2));
}

#[test]
fn a_member_down_heals_after_the_minimum_interval_under_the_token() {
    let mut register = Register::new(one_segment(ME));
    let mut cell = TestCell::new();
    let mut d = driver(100);
    // Acceptor 2 is never heard: it is down from 130 on. The driver saw
    // segment 1 first at 100, so it waits the minimum interval, to 150.
    for now in [100, 130, 149] {
        beat_all(&mut d, now, &[2]);
        let polled = d.poll(now, &mut register, &mut cell);
        assert!(
            register
                .ran
                .iter()
                .all(|c| matches!(c, Change::Elect { .. }))
        );
        if now >= 130 {
            assert_eq!(
                polled.decision.map(|d| d.action),
                Some(Action::Wait(Wait::Interval))
            );
        }
    }
    beat_all(&mut d, 150, &[2]);
    let polled = d.poll(150, &mut register, &mut cell);
    assert!(matches!(
        polled.decision.map(|d| d.action),
        Some(Action::Reshape(_))
    ));
    let Some(Change::Reshape { opening, .. }) = register.ran.last() else {
        panic!("a reshape ran: {:?}", register.ran);
    };
    assert_eq!(opening.grid.columns(), &[nodes(&[1, 20, 3])]);
    assert_eq!(cell.takes, 1);
    // The new segment is pending: the journal keeps its token.
    assert!(cell.held);
    assert!(d.controller().call().is_none());
}

#[test]
fn a_reshape_with_no_answer_is_sent_again_and_its_token_kept() {
    let mut register = Register::new(one_segment(ME));
    let mut cell = TestCell::new();
    let mut d = driver(0);
    beat_all(&mut d, 0, &[2]);
    d.poll(0, &mut register, &mut cell);
    register.lose = 1;
    beat_all(&mut d, 60, &[2]);
    d.poll(60, &mut register, &mut cell);
    assert!(d.controller().call().is_some());
    assert!(cell.held);
    let reshapes = |r: &Register| {
        r.ran
            .iter()
            .filter(|c| matches!(c, Change::Reshape { .. }))
            .count()
    };
    assert_eq!(reshapes(&register), 1);
    // The retry is answered "ok" by the register: the call ends.
    beat_all(&mut d, 61, &[2]);
    d.poll(61, &mut register, &mut cell);
    assert_eq!(reshapes(&register), 2);
    assert!(d.controller().call().is_none());
}

#[test]
fn no_free_token_holds_the_reshape() {
    let mut register = Register::new(one_segment(ME));
    let mut cell = TestCell::new();
    cell.token_free = false;
    let mut d = driver(0);
    beat_all(&mut d, 0, &[2]);
    d.poll(0, &mut register, &mut cell);
    beat_all(&mut d, 60, &[2]);
    let polled = d.poll(60, &mut register, &mut cell);
    assert_eq!(
        polled.decision.map(|d| d.action),
        Some(Action::Wait(Wait::NoToken))
    );
    assert_eq!(cell.takes, 0);
}

#[test]
fn a_cut_off_controller_makes_no_plan() {
    let mut register = Register::new(one_segment(ME));
    let mut cell = TestCell::new();
    let mut d = driver(0);
    beat_all(&mut d, 0, &[2]);
    d.poll(0, &mut register, &mut cell);
    // No report for longer than the stale age: every process looks down,
    // and the stale signals hold the plan.
    let polled = d.poll(100, &mut register, &mut cell);
    assert_eq!(
        polled.decision.map(|d| d.action),
        Some(Action::Wait(Wait::Stale))
    );
}

#[test]
fn a_leader_that_writes_the_newest_segment_is_live() {
    let mut register = Register::new(one_segment(ME));
    let mut cell = TestCell::new();
    let mut d = driver(0);
    beat_all(&mut d, 0, &[]);
    d.publish(
        0,
        Report::Writes {
            process: ProcessId(1),
            segment: SegmentId(1),
        },
    );
    let with_leader = d
        .poll(0, &mut register, &mut cell)
        .decision
        .expect("a decision");
    beat_all(&mut d, 20, &[]);
    let without = d
        .poll(20, &mut register, &mut cell)
        .decision
        .expect("a decision");
    assert_eq!(with_leader.action, Action::Idle);
    assert_eq!(without.action, Action::Idle);
    assert!(d.signals().writes(SegmentId(1), 0, CONFIG.windows));
}

#[test]
fn a_heal_sends_its_copy_and_writes_the_set_on_the_complete_copy() {
    let mut register = Register::new(retired(ME));
    let mut cell = TestCell::new();
    let mut d = driver(0);
    beat_all(&mut d, 0, &[12]);
    d.poll(0, &mut register, &mut cell);
    beat_all(&mut d, 40, &[12]);
    let polled = d.poll(40, &mut register, &mut cell);
    let [
        Ask::Copy {
            segment,
            node,
            copy,
            ..
        },
    ] = polled.sends.as_slice()
    else {
        panic!("a copy ask: {polled:?}");
    };
    assert_eq!((*segment, *node), (SegmentId(1), NodeId(20)));
    assert_eq!(cell.takes, 1);
    assert!(cell.held);
    // The copy reports, and its answer is lost: the role sends the
    // compare-and-set again at its next step.
    register.lose = 1;
    let polled = d.copied(41, SegmentId(1), *copy, &mut register, &mut cell);
    assert_eq!(polled.sends, Vec::<Ask>::new());
    assert!(d.controller().heal().is_some());
    beat_all(&mut d, 42, &[12]);
    d.poll(42, &mut register, &mut cell);
    assert!(d.controller().heal().is_none());
    let value = register.read().expect("a register");
    assert_eq!(
        value.segment(SegmentId(1)).expect("segment 1").replicas(),
        nodes(&[11, 20, 13])
    );
    assert!(!cell.held);
}

#[test]
fn a_refused_heal_sends_its_abandon() {
    let mut register = Register::new(retired(ME));
    let mut cell = TestCell::new();
    let mut d = driver(0);
    beat_all(&mut d, 0, &[12]);
    d.poll(0, &mut register, &mut cell);
    beat_all(&mut d, 40, &[12]);
    let polled = d.poll(40, &mut register, &mut cell);
    let [Ask::Copy { copy, .. }] = polled.sends.as_slice() else {
        panic!("a copy ask: {polled:?}");
    };
    let copy = *copy;
    // Another controller takes the term: the heal's write is refused for
    // the term, and the new replica is told to abandon its copy.
    let value = apply(
        &register.read().expect("a register"),
        &Change::Elect { holder: OTHER },
    );
    register.value = Some(value);
    let polled = d.copied(41, SegmentId(1), copy, &mut register, &mut cell);
    assert!(matches!(
        register
            .value
            .as_ref()
            .map(|v| v.apply(register.ran.last().expect("a change"))),
        Some(Err(ChangeRefusal::Term { .. }))
    ));
    assert!(matches!(polled.sends.as_slice(), [Ask::Abandon { .. }]));
    assert_eq!(d.controller().standing(), &Standing::Idle);
}

#[test]
fn a_report_of_no_heal_changes_nothing() {
    let mut register = Register::new(retired(ME));
    let mut cell = TestCell::new();
    let mut d = driver(0);
    let polled = d.copied(
        0,
        SegmentId(1),
        CopyId { term: 1, number: 0 },
        &mut register,
        &mut cell,
    );
    assert_eq!(polled, Polled::default());
    assert_eq!(register.ran, Vec::<Change>::new());
}

#[test]
fn no_register_no_step() {
    let mut register = Register::new(RegisterValue::new());
    register.value = None;
    let mut cell = TestCell::new();
    let mut d = driver(0);
    assert_eq!(d.poll(0, &mut register, &mut cell), Polled::default());
    assert_eq!(register.ran, Vec::<Change>::new());
}

#[test]
fn the_controller_reports_its_own_process() {
    assert_eq!(
        driver(0).report(),
        Report::Beat {
            process: ProcessId(ME.0)
        }
    );
}

#[test]
#[should_panic(expected = "the program's clock never goes back")]
fn a_poll_before_the_last_time_panics() {
    let mut register = Register::new(RegisterValue::new());
    let mut cell = TestCell::new();
    let mut d = driver(10);
    d.poll(9, &mut register, &mut cell);
}

#[test]
fn every_probe_message_is_named_once() {
    let unique: BTreeSet<&str> = PROBES.iter().copied().collect();
    assert_eq!(unique.len(), PROBES.len());
    assert!(PROBES.iter().all(|p| p.starts_with("driver: ")));
}

/// `one_segment`, then segment 2 on 4, 5, 6 and segment 1 sealed.
fn sealed(holder: NodeId) -> RegisterValue {
    let value = apply(
        &one_segment(holder),
        &Change::Reshape {
            opening: OpeningCall {
                term: 1,
                expected: Some(SegmentId(1)),
                grid: Grid::new(1, vec![nodes(&[4, 5, 6])]).expect("a v1 grid"),
                replicas: nodes(&[14, 15, 16]),
            },
            release: RELEASE,
        },
    );
    apply(
        &value,
        &Change::Seal {
            segment: SegmentId(1),
            end: Slot(3),
        },
    )
}

fn stalled(polled: &Polled) -> bool {
    polled
        .decision
        .as_ref()
        .is_some_and(|d| d.alerts.contains(&crate::decide::Alert::ReshapeStalled))
}

#[test]
fn a_seal_stalls_after_the_stall_window_from_when_the_driver_first_saw_it() {
    let stall = CONFIG.tunables.stall_window;
    let mut register = Register::new(one_segment(ME));
    let mut cell = TestCell::new();
    let mut d = driver(100);
    beat_all(&mut d, 100, &[]);
    d.poll(100, &mut register, &mut cell);
    // The seal is seen at 200, and kept from then on.
    register.value = Some(sealed(ME));
    register.value = Some(apply(
        &register.read().expect("a register"),
        &Change::Elect { holder: ME },
    ));
    d = driver(200);
    for now in [200, 300, 200 + stall] {
        beat_all(&mut d, now, &[]);
        let polled = d.poll(now, &mut register, &mut cell);
        assert!(!stalled(&polled), "not stalled at {now}");
    }
    beat_all(&mut d, 201 + stall, &[]);
    let polled = d.poll(201 + stall, &mut register, &mut cell);
    assert!(stalled(&polled));
    // The sealed segment holds the token.
    assert!(
        cell.gives == 1 && !cell.held,
        "given back only before the seal"
    );
}

#[test]
fn a_seal_seen_later_stalls_from_its_own_sight() {
    let stall = CONFIG.tunables.stall_window;
    let mut register = Register::new(one_segment(ME));
    let mut cell = TestCell::new();
    let mut d = driver(100);
    beat_all(&mut d, 100, &[]);
    d.poll(100, &mut register, &mut cell);
    let value = register.read().expect("a register");
    // The same driver sees segment 1 in force at 100, then sealed at 500.
    let value = apply(
        &value,
        &Change::Reshape {
            opening: OpeningCall {
                term: value.term().number,
                expected: Some(SegmentId(1)),
                grid: Grid::new(1, vec![nodes(&[4, 5, 6])]).expect("a v1 grid"),
                replicas: nodes(&[14, 15, 16]),
            },
            release: RELEASE,
        },
    );
    beat_all(&mut d, 300, &[]);
    register.value = Some(value.clone());
    d.poll(300, &mut register, &mut cell);
    register.value = Some(apply(
        &value,
        &Change::Seal {
            segment: SegmentId(1),
            end: Slot(3),
        },
    ));
    for now in [500, 100 + stall + 1, 500 + stall] {
        beat_all(&mut d, now, &[]);
        let polled = d.poll(now, &mut register, &mut cell);
        assert!(!stalled(&polled), "not stalled at {now}");
    }
    beat_all(&mut d, 501 + stall, &[]);
    assert!(stalled(&d.poll(501 + stall, &mut register, &mut cell)));
}

#[test]
fn the_drivers_own_report_hides_no_silence() {
    let mut d = driver(0);
    d.publish(
        25,
        Report::Beat {
            process: ProcessId(ME.0),
        },
    );
    // The own beat is dropped: the driver heard nothing since its start.
    assert_eq!(d.signals().signal_age(25), 25);
    assert_eq!(
        d.signals().liveness(ProcessId(ME.0), 25, CONFIG.windows),
        Liveness::Suspect
    );
}

#[test]
#[should_panic(expected = "the cell places this controller on the driver's own process")]
fn a_cell_that_places_the_controller_elsewhere_panics() {
    let mut register = Register::new(RegisterValue::new());
    let mut cell = TestCell::new();
    let mut d = ControlDriver::new(ME, ProcessId(7), CONFIG, 0);
    d.poll(0, &mut register, &mut cell);
}

/// Segment 1 on the two columns 1, 2, 3 and 4, 5, 6, in force, and
/// segment 2 pending: the newest segment is not the one in force.
fn two_columns_and_a_pending_segment() -> RegisterValue {
    let value = apply(&RegisterValue::new(), &Change::Elect { holder: ME });
    let value = apply(
        &value,
        &Change::Reshape {
            opening: OpeningCall {
                term: 1,
                expected: None,
                grid: Grid::new(1, vec![nodes(&[1, 2, 3]), nodes(&[4, 5, 6])]).expect("a v1 grid"),
                replicas: nodes(&[11, 12, 13]),
            },
            release: RELEASE,
        },
    );
    apply(
        &value,
        &Change::Reshape {
            opening: OpeningCall {
                term: 1,
                expected: Some(SegmentId(1)),
                grid: Grid::new(1, vec![nodes(&[7, 8, 9])]).expect("a v1 grid"),
                replicas: nodes(&[17, 18, 19]),
            },
            release: RELEASE,
        },
    )
}

/// A column below quorum: the grid writes with a live leader of the
/// segment in force, and blocks an election without one. The leader of
/// another segment does not count.
#[test]
fn the_leader_that_counts_writes_the_segment_in_force() {
    let alerts_with = |segment: Option<SegmentId>| {
        let mut register = Register::new(two_columns_and_a_pending_segment());
        let mut cell = TestCell::new();
        let mut d = driver(0);
        // 2 and 3 are never heard: down at 40.
        beat_all(&mut d, 40, &[2, 3]);
        if let Some(segment) = segment {
            d.publish(
                40,
                Report::Writes {
                    process: ProcessId(1),
                    segment,
                },
            );
        }
        d.poll(40, &mut register, &mut cell)
            .decision
            .expect("a decision")
            .alerts
    };
    let with = alerts_with(Some(SegmentId(1)));
    assert!(with.contains(&Alert::WritesDegraded), "{with:?}");
    for other in [Some(SegmentId(2)), None] {
        let alerts = alerts_with(other);
        assert!(alerts.contains(&Alert::ElectionBlocked), "{other:?}");
        assert!(!alerts.contains(&Alert::WritesDegraded), "{other:?}");
    }
}

#[test]
fn a_deposed_holder_neither_takes_nor_gives_the_token() {
    let mut register = Register::new(one_segment(ME));
    let mut cell = TestCell::new();
    let mut d = driver(0);
    beat_all(&mut d, 0, &[2]);
    d.poll(0, &mut register, &mut cell);
    assert_eq!(d.controller().term(), Some(2));
    let (takes, gives) = (cell.takes, cell.gives);
    // Another controller takes the term while this one still holds 2.
    register.value = Some(apply(
        &register.read().expect("a register"),
        &Change::Elect { holder: OTHER },
    ));
    cell.held = true;
    beat_all(&mut d, 60, &[2]);
    let polled = d.poll(60, &mut register, &mut cell);
    assert_eq!(polled.decision.map(|d| d.action), Some(Action::Stop));
    assert_eq!((cell.takes, cell.gives), (takes, gives));
    assert!(cell.held, "the token of the new holder stays");
    assert!(
        register
            .ran
            .iter()
            .all(|c| !matches!(c, Change::Reshape { .. })),
        "{:?}",
        register.ran
    );
}

#[test]
fn a_segment_that_leaves_the_register_is_forgotten() {
    let mut register = Register::new(sealed(ME));
    let mut cell = TestCell::new();
    let mut d = driver(0);
    beat_all(&mut d, 0, &[]);
    d.poll(0, &mut register, &mut cell);
    assert!(d.seen.contains_key(&SegmentId(2)));
    register.value = Some(one_segment(ME));
    beat_all(&mut d, 10, &[]);
    d.poll(10, &mut register, &mut cell);
    assert_eq!(
        d.seen.keys().copied().collect::<Vec<_>>(),
        vec![SegmentId(1)]
    );
}

/// A copy that reports after another instance of this controller took a
/// later term: the compare-and-set still goes (the register refuses it),
/// but the token is not taken for it.
#[test]
fn a_copy_that_reports_after_a_later_term_takes_no_token() {
    let mut register = Register::new(retired(ME));
    let mut cell = TestCell::new();
    let mut d = driver(0);
    beat_all(&mut d, 0, &[12]);
    d.poll(0, &mut register, &mut cell);
    beat_all(&mut d, 40, &[12]);
    let polled = d.poll(40, &mut register, &mut cell);
    let [Ask::Copy { copy, .. }] = polled.sends.as_slice() else {
        panic!("a copy ask: {polled:?}");
    };
    assert_eq!(cell.takes, 1);
    // A restarted instance of this controller wins the next term, and the
    // cell's token is free again.
    register.value = Some(apply(
        &register.read().expect("a register"),
        &Change::Elect { holder: ME },
    ));
    cell.held = false;
    d.copied(41, SegmentId(1), *copy, &mut register, &mut cell);
    assert!(
        matches!(register.ran.last(), Some(Change::Rereplicate { .. })),
        "{:?}",
        register.ran
    );
    assert_eq!(cell.takes, 1, "no token for a term this driver lost");
    assert!(!cell.held);
}

/// A heal of `ME` whose compare-and-set landed with its answer lost, then
/// a takeover by `OTHER`: the register, the cell (the journal still holds
/// the token, now for `OTHER`), the driver and the heal's copy.
fn a_heal_landed_then_taken_over() -> (Register, TestCell, ControlDriver, CopyId) {
    let mut register = Register::new(retired(ME));
    let mut cell = TestCell::new();
    let mut d = driver(0);
    beat_all(&mut d, 0, &[12]);
    d.poll(0, &mut register, &mut cell);
    beat_all(&mut d, 40, &[12]);
    let polled = d.poll(40, &mut register, &mut cell);
    let [Ask::Copy { copy, .. }] = polled.sends.as_slice() else {
        panic!("a copy ask: {polled:?}");
    };
    let copy = *copy;
    register.lose = 1;
    d.copied(41, SegmentId(1), copy, &mut register, &mut cell);
    assert!(d.controller().heal().is_some(), "the answer was lost");
    register.value = Some(apply(
        &register.read().expect("a register"),
        &Change::Elect { holder: OTHER },
    ));
    assert!(cell.held);
    (register, cell, d, copy)
}

#[test]
fn a_deposed_holder_that_reads_its_heal_done_gives_back_no_token() {
    let (mut register, mut cell, mut d, _) = a_heal_landed_then_taken_over();
    let gives = cell.gives;
    beat_all(&mut d, 42, &[12]);
    d.poll(42, &mut register, &mut cell);
    assert!(
        d.controller().heal().is_none(),
        "the heal ended on the read"
    );
    assert_eq!(cell.gives, gives);
    assert!(cell.held, "the new holder's token stays");
}

#[test]
fn a_deposed_holder_whose_copy_reports_again_gives_back_no_token() {
    let (mut register, mut cell, mut d, copy) = a_heal_landed_then_taken_over();
    let gives = cell.gives;
    d.copied(42, SegmentId(1), copy, &mut register, &mut cell);
    assert_eq!(cell.gives, gives);
    assert!(cell.held, "the new holder's token stays");
}
