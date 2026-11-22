use std::collections::{BTreeMap, BTreeSet};

use paros_consensus::membership::{
    FailureDomain, MatchmakerGeneration, MatchmakerId, MatchmakerSet,
};
use paros_journal::register::{Change, ChangeRefusal, RegisterValue};
use paros_journal::segment::OpeningCall;
use paros_journal::{Grid, RELEASE, SegmentId};

use super::*;
use crate::measure::Liveness;

const ME: NodeId = NodeId(100);
const OTHER: NodeId = NodeId(200);
const TUNABLES: Tunables = Tunables {
    stale_after: 10,
    min_reshape_interval: 50,
    stall_window: 100,
};

fn nodes(ids: &[u64]) -> Vec<NodeId> {
    ids.iter().copied().map(NodeId).collect()
}

fn apply(value: &RegisterValue, change: &Change) -> RegisterValue {
    value.apply(change).expect("the register takes the change")
}

/// The register after `ME` won term 1 and opened segment 1 on 1, 2, 3 with
/// replicas 11, 12, 13.
fn one_segment() -> RegisterValue {
    let value = apply(&RegisterValue::new(), &Change::Elect { holder: ME });
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

/// The signals of a view, owned by the test.
struct Signals {
    liveness: BTreeMap<NodeId, Liveness>,
    zones: BTreeMap<NodeId, FailureDomain>,
    candidates: Vec<NodeId>,
    matchmakers: MatchmakerSet,
    matchmakers_live: BTreeSet<MatchmakerId>,
    repairs: BTreeSet<SegmentId>,
}

impl Signals {
    /// Every node of 1..=30 up but `down`, five matchmakers live.
    fn with_down(down: &[u64]) -> Self {
        let ids: Vec<MatchmakerId> = (1..=5).map(MatchmakerId).collect();
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
            candidates: nodes(&[20, 21, 22]),
            matchmakers: MatchmakerSet::new(MatchmakerGeneration(0), ids.clone()),
            matchmakers_live: ids.into_iter().collect(),
            repairs: BTreeSet::new(),
        }
    }

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

/// A controller that holds term 1 of `one_segment`.
fn holding() -> Controller {
    let mut controller = Controller::new(ME);
    let change = controller.campaign();
    let value = apply(&RegisterValue::new(), &change);
    controller.elected(&Outcome::Done(value));
    assert_eq!(controller.term(), Some(1));
    controller
}

/// A holding controller that planned the heal of member 2: its call.
fn planned(controller: &mut Controller, register: &RegisterValue) -> OpeningCall {
    let signals = Signals::with_down(&[2]);
    let (ask, decision) = controller.step(&signals.view(ME, 1, register), &TUNABLES);
    let Ask::Call(Change::Reshape { opening, release }) = ask else {
        panic!("a heal is a reshape call, got {ask:?}");
    };
    assert_eq!(release, RELEASE);
    assert_eq!(
        decision.map(|d| d.action),
        Some(Action::Reshape(opening.clone()))
    );
    assert_eq!(controller.call(), Some(&opening));
    opening
}

#[test]
fn a_fresh_controller_is_idle_and_asks_nothing() {
    let mut controller = Controller::new(ME);
    assert_eq!(controller.me(), ME);
    assert_eq!(controller.standing(), &Standing::Idle);
    assert_eq!(controller.term(), None);
    assert_eq!(controller.call(), None);
    let register = one_segment();
    let signals = Signals::with_down(&[2]);
    assert_eq!(
        controller.step(&signals.view(ME, 1, &register), &TUNABLES),
        (Ask::Nothing, None),
        "a controller with no term plans nothing"
    );
}

#[test]
fn a_campaign_asks_elect_and_a_win_holds_the_written_term() {
    let mut controller = Controller::new(ME);
    assert_eq!(controller.campaign(), Change::Elect { holder: ME });
    assert_eq!(controller.standing(), &Standing::Campaigning);
    assert_eq!(controller.term(), None, "a campaign holds no term yet");
    let value = apply(
        &apply(&RegisterValue::new(), &Change::Elect { holder: OTHER }),
        &Change::Elect { holder: ME },
    );
    controller.elected(&Outcome::Done(value));
    assert_eq!(controller.standing(), &Standing::Holding { term: 2 });
    assert_eq!(controller.term(), Some(2));
}

#[test]
fn a_campaign_that_does_not_name_it_ends_idle() {
    let lost = apply(&RegisterValue::new(), &Change::Elect { holder: OTHER });
    let refusal = ChangeRefusal::Term { held: 2, asked: 1 };
    for outcome in [
        Outcome::Done(lost),
        Outcome::Refused(refusal),
        Outcome::NoAnswer,
    ] {
        let mut controller = Controller::new(ME);
        controller.campaign();
        controller.elected(&outcome);
        assert_eq!(controller.standing(), &Standing::Idle, "{outcome:?}");
        assert_eq!(controller.term(), None);
        // An idle controller campaigns again.
        assert_eq!(controller.campaign(), Change::Elect { holder: ME });
    }
}

#[test]
#[should_panic(expected = "only an idle controller campaigns")]
fn a_holder_never_campaigns() {
    holding().campaign();
}

#[test]
#[should_panic(expected = "only an idle controller campaigns")]
fn a_campaign_is_not_started_twice() {
    let mut controller = Controller::new(ME);
    controller.campaign();
    controller.campaign();
}

#[test]
#[should_panic(expected = "an election outcome answers a campaign")]
fn an_election_outcome_needs_a_campaign() {
    Controller::new(ME).elected(&Outcome::NoAnswer);
}

#[test]
#[should_panic(expected = "a view of this controller")]
fn a_view_of_another_controller_is_a_bug() {
    let register = one_segment();
    let signals = Signals::with_down(&[]);
    holding().step(&signals.view(OTHER, 1, &register), &TUNABLES);
}

#[test]
#[should_panic(expected = "a view carries the term the controller holds")]
fn a_view_of_another_term_is_a_bug() {
    let register = one_segment();
    let signals = Signals::with_down(&[]);
    holding().step(&signals.view(ME, 2, &register), &TUNABLES);
}

#[test]
fn a_healthy_grid_asks_nothing_but_gives_the_decision() {
    let mut controller = holding();
    let register = one_segment();
    let signals = Signals::with_down(&[]);
    let (ask, decision) = controller.step(&signals.view(ME, 1, &register), &TUNABLES);
    assert_eq!(ask, Ask::Nothing);
    assert_eq!(decision.map(|d| d.action), Some(Action::Idle));
    assert_eq!(controller.call(), None);
    assert_eq!(controller.term(), Some(1));
}

#[test]
fn a_planned_call_is_sent_again_until_it_is_answered() {
    let mut controller = holding();
    let register = one_segment();
    let call = planned(&mut controller, &register);
    assert!(
        !call.grid.columns()[0].contains(&NodeId(2)),
        "the heal drops 2"
    );
    // A view where nothing is down any more: the call in flight is sent
    // again, and no new plan is made.
    let signals = Signals::with_down(&[]);
    for _ in 0..3 {
        let (ask, decision) = controller.step(&signals.view(ME, 1, &register), &TUNABLES);
        assert_eq!(
            ask,
            Ask::Call(Change::Reshape {
                opening: call.clone(),
                release: RELEASE,
            })
        );
        assert_eq!(decision, None, "no plan while a call is in flight");
    }
    controller.reshaped(&call, &Outcome::NoAnswer);
    assert_eq!(controller.call(), Some(&call), "no answer keeps the call");
    let done = apply(
        &register,
        &Change::Reshape {
            opening: call.clone(),
            release: RELEASE,
        },
    );
    controller.reshaped(&call, &Outcome::Done(done));
    assert_eq!(controller.call(), None);
    assert_eq!(controller.term(), Some(1), "a done reshape keeps the term");
}

#[test]
fn a_refusal_for_the_term_deposes_and_another_refusal_drops_the_call() {
    let register = one_segment();

    let mut controller = holding();
    let call = planned(&mut controller, &register);
    controller.reshaped(
        &call,
        &Outcome::Refused(ChangeRefusal::Term { held: 2, asked: 1 }),
    );
    assert_eq!(controller.standing(), &Standing::Idle);
    assert_eq!(controller.call(), None);

    let mut controller = holding();
    let call = planned(&mut controller, &register);
    let refusal = ChangeRefusal::Expected {
        newest: Some(SegmentId(2)),
        asked: Some(SegmentId(1)),
    };
    controller.reshaped(&call, &Outcome::Refused(refusal));
    assert_eq!(controller.call(), None);
    assert_eq!(controller.term(), Some(1), "only a term refusal deposes");
}

#[test]
fn an_outcome_of_another_call_changes_nothing() {
    let register = one_segment();
    let mut controller = holding();
    let call = planned(&mut controller, &register);
    let mut older = call.clone();
    older.replicas = nodes(&[61, 62, 63]);
    for outcome in [
        Outcome::Refused(ChangeRefusal::Term { held: 2, asked: 1 }),
        Outcome::Refused(ChangeRefusal::Unretired(SegmentId(1))),
        Outcome::NoAnswer,
    ] {
        controller.reshaped(&older, &outcome);
        assert_eq!(controller.call(), Some(&call), "{outcome:?}");
        assert_eq!(controller.term(), Some(1), "{outcome:?}");
    }
    // With no call in flight, every outcome is of an older call.
    controller.reshaped(
        &call,
        &Outcome::Refused(ChangeRefusal::Unretired(SegmentId(1))),
    );
    controller.reshaped(
        &call,
        &Outcome::Refused(ChangeRefusal::Term { held: 2, asked: 1 }),
    );
    assert_eq!(controller.term(), Some(1));
}

#[test]
#[should_panic(expected = "a done reshape holds the record its call opened")]
fn a_done_reshape_without_its_record_is_a_bug() {
    let register = one_segment();
    let mut controller = holding();
    let call = planned(&mut controller, &register);
    controller.reshaped(&call, &Outcome::Done(register));
}

#[test]
fn a_view_of_another_term_holder_stops_the_controller() {
    let register = apply(&one_segment(), &Change::Elect { holder: OTHER });
    let mut controller = holding();
    let signals = Signals::with_down(&[2]);
    let (ask, decision) = controller.step(&signals.view(ME, 1, &register), &TUNABLES);
    assert_eq!(ask, Ask::Nothing);
    assert_eq!(decision.map(|d| d.action), Some(Action::Stop));
    assert_eq!(controller.standing(), &Standing::Idle);
    assert_eq!(controller.call(), None, "a stop plans nothing");
}

#[test]
fn a_repair_is_an_act_outside_the_register() {
    let register = one_segment();
    let mut controller = holding();
    let mut signals = Signals::with_down(&[]);
    signals.repairs.insert(SegmentId(1));
    let (ask, decision) = controller.step(&signals.view(ME, 1, &register), &TUNABLES);
    let action = Action::Repair {
        segment: SegmentId(1),
    };
    assert_eq!(ask, Ask::Act(action.clone()));
    assert_eq!(decision.map(|d| d.action), Some(action));
    assert_eq!(controller.call(), None, "an act is not a call in flight");
}

#[test]
fn a_restart_forgets_the_term_and_the_plan() {
    let register = one_segment();
    let mut controller = holding();
    planned(&mut controller, &register);
    let restarted = Controller::new(controller.me());
    assert_eq!(restarted, Controller::new(ME));
    assert_eq!(restarted.term(), None);
    assert_eq!(restarted.call(), None);
}

#[test]
fn every_probe_message_is_named_once() {
    let names: BTreeSet<&str> = PROBES.iter().copied().collect();
    assert_eq!(names.len(), PROBES.len());
    assert!(PROBES.iter().all(|p| p.starts_with("role: ")));
}
