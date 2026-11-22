use paros_consensus::Slot;
use paros_consensus::membership::MatchmakerGeneration;
use paros_journal::register::Change;
use paros_journal::{Grid, RELEASE};

use super::*;

const ME: NodeId = NodeId(100);
const TUNABLES: Tunables = Tunables {
    stale_after: 10,
    min_reshape_interval: 50,
    stall_window: 100,
};

fn nodes(ids: &[u64]) -> Vec<NodeId> {
    ids.iter().copied().map(NodeId).collect()
}

fn opening(term: u64, expected: Option<u64>, members: &[u64], replicas: &[u64]) -> OpeningCall {
    OpeningCall {
        term,
        expected: expected.map(SegmentId),
        grid: Grid::new(1, vec![nodes(members)]).expect("a v1 grid"),
        replicas: nodes(replicas),
    }
}

fn apply(value: &RegisterValue, change: &Change) -> RegisterValue {
    value.apply(change).expect("the register takes the change")
}

fn reshape(value: &RegisterValue, call: OpeningCall) -> RegisterValue {
    apply(
        value,
        &Change::Reshape {
            opening: call,
            release: RELEASE,
        },
    )
}

/// Term 1 held by `ME`; segment 1 in force on 1, 2, 3 with replicas 11,
/// 12, 13.
fn one_segment() -> RegisterValue {
    let value = apply(&RegisterValue::new(), &Change::Elect { holder: ME });
    reshape(&value, opening(1, None, &[1, 2, 3], &[11, 12, 13]))
}

/// Segment 2 pending on 4, 5, 6 with replicas 14, 15, 16.
fn pending() -> RegisterValue {
    reshape(
        &one_segment(),
        opening(1, Some(1), &[4, 5, 6], &[14, 15, 16]),
    )
}

/// Segment 1 sealed, segment 2 in force.
fn sealed() -> RegisterValue {
    apply(
        &pending(),
        &Change::Seal {
            segment: SegmentId(1),
            end: Slot(5),
        },
    )
}

struct Fixture {
    register: RegisterValue,
    liveness: BTreeMap<NodeId, Liveness>,
    zones: BTreeMap<NodeId, FailureDomain>,
    candidates: Vec<NodeId>,
    matchmakers: MatchmakerSet,
    matchmakers_live: BTreeSet<MatchmakerId>,
    repairs: BTreeSet<SegmentId>,
    data_lost: bool,
    leader_live: bool,
    signal_age: u64,
    since: Option<u64>,
    seal_age: Option<u64>,
    token: bool,
    term: u64,
}

impl Fixture {
    /// Every node of 1..=30 up, three matchmakers live, a token free.
    fn new(register: RegisterValue) -> Self {
        let ids = |r: std::ops::RangeInclusive<u64>| r.map(MatchmakerId).collect::<Vec<_>>();
        Self {
            register,
            liveness: (1..=30).map(|n| (NodeId(n), Liveness::Up)).collect(),
            zones: BTreeMap::new(),
            candidates: nodes(&[20, 21, 22]),
            matchmakers: MatchmakerSet::new(MatchmakerGeneration(0), ids(1..=5)),
            matchmakers_live: ids(1..=5).into_iter().collect(),
            repairs: BTreeSet::new(),
            data_lost: false,
            leader_live: true,
            signal_age: 0,
            since: None,
            seal_age: None,
            token: true,
            term: 1,
        }
    }

    fn set(mut self, node: u64, liveness: Liveness) -> Self {
        self.liveness.insert(NodeId(node), liveness);
        self
    }

    fn decide(&self) -> Decision {
        let view = View {
            me: ME,
            term: self.term,
            register: &self.register,
            liveness: &self.liveness,
            zones: &self.zones,
            candidates: &self.candidates,
            leader_live: self.leader_live,
            matchmakers: &self.matchmakers,
            matchmakers_live: &self.matchmakers_live,
            repairs_pending: &self.repairs,
            data_lost: self.data_lost,
            signal_age: self.signal_age,
            since_last_reshape: self.since,
            seal_age: self.seal_age,
            token_free: self.token,
        };
        let decision = decide(&view, &TUNABLES);
        assert_eq!(
            decide(&view, &TUNABLES),
            decision,
            "decide is deterministic"
        );
        decision
    }

    fn action(&self) -> Action {
        self.decide().action
    }
}

#[test]
fn a_healthy_journal_does_nothing() {
    let decision = Fixture::new(one_segment()).decide();
    assert_eq!(decision.action, Action::Idle);
    assert!(decision.alerts.is_empty());
}

#[test]
fn a_controller_of_another_term_stops() {
    let mut fixture = Fixture::new(one_segment()).set(2, Liveness::Down);
    fixture.term = 2;
    assert_eq!(fixture.decide().action, Action::Stop);
    let deposed = apply(
        &fixture.register,
        &Change::Elect {
            holder: NodeId(101),
        },
    );
    let mut fixture = Fixture::new(deposed);
    fixture.data_lost = true;
    let decision = fixture.decide();
    assert_eq!(decision.action, Action::Stop);
    assert!(
        decision.alerts.is_empty(),
        "a deposed controller reports nothing"
    );
}

#[test]
fn a_member_down_is_healed_by_one_fenced_reshape() {
    let fixture = Fixture::new(one_segment()).set(2, Liveness::Down);
    let Action::Reshape(call) = fixture.action() else {
        panic!("a heal");
    };
    assert_eq!(call.term, 1);
    assert_eq!(call.expected, Some(SegmentId(1)));
    assert_eq!(call.grid.columns(), &[nodes(&[1, 20, 3])]);
    assert_eq!(call.replicas, nodes(&[11, 12, 13]));
    // The register takes it, and the same view then waits for the retire:
    // the decision is idempotent.
    let after = reshape(&fixture.register, call);
    let fixture = Fixture {
        register: after,
        ..fixture
    };
    assert_eq!(fixture.action(), Action::Wait(Wait::Reshaping));
}

#[test]
fn a_replica_down_in_the_segment_in_force_is_healed_by_a_reshape() {
    let fixture = Fixture::new(one_segment()).set(12, Liveness::Down);
    let Action::Reshape(call) = fixture.action() else {
        panic!("a heal");
    };
    assert_eq!(call.grid.columns(), &[nodes(&[1, 2, 3])]);
    assert_eq!(call.replicas, nodes(&[11, 20, 13]));
}

#[test]
fn a_suspect_member_is_not_replaced() {
    let fixture = Fixture::new(one_segment()).set(2, Liveness::Suspect);
    assert_eq!(fixture.action(), Action::Wait(Wait::Suspect));
}

#[test]
fn a_heal_waits_for_the_interval_and_for_a_token() {
    let mut fixture = Fixture::new(one_segment()).set(2, Liveness::Down);
    fixture.since = Some(49);
    assert_eq!(fixture.action(), Action::Wait(Wait::Interval));
    fixture.since = Some(50);
    assert!(matches!(fixture.action(), Action::Reshape(_)));
    fixture.token = false;
    assert_eq!(fixture.action(), Action::Wait(Wait::NoToken));
}

#[test]
fn no_plan_is_made_on_stale_signals() {
    let mut fixture = Fixture::new(one_segment()).set(2, Liveness::Down);
    fixture.signal_age = 11;
    assert_eq!(fixture.action(), Action::Wait(Wait::Stale));
    fixture.repairs.insert(SegmentId(1));
    assert_eq!(fixture.action(), Action::Wait(Wait::Stale));
}

#[test]
fn a_repair_comes_before_a_heal() {
    let mut fixture = Fixture::new(one_segment()).set(2, Liveness::Down);
    fixture.repairs.insert(SegmentId(1));
    fixture.token = false;
    assert_eq!(
        fixture.action(),
        Action::Repair {
            segment: SegmentId(1)
        }
    );
}

#[test]
fn the_states_that_hold_alert_and_never_reshape() {
    // Two of three down: not electable, the column cannot write.
    let fixture = Fixture::new(one_segment())
        .set(2, Liveness::Down)
        .set(3, Liveness::Down);
    let decision = fixture.decide();
    assert_eq!(
        decision.action,
        Action::Wait(Wait::Hold(GridState::Stopped))
    );
    assert!(decision.alerts.contains(&Alert::Stopped));
    assert!(Alert::Stopped.pages());
}

#[test]
fn a_zone_loss_on_three_zones_holds_and_alerts() {
    let mut fixture = Fixture::new(one_segment()).set(2, Liveness::Down);
    for (node, zone) in [
        (1, "a"),
        (2, "b"),
        (3, "c"),
        (20, "a"),
        (21, "c"),
        (22, "a"),
    ] {
        fixture
            .zones
            .insert(NodeId(node), FailureDomain::new("r", zone));
    }
    let decision = fixture.decide();
    assert_eq!(
        decision.action,
        Action::Wait(Wait::Hold(GridState::AtRiskUnplaceable))
    );
    assert!(decision.alerts.contains(&Alert::Unplaceable));
}

#[test]
fn matchmakers_below_a_majority_freeze_every_plan() {
    let mut fixture = Fixture::new(one_segment()).set(2, Liveness::Down);
    fixture.matchmakers_live = [1, 2].into_iter().map(MatchmakerId).collect();
    let decision = fixture.decide();
    assert_eq!(decision.action, Action::Wait(Wait::ControlFrozen));
    assert!(decision.alerts.contains(&Alert::ControlFrozen));
    fixture.matchmakers_live = [1, 2, 3].into_iter().map(MatchmakerId).collect();
    let decision = fixture.decide();
    assert!(matches!(decision.action, Action::Reshape(_)));
    assert!(decision.alerts.contains(&Alert::MatchmakersAtRisk));
}

/// Segment 1 retired, with its boundary state; segment 2 in force.
fn retired() -> RegisterValue {
    let value = apply(
        &sealed(),
        &Change::Boundary {
            segment: SegmentId(1),
            state: paros_journal::BoundaryState::fresh(paros_journal::Leader::Unset),
        },
    );
    apply(
        &value,
        &Change::Retire {
            segment: SegmentId(1),
            holders: 2,
            unrepaired_damage: false,
        },
    )
}

#[test]
fn a_sealed_segment_retires_before_its_heal() {
    let fixture = Fixture::new(sealed()).set(12, Liveness::Down);
    assert!(
        !matches!(fixture.action(), Action::Rereplicate { .. }),
        "the register heals only a retired record"
    );
}

#[test]
fn a_retired_segment_with_a_replica_down_is_re_replicated() {
    let fixture = Fixture::new(retired()).set(12, Liveness::Down);
    assert_eq!(
        fixture.action(),
        Action::Rereplicate {
            segment: SegmentId(1),
            from: nodes(&[11, 12, 13]),
            to: nodes(&[11, 20, 13]),
        }
    );
    let mut fixture = fixture;
    fixture.candidates.clear();
    let decision = fixture.decide();
    assert_eq!(decision.action, Action::Wait(Wait::NoReplicaPlacement));
    assert!(decision.alerts.contains(&Alert::ReplicasUnplaceable));
}

#[test]
fn a_seal_older_than_the_window_alerts() {
    let mut fixture = Fixture::new(sealed()).set(5, Liveness::Down);
    fixture.seal_age = Some(101);
    let decision = fixture.decide();
    assert_eq!(decision.action, Action::Wait(Wait::Reshaping));
    assert!(decision.alerts.contains(&Alert::ReshapeStalled));
}

#[test]
fn a_journal_with_no_segment_waits_for_its_creation() {
    let value = apply(&RegisterValue::new(), &Change::Elect { holder: ME });
    assert_eq!(Fixture::new(value).action(), Action::Wait(Wait::NoSegment));
}

#[test]
fn only_data_loss_a_frozen_control_and_a_blocked_or_stopped_journal_page() {
    let pages: Vec<Alert> = [
        Alert::DataLost,
        Alert::ControlFrozen,
        Alert::MatchmakersAtRisk,
        Alert::Unplaceable,
        Alert::WritesDegraded,
        Alert::ElectionBlocked,
        Alert::Stopped,
        Alert::ReshapeStalled,
        Alert::ReplicasUnplaceable,
    ]
    .into_iter()
    .filter(|a| a.pages())
    .collect();
    assert_eq!(
        pages,
        [
            Alert::DataLost,
            Alert::ControlFrozen,
            Alert::ElectionBlocked,
            Alert::Stopped
        ]
    );
}
