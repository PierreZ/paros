//! The grid controller's decision under test (#1261 (grid controller
//! decision tests), `docs/journal/06-controller.md` §J6.2, "Tests of the
//! health measures and the decisions", tests 1, 2, 5 and 6).
//!
//! - [`reference`]: a second `decide`, written again from §J6.2 and from
//!   the order of the rules in the doc of `paros_journal_control::decide`.
//!   It shares no code with `paros_journal_control`: it counts members
//!   where the crate asks `membership/`, judges the zone rule by its
//!   definition, and finds a placement by trying every assignment in order.
//! - [`views`]: every small view: `P <= 3` columns, `F <= 2`, no zones or
//!   three or four zones, each count of up, suspect and down members in each
//!   column, the candidates, the leader, and the other signals mixed in.
//! - [`check`]: the properties of each decision, the oracles of §J6.2 test
//!   5 that a decision alone can judge: [`NO_UNSAFE_PLAN`],
//!   [`BOUNDED_RESHAPE_RATE`], [`FENCED`], [`NO_STALE_PLAN`],
//!   [`NO_SUSPECT_REPLACED`], [`IDEMPOTENT`], [`MONOTONE`], and
//!   [`SAME_AS_REFERENCE`]. Each one has a planted mutant
//!   ([`ControlMutant`]) that it catches.

use std::collections::{BTreeMap, BTreeSet};

use paros_journal::fold::BoundaryState;
use paros_journal::register::{
    Change, MatchmakerGeneration, MatchmakerId, MatchmakerSet, RegisterValue,
};
use paros_journal::segment::{OpeningCall, SegmentId, SegmentRecord, SegmentState};
use paros_journal::{Grid, Leader, NodeId, RELEASE, Slot};
use paros_journal_control::{
    Action, Decision, FailureDomain, GridState, Liveness, Tunables, View, Wait, decide,
};

pub mod reference;

/// Oracle: a plan names no member that is down, admits the zone rule,
/// keeps the shape of the grid, and changes only what is down.
pub const NO_UNSAFE_PLAN: &str = "a plan meets the zone rule and names no member that is down";
/// Oracle: no reshape inside the minimum interval, while a segment is not
/// retired, or with no token of the rate limiter.
pub const BOUNDED_RESHAPE_RATE: &str =
    "no reshape inside the interval, while reshaping or with no token";
/// Oracle: a controller whose term is not the register's stops, and a call
/// carries the term and the newest segment.
pub const FENCED: &str = "a deposed controller stops and a call carries its term";
/// Oracle: no plan on a stale view or while the matchmakers are frozen.
pub const NO_STALE_PLAN: &str = "no plan on a stale view or with the matchmakers frozen";
/// Oracle: a plan replaces only members that are down; a member that is
/// suspect is never replaced (hysteresis: a flap is held as suspect).
pub const NO_SUSPECT_REPLACED: &str = "a plan replaces only members that are down";
/// Oracle: once the register took the plan, the same view gives no second
/// plan.
pub const IDEMPOTENT: &str = "a plan the register took is not asked again";
/// Oracle: one more member down never makes the journal state healthier.
pub const MONOTONE: &str = "one more member down never makes the state healthier";
/// Oracle: `decide` gives the decision of the reference.
pub const SAME_AS_REFERENCE: &str = "decide gives the decision of the reference";

/// A bug planted in front of `decide`: the view it sees, or the decision
/// it gives, is changed. Each oracle catches one.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ControlMutant {
    /// No bug.
    #[default]
    None,
    /// The zones are dropped from the view: a plan can break the zone rule.
    /// [`NO_UNSAFE_PLAN`] catches it.
    IgnoreZones,
    /// The time since the last reshape is dropped. [`BOUNDED_RESHAPE_RATE`]
    /// catches it.
    IgnoreInterval,
    /// The controller takes the register's term as its own.
    /// [`FENCED`] catches it.
    IgnoreTerm,
    /// The signals look fresh. [`NO_STALE_PLAN`] catches it.
    IgnoreAge,
    /// A suspect member looks down. [`NO_SUSPECT_REPLACED`] catches it.
    SuspectAsDown,
    /// The reshape guard is skipped: the plan is asked again after the
    /// register took it. [`IDEMPOTENT`] catches it.
    IgnoreReshaping,
    /// The leader's liveness is read inverted: a state that is not
    /// electable is judged on the wrong side. [`SAME_AS_REFERENCE`] catches
    /// it.
    InvertLeader,
    /// A journal that is stopped is held as at risk and unplaceable: one
    /// more member down looks healthier. [`MONOTONE`] catches it.
    StoppedAsUnplaceable,
}

/// The numbers of the sweep (each **Open** in §J6; the sweep picks its
/// own).
pub const TUNABLES: Tunables = Tunables {
    stale_after: 10,
    min_reshape_interval: 50,
    stall_window: 100,
};

/// The controller of the sweep.
pub const ME: NodeId = NodeId(900);

/// The register of a view, around the grid under test.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Chain {
    /// No segment yet: the journal's creation opens the first one.
    Fresh,
    /// Segment 1 in force: the grid under test.
    InForce,
    /// Segment 1 in force (the grid under test), segment 2 pending.
    Pending,
    /// Segment 1 sealed, segment 2 in force (the grid under test).
    Sealed,
    /// Segment 1 retired, segment 2 in force (the grid under test).
    Retired,
}

/// The signals of a view besides the grid.
#[expect(
    clippy::struct_excessive_bools,
    reason = "each flag is one signal of the controller's view, as `View` has them"
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Signals {
    /// The register around the grid.
    pub chain: Chain,
    /// The liveness of the replica sets: all up, the second down, or the
    /// second suspect.
    pub replicas: Liveness,
    /// The live matchmakers of five.
    pub matchmakers_live: u64,
    /// A repair is pending on segment 1.
    pub repair: bool,
    /// Data is lost.
    pub data_lost: bool,
    /// The age of the oldest signal.
    pub signal_age: u64,
    /// The time since the last reshape.
    pub since_last_reshape: Option<u64>,
    /// The age of the oldest seal.
    pub seal_age: Option<u64>,
    /// A token is free.
    pub token_free: bool,
    /// The controller's term is the register's.
    pub ours: bool,
}

impl Signals {
    /// The signals of a plain view: every rule but the grid's lets a plan
    /// through.
    #[must_use]
    pub fn plain() -> Self {
        Self {
            chain: Chain::InForce,
            replicas: Liveness::Up,
            matchmakers_live: 5,
            repair: false,
            data_lost: false,
            signal_age: 0,
            since_last_reshape: None,
            seal_age: None,
            token_free: true,
            ours: true,
        }
    }

    /// Mixed signals, drawn from `n` with no randomness: each value of each
    /// signal comes back often over a sweep.
    ///
    /// # Panics
    ///
    /// Never: a 31-bit draw fits a `usize`.
    #[must_use]
    pub fn mixed(n: u64) -> Self {
        let mut x = n.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ (n >> 7);
        let mut pick = |k: usize| {
            x = x
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            usize::try_from(x >> 33).expect("31 bits fit a usize") % k
        };
        let chain = [
            Chain::Fresh,
            Chain::InForce,
            Chain::Pending,
            Chain::Sealed,
            Chain::Retired,
        ][pick(5)];
        Self {
            chain,
            replicas: [Liveness::Up, Liveness::Down, Liveness::Suspect][pick(3)],
            matchmakers_live: [5, 3, 2][pick(3)],
            repair: pick(5) == 0,
            data_lost: pick(5) == 0,
            signal_age: [0, 10, 11][pick(3)],
            since_last_reshape: [None, Some(49), Some(50)][pick(3)],
            seal_age: match chain {
                Chain::Sealed => [Some(100), Some(101)][pick(2)],
                Chain::Fresh | Chain::Retired | Chain::Pending | Chain::InForce => None,
            },
            token_free: pick(4) != 0,
            ours: pick(8) != 0,
        }
    }
}

/// One small view, with everything it borrows.
#[derive(Clone, Debug)]
pub struct Case {
    /// The register.
    pub register: RegisterValue,
    /// The grid under test.
    pub grid: Grid,
    /// The liveness of every node.
    pub liveness: BTreeMap<NodeId, Liveness>,
    /// The zones.
    pub zones: BTreeMap<NodeId, FailureDomain>,
    /// The candidates, in order.
    pub candidates: Vec<NodeId>,
    /// The leader is live.
    pub leader_live: bool,
    /// The matchmakers.
    pub matchmakers: MatchmakerSet,
    /// The live matchmakers.
    pub matchmakers_live: BTreeSet<MatchmakerId>,
    /// The pending repairs.
    pub repairs: BTreeSet<SegmentId>,
    /// The signals.
    pub signals: Signals,
}

fn nodes(range: std::ops::Range<u64>) -> Vec<NodeId> {
    range.map(NodeId).collect()
}

fn zone(index: usize) -> FailureDomain {
    FailureDomain::new("region", format!("zone-{index}"))
}

/// The grid under test: `cols` columns of `2f + 1`, member `(c, r)` is
/// `1 + c * rows + r`.
fn grid_of(f: u8, cols: u64) -> Grid {
    let rows = 2 * u64::from(f) + 1;
    Grid::new(
        f,
        (0..cols)
            .map(|c| nodes(1 + c * rows..1 + (c + 1) * rows))
            .collect(),
    )
    .expect("a v1 grid")
}

const REPLICAS: [u64; 3] = [31, 32, 33];
const OTHER_GRID: [u64; 3] = [51, 52, 53];
const OTHER_REPLICAS: [u64; 3] = [61, 62, 63];
const CANDIDATES: [u64; 3] = [41, 42, 43];

fn call(term: u64, expected: Option<SegmentId>, grid: Grid, replicas: &[u64]) -> Change {
    Change::Reshape {
        opening: OpeningCall {
            term,
            expected,
            grid,
            replicas: replicas.iter().copied().map(NodeId).collect(),
        },
        release: RELEASE,
    }
}

fn register_of(chain: Chain, grid: &Grid, ours: bool) -> RegisterValue {
    let ok =
        |value: &RegisterValue, change: &Change| value.apply(change).expect("a register change");
    let mut value = ok(&RegisterValue::new(), &Change::Elect { holder: ME });
    let other =
        Grid::new(1, vec![OTHER_GRID.iter().copied().map(NodeId).collect()]).expect("a grid");
    match chain {
        Chain::Fresh => {}
        Chain::InForce => value = ok(&value, &call(1, None, grid.clone(), &REPLICAS)),
        Chain::Pending => {
            value = ok(&value, &call(1, None, grid.clone(), &REPLICAS));
            value = ok(&value, &call(1, Some(SegmentId(1)), other, &OTHER_REPLICAS));
        }
        Chain::Sealed | Chain::Retired => {
            value = ok(&value, &call(1, None, other, &OTHER_REPLICAS));
            value = ok(
                &value,
                &call(1, Some(SegmentId(1)), grid.clone(), &REPLICAS),
            );
            value = ok(
                &value,
                &Change::Seal {
                    segment: SegmentId(1),
                    end: Slot(4),
                },
            );
            if chain == Chain::Retired {
                value = ok(
                    &value,
                    &Change::Boundary {
                        segment: SegmentId(1),
                        state: BoundaryState::fresh(Leader::Unset),
                    },
                );
                value = ok(
                    &value,
                    &Change::Retire {
                        segment: SegmentId(1),
                        holders: 2,
                        unrepaired_damage: false,
                    },
                );
            }
        }
    }
    if !ours {
        value = ok(
            &value,
            &Change::Elect {
                holder: NodeId(901),
            },
        );
    }
    value
}

/// The liveness of a column with `up` members up, then `suspect` suspect,
/// then the rest down.
fn column_liveness(
    column: &[NodeId],
    up: usize,
    suspect: usize,
) -> impl Iterator<Item = (NodeId, Liveness)> + '_ {
    column.iter().enumerate().map(move |(i, m)| {
        let l = if i < up {
            Liveness::Up
        } else if i < up + suspect {
            Liveness::Suspect
        } else {
            Liveness::Down
        };
        (*m, l)
    })
}

/// The shapes of the sweep: `(F, P)` with `F <= 2` and `P <= 3`.
pub const SHAPES: &[(u8, u64)] = &[(1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (2, 3)];

/// The shapes of the mutant sweeps: every shape but the largest, so each
/// control runs in seconds.
pub const SMALL_SHAPES: &[(u8, u64)] = &[(1, 1), (1, 2), (1, 3), (2, 1), (2, 2)];

/// Every small view of `shapes`, each with the plain signals and with mixed
/// ones, given to `each` one at a time.
///
/// # Panics
///
/// When a shape is not a v1 grid.
pub fn views(shapes: &[(u8, u64)], mut each: impl FnMut(&Case)) {
    let mut n: u64 = 0;
    for &(f, cols) in shapes {
        let grid = grid_of(f, cols);
        let rows = grid.rows();
        let patterns: Vec<(usize, usize)> = (0..=rows)
            .flat_map(|up| (0..=rows - up).map(move |s| (up, s)))
            .collect();
        let columns = usize::try_from(cols).expect("a small count");
        let mut index = vec![0usize; columns];
        loop {
            for zones in [0usize, 3, 4] {
                for candidates in 0..3 {
                    for leader_live in [false, true] {
                        let mut liveness: BTreeMap<NodeId, Liveness> = BTreeMap::new();
                        for (c, column) in grid.columns().iter().enumerate() {
                            let (up, suspect) = patterns[index[c]];
                            liveness.extend(column_liveness(column, up, suspect));
                        }
                        for signals in [Signals::plain(), Signals::mixed(n)] {
                            n += 1;
                            each(&case(
                                &grid,
                                liveness.clone(),
                                zones,
                                candidates,
                                leader_live,
                                signals,
                            ));
                        }
                    }
                }
            }
            // The next pattern of the columns, as an odometer.
            let mut c = 0;
            loop {
                if c == columns {
                    break;
                }
                index[c] += 1;
                if index[c] < patterns.len() {
                    break;
                }
                index[c] = 0;
                c += 1;
            }
            if c == columns {
                break;
            }
        }
    }
}

fn case(
    grid: &Grid,
    mut liveness: BTreeMap<NodeId, Liveness>,
    zones: usize,
    candidates: u64,
    leader_live: bool,
    signals: Signals,
) -> Case {
    let mut labels = BTreeMap::new();
    if zones > 0 {
        // Row r of every column in zone r mod Z; the candidates fill the
        // zones from the last one down, so with four zones a candidate can
        // stand in the zone no member has.
        for column in grid.columns() {
            for (r, m) in column.iter().enumerate() {
                labels.insert(*m, zone(r % zones));
            }
        }
        for (i, c) in CANDIDATES.iter().enumerate() {
            labels.insert(NodeId(*c), zone((zones - 1 - i % zones) % zones));
        }
    }
    // Candidates: none; three up; three with the first down.
    let offered: Vec<NodeId> = if candidates == 0 {
        Vec::new()
    } else {
        CANDIDATES.iter().copied().map(NodeId).collect()
    };
    for (i, c) in CANDIDATES.iter().enumerate() {
        let l = if candidates == 2 && i == 0 {
            Liveness::Down
        } else {
            Liveness::Up
        };
        liveness.insert(NodeId(*c), l);
    }
    for set in [REPLICAS, OTHER_REPLICAS] {
        for (i, r) in set.iter().enumerate() {
            liveness.insert(
                NodeId(*r),
                if i == 1 {
                    signals.replicas
                } else {
                    Liveness::Up
                },
            );
        }
    }
    for m in OTHER_GRID {
        liveness.insert(NodeId(m), Liveness::Up);
    }
    let matchmakers =
        MatchmakerSet::new(MatchmakerGeneration(0), (1..=5).map(MatchmakerId).collect());
    Case {
        register: register_of(signals.chain, grid, signals.ours),
        grid: grid.clone(),
        liveness,
        zones: labels,
        candidates: offered,
        leader_live,
        matchmakers,
        matchmakers_live: (1..=signals.matchmakers_live).map(MatchmakerId).collect(),
        repairs: if signals.repair {
            BTreeSet::from([SegmentId(1)])
        } else {
            BTreeSet::new()
        },
        signals,
    }
}

impl Case {
    /// The view of this case.
    #[must_use]
    pub fn view(&self) -> View<'_> {
        View {
            me: ME,
            term: 1,
            register: &self.register,
            liveness: &self.liveness,
            zones: &self.zones,
            candidates: &self.candidates,
            leader_live: self.leader_live,
            matchmakers: &self.matchmakers,
            matchmakers_live: &self.matchmakers_live,
            repairs_pending: &self.repairs,
            data_lost: self.signals.data_lost,
            signal_age: self.signals.signal_age,
            since_last_reshape: self.signals.since_last_reshape,
            seal_age: self.signals.seal_age,
            token_free: self.signals.token_free,
        }
    }

    /// `decide` on this case, behind `mutant`.
    #[must_use]
    pub fn decide(&self, mutant: ControlMutant) -> Decision {
        let mut case = self.clone();
        match mutant {
            ControlMutant::None
            | ControlMutant::InvertLeader
            | ControlMutant::IgnoreReshaping
            | ControlMutant::StoppedAsUnplaceable
            | ControlMutant::IgnoreTerm => {}
            ControlMutant::IgnoreZones => case.zones.clear(),
            ControlMutant::IgnoreInterval => case.signals.since_last_reshape = None,
            ControlMutant::IgnoreAge => case.signals.signal_age = 0,
            ControlMutant::SuspectAsDown => {
                for l in case.liveness.values_mut() {
                    if *l == Liveness::Suspect {
                        *l = Liveness::Down;
                    }
                }
            }
        }
        if mutant == ControlMutant::InvertLeader {
            case.leader_live = !case.leader_live;
        }
        let mut view = case.view();
        if mutant == ControlMutant::IgnoreTerm {
            view.term = case.register.term().number;
            view.me = case.register.term().holder.unwrap_or(ME);
        }
        let mut decision = decide(&view, &TUNABLES);
        if mutant == ControlMutant::IgnoreReshaping
            && decision.action == Action::Wait(Wait::Reshaping)
        {
            // The bug: the guard of the reshape is gone, so the heal of the
            // grid in force is asked again.
            let mut free = case.clone();
            free.register = self.register_without_pending();
            let fresh = decide(&free.view(), &TUNABLES);
            if let Action::Reshape(mut call) = fresh.action {
                call.expected = case.register.newest().map(SegmentRecord::id);
                decision.action = Action::Reshape(call);
            }
        }
        if mutant == ControlMutant::StoppedAsUnplaceable
            && decision.action == Action::Wait(Wait::Hold(GridState::Stopped))
        {
            decision.action = Action::Wait(Wait::Hold(GridState::AtRiskUnplaceable));
        }
        decision
    }

    /// The register with only the segment in force: what a controller with
    /// no reshape guard would see.
    fn register_without_pending(&self) -> RegisterValue {
        let in_force = self
            .register
            .in_force()
            .expect("a segment in force")
            .clone();
        let term = self.register.term();
        let mut value = RegisterValue::new();
        for _ in 0..term.number {
            value = value
                .apply(&Change::Elect {
                    holder: term.holder.expect("a holder"),
                })
                .expect("elect");
        }
        value
            .apply(&Change::Reshape {
                opening: OpeningCall {
                    term: term.number,
                    expected: None,
                    ..in_force.opening().clone()
                },
                release: RELEASE,
            })
            .expect("a first segment")
    }
}

/// The rank of a journal state, from the healthiest.
#[must_use]
pub fn rank(state: GridState) -> u8 {
    match state {
        GridState::Healthy => 0,
        GridState::AtRisk | GridState::AtRiskUnplaceable => 1,
        GridState::WritesDegraded | GridState::ElectionBlocked => 2,
        GridState::Stopped => 3,
    }
}

/// Check every oracle on `case` behind `mutant`.
///
/// # Errors
///
/// The name of the first oracle that fails, with the case.
///
/// # Panics
///
/// When the register refuses a plan of `decide`: the plan is fenced on a
/// done read, so a refusal is a bug of the sweep.
pub fn check(case: &Case, mutant: ControlMutant) -> Result<(), String> {
    let decision = case.decide(mutant);
    let fail = |oracle: &str| Err(format!("{oracle}: {decision:?} on {case:#?}"));
    let expected = reference::decide(case);
    if (case.register.term().holder != Some(ME)) != (decision.action == Action::Stop) {
        return fail(FENCED);
    }
    let stale = case.signals.signal_age > TUNABLES.stale_after || case.signals.matchmakers_live < 3;
    let planned = !matches!(
        decision.action,
        Action::Stop | Action::Idle | Action::Wait(_)
    );
    if stale && planned {
        return fail(NO_STALE_PLAN);
    }
    if let Action::Reshape(call) = &decision.action
        && let Err(oracle) = check_reshape(case, mutant, call)
    {
        return fail(oracle);
    }
    if let Action::Rereplicate { from, to, .. } = &decision.action {
        let swapped = from.iter().zip(to).filter(|(a, b)| a != b);
        for (gone, new) in swapped {
            if case.liveness.get(gone) != Some(&Liveness::Down) {
                return fail(NO_SUSPECT_REPLACED);
            }
            if case.liveness.get(new) != Some(&Liveness::Up) {
                return fail(NO_UNSAFE_PLAN);
            }
        }
    }
    if let Some(worse) = one_more_down(case) {
        let before = reference::state(case);
        let after = reference::state(&worse);
        if rank(after) < rank(before) {
            return fail(MONOTONE);
        }
        let ours = |c: &Case| match c.decide(mutant).action {
            Action::Wait(Wait::Hold(s)) => Some(s),
            _ => None,
        };
        if let (Some(b), Some(a)) = (ours(case), ours(&worse))
            && rank(a) < rank(b)
        {
            return fail(MONOTONE);
        }
    }
    if decision != expected {
        return Err(format!(
            "{SAME_AS_REFERENCE}: decide {decision:?}, reference {expected:?} on {case:#?}"
        ));
    }
    Ok(())
}

/// The oracles of a reshape plan.
fn check_reshape(
    case: &Case,
    mutant: ControlMutant,
    call: &OpeningCall,
) -> Result<(), &'static str> {
    if call.term != 1 || call.expected != case.register.newest().map(SegmentRecord::id) {
        return Err(FENCED);
    }
    let reshaping = case
        .register
        .segments()
        .values()
        .any(|r| r.state() < SegmentState::InForce || r.state() == SegmentState::Sealed);
    let early = case
        .signals
        .since_last_reshape
        .is_some_and(|s| s < TUNABLES.min_reshape_interval);
    if reshaping || early || !case.signals.token_free {
        return Err(BOUNDED_RESHAPE_RATE);
    }
    let in_force = case.register.in_force().expect("a segment in force");
    if !reference::plan_is_safe(
        case,
        in_force.grid(),
        &call.grid,
        in_force.replicas(),
        &call.replicas,
    ) {
        return Err(NO_UNSAFE_PLAN);
    }
    let replaced: Vec<NodeId> = changed(in_force.grid(), &call.grid)
        .chain(
            in_force
                .replicas()
                .iter()
                .zip(&call.replicas)
                .filter(|(a, b)| a != b)
                .map(|(a, _)| *a),
        )
        .collect();
    if replaced
        .iter()
        .any(|m| case.liveness.get(m) != Some(&Liveness::Down))
    {
        return Err(NO_SUSPECT_REPLACED);
    }
    let mut after = case.clone();
    after.register = case
        .register
        .apply(&Change::Reshape {
            opening: call.clone(),
            release: RELEASE,
        })
        .expect("the register takes the plan");
    if matches!(after.decide(mutant).action, Action::Reshape(_)) {
        return Err(IDEMPOTENT);
    }
    Ok(())
}

fn changed<'a>(old: &'a Grid, new: &'a Grid) -> impl Iterator<Item = NodeId> + 'a {
    old.columns()
        .iter()
        .flatten()
        .zip(new.columns().iter().flatten())
        .filter(|(a, b)| a != b)
        .map(|(a, _)| *a)
}

/// The case with the first member of the grid that is up made down.
fn one_more_down(case: &Case) -> Option<Case> {
    let member = case
        .grid
        .columns()
        .iter()
        .flatten()
        .find(|m| case.liveness.get(m) == Some(&Liveness::Up))?;
    let mut worse = case.clone();
    worse.liveness.insert(*member, Liveness::Down);
    Some(worse)
}
