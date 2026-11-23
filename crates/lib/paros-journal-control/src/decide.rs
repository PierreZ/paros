//! **`decide`**: the pure decision of the v1 grid controller
//! (`docs/journal/06-controller.md` §J6, #1328 (paros-journal-control),
//! #1261 (grid controller decision tests)).
//!
//! The controller decides when and where; the protocol decides whether it
//! is safe. [`decide`] takes the controller's [`View`] (a done read of the
//! journal's matchmaker register and the signals the host gathered) and the
//! host's [`Tunables`], and gives one [`Decision`]: at most one action, and
//! the alerts of the view. It has no clock, no randomness and no state: the
//! same view gives the same decision.
//!
//! The v1 duties (Pierre, 2026-11-20, §J6): heal (a reshape, or the
//! re-replication of a sealed segment), scrub with repair through the
//! protocol, and hold and alert in every other case. No roll, no offload,
//! no service level objective, no scaling from load: the view carries no
//! latency, load or error signal, so no plan can follow one
//! (`NO_LOAD_PLAN`).
//!
//! The order of the rules, each a `probe!` that the coverage gate must
//! reach:
//!
//! 1. A controller whose term is not the register's term stops (§J6.1).
//! 2. The alerts: data loss, the matchmakers, the journal state, a stalled
//!    reshape.
//! 3. No plan while the register cannot take a write (the matchmakers are
//!    below a majority, `ControlFrozen`) or while a signal is stale.
//! 4. The cheap action first: the repair of a found damage (§J5.3).
//! 5. The re-replication of a retired segment's replica set (§J3.1, #1314),
//!    under a token of the rate limiter.
//! 6. The heal reshape (§J6.2, the journal states), under the reshape
//!    guard, the minimum interval and a token.

use std::collections::{BTreeMap, BTreeSet};

use paros_consensus::NodeId;
use paros_consensus::membership::{FailureDomain, MatchmakerId, MatchmakerSet};
use paros_journal::register::{RegisterValue, Term};
use paros_journal::segment::{OpeningCall, SegmentId, SegmentRecord, SegmentState};

use crate::measure::{GridState, Liveness, grid_state};
use crate::placement::{down, heal_grid, heal_one_replica, heal_replicas};

/// The host's numbers, in the host's unit of time. Each one is **Open**
/// (§J6): no number is set until Pierre or a source gives one, so this type
/// has no default and every test chooses its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Tunables {
    /// A signal older than this is stale: no plan is made on it.
    pub stale_after: u64,
    /// The minimum time between two reshapes of the journal.
    pub min_reshape_interval: u64,
    /// A seal older than this, with its segment not retired, is stalled.
    pub stall_window: u64,
}

/// What the controller knows when it decides. Every field is an input; the
/// host gathers it, `decide` only reads it.
#[derive(Clone, Copy, Debug)]
pub struct View<'a> {
    /// This controller.
    pub me: NodeId,
    /// The term this controller won.
    pub term: u64,
    /// The value of the journal's matchmaker register, from a done read.
    pub register: &'a RegisterValue,
    /// The liveness of the acceptors, the replicas and the candidates. A
    /// node that is not in the map is not known to be up and not known to
    /// be down: it counts as lost, and no heal replaces it.
    pub liveness: &'a BTreeMap<NodeId, Liveness>,
    /// The failure domain of each machine that has one.
    pub zones: &'a BTreeMap<NodeId, FailureDomain>,
    /// The machines that the host's placement offers, in its order.
    pub candidates: &'a [NodeId],
    /// A leader of the segment in force is live (`leader_known`).
    pub leader_live: bool,
    /// The journal's matchmaker set.
    pub matchmakers: &'a MatchmakerSet,
    /// Its members that are live.
    pub matchmakers_live: &'a BTreeSet<MatchmakerId>,
    /// The segments with a found damage that is not repaired
    /// (`repairs_pending`, from the scrub and the CTRL faulty records).
    pub repairs_pending: &'a BTreeSet<SegmentId>,
    /// A slot or a segment has no good copy (`data_lost`).
    pub data_lost: bool,
    /// The age of the oldest signal of the view (`signal_age`).
    pub signal_age: u64,
    /// The time since the last reshape of the journal, `None` for none
    /// (`since_last_reshape`).
    pub since_last_reshape: Option<u64>,
    /// The time since the seal of the oldest segment that is not retired
    /// (`seal_age`), `None` when no segment is sealed and not retired.
    pub seal_age: Option<u64>,
    /// The rate limiter has a token free for an expensive action
    /// (`tokens_held` below the cap).
    pub token_free: bool,
}

/// One decision: at most one action, and every alert of the view.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Decision {
    /// What the controller does now.
    pub action: Action,
    /// What the controller reports. An alert never changes the action.
    pub alerts: BTreeSet<Alert>,
}

/// What the controller does.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// The term of the register is not this controller's: stop (§J6.1).
    Stop,
    /// Nothing to do.
    Idle,
    /// Something is to do, and a rule holds it back.
    Wait(Wait),
    /// Repair the found damage of `segment` through the protocol (the CTRL
    /// repair probe, §J5.3). A cheap action: no token.
    Repair {
        /// The segment.
        segment: SegmentId,
    },
    /// Copy the sealed `segment` to a new replica set (§J3.1, #1314). An
    /// expensive action: it takes a token.
    Rereplicate {
        /// The sealed segment.
        segment: SegmentId,
        /// The set the register held when the controller read it.
        from: Vec<NodeId>,
        /// The new set: each member that is down replaced.
        to: Vec<NodeId>,
    },
    /// The fenced reshape call (§J6.1) that heals the grid and the replica
    /// set of the segment in force. An expensive action: it takes a token.
    Reshape(OpeningCall),
}

/// Why an action waits.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Wait {
    /// The register holds no segment: the journal's creation opens the
    /// first one, not a heal.
    NoSegment,
    /// The matchmakers are below a majority: no register write can run.
    ControlFrozen,
    /// A signal of the view is older than [`Tunables::stale_after`].
    Stale,
    /// The journal state holds every reshape (§J6.2): hold and alert.
    Hold(GridState),
    /// A segment is not retired: no second reshape (§J6.1).
    Reshaping,
    /// The last reshape is inside [`Tunables::min_reshape_interval`].
    Interval,
    /// The rate limiter has no token free.
    NoToken,
    /// A column is at risk only through members that are suspect: the
    /// controller does not replace a member that may come back.
    Suspect,
    /// No candidate can take the place of a replica that is down.
    NoReplicaPlacement,
    /// A replica of the segment in force is down, and no placement of the
    /// grid meets the zone rule.
    NoGridPlacement,
}

/// What the controller reports (§J6.2, the catalogue).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Alert {
    /// `data_lost` above 0: page.
    DataLost,
    /// The matchmakers are below a majority (`ControlFrozen`): page.
    ControlFrozen,
    /// The matchmakers are at a majority with no spare (`mm_spare = 0`).
    MatchmakersAtRisk,
    /// `AtRisk-Unplaceable`: no placement meets the zone rule.
    Unplaceable,
    /// `WritesDegraded`: v1 holds and alerts.
    WritesDegraded,
    /// `ElectionBlocked`: page.
    ElectionBlocked,
    /// `Stopped`: page.
    Stopped,
    /// `ReshapeStalled`: a seal older than the stall window.
    ReshapeStalled,
    /// No candidate can replace a replica that is down.
    ReplicasUnplaceable,
}

impl Alert {
    /// Whether the alert pages a human now.
    #[must_use]
    pub fn pages(self) -> bool {
        matches!(
            self,
            Self::DataLost | Self::ControlFrozen | Self::ElectionBlocked | Self::Stopped
        )
    }
}

/// The live acceptors of a group: the members that are up.
fn up(members: &[NodeId], liveness: &BTreeMap<NodeId, Liveness>) -> BTreeSet<NodeId> {
    members
        .iter()
        .copied()
        .filter(|m| liveness.get(m) == Some(&Liveness::Up))
        .collect()
}

/// The decision of the controller for `view` (see the module doc).
///
/// # Panics
///
/// When an assertion on its own postconditions fails: a programmer error,
/// never an operating condition.
#[must_use]
pub fn decide(view: &View<'_>, tunables: &Tunables) -> Decision {
    let mut alerts = BTreeSet::new();
    let action = act(view, tunables, &mut alerts);
    assert!(
        action != Action::Stop || alerts.is_empty(),
        "a deposed controller reports nothing"
    );
    if let Action::Reshape(call) = &action {
        assert!(
            call.term == view.term,
            "a call carries the controller's term"
        );
        assert!(
            call.expected == view.register.newest().map(SegmentRecord::id),
            "a call expects the newest segment"
        );
        assert!(
            view.token_free,
            "an expensive action takes a token of the rate limiter"
        );
    }
    if let Action::Rereplicate { from, to, .. } = &action {
        assert!(
            from.len() == to.len(),
            "a re-replication keeps the size of the set"
        );
        assert!(from != to, "a re-replication changes the set");
        assert!(view.token_free, "an expensive action takes a token");
    }
    Decision { action, alerts }
}

fn act(view: &View<'_>, tunables: &Tunables, alerts: &mut BTreeSet<Alert>) -> Action {
    let ours = Term {
        number: view.term,
        holder: Some(view.me),
    };
    if view.register.term() != ours {
        probe!(reachable, "decide: a deposed controller stops");
        return Action::Stop;
    }
    if view.data_lost {
        probe!(reachable, "decide: data loss pages");
        alerts.insert(Alert::DataLost);
    }
    let frozen = matchmakers(view, alerts);
    let Some(in_force) = view.register.in_force() else {
        probe!(reachable, "decide: no segment in force");
        return Action::Wait(Wait::NoSegment);
    };
    let grid = in_force.grid();
    let members: Vec<NodeId> = grid.columns().iter().flatten().copied().collect();
    let placeable = heal_grid(grid, view.liveness, view.candidates, view.zones).is_some();
    let state = grid_state(
        grid,
        &up(&members, view.liveness),
        view.leader_live,
        placeable,
    );
    state_alert(state, alerts);
    let reshaping = view
        .register
        .segments()
        .values()
        .any(|r| r.id() != in_force.id() && r.state() < SegmentState::Retired);
    if reshaping && view.seal_age.is_some_and(|age| age > tunables.stall_window) {
        probe!(reachable, "decide: a stalled reshape alerts");
        alerts.insert(Alert::ReshapeStalled);
    }
    if frozen {
        probe!(reachable, "decide: control frozen holds every plan");
        return Action::Wait(Wait::ControlFrozen);
    }
    if view.signal_age > tunables.stale_after {
        probe!(reachable, "decide: stale signals hold every plan");
        return Action::Wait(Wait::Stale);
    }
    if let Some(segment) = view.repairs_pending.first() {
        probe!(reachable, "decide: repair a found damage first");
        return Action::Repair { segment: *segment };
    }
    if let Some(action) = rereplicate(view, alerts) {
        return action;
    }
    heal(view, tunables, in_force, state, reshaping, alerts)
}

/// The alerts of the matchmakers; whether they are below a majority.
fn matchmakers(view: &View<'_>, alerts: &mut BTreeSet<Alert>) -> bool {
    let live: BTreeSet<MatchmakerId> = view
        .matchmakers
        .members()
        .iter()
        .copied()
        .filter(|m| view.matchmakers_live.contains(m))
        .collect();
    if !view.matchmakers.has_quorum(&live) {
        probe!(reachable, "decide: matchmakers below a majority page");
        alerts.insert(Alert::ControlFrozen);
        return true;
    }
    let no_spare = live.iter().any(|gone| {
        let mut rest = live.clone();
        rest.remove(gone);
        !view.matchmakers.has_quorum(&rest)
    });
    if no_spare {
        probe!(reachable, "decide: matchmakers with no spare alert");
        alerts.insert(Alert::MatchmakersAtRisk);
    }
    false
}

fn state_alert(state: GridState, alerts: &mut BTreeSet<Alert>) {
    let alert = match state {
        GridState::Healthy | GridState::AtRisk => return,
        GridState::AtRiskUnplaceable => {
            probe!(reachable, "decide: state at risk and unplaceable");
            Alert::Unplaceable
        }
        GridState::WritesDegraded => {
            probe!(reachable, "decide: state writes degraded");
            Alert::WritesDegraded
        }
        GridState::ElectionBlocked => {
            probe!(reachable, "decide: state election blocked");
            Alert::ElectionBlocked
        }
        GridState::Stopped => {
            probe!(reachable, "decide: state stopped");
            Alert::Stopped
        }
    };
    alerts.insert(alert);
}

/// The re-replication of the first retired segment, not trimmed, whose
/// replica set has a member that is down. A sealed segment still has its
/// acceptors: it retires first, at `F + 1` holders, and the register heals
/// only a retired record (§J3.4, #1314).
fn rereplicate(view: &View<'_>, alerts: &mut BTreeSet<Alert>) -> Option<Action> {
    let record = view.register.segments().values().find(|r| {
        r.state() == SegmentState::Retired && !down(r.replicas(), view.liveness).is_empty()
    })?;
    let Some(to) = heal_one_replica(
        record.replicas(),
        view.liveness,
        view.candidates,
        view.zones,
    ) else {
        probe!(
            reachable,
            "decide: no candidate for a sealed segment's replica"
        );
        alerts.insert(Alert::ReplicasUnplaceable);
        return Some(Action::Wait(Wait::NoReplicaPlacement));
    };
    if !view.token_free {
        probe!(reachable, "decide: a re-replication waits for a token");
        return Some(Action::Wait(Wait::NoToken));
    }
    probe!(reachable, "decide: re-replicate a sealed segment");
    Some(Action::Rereplicate {
        segment: record.id(),
        from: record.replicas().to_vec(),
        to,
    })
}

/// The heal reshape of the segment in force.
fn heal(
    view: &View<'_>,
    tunables: &Tunables,
    in_force: &SegmentRecord,
    state: GridState,
    reshaping: bool,
    alerts: &mut BTreeSet<Alert>,
) -> Action {
    let grid = in_force.grid();
    let members: Vec<NodeId> = grid.columns().iter().flatten().copied().collect();
    let acceptors_down = !down(&members, view.liveness).is_empty();
    let replicas_down = !down(in_force.replicas(), view.liveness).is_empty();
    match state {
        GridState::Healthy if !replicas_down => {
            probe!(reachable, "decide: healthy, nothing to do");
            return Action::Idle;
        }
        GridState::Healthy => probe!(
            reachable,
            "decide: a replica of the segment in force is down"
        ),
        GridState::AtRisk if !acceptors_down && !replicas_down => {
            probe!(reachable, "decide: at risk through suspects only");
            return Action::Wait(Wait::Suspect);
        }
        GridState::AtRisk => probe!(reachable, "decide: at risk with a member down"),
        held => {
            probe!(reachable, "decide: the journal state holds the heal");
            return Action::Wait(Wait::Hold(held));
        }
    }
    if reshaping {
        probe!(reachable, "decide: a heal waits for the retire");
        return Action::Wait(Wait::Reshaping);
    }
    if view
        .since_last_reshape
        .is_some_and(|since| since < tunables.min_reshape_interval)
    {
        probe!(reachable, "decide: a heal waits for the minimum interval");
        return Action::Wait(Wait::Interval);
    }
    if !view.token_free {
        probe!(reachable, "decide: a heal waits for a token");
        return Action::Wait(Wait::NoToken);
    }
    let Some(new_grid) = heal_grid(grid, view.liveness, view.candidates, view.zones) else {
        // Only a healthy grid gets here with no placement: a replica is
        // down, and a member that is down in a column with a spare cannot
        // be placed.
        assert!(state == GridState::Healthy, "AtRisk has a placement");
        probe!(
            reachable,
            "decide: no placement for the grid of a healthy heal"
        );
        alerts.insert(Alert::Unplaceable);
        return Action::Wait(Wait::NoGridPlacement);
    };
    let Some(replicas) = heal_replicas(
        in_force.replicas(),
        view.liveness,
        view.candidates,
        view.zones,
    ) else {
        probe!(reachable, "decide: no candidate for a replica of the heal");
        alerts.insert(Alert::ReplicasUnplaceable);
        return Action::Wait(Wait::NoReplicaPlacement);
    };
    probe!(reachable, "decide: heal reshape");
    Action::Reshape(OpeningCall {
        term: view.term,
        expected: view.register.newest().map(SegmentRecord::id),
        grid: new_grid,
        replicas,
    })
}

/// The message of every `probe!` of [`decide`]: the coverage gate of
/// `journal-sim` requires each one, with no excused probe.
pub const PROBES: &[&str] = &[
    "decide: a deposed controller stops",
    "decide: data loss pages",
    "decide: no segment in force",
    "decide: matchmakers below a majority page",
    "decide: matchmakers with no spare alert",
    "decide: state at risk and unplaceable",
    "decide: state writes degraded",
    "decide: state election blocked",
    "decide: state stopped",
    "decide: a stalled reshape alerts",
    "decide: control frozen holds every plan",
    "decide: stale signals hold every plan",
    "decide: repair a found damage first",
    "decide: no candidate for a sealed segment's replica",
    "decide: a re-replication waits for a token",
    "decide: re-replicate a sealed segment",
    "decide: healthy, nothing to do",
    "decide: a replica of the segment in force is down",
    "decide: at risk through suspects only",
    "decide: at risk with a member down",
    "decide: the journal state holds the heal",
    "decide: a heal waits for the retire",
    "decide: a heal waits for the minimum interval",
    "decide: a heal waits for a token",
    "decide: no placement for the grid of a healthy heal",
    "decide: no candidate for a replica of the heal",
    "decide: heal reshape",
];

#[cfg(test)]
mod tests;
