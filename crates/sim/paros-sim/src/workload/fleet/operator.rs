//! **The operator lane** (#489 (operator workload)): on a run that draws
//! it ([`crate::shape::operator_lane`]), client 0 runs an operator of its
//! own in a task beside its data operations. The operator takes one admin
//! process after another for the whole chaos phase: an admission, a
//! drain, a maintenance window or another cell (`ADMIT`), a tenant
//! operation or the second cell's next phase (`TENANT`), a coordinator
//! candidacy (`ELECTION`), a tag (`TAG`). So a long admin process stays
//! open while every client writes and reads, also on a seed with one
//! client, where the client's own steps never overlap its admin steps.
//!
//! The operator is a [`FleetOps`] of its own, under its own client id
//! ([`OPERATOR_CLIENT`]): its calls join the control journals' histories
//! as one sequential client, apart from the client whose task runs it.
//! It drives only the shipped `paros::client` code, as `parosctl` does.
//!
//! It publishes the admin process it has open ([`Action`]): a data client
//! whose write is acknowledged while one is open reaches that action's
//! gate ([`note_acknowledged`]).

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use moonpool_sim::{SimContext, StateHandle, TimeProvider as _, assert_reachable};
use paros::client::checkpoint::CheckpointPolicy;
use paros::client::election::ElectionTunables;

use super::FleetOps;

/// The client id the operator lane's calls log under in the control
/// journals' histories: far above the workload's clients, below the
/// machines' own (`crate::machine::MACHINE_CLIENT_BASE`).
pub(in crate::workload) const OPERATOR_CLIENT: u64 = 1 << 24;

/// The pause between two admin processes of the lane, at most: a lane
/// that never pauses would starve the run of data steps on one client.
const PAUSE_MS: std::ops::Range<u64> = 50..1_500;

/// The admin process the lane has open.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::workload) enum Action {
    /// `cell init`, when the lane found no cell formed.
    Init,
    /// An admission, a drain, a maintenance window or another cell.
    Admit,
    /// A tenant operation, or the second cell's next phase.
    Tenant,
    /// A coordinator candidacy.
    Election,
    /// Tags on a tenant, a cell, a machine or a journal.
    Tag,
}

const OPEN_KEY: &str = "paros-operator-open";

/// The action the lane has open now, shared by every client of the run.
fn open(state: &StateHandle) -> Arc<Mutex<Option<Action>>> {
    crate::state::published_arc(state, OPEN_KEY, || Mutex::new(None))
}

fn set_open(state: &StateHandle, action: Option<Action>) {
    *open(state).lock().unwrap_or_else(PoisonError::into_inner) = action;
}

/// A data client's write was acknowledged: the gate of the admin process
/// the operator lane has open, if any.
pub(in crate::workload) fn note_acknowledged(state: &StateHandle) {
    let action = *open(state).lock().unwrap_or_else(PoisonError::into_inner);
    match action {
        None => {}
        Some(Action::Init) => {
            assert_reachable!("operator: a write is acknowledged while an init is open");
        }
        Some(Action::Admit) => {
            assert_reachable!(
                "operator: a write is acknowledged while an admission or a drain is open"
            );
        }
        Some(Action::Tenant) => {
            assert_reachable!("operator: a write is acknowledged while a tenant operation is open");
        }
        Some(Action::Election) => {
            assert_reachable!("operator: a write is acknowledged while a candidacy is open");
        }
        Some(Action::Tag) => {
            assert_reachable!("operator: a write is acknowledged while a tag is open");
        }
    }
}

/// What the lane runs with: the checkpoint policy and the election
/// tunables of the client whose task runs it, and when it stops.
pub(in crate::workload) struct LaneConfig {
    pub(in crate::workload) policy: CheckpointPolicy,
    pub(in crate::workload) election: ElectionTunables,
    pub(in crate::workload) run_end: Duration,
}

/// Run the operator lane until the run's end: one admin process after
/// another, each drawn on the lane's own coins, with a short pause
/// between two. Returns the operator, so the client whose task ran it
/// finishes the operation it stopped in during the settle tail
/// ([`FleetOps::settle`]).
pub(in crate::workload) async fn run(
    ctx: SimContext,
    mut ops: FleetOps,
    config: LaneConfig,
) -> FleetOps {
    assert_reachable!("operator: a run has an operator lane");
    while ctx.time().now() < config.run_end && crate::in_chaos() {
        if ctx.shutdown().is_cancelled() {
            break;
        }
        let draw = moonpool_sim::sim_random::<u64>();
        let class = moonpool_sim::sim_random::<u64>();
        let action = if ops.initialized {
            match moonpool_sim::sim_random_range(0_u32..10) {
                0..=3 => Action::Admit,
                4..=6 => Action::Tenant,
                7..=8 => Action::Election,
                _ => Action::Tag,
            }
        } else {
            Action::Init
        };
        set_open(ctx.state(), Some(action));
        match action {
            Action::Init => ops.init(&ctx, config.policy, draw).await,
            Action::Admit => ops.admit(&ctx, config.policy, draw).await,
            Action::Tenant => ops.tenant(&ctx, config.policy, (class, draw)).await,
            Action::Election => ops.elect(&ctx, config.policy, config.election, draw).await,
            Action::Tag => ops.tag(&ctx, config.policy, draw).await,
        }
        set_open(ctx.state(), None);
        let pause = moonpool_sim::sim_random_range(PAUSE_MS);
        ctx.time()
            .sleep(Duration::from_millis(pause))
            .await
            .unwrap_or(());
    }
    set_open(ctx.state(), None);
    ops
}
