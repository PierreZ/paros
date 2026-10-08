//! The **correlated outage**'s losses (#263). moonpool's `Chaos::Outage`
//! (moonpool#311, registered by [`regime`]) takes every acceptor and every
//! proxy leader down at one instant on some seeds, each back after its own
//! delay, one straggler last: the power loss of a whole rack, the one
//! environmental fault that empties every memory at once, so what survives
//! is exactly what the disks hold.
//!
//! That is what makes the CTRL shapes reachable without a script. The
//! moment every victim is down (moonpool's `OutageLanded`), [`OutageLosses`]
//! plans latent damage aimed by the custody ledger
//! ([`super::StorageWorld::plan_outage_loss`]) for each holder's next boot,
//! so no peer can repair a copy from memory before the last one is lost.
//! The [`LossShape`] drawn then says how much is lost:
//!
//! - **aim**: the slot is the most recent one the ledger holds, or one drawn
//!   uniformly among them;
//! - **keep**: how many clean copies are left. Under the usual budget a
//!   quorum's copies stay clean; under the loss budget's extreme
//!   ([`LossShape::loss_budget`]) a bounded number of slots may keep one
//!   clean copy or none (CTRL's E1 family: one clean copy recovers intact,
//!   none is waited on, never fabricated);
//! - **prefer removed**: the copy left clean is one on a node outside the
//!   configuration the operator last installed, when there is one (the
//!   departed straggler, composed with `RECONFIGURE` and `withhold_gc`).
//!
//! The straggler, back last, is the one the cluster must wait for, then
//! recover through. Each draw is its own BUGGIFY location, paired with a
//! reachable where it takes effect; the audit recognizes the shapes from what
//! the losses leave ([`crate::audit`]).

use std::sync::PoisonError;
use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{
    Chaos, ChaosMode, FaultContext, FaultInjector, OUTAGE_STATE_KEY, Outage, OutageLanded,
    SimulationResult, StateHandle, TimeProvider, assert_reachable, buggify_with_prob,
};

/// When an outage may strike, from the opening of the chaos window: late,
/// because the cluster decides little early in the window (its first
/// decisions land around 3 to 6 s), and inside it, as every new fault is.
const START: std::ops::Range<Duration> =
    Duration::from_millis(1_500)..Duration::from_millis(crate::CHAOS_DURATION_MS - 100);

/// How long each victim stays down. Floor 20 ms: a reboot, and longer than
/// [`POLL`], so the losses are planned while every victim is still down.
const DOWN: std::ops::Range<Duration> = Duration::from_millis(20)..Duration::from_millis(600);

/// How long the straggler stays down: back last, early in the recovery
/// tail, which then still has its whole settle budget to converge in.
const STRAGGLER: std::ops::Range<Duration> =
    Duration::from_millis(600)..Duration::from_millis(2_500);

/// How often [`OutageLosses`] looks for the outage's landing.
const POLL: Duration = Duration::from_millis(5);

const _: () = assert!(POLL.as_millis() < DOWN.start.as_millis());

/// The outage regime of [`crate::chaos_surfaces`]: every acceptor and proxy
/// leader, swarm-masked per seed (moonpool keeps it on half the seeds and
/// its straggler on half of those).
pub(crate) fn regime() -> Chaos {
    Chaos::Outage {
        config: Outage {
            groups: vec![
                crate::roles::ACCEPTOR_GROUP.to_string(),
                crate::roles::PROXY_GROUP.to_string(),
            ],
            probability: 1.0,
            start: START,
            down: DOWN,
            straggler: Some(STRAGGLER),
        },
        mode: ChaosMode::Swarm,
    }
}

/// How a seed's outage loses copies (see the module doc).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LossShape {
    /// The most recent slot the ledger holds, rather than a uniform one.
    pub(crate) recent: bool,
    /// The clean copies to leave under the loss budget's extreme: `0` or
    /// `1`. `None` keeps the usual budget (a quorum's copies stay clean).
    pub(crate) keep: Option<usize>,
    /// Leave the clean copy on a node outside the last installed
    /// configuration, when one holds the slot.
    pub(crate) prefer_removed: bool,
}

impl LossShape {
    /// How many slots of one journal may lose their clean quorum: zero
    /// under the usual budget. Floor `0`: the budget a quorum of clean
    /// copies defends. Extreme `2`: one or a few slots, never more — a lost
    /// slot freezes its journal's chosen prefix for good, and only a
    /// genesis journal ever spends it (the control journals are quiet seats
    /// with no budget), so the run's control state is never lost.
    pub(crate) fn loss_budget(&self) -> usize {
        if self.keep.is_some() { 2 } else { 0 }
    }
}

/// The injector that plans an outage's losses (see the module doc): a
/// fresh one per timeline.
pub(crate) struct OutageLosses;

#[async_trait]
impl FaultInjector for OutageLosses {
    fn name(&self) -> &'static str {
        "paros-outage-losses"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn inject(&mut self, ctx: &FaultContext) -> SimulationResult<()> {
        let time = ctx.time();
        loop {
            if let Some(landed) = ctx.state().get::<OutageLanded>(OUTAGE_STATE_KEY) {
                if landed
                    .victims
                    .iter()
                    .any(|ip| ctx.ips_in_group(crate::roles::ACCEPTOR_GROUP).contains(ip))
                {
                    assert_reachable!(
                        "storage: a correlated outage takes every acceptor down at once"
                    );
                    plan_losses(ctx.state(), draw_loss());
                }
                return Ok(());
            }
            if ctx.chaos_shutdown().is_cancelled() || time.sleep(POLL).await.is_err() {
                return Ok(());
            }
        }
    }
}

/// The outage's loss shape, drawn when it lands (see the module doc).
fn draw_loss() -> LossShape {
    let lossy = buggify_with_prob!(0.4);
    LossShape {
        recent: buggify_with_prob!(0.5),
        keep: lossy.then(|| usize::from(moonpool_sim::sim_random_bool(0.5))),
        prefer_removed: buggify_with_prob!(0.5),
    }
}

/// Plan the outage's losses in every genesis journal (see the module doc).
fn plan_losses(state: &StateHandle, loss: LossShape) {
    let plan = crate::shape::journals(state);
    for journal in plan.ids {
        let audit = crate::audit::audit_world_for(state, journal);
        let decided = audit.decided_slots();
        let world = super::storage_world_for(state, journal);
        let planned = world
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .plan_outage_loss(loss, &decided);
        if let Some(planned) = planned {
            audit.note_outage_loss(planned.slot, &planned.holders, &planned.damaged);
        }
    }
}

/// What [`super::StorageWorld::plan_outage_loss`] planned for one journal.
#[derive(Clone, Debug)]
pub(crate) struct PlannedLoss {
    /// The slot whose copies are lost.
    pub(crate) slot: u64,
    /// Every acceptor holding it (node ids).
    pub(crate) holders: Vec<u64>,
    /// The holders whose copy is damaged at their next boot.
    pub(crate) damaged: Vec<u64>,
}
