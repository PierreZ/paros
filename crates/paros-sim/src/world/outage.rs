//! The **correlated outage** (#263): on some seeds, once inside the chaos
//! window, every acceptor and every proxy leader loses power at the same
//! instant, and each comes back after its own delay. It is the power loss of
//! a whole rack, valid at any moment, and the one environmental fault that
//! empties every memory at once: what survives is exactly what the disks
//! hold. moonpool's attrition never takes more than `max_dead` processes of a
//! group, so this injector is paros-side until moonpool has a correlated
//! group outage (moonpool#311).
//!
//! The outage is what makes the CTRL shapes reachable without a script. While
//! every holder of a slot is down, latent damage aimed by the custody ledger
//! ([`super::StorageWorld::plan_outage_loss`]) is planned for each holder's next
//! boot, so no peer can repair a copy from memory before the last one is
//! lost. The [`LossShape`] drawn per seed says how much is lost:
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
//! A holder that comes back long after the others is the straggler: the
//! cluster must wait for it, then recover through it. Each draw is its own
//! BUGGIFY location, paired with a reachable where it takes effect; the audit
//! recognizes the shapes from what the losses leave ([`crate::audit`]).

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{
    FaultContext, FaultInjector, SimulationResult, StateHandle, TimeProvider, assert_reachable,
    buggify_knob, buggify_with_prob,
};

/// The well-known key of the run's outage draw.
const OUTAGE_KEY: &str = "paros-outage";

/// The latest an outage starts: inside the chaos window, as every new fault
/// is (an oracle constant's derivative, never buggified). The cluster
/// decides little early in the window, so the outage leans late.
const LATEST_START_MS: u64 = crate::CHAOS_DURATION_MS - 100;

/// When every node is back at the latest: early in the recovery tail, which
/// then still has its whole settle budget to converge in.
const BACK_BY_MS: u64 = crate::CHAOS_DURATION_MS + 2_000;

const _: () = assert!(LATEST_START_MS < BACK_BY_MS);

/// How often a drawn outage looks for a decided slot to lose.
const POLL: Duration = Duration::from_millis(10);

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

/// The run's outage, drawn once per seed by whoever asks first.
#[derive(Clone, Debug)]
pub(crate) struct Outage {
    /// When every node loses power, from the start of the run.
    at: Duration,
    /// How long each node stays down, by its rank among the outage's
    /// victims (the acceptors, then the proxies, each in IP order); a rank
    /// past the list stays down for the first entry.
    down: Vec<Duration>,
    /// How the outage loses copies.
    pub(crate) loss: LossShape,
}

/// The run's outage, if it has one (see the module doc).
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn outage(state: &StateHandle) -> Option<Outage> {
    let draw: Arc<Mutex<Option<Option<Outage>>>> =
        crate::state::published_arc(state, OUTAGE_KEY, || Mutex::new(None));
    let mut guard = draw.lock().unwrap_or_else(PoisonError::into_inner);
    guard.get_or_insert_with(draw_outage).clone()
}

fn draw_outage() -> Option<Outage> {
    if !buggify_with_prob!(0.5) {
        return None;
    }
    // The earliest start is a knob (the outage then waits for a slot to
    // lose); its ceiling keeps it inside the chaos window.
    let at = buggify_knob!(500_u64, 100_u64..LATEST_START_MS);
    // Each victim's downtime: one knob for the common return, and a
    // straggler (its own location) that returns last, long after. Floor
    // 10 ms: a reboot, not a skipped outage; the ceiling is `BACK_BY_MS`.
    let common = buggify_knob!(100_u64, 10_u64..600_u64);
    let straggler = buggify_with_prob!(0.5);
    let late = buggify_knob!(1_500_u64, 600_u64..(BACK_BY_MS - LATEST_START_MS));
    let victims = 2 * *crate::PROCESS_POOL_RANGE.end() + *crate::PROXY_POOL_RANGE.end();
    let straggler_rank = moonpool_sim::sim_random_range(0..*crate::PROCESS_POOL_RANGE.start());
    let down = (0..victims)
        .map(|rank| {
            let jitter = moonpool_sim::sim_random_range(0..common.max(1));
            let ms = if straggler && rank == straggler_rank {
                late
            } else {
                common + jitter
            };
            Duration::from_millis(ms.min(BACK_BY_MS - at))
        })
        .collect();
    let lossy = buggify_with_prob!(0.4);
    let loss = LossShape {
        recent: buggify_with_prob!(0.5),
        keep: lossy.then(|| usize::from(moonpool_sim::sim_random_bool(0.5))),
        prefer_removed: buggify_with_prob!(0.5),
    };
    Some(Outage {
        at: Duration::from_millis(at),
        down,
        loss,
    })
}

/// The injector (see the module doc): a fresh one per timeline.
pub(crate) struct CorrelatedOutage;

#[async_trait]
impl FaultInjector for CorrelatedOutage {
    fn name(&self) -> &'static str {
        "paros-correlated-outage"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn inject(&mut self, ctx: &FaultContext) -> SimulationResult<()> {
        let Some(outage) = outage(ctx.state()) else {
            return Ok(());
        };
        let time = ctx.time();
        let wait = outage.at.saturating_sub(time.now());
        if time.sleep(wait).await.is_err() {
            return Ok(());
        }
        // The cluster decides little under chaos: the outage waits, from
        // its drawn start, for a decided slot to lose, and is skipped when
        // none comes before the window closes.
        let latest = Duration::from_millis(LATEST_START_MS);
        while !has_target(ctx.state()) {
            if time.now() >= latest
                || ctx.chaos_shutdown().is_cancelled()
                || time.sleep(POLL).await.is_err()
            {
                return Ok(());
            }
        }
        let victims: Vec<String> = ctx
            .ips_in_group(crate::roles::ACCEPTOR_GROUP)
            .into_iter()
            .chain(ctx.ips_in_group(crate::roles::PROXY_GROUP))
            .collect();
        if victims.is_empty() {
            return Ok(());
        }
        for ip in &victims {
            ctx.crash(ip)?;
        }
        // The kills land at the next step: plan once every memory is gone.
        if time.sleep(Duration::from_millis(1)).await.is_err() {
            return Ok(());
        }
        assert_reachable!("storage: a correlated outage takes every acceptor down at once");
        plan_losses(ctx.state(), outage.loss);
        let start = time.now();
        let mut order: Vec<(Duration, &String)> = victims
            .iter()
            .enumerate()
            .map(|(rank, ip)| {
                let down = outage
                    .down
                    .get(rank)
                    .or(outage.down.first())
                    .copied()
                    .unwrap_or(Duration::from_millis(10));
                (start + down, ip)
            })
            .collect();
        order.sort();
        for (back, ip) in order {
            // Restarts are owed even once the window closed: a victim left
            // down would be a partition the recovery tail never heals.
            let _ = time.sleep(back.saturating_sub(time.now())).await;
            ctx.restart(ip)?;
        }
        Ok(())
    }
}

/// Whether some genesis journal's custody ledger holds a decided slot an
/// outage could lose.
fn has_target(state: &StateHandle) -> bool {
    crate::shape::journals(state)
        .ids
        .into_iter()
        .any(|journal| {
            let decided = crate::audit::audit_world_for(state, journal).decided_slots();
            super::storage_world_for(state, journal)
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .has_outage_target(&decided)
        })
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
