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
//! - **prefer removed**: the one copy left clean is on a member of the
//!   configuration that decided the slot, outside the one the operator last
//!   installed, when there is one (the
//!   departed straggler, composed with the owner's reconfiguration right
//!   after its claim, `ChainConfig::reconfigure_after_claim`, and
//!   `withhold_gc`); a removed holder's copy kept clean beside a quorum of
//!   clean members would be no shape, so the draw spends the loss budget
//!   on exactly that one copy.
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
    sim_random_range,
};

/// When an outage may strike, from the opening of the chaos window: late,
/// because the cluster decides little early in the window (over a thousand
/// seeds, a tenth of the first decisions land before 3.5 s and half after
/// 5 s; the departed-straggler shape needs a claim, a reconfiguration and
/// its election first, 2 to 3.5 s on a kind seed), and inside it, as every
/// new fault is.
const START: std::ops::Range<Duration> =
    Duration::from_millis(2_500)..Duration::from_millis(crate::CHAOS_DURATION_MS - 100);

/// How long each victim stays down. Floor 20 ms: a reboot, and longer than
/// [`POLL`], so the losses are planned while every victim is still down.
pub(super) const DOWN: std::ops::Range<Duration> =
    Duration::from_millis(20)..Duration::from_millis(600);

/// How long the straggler stays down: back last, early in the recovery
/// tail, which then still has its whole settle budget to converge in.
pub(super) const STRAGGLER: std::ops::Range<Duration> =
    Duration::from_millis(600)..Duration::from_millis(2_500);

/// How often [`OutageLosses`] looks for the outage's landing.
pub(super) const POLL: Duration = Duration::from_millis(5);

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
/// Its bools are a flag set, one coin per ingredient.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub(crate) struct LossShape {
    /// The most recent slot the ledger holds, rather than a uniform one.
    pub(crate) recent: bool,
    /// The clean copies to leave under the loss budget's extreme: `0` or
    /// `1`. `None` keeps the usual budget (a quorum's copies stay clean).
    pub(crate) keep: Option<usize>,
    /// Leave the one clean copy on a node outside the last installed
    /// configuration, when one holds the slot: the departed straggler,
    /// CTRL Case 3 across a reconfiguration. Only then does it spend the
    /// loss budget; with no removed holder it leaves the budget alone.
    pub(crate) prefer_removed: bool,
    /// Aim at a slot a member of its deciding configuration never held,
    /// every holder's copy settled: the bare quorum, whose Phase-1 tally
    /// can then read `faulty, faulty, none` once every copy is lost.
    pub(crate) short: bool,
    /// With [`Self::prefer_removed`], also leave clean every copy on a
    /// spare: a holder neither the last installed configuration nor the
    /// deciding one names. A spare that took a re-proposal holds the slot
    /// above the straggler's ballot, and no Phase 1 asks it, so the
    /// straggler sets the CTRL threshold alone and the lost copies above it
    /// wait (#375).
    pub(crate) spare_kept: bool,
}

impl LossShape {
    /// The bare-quorum scenario's loss (see [`draw_loss`]): the most recent
    /// slot, no clean copy left.
    pub(crate) const BARE_QUORUM: Self = Self {
        recent: true,
        keep: Some(0),
        prefer_removed: false,
        short: true,
        spare_kept: false,
    };

    /// The departed-straggler scenario's loss
    /// (`crate::shape::departed_straggler`): the most recent slot, down to
    /// one clean copy on a member a reconfiguration removed.
    pub(crate) const DEPARTED_STRAGGLER: Self = Self {
        recent: true,
        keep: None,
        prefer_removed: true,
        short: false,
        spare_kept: false,
    };

    /// The shape with its spares' copies kept clean on a coin, its own
    /// BUGGIFY location: a spare's copy above the straggler's ballot is one
    /// ingredient of #375's shape, which a 3,000-seed hunt met once.
    pub(crate) fn with_spares_kept(self) -> Self {
        let spare_kept = self.prefer_removed && buggify_with_prob!(0.5);
        if spare_kept {
            assert_reachable!("storage: an outage draws a loss that keeps the spares' copies");
        }
        Self { spare_kept, ..self }
    }

    /// How many slots of one journal may lose their clean quorum: zero
    /// under the usual budget. Floor `0`: the budget a quorum of clean
    /// copies defends. Extreme `2`: one or a few slots, never more — a lost
    /// slot freezes its journal's chosen prefix for good, and only a
    /// genesis journal ever spends it (the control journals are quiet seats
    /// with no budget), so the run's control state is never lost.
    pub(crate) fn loss_budget(&self) -> usize {
        if self.keep.is_some() || self.prefer_removed {
            2
        } else {
            0
        }
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
                    let loss = if crate::shape::departed_straggler(ctx.state()) {
                        assert_reachable!(
                            "storage: an outage lands on a seed drawing the departed-straggler scenario"
                        );
                        LossShape::DEPARTED_STRAGGLER.with_spares_kept()
                    } else if crate::shape::bare_quorum(ctx.state()) {
                        assert_reachable!(
                            "storage: an outage lands on a seed drawing the bare-quorum scenario"
                        );
                        LossShape::BARE_QUORUM
                    } else {
                        draw_loss()
                    };
                    let _ = plan_losses(ctx.state(), loss);
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
    // The bare-quorum scenario, its own BUGGIFY location: the most recent
    // slot, decided on a quorum short of every member, left no clean copy,
    // so its tally is `faulty, faulty, none` and the no-op fill must be
    // refused. Drawn whole, not as the product of the coins below.
    if buggify_with_prob!(0.25) {
        assert_reachable!("storage: an outage draws the bare-quorum loss");
        return LossShape::BARE_QUORUM;
    }
    let lossy = buggify_with_prob!(0.4);
    LossShape {
        recent: buggify_with_prob!(0.5),
        keep: lossy.then(|| usize::from(moonpool_sim::sim_random_bool(0.5))),
        prefer_removed: buggify_with_prob!(1.0),
        short: false,
        spare_kept: false,
    }
    .with_spares_kept()
}

/// Plan the outage's losses in every genesis journal (see the module doc).
/// Returns the holders the main journal's plan left clean: the copies the
/// cluster must wait for.
pub(super) fn plan_losses(state: &StateHandle, loss: LossShape) -> Vec<u64> {
    let plan = crate::shape::journals(state);
    let mut kept = Vec::new();
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
            if journal == plan.main {
                kept = planned
                    .holders
                    .iter()
                    .copied()
                    .filter(|node| !planned.damaged.contains(node))
                    .collect();
            }
        }
    }
    kept
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

/// Every acceptor and proxy leader: the processes a strike takes down.
fn victims(ctx: &FaultContext) -> Vec<String> {
    let mut victims = ctx.ips_in_group(crate::roles::ACCEPTOR_GROUP);
    victims.extend(ctx.ips_in_group(crate::roles::PROXY_GROUP));
    victims
}

/// Whether a [`strike`] may take every victim down now: no victim has a
/// commit in flight whose cut a budget would have to pay (#332,
/// [`super::cut`]).
pub(super) fn may_strike(ctx: &FaultContext) -> bool {
    super::cut::outage_permitted(ctx.state(), &victims(ctx))
}

/// Take every acceptor and proxy leader down now, each back after its own
/// delay in [`DOWN`], one acceptor straggling in [`STRAGGLER`]: the one
/// clean copy the loss left (`kept`, node ids, which are ranks in the
/// acceptor group), so the cluster must recover through the prior
/// configuration while it is still down (#267), else a random acceptor.
///
/// The caller strikes only once [`may_strike`] permits it, with no await
/// between them (#332).
pub(super) fn strike(ctx: &FaultContext, kept: &[u64]) -> SimulationResult<()> {
    super::cut::assert_outage_permitted(ctx.state(), &victims(ctx));
    let acceptors = ctx.ips_in_group(crate::roles::ACCEPTOR_GROUP);
    let proxies = ctx.ips_in_group(crate::roles::PROXY_GROUP);
    let clean = match kept {
        [node] => usize::try_from(*node)
            .ok()
            .filter(|rank| *rank < acceptors.len()),
        _ => None,
    };
    if clean.is_some() {
        assert_reachable!("outage: the departed straggler's clean holder is back last");
    }
    let straggler = clean.unwrap_or_else(|| {
        usize::try_from(sim_random_range(0..acceptors.len().max(1) as u64)).unwrap_or(0)
    });
    let millis = |range: &std::ops::Range<Duration>| {
        Duration::from_millis(sim_random_range(
            u64::try_from(range.start.as_millis()).unwrap_or(0)
                ..u64::try_from(range.end.as_millis()).unwrap_or(1),
        ))
    };
    for (rank, ip) in acceptors.iter().enumerate() {
        let down = if rank == straggler {
            millis(&STRAGGLER)
        } else {
            millis(&DOWN)
        };
        ctx.crash_for(ip, down)?;
    }
    for ip in &proxies {
        ctx.crash_for(ip, millis(&DOWN))?;
    }
    Ok(())
}
