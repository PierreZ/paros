//! The **correlated outage**'s losses (#263). moonpool's `Chaos::Outage`
//! (moonpool#311, registered by [`regime`]) takes every acceptor and every
//! proxy leader down at one instant on some seeds, each back after its own
//! delay, one straggler last: the power loss of a whole rack, the one
//! environmental fault that empties every memory at once, so what survives
//! is exactly what the disks hold.
//!
//! The moment every victim is down (moonpool's `OutageLanded`), [`OutageLosses`]
//! plans latent damage aimed by the custody ledger
//! ([`super::StorageWorld::plan_outage_loss`]) for each holder's next boot,
//! so no peer can repair a copy from memory before the last one is lost.
//! The [`LossShape`] drawn then aims the loss: at the most recent slot the
//! ledger holds, or at one drawn uniformly among them.
//!
//! A plan never takes a slot's clean quorum: it loses the copies of at most
//! the holders a quorum of clean copies can spare, and never below the
//! copies the journal's shard needs. A loss beyond a set's survive number
//! is a fault the cell does not promise to survive, and the simulator
//! refuses it first (step 1.9 of the rework, decision 16, decided on
//! 2026-11-17).
//!
//! A second regime, [`machine_regime`], takes every machine down at one
//! instant on a larger cell (step 1.3, `crate::shape::larger`): the seed
//! plan calms it on every other seed. It plans no loss: [`OutageLosses`]
//! waits for a landing that takes acceptors down.
//!
//! The straggler, back last, is the one the cluster must wait for. Each
//! draw is its own BUGGIFY location, paired with a reachable where it takes
//! effect; the audit judges what the losses leave ([`crate::audit`]).

use std::sync::PoisonError;
use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{
    Chaos, ChaosMode, FaultContext, FaultInjector, OUTAGE_STATE_KEY, Outage, OutageLanded, Shards,
    SimulationResult, StateHandle, TimeProvider, assert_reachable, buggify_with_prob,
    sim_random_range,
};

/// When an outage may strike, from the opening of the chaos window: late,
/// because the cluster decides little early in the window (over a thousand
/// seeds, a tenth of the first decisions land before 3.5 s and half after
/// 5 s; the departed-straggler shape needs a claim, a reconfiguration and
/// its election first, 2 to 3.5 s on a kind seed), and inside the
/// shortest run, as every new fault is (#454 (the control plane meets
/// chaos): chaos lasts the clients' whole run, `shape::run_end`).
const START: std::ops::Range<Duration> =
    Duration::from_millis(2_500)..crate::RUN_FLOOR.saturating_sub(Duration::from_millis(100));

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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LossShape {
    /// The most recent slot the ledger holds, rather than a uniform one.
    pub(crate) recent: bool,
}

/// The outage of every machine of a larger cell (step 1.3 of the rework,
/// #1092 (larger cell)): on each seed whose plan draws a larger cell, every
/// machine goes down at one instant inside the chaos window, each back after
/// its own delay, one straggler last. The seed plan
/// (`crate::shape::larger_cell_plan`) calms the machine group on every other
/// seed, so this regime strikes only on a larger cell. The machines' disks
/// keep what they synced: the cell must form or recover from them.
pub(crate) fn machine_regime() -> Chaos {
    Chaos::Outage {
        config: Outage {
            groups: vec![crate::roles::MACHINE_GROUP.to_string()],
            probability: 1.0,
            start: START,
            down: DOWN,
            straggler: Some(STRAGGLER),
        },
        mode: ChaosMode::Random,
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
            // A landing with no acceptor is the machines' outage
            // (`machine_regime`): the acceptors' outage may still come.
            if let Some(landed) = ctx.state().get::<OutageLanded>(OUTAGE_STATE_KEY)
                && landed
                    .victims
                    .iter()
                    .any(|ip| ctx.ips_in_group(crate::roles::ACCEPTOR_GROUP).contains(ip))
            {
                assert_reachable!("storage: a correlated outage takes every acceptor down at once");
                let loss = LossShape {
                    recent: buggify_with_prob!(0.5),
                };
                // Planned after the outage landed: a victim already
                // back (or never down) is up, and its copy is spared.
                let up = |ip: &str| !ctx.is_dead(ip);
                let _ = plan_losses(ctx.state(), &ctx.shards(), loss, &up);
                return Ok(());
            }
            if ctx.chaos_shutdown().is_cancelled() || time.sleep(POLL).await.is_err() {
                return Ok(());
            }
        }
    }
}

/// Plan the outage's losses in every genesis journal (see the module doc).
/// Returns the holders the main journal's plan left clean: the copies the
/// cluster must wait for. Outside the loss budget, a plan keeps the clean
/// copies each journal's shard declares it needs (`shards`, #760
/// (moonpool owns fault placement)), since its damage lands past the gate.
/// A store whose process is `up` keeps its copy: it would never boot to
/// apply the loss. A plan made at the instant of its strike passes
/// `|_| false`.
pub(super) fn plan_losses(
    state: &StateHandle,
    shards: &Shards,
    loss: LossShape,
    up: &dyn Fn(&str) -> bool,
) -> Vec<u64> {
    let plan = crate::shape::journals(state);
    let mut kept = Vec::new();
    for journal in plan.ids {
        let audit = crate::audit::audit_world_for(state, journal);
        let decided = audit.decided_slots();
        let world = super::storage_world_for(state, journal);
        let shard = format!("journal/{journal}");
        let keeps = |slot: u64, lost: &[String]| shard_keeps(shards, &shard, slot, lost);
        let planned = world
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .plan_outage_loss(loss, &decided, &keeps, up);
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

/// Whether `shard` keeps the clean copies of `slot` it declares it needs
/// (`durable`) once the copies on the stores at `lost` (their keys) are
/// lost too. An undeclared or switched-off shard needs nothing.
fn shard_keeps(shards: &Shards, shard: &str, slot: u64, lost: &[String]) -> bool {
    let Some(declared) = shards.get(shard).filter(|_| !shards.is_off(shard)) else {
        return true;
    };
    let clean = shards.clean_copies(shard, slot);
    let on = |address: &str, key: &str| {
        address
            .parse::<std::net::SocketAddr>()
            .map_or(address == key, |at| at.ip().to_string() == key)
    };
    declared.replications().iter().all(|replication| {
        replication.durable.holds(&|replica| {
            clean.contains(replica) && !lost.iter().any(|key| on(replica.address(), key))
        })
    })
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

/// Take every acceptor and proxy leader down now, each back after its own
/// delay in [`DOWN`], one random acceptor straggling in [`STRAGGLER`].
///
/// Moonpool's crash gate keeps each victim's cut inside its shards' needs
/// (#760 (moonpool owns fault placement)), and the kill policy
/// (`super::can_kill`) is asked first.
pub(super) fn strike(ctx: &FaultContext) -> SimulationResult<()> {
    let acceptors = ctx.ips_in_group(crate::roles::ACCEPTOR_GROUP);
    let proxies = ctx.ips_in_group(crate::roles::PROXY_GROUP);
    let straggler =
        usize::try_from(sim_random_range(0..acceptors.len().max(1) as u64)).unwrap_or(0);
    strike_with_straggler(ctx, &acceptors, &proxies, straggler)
}

/// Take `acceptors` and `proxies` down now, each back after its own delay
/// in [`DOWN`], the acceptor of rank `straggler` in [`STRAGGLER`].
fn strike_with_straggler(
    ctx: &FaultContext,
    acceptors: &[String],
    proxies: &[String],
    straggler: usize,
) -> SimulationResult<()> {
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
    for ip in proxies {
        ctx.crash_for(ip, millis(&DOWN))?;
    }
    Ok(())
}
