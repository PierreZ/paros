//! The **slow-machine scenario** (#424 (busyness metrics)): on a seed that
//! draws it (`crate::shape::slow_machine`), one founding member of the
//! formed cell stays alive and slow for a few seconds, through moonpool's
//! own `FaultContext::set_slowness`: its CPU (only when the seed's CPU model
//! is on), its disk, or both, with factors from the top of the gray
//! failures' ranges. The member hosts the cell's journals, so it has real
//! work to be slow at, and the clients' `LOAD` meets a full window inside
//! its slow time (`crate::chain_workload::fleet`).
//!
//! The injector strikes once, when the cell formed, then makes the machine
//! healthy again. `init` mostly runs in the recovery tail, so like the
//! silent machine (`super::silent_machine`) it may strike up to
//! `LATE_WINDOW` into the tail. A slow machine is not a dead one: the cell
//! keeps every member, and the slowness ends long before the tail's
//! liveness claims are judged.

use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{
    FaultContext, FaultInjector, SimulationResult, Slowness, TimeProvider, assert_reachable,
};

use super::late_outage::LATE_WINDOW;
use super::outage::POLL;

/// How long the machine stays slow, in milliseconds: several `Load`
/// windows at the scenario's 1 s `load_interval`.
const SLOW_MS: std::ops::Range<u64> = 4_000..6_001;

/// The CPU factor range: the top of the gray failures' `10..=1000`, so a
/// member's ordinary work (about a thousandth of a core) fills its core.
const CPU_FACTOR: std::ops::Range<u64> = 500..1_001;

/// The disk factor range: the top of the gray failures' `5..=100`.
const DISK_FACTOR: std::ops::Range<u64> = 50..101;

/// The slow-machine injector (see the module doc): a fresh one per
/// timeline.
pub(crate) struct SlowMachine;

#[async_trait]
impl FaultInjector for SlowMachine {
    fn name(&self) -> &'static str {
        "paros-slow-machine"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn inject(&mut self, ctx: &FaultContext) -> SimulationResult<()> {
        if !crate::shape::slow_machine(ctx.state()) {
            return Ok(());
        }
        let time = ctx.time();
        let target = loop {
            if time.now() >= crate::CHAOS_DURATION + LATE_WINDOW {
                return Ok(());
            }
            if let Some(target) = crate::machine::slow_target(ctx.state(), |ip| ctx.is_dead(ip)) {
                break target;
            }
            if time.sleep(POLL).await.is_err() {
                return Ok(());
            }
        };
        // Which resources are slow: the CPU only on a CPU-model seed, where
        // the factor has an effect; else the disk alone.
        let kind = if ctx.cpu_model().is_some() {
            moonpool_sim::sim_random_range(0..3_u64)
        } else {
            1
        };
        let factor = |range: std::ops::Range<u64>| {
            u32::try_from(moonpool_sim::sim_random_range(range)).unwrap_or(u32::MAX)
        };
        let (cpu, disk) = (factor(CPU_FACTOR), factor(DISK_FACTOR));
        let slowness = Slowness {
            cpu: if kind == 1 { 1 } else { cpu },
            disk: if kind == 0 { 1 } else { disk },
            network: 1,
        };
        assert_reachable!("load: the scenario slows a machine of the cell");
        tracing::info!(%target, cpu = slowness.cpu, disk = slowness.disk, "slow_machine_strikes");
        ctx.set_slowness(&target, slowness)?;
        let hold = Duration::from_millis(moonpool_sim::sim_random_range(SLOW_MS));
        // A sleep cut short ends the run: the machine is healthy either way
        // once nobody runs.
        let _ = time.sleep(hold).await;
        ctx.set_slowness(&target, Slowness::HEALTHY)
    }
}
