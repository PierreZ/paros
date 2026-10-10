//! The **silent-machine scenario** (#211): on a seed that draws it
//! (`crate::shape::silent_machine`), one machine of the formed cell crashes
//! and stays down for longer than any `machine_down_after` the knob draws,
//! through moonpool's own `Crash` with a long recovery delay. The cell
//! coordinator's watch marks it down, then up when it is back as a new
//! incarnation (`paros::machine::coordinator::Watch`), and the final fleet
//! check judges the entries it wrote (`crate::audit::liveness`).
//!
//! The target is an admitted machine when the cell has one, else a founding
//! member of a cell of three or more, so the others keep a majority and one
//! of them leads (`crate::machine::silent_target`). The injector strikes
//! once, after the cell coordinator had time to serve a term. `init` mostly
//! runs in the recovery tail, so like the departed straggler's outage
//! (`super::late_outage`) it may strike up to `LATE_WINDOW` into the tail;
//! the cell keeps a majority throughout, so the tail's liveness claims
//! still hold.

use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{
    FaultContext, FaultInjector, RebootKind, SimulationResult, TimeProvider, assert_reachable,
};

use super::late_outage::LATE_WINDOW;
use super::outage::POLL;

/// The earliest the injector strikes: a coordinator elected and its term
/// served.
const EARLIEST: Duration = Duration::from_millis(1_500);

/// How long the machine stays down, in milliseconds: past the largest
/// `machine_down_after` the simulation draws (`crate::shape`: a renewal
/// period of at most 1 s plus at most 3 s).
const DOWN_MS: std::ops::Range<usize> = 4_500..6_501;

/// The silent-machine injector (see the module doc): a fresh one per
/// timeline.
pub(crate) struct SilentMachine;

#[async_trait]
impl FaultInjector for SilentMachine {
    fn name(&self) -> &'static str {
        "paros-silent-machine"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn inject(&mut self, ctx: &FaultContext) -> SimulationResult<()> {
        if !crate::shape::silent_machine(ctx.state()) {
            return Ok(());
        }
        let time = ctx.time();
        let target = loop {
            if time.now() >= crate::CHAOS_DURATION + LATE_WINDOW {
                return Ok(());
            }
            if time.now() >= EARLIEST
                && let Some(target) =
                    crate::machine::silent_target(ctx.state(), |ip| ctx.is_dead(ip))
            {
                break target;
            }
            if time.sleep(POLL).await.is_err() {
                return Ok(());
            }
        };
        assert_reachable!("machine: the scenario holds a machine of the cell down");
        tracing::info!(%target, "silent_machine_strikes");
        ctx.reboot_with_delays(&target, RebootKind::Crash, &DOWN_MS, &(0..1))
    }
}
