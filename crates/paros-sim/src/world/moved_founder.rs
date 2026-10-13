//! The **moved-founder scenario** (#211): on a seed that draws it
//! (`crate::shape::moved_founder`), a founding member of the formed cell
//! comes back under a new name, and then a machine whose durable cached
//! registry fold follows the move restarts. That restart dials the founder
//! where only its cache knows it is: the static-stability case of
//! `docs/architecture.md` §3.2.
//!
//! On that seed the machines advertise names, and the first crash's reboot
//! renames the founder (`crate::machine::founder_to_move`). The
//! second strikes once a machine's cache holds the new name
//! (`crate::machine::cached_mover`). Both are short crashes through
//! moonpool's own `Crash`. Like the silent machine (`super::silent_machine`)
//! the move may strike up to `LATE_WINDOW` into the recovery tail, and
//! `may_rename` keeps the cell's majority. A cache follows the move 1 to
//! 25 s after it, so the second crash may strike up to [`FOLLOW`] after the
//! move; the rest of the tail is still the recovery the oracles judge.

use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{
    FaultContext, FaultInjector, RebootKind, SimulationResult, TimeProvider, assert_reachable,
};

use super::late_outage::LATE_WINDOW;
use super::outage::POLL;

/// How long each crashed machine stays down, in milliseconds: a usual
/// reboot.
const DOWN_MS: std::ops::Range<usize> = 100..601;

/// How long after the move the second crash may still strike: a cache
/// followed the move within 8 s on half the scenario seeds and within 12 s
/// on four in five (900 hunt seeds), so a bound of `LATE_WINDOW` left the
/// gate silent on most of them (#441 (metainfo twin) met it in CI). Far
/// below the workload's recovery budget (45 s at its floor, counted from
/// the cutoff) with the move window before it.
const FOLLOW: Duration = Duration::from_secs(12);

const _: () =
    assert!(LATE_WINDOW.as_millis() + FOLLOW.as_millis() + (DOWN_MS.end as u128) < 45_000);

/// The moved-founder injector (see the module doc): a fresh one per
/// timeline.
pub(crate) struct MovedFounder;

#[async_trait]
impl FaultInjector for MovedFounder {
    fn name(&self) -> &'static str {
        "paros-moved-founder"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn inject(&mut self, ctx: &FaultContext) -> SimulationResult<()> {
        if !crate::shape::moved_founder(ctx.state()) {
            return Ok(());
        }
        let time = ctx.time();
        // The move's deadline (`LATE_WINDOW` past the cutoff); once it
        // struck, the second crash's.
        let mut deadline: Option<Duration> = None;
        let mut moved = false;
        loop {
            let late = deadline.map_or_else(
                || crate::past_cutoff(time.now(), LATE_WINDOW),
                |at| time.now() >= at,
            );
            if late {
                return Ok(());
            }
            if !moved
                && let Some(founder) =
                    crate::machine::founder_to_move(ctx.state(), |ip| ctx.is_dead(ip))
            {
                assert_reachable!("machine: the scenario moves a founding member");
                tracing::info!(%founder, "moved_founder_strikes");
                ctx.reboot_with_delays(&founder, RebootKind::Crash, &DOWN_MS, &(0..1))?;
                moved = true;
                deadline = Some(time.now() + FOLLOW);
            } else if moved
                && let Some(target) =
                    crate::machine::cached_mover(ctx.state(), |ip| ctx.is_dead(ip))
            {
                assert_reachable!("machine: the scenario restarts a machine that cached a move");
                tracing::info!(%target, "cached_mover_strikes");
                return ctx.reboot_with_delays(&target, RebootKind::Crash, &DOWN_MS, &(0..1));
            }
            if time.sleep(POLL).await.is_err() {
                return Ok(());
            }
        }
    }
}
