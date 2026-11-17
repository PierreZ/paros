//! The **moved-member scenario** (#211): on a seed that draws it
//! (`crate::shape::moved_member`), a member in force of the formed cell's
//! plan journals (any one, not only an initial one, #685 (no founders))
//! comes back under a new name, and then a machine whose durable cached
//! registry fold follows the move restarts. That restart dials the member
//! where only its cache knows it is: the static-stability case of
//! `docs/architecture/03-2-machines.md` §3.2.
//!
//! On that seed the machines advertise names, and the first crash's reboot
//! renames the member (`crate::machine::member_to_move`). The
//! second strikes once a machine's cache holds the new name
//! (`crate::machine::cached_mover`). Both are short crashes through
//! moonpool's own `Crash`. Both strike only before the chaos cutoff (rule
//! 8, `docs/architecture/06-verification.md`), and `may_rename` keeps the
//! cell's majority. A cache follows the move 1 to 25 s after it, so the
//! second crash may strike up to [`FOLLOW`] after the move.
//!
//! On a repointed-cell seed (`crate::shape::repointed_cell`, #854 (universe
//! repoint gates)) the same move waits until the run's second cell joined,
//! and there is no second crash: the move leaves the joined cell's universe
//! pointer naming the old name.

use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{
    FaultContext, FaultInjector, RebootKind, SimulationResult, TimeProvider, assert_reachable,
};

use crate::world::outage::POLL;

/// How long each crashed machine stays down, in milliseconds: a usual
/// reboot.
const DOWN_MS: std::ops::Range<usize> = 100..601;

/// How long after the move the second crash may still strike, before the
/// chaos cutoff: a cache followed the move within 8 s on half the scenario
/// seeds and within 12 s on four in five (900 hunt seeds).
const FOLLOW: Duration = Duration::from_secs(12);

/// The moved-member injector (see the module doc): a fresh one per
/// timeline.
pub(crate) struct MovedMember;

#[async_trait]
impl FaultInjector for MovedMember {
    fn name(&self) -> &'static str {
        "paros-moved-member"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn inject(&mut self, ctx: &FaultContext) -> SimulationResult<()> {
        let repointed = crate::shape::repointed_cell(ctx.state());
        if !crate::shape::moved_member(ctx.state()) && !repointed {
            return Ok(());
        }
        let time = ctx.time();
        // The chaos cutoff ends both strikes; once the move struck, the
        // second crash's own deadline too.
        let mut deadline: Option<Duration> = None;
        let mut moved = false;
        loop {
            let late =
                crate::past_cutoff(time.now()) || deadline.is_some_and(|at| time.now() >= at);
            if late {
                return Ok(());
            }
            if !moved
                && (!repointed || crate::workload::second_cell_joined(ctx.state()))
                && let Some(member) =
                    crate::machine::member_to_move(ctx.state(), |ip| ctx.is_dead(ip))
            {
                assert_reachable!("machine: the scenario moves a member in force");
                tracing::info!(%member, "moved_member_strikes");
                ctx.reboot_with_delays(&member, RebootKind::Crash, &DOWN_MS, &(0..1))?;
                if repointed {
                    assert_reachable!(
                        "universe: the scenario moves a member after the second cell joined"
                    );
                    return Ok(());
                }
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
