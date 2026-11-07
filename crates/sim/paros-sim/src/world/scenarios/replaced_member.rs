//! The **replaced-member scenario** (#423): on a seed that draws it
//! (`crate::shape::replaced_member`), a member in force of the plan
//! journals of a formed cell of three or more loses its whole disk through
//! moonpool's own `CrashAndWipe`. It is any member in force, not only an
//! initial one (#685 (no founders)). The machine at that address comes back
//! as a new, idle one, and the cell keeps a majority. Client 0 then admits
//! that machine with `cell add-machine` and runs `init` again over the
//! initial members (`crate::workload::fleet`): when the wiped member was an
//! initial one, the re-run meets formed initial members and one admitted
//! machine at a listed address, and must find the cell.
//!
//! The injector polls the machine board for the moment every member in
//! force formed or was admitted (`crate::machine::replace_target`), then
//! strikes once. The cell often forms only after the chaos window, so like
//! the moved member (`super::moved_member`) it may strike up to
//! `LATE_WINDOW` into the recovery tail; the cell keeps a majority of its
//! members.

use async_trait::async_trait;
use moonpool_sim::{
    FaultContext, FaultInjector, RebootKind, SimulationResult, TimeProvider, assert_reachable,
};

use super::late_outage::LATE_WINDOW;
use crate::world::outage::POLL;

/// The replaced-member injector (see the module doc): a fresh one per
/// timeline.
pub(crate) struct ReplacedMember;

#[async_trait]
impl FaultInjector for ReplacedMember {
    fn name(&self) -> &'static str {
        "paros-replaced-member"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn inject(&mut self, ctx: &FaultContext) -> SimulationResult<()> {
        if !crate::shape::replaced_member(ctx.state()) {
            return Ok(());
        }
        let time = ctx.time();
        let target = loop {
            if crate::past_cutoff(time.now(), LATE_WINDOW) {
                return Ok(());
            }
            if let Some(target) = crate::machine::replace_target(ctx.state(), |ip| ctx.is_dead(ip))
            {
                break target;
            }
            if time.sleep(POLL).await.is_err() {
                return Ok(());
            }
        };
        assert_reachable!("machine: the scenario wipes a member of a formed cell");
        tracing::info!(%target, "replaced_member_strikes");
        // The scenario wipes a member past what its journals declare they
        // keep, on purpose: the shards that list it leave the gate.
        super::switch_off_shards_of(&ctx.shards(), &target);
        ctx.reboot_with_delays(&target, RebootKind::CrashAndWipe, &(250..1_501), &(0..1))
    }
}
