//! The **down-member scenario** (#777 (mode change meets a down member)):
//! on a changed-mode seed that draws it (`crate::shape::down_member`), the
//! last operator creates a journal in the settle tail and picks one of its
//! members (`crate::workload::fleet`'s down-member step), and this injector
//! crashes it through moonpool's own `Crash` for a few seconds past every
//! `machine_down_after` the knob draws. On these seeds no machine re-places
//! a down machine's roles before [`crate::shape::DOWN_MEMBER_REPLACE_MARGIN`]
//! past its own bound, so the machine keeps its slots. The board notes when
//! every cell coordinator marked it down and when it comes back; inside that
//! span the operator configures the journal with a mode of the same
//! acceptor count, whose change waits until the member is back.
//!
//! It strikes only while every machine of the run is alive, every renamed
//! machine registered, and a few leases after a zone outage ended, so the
//! cell and the journal keep a majority and one of the others leads.

use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{
    FaultContext, FaultInjector, RebootKind, SimulationResult, TimeProvider, assert_reachable,
};

use crate::world::outage::POLL;

/// How long the machine stays down past the longest `machine_down_after`,
/// in milliseconds: the span the operator configures in. It ends before
/// any machine re-places it (`crate::shape::DOWN_MEMBER_REPLACE_MARGIN` past
/// its own bound).
const PAST_DOWN_MS: std::ops::Range<u64> = 3_000..5_001;

/// The margin past the longest `machine_down_after` before the operator
/// counts the machine as marked down: the cell coordinator's watch reads
/// each machine once per renewal period, of at most 1 s.
const MARK_MARGIN: Duration = Duration::from_millis(1_000);

/// The widest spread of `machine_down_after` between two machines: a
/// renewal period of 100 ms to 1 s plus a detector timeout of a round trip
/// to 3 s (`crate::shape`).
const DOWN_SPREAD: Duration = Duration::from_millis(3_900);

const _: () = assert!(
    PAST_DOWN_MS.end as u128 + DOWN_SPREAD.as_millis()
        < crate::shape::DOWN_MEMBER_REPLACE_MARGIN.as_millis()
);
const _: () = assert!(MARK_MARGIN.as_millis() < PAST_DOWN_MS.start as u128);

/// How many election leases past a zone outage's end the injector waits,
/// so the cell elected its coordinator again before a member goes down.
const SETTLE_LEASES: u32 = 3;

/// The down-member injector (see the module doc): a fresh one per
/// timeline.
pub(crate) struct DownMember;

#[async_trait]
impl FaultInjector for DownMember {
    fn name(&self) -> &'static str {
        "paros-down-member"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn inject(&mut self, ctx: &FaultContext) -> SimulationResult<()> {
        if !crate::shape::down_member(ctx.state()) {
            return Ok(());
        }
        let time = ctx.time();
        let target = loop {
            // A settled cell: a few leases after any zone outage ended.
            let settle = crate::shape::longest_election_lease(ctx.state()) * SETTLE_LEASES;
            if let Some(target) =
                crate::machine::member_down_target(ctx.state(), (time.now(), settle), |ip| {
                    ctx.is_dead(ip)
                })
            {
                break target;
            }
            if time.sleep(POLL).await.is_err() {
                return Ok(());
            }
        };
        let down_after = crate::shape::longest_down_after(ctx.state());
        let past = Duration::from_millis(moonpool_sim::sim_random_range(PAST_DOWN_MS));
        let now = time.now();
        assert_reachable!("machine: the scenario holds a journal's member down");
        tracing::info!(%target, "down_member_strikes");
        crate::machine::note_member_down(
            ctx.state(),
            now + down_after + MARK_MARGIN,
            now + down_after + past,
        );
        let hold = usize::try_from((down_after + past).as_millis()).unwrap_or(usize::MAX);
        ctx.reboot_with_delays(&target, RebootKind::Crash, &(hold..hold + 1), &(0..1))
    }
}
