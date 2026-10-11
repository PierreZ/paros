//! The **split-floor outage** (#409 (repair probe blocked below a trim
//! point)): on a seed that draws the split-floor scenario
//! (`crate::shape::split_floor`), the acceptors whose floor passed a
//! decided slot the others still hold
//! ([`super::StorageWorld::split_floor`]) go down the moment the split
//! shows. Once the acceptors behind them are quiet (every holder's custody
//! settled, no budgeted commit in flight), the correlated outage takes the
//! others down too, and the loss planned at that instant takes every copy
//! they hold of the slot ([`super::StorageWorld::plan_split_floor_loss`]).
//! The ahead acceptors are back last.
//!
//! The others boot with the slot faulty. A leader among them wins Phase 1
//! without the ahead acceptors and cannot decide the slot (CTRL Case 3), so
//! it opens a repair probe. When an ahead acceptor comes back, the
//! leader's heartbeat shows it behind, and the ahead acceptor answers with
//! its trim point (`Message::TrimmedTo`): the leader jumps with the probe
//! open, and the jump must resolve every blocked slot below the point
//! (`Proposer::probe_retain_from`). Nothing is lost: the slot is chosen and
//! truncated on the ahead acceptors.
//!
//! A split floor exists only between one acceptor's compaction and the
//! others' (a `Commit` in flight), and the acceptors behind are almost
//! always mid-sync then: a strike at the split itself met 1 of 40 scenario
//! seeds. Holding the ahead acceptors down first freezes the split: the
//! others learn the truncation only once a new leader commits it. The
//! journal decides little in the chaos window, and a split shows mostly 5
//! to 20 s into a run, so, like the departed straggler's outage
//! (`super::late_outage`), the hold may begin up to [`SPLIT_WINDOW`] into
//! the recovery tail, and lasts at most `HOLD`; the rest of the tail is
//! still the recovery the oracles judge.

use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{
    FaultContext, FaultInjector, SimulationResult, TimeProvider, assert_reachable, sim_random_range,
};

use super::outage::{STRAGGLER, may_strike, strike};

/// How far into the recovery tail the split may still be held: a split
/// floor shows mostly 5 to 20 s into a run (600 hunt seeds), past the
/// `LATE_WINDOW` the other tail exceptions keep. Far below the workload's
/// recovery budget (45 s at its floor) with the hold and the straggler
/// after it.
pub(crate) const SPLIT_WINDOW: Duration = Duration::from_secs(16);

/// How long the ahead acceptors stay down waiting for the acceptors behind
/// to go quiet: a strike follows the hold within milliseconds on most
/// seeds, and a hold with no strike gives the acceptors back.
const HOLD: Duration = STRAGGLER.end;

const _: () =
    assert!(SPLIT_WINDOW.as_millis() + HOLD.as_millis() + STRAGGLER.end.as_millis() < 45_000);

/// The last instant the split may be held.
fn deadline() -> Duration {
    Duration::from_millis(crate::CHAOS_DURATION_MS) + SPLIT_WINDOW
}

/// How often the injector looks for a split floor: a split lasts only
/// while a `Commit` is in flight, a few milliseconds.
const POLL: Duration = Duration::from_millis(1);

/// The split-floor injector (see the module doc): a fresh one per
/// timeline.
pub(crate) struct SplitFloor;

#[async_trait]
impl FaultInjector for SplitFloor {
    fn name(&self) -> &'static str {
        "paros-split-floor"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn inject(&mut self, ctx: &FaultContext) -> SimulationResult<()> {
        if !crate::shape::split_floor(ctx.state()) {
            return Ok(());
        }
        let time = ctx.time();
        let main = crate::shape::journals(ctx.state()).main;
        let acceptors = ctx.ips_in_group(crate::roles::ACCEPTOR_GROUP);
        // Node ids are ranks in the acceptor group.
        let ip_of = |node: u64| {
            usize::try_from(node)
                .ok()
                .and_then(|rank| acceptors.get(rank))
                .cloned()
        };
        // The split: the ahead acceptors go down the moment it shows.
        let ahead: Vec<String> = loop {
            if time.now() >= deadline() {
                return Ok(());
            }
            let decided = crate::audit::audit_world_for(ctx.state(), main).decided_slots();
            let split = super::storage_world_for(ctx.state(), main)
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .split_floor(&decided);
            if let Some((nodes, _)) = split {
                let ips: Option<Vec<String>> = nodes.iter().map(|node| ip_of(*node)).collect();
                if let Some(ips) = ips.filter(|ips| {
                    ips.len() < acceptors.len()
                        && ips.iter().all(|ip| !ctx.is_dead(ip))
                        && super::cut::outage_permitted(ctx.state(), ips)
                }) {
                    // Checked and struck with no await between them (#332).
                    super::cut::assert_outage_permitted(ctx.state(), &ips);
                    for ip in &ips {
                        ctx.crash(ip)?;
                    }
                    break ips;
                }
            }
            if time.sleep(POLL).await.is_err() {
                return Ok(());
            }
        };
        assert_reachable!("outage: a split floor holds its ahead acceptors down");
        tracing::info!(?ahead, "split_floor_ahead_down");
        // The outage, once the acceptors behind are quiet.
        let release = time.now() + HOLD;
        let struck = loop {
            if time.now() >= release {
                break false;
            }
            if may_strike(ctx) {
                let audit = crate::audit::audit_world_for(ctx.state(), main);
                let decided = audit.decided_slots();
                // Planned at the instant of the strike, with no await
                // between them, so no holder repairs its copy first.
                let planned = super::storage_world_for(ctx.state(), main)
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .plan_split_floor_loss(&decided);
                if let Some(planned) = planned {
                    audit.note_outage_loss(planned.slot, &planned.holders, &planned.damaged);
                    if time.now() >= Duration::from_millis(crate::CHAOS_DURATION_MS) {
                        assert_reachable!(
                            "outage: a split-floor outage strikes in the recovery tail"
                        );
                    }
                    tracing::info!(slot = planned.slot, "split_floor_outage");
                    strike(ctx, &[])?;
                    break true;
                }
            }
            if time.sleep(POLL).await.is_err() {
                break false;
            }
        };
        // The ahead acceptors back last: after every other victim of the
        // outage, or at once when it never struck.
        if struck {
            let back = sim_random_range(
                u64::try_from(STRAGGLER.start.as_millis()).unwrap_or(0)
                    ..u64::try_from(STRAGGLER.end.as_millis()).unwrap_or(1),
            );
            // A failed sleep is the run ending: restart them all the same.
            let _ = time.sleep(Duration::from_millis(back)).await;
        }
        for ip in &ahead {
            ctx.restart(ip)?;
        }
        Ok(())
    }
}
