//! The **disk-full scenario** (#360 (disk full)): on a seed that draws it
//! (`crate::shape::disk_full`), the machines' and the acceptors' disks fill
//! during the chaos window, through moonpool's own
//! `FaultContext::set_disk_capacity` (moonpool#343 (disk capacity and
//! ENOSPC)). No paros code is wrapped: the shipped stores meet `ENOSPC` on
//! the simulated disk exactly as on a real one.
//!
//! Every strike picks one live process of the two groups, reads how much
//! its disk uses, and gives the disk a capacity a little above that: room
//! for at most a few segments of the compact geometry. On half the strikes
//! the disk also counts each byte two to four times, so it fills faster
//! than the workload grows. A store that rolls a segment past the room
//! finds the disk full and goes read-only. After 2–8 s the disk grows
//! back to 64 MiB of room, so the machine reports its space again and the
//! store comes back writable; on one strike in three the capacity stays
//! for the rest of the chaos window, so only a truncation (its dead
//! segments removed) frees space. Recovery mode lifts every capacity, so
//! the quiet tail runs on disks with room and the liveness oracles hold.
//!
//! The oracles are the campaign's own: no acknowledged write is lost (the
//! journal model and the acked bytes), and a full disk never crashes a
//! node (the audit judges the decision `ReadOnly`, and a panic fails the
//! run).

use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{
    DiskCapacity, FaultContext, FaultInjector, SimulationResult, TimeProvider, assert_reachable,
};

use crate::roles::{ACCEPTOR_GROUP, MACHINE_GROUP};

/// How long between two strikes, in milliseconds.
const GAP_MS: std::ops::Range<u64> = 1_000..4_001;

/// How long a strike's capacity holds before the disk grows back, in
/// milliseconds.
const HOLD_MS: std::ops::Range<u64> = 2_000..8_001;

/// The room a strike leaves, in bytes: from none up to about three
/// segments of the compact geometry (about 100 KB each).
const ROOM: std::ops::Range<u64> = 0..300_001;

/// How many times an amplified disk counts each byte.
const AMPLIFICATION: std::ops::Range<u64> = 2..5;

/// The room a disk grows back to: far above any run's writes.
const GROWN: u64 = 64 * 1024 * 1024;

/// The disk-full injector (see the module doc): a fresh one per timeline.
pub(crate) struct DiskFull;

/// How many bytes `ip`'s disk uses now, counted `amplification` times: read
/// through a capacity too large to ever fill.
fn used(ctx: &FaultContext, ip: &str, amplification: u32) -> SimulationResult<Option<u64>> {
    ctx.set_disk_capacity(
        ip,
        Some(DiskCapacity::new(u64::MAX).amplified(amplification)),
    )?;
    Ok(ctx
        .disk_space(ip)?
        .map(|space| space.total_bytes - space.free_bytes))
}

#[async_trait]
impl FaultInjector for DiskFull {
    fn name(&self) -> &'static str {
        "paros-disk-full"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn inject(&mut self, ctx: &FaultContext) -> SimulationResult<()> {
        if !crate::shape::disk_full(ctx.state()) {
            return Ok(());
        }
        let time = ctx.time();
        let pause = |range: std::ops::Range<u64>| {
            Duration::from_millis(moonpool_sim::sim_random_range(range))
        };
        while crate::in_chaos() {
            if time.sleep(pause(GAP_MS)).await.is_err() || !crate::in_chaos() {
                return Ok(());
            }
            let mut targets = ctx.ips_in_group(MACHINE_GROUP);
            targets.extend(ctx.ips_in_group(ACCEPTOR_GROUP));
            targets.retain(|ip| !ctx.is_dead(ip));
            if targets.is_empty() {
                continue;
            }
            let pick = moonpool_sim::sim_random_range(0..targets.len() as u64);
            let target = targets[usize::try_from(pick).unwrap_or(0)].clone();
            let amplification = if moonpool_sim::sim_random_range(0..2_u64) == 0 {
                1
            } else {
                u32::try_from(moonpool_sim::sim_random_range(AMPLIFICATION)).unwrap_or(2)
            };
            let Some(used) = used(ctx, &target, amplification)? else {
                continue;
            };
            let room = moonpool_sim::sim_random_range(ROOM);
            assert_reachable!("disk full: the scenario fills a disk");
            if amplification > 1 {
                assert_reachable!("disk full: the scenario fills an amplified disk");
            }
            tracing::info!(%target, used, room, amplification, "disk_full_strikes");
            ctx.set_disk_capacity(
                &target,
                Some(DiskCapacity::new(used.saturating_add(room)).amplified(amplification)),
            )?;
            // One strike in three keeps its capacity for the rest of the
            // chaos window: only a truncation frees space then.
            if moonpool_sim::sim_random_range(0..3_u64) == 0 {
                assert_reachable!("disk full: the scenario keeps a disk full");
                continue;
            }
            if time.sleep(pause(HOLD_MS)).await.is_err() {
                return Ok(());
            }
            let Some(now_used) = self::used(ctx, &target, amplification)? else {
                continue;
            };
            tracing::info!(%target, "disk_full_grows_back");
            ctx.set_disk_capacity(
                &target,
                Some(DiskCapacity::new(now_used.saturating_add(GROWN)).amplified(amplification)),
            )?;
        }
        Ok(())
    }
}
