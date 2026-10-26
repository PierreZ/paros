//! The failure detector's end state (#654 (live machine marked down)):
//! after chaos, every registered machine that is live (its process runs,
//! it holds the id it minted, it was never renamed:
//! `crate::machine::live_since`) since longer than the detector needs to
//! hear it is held up and not slow in the cell's registry. A machine the
//! coordinator marked down or slow for no reason is otherwise re-placed or
//! moved off for good, and the re-placement oracles pass all the same.
//!
//! The registry is read again until it holds, or until the deadline: a
//! machine that came back late in the tail is marked up within a few
//! renewal periods, and a slow mark clears after `slow_rounds` fast rounds.
//! A retired machine is not judged, nor a wiped one's old id.

use std::time::Duration;

use moonpool_sim::{SimContext, TimeProvider, assert_always, assert_reachable, assert_sometimes};
use paros::Address;
use paros::client::checkpoint::LoadOutcome;
use paros::client::load::{LoadOutcome as Load, ask_all};
use paros::coordinator::cell::slow::{BUSY_MARGIN, Sample};
use paros::model::registry::NodeStanding;

use super::cell::Cell;
use super::{FleetOps, judged_cell};

/// How long past the detector's bounds a machine must be live to be
/// judged: a late boot (2.5 s), its first answer, and the rounds that clear
/// a slow mark.
const SLACK: Duration = Duration::from_secs(10);

impl FleetOps {
    /// Whether the machine at `addr` still measures busy: its disk or CPU
    /// at or above the lowest slow bound any node drew, less
    /// [`BUSY_MARGIN`]. The coordinator clears a slow mark only once the
    /// machine leans fast (`paros::coordinator::cell::slow`), so a mark held over a
    /// machine that is still this busy has a reason: its own work keeps it
    /// in the band between the bounds. The cell coordinator's lease
    /// renewals did that to its own machine, the one founder, at the
    /// 100 ms renewal floor: 70 to 90 % disk busy for the whole tail
    /// (witness `replay-main 10435710832576718291` on 0fe713a). A machine
    /// that does not answer is judged as usual.
    async fn still_busy(&self, ctx: &SimContext, node: u64, addr: &str) -> bool {
        let Ok(target) = Address::parse(addr) else {
            return false;
        };
        let outcomes = ask_all(
            self.connector.providers(),
            self.connector.rpc(),
            self.connector.names(),
            &[(node, target)],
            self.patience,
        )
        .await;
        let Some(Load::Answered(ack)) = outcomes.get(&node) else {
            return false;
        };
        let sample = Sample::of(Duration::ZERO, Some(ack));
        let busiest = sample
            .disk_busy_permille
            .max(sample.cpu_busy_permille)
            .unwrap_or(0);
        let bound = crate::shape::lowest_slow_busy(ctx.state()).saturating_sub(BUSY_MARGIN);
        let busy = busiest >= bound;
        if busy {
            assert_reachable!("registry: a slow mark is held over a machine still busy");
        }
        busy
    }

    /// Read the cell's registry until every machine live long enough is
    /// held up and not slow, or `deadline`.
    pub(super) async fn machines_live(&self, ctx: &SimContext, cell: &Cell, deadline: Duration) {
        let bound = crate::shape::longest_down_after(ctx.state())
            + crate::shape::longest_internal_timeout(ctx.state())
            + SLACK;
        let mut attempt = 0_u64;
        let mut last = None;
        while ctx.time().now() < deadline && !ctx.shutdown().is_cancelled() {
            if crate::machine::cell_lost(ctx.state()) {
                return;
            }
            let first = cell.first(attempt);
            attempt += 1;
            let mut fold = judged_cell(&cell.members);
            let loaded = paros::client::checkpoint::load(
                &mut fold,
                cell.journals.cell,
                &cell.client,
                first,
                0,
            )
            .await;
            // The `MachineDown`s sent in the tail and not judged yet
            // (`crate::machine::judge_sent`).
            crate::machine::judge_pending(ctx.state(), ctx.time().now());
            if matches!(loaded, LoadOutcome::Loaded { .. }) {
                let now = ctx.time().now();
                let judged = fold.state();
                let registry = judged.registry();
                let mut wrong = None;
                let mut live = Vec::new();
                for (id, node) in registry.nodes() {
                    if node.standing == NodeStanding::Retired {
                        continue;
                    }
                    let Some(since) = crate::machine::live_since(ctx.state(), id.0) else {
                        continue;
                    };
                    if now.saturating_sub(since) < bound
                        || crate::machine::slowed_after(ctx.state(), now.saturating_sub(bound))
                    {
                        continue;
                    }
                    let held = registry.liveness(id);
                    if held.up && held.slow && self.still_busy(ctx, id.0, &node.addr).await {
                        continue;
                    }
                    if !held.up || held.slow {
                        wrong = Some((id.0, held.up, held.slow));
                        break;
                    }
                    live.push(id);
                }
                match wrong {
                    None => {
                        for id in live {
                            assert_sometimes!(
                                judged.seen_down(id),
                                "registry: a crashed machine is marked down and up again"
                            );
                        }
                        assert_reachable!("registry: every live machine is held up after chaos");
                        return;
                    }
                    Some(found) => last = Some(found),
                }
            }
            if ctx.time().sleep(Duration::from_millis(500)).await.is_err() {
                return;
            }
        }
        if let Some((node, up, slow)) = last {
            assert_always!(
                ctx.shutdown().is_cancelled() || crate::machine::cell_lost(ctx.state()),
                "registry: a live machine is held up and not slow after chaos",
                { "node" => node, "up" => up, "slow" => slow }
            );
        }
    }
}
