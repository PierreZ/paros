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
use paros::client::checkpoint::LoadOutcome;
use paros::system::NodeStanding;

use super::cell::Cell;
use super::{FleetOps, judged_cell};

/// How long past the detector's bounds a machine must be live to be
/// judged: a late boot (2.5 s), its first answer, and the rounds that clear
/// a slow mark.
const SLACK: Duration = Duration::from_secs(10);

impl FleetOps {
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
