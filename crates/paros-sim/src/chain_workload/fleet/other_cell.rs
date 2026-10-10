//! Another cell (#216): on a run where a wipe replaced a member of the
//! cell, an operator may found a cell of its own on the new machine at that
//! address — `parosctl init` over that one machine — instead of admitting
//! it. The run's cell still names the old machine there, so its members
//! keep sending their peer traffic to an address another cell now serves:
//! the shape `Deliver`'s `cell_id` refuses (`docs/architecture.md` §3.2,
//! the cluster id `ScyllaDB` carries in gossip). From then on the run's operators do
//! not count that address as their cell's: `Inspect` there names the other
//! cell, and a re-run `init` learns the cell a majority of the founding
//! members serve.

use moonpool_sim::{SimContext, assert_always, assert_reachable};
use paros::client::bootstrap::{self, InitOutcome};

use super::FleetOps;

impl FleetOps {
    /// Found another cell on the machine that replaced a wiped member of
    /// the run's cell, if the run has one (`crate::machine::other_cell_target`).
    /// Whether it ran.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(super) async fn found_other_cell(&mut self, ctx: &SimContext) -> bool {
        let Some(target) = crate::machine::other_cell_target(ctx.state()) else {
            return false;
        };
        crate::machine::note_other_cell(ctx.state(), target);
        assert_reachable!("admit: an operator founds another cell on a wiped member's address");
        let outcome = bootstrap::cell_init(
            self.connector.providers(),
            self.connector.rpc(),
            target,
            &[target],
            self.patience,
        )
        .await;
        match outcome {
            InitOutcome::Formed(plan) => {
                assert_always!(
                    plan.addrs().into_iter().eq(std::iter::once(target)),
                    "admit: another cell forms over the one machine it lists",
                    { "cell" => plan.cell_id }
                );
                assert_always!(
                    crate::machine::formed_cell(ctx.state())
                        .is_none_or(|cell| cell.cell_id != plan.cell_id),
                    "admit: another cell is never the run's cell",
                    { "cell" => plan.cell_id }
                );
            }
            InitOutcome::Refused(label) => {
                // Its write failed under it, or another operator's admission
                // reached the machine first.
                assert_always!(
                    label == "storage" || label == "cell_exists",
                    "admit: another cell's init is refused only for a failed write or an admission",
                    { "refusal" => label.as_str() }
                );
            }
            InitOutcome::NotWaiting | InitOutcome::Malformed | InitOutcome::Unreachable => {}
        }
        true
    }
}
