//! `ADMIT` (#216): `cell add-machine` against the machines, through the
//! library's `paros::client::cell` — the code `parosctl cell add-machine`
//! prints. The target is a machine outside the founding members (an idle
//! machine, or one admitted before), or on its own BUGGIFY location a
//! founding member, which is in the cell already. The admission may stop
//! after its registration (a crash at a step) and is resumed by this
//! operator's next `ADMIT`. On its own BUGGIFY location, an operator founds
//! another cell on a wiped member's address instead (`other_cell`).

use std::net::SocketAddr;

use moonpool_sim::{
    SimContext, assert_always, assert_reachable, assert_sometimes, buggify_with_prob,
};
use paros::NodeId;
use paros::client::cell::CellSession;
use paros::client::checkpoint::CheckpointPolicy;
use paros::client::fleet::{FleetRefusal, Run, Stage, Step};
use paros::system::Registry;

use super::{FleetOps, reach};

impl FleetOps {
    /// `ADMIT`: admit a machine into the cell this operator knows.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(in crate::chain_workload) async fn admit(
        &mut self,
        ctx: &SimContext,
        policy: CheckpointPolicy,
        draw: u64,
    ) {
        if self.admitting.is_none()
            && crate::machine::other_cell_target(ctx.state()).is_some()
            // The wiped-founder scenario lines up the wipe this needs: on
            // its seeds, the operator always founds the other cell.
            && (crate::shape::wiped_founder(ctx.state()) || buggify_with_prob!(0.5))
            && self.found_other_cell(ctx).await
        {
            return;
        }
        let founders = self.layout.founders;
        let outside = founders..self.machines.len();
        let target = match self.admitting.take() {
            Some(target) => target,
            None if outside.is_empty() || buggify_with_prob!(0.1) => {
                self.machines[usize::try_from(draw % founders.max(1) as u64).unwrap_or(0)]
            }
            None => {
                let span = (outside.end - outside.start) as u64;
                self.machines[outside.start + usize::try_from(draw % span).unwrap_or(0)]
            }
        };
        let Some(cell) = self.learn(ctx).await else {
            assert_reachable!("admit: an admission finds no cell formed yet");
            return;
        };
        let founders: Vec<(NodeId, SocketAddr)> = cell
            .servers
            .iter()
            .map(|(id, addr)| (NodeId(*id), *addr))
            .collect();
        let mut session =
            CellSession::new(cell.journals, founders, self.leader_seeds.next(), policy);
        let client = cell.client.clone();
        let first = cell.first(draw);
        let providers = self.connector.providers().clone();
        let rpc = self.connector.rpc().clone();
        if buggify_with_prob!(0.25) {
            let step = session
                .admit_step(&providers, &rpc, &client, first, target)
                .await;
            if let Step::Advanced(stage) = step {
                assert_reachable!("admit: an admission stops after its registration");
                reach(stage);
                self.admitting = Some(target);
            }
            return;
        }
        let run = session
            .add_machine(&providers, &rpc, &client, first, target, self.patience)
            .await;
        self.judge_admission(ctx, &session, target, run);
    }

    /// What an admission came to, judged.
    fn judge_admission(
        &mut self,
        ctx: &SimContext,
        session: &CellSession,
        target: SocketAddr,
        run: Run<NodeId>,
    ) {
        run.steps.iter().copied().for_each(reach);
        let founder = crate::machine::is_founder(ctx.state(), target);
        match run.outcome {
            Step::Done { result, .. } => {
                if run.steps.is_empty() {
                    if founder {
                        assert_reachable!("admit: a founding member is in its cell already");
                    } else {
                        assert_reachable!(
                            "admit: an admitted machine is admitted again, unchanged"
                        );
                    }
                } else {
                    assert_always!(
                        session.registry().get(result).is_some(),
                        "admit: an admission registers the machine before it admits it",
                        { "node" => result.0 }
                    );
                    if run.steps == [Stage::Admit] {
                        assert_reachable!("admit: an admission resumes after its registration");
                    }
                }
                assert_sometimes!(
                    !run.steps.is_empty(),
                    "admit: a run admits a machine into the cell"
                );
            }
            Step::Refused(FleetRefusal::InCellInit { .. }) => {
                // Only a founding member takes part in `cell init`: one that
                // promised and has no vote yet, mid-decree.
                assert_always!(
                    founder,
                    "admit: only a founding member refuses an admission for cell init"
                );
                assert_reachable!("admit: a founding member mid cell init refuses an admission");
            }
            Step::Refused(FleetRefusal::OtherCell { cell_id, .. }) => {
                // One cell per run, unless a wiped one-founder cell formed
                // again over the new machine (#246).
                assert_always!(
                    crate::machine::founder_wiped(ctx.state()),
                    "admit: a machine of another cell exists only after a wipe",
                    { "cell" => cell_id }
                );
            }
            Step::Refused(refusal) => {
                assert_always!(
                    false,
                    "admit: an admission is refused only for another cell or cell init",
                    { "refusal" => format!("{refusal:?}") }
                );
            }
            Step::Interrupted(_) => {
                assert_reachable!("admit: an interrupted admission is run again");
                self.admitting = Some(target);
            }
            Step::Advanced(_) => {}
        }
    }
}

/// An admitted machine is a registered one (#216): `cell add-machine` writes
/// `RegisterNode` before its `Admit`. Judged over the final fold of cell
/// `cell_id`'s control journal.
pub(super) fn admitted_registered(ctx: &SimContext, cell: &Registry, cell_id: u64) {
    for node in crate::machine::admitted_into(ctx.state(), cell_id) {
        assert_always!(
            cell.get(NodeId(node)).is_some(),
            "admit: every admitted machine is registered in its cell",
            { "node" => node, "cell" => cell_id }
        );
    }
}
