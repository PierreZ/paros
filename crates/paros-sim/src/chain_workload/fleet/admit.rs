//! `ADMIT` (#216): `cell add-machine` against the machines, through the
//! library's `paros::client::cell` — the code `parosctl cell add-machine`
//! prints. The target is a machine outside the founding members (an idle
//! machine, or one admitted before), or on its own BUGGIFY location a
//! founding member, which is in the cell already. The admission may stop
//! after its registration (a crash at a step) and is resumed by this
//! operator's next `ADMIT`. On its own BUGGIFY location, an operator founds
//! another cell on a wiped member's address instead (`other_cell`).

use paros::Address;

use moonpool_sim::{Providers as _, TimeProvider as _};
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
        let replaced = self.replaced_target(ctx);
        if self.admitting.is_none()
            && replaced.is_none()
            && !crate::shape::replaced_founder(ctx.state())
            && crate::machine::other_cell_target(ctx.state()).is_some()
            // The wiped-founder scenario lines up the wipe this needs: on
            // its seeds, the operator always founds the other cell.
            && (crate::shape::wiped_founder(ctx.state()) || buggify_with_prob!(0.5))
            && self.found_other_cell(ctx).await
        {
            return;
        }
        // On its own location, the operator drains an admitted machine
        // instead (#474 (journals on admitted machines)).
        let drained = crate::shape::drained_machine(ctx.state());
        if self.admitting.is_none()
            && replaced.is_none()
            && (drained || buggify_with_prob!(0.25))
            && self.drain(ctx, policy, draw, drained).await
        {
            return;
        }
        let founders = self.layout.founders;
        // On a second-cell seed (#232 (multiple cells)) the last machine is
        // the second cell's.
        let reserved = usize::from(self.layout.reserved);
        let outside = founders..self.machines.len() - reserved;
        let mut at_founder = false;
        let fresh = self.admitting.is_none() && replaced.is_none();
        let mut target = match self.admitting.take().or(replaced) {
            Some(target) => target,
            None if outside.is_empty() || buggify_with_prob!(0.1) => {
                at_founder = true;
                self.machines[usize::try_from(draw % founders.max(1) as u64).unwrap_or(0)].clone()
            }
            None => {
                let span = (outside.end - outside.start) as u64;
                self.machines[outside.start + usize::try_from(draw % span).unwrap_or(0)].clone()
            }
        };
        let Some(cell) = self.learn(ctx).await else {
            assert_reachable!("admit: an admission finds no cell formed yet");
            return;
        };
        // On its own location, a founding member the operator did not reach
        // (one that does not serve its cell yet, mid `cell init`): the
        // admission names it in no book the session holds. The library
        // asserted on it once (hunt seed 7921884428541148436). Also on a
        // freshly drawn target outside the founders, and always on a
        // downed-founder seed, whose down founder serves nothing: the gate
        // needed a founder target and a founder unreached at once, and
        // main's sweep at c89f86e missed it (#446).
        if (at_founder
            || (fresh && (crate::shape::downed_founder(ctx.state()) || buggify_with_prob!(0.1))))
            && buggify_with_prob!(0.5)
            && let Some(unreached) = self.machines[..founders.min(self.machines.len())]
                .iter()
                .find(|addr| cell.servers.iter().all(|(_, served)| served != *addr))
        {
            assert_reachable!("admit: an operator admits a founding member it did not reach");
            target = unreached.clone();
        }
        let members: Vec<NodeId> = cell.members.iter().copied().map(NodeId).collect();
        let founders: Vec<(NodeId, Address)> = cell
            .servers
            .iter()
            .map(|(id, addr)| (NodeId(*id), addr.clone()))
            .collect();
        let (token, bad_token) = self.admission_token(ctx);
        let mut session = CellSession::new(
            cell.journals,
            &members,
            founders,
            self.leader_seeds.next(),
            policy,
        )
        .with_token(token);
        let client = cell.client.clone();
        let first = cell.first(draw);
        let providers = self.connector.providers().clone();
        let rpc = self.connector.rpc().clone();
        let names = self.connector.names().clone();
        if buggify_with_prob!(0.25) {
            let step = session
                .admit_step(&providers, &rpc, &client, &names, first, &target)
                .await;
            if let Step::Advanced(stage) = step {
                assert_reachable!("admit: an admission stops after its registration");
                reach(stage);
                self.admitting = Some(target);
            }
            return;
        }
        let run = session
            .add_machine(
                &providers,
                &rpc,
                &client,
                &names,
                first,
                &target,
                self.patience,
            )
            .await;
        self.judge_admission(ctx, &session, target, run, bad_token);
    }

    /// The operator's token for an admission (#245 (Biscuit tokens)):
    /// `admin` on a pinned seed; on its own location a tenant's, which a
    /// pinned machine refuses. Whether it is the tenant's.
    fn admission_token(&self, ctx: &SimContext) -> (Vec<u8>, bool) {
        let now = self.connector.providers().time().now();
        if crate::frontend::machines_pinned(ctx.state()) && buggify_with_prob!(0.15) {
            assert_reachable!("admit: an operator presents a tenant token to a pinned machine");
            let token = crate::frontend::token(
                ctx.state(),
                "acme",
                "",
                crate::frontend::Presented::Own,
                now,
            );
            return (token, true);
        }
        (crate::frontend::operator_token(ctx.state(), now), false)
    }

    /// One admission step at a machine outside the founding members, kept
    /// only when it stops after the registration: the admission is then in
    /// flight, for this operator's next `ADMIT` or for the next term's
    /// coordinator (#240). Whether one is in flight now.
    pub(in crate::chain_workload) async fn register_only(
        &mut self,
        ctx: &SimContext,
        policy: CheckpointPolicy,
        draw: u64,
    ) -> bool {
        let reserved = usize::from(self.layout.reserved);
        let outside = self.layout.founders..self.machines.len() - reserved;
        if outside.is_empty() {
            return false;
        }
        let span = (outside.end - outside.start) as u64;
        let target =
            self.machines[outside.start + usize::try_from(draw % span).unwrap_or(0)].clone();
        let Some(cell) = self.learn(ctx).await else {
            return false;
        };
        let members: Vec<NodeId> = cell.members.iter().copied().map(NodeId).collect();
        let founders: Vec<(NodeId, Address)> = cell
            .servers
            .iter()
            .map(|(id, addr)| (NodeId(*id), addr.clone()))
            .collect();
        let token =
            crate::frontend::operator_token(ctx.state(), self.connector.providers().time().now());
        let mut session = CellSession::new(
            cell.journals,
            &members,
            founders,
            self.leader_seeds.next(),
            policy,
        )
        .with_token(token);
        let providers = self.connector.providers().clone();
        let rpc = self.connector.rpc().clone();
        let names = self.connector.names().clone();
        let step = session
            .admit_step(
                &providers,
                &rpc,
                &cell.client,
                &names,
                cell.first(draw),
                &target,
            )
            .await;
        if let Step::Advanced(stage) = step {
            assert_reachable!("admit: an admission stops after its registration");
            reach(stage);
            self.admitting = Some(target);
        }
        self.admitting.is_some()
    }

    /// What an admission came to, judged.
    fn judge_admission(
        &mut self,
        ctx: &SimContext,
        session: &CellSession,
        target: Address,
        run: Run<NodeId>,
        bad_token: bool,
    ) {
        run.steps.iter().copied().for_each(reach);
        let founder = crate::machine::is_founder(ctx.state(), &target);
        assert_always!(
            !bad_token || !run.steps.contains(&Stage::Admit),
            "admit: a pinned machine never admits for a tenant token"
        );
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
                    if session.founders().len() < session.members().len() {
                        // The admission still names every founder: the
                        // admitted machine's journals derive the same
                        // matchmaker set as the founders' (the matchmaker
                        // audit judges it at their boot).
                        assert_reachable!(
                            "admit: an admission names a founder the operator did not reach"
                        );
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
                // Or the machine an operator founds the second cell on
                // (#232 (multiple cells)).
                assert_always!(
                    founder || crate::machine::is_other_founder(ctx.state(), &target),
                    "admit: only a founding member refuses an admission for cell init"
                );
                assert_reachable!("admit: a founding member mid cell init refuses an admission");
            }
            Step::Refused(FleetRefusal::Denied { denial, .. }) => {
                assert_always!(
                    bad_token && denial == paros::frontend::Denial::Forbidden,
                    "admit: a pinned machine denies only a tenant token",
                    { "denial" => denial.as_str() }
                );
                assert_reachable!("admit: a pinned machine denies a tenant token");
                // The registration may stand: the next `ADMIT` finishes it
                // with the operator's own token.
                self.admitting = Some(target);
            }
            Step::Refused(FleetRefusal::OtherCell { cell_id, .. }) => {
                // One cell per run, unless a wiped one-founder cell formed
                // again over the new machine (#246), or an operator founded
                // a cell on the machine (#216, #232 (multiple cells)).
                assert_always!(
                    crate::machine::founder_wiped(ctx.state())
                        || crate::machine::is_other_founder(ctx.state(), &target),
                    "admit: a machine of another cell exists only after a wipe",
                    { "cell" => cell_id }
                );
            }
            Step::Refused(FleetRefusal::Retired { node }) => {
                // A machine an operator removed is retired, and a retired
                // id never comes back (#359, witness 3450446110933049764
                // on 0bf2b96).
                assert_always!(
                    crate::machine::remove_asked(ctx.state(), node.0),
                    "admit: only a removed machine is refused as retired",
                    { "node" => node.0 }
                );
                assert_reachable!("admit: a removed machine is refused admission");
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
