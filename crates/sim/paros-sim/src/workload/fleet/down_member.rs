//! The down-member scenario's operator (#777 (mode change meets a down
//! member), `crate::shape::down_member`): in the settle tail, on a calm
//! cell, the last operator creates a journal of the default mode, picks
//! one of its members for `crate::world::scenarios::down_member` to crash,
//! and, once every cell coordinator marked it down, configures the journal
//! with three acceptor partitions of one acceptor each: the same members
//! under another quorum system. The tenant coordinator books no new slot,
//! so the change's new configuration names the down member, and the change
//! waits until the member is back. The operator waits for that too, so the
//! final checks after it judge a cell whose machines are all up.

use std::time::Duration;

use moonpool_sim::{RandomProvider, SimContext, TimeProvider, assert_reachable};
use paros::client::journals::{JournalAnswer, JournalOp, JournalRequest};
use paros::core::journal::{JournalIdentifier, WriterMode};
use paros::model::tenant::{Desired, Durability, StoreDurability};

use paros::client::checkpoint::CheckpointPolicy;

use super::journals::drawn_counts;
use super::{FleetOps, NAMES, Pending, tenant_identifier};

/// The journal the operator creates for the scenario, apart from the ones
/// the run's steps use.
const NAME: &[u8] = b"tail-down";

/// How long the operator waits for the injector to strike: it polls once
/// per 50 ms (`crate::world::outage::POLL`).
const STRIKE_WITHIN: Duration = Duration::from_secs(2);

/// The settle tail's beat for the tenant creation.
const BEAT: Duration = Duration::from_millis(100);

/// How often the operator looks for the strike.
const LOOK: Duration = Duration::from_millis(50);

impl FleetOps {
    /// The down-member scenario's tail step (see the module doc).
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(in crate::workload) async fn down_member_in_tail(
        &mut self,
        ctx: &SimContext,
        policy: CheckpointPolicy,
        draw: u64,
    ) {
        let mut ready = self.journal_tenant(ctx, draw).await;
        if ready.is_none() {
            self.tenant_in_tail(ctx, policy, draw).await;
            ready = self.journal_tenant(ctx, draw).await;
        }
        let Some((cell, tenant, _, _)) = ready else {
            return;
        };
        let Some(election) = cell.journals.election else {
            return;
        };
        let create = JournalRequest {
            request: ctx.random().random_range(1..u64::MAX),
            tenant,
            op: JournalOp::Create {
                tags: self.birth_tags(tenant, NAME),
                name: NAME.to_vec(),
                writer: WriterMode::Single,
                store: StoreDurability::default(),
                desired: Some(Desired::DEFAULT),
            },
        };
        let JournalAnswer::Created { id, config } =
            self.ask_coordinator(&cell, election, &create).await
        else {
            return;
        };
        self.note_journals([JournalIdentifier::new(tenant, id)]);
        let members: Vec<u64> = config.members().iter().map(|m| m.0).collect();
        // One member down must leave the journal a majority.
        if members.len() < 3 {
            return;
        }
        let target = members[crate::draw_index(draw, members.len())];
        if !crate::machine::request_member_down(ctx.state(), target) {
            return;
        }
        let Some((from, until)) = self.member_struck(ctx).await else {
            return;
        };
        if self
            .time
            .sleep(from.saturating_sub(self.time.now()))
            .await
            .is_err()
        {
            return;
        }
        assert_reachable!("journals: a configure is sent while a member is down");
        let configure = JournalRequest {
            request: ctx.random().random_range(1..u64::MAX),
            tenant,
            op: JournalOp::Configure {
                name: Some(NAME.to_vec()),
                desired: same_members(draw),
                roles: None,
            },
        };
        if let JournalAnswer::Configured { .. } =
            self.ask_coordinator(&cell, election, &configure).await
        {
            assert_reachable!("journals: a configure lands while a member is down");
        }
        // The machine is back before the final checks go on.
        let _ = self.time.sleep(until.saturating_sub(self.time.now())).await;
    }

    /// Create a tenant on a cell that has no `READY` one, as `TENANT`
    /// does, so the scenario has a tenant to create its journal in. The
    /// creation runs as an operation the settle tail finishes: to its end,
    /// through a coordinator handover.
    pub(super) async fn tenant_in_tail(
        &mut self,
        ctx: &SimContext,
        policy: CheckpointPolicy,
        draw: u64,
    ) {
        // Only on a cell this operator reaches: a creation that finds no
        // cell never ends.
        if self.pending.is_some()
            || crate::machine::cell_lost(ctx.state())
            || self.learn(ctx).await.is_none()
        {
            return;
        }
        let name = NAMES[crate::draw_index(draw, NAMES.len())].to_vec();
        let identifier = tenant_identifier(draw);
        self.pending = Some(Pending::Create(name, vec![identifier]));
        self.settle(ctx, policy, BEAT).await;
        assert_reachable!("fleet: the down-member operator creates a tenant in the tail");
    }

    /// When the struck member is marked down and when it comes back, once
    /// the injector struck it; `None` when it did not within
    /// [`STRIKE_WITHIN`] (a machine of the run was dead).
    async fn member_struck(&self, ctx: &SimContext) -> Option<(Duration, Duration)> {
        let deadline = self.time.now() + STRIKE_WITHIN;
        loop {
            if let Some(span) = crate::machine::member_down_span(ctx.state()) {
                return Some(span);
            }
            if self.time.now() >= deadline || self.time.sleep(LOOK).await.is_err() {
                return None;
            }
        }
    }
}

/// Three acceptor partitions of one acceptor each: on a journal of the
/// default mode's three members, the same members under another quorum
/// system. The role counts are drawn as a configure's.
fn same_members(draw: u64) -> Desired {
    let desired = Durability::new(0, 3).map_or(Desired::DEFAULT, |durability| Desired {
        durability,
        ..Desired::DEFAULT
    });
    drawn_counts(desired, draw)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The configure keeps a default journal's three members: three
    /// partitions of one acceptor each, whatever counts the draw names.
    #[test]
    fn the_configure_keeps_three_members_under_another_system() {
        for draw in [0, 1, 7, u64::MAX, 0x9E37_79B9_7F4A_7C15] {
            let desired = same_members(draw);
            assert_eq!(desired.durability.partitions, 3);
            assert_eq!(desired.acceptors(), Desired::DEFAULT.acceptors());
            assert_ne!(desired.durability, Desired::DEFAULT.durability);
        }
    }
}
