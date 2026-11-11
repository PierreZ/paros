//! The regrown-journal scenario's operator (#1003 (uncounted skips from
//! fix-main), `crate::shape::regrown_journal`): in the settle tail, the
//! last operator creates a journal of the default mode, configures it to
//! one acceptor, and waits until a member whose slot the cell released
//! stopped the journal. Then it configures the journal back to the default
//! mode. On a cell of four machines or fewer the tenant coordinator books a
//! released machine again, and that machine must start the journal again:
//! the heal oracle judges the journal after this step, and the convergence
//! oracle excuses a machine only while it has the journal stopped. The
//! negative hunt's mutant `machine-released-booking-never-restarts` breaks
//! that start.

use std::collections::BTreeSet;
use std::time::Duration;

use moonpool_sim::{RandomProvider, SimContext, TimeProvider, assert_reachable};
use paros::client::checkpoint::CheckpointPolicy;
use paros::client::journals::{JournalAnswer, JournalOp, JournalRequest};
use paros::core::journal::{JournalIdentifier, TenantId, WriterMode};
use paros::model::tenant::{Desired, StoreDurability};

use super::FleetOps;
use super::journals::one_acceptor;

/// The journal the operator creates for the scenario, apart from the ones
/// the run's steps use.
const NAME: &[u8] = b"tail-regrow";

/// How long the operator waits for a released member to stop the journal,
/// and then for a booked one to start it again: a mode change, its
/// release and the machines' next fold, over the tail's slow links.
const WAIT: Duration = Duration::from_secs(30);

/// How often the operator reads the tenant board.
const LOOK: Duration = Duration::from_millis(100);

impl FleetOps {
    /// The regrown-journal scenario's tail step (see the module doc).
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(in crate::workload) async fn regrown_journal_in_tail(
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
        let journal = JournalIdentifier::new(tenant, id);
        self.note_journals([journal]);
        // A journal of one member releases nothing when it shrinks.
        if config.members().len() < 2 {
            return;
        }
        if !self
            .configure_tail(ctx, &cell, (election, tenant), one_acceptor())
            .await
        {
            return;
        }
        let Some(released) = self
            .wait_board(ctx, journal, |stopped| !stopped.is_empty())
            .await
        else {
            return;
        };
        assert_reachable!("journals: a narrowed journal stops on a released member");
        if !self
            .configure_tail(ctx, &cell, (election, tenant), Desired::DEFAULT)
            .await
        {
            return;
        }
        // A released machine that the widened mode books again starts the
        // journal again: it leaves the board's stopped set.
        if self
            .wait_board(ctx, journal, |stopped| !released.is_subset(stopped))
            .await
            .is_some()
        {
            assert_reachable!("journals: a widened journal starts again on a released member");
        }
    }

    /// Configure the scenario's journal to `desired`; whether the tenant
    /// coordinator took the change.
    async fn configure_tail(
        &mut self,
        ctx: &SimContext,
        cell: &super::cell::Cell,
        (election, tenant): (JournalIdentifier, TenantId),
        desired: Desired,
    ) -> bool {
        let configure = JournalRequest {
            request: ctx.random().random_range(1..u64::MAX),
            tenant,
            op: JournalOp::Configure {
                name: Some(NAME.to_vec()),
                desired,
                roles: None,
            },
        };
        matches!(
            self.ask_coordinator(cell, election, &configure).await,
            JournalAnswer::Configured { .. }
        )
    }

    /// Read the machines that have `journal` stopped from the tenant board
    /// until `done` holds, for at most [`WAIT`]: the set that made it hold,
    /// or `None` when the wait ran out.
    async fn wait_board(
        &self,
        ctx: &SimContext,
        journal: JournalIdentifier,
        done: impl Fn(&BTreeSet<u64>) -> bool,
    ) -> Option<BTreeSet<u64>> {
        let deadline = self.time.now() + WAIT;
        loop {
            let stopped = {
                let board = crate::audit::tenants::tenant_board(ctx.state());
                crate::audit::tenants::lock(&board).stopped_on(journal)
            };
            if done(&stopped) {
                return Some(stopped);
            }
            if self.time.now() >= deadline || self.time.sleep(LOOK).await.is_err() {
                return None;
            }
        }
    }
}
