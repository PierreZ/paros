//! Maintenance windows (#359, #661 (maintenance windows)): every move
//! `parosctl cell maintenance` can make. On its own BUGGIFY location of
//! `ADMIT`, on any seed, the operator opens a window over any registered
//! machine of the cell (a founding member included) or over its zone,
//! opens it again with a new length (the cell coordinator restarts the
//! window's clock), or ends it (`--for 0s`, `for_ms == 0`). A machine may
//! be up or down while it is under a window. On a gone-machine seed
//! (`crate::shape::gone_machine`), the operator ends the window over the
//! gone machine early, on a coin, while the machine is down.
//!
//! The board notes every window an operator sent
//! ([`crate::machine::MaintenanceNote`]) and cuts a note short when the
//! operator sends an end or a new window over its scope, so the final
//! oracle (`placement::maintained`) judges only the time a window held.
//! Once a window ends, a machine down past `machine_replace_after` is
//! re-placed: the final check `healthy::journals_heal` excuses a journal
//! only while a window covers a member.

use std::time::Duration;

use moonpool_sim::{RandomProvider, SimContext, TimeProvider, assert_reachable, buggify_with_prob};
use paros::NodeId;
use paros::client::cell::CellSession;
use paros::client::checkpoint::CheckpointPolicy;
use paros::model::registry::{BookingTarget, MaintenanceScope, NodeStanding};

use super::FleetOps;

/// A maintenance window that outlasts every run (#359).
pub(super) const LONG_WINDOW: Duration = Duration::from_secs(3_600);

impl FleetOps {
    /// Open, open again or end a maintenance window over a registered
    /// machine of the cell or its zone (see the module doc). Whether it
    /// ran.
    #[tracing::instrument(level = "debug", skip_all, fields(client = self.client_id))]
    pub(in crate::chain_workload) async fn window_step(
        &mut self,
        ctx: &SimContext,
        policy: CheckpointPolicy,
        draw: u64,
    ) -> bool {
        let Some(cell) = self.learn(ctx).await else {
            return false;
        };
        let members: Vec<NodeId> = cell.members.iter().copied().map(NodeId).collect();
        let founders: Vec<(NodeId, paros::Address)> = cell
            .servers
            .iter()
            .map(|(id, addr)| (NodeId(*id), addr.clone()))
            .collect();
        let mut session = CellSession::new(
            cell.journals,
            &members,
            founders,
            self.leader_seeds.next(),
            policy,
        );
        let first = cell.first(draw);
        if session.open(&cell.client, first).await.is_err() {
            return false;
        }
        let registry = session.registry();
        // On a gone-machine seed, the window over the gone machine ends
        // early while the machine is down (#475 (re-place after the Down
        // bound)).
        let over_gone = crate::machine::gone_down(ctx.state()).and_then(|gone| {
            registry
                .maintenance_of(NodeId(gone))
                .map(|(scope, _)| scope.clone())
                .next()
        });
        if let Some(scope) = over_gone
            && buggify_with_prob!(0.5)
        {
            assert_reachable!("maintenance: an operator ends a window while its machine is down");
            self.send_window(ctx, &mut session, (&cell.client, first), scope, 0)
                .await;
            return true;
        }
        let open: Vec<MaintenanceScope> = registry
            .maintenance()
            .map(|(scope, _)| scope.clone())
            .collect();
        if !open.is_empty() && buggify_with_prob!(0.5) {
            let scope = open[usize::try_from(draw % open.len() as u64).unwrap_or(0)].clone();
            let for_ms = if buggify_with_prob!(0.5) {
                assert_reachable!("maintenance: an operator ends a window");
                0
            } else {
                assert_reachable!("maintenance: an operator opens a window again");
                window_ms(ctx)
            };
            self.send_window(ctx, &mut session, (&cell.client, first), scope, for_ms)
                .await;
            return true;
        }
        let machines: Vec<NodeId> = registry
            .nodes()
            .filter(|(_, n)| n.standing == NodeStanding::Registered)
            .map(|(id, _)| id)
            .collect();
        if machines.is_empty() {
            return false;
        }
        let node = machines[usize::try_from(draw % machines.len() as u64).unwrap_or(0)];
        if cell.members.contains(&node.0) {
            assert_reachable!("maintenance: an operator opens a window over a founding member");
        }
        let Some(scope) = scope_of(&session, node) else {
            return false;
        };
        let for_ms = window_ms(ctx);
        self.send_window(ctx, &mut session, (&cell.client, first), scope, for_ms)
            .await;
        true
    }

    /// Send a window of `for_ms` over `scope` (`0` ends it) through
    /// `session`, noting it on the board first: an answer lost on the way
    /// may still have landed.
    pub(super) async fn send_window(
        &mut self,
        ctx: &SimContext,
        session: &mut CellSession,
        (client, first): (&crate::client::ChainClient, usize),
        scope: MaintenanceScope,
        for_ms: u64,
    ) {
        let sent = ctx.time().now();
        // A window this replaces or ends holds no longer than now.
        crate::machine::note_window_cut(ctx.state(), &scope, sent);
        let registry = session.registry();
        // A zone window covers every machine of the zone; the oracle
        // judges each machine it covers now.
        let covered: Vec<NodeId> = registry
            .nodes()
            .filter(|(id, n)| {
                n.standing == NodeStanding::Registered
                    && match &scope {
                        MaintenanceScope::Machine(machine) => machine == id,
                        MaintenanceScope::Zone(zone) => n.locality.zone == *zone,
                    }
            })
            .map(|(id, _)| id)
            .collect();
        let notes: Vec<crate::machine::MaintenanceNote> = covered
            .iter()
            .map(|node| crate::machine::MaintenanceNote {
                node: node.0,
                scope: scope.clone(),
                sent,
                length: Duration::from_millis(for_ms),
                slots: registry
                    .bookings()
                    .filter(|(_, b)| b.node == *node)
                    .map(|(id, b)| (id, b.target))
                    .collect::<Vec<(u64, BookingTarget)>>(),
            })
            .collect();
        if let Ok(true) = session.maintenance(client, first, scope, for_ms).await {
            if for_ms == 0 {
                tracing::info!("maintenance_ended");
                return;
            }
            assert_reachable!("drain: an operator opens a maintenance window");
            tracing::info!(for_ms, "maintenance_opened");
            for note in notes {
                crate::machine::note_maintenance(ctx.state(), note);
            }
        }
    }
}

/// The scope of a window over `node`: on a coin its zone, else the machine.
pub(super) fn scope_of(session: &CellSession, node: NodeId) -> Option<MaintenanceScope> {
    let zone = session.registry().get(node)?.locality.zone.clone();
    Some(if buggify_with_prob!(0.5) {
        assert_reachable!("drain: an operator puts a zone in maintenance");
        MaintenanceScope::Zone(zone)
    } else {
        MaintenanceScope::Machine(node)
    })
}

/// A window's length: on a coin a short one, 1 to 4 s, which ends while
/// the run goes on, else one that outlasts the run.
pub(super) fn window_ms(ctx: &SimContext) -> u64 {
    let length = if buggify_with_prob!(0.5) {
        Duration::from_millis(1_000 + ctx.random().random_range(0..3_000))
    } else {
        LONG_WINDOW
    };
    u64::try_from(length.as_millis()).unwrap_or(u64::MAX)
}
