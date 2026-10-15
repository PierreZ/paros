//! Every journal heals after chaos (#475 (re-place after the Down bound),
//! #191 (status)): once chaos ends, every live journal whose shape the cell
//! can hold shows healthy, as `parosctl status` judges it from an admin cell
//! view (`paros::status::observe`). A member that died for good, or stayed
//! down past `machine_replace_after`, has its slot re-placed; one that
//! answers again rejoins in place.
//!
//! A journal is excused when the cell cannot hold its shape: a member is
//! not serving (down, draining, unknown) and the cell has fewer storage
//! machines to go to than such members (registered, serving the cell, up,
//! with a free slot, outside any maintenance window and holding no slot of
//! the journal). It is excused when the members that serve hold no quorum:
//! no leader can reconfigure it (a wipe of a quorum loses it for good,
//! witness 17795399505419045907 on a0c4aa6). It is excused too while a
//! maintenance window covers one
//! of its members (#359), and on a seed whose coordinators wait longer than
//! the settle tail before they re-place (the shipped profile's bound).

use std::collections::BTreeSet;
use std::time::Duration;

use moonpool_sim::{SimContext, TimeProvider, assert_always, assert_reachable};
use paros::NodeId;
use paros::client::checkpoint::LoadOutcome;
use paros::client::views::{ViewOutcome, ask, request};
use paros::machine::Class;
use paros::status::{Member, Word, judge, member, observe, placed};
use paros::system::{NodeStanding, Registry};
use paros::view::Scope;
use paros::wire::view::{CellQuery, JournalView, ViewReply, view_request::Query};

use super::cell::Cell;
use super::{FleetOps, judged_cell};

impl FleetOps {
    /// Ask the cell for an admin view until every journal it can place
    /// shows healthy, or `deadline`.
    pub(super) async fn journals_heal(&self, ctx: &SimContext, cell: &Cell, deadline: Duration) {
        // A coordinator that waits past the tail never re-places in it.
        if crate::shape::longest_replace_after(ctx.state()) * 2
            > deadline.saturating_sub(ctx.time().now())
        {
            return;
        }
        let servers: Vec<_> = cell.servers.iter().map(|(_, addr)| addr.clone()).collect();
        let mut attempt = 0_u64;
        let mut last: Option<(u64, u64, String)> = None;
        while ctx.time().now() < deadline && !ctx.shutdown().is_cancelled() {
            attempt += 1;
            let outcome = ask(
                self.connector.providers(),
                self.connector.rpc(),
                self.connector.names(),
                &servers,
                cell.first(attempt),
                &request(&Scope::Admin, Query::Cell(CellQuery {})),
                self.patience,
            )
            .await;
            if let ViewOutcome::Answered(reply) = outcome
                && let Some(registry) = read(cell, attempt).await
            {
                let wiped = crate::machine::wiped_ids(ctx.state());
                match unhealthy(&reply, &registry, &wiped) {
                    None => {
                        assert_reachable!("status: every journal shows healthy after chaos");
                        return;
                    }
                    Some(found) => last = Some(found),
                }
            }
            if ctx.time().sleep(Duration::from_millis(500)).await.is_err() {
                return;
            }
        }
        if let Some((tenant, journal, word)) = last {
            assert_always!(
                crate::machine::cell_lost(ctx.state()),
                "status: a journal the cell can hold shows healthy after chaos",
                { "tenant" => tenant, "journal" => journal, "word" => word }
            );
        }
    }
}

/// The cell's registry, folded to its tail.
async fn read(cell: &Cell, attempt: u64) -> Option<Registry> {
    let mut fold = judged_cell(&cell.members);
    let loaded = paros::client::checkpoint::load(
        &mut fold,
        cell.journals.cell,
        &cell.client,
        cell.first(attempt),
        0,
    )
    .await;
    matches!(loaded, LoadOutcome::Loaded { .. }).then(|| fold.state().registry().clone())
}

/// The first data journal of `reply` that does not show healthy and is not
/// excused, as `(tenant, journal, word)`.
fn unhealthy(
    reply: &ViewReply,
    registry: &Registry,
    wiped: &BTreeSet<u64>,
) -> Option<(u64, u64, String)> {
    for tenant in &reply.tenants {
        for journal in tenant.journals.iter().filter(|j| j.kind == "data") {
            let word = observe(journal, &reply.machines);
            if word == Some(Word::Healthy) || excused(reply, registry, journal, wiped) {
                continue;
            }
            let word = word.map_or("none", Word::as_str).to_string();
            return Some((tenant.tenant, journal.journal, word));
        }
    }
    None
}

/// Whether the cell cannot hold `journal`'s shape now: the members that
/// serve hold no quorum (a wipe or an outage took it), a maintenance window
/// covers a member, or fewer machines can take a slot than members fail to
/// serve.
fn excused(
    reply: &ViewReply,
    registry: &Registry,
    journal: &JournalView,
    wiped: &BTreeSet<u64>,
) -> bool {
    let machine = |id: u64| reply.machines.iter().find(|m| m.node_id == id);
    // A re-placement is a reconfiguration by the journal's leader: while the
    // members that serve hold no Phase-1 or no Phase-2 quorum, no leader can
    // run it. A wiped member never serves again: the journal is lost. A down
    // one may answer again later than the tail.
    if let Some(placed) = placed(journal) {
        let serving = |id: NodeId| {
            if wiped.contains(&id.0) {
                Member::Gone
            } else {
                member(machine(id.0))
            }
        };
        let size = placed.members().len();
        if judge(&placed, size, placed.quorum_system(), serving) == Word::Unavailable {
            return true;
        }
    }
    if journal
        .acceptors
        .iter()
        .any(|id| machine(*id).is_some_and(|m| m.maintenance_ms > 0))
    {
        assert_reachable!("status: a maintenance window excuses a journal");
        return true;
    }
    let failing = journal
        .acceptors
        .iter()
        .filter(|id| member(machine(**id)) != Member::Serving)
        .count();
    let short = usize::try_from(journal.desired_acceptors)
        .unwrap_or(usize::MAX)
        .saturating_sub(journal.acceptors.len());
    let held: BTreeSet<u64> = journal.acceptors.iter().copied().collect();
    let free = registry
        .nodes()
        .filter(|(id, n)| {
            n.standing == NodeStanding::Registered
                && n.class == Class::Storage
                && registry.serves(*id)
                && registry.liveness(*id).up
                && registry.available(*id) > 0
                && registry.maintenance_of(*id).next().is_none()
                && !held.contains(&id.0)
        })
        .count();
    // A founding member the registry does not list stores too.
    let founders = reply
        .machines
        .iter()
        .filter(|m| {
            m.founder
                && registry.get(NodeId(m.node_id)).is_none()
                && m.up
                && !held.contains(&m.node_id)
        })
        .count();
    free + founders < failing + short
}
