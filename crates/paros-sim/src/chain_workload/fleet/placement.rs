//! Placement's oracles (#212 (placement)), over the final folds: the
//! coordinator books an acceptor slot on every member of a journal before it
//! writes the create, and releases them only after the journal's tombstone,
//! so every live journal of every tenant the cell hosts holds one live
//! acceptor booking per member in the cell's registry. The registry itself
//! judges each booking at apply (the class, the capacity, an id booked once),
//! so a placement never exceeds the capacity it was granted.

use std::collections::BTreeSet;
use std::time::Duration;

use moonpool_sim::{SimContext, TimeProvider, assert_always, assert_reachable, assert_sometimes};
use paros::client::checkpoint::LoadOutcome;
use paros::client::journals;
use paros::system::{BookingTarget, Registry, Role};
use paros::tenant::TenantControl;
use paros::{JournalIdentifier, TenantId};

use super::cell::Cell;
use super::judged_cell;

/// Read every hosted tenant's control journal, then the cell's registry,
/// and hold each live journal's members against the registry's bookings.
/// Gives up quietly at `deadline`: the folds' liveness is
/// `final_fleet`'s. The operator learns each tenant control journal from
/// the cell's `HostTenant` (§3.8) and tells `learn` before it reads it.
pub(super) async fn placements_booked(
    ctx: &SimContext,
    known: &Cell,
    deadline: Duration,
    learn: impl Fn(JournalIdentifier),
) {
    let mut attempt = 0_u64;
    while ctx.time().now() < deadline && !ctx.shutdown().is_cancelled() {
        let first = known.first(attempt);
        attempt += 1;
        if let Some((tenants, registry)) = read(known, first, &learn).await {
            judge(&tenants, &registry);
            return;
        }
        if ctx.time().sleep(Duration::from_millis(50)).await.is_err() {
            return;
        }
    }
}

/// The hosted tenants' control journals, read before the registry: a
/// journal created before the first read had its slots booked before it.
async fn read(
    known: &Cell,
    first: usize,
    learn: &impl Fn(JournalIdentifier),
) -> Option<(Vec<TenantControl>, Registry)> {
    let mut cell = judged_cell(&known.members);
    let loaded =
        paros::client::checkpoint::load(&mut cell, known.journals.cell, &known.client, first, 0)
            .await;
    if !matches!(loaded, LoadOutcome::Loaded { .. }) {
        return None;
    }
    let hosted: Vec<(TenantId, paros::JournalId)> = cell
        .state()
        .registry()
        .hosted()
        .filter_map(|tenant| {
            cell.state()
                .registry()
                .hosted_tenant(tenant)
                .map(|h| (tenant, h.control))
        })
        .collect();
    let mut tenants = Vec::with_capacity(hosted.len());
    for (tenant, control) in hosted {
        learn(JournalIdentifier::new(tenant, control));
        tenants.push(journals::list(&known.client, tenant, control).await?);
    }
    let mut after = judged_cell(&known.members);
    let loaded =
        paros::client::checkpoint::load(&mut after, known.journals.cell, &known.client, first, 0)
            .await;
    if !matches!(loaded, LoadOutcome::Loaded { .. }) {
        return None;
    }
    Some((tenants, after.state().registry().clone()))
}

/// The oracle itself, and the gate that a placement spread over zones.
fn judge(tenants: &[TenantControl], registry: &Registry) {
    for control in tenants {
        let tenant = control.tenant();
        if registry.dropped(tenant) {
            continue;
        }
        for (id, journal) in control.live() {
            let target = BookingTarget::Journal(JournalIdentifier::new(tenant, id));
            let members = journal.config.members();
            let booked = members
                .iter()
                .filter(|member| {
                    registry.bookings().any(|(_, b)| {
                        b.node == **member && b.role == Role::Acceptor && b.target == target
                    })
                })
                .count();
            assert_always!(
                booked == members.len(),
                "placement: every live journal's members hold its slots after chaos",
                { "journal" => target.tenant().0, "members" => members.len(), "booked" => booked }
            );
            assert_reachable!("placement: a live journal's slots are judged after chaos");
            let zones: BTreeSet<&str> = members
                .iter()
                .filter_map(|m| registry.get(*m).map(|n| n.failure_domain.as_str()))
                .collect();
            if members.len() > 1 {
                assert_sometimes!(
                    zones.len() > 1,
                    "placement: a journal's members span two zones"
                );
            }
        }
    }
}
