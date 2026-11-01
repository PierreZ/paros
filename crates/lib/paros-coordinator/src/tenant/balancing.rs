//! The tenant coordinator's half of the balancer (#687 (rebalance a cell)):
//! the decision is the pure [`paros_model::balance`]; this module records it
//! in the registry, the balancer's one store, through the term's session.

use moonpool_rpc::RpcHandle;
use paros_client::Client;
use paros_client::cell::CellSession;
use paros_model::address::Names;
use paros_model::balance::{Balance, LeaderMove, Leading, Rebalance, Step};
use paros_model::placement;
use paros_model::registry::{BookingTarget, Role, SystemCommand};
use paros_net::runtime::ParosProviders;

use crate::tenant::moves::{Leaving, journal_nodes, stays};

/// One balance round over the storage machines `storage` (with their
/// busyness) and the round's `gone`: the move for the move path to take this
/// round, if any. `None` when a write did not land: the term writes nothing
/// more.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) async fn step<P: ParosProviders>(
    ctx: (&P, &RpcHandle<P>, &Names, &Client<P>),
    cell: &mut CellSession,
    balance: &mut Balance,
    (storage, leading): (&[placement::Machine], &Leading),
    gone: &Leaving,
) -> Option<Option<Rebalance>> {
    let client = ctx.3;
    let registry = cell.registry();
    let step = balance.round(registry, storage, |node| stays(registry, gone, node));
    match step {
        Step::Idle => {
            let handoff = balance.leader_round(
                registry,
                storage,
                |node| stays(registry, gone, node),
                leading,
            );
            if let Some(handoff) = handoff {
                hand_on(ctx, cell, handoff).await;
            }
            Some(None)
        }
        Step::Moving(rebalance) => {
            assert_eq!(
                cell.registry().balance().moving().map(|m| m.booking),
                Some(rebalance.booking),
                "the move in flight is the registry's"
            );
            Some(Some(rebalance))
        }
        Step::Start(rebalance) => {
            let command = SystemCommand::BalanceStarted {
                booking: rebalance.booking,
                journal: rebalance.journal,
                from: rebalance.from,
                to: rebalance.to,
                reason: rebalance.reason,
            };
            cell.record(client, 0, &command).await.ok()?;
            // The registry judges it again at apply: a booking released in
            // between, or a pause an operator wrote, refuses it.
            let started = cell
                .registry()
                .balance()
                .moving()
                .is_some_and(|m| m.booking == rebalance.booking);
            if !started {
                moonpool_assertions::reachable!("coordinator: the registry refuses a balance move");
                return Some(None);
            }
            moonpool_assertions::reachable!("coordinator: a balance move is recorded");
            moonpool_buggify::hint!("balance move recorded, not booked").await;
            Some(Some(rebalance))
        }
        Step::Abandon(booking) => {
            cell.record(client, 0, &SystemCommand::BalanceAbandoned { booking })
                .await
                .ok()?;
            assert!(
                cell.registry()
                    .balance()
                    .moving()
                    .is_none_or(|m| m.booking != booking),
                "an abandoned move is not in flight"
            );
            Some(None)
        }
    }
}

/// Ask `handoff`'s leader to hand its journal on to its member (#687
/// (rebalance a cell)): the cooperative handoff, with no election. A
/// refusal or no answer changes nothing: the next round judges again.
async fn hand_on<P: ParosProviders>(
    ctx: (&P, &RpcHandle<P>, &Names, &Client<P>),
    cell: &CellSession,
    handoff: LeaderMove,
) {
    let target = BookingTarget::Journal(handoff.journal);
    let slots: Vec<_> = cell
        .registry()
        .bookings()
        .filter(|(_, b)| b.role == Role::Acceptor && b.target == target)
        .map(|(id, b)| (id, *b))
        .collect();
    let Some((nodes, ids)) = journal_nodes(ctx, cell, &slots) else {
        return;
    };
    let Some(at) = ids.iter().position(|id| *id == handoff.from) else {
        return;
    };
    assert!(
        ids.contains(&handoff.from),
        "the leader is a member it calls"
    );
    if nodes.handoff(at, handoff.journal, handoff.to).await {
        moonpool_assertions::reachable!("coordinator: a leader balance hands a journal on");
        tracing::info!(journal = %handoff.journal, from = handoff.from.0, to = handoff.to.0, "leader_balanced");
    }
}
