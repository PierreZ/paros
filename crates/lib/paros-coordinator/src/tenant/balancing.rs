//! The tenant coordinator's half of the balancer (#687 (rebalance a cell)):
//! the decision is the pure [`paros_model::balance`]; this module records it
//! in the registry, the balancer's one store, through the term's session.

use paros_client::Client;
use paros_client::cell::CellSession;
use paros_model::balance::{Balance, Rebalance, Step};
use paros_model::placement;
use paros_model::registry::SystemCommand;
use paros_net::runtime::ParosProviders;

use crate::tenant::moves::{Leaving, stays};

/// One balance round over the storage machines `storage` (with their
/// busyness) and the round's `gone`: the move for the move path to take this
/// round, if any. `None` when a write did not land: the term writes nothing
/// more.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) async fn step<P: ParosProviders>(
    client: &Client<P>,
    cell: &mut CellSession,
    balance: &mut Balance,
    storage: &[placement::Machine],
    gone: &Leaving,
) -> Option<Option<Rebalance>> {
    let registry = cell.registry();
    let step = balance.round(registry, storage, |node| stays(registry, gone, node));
    match step {
        Step::Idle => Some(None),
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
