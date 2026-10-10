//! A machine registers the address it advertises (#349,
//! `docs/architecture.md` §3.2, §3.3): the machine-to-coordinator request
//! path.
//!
//! A machine of a cell may come back advertising another address than the
//! one the cell knows (same `node_id`, new `addr`: the machine moved). The
//! cell control journal has one writer, the cell coordinator, so the
//! machine asks it: a request to a leader. On every start, a formed or
//! admitted machine:
//!
//! 1. reads the cell's election journal, through a client over the cell's
//!    machines it knows, and finds the coordinator's interface (the address
//!    the coordinator publishes with its first renewal of a served term);
//! 2. sends it `Register` with its identity and the address it advertises
//!    now;
//! 3. stops once the coordinator answers that the registry holds that
//!    address, or refuses for good. Any other answer, or none, is asked
//!    again after one renewal period: the coordinator may change, or not be
//!    elected yet.
//!
//! The coordinator writes `RegisterNode` only when the address changed, so
//! a restart at the same address writes nothing. The request draws no
//! randomness: it runs in a task of its own beside the machine. It tells
//! the machine when it ends ([`spawn`]'s receiver): a moved founding member
//! holds its journals' clocks until then (#390, `crate::driver`).

use std::time::Duration;

use moonpool_core::{Detach, Providers, TaskProvider, TimeProvider};
use moonpool_rpc::RpcHandle;
use paros_core::{JournalIdentifier, NodeId};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::MachineFacts;
use crate::client::bootstrap::{self, RegisterOutcome};
use crate::client::election::read_election;
use crate::client::{Client, ClientTunables, Server};
use crate::rpc::NodeClient;
use crate::{Address, DriverTunables};

/// What a machine's registration asks, and of whom.
pub(crate) struct Registration {
    /// The machine, serving its cell now (its incarnation is set).
    pub(crate) facts: MachineFacts,
    /// Its cell.
    pub(crate) cell_id: u64,
    /// The cell's election journal, where the coordinator is found.
    pub(crate) election: JournalIdentifier,
    /// The cell's machines it knows, by advertised address.
    pub(crate) book: Vec<(NodeId, Address)>,
}

/// Start the registration of `registration` in a task that ends with its
/// answer or with `shutdown`. Nothing starts without an election journal or
/// a known machine to read it from. The receiver turns `true` once the
/// registration ended: the registry holds the address, or nothing can
/// change the answer.
pub(crate) fn spawn<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    registration: Registration,
    tunables: &DriverTunables,
    shutdown: CancellationToken,
) -> watch::Receiver<bool> {
    let Registration {
        facts,
        cell_id,
        election,
        book,
    } = registration;
    let (ended, receiver) = watch::channel(false);
    if !election.is_set() || book.is_empty() || cell_id == 0 {
        ended.send_replace(true);
        return receiver;
    }
    assert!(facts.node_id.0 != 0, "a machine of a cell has an identity");
    let servers = book
        .iter()
        .map(|(id, addr)| Server {
            id: id.0,
            node: NodeClient::named(rpc, facts.names.clone(), addr.clone()),
        })
        .collect();
    let client =
        Client::new(providers, servers, ClientTunables::default()).with_shutdown(shutdown.clone());
    let every = tunables.election_renew.max(Duration::from_millis(1));
    let providers_task = providers.clone();
    let rpc = rpc.clone();
    providers
        .task()
        .spawn_task(
            "paros-machine-register",
            run(
                providers_task,
                rpc,
                client,
                (facts, cell_id, election),
                (every, ended),
                shutdown,
            ),
        )
        .detach();
    receiver
}

/// The registration's loop: find the coordinator, ask it, and stop once
/// the registry holds this machine's address or the ask is refused for
/// good.
#[tracing::instrument(level = "debug", skip_all, fields(node = facts.node_id.0, cell = cell_id, addr = %facts.addr))]
async fn run<P: Providers>(
    providers: P,
    rpc: RpcHandle<P>,
    client: Client<P>,
    (facts, cell_id, election): (MachineFacts, u64, JournalIdentifier),
    (every, ended): (Duration, watch::Sender<bool>),
    shutdown: CancellationToken,
) {
    let identity = facts.identify_ack(cell_id);
    assert_eq!(identity.cell_id, cell_id, "a machine registers in its cell");
    let timeout = client.tunables().request_timeout;
    while !shutdown.is_cancelled() {
        let coordinator = read_election(&client, election, 0)
            .await
            .and_then(|fold| fold.leader().cloned())
            .and_then(|leader| Address::parse(&leader.candidate.interface).ok());
        if let Some(target) = coordinator {
            match bootstrap::register(
                &providers,
                &rpc,
                &facts.names,
                &target,
                identity.clone(),
                timeout,
            )
            .await
            {
                RegisterOutcome::Registered => {
                    tracing::info!(node = facts.node_id.0, addr = %facts.addr, "machine_registered");
                    ended.send_replace(true);
                    return;
                }
                RegisterOutcome::Refused(refusal) if final_refusal(&refusal) => {
                    tracing::warn!(node = facts.node_id.0, %refusal, "machine_register_refused");
                    ended.send_replace(true);
                    return;
                }
                RegisterOutcome::Refused(_) | RegisterOutcome::Unreachable => {}
            }
        }
        if providers.time().sleep(every).await.is_err() {
            return;
        }
    }
}

/// Whether the coordinator's refusal ends the registration: the machine is
/// not one of the cell's, or its request is malformed. Asking again cannot
/// change either.
fn final_refusal(refusal: &str) -> bool {
    matches!(refusal, "other_cell" | "unknown_machine" | "malformed")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_refusal_no_retry_can_change_ends_the_registration() {
        for refusal in ["other_cell", "unknown_machine", "malformed"] {
            assert!(final_refusal(refusal));
        }
        for refusal in ["not_coordinator", "unreachable", "unavailable", ""] {
            assert!(!final_refusal(refusal));
        }
    }
}
