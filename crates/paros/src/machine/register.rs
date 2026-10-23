//! A machine registers the address it advertises (#349,
//! `docs/architecture/03-2-machines.md` §3.2, §3.3): the machine-to-coordinator request
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
//!    and the term group's reference the coordinator publishes with its
//!    first renewal of a served term, #404);
//! 2. sends it `Register` with its identity, the address it advertises now
//!    and its machine interface;
//! 3. stops once the coordinator answers that the registry holds that
//!    address, or refuses for good. Any other answer, or none, is asked
//!    again after one renewal period: the coordinator may change, or not be
//!    elected yet.
//!
//! The machine reads the election journal from its peers, never from its
//! own node while it has one: a moved founding member's node is the one the
//! cell cannot reach until the registry holds its new address, so a read it
//! serves never confirms, and each round would wait out the read timeout
//! (#479 (renamed founder not registered): 5 s a round, past the final
//! check). A read that finds no coordinator starts from the next peer, so a
//! peer that is down costs one round in two, not every round. The first
//! peer asked is a BUGGIFY decision of the node loop, carried in.
//!
//! The coordinator writes `RegisterNode` only when the address, the name or
//! the machine interface changed. A founding member serves a new machine
//! interface at every start (#404), so each of its starts registers once. The request draws no
//! randomness: it runs in a task of its own beside the machine. It tells
//! the machine when it ends ([`spawn`]'s receiver): a moved founding member
//! holds its journals' clocks (#390, `crate::driver`) and its candidacy for
//! the cell coordinator (#506, `super::coordinator`) until then.

use std::time::Duration;

use crate::ParosProviders;
use moonpool_core::{Detach, LocalTaskProvider, LocalTimeProvider};
use moonpool_rpc::RpcHandle;
use paros_core::{JournalIdentifier, NodeId};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::MachineFacts;
use crate::client::bootstrap::{self, RegisterOutcome};
use crate::client::election::read_election;
use crate::client::journals::Coordinator;
use crate::client::{Client, Server};
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
pub(crate) fn spawn<P: ParosProviders>(
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
    // Its peers, not itself (see the module doc); a lone founding member
    // has only itself, and is its own quorum.
    let peers: Vec<&(NodeId, Address)> =
        book.iter().filter(|(id, _)| *id != facts.node_id).collect();
    let asked: Vec<&(NodeId, Address)> = if peers.is_empty() {
        book.iter().collect()
    } else {
        peers
    };
    assert!(!asked.is_empty(), "a registration asks some machine");
    assert!(
        asked.len() == 1 || asked.iter().all(|(id, _)| *id != facts.node_id),
        "a machine with peers never reads the election from itself"
    );
    // Which peer it asks first (#479): on a BUGGIFY decision, another than
    // the book's first, so a first peer that is down or slow is common.
    let count = asked.len();
    let first = moonpool_buggify::buggify_range!(0.5, 0..count as u64)
        .and_then(|first| usize::try_from(first).ok())
        .unwrap_or(0);
    if first != 0 {
        moonpool_assertions::reachable!("machine: a registration asks another peer first");
    }
    let servers = asked
        .into_iter()
        .map(|(id, addr)| Server {
            id: id.0,
            node: NodeClient::named(rpc, facts.names.clone(), addr.clone()),
        })
        .collect();
    let client =
        Client::new(providers, servers, tunables.internal_client()).with_shutdown(shutdown.clone());
    let every = tunables.election_renew.max(Duration::from_millis(1));
    let providers_task = providers.clone();
    let rpc = rpc.clone();
    providers
        .local_task()
        .spawn_local_task(
            "paros-machine-register",
            run(
                providers_task,
                rpc,
                client,
                (facts, cell_id, election),
                (every, (first, count), ended),
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
async fn run<P: ParosProviders>(
    providers: P,
    rpc: RpcHandle<P>,
    client: Client<P>,
    (facts, cell_id, election): (MachineFacts, u64, JournalIdentifier),
    (every, (first, servers), ended): (Duration, (usize, usize), watch::Sender<bool>),
    shutdown: CancellationToken,
) {
    let identity = facts.identify_ack(cell_id);
    assert_eq!(identity.cell_id, cell_id, "a machine registers in its cell");
    let timeout = client.tunables().request_timeout;
    assert!(servers > 0, "a registration asks some machine");
    let mut first = first % servers;
    while !shutdown.is_cancelled() {
        let coordinator = read_election(&client, election, first)
            .await
            .and_then(|fold| fold.leader().cloned())
            .and_then(|leader| Coordinator::of(&leader.candidate));
        if coordinator.is_none() {
            // The next round starts from the next peer (see the module doc).
            first = (first + 1) % servers;
        }
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
                    tracing::info!(machine = %facts.name, addr = %facts.addr, "machine_registered");
                    ended.send_replace(true);
                    return;
                }
                RegisterOutcome::Refused(refusal) if final_refusal(&refusal) => {
                    tracing::warn!(machine = %facts.name, %refusal, "machine_register_refused");
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
