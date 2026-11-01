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
//! The machine reads the election journal from its peers first and from
//! its own node last: a moved founding member's node is the one the
//! cell cannot reach until the registry holds its new address, so a read it
//! serves may never confirm, and each round would wait out the read timeout
//! (#479 (renamed founder not registered): 5 s a round, past the final
//! check). A founding member that is the election journal's only member is
//! its own quorum, so its own read is the one that confirms. A read that finds no coordinator starts from the next peer, so a
//! peer that is down costs one round in two, not every round. The first
//! peer asked is a BUGGIFY decision of the node loop, carried in.
//!
//! The coordinator writes `RegisterNode` only when the address, the name or
//! the machine interface changed. A founding member serves a new machine
//! interface at every start (#404), so each of its starts registers once. The request draws no
//! randomness: it runs in a task of its own beside the machine. It tells
//! the machine when it ends ([`spawn`]'s receiver): a moved founding member
//! holds its journals' clocks (#390, `paros_roles::node`) and its candidacy for
//! the cell coordinator (#506, `paros_coordinator::cell`) until then.

use std::time::Duration;

use moonpool_core::{Detach, LocalTaskProvider, LocalTimeProvider};
use moonpool_rpc::RpcHandle;
use paros_core::NodeId;
use paros_core::journal::JournalIdentifier;
use paros_net::runtime::ParosProviders;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::MachineFacts;
use paros_client::bootstrap::{self, RegisterOutcome};
use paros_client::election::read_election;
use paros_client::journals::Coordinator;
use paros_client::{Client, Server};
use paros_model::address::Address;
use paros_net::NodeClient;
use paros_tunables::DriverTunables;

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
    // Its peers first, itself last (see the module doc).
    let (asked, peers) = asked_order(&book, facts.node_id);
    assert!(!asked.is_empty(), "a registration asks some machine");
    // Which peer it asks first (#479): on a BUGGIFY decision, another than
    // the book's first, so a first peer that is down or slow is common.
    let count = peers.max(1);
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

/// The machines a registration reads the election journal from, in the
/// order it asks them, and how many of them are peers: every peer in the
/// book's order, then the machine itself. A moved founding member's own
/// read waits on peers that cannot reach it yet (#479), so the peers come
/// first; but the machine itself comes last, never out: a founding member
/// that is the election journal's only member is its own quorum, and its
/// admitted peers' reads wait on it at the address the cell no longer
/// reaches. Seed 9532234736792149858 on 88fc10a: a lone founder renamed in
/// the tail asked only its admitted peer, every read waited out the read
/// timeout, and it registered its new name 32 s after the rename, past
/// the final check.
fn asked_order(book: &[(NodeId, Address)], me: NodeId) -> (Vec<&(NodeId, Address)>, usize) {
    let mut asked: Vec<&(NodeId, Address)> = book.iter().filter(|(id, _)| *id != me).collect();
    let peers = asked.len();
    asked.extend(book.iter().filter(|(id, _)| *id == me));
    assert!(
        asked.len() == book.len(),
        "every machine of the book is asked"
    );
    assert!(
        asked[..peers].iter().all(|(id, _)| *id != me),
        "a machine asks its peers before itself"
    );
    (asked, peers)
}

/// Whether the coordinator's refusal ends the registration: the machine is
/// not one of the cell's, its request is malformed, or it names fewer roles
/// than the registry holds. Asking again cannot change any of them.
fn final_refusal(refusal: &str) -> bool {
    matches!(
        refusal,
        "other_cell" | "unknown_machine" | "malformed" | "fewer_roles"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn book(ids: &[u64]) -> Vec<(NodeId, Address)> {
        ids.iter()
            .map(|id| {
                let addr = Address::parse(&format!("machine-{id}.paros:4500")).expect("an address");
                (NodeId(*id), addr)
            })
            .collect()
    }

    fn ids(asked: &[&(NodeId, Address)]) -> Vec<u64> {
        asked.iter().map(|(id, _)| id.0).collect()
    }

    #[test]
    fn a_machine_asks_its_peers_then_itself() {
        let book = book(&[1, 2, 3]);
        let (asked, peers) = asked_order(&book, NodeId(2));
        assert_eq!(ids(&asked), vec![1, 3, 2]);
        assert_eq!(peers, 2);
    }

    #[test]
    fn a_lone_founder_asks_itself() {
        let book = book(&[7]);
        let (asked, peers) = asked_order(&book, NodeId(7));
        assert_eq!(ids(&asked), vec![7]);
        assert_eq!(peers, 0);
    }

    #[test]
    fn a_machine_outside_its_book_asks_every_peer() {
        let book = book(&[1, 2]);
        let (asked, peers) = asked_order(&book, NodeId(9));
        assert_eq!(ids(&asked), vec![1, 2]);
        assert_eq!(peers, 2);
    }

    #[test]
    fn only_a_refusal_no_retry_can_change_ends_the_registration() {
        for refusal in ["other_cell", "unknown_machine", "malformed", "fewer_roles"] {
            assert!(final_refusal(refusal));
        }
        for refusal in ["not_coordinator", "unreachable", "unavailable", ""] {
            assert!(!final_refusal(refusal));
        }
    }
}
