//! An admitted machine follows its cell's registry (#211,
//! `docs/architecture.md` §3.2), so its cached registry fold stays current.
//!
//! A founding member serves the cell control journal and folds its own copy
//! (`crate::driver::book`). An admitted machine serves no journal until
//! placement (#212), so it reads the registry through the cell's machines
//! it knows, as any client does:
//!
//! 1. it learns the founding members (the registry's genesis pool) from the
//!    control journal's membership, with a node-only `Inspect`;
//! 2. it folds the registry to its tail each renewal period, through a
//!    client over its book;
//! 3. each new book at or past the cached position is offered to the cache
//!    ([`super::CacheSink`]), and the next read dials the new book.
//!
//! The follow draws no randomness and writes nothing to the cell: it runs in
//! a task of its own beside the machine.

use std::time::Duration;

use moonpool_core::{Detach, Providers, TaskProvider, TimeProvider};
use moonpool_rpc::RpcHandle;
use paros_core::NodeId;
use tokio_util::sync::CancellationToken;

use super::{CacheSink, CachedRegistry, ControlJournals, MachineFacts, cell_book};
use crate::client::bootstrap::cell_members;
use crate::client::checkpoint::{Folder, LoadOutcome, load};
use crate::client::{Client, ClientTunables, Server};
use crate::rpc::NodeClient;
use crate::system::Registry;
use crate::{Address, DriverTunables};

/// What an admitted machine's follow reads, and where it starts.
pub(crate) struct Follow {
    /// The machine, serving its cell now.
    pub(crate) facts: MachineFacts,
    /// Its cell.
    pub(crate) cell: ControlJournals,
    /// The cell's machines it knows at start: its cached registry fold,
    /// else its admission.
    pub(crate) book: Vec<(NodeId, Address)>,
    /// The cached fold's position: a book below it is older than the cache.
    pub(crate) floor: u64,
    /// The machine's cache.
    pub(crate) sink: CacheSink,
}

/// Start `follow` in a task that ends with `shutdown`.
pub(crate) fn spawn<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    follow: Follow,
    tunables: &DriverTunables,
    shutdown: CancellationToken,
) {
    assert!(
        follow.facts.node_id.0 != 0,
        "a machine of a cell has an identity"
    );
    assert!(
        follow.cell.cell.is_set(),
        "a cell names its control journal"
    );
    let every = tunables.election_renew.max(Duration::from_millis(1));
    providers
        .task()
        .spawn_task(
            "paros-machine-follow",
            run(providers.clone(), rpc.clone(), follow, every, shutdown),
        )
        .detach();
}

/// A client over `book`, this machine left out: it serves no journal.
fn client_over<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    facts: &MachineFacts,
    book: &[(NodeId, Address)],
    shutdown: &CancellationToken,
) -> Client<P> {
    let servers = book
        .iter()
        .filter(|(id, _)| *id != facts.node_id)
        .map(|(id, addr)| Server {
            id: id.0,
            node: NodeClient::named(rpc, facts.names.clone(), addr.clone()),
        })
        .collect();
    Client::new(providers, servers, ClientTunables::default()).with_shutdown(shutdown.clone())
}

/// The follow's loop: learn the genesis pool, then fold the registry to its
/// tail each period and offer every new book.
#[tracing::instrument(level = "debug", skip_all, fields(node = follow.facts.node_id.0, cell = follow.cell.cell_id))]
async fn run<P: Providers>(
    providers: P,
    rpc: RpcHandle<P>,
    follow: Follow,
    every: Duration,
    shutdown: CancellationToken,
) {
    let Follow {
        facts,
        cell,
        mut book,
        floor,
        sink,
    } = follow;
    let me = facts.node_id;
    let mut fold: Option<(Vec<NodeId>, Folder<Registry>)> = None;
    let mut first = 0_usize;
    while !shutdown.is_cancelled() {
        let client = client_over(&providers, &rpc, &facts, &book, &shutdown);
        if client.server_count() > 0 {
            first = (first + 1) % client.server_count();
            if fold.is_none() {
                fold = cell_members(&client, cell.cell).await.map(|members| {
                    let founders: Vec<NodeId> = members.into_iter().map(NodeId).collect();
                    let genesis = Registry::new(founders.iter().copied());
                    (founders, Folder::new(genesis))
                });
            }
            if let Some((founders, folder)) = fold.as_mut()
                && let LoadOutcome::Loaded { .. } = load(folder, cell.cell, &client, first, 0).await
                && folder.is_whole()
                && folder.next_seq() > 0
                && folder.next_seq() >= floor
            {
                // The founding members at the addresses this machine knows:
                // the registry moves the ones that registered elsewhere.
                let known: Vec<(NodeId, Address)> = founders
                    .iter()
                    .filter_map(|id| {
                        book.iter()
                            .find(|(b, _)| b == id)
                            .map(|(_, addr)| (*id, addr.clone()))
                    })
                    .collect();
                let machines = cell_book(&known, folder.state());
                if machines != book && !machines.is_empty() {
                    moonpool_assertions::reachable!(
                        "machine: an admitted machine's registry fold moves its book"
                    );
                    tracing::info!(
                        node = me.0,
                        machines = machines.len(),
                        "admitted_book_moved"
                    );
                    book.clone_from(&machines);
                }
                if !machines.is_empty() {
                    sink.offer(CachedRegistry {
                        node: me,
                        position: folder.next_seq(),
                        machines,
                    });
                }
            }
        }
        if providers.time().sleep(every).await.is_err() {
            return;
        }
    }
}
