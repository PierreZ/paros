//! **`Resolve`** (#216, `docs/architecture.md` §3.5): "which references
//! serve tenant T", the client's first call to its entry endpoint.
//!
//! The design has two answerers: a resolver per region answers the cell hop
//! from the universe directory (M12), and any machine of the tenant's cell
//! answers the frontend hop from its registry fold (#192). Neither role
//! exists yet, so every machine of a cell answers both hops in one answer:
//!
//! 1. the tenant's name to its id, its control journal and its cell, from a
//!    fold of the universe directory ([`FleetDirectory`]), when this cell
//!    serves it. Only a `READY` `users` tenant resolves
//!    ([`crate::client::names::tenant_in`]);
//! 2. the cell's machines, at the addresses its registry holds
//!    ([`super::cell_book`]). The client calls them until the frontend
//!    exists; then the answer names the tenant's frontends.
//!
//! The machine reads both journals as any client does, through the cell's
//! machines it knows, and keeps both folds between requests, so an answer
//! reads only what the journals gained since the last one. A fold is a
//! hint, never the authority: a stale answer costs the client one refused
//! call, and the client resolves again.
//!
//! The answer is served in a task of its own beside the machine, one request
//! at a time, each bounded by [`ANSWER_WITHIN`]: nothing here touches the
//! node loop, and nothing here writes. The task draws no randomness: its one
//! BUGGIFY decision (answer every request from fresh folds, the path a
//! restarted machine takes) is drawn where the task is started.

use std::time::Duration;

use moonpool_core::{Detach, Providers, SimulationResult, TaskProvider, TimeProvider};
use moonpool_rpc::RpcHandle;
use paros_core::NodeId;
use tokio_util::sync::CancellationToken;

use super::{ControlJournals, MachineFacts, cell_book};
use crate::Address;
use crate::client::bootstrap::cell_members;
use crate::client::checkpoint::{Folder, LoadOutcome, load};
use crate::client::names::{TenantResolution, tenant_in};
use crate::client::{Client, ClientTunables, Server};
use crate::fleet::FleetDirectory;
use crate::rpc::machine as wire;
use crate::rpc::methods::ResolveRpc;
use crate::rpc::{Inbound, NodeClient, serve_well_known};
use crate::system::Registry;

/// How long one answer may take: past it the answer is `unavailable`, and
/// the next request starts from fresh folds.
pub(crate) const ANSWER_WITHIN: Duration = Duration::from_secs(2);

/// What a machine answers `Resolve` from.
pub(crate) struct Resolver {
    /// The machine, serving its cell.
    pub(crate) facts: MachineFacts,
    /// Its cell: the registry, and the universe directory when the cell
    /// serves it.
    pub(crate) cell: ControlJournals,
    /// The founding members (the registry's genesis pool), when the machine
    /// knows them: a founding member does, an admitted machine learns them
    /// from the control journal's membership.
    pub(crate) founders: Option<Vec<NodeId>>,
    /// The cell's machines it dials at start.
    pub(crate) book: Vec<(NodeId, Address)>,
    /// The machine serves the cell's journals (a founding member), so its
    /// own reads may go to itself.
    pub(crate) serves: bool,
}

/// Serve `Resolve` on `rpc` until `shutdown`, in a task of its own.
///
/// # Errors
///
/// The endpoint could not be registered.
#[tracing::instrument(level = "debug", skip_all, fields(node = resolver.facts.node_id.0, cell = resolver.cell.cell_id))]
pub(crate) fn spawn<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    resolver: Resolver,
    shutdown: CancellationToken,
) -> SimulationResult<()> {
    assert!(
        resolver.cell.cell.is_set(),
        "a cell names its control journal"
    );
    let mut requests = Inbound::plain(serve_well_known::<P, ResolveRpc>(rpc)?);
    // Fold both journals again from position 0 for every answer: the path a
    // restarted machine takes, on every request of this incarnation.
    let fresh = moonpool_buggify::buggify_with_prob!(0.25);
    if fresh {
        moonpool_assertions::reachable!("resolve: a machine answers from fresh folds");
    }
    let providers = providers.clone();
    let rpc = rpc.clone();
    providers
        .clone()
        .task()
        .spawn_task("paros-machine-resolve", async move {
            let mut folds = Folds::new(&resolver);
            loop {
                moonpool_core::select! {
                    biased;
                    () = shutdown.cancelled() => return,
                    Some((request, reply)) = requests.recv() => {
                        if fresh {
                            folds = Folds::new(&resolver);
                        }
                        let answer = folds.answer(&providers, &rpc, &resolver, &request, &shutdown);
                        let answer = providers.time().timeout(ANSWER_WITHIN, answer).await;
                        let ack = answer.unwrap_or_else(|_| {
                            // A read cut short may leave a fold mid-page:
                            // start the next answer from fresh folds.
                            folds = Folds::new(&resolver);
                            refused(&resolver, "unavailable")
                        });
                        reply.send(ack);
                    }
                    else => return,
                }
            }
        })
        .detach();
    Ok(())
}

/// The folds an answer reads, kept between requests.
struct Folds {
    /// The registry, once its genesis pool is known.
    registry: Option<(Vec<NodeId>, Folder<Registry>)>,
    /// The universe directory.
    directory: Folder<FleetDirectory>,
    /// The cell's machines, as the registry last placed them.
    book: Vec<(NodeId, Address)>,
    /// The server a read asks first, rotated per answer.
    first: usize,
}

impl Folds {
    fn new(resolver: &Resolver) -> Self {
        Self {
            registry: resolver.founders.as_ref().map(|founders| {
                (
                    founders.clone(),
                    Folder::new(Registry::new(founders.iter().copied())),
                )
            }),
            directory: Folder::new(FleetDirectory::default()),
            book: resolver.book.clone(),
            first: 0,
        }
    }

    /// A client over the cell's machines this one knows.
    fn client<P: Providers>(
        &self,
        providers: &P,
        rpc: &RpcHandle<P>,
        resolver: &Resolver,
        shutdown: &CancellationToken,
    ) -> Client<P> {
        let me = resolver.facts.node_id;
        let servers = self
            .book
            .iter()
            .filter(|(id, _)| resolver.serves || *id != me)
            .map(|(id, addr)| Server {
                id: id.0,
                node: NodeClient::named(rpc, resolver.facts.names.clone(), addr.clone()),
            })
            .collect();
        Client::new(providers, servers, ClientTunables::default()).with_shutdown(shutdown.clone())
    }

    /// The answer to `request`, read from the folds brought to their tails.
    async fn answer<P: Providers>(
        &mut self,
        providers: &P,
        rpc: &RpcHandle<P>,
        resolver: &Resolver,
        request: &wire::Resolve,
        shutdown: &CancellationToken,
    ) -> wire::ResolveAck {
        let client = self.client(providers, rpc, resolver, shutdown);
        if client.server_count() == 0 {
            return refused(resolver, "unavailable");
        }
        self.first = (self.first + 1) % client.server_count();
        if !self.fold_registry(&client, resolver).await {
            return refused(resolver, "unavailable");
        }
        let Some(universe) = resolver.cell.fleet else {
            return refused(resolver, "no_universe");
        };
        let LoadOutcome::Loaded { .. } =
            load(&mut self.directory, universe, &client, self.first, 0).await
        else {
            self.directory = Folder::new(FleetDirectory::default());
            return refused(resolver, "unavailable");
        };
        let directory = self.directory.state();
        let Some(universe_id) = directory.fleet() else {
            return refused(resolver, "no_universe");
        };
        let at = directory.next_seq();
        let mut ack = wire::ResolveAck {
            universe_id,
            at,
            ..refused(resolver, "")
        };
        match tenant_in(directory, &request.tenant, at) {
            TenantResolution::Resolved {
                tenant, control, ..
            } => {
                let entry = directory
                    .tenant(tenant)
                    .expect("a resolved tenant has an entry");
                ack.tenant = tenant.0;
                ack.tenant_cell = entry.cell_id;
                if entry.cell_id == resolver.cell.cell_id {
                    moonpool_assertions::reachable!("resolve: a machine resolves a tenant");
                    ack.control = Some(wire::JournalIdentifier {
                        tenant: control.tenant.0,
                        journal: control.journal.0,
                    });
                    ack.machines = self
                        .book
                        .iter()
                        .map(|(id, addr)| wire::Member {
                            node_id: id.0,
                            addr: addr.to_string(),
                        })
                        .collect();
                } else {
                    ack.refusal = "other_cell".into();
                }
            }
            TenantResolution::NotReady { tenant, .. } => {
                ack.tenant = tenant.0;
                ack.refusal = "not_ready".into();
            }
            TenantResolution::Internal => ack.refusal = "internal".into(),
            TenantResolution::Unknown => {
                moonpool_assertions::reachable!("resolve: a machine knows no tenant by the name");
                ack.refusal = "unknown_tenant".into();
            }
            TenantResolution::Unreadable(_) => unreachable!("the directory was folded"),
        }
        ack
    }

    /// Bring the registry to its tail and the book with it; `false` when it
    /// could not be read.
    async fn fold_registry<P: Providers>(
        &mut self,
        client: &Client<P>,
        resolver: &Resolver,
    ) -> bool {
        if self.registry.is_none() {
            let Some(members) = cell_members(client, resolver.cell.cell).await else {
                return false;
            };
            let founders: Vec<NodeId> = members.into_iter().map(NodeId).collect();
            let genesis = Registry::new(founders.iter().copied());
            self.registry = Some((founders, Folder::new(genesis)));
        }
        let Some((founders, folder)) = self.registry.as_mut() else {
            return false;
        };
        if !matches!(
            load(folder, resolver.cell.cell, client, self.first, 0).await,
            LoadOutcome::Loaded { .. }
        ) || !folder.is_whole()
        {
            self.registry = None;
            return false;
        }
        // The founding members at the addresses this machine knows: the
        // registry moves the ones that registered elsewhere.
        let known: Vec<(NodeId, Address)> = founders
            .iter()
            .filter_map(|id| {
                self.book
                    .iter()
                    .find(|(b, _)| b == id)
                    .map(|(_, addr)| (*id, addr.clone()))
            })
            .collect();
        let machines = cell_book(&known, folder.state());
        if !machines.is_empty() {
            self.book = machines;
        }
        true
    }
}

/// An answer with only the machine's own facts and `refusal`.
fn refused(resolver: &Resolver, refusal: &str) -> wire::ResolveAck {
    wire::ResolveAck {
        refusal: refusal.into(),
        cell_id: resolver.cell.cell_id,
        ..wire::ResolveAck::default()
    }
}
