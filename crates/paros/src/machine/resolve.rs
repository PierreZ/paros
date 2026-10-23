//! **`Resolve`** (#216, `docs/architecture/03-5-frontend-and-resolver.md` §3.5): "which references
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
//!    ([`super::cell_book`]), each with the machine interface the registry
//!    holds for it (#404 (static vs dynamic endpoints)). The client sends
//!    its data plane through those references until the frontend exists;
//!    then the answer names the tenant's frontends.
//!
//! The machine reads both journals as any client does, through the cell's
//! machines it knows, and keeps both folds between requests, so an answer
//! reads only what the journals gained since the last one. A fold is a
//! hint, never the authority: a stale answer costs the client one refused
//! call, and the client resolves again.
//!
//! The answer is served in a task of its own beside the machine, one request
//! at a time, each bounded by `DriverTunables::resolve_answer_within`:
//! nothing here touches the node loop, and nothing here writes. An answer
//! cut short keeps what its folds read (#457 (hard-coded tunables)): a fold
//! is a consistent prefix at every await, so the next request goes on from
//! there. Before, a cut answer reset both folds to position 0, so once a
//! fold took longer than the bound no answer ever came back again, a
//! metastable loop the simulation's drawn bound and the final resolve check
//! (`paros_sim`'s `resolve_answers`) now exercise. The task draws no randomness: its one
//! BUGGIFY decision (answer every request from fresh folds, the path a
//! restarted machine takes) is drawn where the task is started.

use std::time::Duration;

use crate::ParosProviders;
use moonpool_core::{Detach, LocalTaskProvider, LocalTimeProvider, SimulationResult};
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
    /// The tunables of its clients (`DriverTunables::internal_client`).
    pub(crate) client: ClientTunables,
    /// How long one answer may take: past it the answer is `unavailable`
    /// (`DriverTunables::resolve_answer_within`).
    pub(crate) answer_within: Duration,
}

/// Serve `Resolve` on `rpc` until `shutdown`, in a task of its own.
///
/// # Errors
///
/// The endpoint could not be registered.
#[tracing::instrument(level = "debug", skip_all, fields(node = resolver.facts.node_id.0, cell = resolver.cell.cell_id))]
pub(crate) fn spawn<P: ParosProviders>(
    providers: &P,
    rpc: &RpcHandle<P>,
    resolver: Resolver,
    shutdown: CancellationToken,
) -> SimulationResult<()> {
    assert!(
        resolver.cell.cell.is_set(),
        "a cell names its control journal"
    );
    assert!(
        !resolver.answer_within.is_zero(),
        "an answer has time to read"
    );
    let mut requests = Inbound::plain(serve_well_known::<P, ResolveRpc>(rpc)?);
    // Fold both journals again from position 0 for every answer that follows
    // a completed one: the path a restarted machine takes, on every request of
    // this incarnation. An answer cut short still keeps its folds, so a fold
    // that costs more than `answer_within` finishes over several answers.
    let fresh = moonpool_buggify::buggify_with_prob!(0.25);
    if fresh {
        moonpool_assertions::reachable!("resolve: a machine answers from fresh folds");
    }
    // The incarnation's first answer is cut short at once, on a BUGGIFY
    // decision: it answers `unavailable`, keeps what it read, and the next
    // answer goes on from there (#457). A short drawn bound alone left the
    // cut-short gate unreached in sweeps (PR #552, #446). Drawn here, on
    // the machine's start path, and carried to the task.
    let mut hasty = moonpool_buggify::buggify_with_prob!(0.2);
    let providers = providers.clone();
    let rpc = rpc.clone();
    providers
        .clone()
        .local_task()
        .spawn_local_task("paros-machine-resolve", async move {
            let mut folds = Folds::new(&resolver);
            let mut completed = false;
            loop {
                moonpool_core::select! {
                    biased;
                    () = shutdown.cancelled() => return,
                    Some((request, reply)) = requests.recv() => {
                        if fresh && completed {
                            folds = Folds::new(&resolver);
                        }
                        let answer = folds.answer(&providers, &rpc, &resolver, &request, &shutdown);
                        let within = if std::mem::take(&mut hasty) {
                            Duration::from_millis(1)
                        } else {
                            resolver.answer_within
                        };
                        let answer = providers.time().timeout(within, answer).await;
                        completed = answer.is_ok();
                        // A fold is a consistent prefix at every await
                        // (each page folds whole before the next read):
                        // an answer cut short keeps what it read, and the
                        // next one goes on from there (#457).
                        let ack = answer.unwrap_or_else(|_| {
                            moonpool_assertions::reachable!(
                                "resolve: an answer is cut short and its folds are kept"
                            );
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
    fn client<P: ParosProviders>(
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
        Client::new(providers, servers, resolver.client).with_shutdown(shutdown.clone())
    }

    /// The answer to `request`, read from the folds brought to their tails.
    async fn answer<P: ParosProviders>(
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
        // The cell's own universe directory, else the one its join points
        // at (#232 (multiple cells)), read through that cell's machines.
        let registry = self.registry.as_ref().map(|(_, folder)| folder.state());
        let pointer = registry.and_then(Registry::universe).cloned();
        let (universe, reader, first) = match (resolver.cell.fleet, pointer) {
            (_, Some(pointer)) => (pointer.directory, client.elsewhere(&pointer.hosts), 0),
            (Some(fleet), None) => (fleet, client.clone(), self.first),
            (None, None) => return refused(resolver, "no_universe"),
        };
        let LoadOutcome::Loaded { .. } =
            load(&mut self.directory, universe, &reader, first, 0).await
        else {
            // Kept (#457): a fold that stopped is a prefix the next answer
            // goes on from; only a fold with a hole starts again.
            if !self.directory.is_whole() {
                self.directory = Folder::new(FleetDirectory::default());
            }
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
            universe_name: directory.universe_name().to_vec(),
            cell_name: directory.cell_name(resolver.cell.cell_id).to_vec(),
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
                ack.tenant_cell_name = directory.cell_name(entry.cell_id).to_vec();
                if entry.cell_id == resolver.cell.cell_id {
                    moonpool_assertions::reachable!("resolve: a machine resolves a tenant");
                    ack.control = Some(wire::JournalIdentifier {
                        tenant: control.tenant.0,
                        journal: control.journal.0,
                    });
                    // Each machine's interface as the registry holds it
                    // (#404 (static vs dynamic endpoints)): the client
                    // sends its data plane through it.
                    let registry = self.registry.as_ref().map(|(_, folder)| folder.state());
                    ack.machines = self
                        .book
                        .iter()
                        .map(|(id, addr)| member(registry, *id, addr))
                        .collect();
                    assert_eq!(
                        ack.machines.len(),
                        self.book.len(),
                        "every machine of the book is answered"
                    );
                    // The tenant's batchers (#145, #604): each machine the
                    // registry books a `batcher` slot of the tenant on, at
                    // the address the book holds for it.
                    ack.batchers = registry
                        .map(|registry| batchers_of(registry, tenant, &self.book))
                        .unwrap_or_default();
                } else {
                    moonpool_assertions::reachable!(
                        "resolve: a machine points at the tenant's other cell"
                    );
                    ack.refusal = "other_cell".into();
                    ack.tenant_cell_endpoint = directory
                        .endpoint(entry.cell_id)
                        .iter()
                        .map(|(id, addr)| wire::Member {
                            node_id: *id,
                            addr: addr.to_string(),
                            machine_interface: Vec::new(),
                        })
                        .collect();
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
    async fn fold_registry<P: ParosProviders>(
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
            // Kept (#457), as the directory: only a fold with a hole starts
            // again.
            if !folder.is_whole() {
                self.registry = None;
            }
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
            // Itself at the address it advertises now, which the registry
            // may not hold yet (#349): a founding member that moved and
            // serves its cell alone otherwise reads only from its old
            // address, so its fold never learns the move and it never
            // answers again (#457: found by the final resolve check, seed
            // 13674945314749361170, a one-member cell after the
            // moved-founder scenario).
            self.book = super::with_own(&machines, resolver.facts.node_id, &resolver.facts.addr);
        }
        true
    }
}

/// Machine `id` at `addr`, with the machine interface `registry` holds
/// for it (#404 (static vs dynamic endpoints)).
fn member(registry: Option<&Registry>, id: NodeId, addr: &Address) -> wire::Member {
    wire::Member {
        node_id: id.0,
        addr: addr.to_string(),
        machine_interface: registry
            .and_then(|registry| registry.get(id))
            .map(|node| node.machine_interface.clone())
            .unwrap_or_default(),
    }
}

/// The machines `registry` books a `batcher` slot of `tenant` on, each
/// once, at the address `book` holds for it (a machine the book does not
/// place is left out), with its machine interface.
fn batchers_of(
    registry: &Registry,
    tenant: paros_core::TenantId,
    book: &[(NodeId, Address)],
) -> Vec<wire::Member> {
    let nodes: std::collections::BTreeSet<NodeId> = registry
        .bookings()
        .filter(|(_, b)| {
            b.role == crate::system::Role::Batcher
                && b.target == crate::system::BookingTarget::Tenant { tenant }
        })
        .map(|(_, b)| b.node)
        .collect();
    let members: Vec<wire::Member> = nodes
        .iter()
        .filter_map(|id| {
            let (_, addr) = book.iter().find(|(b, _)| b == id)?;
            Some(member(Some(registry), *id, addr))
        })
        .collect();
    assert!(members.len() <= nodes.len(), "a batcher is named once");
    if !members.is_empty() {
        moonpool_assertions::reachable!("resolve: a machine names a tenant's batchers");
    }
    members
}

/// An answer with only the machine's own facts and `refusal`.
fn refused(resolver: &Resolver, refusal: &str) -> wire::ResolveAck {
    wire::ResolveAck {
        refusal: refusal.into(),
        cell_id: resolver.cell.cell_id,
        ..wire::ResolveAck::default()
    }
}
