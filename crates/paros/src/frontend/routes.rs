//! The frontend's cached folds (#192 (the frontend), §3.5): the cell it
//! fronts, the universe directory, each tenant's names, and a leader hint
//! per journal.
//!
//! Every fold is the library's own (`paros::client::names`): the tenant
//! hop through the universe directory, the journal hop through the
//! tenant's control journal. A resolution is reused until a machine
//! refuses its journal as unknown ([`Routes::stale`]). **Static
//! stability** (§3.3): while the universe directory cannot be read, the
//! tenant hop answers from the directory last read.
//!
//! **Root keys** (#245 (Biscuit tokens)): every read of the universe
//! directory also takes its root public keys, which a token is checked
//! against ([`keys`]). They are kept apart from the resolutions: forgetting
//! a resolution never forgets the keys, and while the directory cannot be
//! read the keys last read still rule.

use std::collections::BTreeMap;
use std::sync::Arc;

use moonpool_core::Providers;
use moonpool_rpc::RpcHandle;
use paros_core::{JournalIdentifier, TenantId};

use super::{Shared, lock};
use crate::audit::Audit;
use crate::client::bootstrap::majority_cell;
use crate::client::checkpoint::{Folder, LoadOutcome, load};
use crate::client::fleet::read_directory;
use crate::client::names::{
    JournalNames, JournalResolution, TenantResolution, Unreadable, resolve_journal, tenant_in,
};
use crate::client::{Client, Server};
use crate::fleet::{FleetDirectory, RootKeyEntry};
use crate::name::JournalName;
use crate::rpc::NodeClient;
use crate::system::Registry;

/// What resolving a name came to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Resolved {
    /// The name's journal, read from the control journal `control` at `at`.
    Journal {
        journal: JournalIdentifier,
        control: JournalIdentifier,
        at: u64,
    },
    /// No `READY` `users` tenant, or no live journal, holds the name;
    /// `read` is the control journal and the position the journal hop read
    /// it at, when that hop found no live journal there (#451).
    Unknown {
        read: Option<(JournalIdentifier, u64)>,
    },
    /// A fold could not be read now, and nothing cached answers.
    Unavailable,
}

/// The cell the frontend fronts, as its founding members name it.
#[derive(Clone)]
pub(super) struct Cell<P: Providers> {
    /// The cell's id.
    id: u64,
    /// The universe tenant's control journal: the universe directory.
    universe: JournalIdentifier,
    /// The cell's election journal: where the tenant coordinator publishes
    /// itself (#575 (key:value tags): a tag request goes to it).
    election: Option<JournalIdentifier>,
    /// A client over the founding members that answered.
    client: Client<P>,
    /// A client that reads the universe directory: `client`, or one over
    /// the machines of the cell that hosts it, which the cell's join
    /// points at (#232 (multiple cells)).
    reader: Client<P>,
}

/// The frontend's caches. Never held across an await.
pub(super) struct Routes<P: Providers> {
    rpc: RpcHandle<P>,
    cell: Option<Cell<P>>,
    /// The universe directory as last read.
    directory: Option<FleetDirectory>,
    /// The universe entry's root public keys as last read (#245).
    keys: Option<Arc<[RootKeyEntry]>>,
    /// Each tenant name's `TenantId` and control journal.
    tenants: BTreeMap<String, (TenantId, JournalIdentifier)>,
    /// Each tenant's journal names, keyed by its control journal.
    journals: BTreeMap<JournalIdentifier, JournalNames>,
    /// Each journal's own client: one leader hint per journal.
    leaders: BTreeMap<JournalIdentifier, Client<P>>,
}

impl<P: Providers> Routes<P> {
    /// Nothing cached, calls through `rpc`.
    pub(super) fn new(rpc: RpcHandle<P>) -> Self {
        Self {
            rpc,
            cell: None,
            directory: None,
            keys: None,
            tenants: BTreeMap::new(),
            journals: BTreeMap::new(),
            leaders: BTreeMap::new(),
        }
    }

    /// Keep `directory` as the one last read, and its root keys.
    fn learn(&mut self, directory: FleetDirectory) -> &FleetDirectory {
        let keys: Arc<[RootKeyEntry]> = directory.root_keys().cloned().collect();
        if self.keys.as_deref() != Some(&*keys) {
            tracing::info!(keys = keys.len(), "frontend_learned_root_keys");
        }
        self.keys = Some(keys);
        self.directory.insert(directory)
    }

    /// Forget every resolution (the cell and the leader hints stay).
    pub(super) fn forget(&mut self) {
        self.directory = None;
        self.tenants.clear();
        self.journals.clear();
        assert!(self.tenants.is_empty() && self.journals.is_empty());
    }

    /// A server refused a call through a machine interface it no longer
    /// serves (#404 (static vs dynamic endpoints)): it restarted since the
    /// frontend learned the cell. Forget the cell and its clients, so the
    /// next call learns the cell, and every server's reference, again.
    pub(super) fn stale_cell(&mut self) {
        if self.cell.take().is_some() {
            moonpool_assertions::reachable!(
                "frontend: a stale machine interface sends the frontend back to its cell"
            );
        }
        self.leaders.clear();
        assert!(self.cell.is_none() && self.leaders.is_empty());
    }

    /// A machine refused `journal`, resolved from `name`, as unknown: drop
    /// the journal's resolution, and the tenant's (the tenant may be gone).
    /// Each call is its own task, so another call may have resolved the
    /// tenant's name again since this one resolved it: when the name now
    /// names another tenant (the old one removed, a new one created under
    /// its name), that newer resolution stays (recipe seed
    /// 200353386032078490: the frontend asserted the two tenants equal).
    pub(super) fn stale(&mut self, name: &JournalName, journal: JournalIdentifier) {
        match self.tenants.get(name.tenant()).copied() {
            Some((tenant, control)) if tenant == journal.tenant => {
                if let Some(names) = self.journals.get_mut(&control) {
                    names.stale(name.journal().as_bytes(), journal);
                }
                self.tenants.remove(name.tenant());
                assert!(!self.tenants.contains_key(name.tenant()));
            }
            Some(_) | None => {}
        }
        self.directory = None;
    }
}

/// The cell: cached, or learned from a majority of its founding members
/// (`Inspect`, as an operator learns it, §3.8).
async fn cell<P, A>(shared: &Shared<P, A>) -> Option<Cell<P>>
where
    P: Providers,
    A: Audit,
{
    let rpc = {
        let routes = lock(&shared.routes);
        if let Some(cell) = &routes.cell {
            return Some(cell.clone());
        }
        routes.rpc.clone()
    };
    let settings = &shared.settings;
    let (journals, servers) = majority_cell(
        &shared.providers,
        &rpc,
        &settings.names,
        &settings.cell,
        settings.client.request_timeout,
    )
    .await?;
    assert!(!servers.is_empty(), "a majority holds at least one server");
    let count = servers.len();
    // Each server through the machine interface its incarnation published
    // (#404 (static vs dynamic endpoints)): a server that restarted since
    // refuses the call unrun, and the frontend learns the cell again.
    let servers: Vec<Server<P>> = servers
        .iter()
        .map(|server| Server {
            id: server.id,
            node: NodeClient::named(&rpc, settings.names.clone(), server.address.clone())
                .with_interface(&server.interface),
        })
        .collect();
    assert_eq!(servers.len(), count, "every server that answered is called");
    let mut client = Client::new(&shared.providers, servers, settings.client).rotating_over(count);
    // A harness observes the calls the frontend forwards, as it observes
    // its own clients' (`Audit::call_observer`); production observes none.
    if let Some(observer) = shared.audit.call_observer() {
        client = client.with_observer(observer);
    }
    // The cell's own universe directory, else the one its join points at
    // (#232 (multiple cells)): read from the cell control journal.
    let (universe, reader) = if let Some(universe) = journals.fleet {
        (universe, client.clone())
    } else {
        let genesis = servers_ids(&client);
        let mut registry = Folder::new(Registry::new(genesis));
        let loaded = load(&mut registry, journals.cell, &client, 0, 0).await;
        let pointer = registry.state().universe().cloned();
        // An idle cell that joined no universe yet fronts nothing.
        let (LoadOutcome::Loaded { .. }, Some(pointer)) = (loaded, pointer) else {
            moonpool_assertions::reachable!("frontend: the cell hosts no universe tenant yet");
            return None;
        };
        (pointer.directory, client.elsewhere(&pointer.hosts))
    };
    tracing::info!(
        cell = journals.cell_id,
        servers = count,
        "frontend_learned_cell"
    );
    let mut routes = lock(&shared.routes);
    let cell = routes.cell.get_or_insert(Cell {
        id: journals.cell_id,
        universe,
        election: journals.election,
        client,
        reader,
    });
    Some(cell.clone())
}

/// The ids of `client`'s servers: the genesis pool a fold of the cell
/// control journal starts from.
fn servers_ids<P: Providers>(client: &Client<P>) -> Vec<paros_core::NodeId> {
    (0..client.server_count())
        .map(|i| paros_core::NodeId(client.id_of(i)))
        .collect()
}

/// The cell's election journal, a client over its servers and the RPC
/// handle: what a request to the tenant coordinator needs (#575).
pub(super) async fn coordinator_route<P, A>(
    shared: &Shared<P, A>,
) -> Option<(JournalIdentifier, Client<P>, RpcHandle<P>)>
where
    P: Providers,
    A: Audit,
{
    let cell = cell(shared).await?;
    let election = cell.election?;
    let routes = lock(&shared.routes);
    Some((election, cell.client, routes.rpc.clone()))
}

/// `journal`'s own client: its own leader hint over the cell's servers.
pub(super) async fn client_for<P, A>(
    shared: &Shared<P, A>,
    journal: JournalIdentifier,
) -> Option<Client<P>>
where
    P: Providers,
    A: Audit,
{
    let client = cell(shared).await?.client;
    let mut routes = lock(&shared.routes);
    let client = routes
        .leaders
        .entry(journal)
        .or_insert_with(|| client.with_own_leader_hint())
        .clone();
    assert!(client.server_count() > 0);
    Some(client)
}

/// `name`'s journal: each hop from the cache, else read afresh.
pub(super) async fn resolve<P, A>(shared: &Shared<P, A>, name: &JournalName) -> Resolved
where
    P: Providers,
    A: Audit,
{
    let Some(Cell {
        id: cell_id,
        universe,
        client,
        reader,
        ..
    }) = cell(shared).await
    else {
        return Resolved::Unavailable;
    };
    let (tenant, control) = match tenant(shared, &reader, (universe, cell_id), name).await {
        Ok(held) => held,
        Err(resolved) => return resolved,
    };
    let cached = lock(&shared.routes)
        .journals
        .get(&control)
        .and_then(|names| names.cached(name.journal().as_bytes()));
    if let Some((journal, at)) = cached {
        assert_eq!(
            journal.tenant, tenant,
            "a cached name stays inside its tenant"
        );
        return Resolved::Journal {
            journal,
            control,
            at,
        };
    }
    let resolution = resolve_journal(&client, 0, control, name.journal().as_bytes()).await;
    let mut routes = lock(&shared.routes);
    routes
        .journals
        .entry(control)
        .or_insert_with(|| JournalNames::new(control))
        .absorb(name.journal().as_bytes(), resolution);
    match resolution {
        JournalResolution::Resolved { journal, at } => {
            assert_eq!(journal.tenant, tenant, "a name resolves inside its tenant");
            Resolved::Journal {
                journal,
                control,
                at,
            }
        }
        JournalResolution::Unknown { at } => Resolved::Unknown {
            read: Some((control, at)),
        },
        JournalResolution::Unreadable(Unreadable::UnknownJournal) => {
            // No machine serves the tenant's control journal: the tenant
            // is gone since the directory was read.
            routes.tenants.remove(name.tenant());
            routes.directory = None;
            Resolved::Unknown { read: None }
        }
        JournalResolution::Unreadable(_) => Resolved::Unavailable,
    }
}

/// The tenant hop: the tenant's id and control journal, from the cache,
/// else from the universe directory read afresh, else (static stability)
/// from the directory last read.
async fn tenant<P, A>(
    shared: &Shared<P, A>,
    client: &Client<P>,
    (universe, cell_id): (JournalIdentifier, u64),
    name: &JournalName,
) -> Result<(TenantId, JournalIdentifier), Resolved>
where
    P: Providers,
    A: Audit,
{
    if let Some(held) = lock(&shared.routes).tenants.get(name.tenant()).copied() {
        return Ok(held);
    }
    let read = read_directory(client, 0, universe).await;
    let mut routes = lock(&shared.routes);
    let directory = match read {
        Ok(directory) => routes.learn(directory),
        Err(_) => match &routes.directory {
            Some(held) => {
                moonpool_assertions::reachable!(
                    "frontend: a name resolves from the cached directory while the universe directory is unreadable"
                );
                held
            }
            None => return Err(Resolved::Unavailable),
        },
    };
    match tenant_in(directory, name.tenant().as_bytes(), directory.next_seq()) {
        TenantResolution::Resolved {
            tenant, control, ..
        } => {
            assert_eq!(
                control.tenant, tenant,
                "a tenant's control journal is its own"
            );
            if directory
                .tenant(tenant)
                .is_some_and(|t| t.cell_id != cell_id)
            {
                // Another cell hosts it (#232 (multiple cells)): this
                // frontend fronts only its own cell.
                moonpool_assertions::reachable!(
                    "frontend: a tenant of another cell is unknown here"
                );
                return Err(Resolved::Unknown { read: None });
            }
            routes
                .tenants
                .insert(name.tenant().to_string(), (tenant, control));
            Ok((tenant, control))
        }
        TenantResolution::Unknown
        | TenantResolution::NotReady { .. }
        | TenantResolution::Internal => Err(Resolved::Unknown { read: None }),
        TenantResolution::Unreadable(_) => Err(Resolved::Unavailable),
    }
}

/// The universe entry's root public keys a token is checked against
/// (#245): the keys last read, else (or with `refresh`) read from the
/// universe directory now. `None` while no read ever succeeded: the
/// [`super::Authz`]'s own pin then rules.
pub(super) async fn keys<P, A>(shared: &Shared<P, A>, refresh: bool) -> Option<Arc<[RootKeyEntry]>>
where
    P: Providers,
    A: Audit,
{
    let held = lock(&shared.routes).keys.clone();
    if held.is_some() && !refresh {
        return held;
    }
    let Some(Cell {
        universe, reader, ..
    }) = cell(shared).await
    else {
        return held;
    };
    let read = read_directory(&reader, 0, universe).await;
    let mut routes = lock(&shared.routes);
    match read {
        Ok(directory) => {
            routes.learn(directory);
        }
        Err(_) => {
            moonpool_assertions::reachable!(
                "frontend: the root keys last read rule while the universe directory is unreadable"
            );
        }
    }
    routes.keys.clone()
}
