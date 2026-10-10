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

use std::collections::BTreeMap;

use moonpool_core::Providers;
use moonpool_rpc::RpcHandle;
use paros_core::{JournalIdentifier, TenantId};

use super::{Shared, lock};
use crate::client::Client;
use crate::client::bootstrap::majority_cell;
use crate::client::fleet::read_directory;
use crate::client::names::{
    JournalNames, JournalResolution, TenantResolution, Unreadable, resolve_journal, tenant_in,
};
use crate::fleet::FleetDirectory;
use crate::name::JournalName;

/// What resolving a name came to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Resolved {
    /// The name's journal, read from the control journal `control` at `at`.
    Journal {
        journal: JournalIdentifier,
        control: JournalIdentifier,
        at: u64,
    },
    /// No `READY` `users` tenant, or no live journal, holds the name.
    Unknown,
    /// A fold could not be read now, and nothing cached answers.
    Unavailable,
}

/// The cell the frontend fronts, as its founding members name it.
struct Cell<P: Providers> {
    /// The universe tenant's control journal: the universe directory.
    universe: JournalIdentifier,
    /// A client over the founding members that answered.
    client: Client<P>,
}

/// The frontend's caches. Never held across an await.
pub(super) struct Routes<P: Providers> {
    rpc: RpcHandle<P>,
    cell: Option<Cell<P>>,
    /// The universe directory as last read.
    directory: Option<FleetDirectory>,
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
            tenants: BTreeMap::new(),
            journals: BTreeMap::new(),
            leaders: BTreeMap::new(),
        }
    }

    /// Forget every resolution (the cell and the leader hints stay).
    pub(super) fn forget(&mut self) {
        self.directory = None;
        self.tenants.clear();
        self.journals.clear();
        assert!(self.tenants.is_empty() && self.journals.is_empty());
    }

    /// A machine refused `journal`, resolved from `name`, as unknown: drop
    /// the journal's resolution, and the tenant's (the tenant may be gone).
    pub(super) fn stale(&mut self, name: &JournalName, journal: JournalIdentifier) {
        if let Some((tenant, control)) = self.tenants.get(name.tenant()).copied() {
            assert_eq!(tenant, journal.tenant, "a name resolves inside its tenant");
            if let Some(names) = self.journals.get_mut(&control) {
                names.stale(name.journal().as_bytes(), journal);
            }
            self.tenants.remove(name.tenant());
        }
        self.directory = None;
        assert!(!self.tenants.contains_key(name.tenant()));
    }
}

/// The cell: cached, or learned from a majority of its founding members
/// (`Inspect`, as an operator learns it, §3.8).
async fn cell<P, A>(shared: &Shared<P, A>) -> Option<(JournalIdentifier, Client<P>)>
where
    P: Providers,
{
    let rpc = {
        let routes = lock(&shared.routes);
        if let Some(cell) = &routes.cell {
            return Some((cell.universe, cell.client.clone()));
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
    let Some(universe) = journals.fleet else {
        moonpool_assertions::reachable!("frontend: the cell hosts no universe tenant yet");
        return None;
    };
    assert!(!servers.is_empty(), "a majority holds at least one server");
    let count = servers.len();
    let client = Client::connect_named(
        &shared.providers,
        &rpc,
        &settings.names,
        &servers,
        settings.client,
    )
    .rotating_over(count);
    tracing::info!(
        cell = journals.cell_id,
        servers = count,
        "frontend_learned_cell"
    );
    let mut routes = lock(&shared.routes);
    let cell = routes.cell.get_or_insert(Cell { universe, client });
    Some((cell.universe, cell.client.clone()))
}

/// `journal`'s own client: its own leader hint over the cell's servers.
pub(super) async fn client_for<P, A>(
    shared: &Shared<P, A>,
    journal: JournalIdentifier,
) -> Option<Client<P>>
where
    P: Providers,
{
    let (_, client) = cell(shared).await?;
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
{
    let Some((universe, client)) = cell(shared).await else {
        return Resolved::Unavailable;
    };
    let (tenant, control) = match tenant(shared, &client, universe, name).await {
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
        JournalResolution::Unknown { .. } => Resolved::Unknown,
        JournalResolution::Unreadable(Unreadable::UnknownJournal) => {
            // No machine serves the tenant's control journal: the tenant
            // is gone since the directory was read.
            routes.tenants.remove(name.tenant());
            routes.directory = None;
            Resolved::Unknown
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
    universe: JournalIdentifier,
    name: &JournalName,
) -> Result<(TenantId, JournalIdentifier), Resolved>
where
    P: Providers,
{
    if let Some(held) = lock(&shared.routes).tenants.get(name.tenant()).copied() {
        return Ok(held);
    }
    let read = read_directory(client, 0, universe).await;
    let mut routes = lock(&shared.routes);
    let directory = match read {
        Ok(directory) => routes.directory.insert(directory),
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
            routes
                .tenants
                .insert(name.tenant().to_string(), (tenant, control));
            Ok((tenant, control))
        }
        TenantResolution::Unknown
        | TenantResolution::NotReady { .. }
        | TenantResolution::Internal => Err(Resolved::Unknown),
        TenantResolution::Unreadable(_) => Err(Resolved::Unavailable),
    }
}
