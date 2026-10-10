//! **The cell answers its views** (#399, `docs/architecture.md` §3.6):
//! every founding member serves `View` in a task beside its node loop, as
//! any other client of its cell would read it.
//!
//! A request names one query and the caller's scope claim. The member
//! decides the scope ([`crate::view::authorize`]), reads the cell's journals
//! through a [`Client`] over the founding members — the registry, the
//! universe directory when the cell hosts it, the election journal and the
//! hosted tenants' control journals — and builds the answer with
//! [`crate::view`], filtered by the scope before it leaves. The registry and
//! the directory folds are kept between requests and caught up at each one;
//! the answer names the positions it was folded at.
//!
//! An admitted machine serves no journal and no view: it does not register
//! the endpoint, so a call to it is refused before any handler runs, and the
//! caller asks the next server.
//!
//! The task draws no randomness of its own: its one BUGGIFY decision (a
//! member that answers from a fold it did not catch up, as a lagging member
//! would) is drawn on the node loop before the task starts.

use moonpool_core::{Detach, Providers, SimulationResult, TaskProvider};
use moonpool_rpc::RpcHandle;
use paros_core::{NodeId, TenantId};
use tokio_util::sync::CancellationToken;

use super::{CellPlan, FormedCell};
use crate::Address;
use crate::client::Client;
use crate::client::checkpoint::{Folder, LoadOutcome, load};
use crate::client::election::read_election;
use crate::client::names::read_tenant_control;
use crate::fleet::FleetDirectory;
use crate::rpc::methods::ViewRpc;
use crate::rpc::view as wire;
use crate::rpc::{Inbound, serve_well_known};
use crate::system::Registry;
use crate::tenant::TenantControl;
use crate::view::{CellFacts, Scope, authorize, cell_view, refusal, tenant_view, universe_view};

/// The views' endpoint.
type Requests = Inbound<ViewRpc, wire::ViewRequest, wire::ViewReply>;

/// What one founding member keeps between views: its folds of the
/// registry and the universe directory.
struct Desk<P: Providers> {
    client: Client<P>,
    me: NodeId,
    plan: CellPlan,
    founders: Vec<(NodeId, Address)>,
    registry: Folder<Registry>,
    directory: Folder<FleetDirectory>,
    /// Answer from the folds as they stand, without catching up (a BUGGIFY
    /// decision of the node loop): a lagging member.
    lag: bool,
}

/// Start serving the views of the founding member `formed`, in a task that
/// stops with `shutdown`. Nothing starts when the plan names no founding
/// member.
///
/// # Errors
///
/// The `View` endpoint could not be registered.
pub(crate) fn spawn<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    formed: &FormedCell,
    shutdown: CancellationToken,
) -> SimulationResult<()> {
    if formed.plan.members.is_empty() {
        return Ok(());
    }
    let requests = Inbound::plain(serve_well_known::<P, ViewRpc>(rpc)?);
    let founders = formed.plan.members.clone();
    let desk = Desk {
        client: super::coordinator::cell_client(providers, rpc, formed)
            .with_shutdown(shutdown.clone()),
        me: formed.facts.node_id,
        plan: formed.plan.clone(),
        registry: Folder::new(Registry::new(founders.iter().map(|(id, _)| *id))),
        directory: Folder::new(FleetDirectory::default()),
        founders,
        lag: moonpool_buggify::buggify_with_prob!(0.2),
    };
    providers
        .task()
        .spawn_task("paros-cell-views", serve(desk, requests, shutdown))
        .detach();
    Ok(())
}

/// Answer views until `shutdown`.
async fn serve<P: Providers>(
    mut desk: Desk<P>,
    mut requests: Requests,
    shutdown: CancellationToken,
) {
    loop {
        moonpool_core::select! {
            biased;
            () = shutdown.cancelled() => return,
            next = requests.recv() => {
                let Some((request, reply)) = next else { return };
                let answer = desk.answer(request).await;
                reply.send(answer);
            }
        }
    }
}

impl<P: Providers> Desk<P> {
    /// The answer to `request`.
    #[tracing::instrument(level = "debug", skip_all, fields(node = self.me.0, cell = self.plan.cell_id))]
    async fn answer(&mut self, request: wire::ViewRequest) -> wire::ViewReply {
        let Ok(scope) = authorize(request.scope.as_ref()) else {
            return refusal("malformed");
        };
        let Some(query) = request.query else {
            return refusal("malformed");
        };
        if !self.catch_up().await {
            return refusal("unavailable");
        }
        let coordinator = read_election(&self.client, self.plan.election, 0)
            .await
            .and_then(|fold| fold.leader().map(|l| (l.candidate.id, l.term)));
        let directory = self
            .plan
            .fleet
            .is_some()
            .then(|| self.directory.state().clone())
            .filter(|d| d.fleet().is_some());
        let tenants = match &query {
            wire::view_request::Query::Cell(_) => self.read_tenants(None).await,
            wire::view_request::Query::Tenant(t) => self.read_tenants(Some(&t.name)).await,
            wire::view_request::Query::Universe(_) => Vec::new(),
        };
        let facts = CellFacts {
            cell_id: self.plan.cell_id,
            answered_by: self.me,
            founders: &self.founders,
            control: self.plan.control,
            election: self.plan.election,
            universe: self.plan.fleet,
            registry: self.registry.state(),
            directory: directory.as_ref(),
            tenants: &tenants,
            coordinator,
        };
        match query {
            wire::view_request::Query::Cell(_) => cell_view(&scope, &facts),
            wire::view_request::Query::Tenant(t) => tenant_view(&scope, &facts, &t.name),
            wire::view_request::Query::Universe(_) => match directory.as_ref() {
                Some(directory) => universe_view(&scope, &facts, directory),
                None if scope != Scope::Admin => refusal("forbidden"),
                None => refusal("not_universe"),
            },
        }
    }

    /// Fold the registry and, when this cell hosts it, the universe
    /// directory to their tails; a lagging member answers from where its
    /// folds stand once it folded anything. Whether the folds hold an
    /// answer.
    async fn catch_up(&mut self) -> bool {
        if self.lag && self.registry.next_seq() > 0 {
            moonpool_assertions::reachable!("view: a lagging member answers from its fold");
            self.lag = false;
            return true;
        }
        let registry = load(&mut self.registry, self.plan.control, &self.client, 0, 0).await;
        if !matches!(registry, LoadOutcome::Loaded { .. }) {
            // A fold that jumped a gap and did not heal starts again.
            self.registry = Folder::new(Registry::new(self.founders.iter().map(|(id, _)| *id)));
            return false;
        }
        if let Some(universe) = self.plan.fleet
            && !matches!(
                load(&mut self.directory, universe, &self.client, 0, 0).await,
                LoadOutcome::Loaded { .. }
            )
        {
            self.directory = Folder::new(FleetDirectory::default());
        }
        true
    }

    /// The control journals of the hosted tenants (only the one named
    /// `name`, when given) that could be read.
    async fn read_tenants(&self, name: Option<&[u8]>) -> Vec<TenantControl> {
        let registry = self.registry.state();
        let wanted: Vec<(TenantId, crate::JournalId)> = registry
            .hosted()
            .filter_map(|t| registry.hosted_tenant(t).map(|h| (t, h)))
            .filter(|(_, h)| name.is_none_or(|n| h.name == n))
            .map(|(t, h)| (t, h.control))
            .collect();
        let mut read = Vec::new();
        for (tenant, control) in wanted {
            let journal = crate::JournalIdentifier::new(tenant, control);
            if let Ok(fold) = read_tenant_control(&self.client, 0, journal).await {
                read.push(fold);
            }
        }
        read
    }
}
