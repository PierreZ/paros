//! **The frontend** (#192 (the frontend), `docs/architecture.md` §3.5): the
//! stateless entry role in front of a cell's machines.
//!
//! A client reaches a frontend, never a machine, for its data. The
//! frontend takes each of the four journal calls with the caller's entry
//! (a token and the journal's names, `proto/paros.proto`'s `Frontend`
//! service), and for each one:
//!
//! 1. **authorizes** it through [`Authz`], in names, before any I/O: a
//!    refused call reaches no machine and is answered with its [`Denial`];
//! 2. **resolves** the names to the `(TenantId, JournalId)` the machines
//!    know, through the library's two hops (`paros::client::names`): the
//!    tenant through its cached fold of the universe directory, the journal
//!    through its cached fold of the tenant's control journal. Only a
//!    `READY` `users` tenant and a live journal resolve. An entry with no
//!    tenant name names an internal journal by the call's ids, which only
//!    an `admin` token reaches;
//! 3. **forwards** the call to the machines and their answer back, never a
//!    redirect (decided on 2026-10-04): it follows the leader hints itself
//!    and strips them from the answer, so the client never learns
//!    placement. Past the frontend nothing knows a tenant name.
//!
//! **Static stability** (§3.3): a resolution is cached and reused until a
//! machine refuses its journal as unknown; while the universe directory
//! cannot be read, the frontend resolves from the fold it holds.
//!
//! **Trust** (§3.5): the network is the boundary. A frontend holds no key
//! and no state that survives it; nodes do no authorization.
//!
//! Until placement (#212) the frontend fronts the cell's founding members,
//! which serve every journal; and since `Resolve` (#216) names no frontend
//! until frontends are booked slots, a client is configured with its
//! frontends' addresses. The
//! administration calls that are journal calls on internal journals (the
//! universe tenant's, the cell's) go through a frontend with an `admin`
//! token.
//!
//! The frontend's own choices are inline BUGGIFY sites drawn on its loop,
//! never in a call's task: a cold cache, a stale resolution answered as
//! unknown, a call routed to a server other than the known leader.

mod authz;
mod forward;
mod routes;

pub use authz::{Authz, Denial, Operation, Request, Target};

use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use moonpool_core::{Detach, Providers, RandomProvider, SimulationResult, TaskProvider};
use paros_core::JournalIdentifier;
use tokio_util::sync::CancellationToken;

use crate::audit::Audit;
use crate::client::ClientTunables;
use crate::driver::DriverTunables;
use crate::driver::edge::{Plain, RpcEdge};
use crate::name::JournalName;
use crate::rpc::inbound::{Inbound, ReplySender, serve_well_known};
use crate::rpc::methods::{FrontReadRpc, FrontSetLeaderRpc, FrontTruncateRpc, FrontWriteRpc};
use crate::rpc::{Entry, FrontRead, FrontSetLeader, FrontTruncate, FrontWrite};
use crate::{Address, Names};
use forward::Forwarded;
use routes::{Resolved, Routes};

/// What a frontend is given to run.
#[derive(Clone, Debug)]
pub struct FrontendSettings {
    /// The address it binds and serves the frontend contract on.
    pub listen: SocketAddr,
    /// The cell it fronts: its founding members' advertised addresses.
    pub cell: Vec<Address>,
    /// The name table every address it dials resolves through.
    pub names: Names,
    /// The wall clock at its provider's zero, since the Unix epoch: a
    /// token's expiry is checked against `epoch + time.now()`.
    pub epoch: Duration,
    /// Its RPC edge's shape (queue, connect budget, keep-alive).
    pub tunables: DriverTunables,
    /// How it calls the machines: each attempt's timeout, the redirects it
    /// follows, how long it waits for the cell to answer an `Inspect`.
    pub client: ClientTunables,
}

/// One call's own choices, drawn on the frontend's loop and carried to the
/// call's task (a site never draws in a spawned task).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Draws {
    /// Forget every resolution before this call.
    cold: bool,
    /// Answer a stale resolution as unknown instead of resolving again.
    stale: bool,
    /// Start at this server (modulo the count) instead of the known leader.
    stray: Option<u64>,
}

impl Draws {
    fn draw<P: Providers>(providers: &P) -> Self {
        let cold = moonpool_buggify::buggify_with_prob!(0.05);
        let stale = moonpool_buggify::buggify_with_prob!(0.1);
        let stray = moonpool_buggify::buggify_with_prob!(0.2)
            .then(|| providers.random().random_range(0..u64::from(u32::MAX)));
        Self { cold, stale, stray }
    }
}

/// The frontend's inbound queues: the four calls of its contract.
struct FrontendInbox {
    write: Plain<FrontWriteRpc>,
    read: Plain<FrontReadRpc>,
    truncate: Plain<FrontTruncateRpc>,
    set_leader: Plain<FrontSetLeaderRpc>,
}

/// What every call's task shares: the cache, the providers, the policy.
struct Shared<P: Providers, A> {
    providers: P,
    routes: Mutex<Routes<P>>,
    authz: Arc<dyn Authz>,
    audit: A,
    settings: FrontendSettings,
}

/// Run a frontend until `shutdown`: bind `settings.listen`, learn the cell
/// from its founding members, and serve the frontend contract, checking
/// every call through `authz` and reporting resolutions to `audit`.
///
/// # Errors
///
/// A failed bind, a registration the runtime refuses, or a runtime that
/// fails for good.
///
/// # Panics
///
/// If `settings.cell` is empty: a frontend fronts at least one machine.
#[tracing::instrument(level = "debug", skip_all, fields(listen = %settings.listen))]
pub async fn run_frontend<P, A>(
    providers: P,
    settings: FrontendSettings,
    authz: Arc<dyn Authz>,
    audit: A,
    shutdown: CancellationToken,
) -> SimulationResult<()>
where
    P: Providers,
    A: Audit + Clone + Send + Sync + 'static,
{
    assert!(!settings.cell.is_empty(), "a frontend fronts a cell");
    assert!(
        settings.client.redirect_limit >= 1,
        "a frontend follows at least one redirect"
    );
    let mut edge = RpcEdge::listen(
        &providers,
        &settings.listen.to_string(),
        "frontend",
        &settings.tunables,
    )
    .await?;
    let rpc = edge.handle().clone();
    let mut inbox = FrontendInbox {
        write: Inbound::plain(serve_well_known(&rpc)?),
        read: Inbound::plain(serve_well_known(&rpc)?),
        truncate: Inbound::plain(serve_well_known(&rpc)?),
        set_leader: Inbound::plain(serve_well_known(&rpc)?),
    };
    tracing::info!(listen = %settings.listen, cell = settings.cell.len(), "frontend_starting");
    let shared = Arc::new(Shared {
        routes: Mutex::new(Routes::new(rpc)),
        providers: providers.clone(),
        authz,
        audit,
        settings,
    });
    loop {
        moonpool_core::select! {
            biased;
            () = shutdown.cancelled() => return Ok(()),
            error = edge.run() => return Err(error),
            Some((FrontWrite { entry, call }, reply)) = inbox.write.recv() => {
                spawn_call(&shared, &shutdown, entry, call, reply, Draws::draw(&providers));
            }
            Some((FrontRead { entry, call }, reply)) = inbox.read.recv() => {
                spawn_call(&shared, &shutdown, entry, call, reply, Draws::draw(&providers));
            }
            Some((FrontTruncate { entry, call }, reply)) = inbox.truncate.recv() => {
                spawn_call(&shared, &shutdown, entry, call, reply, Draws::draw(&providers));
            }
            Some((FrontSetLeader { entry, call }, reply)) = inbox.set_leader.recv() => {
                spawn_call(&shared, &shutdown, entry, call, reply, Draws::draw(&providers));
            }
        }
    }
}

/// Serve one call in its own task, until it answers or the frontend stops
/// (the reply is then dropped: the caller sees a broken promise, an
/// ambiguity).
fn spawn_call<P, A, C>(
    shared: &Arc<Shared<P, A>>,
    shutdown: &CancellationToken,
    entry: Option<Entry>,
    call: Option<C>,
    reply: ReplySender<C::Ack>,
    draws: Draws,
) where
    P: Providers,
    A: Audit + Clone + Send + Sync + 'static,
    C: Forwarded,
{
    let shutdown = shutdown.clone();
    let task = shared.clone();
    shared
        .providers
        .task()
        .spawn_task(C::TASK, async move {
            moonpool_core::select! {
                biased;
                () = shutdown.cancelled() => {}
                ack = serve(&task, entry, call, draws) => {
                    let _ = reply.send(ack);
                }
            }
        })
        .detach();
}

/// One call: admitted, resolved, forwarded; its answer.
#[tracing::instrument(level = "debug", skip_all, fields(op = C::OPERATION.as_str()))]
async fn serve<P, A, C>(
    shared: &Shared<P, A>,
    entry: Option<Entry>,
    call: Option<C>,
    draws: Draws,
) -> C::Ack
where
    P: Providers,
    A: Audit + Clone + Send + Sync + 'static,
    C: Forwarded,
{
    let (Some(entry), Some(mut call)) = (entry, call) else {
        moonpool_assertions::reachable!("frontend: a call without its entry is malformed");
        return C::denied(Denial::Malformed);
    };
    let (journal, name) = match admit(shared, &entry, call.ids(), C::OPERATION, draws).await {
        Ok(admitted) => admitted,
        Err(Refusal::Denied(denial)) => return C::denied(denial),
        Err(Refusal::Unknown) => return C::unknown(),
        Err(Refusal::Unavailable) => return C::unavailable(),
    };
    assert!(journal.is_set(), "a call is forwarded to a set journal");
    call.aim(journal);
    assert_eq!(call.ids(), journal, "a forwarded call names its journal");
    let Some(client) = routes::client_for(shared, journal).await else {
        return C::unavailable();
    };
    let ack = forward::forward(shared, &client, &call, draws).await;
    if C::is_unknown(&ack)
        && let Some(name) = name
    {
        // The machines do not know the journal the name resolved to: the
        // resolution is stale (a deleted journal, a removed tenant). Drop
        // it, and resolve again once.
        lock(&shared.routes).stale(&name, journal);
        if draws.stale {
            moonpool_assertions::reachable!(
                "frontend: a stale resolution is answered as an unknown journal"
            );
            return ack;
        }
        return match routes::resolve(shared, &name).await {
            Resolved::Journal { journal: again, .. } if again != journal => {
                moonpool_assertions::reachable!(
                    "frontend: a stale resolution is dropped and the name resolves again"
                );
                call.aim(again);
                forward::forward(shared, &client, &call, draws).await
            }
            _ => ack,
        };
    }
    ack
}

/// Why a call is answered by the frontend itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Refusal {
    /// The entry or the token does not allow it.
    Denied(Denial),
    /// No live journal holds the name: the call's `unknown_journal`.
    Unknown,
    /// The names could not be resolved now: no verdict, nothing forwarded.
    Unavailable,
}

/// How a call names its journal, judged before any I/O.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Named {
    /// A `users` tenant's journal, by its name.
    Users(JournalName),
    /// An internal journal, by the call's ids.
    Internal(JournalIdentifier),
}

/// The journal `entry` names (or the call's ids name, with no tenant
/// name), or why the entry is malformed.
fn named(entry: &Entry, ids: JournalIdentifier) -> Result<Named, Denial> {
    if entry.tenant.is_empty() {
        if !entry.journal.is_empty() || !ids.is_set() {
            return Err(Denial::Malformed);
        }
        return Ok(Named::Internal(ids));
    }
    JournalName::new(&entry.tenant, &entry.journal)
        .map(Named::Users)
        .map_err(|_| Denial::Malformed)
}

/// The checks every call passes before it is forwarded: the entry, the
/// token, the names. `Ok` is the journal to forward to, and the name it
/// was resolved from; `Err` is why the frontend answers itself.
async fn admit<P, A>(
    shared: &Shared<P, A>,
    entry: &Entry,
    ids: JournalIdentifier,
    operation: Operation,
    draws: Draws,
) -> Result<(JournalIdentifier, Option<JournalName>), Refusal>
where
    P: Providers,
    A: Audit + Clone + Send + Sync + 'static,
{
    let named = named(entry, ids).map_err(|denial| {
        moonpool_assertions::reachable!("frontend: a malformed entry is denied");
        Refusal::Denied(denial)
    })?;
    let target = match &named {
        Named::Users(name) => Target::Users {
            tenant: name.tenant(),
            journal: name.journal(),
        },
        Named::Internal(_) => Target::Internal,
    };
    let now = shared.settings.epoch + moonpool_core::TimeProvider::now(shared.providers.time());
    let request = Request {
        operation,
        target,
        now,
    };
    if let Err(denial) = shared.authz.authorize(&entry.token, &request) {
        assert!(
            denial != Denial::Malformed,
            "an Authz never answers malformed"
        );
        tracing::debug!(
            op = operation.as_str(),
            denial = denial.as_str(),
            "frontend_denied"
        );
        moonpool_assertions::reachable!("frontend: a call is denied before any machine sees it");
        return Err(Refusal::Denied(denial));
    }
    match named {
        Named::Internal(journal) => {
            moonpool_assertions::reachable!("frontend: an internal journal is reached by its ids");
            Ok((journal, None))
        }
        Named::Users(name) => {
            if draws.cold {
                moonpool_assertions::reachable!("frontend: a call resolves on a cold cache");
                lock(&shared.routes).forget();
            }
            match routes::resolve(shared, &name).await {
                Resolved::Journal {
                    journal,
                    control,
                    at,
                } => {
                    assert!(journal.is_set(), "a resolved journal is set");
                    assert_eq!(
                        journal.tenant, control.tenant,
                        "a name resolves inside its tenant"
                    );
                    shared.audit.frontend_resolved(&name, journal, control, at);
                    Ok((journal, Some(name)))
                }
                Resolved::Unknown => Err(Refusal::Unknown),
                Resolved::Unavailable => Err(Refusal::Unavailable),
            }
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use paros_core::{JournalId, TenantId};

    use super::*;

    fn entry(tenant: &str, journal: &str) -> Entry {
        Entry {
            token: Vec::new(),
            tenant: tenant.into(),
            journal: journal.into(),
        }
    }

    const UNSET: JournalIdentifier = JournalIdentifier::new(TenantId(0), JournalId(0));

    const IDS: JournalIdentifier = JournalIdentifier {
        tenant: TenantId(7),
        journal: JournalId(9),
    };

    #[test]
    fn an_entry_names_a_journal_by_name_or_an_internal_one_by_its_ids() {
        assert_eq!(
            named(&entry("acme", "orders"), UNSET),
            Ok(Named::Users(JournalName::new("acme", "orders").unwrap()))
        );
        assert_eq!(named(&entry("", ""), IDS), Ok(Named::Internal(IDS)));
        // A journal name with no tenant, unset ids, a bad name: malformed.
        assert_eq!(named(&entry("", "orders"), IDS), Err(Denial::Malformed));
        assert_eq!(named(&entry("", ""), UNSET), Err(Denial::Malformed));
        assert_eq!(named(&entry("acme", ""), IDS), Err(Denial::Malformed));
    }
}
