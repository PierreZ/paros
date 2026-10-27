//! **The frontend** (#192 (the frontend), `docs/architecture/03-5-frontend-and-resolver.md` §3.5): the
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
//! and no state that survives it; nodes do no authorization. A token is
//! checked against the universe entry's root public keys, read with the
//! universe directory and read again, once, when a token does not verify
//! (a key rotated in since, #245 (Biscuit tokens)); until a first read the
//! [`Authz`]'s own pin rules.
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

mod forward;
mod routes;

pub use paros_model::authz::{Authz, Denial, Operation, Request, Target};

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use moonpool_core::{
    Detach, LocalTaskProvider, LocalTimeProvider, RandomProvider, SimulationResult,
};
use moonpool_rpc::RpcHandle;
use paros_core::journal::JournalIdentifier;
use paros_net::runtime::ParosProviders;
use tokio_util::sync::CancellationToken;

use crate::node::edge::{Plain, RpcEdge};
use forward::Forwarded;
use paros_client::ClientTunables;
use paros_model::address::Address;
use paros_model::address::Names;
use paros_model::name::JournalName;
use paros_net::inbound::{Inbound, ReplySender, serve_well_known};
use paros_net::transport::Transport;
use paros_observe::audit::Audit;
use paros_tunables::DriverTunables;
use paros_wire::methods::{
    FrontReadRpc, FrontSetJournalTagsRpc, FrontSetLeaderRpc, FrontTruncateRpc, FrontWriteRpc,
};
use paros_wire::{
    Entry, FrontRead, FrontSetJournalTags, FrontSetLeader, FrontTruncate, FrontWrite,
    FrontendVerdict, SetJournalTagsAck,
};
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
    /// Its RPC edge's shape (queue, connect budget, keep-alive) and its
    /// clean stop's bound.
    pub tunables: DriverTunables,
    /// How it calls the machines: each attempt's timeout, the redirects it
    /// follows, how long it waits for the cell to answer an `Inspect`.
    pub client: ClientTunables,
}

/// The frontend role a machine is given to serve (#723 (frontend as a
/// parosd role)): the `Authz` it checks every call through and the wall
/// clock that judges a token, beside the client policy it forwards with.
/// A machine given none serves no frontend.
#[derive(Clone)]
pub struct FrontendRole {
    /// What every call is checked against (`BiscuitAuthz` in `parosd`).
    pub authz: Arc<dyn Authz>,
    /// The wall clock at the provider's zero, since the Unix epoch.
    pub epoch: Duration,
    /// How it calls the machines.
    pub client: ClientTunables,
}

impl FrontendRole {
    /// The settings of the frontend a machine serves on its own edge: it
    /// listens where the machine does and fronts `cell`, the founding
    /// members' addresses.
    ///
    /// # Panics
    ///
    /// If `cell` is empty: a frontend fronts at least one machine.
    #[must_use]
    pub fn settings(
        &self,
        listen: SocketAddr,
        cell: Vec<Address>,
        names: Names,
        tunables: DriverTunables,
    ) -> FrontendSettings {
        assert!(
            !cell.is_empty(),
            "a machine's frontend fronts a founding member"
        );
        FrontendSettings {
            listen,
            cell,
            names,
            epoch: self.epoch,
            tunables,
            client: self.client,
        }
    }
}

impl std::fmt::Debug for FrontendRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrontendRole")
            .field("epoch", &self.epoch)
            .field("client", &self.client)
            .finish_non_exhaustive()
    }
}

impl PartialEq for FrontendRole {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.authz, &other.authz)
            && self.epoch == other.epoch
            && self.client == other.client
    }
}

impl Eq for FrontendRole {}

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
    /// Keep the cell after a server refused a stale machine interface
    /// (#404 (static vs dynamic endpoints)).
    keep_stale: bool,
}

impl Draws {
    fn draw<P: ParosProviders>(providers: &P) -> Self {
        let cold = moonpool_buggify::buggify_with_prob!(0.05);
        let stale = moonpool_buggify::buggify_with_prob!(0.1);
        let stray = moonpool_buggify::buggify_with_prob!(0.2)
            .then(|| providers.random().random_range(0..u64::from(u32::MAX)));
        let keep_stale = moonpool_buggify::buggify_with_prob!(0.25);
        Self {
            cold,
            stale,
            stray,
            keep_stale,
        }
    }
}

/// The frontend's inbound queues: the four calls of its contract, and a
/// tenant's tag request (#575 (key:value tags)).
struct FrontendInbox {
    write: Plain<FrontWriteRpc>,
    read: Plain<FrontReadRpc>,
    truncate: Plain<FrontTruncateRpc>,
    set_leader: Plain<FrontSetLeaderRpc>,
    set_tags: Plain<FrontSetJournalTagsRpc>,
}

/// What every call's task shares: the cache, the providers, the policy.
struct Shared<P: ParosProviders, A> {
    providers: P,
    routes: Mutex<Routes<P>>,
    authz: Arc<dyn Authz>,
    audit: A,
    settings: FrontendSettings,
    /// The calls whose task runs (#560 (clean Ctrl-C shutdown)): a stopping
    /// frontend waits for them.
    in_flight: AtomicUsize,
}

/// One running call, counted in [`Shared::in_flight`] while it lives.
struct Running<P: ParosProviders, A>(Arc<Shared<P, A>>);

impl<P: ParosProviders, A> Running<P, A> {
    fn start(shared: &Arc<Shared<P, A>>) -> Self {
        shared.in_flight.fetch_add(1, Ordering::SeqCst);
        Self(shared.clone())
    }
}

impl<P: ParosProviders, A> Drop for Running<P, A> {
    fn drop(&mut self) {
        let before = self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
        assert!(before > 0, "a running call was counted");
    }
}

/// How often a stopping frontend looks whether its calls ended. Not a
/// tunable: it only sets how late, by at most this much, a drained frontend
/// sees it is drained; `shutdown_within` is the bound that shapes a stop.
const STOP_POLL: Duration = Duration::from_millis(50);

/// Run a frontend on its own edge until `shutdown`: bind `settings.listen`
/// with `transport`, learn the cell from its founding members, and serve
/// the frontend contract, checking every call through `authz` and reporting
/// resolutions to `audit`. A machine serves the same contract on its own
/// edge instead ([`spawn`], #723 (frontend as a parosd role)); this entry
/// stays for an embedder.
///
/// `shutdown` begins a clean stop (#560 (clean Ctrl-C shutdown)): every new
/// call is refused `stopping`, and the frontend returns once the calls it
/// took are answered, or once `tunables.shutdown_within` ran out; a call
/// still running then gets no answer, which its caller sees as ambiguous.
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
    transport: Transport,
    settings: FrontendSettings,
    authz: Arc<dyn Authz>,
    audit: A,
    shutdown: CancellationToken,
) -> SimulationResult<()>
where
    P: ParosProviders,
    A: Audit + Clone + Send + Sync + 'static,
{
    check(&settings);
    let edge = RpcEdge::listen(
        &providers,
        &settings.listen.to_string(),
        "frontend",
        &settings.tunables,
        &transport,
    )
    .await?;
    let rpc = edge.handle().clone();
    let inbox = FrontendInbox::on(&rpc)?;
    moonpool_core::select! {
        error = edge.run() => Err(error),
        served = serve_frontend(providers, rpc, inbox, settings, (authz, audit), shutdown) => served,
    }
}

/// Serve the frontend contract on the machine edge `rpc` until `shutdown`,
/// in a task of its own (#723 (frontend as a parosd role)): the frontend
/// role a machine serves beside its journals, at its own address.
///
/// # Errors
///
/// A registration the runtime refuses.
///
/// # Panics
///
/// If `settings.cell` is empty: a frontend fronts at least one machine.
#[tracing::instrument(level = "debug", skip_all, fields(listen = %settings.listen))]
pub fn spawn<P, A>(
    providers: &P,
    rpc: &RpcHandle<P>,
    settings: FrontendSettings,
    (authz, audit): (Arc<dyn Authz>, A),
    shutdown: CancellationToken,
) -> SimulationResult<()>
where
    P: ParosProviders,
    A: Audit + Clone + Send + Sync + 'static,
{
    check(&settings);
    let inbox = FrontendInbox::on(rpc)?;
    let task = serve_frontend(
        providers.clone(),
        rpc.clone(),
        inbox,
        settings,
        (authz, audit),
        shutdown,
    );
    providers
        .local_task()
        .spawn_local_task("paros-frontend", async move {
            if let Err(error) = task.await {
                tracing::warn!(%error, "frontend_ended");
            }
        })
        .detach();
    moonpool_assertions::reachable!("frontend: a machine serves the frontend role");
    Ok(())
}

/// A frontend's settings, checked before it serves.
fn check(settings: &FrontendSettings) {
    assert!(!settings.cell.is_empty(), "a frontend fronts a cell");
    assert!(
        settings.client.redirect_limit >= 1,
        "a frontend follows at least one redirect"
    );
}

impl FrontendInbox {
    /// The frontend's queues, registered on `rpc`.
    fn on<P: ParosProviders>(rpc: &RpcHandle<P>) -> SimulationResult<Self> {
        Ok(Self {
            write: Inbound::plain(serve_well_known(rpc)?),
            read: Inbound::plain(serve_well_known(rpc)?),
            truncate: Inbound::plain(serve_well_known(rpc)?),
            set_leader: Inbound::plain(serve_well_known(rpc)?),
            set_tags: Inbound::plain(serve_well_known(rpc)?),
        })
    }
}

/// The frontend's loop over the queues `inbox` of the edge `rpc`, until a
/// clean stop ends.
async fn serve_frontend<P, A>(
    providers: P,
    rpc: RpcHandle<P>,
    mut inbox: FrontendInbox,
    settings: FrontendSettings,
    (authz, audit): (Arc<dyn Authz>, A),
    shutdown: CancellationToken,
) -> SimulationResult<()>
where
    P: ParosProviders,
    A: Audit + Clone + Send + Sync + 'static,
{
    tracing::info!(listen = %settings.listen, cell = settings.cell.len(), "frontend_starting");
    let shared = Arc::new(Shared {
        routes: Mutex::new(Routes::new(rpc)),
        providers: providers.clone(),
        authz,
        audit,
        settings,
        in_flight: AtomicUsize::new(0),
    });
    // Every call's task ends with the frontend: a call still running when
    // it returns gets no answer.
    let ended = CancellationToken::new();
    let _ended = ended.clone().drop_guard();
    let within = shared.settings.tunables.shutdown_within;
    let time = providers.time().clone();
    // When a clean stop returns whatever still runs, once one began.
    let mut stop: Option<Duration> = None;
    // Whether the stop waits out its whole bound, drained or not: a
    // frontend behind a slow load balancer still gets calls a while, and
    // refuses them (#560). Drawn once, at the stop.
    let mut linger = false;
    loop {
        moonpool_core::select! {
            biased;
            () = shutdown.cancelled(), if stop.is_none() => {
                stop = Some(time.now().saturating_add(within));
                linger = moonpool_buggify::buggify_with_prob!(0.5);
                if linger {
                    moonpool_assertions::reachable!("stop: a stopping frontend lingers to its bound");
                }
                tracing::info!(listen = %shared.settings.listen, within_ms = u64::try_from(within.as_millis()).unwrap_or(u64::MAX), "frontend_stop_started");
            }
            _ = time.sleep(STOP_POLL), if stop.is_some() => {
                let expired = stop.is_some_and(|deadline| time.now() >= deadline);
                if stopped(&shared, expired, linger) {
                    return Ok(());
                }
            }
            Some((FrontWrite { entry, call }, reply)) = inbox.write.recv() => {
                if stop.is_some() {
                    refuse(call.as_ref(), reply);
                } else {
                    spawn_call(&shared, &ended, entry, call, reply, Draws::draw(&providers));
                }
            }
            Some((FrontRead { entry, call }, reply)) = inbox.read.recv() => {
                if stop.is_some() {
                    refuse(call.as_ref(), reply);
                } else {
                    spawn_call(&shared, &ended, entry, call, reply, Draws::draw(&providers));
                }
            }
            Some((FrontTruncate { entry, call }, reply)) = inbox.truncate.recv() => {
                if stop.is_some() {
                    refuse(call.as_ref(), reply);
                } else {
                    spawn_call(&shared, &ended, entry, call, reply, Draws::draw(&providers));
                }
            }
            Some((FrontSetLeader { entry, call }, reply)) = inbox.set_leader.recv() => {
                if stop.is_some() {
                    refuse(call.as_ref(), reply);
                } else {
                    spawn_call(&shared, &ended, entry, call, reply, Draws::draw(&providers));
                }
            }
            Some((call, reply)) = inbox.set_tags.recv() => {
                if stop.is_some() {
                    refuse_tags(call, reply);
                } else {
                    spawn_tags(&shared, &ended, call, reply, Draws::draw(&providers));
                }
            }
        }
    }
}

/// Whether a stopping frontend is done (#560 (clean Ctrl-C shutdown)): no
/// call it took still runs and it does not linger to its bound, or the bound
/// ran out.
fn stopped<P, A>(shared: &Shared<P, A>, expired: bool, linger: bool) -> bool
where
    P: ParosProviders,
    A: Audit + Clone + Send + Sync + 'static,
{
    let running = shared.in_flight.load(Ordering::SeqCst);
    if running == 0 && (!linger || expired) {
        moonpool_assertions::reachable!("stop: a frontend stopped cleanly");
        tracing::info!(listen = %shared.settings.listen, "frontend_stop_completed");
        return true;
    }
    if running > 0 && expired {
        moonpool_assertions::reachable!("stop: a frontend's stop ran out of time");
        tracing::warn!(listen = %shared.settings.listen, running, "frontend_stop_timed_out");
        return true;
    }
    assert!(!expired, "an expired stop always ends");
    false
}

/// Refuse a call a stopping frontend took (#560 (clean Ctrl-C shutdown)):
/// `stopping`, retry elsewhere.
fn refuse<C: Forwarded>(_call: Option<&C>, reply: ReplySender<C::Ack>) {
    moonpool_assertions::reachable!("stop: a stopping frontend refuses a call");
    let _ = reply.send(C::stopping());
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
    P: ParosProviders,
    A: Audit + Clone + Send + Sync + 'static,
    C: Forwarded,
{
    let shutdown = shutdown.clone();
    let task = shared.clone();
    let running = Running::start(shared);
    shared
        .providers
        .local_task()
        .spawn_local_task(C::TASK, async move {
            let _running = running;
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
    P: ParosProviders,
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

/// Serve one tag request in its own task (#575 (key:value tags)), as
/// [`spawn_call`] serves a journal call.
fn spawn_tags<P, A>(
    shared: &Arc<Shared<P, A>>,
    shutdown: &CancellationToken,
    call: FrontSetJournalTags,
    reply: ReplySender<SetJournalTagsAck>,
    draws: Draws,
) where
    P: ParosProviders,
    A: Audit + Clone + Send + Sync + 'static,
{
    let shutdown = shutdown.clone();
    let task = shared.clone();
    let running = Running::start(shared);
    shared
        .providers
        .local_task()
        .spawn_local_task("frontend-set-tags", async move {
            let _running = running;
            moonpool_core::select! {
                biased;
                () = shutdown.cancelled() => {}
                ack = serve_tags(&task, call, draws) => {
                    let _ = reply.send(ack);
                }
            }
        })
        .detach();
}

/// A tag request's answer by the frontend itself.
fn tags_denied(denial: Denial) -> SetJournalTagsAck {
    let mut ack = SetJournalTagsAck::default();
    ack.set_frontend(denial.verdict());
    ack
}

/// A stopping frontend's answer to a tag request (#560 (clean Ctrl-C
/// shutdown)): a malformed one is still denied as malformed, and only a
/// well-formed one is told to send again elsewhere.
fn refuse_tags(call: FrontSetJournalTags, reply: ReplySender<SetJournalTagsAck>) {
    let ack = match well_formed_tags(call) {
        Ok(_) => SetJournalTagsAck {
            outcome: "unavailable".into(),
            ..SetJournalTagsAck::default()
        },
        Err(denied) => denied,
    };
    assert!(
        ack.outcome == "unavailable" || ack.outcome.is_empty(),
        "a stopping frontend decides no tag request"
    );
    moonpool_assertions::reachable!("stop: a stopping frontend refuses a call");
    let _ = reply.send(ack);
}

/// A tag request's entry and tags when it is well formed (#575 (key:value
/// tags)), or its malformed denial: a user's journal only, by its names (an
/// internal journal carries no user tag a tenant sets), with an id.
fn well_formed_tags(
    call: FrontSetJournalTags,
) -> Result<(Entry, paros_model::tags::UserTags), SetJournalTagsAck> {
    let Some(entry) = call.entry else {
        return Err(tags_denied(Denial::Malformed));
    };
    let tags = paros_model::tags::UserTags::from_wire(call.tags);
    let (Ok(tags), false, true) = (tags, entry.tenant.is_empty(), call.request != 0) else {
        moonpool_assertions::reachable!("frontend: a malformed tag request is denied");
        return Err(tags_denied(Denial::Malformed));
    };
    assert!(
        !entry.tenant.is_empty(),
        "a well-formed tag request names a tenant"
    );
    Ok((entry, tags))
}

/// A tag request (#575): admitted like a journal call, then sent to the
/// tenant coordinator as a `SetJournalTags` of the journal the name
/// resolved to. The caller's id rides through, so a retry acts once.
#[tracing::instrument(level = "debug", skip_all)]
async fn serve_tags<P, A>(
    shared: &Shared<P, A>,
    call: FrontSetJournalTags,
    draws: Draws,
) -> SetJournalTagsAck
where
    P: ParosProviders,
    A: Audit + Clone + Send + Sync + 'static,
{
    let unavailable = || SetJournalTagsAck {
        outcome: "unavailable".into(),
        ..SetJournalTagsAck::default()
    };
    let request_id = call.request;
    let (entry, tags) = match well_formed_tags(call) {
        Ok(parts) => parts,
        Err(denied) => return denied,
    };
    let unset = JournalIdentifier::new(
        paros_core::journal::TenantId(0),
        paros_core::journal::JournalId(0),
    );
    let (journal, name) = match admit(shared, &entry, unset, Operation::SetTags, draws).await {
        Ok((journal, Some(name))) => (journal, name),
        Ok((_, None)) => return tags_denied(Denial::Malformed),
        Err(Refusal::Denied(denial)) => return tags_denied(denial),
        Err(Refusal::Unknown) => {
            return SetJournalTagsAck {
                outcome: "unknown_journal".into(),
                ..SetJournalTagsAck::default()
            };
        }
        Err(Refusal::Unavailable) => return unavailable(),
    };
    assert!(journal.is_set(), "a tag request names a set journal");
    let Some((election, client, rpc)) = routes::coordinator_route(shared).await else {
        return unavailable();
    };
    let request = paros_client::journals::JournalRequest {
        request: request_id,
        tenant: journal.tenant,
        op: paros_client::journals::JournalOp::SetTags {
            name: name.journal().as_bytes().to_vec(),
            tags,
        },
    };
    let answer = paros_client::journals::request(
        &shared.providers,
        &rpc,
        &shared.settings.names,
        &client,
        election,
        &request,
        shared.settings.client.request_timeout,
    )
    .await;
    moonpool_assertions::reachable!("frontend: a tenant's tag request reaches its coordinator");
    let ack = answer.to_wire();
    let mut out = SetJournalTagsAck {
        outcome: ack.outcome,
        journal: ack.journal,
        ..SetJournalTagsAck::default()
    };
    out.set_frontend(FrontendVerdict::None);
    out
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
    P: ParosProviders,
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
    let now =
        shared.settings.epoch + moonpool_core::LocalTimeProvider::now(shared.providers.time());
    let keys = routes::keys(shared, false).await;
    let refreshed;
    let mut request = Request {
        operation,
        target,
        now,
        keys: keys.as_deref(),
    };
    let mut checked = shared.authz.authorize(&entry.token, &request);
    if checked == Err(Denial::InvalidToken) && keys.is_some() {
        // The token may be signed by a key the universe entry took since
        // the frontend last read it (a rotation): read the keys again,
        // once, and check again.
        refreshed = routes::keys(shared, true).await;
        if refreshed != keys {
            moonpool_assertions::reachable!(
                "frontend: a token is checked again against root keys read afresh"
            );
            request.keys = refreshed.as_deref();
            checked = shared.authz.authorize(&entry.token, &request);
        }
    }
    if let Err(denial) = checked {
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
                Resolved::Unknown { read } => {
                    if let Some((control, at)) = read {
                        shared.audit.frontend_unknown(&name, control, at);
                    }
                    Err(Refusal::Unknown)
                }
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
    use paros_core::journal::{JournalId, TenantId};

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
