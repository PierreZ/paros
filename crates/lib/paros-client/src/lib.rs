//! `paros::client` — the Rust client of a paros deployment (#221).
//!
//! [`NodeClient`] is a typed RPC stub: one call to one server, nothing
//! more. A correct client needs a **policy** on top of it, and this module
//! is that policy, written once, provider-generic (`P: ParosProviders`) like the
//! drivers, so the client the deterministic simulation's sweep, its
//! linearizability checker and its hunts judge is the client `parosctl`
//! and every later consumer ships:
//!
//! - **Which server to ask** ([`Client`]): a server list with a node id per
//!   server, a leader hint learned from every verdict and every redirect,
//!   and a [`Retarget`] rule for where an attempt goes after a redirect.
//!   The redirect budget and the backoff are [`ClientTunables`].
//! - **A retry is the same write.** Every retry re-sends the request it
//!   retries — generation, owner, position and bytes — so the journal
//!   answers it from the log (`Duplicate`) and never holds it twice. There
//!   is no "retry with a fresh position" anywhere in this module.
//! - **Ambiguity is a result.** A write whose answer never came is
//!   [`WriteOutcome::Ambiguous`], never "not done". [`Client::resolve`]
//!   settles it: it reads the position back, and re-sends the identical
//!   write, which the log answers.
//! - **A writer session** ([`Writer`]): claim a journal with `SetLeader`
//!   against the generation read, track the generation and the next
//!   position, write at the tail; superseded by a newer owner, it says so
//!   and writes nothing more until its caller claims again.
//! - **Several writes in flight** ([`Pipeline`], #597 (several writes in
//!   flight per journal)): one writer's leadership shared by many callers,
//!   each write at the next position without waiting for the answer of the
//!   one in front of it. A write that arrives early waits and is sent again
//!   unchanged; a failure stops new writes until the ones in flight settle.
//! - **A reader** ([`Reader`]): a cursor, paged `Read`s with a long-poll at
//!   the tail, and a `truncated` answer resumed at the floor it names and
//!   reported as a [`ReaderOutcome::Gap`] — never skipped silently.
//! - **A truncation is fenced like a write** (#228): [`Writer::truncate`]
//!   sends the owner's own `(generation, owner)`, and a refusal supersedes
//!   the writer like a refused write. [`Client::truncate`] follows
//!   redirects for whatever request it is handed.
//! - **Checkpoint and truncate** ([`checkpoint`], #230): a journal owner
//!   folds its journal, writes the state as a checkpoint record, and
//!   truncates to it; every reader restarts from the checkpoint at the
//!   floor. The pure [`checkpoint::Folder`] is what the system journals'
//!   node follower runs too.
//! - **The operator calls** pass through with the same discipline: [`Client::reconfigure`] and
//!   [`Client::reconfigure_matchmakers`] re-ask a busy or unsettled node,
//!   [`Client::inspect`] and [`Client::retire`] are one bounded attempt.
//!
//! Every outcome is a typed value ([`outcome`]), never a string, and every
//! tunable is plain data ([`ClientTunables`]) a harness can push to an
//! extreme. The client draws no randomness: where a choice is the caller's
//! (the first server, the retarget rule), the caller makes it.
//!
//! **Deliberate misbehaviour is explicit.** A harness needs a client that
//! writes under a stale generation, re-sends a write it saw written,
//! submits one write to two servers at once, or gives up on an attempt
//! before its ack can come back. Each of those is a call a caller makes on
//! purpose — [`Writer::stale_entry`], [`Writer::stale_truncate_request`]
//! (a superseded owner's truncation), [`Client::write_attempt`] with a
//! `listen` bound, [`Client::write_attempt`] to two targets — and none of
//! them is what [`Writer::write`] does.
//!
//! **Observation** goes through [`CallObserver`]: every attempt at the four
//! journal calls is reported when it is built and when it is judged, which
//! is the seam the simulation's history checker reads. Production passes
//! [`NoObserver`].
//!
//! The module is wasm-safe: it needs the provider's time and the caller's
//! RPC runtime, and nothing else.

pub mod bootstrap;
pub mod capacity;
pub mod cell;
pub mod checkpoint;
pub mod election;
pub mod fleet;
pub mod initialize;
pub mod journals;
pub mod load;
pub mod multi;
pub mod names;
mod observer;
pub mod outcome;
mod pipeline;
mod reader;
pub mod resolve;
#[cfg(test)]
mod tests;
pub mod universe;
pub mod views;
mod writer;

use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use moonpool_core::LocalTimeProvider;
use moonpool_rpc::RpcHandle;
use paros_core::{JournalIdentifier, JournalView, LeaderUuid, QuorumSystem, TenantId};
use paros_net::runtime::ParosProviders;
use tokio_util::sync::CancellationToken;

pub use observer::{Answered, Attempted, CallObserver, NoObserver};
pub use outcome::{
    ClaimOutcome, MatchmakersRefusal, ReadOutcome, ReconfigureMatchmakersOutcome,
    ReconfigureOutcome, RetireOutcome, RetireRefusal, SetLeaderOutcome, TruncateOutcome,
    WriteOutcome,
};
pub use paros_tunables::ClientTunables;
pub use pipeline::{Pipeline, Pipelined, Ticket};
pub use reader::{Reader, ReaderOutcome};
pub use writer::{Learned, Writer, WriterOutcome, leader_uuid, write_request};

use paros_model::address::Address;
use paros_model::address::Names;
use paros_net::NodeClient;
use paros_wire::{
    InspectReply, Read, Reconfigure, ReconfigureMatchmakers, RetireRequest, SetLeader, Truncate,
    Write, leader_uuid_from_proto, leader_uuid_to_proto, quorum_system_to_proto,
};

/// Where an attempt goes after a redirect, a transport error or an
/// ambiguous outcome. Every rule is a valid client; which one is the
/// caller's choice.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Retarget {
    /// The leader the reply named, or the next server when it named none.
    #[default]
    FollowHint,
    /// The same server again (the node that may have decided the attempt).
    SameNode,
    /// The next server, whatever the reply named.
    NextNode,
}

/// One server of a deployment: its node id (what a redirect names) and its
/// stub.
pub struct Server<P: ParosProviders> {
    /// The node id a leader hint names it by.
    pub id: u64,
    /// The stub that reaches it.
    pub node: NodeClient<P>,
}

impl<P: ParosProviders> Clone for Server<P> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            node: self.node.clone(),
        }
    }
}

/// The client's belief about who leads: the server it believes leads now,
/// and the one it believed before the last change.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LeaderHint {
    /// The server (an index into the server list) believed to lead.
    pub current: Option<usize>,
    /// The server believed to lead before `current` replaced it.
    pub stale: Option<usize>,
    /// The server whose last read went unanswered (a moved or down
    /// machine): a read that would start there starts at the next server
    /// instead, until it serves a read again.
    pub silent: Option<usize>,
}

/// What [`Client::write`] came back with: the outcome, the server that gave
/// it, and how many redirects it followed on the way.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteReport {
    /// The outcome of the last attempt.
    pub outcome: WriteOutcome,
    /// The server the last attempt went to.
    pub server: usize,
    /// Redirects followed before it.
    pub redirects: u32,
}

/// How [`Client::write`] sends a write. The default is the ordinary client:
/// follow the leader hint, follow redirects, wait for the answer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WriteOptions {
    /// Where an attempt goes after a redirect.
    pub retarget: Retarget,
    /// `false`: stop at the first redirect (a write aimed at a non-leader
    /// on purpose).
    pub stop_at_redirect: bool,
    /// Stop listening for the **first** attempt's answer after this long
    /// and report it ambiguous — a harness's honest-ambiguity generator.
    /// The attempt is still sent; it may still land.
    pub abandon_first_after: Option<Duration>,
}

/// What [`Client::resolve`] settled an ambiguous write to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Resolution {
    /// The journal holds the write at `[seq, seq + count)`.
    Written {
        /// The first record's position.
        seq: u64,
        /// The records the batch holds.
        count: u64,
    },
    /// The journal does not hold the write, and never will: the position
    /// holds another write, or another leader fenced the write's uuid before
    /// the journal reached the position. "Never" holds for a caller that
    /// never reinstates a uuid (the library's writer never does): one that
    /// reinstates the write's uuid lets a delayed copy land again
    /// (`docs/architecture/02-data-plane.md` §2.3).
    NotWritten {
        /// The journal state that proves it.
        state: JournalView,
    },
    /// The position is below the journal's floor: whether it was written is
    /// unknowable.
    Truncated {
        /// The journal state that says so.
        state: JournalView,
    },
    /// Still unknown: no server answered within the budget, or the write
    /// is ahead of the journal's tail and may still land.
    Unresolved,
}

/// What [`Client::resolve`] did: the resolution, and whether a read-back
/// settled it on its own (without an identical re-send answering).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolveReport {
    /// What the write resolved to.
    pub resolution: Resolution,
    /// A read-back answered on its own: the position is below the floor,
    /// the write was fenced before the journal reached its position, or the
    /// position holds exactly the write under its leader (#564).
    pub by_read_back: bool,
}

/// What [`Client::read_any`] came back with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadReport {
    /// The outcome of the last attempt.
    pub outcome: ReadOutcome,
    /// The server the last attempt went to.
    pub server: usize,
    /// Attempts made.
    pub attempts: u64,
}

/// A client of one deployment: a server list, a leader hint, the tunables,
/// and the caller's observer and shutdown signal. Cheap to clone; clones
/// share the leader hint.
pub struct Client<P: ParosProviders> {
    time: P::Time,
    servers: Arc<[Server<P>]>,
    rotation: usize,
    tunables: ClientTunables,
    observer: Arc<dyn CallObserver>,
    shutdown: CancellationToken,
    hint: Arc<Mutex<LeaderHint>>,
}

impl<P: ParosProviders> Clone for Client<P> {
    fn clone(&self) -> Self {
        Self {
            time: self.time.clone(),
            servers: self.servers.clone(),
            rotation: self.rotation,
            tunables: self.tunables,
            observer: self.observer.clone(),
            shutdown: self.shutdown.clone(),
            hint: self.hint.clone(),
        }
    }
}

impl<P: ParosProviders> Client<P> {
    /// A client of `servers`, timed by `providers`.
    ///
    /// # Panics
    ///
    /// If `servers` is empty: a client with nowhere to send is a
    /// programming error, not an operating condition.
    #[must_use]
    pub fn new(providers: &P, servers: Vec<Server<P>>, tunables: ClientTunables) -> Self {
        assert!(!servers.is_empty(), "a client needs at least one server");
        let rotation = servers.len();
        Self {
            time: providers.time().clone(),
            servers: servers.into(),
            rotation,
            tunables,
            observer: Arc::new(NoObserver),
            shutdown: CancellationToken::new(),
            hint: Arc::default(),
        }
    }

    /// A client of the servers at `addrs` (`(node id, address)` pairs),
    /// called through the caller's RPC runtime `rpc`.
    ///
    /// # Panics
    ///
    /// If `addrs` is empty.
    #[must_use]
    pub fn connect(
        providers: &P,
        rpc: &RpcHandle<P>,
        addrs: &[(u64, SocketAddr)],
        tunables: ClientTunables,
    ) -> Self {
        let servers = addrs
            .iter()
            .map(|(id, addr)| Server {
                id: *id,
                node: NodeClient::new(rpc, *addr),
            })
            .collect();
        Self::new(providers, servers, tunables)
    }

    /// A client of the servers at `addrs` (`(node id, advertised address)`
    /// pairs, #257), each resolved through `names` at every call, called
    /// through the caller's RPC runtime `rpc`.
    ///
    /// # Panics
    ///
    /// If `addrs` is empty.
    #[must_use]
    pub fn connect_named(
        providers: &P,
        rpc: &RpcHandle<P>,
        names: &Names,
        addrs: &[(u64, Address)],
        tunables: ClientTunables,
    ) -> Self {
        let servers = addrs
            .iter()
            .map(|(id, addr)| Server {
                id: *id,
                node: NodeClient::named(rpc, names.clone(), addr.clone()),
            })
            .collect();
        Self::new(providers, servers, tunables)
    }

    /// The same servers, runtime and policies under a leader hint of its
    /// own. A hint names one journal's leader: a caller driving several
    /// journals through one client (a journal's and the control journals',
    /// #247) keeps one client per journal, or every call to the others
    /// starts at the first one's leader — which may not serve them at all.
    #[must_use]
    pub fn with_own_leader_hint(mut self) -> Self {
        self.hint = Arc::default();
        self
    }

    /// The observer every attempt is reported to.
    pub fn observer(&self) -> &Arc<dyn CallObserver> {
        &self.observer
    }

    /// Report every attempt to `observer`.
    #[must_use]
    pub fn with_observer(mut self, observer: Arc<dyn CallObserver>) -> Self {
        self.observer = observer;
        self
    }

    /// Give up every wait the moment `shutdown` is cancelled (the outcome
    /// is then ambiguous, never assumed).
    #[must_use]
    pub fn with_shutdown(mut self, shutdown: CancellationToken) -> Self {
        self.shutdown = shutdown;
        self
    }

    /// This client over other servers, with its runtime, tunables, observer
    /// and shutdown, under a leader hint of its own (a hint names a server
    /// of the old list).
    ///
    /// # Panics
    ///
    /// If `servers` is empty.
    #[must_use]
    pub fn with_servers(&self, servers: Vec<Server<P>>) -> Self {
        assert!(!servers.is_empty(), "a client needs at least one server");
        let rotation = servers.len();
        Self {
            servers: servers.into(),
            rotation,
            hint: Arc::default(),
            ..self.clone()
        }
    }

    /// This client over the machines at `addrs` (`(node id, address)`
    /// pairs), through the same runtime and resolver (#232 (multiple
    /// cells)): another cell's machines, from the universe directory.
    ///
    /// # Panics
    ///
    /// If `addrs` is empty.
    #[must_use]
    pub fn elsewhere(&self, addrs: &[(u64, paros_model::address::Address)]) -> Self {
        let base = &self.servers[0].node;
        let servers = addrs
            .iter()
            .map(|(id, addr)| Server {
                id: *id,
                node: base.elsewhere(addr.clone()),
            })
            .collect();
        self.with_servers(servers)
    }

    /// This client under other tunables, sharing its leader hint.
    #[must_use]
    pub fn with_tunables(&self, tunables: ClientTunables) -> Self {
        Self {
            tunables,
            ..self.clone()
        }
    }

    /// Walk only the first `count` servers when moving on to "the next
    /// server" (a retarget, a read's rotation); the rest are reached only
    /// when a reply names them. A deployment's bootstrap pool, say, before
    /// nodes that may not serve every journal yet.
    ///
    /// # Panics
    ///
    /// If `count` is zero or past the server list.
    #[must_use]
    pub fn rotating_over(mut self, count: usize) -> Self {
        assert!(
            count > 0 && count <= self.servers.len(),
            "a rotation covers at least one server and at most all of them"
        );
        self.rotation = count;
        self
    }

    /// The provider's time now (what a checkpoint policy's time bound reads).
    #[must_use]
    pub fn now(&self) -> Duration {
        self.time.now()
    }

    /// The tunables.
    #[must_use]
    pub fn tunables(&self) -> &ClientTunables {
        &self.tunables
    }

    /// How many servers the client knows.
    #[must_use]
    pub fn server_count(&self) -> usize {
        self.servers.len()
    }

    /// The server a node id names, when the client knows it.
    #[must_use]
    pub fn index_of(&self, id: u64) -> Option<usize> {
        self.servers.iter().position(|server| server.id == id)
    }

    /// The node id of server `index`.
    #[must_use]
    pub fn id_of(&self, index: usize) -> u64 {
        self.servers[index % self.servers.len()].id
    }

    /// The stub of server `index` (its raw, unobserved calls).
    #[must_use]
    pub fn node(&self, index: usize) -> &NodeClient<P> {
        &self.servers[index % self.servers.len()].node
    }

    /// The leader hint.
    #[must_use]
    pub fn hint(&self) -> LeaderHint {
        *self.hint.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The server believed to lead.
    #[must_use]
    pub fn leader(&self) -> Option<usize> {
        self.hint().current
    }

    /// Adopt the leader a reply named, by node id (`None`, or an id the
    /// client does not know, clears the hint); a change of leader remembers
    /// the previous one.
    pub fn observe_leader(&self, id: Option<u64>) {
        let next = id.and_then(|id| self.index_of(id));
        self.adopt_leader(next);
    }

    /// Adopt server `index` as the leader (it gave a verdict).
    pub fn observe_leader_at(&self, index: usize) {
        self.adopt_leader(Some(index % self.servers.len()));
    }

    /// Drop the leader hint (its server did not answer); it becomes the
    /// stale one.
    fn forget_leader(&self) {
        let mut hint = self.hint.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(previous) = hint.current.take() {
            hint.stale = Some(previous);
        }
    }

    fn adopt_leader(&self, next: Option<usize>) {
        let mut hint = self.hint.lock().unwrap_or_else(PoisonError::into_inner);
        if let (Some(previous), Some(next)) = (hint.current, next)
            && previous != next
        {
            hint.stale = Some(previous);
        }
        hint.current = next;
    }

    /// Where a policy loop asks next after server `current` redirected
    /// naming `leader` (a node id): the named server when the client knows
    /// it and it is another one; otherwise the next server of the rotation,
    /// `backoff` later. `None` when the client shut down during the pause.
    async fn follow_redirect(
        &self,
        current: usize,
        leader: Option<u64>,
        backoff: Duration,
    ) -> Option<usize> {
        match leader.and_then(|id| self.index_of(id)) {
            Some(next) if next != current => Some(next),
            _ => self
                .pause(backoff)
                .await
                .then(|| (current + 1) % self.rotation),
        }
    }

    /// Where `retarget` sends the attempt after one at `current` named
    /// `hinted` (a node id).
    #[must_use]
    pub fn retarget(&self, retarget: Retarget, current: usize, hinted: Option<u64>) -> usize {
        let next = (current + 1) % self.rotation;
        match retarget {
            Retarget::FollowHint => hinted.and_then(|id| self.index_of(id)).unwrap_or(next),
            Retarget::SameNode => current,
            Retarget::NextNode => next,
        }
    }

    /// Race `request` against `timeout` and the shutdown: `fallback` when
    /// either comes first.
    async fn bounded<T>(
        &self,
        timeout: Duration,
        fallback: T,
        request: impl Future<Output = T>,
    ) -> T {
        moonpool_core::select! {
            result = request => result,
            _ = self.time.sleep(timeout) => fallback,
            () = self.shutdown.cancelled() => fallback,
        }
    }

    /// Sleep `duration`, unless the shutdown comes first; whether the sleep
    /// ran its course.
    async fn pause(&self, duration: Duration) -> bool {
        moonpool_core::select! {
            slept = self.time.sleep(duration) => slept.is_ok(),
            () = self.shutdown.cancelled() => false,
        }
    }

    // --- One attempt each: observed, unbounded (the caller bounds them). ---
    //
    // Each builds its request and reports it to the observer **when
    // called**, and returns the attempt's future: the caller alone fixes the
    // order calls leave in, and what races them.

    /// One `Write` of `request` to server `target`. With `listen`, stop
    /// listening after that long and report the attempt ambiguous (it is
    /// still sent).
    pub fn write_attempt(
        &self,
        target: usize,
        request: Write,
        listen: Option<Duration>,
    ) -> impl Future<Output = WriteOutcome> + use<P> {
        let node = self.node(target).clone();
        let time = self.time.clone();
        let observer = self.observer.clone();
        let token = observer.invoked(Attempted::Write(&request));
        async move {
            let call = node.write(&request);
            let outcome = match listen {
                Some(listen) => moonpool_core::select! {
                    response = call => WriteOutcome::judge(&response),
                    _ = time.sleep(listen) => WriteOutcome::Ambiguous,
                },
                None => WriteOutcome::judge(&call.await),
            };
            if let Some(token) = token {
                observer.answered(token, Answered::Write(&outcome));
            }
            outcome
        }
    }

    /// One `SetLeader(old → new)` on `journal`, asked of `target`.
    pub fn set_leader_attempt(
        &self,
        target: usize,
        journal: JournalIdentifier,
        new: LeaderUuid,
        old: Option<LeaderUuid>,
    ) -> impl Future<Output = SetLeaderOutcome> + use<P> {
        let node = self.node(target).clone();
        let observer = self.observer.clone();
        let request = SetLeader {
            journal: journal.journal.0,
            tenant: journal.tenant.0,
            new: Some(leader_uuid_to_proto(new)),
            old: old.map(leader_uuid_to_proto),
        };
        let token = observer.invoked(Attempted::SetLeader(&request));
        async move {
            let outcome = SetLeaderOutcome::judge(&node.set_leader(&request).await);
            if let Some(token) = token {
                observer.answered(token, Answered::SetLeader(&outcome));
            }
            outcome
        }
    }

    /// One `Read` asked of `target`.
    pub fn read_attempt(
        &self,
        target: usize,
        request: Read,
    ) -> impl Future<Output = ReadOutcome> + use<P> {
        let node = self.node(target).clone();
        let observer = self.observer.clone();
        let token = observer.invoked(Attempted::Read(&request));
        async move {
            let outcome = ReadOutcome::judge(node.read(&request).await);
            if let Some(token) = token {
                observer.answered(token, Answered::Read(&outcome));
            }
            outcome
        }
    }

    /// One fenced `Truncate` (#228), asked of `target`. Build `request` with
    /// [`Writer::truncate_request`] (the owner's own fence) or, as a
    /// deliberate misbehaviour, [`Writer::stale_truncate_request`].
    pub fn truncate_attempt(
        &self,
        target: usize,
        request: Truncate,
    ) -> impl Future<Output = TruncateOutcome> + use<P> {
        let node = self.node(target).clone();
        let observer = self.observer.clone();
        let token = observer.invoked(Attempted::Truncate(&request));
        async move {
            let outcome = TruncateOutcome::judge(&node.truncate(&request).await);
            if let Some(token) = token {
                observer.answered(token, Answered::Truncate(&outcome));
            }
            outcome
        }
    }

    // --- The policy: bounded, redirect-following, re-asking. ---

    /// Write `request`, starting at server `first`: follow redirects (at
    /// most `redirect_limit`, `redirect_backoff` apart) until a verdict,
    /// all inside one `request_timeout`. Every attempt is the identical
    /// write. An ambiguous outcome is reported as such — settle it with
    /// [`Client::resolve`].
    pub async fn write(&self, request: &Write, first: usize, options: WriteOptions) -> WriteReport {
        let deadline = self.time.now() + self.tunables.request_timeout;
        let mut server = first % self.servers.len();
        let mut redirects = 0;
        let mut busy = false;
        // The servers that answered `UnknownJournal`: a server that does not
        // host the journal says nothing about it, as a read treats it
        // (`ReadOutcome::UnknownJournal` only when every server says so), so
        // the write asks the next one, once each. Without it, a writer
        // whose first server stopped hosting the journal (a founding member
        // wiped before the cell formed, its address now an admitted
        // machine's) never reached the journal's leader again once it lost
        // its leader hint: witness seed 2897542392826386941 on 448be19, a
        // coordinator's `Placed` record unavailable for 30 s.
        let mut unknown = 0;
        let mut listen = options.abandon_first_after;
        loop {
            let remaining = deadline.saturating_sub(self.time.now());
            if remaining.is_zero() {
                return WriteReport {
                    outcome: WriteOutcome::Ambiguous,
                    server,
                    redirects,
                };
            }
            let attempt = self.write_attempt(server, request.clone(), listen.take());
            let outcome = self
                .bounded(remaining, WriteOutcome::Ambiguous, attempt)
                .await;
            match outcome {
                // Admission control (#360 (disk full)): the leader admits no
                // write now. The same write goes to the same leader again,
                // after a pause, until the deadline.
                WriteOutcome::Busy if self.time.now() < deadline => {
                    moonpool_assertions::reachable!("admission: a client retries a busy write");
                    self.observe_leader_at(server);
                    busy = true;
                    if !self.pause(self.tunables.redirect_backoff).await {
                        return WriteReport {
                            outcome: WriteOutcome::Busy,
                            server,
                            redirects,
                        };
                    }
                }
                WriteOutcome::UnknownJournal
                    if unknown + 1 < self.servers.len()
                        && self.retarget(options.retarget, server, None) != server
                        && self.time.now() < deadline =>
                {
                    moonpool_assertions::reachable!(
                        "client: a write asks another server past one that does not host the journal"
                    );
                    unknown += 1;
                    server = self.retarget(options.retarget, server, None);
                    if !self.pause(self.tunables.redirect_backoff).await {
                        return WriteReport {
                            outcome: WriteOutcome::Ambiguous,
                            server,
                            redirects,
                        };
                    }
                }
                WriteOutcome::Redirect { leader }
                    if !options.stop_at_redirect
                        && redirects < self.tunables.redirect_limit
                        && self.time.now() < deadline =>
                {
                    self.observe_leader(leader);
                    server = self.retarget(options.retarget, server, leader);
                    redirects += 1;
                    if !self.pause(self.tunables.redirect_backoff).await {
                        return WriteReport {
                            outcome: WriteOutcome::Ambiguous,
                            server,
                            redirects,
                        };
                    }
                }
                outcome => {
                    if busy && matches!(outcome, WriteOutcome::Written { .. }) {
                        moonpool_assertions::reachable!(
                            "admission: a write answered busy then succeeded"
                        );
                    }
                    match &outcome {
                        WriteOutcome::Redirect { leader } => self.observe_leader(*leader),
                        outcome if outcome.is_verdict() => self.observe_leader_at(server),
                        // No answer from the server believed to lead: the
                        // belief is dropped, so the caller's next write
                        // starts where *it* chooses. Kept, a hint naming a
                        // node that went down for good pinned every later
                        // write (and every re-send `resolve` makes) on it,
                        // each one ambiguous, and the writer never reached
                        // the new leader nor learned it was superseded
                        // (#224's hunt: witness 17857554070660782028, a
                        // recovery that never acked a write in 60 s).
                        WriteOutcome::Ambiguous if self.leader() == Some(server) => {
                            self.forget_leader();
                        }
                        _ => {}
                    }
                    return WriteReport {
                        outcome,
                        server,
                        redirects,
                    };
                }
            }
        }
    }

    /// Settle a write whose answer never came. First read the position
    /// back: a journal whose tail has not reached it under another leader
    /// proves the write fenced for good, and a page that holds exactly the
    /// write at the tail under its leader, at or above `led_from`, proves it
    /// written (#564). Otherwise re-send the
    /// identical write — the journal answers a write it holds from the log
    /// (`Duplicate`) and refuses one whose position another write took —
    /// up to `retry_budget` times, `retry_backoff` apart, each after a new
    /// read-back.
    ///
    /// The observer hears the settlement ([`CallObserver::resolving`],
    /// [`CallObserver::resolved`]): a write resolved not written is one a
    /// history checker holds to never landing in the leadership it was
    /// sent under (#444).
    ///
    /// `led_from` is a position from which the caller knows the request's
    /// uuid leads: the `next_seq` of a view that named it the leader
    /// ([`Writer::led_from`]). Only with it, and only for a request at or
    /// above it, does a read-back prove the write written; `None` leaves
    /// that proof to the re-sends.
    pub async fn resolve(
        &self,
        request: &Write,
        first: usize,
        retarget: Retarget,
        led_from: Option<u64>,
    ) -> ResolveReport {
        let token = self.observer.resolving(request);
        let report = self.settle(request, first, retarget, led_from).await;
        if let Some(token) = token {
            self.observer.resolved(token, &report);
        }
        report
    }

    /// [`Client::resolve`]'s read-backs and re-sends: a read-back before
    /// every re-send, so a first attempt that lands while the re-sends
    /// time out still settles (#564).
    async fn settle(
        &self,
        request: &Write,
        first: usize,
        retarget: Retarget,
        led_from: Option<u64>,
    ) -> ResolveReport {
        let mut server = first;
        for attempt in 0..self.tunables.retry_budget.max(1) {
            if let Some(resolution) = self
                .read_back(request, server, led_from, attempt == 0)
                .await
            {
                return ResolveReport {
                    resolution,
                    by_read_back: true,
                };
            }
            let report = self
                .write(
                    request,
                    server,
                    WriteOptions {
                        retarget,
                        ..WriteOptions::default()
                    },
                )
                .await;
            server = report.server;
            let leader = leader_uuid_from_proto(request.leader);
            let resolution = match report.outcome {
                WriteOutcome::Written { seq, count, .. } => Resolution::Written { seq, count },
                WriteOutcome::Truncated { state } => Resolution::Truncated { state },
                // Of the wrong mode (#241): a journal's mode never changes,
                // so the write is not in it and never will be.
                WriteOutcome::WrongMode { state } => Resolution::NotWritten { state },
                // Refused: the log does not hold this write at its
                // position. For good when another leader fenced it or
                // another write took the position; a write ahead of the
                // tail under the leader in force may still land.
                WriteOutcome::Refused { state }
                    if state.leader != Some(leader) || state.next_seq.0 > request.seq =>
                {
                    Resolution::NotWritten { state }
                }
                // Refused under the leader in force, ahead of the tail: it
                // may still land. A frontend that refused the re-send (an
                // expired token, say) says nothing about the first attempt.
                WriteOutcome::Refused { .. } | WriteOutcome::Denied(_) => Resolution::Unresolved,
                // A node whose limits refuse the re-send proposed nothing:
                // it says nothing about the first attempt, so ask another.
                WriteOutcome::Redirect { .. }
                | WriteOutcome::TooLarge { .. }
                | WriteOutcome::Busy
                | WriteOutcome::UnknownJournal
                | WriteOutcome::Malformed
                | WriteOutcome::Ambiguous => {
                    server = self.retarget(retarget, server, None);
                    if !self.pause(self.tunables.retry_backoff).await {
                        break;
                    }
                    continue;
                }
            };
            return ResolveReport {
                resolution,
                by_read_back: false,
            };
        }
        ResolveReport {
            resolution: Resolution::Unresolved,
            by_read_back: false,
        }
    }

    /// Read `request`'s position back, from server `first` on, and settle
    /// it when the page proves its fate; `None` when it does not.
    ///
    /// - Below the journal's floor: [`Resolution::Truncated`].
    /// - Not reached under another leader: [`Resolution::NotWritten`], for
    ///   good, since the fenced uuid never leads again.
    /// - Exactly the request's records at its position, at or above
    ///   `led_from`, ending at the journal's tail, read while the journal's
    ///   leader is still the request's uuid: [`Resolution::Written`]
    ///   (#564). The uuid led from `led_from` and leads still, and a uuid
    ///   never leads twice, so every position from `led_from` on was
    ///   written under it. A page shows no batch boundaries, so the tail
    ///   bounds the batch: nothing follows the records, so the journal
    ///   holds a write with this uuid and exactly these records at this
    ///   position, one the journal itself answers as this write
    ///   (`Duplicate`). A re-send
    ///   would take a new slot that may time out like the first attempt,
    ///   so without this a client whose deadline is shorter than the commit
    ///   path never settles. The bytes alone prove nothing (two writers'
    ///   one-byte records are equal), nor does the leader alone: a request
    ///   below `led_from` can find a predecessor's identical bytes (a
    ///   writer that wrote before its claim's answer came back, hunt seed
    ///   10942456551287199571). The position, the length, the records, the
    ///   leader, the tail and `led_from` must all match. And no other write
    ///   of this uuid may share the request's positions
    ///   ([`Writer::led_from`]): its own earlier batches can tile them with
    ///   the same bytes (a broken pipeline it rolled back behind), and the
    ///   journal refuses the request there.
    ///
    /// Both proofs rest on fresh uuids, the precondition the journal
    /// already trusts its clients with (`docs/architecture/02-data-plane.md` §2.3).
    ///
    /// The first read-back walks the server list ([`Client::read_any`]); a
    /// later one, before each further re-send, asks `first` alone within
    /// `read_timeout`, so the read-backs add at most one read timeout per
    /// re-send to a resolve.
    async fn read_back(
        &self,
        request: &Write,
        first: usize,
        led_from: Option<u64>,
        whole: bool,
    ) -> Option<Resolution> {
        let count = request.records.len() as u64;
        let leader = leader_uuid_from_proto(request.leader);
        let read = Read {
            journal: request.journal,
            tenant: request.tenant,
            from_seq: request.seq,
            limit: count.max(1),
            wait_ms: 0,
        };
        let back = if whole {
            self.read_any(&read, first).await.outcome
        } else {
            let attempt = self.read_attempt(first % self.servers.len(), read);
            self.bounded(self.tunables.read_timeout, ReadOutcome::Ambiguous, attempt)
                .await
        };
        let state = back.state()?;
        if state.first_seq.0 > request.seq {
            return Some(Resolution::Truncated { state });
        }
        if state.next_seq.0 <= request.seq && state.leader != Some(leader) {
            return Some(Resolution::NotWritten { state });
        }
        if let ReadOutcome::Page { from, records, .. } = &back
            && count > 0
            && led_from.is_some_and(|from| from <= request.seq)
            && *from == request.seq
            && state.next_seq.0 == request.seq + count
            && state.leader == Some(leader)
            && records.len() as u64 == count
            && *records == request.records
        {
            return Some(Resolution::Written {
                seq: request.seq,
                count,
            });
        }
        None
    }

    /// Read `request`, starting at server `first` and moving on through the
    /// whole server list while a server leaves it unserved, unanswered, or
    /// does not serve the journal — one attempt per server, each bounded by
    /// `read_timeout` plus the request's own `wait_ms`. A served answer is
    /// returned at once; [`ReadOutcome::UnknownJournal`] only when every
    /// server answered so.
    pub async fn read_any(&self, request: &Read, first: usize) -> ReadReport {
        let span = self.tunables.read_timeout + Duration::from_millis(request.wait_ms);
        let servers = self.servers.len();
        let mut server = first % servers;
        if servers > 1 && self.hint().silent == Some(server) {
            // A server that left the last read unanswered is asked last: a
            // founder that moved holds its journals until it registers
            // (#390), and every read asked of it first costs `read_timeout`.
            // Seed 410762996391560630 on 522c917: the operator's `init` and
            // the coordinator's directory reads each paid it per call, and
            // the resumed `init` missed its recovery-tail deadline.
            moonpool_assertions::reachable!("client: a read skips a silent server");
            server = (server + 1) % servers;
        }
        let mut attempts = 0_u64;
        // The last answer that was not "unknown journal", and how many
        // servers in a row answered so.
        let mut other = ReadOutcome::Ambiguous;
        let mut unknown_run = 0;
        loop {
            if self.shutdown.is_cancelled() {
                break;
            }
            let bound = if usize::try_from(attempts).unwrap_or(usize::MAX) >= servers {
                Duration::ZERO
            } else {
                span
            };
            if bound.is_zero() {
                break;
            }
            attempts += 1;
            let attempt = self.read_attempt(server, *request);
            let outcome = self.bounded(bound, ReadOutcome::Ambiguous, attempt).await;
            {
                let mut hint = self.hint.lock().unwrap_or_else(PoisonError::into_inner);
                if outcome == ReadOutcome::Ambiguous {
                    hint.silent = Some(server);
                } else if hint.silent == Some(server) {
                    hint.silent = None;
                }
            }
            if outcome.is_served() || matches!(outcome, ReadOutcome::Denied(_)) {
                return ReadReport {
                    outcome,
                    server,
                    attempts,
                };
            }
            if outcome == ReadOutcome::UnknownJournal {
                unknown_run += 1;
                if unknown_run >= servers {
                    break;
                }
            } else {
                unknown_run = 0;
                other = outcome;
            }
            server = (server + 1) % servers;
        }
        ReadReport {
            outcome: if unknown_run >= servers {
                ReadOutcome::UnknownJournal
            } else {
                other
            },
            server,
            attempts,
        }
    }

    /// Where `journal` stands, read from server `first` on
    /// ([`Client::read_any`]): its state, `None` when no server served it.
    pub async fn journal_state(
        &self,
        journal: JournalIdentifier,
        first: usize,
    ) -> Option<JournalView> {
        self.read_any(&state_read(journal), first)
            .await
            .outcome
            .state()
    }

    /// Claim `journal` for `uuid` (#204, #241): read where it stands — from
    /// server `first` on, see [`Client::read_any`] — then `SetLeader`
    /// against the leader read, asked of the server that served the
    /// read, the one just proven reachable (asked of `first` instead, a
    /// claim whose read had moved past an unreachable `first` sent its
    /// `SetLeader` down the same dead link and came back ambiguous at
    /// once).
    ///
    /// A well-behaved writer leads under a uuid for one term only, so a read
    /// already naming `uuid` is a claim that won and whose answer was lost:
    /// it is adopted as
    /// [`ClaimOutcome::Owned`]. A writer that wants a new term asks with a
    /// new uuid ([`Writer::claim`]). A uuid that already led is re-claimed
    /// with [`Client::reclaim`], never with this call.
    pub async fn claim(
        &self,
        journal: JournalIdentifier,
        uuid: LeaderUuid,
        first: usize,
    ) -> ClaimOutcome {
        self.claim_as(journal, uuid, ClaimMode::Fresh, first).await
    }

    /// Claim `journal` for `uuid` over `old` only (#653 (election term to
    /// installed leader)): as [`Client::claim`], but a read naming another
    /// leader than `old` is [`ClaimOutcome::Lost`] and sends no
    /// `SetLeader`. A caller that judged `old` safe to replace (an elected
    /// coordinator that read its election after it read `old`) never
    /// installs over a leader it did not judge.
    pub async fn claim_over(
        &self,
        journal: JournalIdentifier,
        uuid: LeaderUuid,
        old: Option<LeaderUuid>,
        first: usize,
    ) -> ClaimOutcome {
        self.claim_as(journal, uuid, ClaimMode::Over(old), first)
            .await
    }

    /// Re-claim `journal` for `uuid`, a uuid that already led (#241, #564):
    /// a read naming it is [`ClaimOutcome::Owned`], as in
    /// [`Client::claim`]; a read naming any other leader is
    /// [`ClaimOutcome::Lost`] and sends no `SetLeader`. Asking again would
    /// make `uuid` lead a second term, which breaks the read-back proof of
    /// [`Client::resolve`] (a uuid leads one term only). The writer then
    /// learns it was superseded and claims under a fresh uuid.
    pub async fn reclaim(
        &self,
        journal: JournalIdentifier,
        uuid: LeaderUuid,
        first: usize,
    ) -> ClaimOutcome {
        self.claim_as(journal, uuid, ClaimMode::Led, first).await
    }

    /// [`Client::claim`], [`Client::reclaim`] or [`Client::claim_over`], by
    /// `mode`.
    async fn claim_as(
        &self,
        journal: JournalIdentifier,
        uuid: LeaderUuid,
        mode: ClaimMode,
        first: usize,
    ) -> ClaimOutcome {
        let report = self.read_any(&state_read(journal), first).await;
        let state = match report.outcome {
            ReadOutcome::Page { state, .. } | ReadOutcome::Truncated { state } => state,
            ReadOutcome::UnknownJournal => return ClaimOutcome::UnknownJournal,
            ReadOutcome::Denied(denial) => return ClaimOutcome::Denied(denial),
            ReadOutcome::Malformed => return ClaimOutcome::Malformed,
            ReadOutcome::Unserved | ReadOutcome::Ambiguous => return ClaimOutcome::Unread,
        };
        if state.leader == Some(uuid) {
            return ClaimOutcome::Owned { state };
        }
        match mode {
            // Superseded: `uuid` led a term and another leader followed it.
            ClaimMode::Led => return ClaimOutcome::Lost { state },
            // Another leader than the one the caller judged (#653).
            ClaimMode::Over(old) if old != state.leader => {
                moonpool_assertions::reachable!(
                    "client: a claim over a judged leader finds another"
                );
                return ClaimOutcome::Lost { state };
            }
            ClaimMode::Fresh | ClaimMode::Over(_) => {}
        }
        self.set_leader(journal, uuid, state.leader, report.server)
            .await
            .into()
    }

    /// `SetLeader(old → new)` on `journal`, starting at server
    /// `first`: a redirect naming another server is followed, one naming
    /// none is re-asked of the next server `redirect_backoff` later, for at
    /// most `redirect_limit` redirects, all inside one `request_timeout`.
    /// Only a redirect is re-asked — a node that redirects proposed
    /// nothing — so this never mints two terms.
    pub async fn set_leader(
        &self,
        journal: JournalIdentifier,
        new: LeaderUuid,
        old: Option<LeaderUuid>,
        first: usize,
    ) -> SetLeaderOutcome {
        let deadline = self.time.now() + self.tunables.request_timeout;
        let mut server = first % self.servers.len();
        let mut redirects = 0;
        loop {
            let remaining = deadline.saturating_sub(self.time.now());
            if remaining.is_zero() {
                return SetLeaderOutcome::Ambiguous;
            }
            let ask = self.set_leader_attempt(server, journal, new, old);
            let outcome = self
                .bounded(remaining, SetLeaderOutcome::Ambiguous, ask)
                .await;
            match outcome {
                SetLeaderOutcome::Redirect { leader }
                    if redirects < self.tunables.redirect_limit =>
                {
                    self.observe_leader(leader);
                    redirects += 1;
                    let Some(next) = self
                        .follow_redirect(server, leader, self.tunables.redirect_backoff)
                        .await
                    else {
                        return outcome;
                    };
                    server = next;
                }
                SetLeaderOutcome::Won { .. } | SetLeaderOutcome::Lost { .. } => {
                    self.observe_leader_at(server);
                    return outcome;
                }
                SetLeaderOutcome::Redirect { leader } => {
                    self.observe_leader(leader);
                    return outcome;
                }
                outcome => return outcome,
            }
        }
    }

    /// Truncate `journal` below `up_to` (a caller's fence: everything it
    /// still needs is at or past `up_to`), starting at server `target`: a
    /// redirect naming another server is followed, one naming none (or the
    /// same server) is re-asked of the next server `retry_backoff` later,
    /// for at most `redirect_limit` asks in all.
    pub async fn truncate(&self, request: &Truncate, target: usize) -> TruncateOutcome {
        let mut server = target % self.servers.len();
        for _ in 0..self.tunables.redirect_limit.max(1) {
            let attempt = self.truncate_attempt(server, *request);
            let outcome = self
                .bounded(
                    self.tunables.request_timeout,
                    TruncateOutcome::Ambiguous,
                    attempt,
                )
                .await;
            match outcome {
                TruncateOutcome::Redirect { leader } => {
                    let Some(next) = self
                        .follow_redirect(server, leader, self.tunables.retry_backoff)
                        .await
                    else {
                        return TruncateOutcome::Redirect { leader };
                    };
                    server = next;
                }
                TruncateOutcome::Applied { state } => {
                    self.observe_leader_at(server);
                    return TruncateOutcome::Applied { state };
                }
                TruncateOutcome::Refused { state } => {
                    self.observe_leader_at(server);
                    return TruncateOutcome::Refused { state };
                }
                terminal => return terminal,
            }
        }
        TruncateOutcome::Ambiguous
    }

    /// Ask for the acceptor set `members` of `journal` under
    /// `quorum_system`, starting at server `target`: a `not_leader` redirect
    /// is followed, an `unsettled` leader is re-asked `retry_backoff` later,
    /// every other refusal is the answer; at most `retry_budget` asks. A
    /// machine serves many journals, so the request names the one it
    /// changes (#474); no journal has a default (#439 (operator calls name
    /// their journal)).
    pub async fn reconfigure(
        &self,
        journal: JournalIdentifier,
        members: &[u64],
        quorum_system: QuorumSystem,
        target: usize,
    ) -> ReconfigureOutcome {
        self.reconfigure_wire(journal, members, (Vec::new(), 0), quorum_system, target)
            .await
    }

    /// [`Client::reconfigure`] onto a composed `config`, its failure
    /// domains and its cell included (#215 (zones)): what a coordinator's
    /// move and the simulation's composer send.
    pub async fn reconfigure_to(
        &self,
        journal: JournalIdentifier,
        config: &paros_core::AcceptorConfig,
        target: usize,
    ) -> ReconfigureOutcome {
        let members: Vec<u64> = config.members().iter().map(|n| n.0).collect();
        let domains = config
            .domains()
            .iter()
            .map(paros_wire::domain_to_proto)
            .collect();
        self.reconfigure_wire(
            journal,
            &members,
            (domains, config.cell()),
            config.quorum_system(),
            target,
        )
        .await
    }

    /// The reconfiguration as the wire carries it: `members` with their
    /// labels (empty, or one per member) and the configuration's cell (`0`:
    /// none).
    async fn reconfigure_wire(
        &self,
        journal: JournalIdentifier,
        members: &[u64],
        (domains, cell_id): (Vec<paros_wire::common::FailureDomain>, u64),
        quorum_system: QuorumSystem,
        target: usize,
    ) -> ReconfigureOutcome {
        assert!(
            domains.is_empty() || domains.len() == members.len(),
            "a reconfiguration labels every member or none"
        );
        let mut server = target % self.servers.len();
        let wire = quorum_system_to_proto(quorum_system);
        for _ in 0..self.tunables.retry_budget.max(1) {
            let request = Reconfigure {
                members: members.to_vec(),
                quorum_system: wire.quorum_system,
                phase1_quorum: wire.phase1_quorum,
                phase2_quorum: wire.phase2_quorum,
                rows: wire.rows,
                cols: wire.cols,
                tenant: journal.tenant.0,
                journal: journal.journal.0,
                domains: domains.clone(),
                cell_id,
            };
            let node = self.node(server).clone();
            let attempt =
                async move { ReconfigureOutcome::judge(node.reconfigure(&request).await) };
            let outcome = self
                .bounded(
                    self.tunables.request_timeout,
                    ReconfigureOutcome::Ambiguous,
                    attempt,
                )
                .await;
            match outcome {
                ReconfigureOutcome::NotLeader { leader: Some(next) } => {
                    let Some(next) = self.index_of(next) else {
                        return outcome;
                    };
                    server = next;
                }
                ReconfigureOutcome::Refused {
                    refusal: paros_core::ReconfigureRefusal::Unsettled,
                    ..
                } => {
                    if !self.pause(self.tunables.retry_backoff).await {
                        return outcome;
                    }
                }
                ReconfigureOutcome::Started { leader, .. } => {
                    self.observe_leader(leader);
                    return outcome;
                }
                outcome => return outcome,
            }
        }
        ReconfigureOutcome::Ambiguous
    }

    /// Ask server `target` to drive a handover of `tenant`'s matchmaker set
    /// onto `members`: a busy reconfigurer is re-asked `retry_backoff`
    /// later, every other refusal is the answer; at most `retry_budget`
    /// asks. A node hosts many tenants' journals, so the request names the
    /// set it hands over (#501 (matchmaker sets off the founders)); no
    /// tenant has a default (#439 (operator calls name their journal)).
    pub async fn reconfigure_matchmakers(
        &self,
        tenant: TenantId,
        members: &[u64],
        target: usize,
    ) -> ReconfigureMatchmakersOutcome {
        let node = self.node(target).clone();
        for _ in 0..self.tunables.retry_budget.max(1) {
            let request = ReconfigureMatchmakers {
                members: members.to_vec(),
                tenant: tenant.0,
            };
            let node = node.clone();
            let attempt = async move {
                ReconfigureMatchmakersOutcome::judge(node.reconfigure_matchmakers(&request).await)
            };
            let outcome = self
                .bounded(
                    self.tunables.request_timeout,
                    ReconfigureMatchmakersOutcome::Ambiguous,
                    attempt,
                )
                .await;
            match outcome {
                ReconfigureMatchmakersOutcome::Refused(MatchmakersRefusal::Busy) => {
                    if !self.pause(self.tunables.retry_backoff).await {
                        return outcome;
                    }
                }
                outcome => return outcome,
            }
        }
        ReconfigureMatchmakersOutcome::Ambiguous
    }

    /// One bounded `Inspect` of `journal` on server `target`; `None`
    /// without an answer, and on a refusal (an unset identifier, a journal not
    /// live there): no identifier has a default (§3.8, #243).
    pub async fn inspect(&self, target: usize, journal: JournalIdentifier) -> Option<InspectReply> {
        let node = self.node(target).clone();
        let probe = async move { node.inspect_journal(journal).await.ok() };
        self.bounded(self.tunables.request_timeout, None, probe)
            .await
            .filter(|reply| reply.refusal.is_empty())
    }

    /// One bounded node-only `Inspect` of server `target` (#243): its id,
    /// its cell and the control journals' identifiers; `None` without an answer.
    pub async fn inspect_node(&self, target: usize) -> Option<InspectReply> {
        let node = self.node(target).clone();
        let probe = async move { node.inspect_node().await.ok() };
        self.bounded(self.tunables.request_timeout, None, probe)
            .await
            .filter(|reply| reply.refusal.is_empty())
    }

    /// One bounded scrub page of `request`'s journal at server `target`
    /// (#362 (background scrubbing)); `None` without an answer or on a
    /// refusal.
    pub async fn scrub(
        &self,
        target: usize,
        request: paros_wire::ScrubRequest,
    ) -> Option<paros_wire::ScrubReply> {
        let node = self.node(target).clone();
        let probe = async move { node.scrub(&request).await.ok() };
        self.bounded(self.tunables.request_timeout, None, probe)
            .await
            .filter(|reply| reply.refusal.is_empty())
    }

    /// One bounded `Retire` of server `target`, carrying the GC watermark
    /// the operator read from a leader's `Inspect` (the RPC's evidence).
    pub async fn retire(&self, target: usize, request: RetireRequest) -> RetireOutcome {
        let node = self.node(target).clone();
        let attempt = async move { RetireOutcome::judge(node.retire(&request).await) };
        self.bounded(
            self.tunables.request_timeout,
            RetireOutcome::Ambiguous,
            attempt,
        )
        .await
    }
}

/// The read that asks where `journal` stands: one record from position 0,
/// no wait — answered with the journal state whatever the log holds.
pub(crate) fn state_read(journal: JournalIdentifier) -> Read {
    Read {
        journal: journal.journal.0,
        tenant: journal.tenant.0,
        from_seq: 0,
        limit: 1,
        wait_ms: 0,
    }
}

/// How [`Client::claim_as`] treats the leader its read names.
#[derive(Clone, Copy, Debug)]
enum ClaimMode {
    /// Replace any leader ([`Client::claim`]).
    Fresh,
    /// The uuid led already: replace none ([`Client::reclaim`]).
    Led,
    /// Replace only this leader ([`Client::claim_over`], #653).
    Over(Option<LeaderUuid>),
}
