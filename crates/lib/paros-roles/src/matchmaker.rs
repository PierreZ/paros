//! The provider-generic **matchmaker driver** — the I/O layer that owns a
//! sans-IO [`paros_core::Matchmaker`], the twin of [`run_node`](crate::node::run_node)
//! for the registry role.
//!
//! Written once over `P: ParosProviders`, so the *same* loop runs in
//! production and deterministic simulation; the harness adapts a moonpool
//! `Process` to it exactly as it adapts the node. The loop serves the
//! matchmaker RPC contract, feeds each request into the core, and drains every
//! [`MatchmakerReady`](paros_core::MatchmakerReady) in **persist → fsync →
//! reply** order: a `Registered` reply leaves only once its registration is
//! durable, the registry's version of the acceptor's persist-before-`Promise`
//! rule — and the same order carries a freeze (`StopAck`), a pending
//! bootstrap, a decree promise or vote, and an activation (#125). The registry
//! is read back through the core's [`RegistryStorage`](paros_core::RegistryStorage)
//! port and written through [`MatchmakerStorage`] — the node's `Storage` /
//! `LogStorage` split, mirrored so the log's CTRL recovery applies to the
//! registry (see [`storage`]).
//!
//! **One process hosts one matchmaker set per tenant** (#190): every set is
//! its own [`Matchmaker`] over its own store, and keeps one registry per
//! journal of its tenant. A request names its tenant and its journal; the
//! host routes on the tenant, the set on the journal. Every journal reports
//! to its own audit port, so a harness judges each journal's registry alone.
//!
//! A cell deployed without matchmakers never runs this loop; the node
//! driver does not know it exists.

use std::collections::BTreeMap;

use moonpool_buggify::hint::Strike;
use paros_core::{
    GcAck, GcOutcome, GcRequest, JournalId, JournalIdentifier, Limits, MatchOutcome, MatchReply,
    Matchmaker, MatchmakerConfig, MatchmakerId, MatchmakerSet, MatchmakerWriteOp, ReconfigureReply,
    TenantId,
};
use paros_net::runtime::ParosProviders;
use tokio_util::sync::CancellationToken;

use crate::node::edge::{MatchmakerInbox, RpcEdge};
use crate::node::reply::Reply;
use crate::node::reply::match_answer;
use paros_net::transport::Transport;
use paros_observe::audit::{Audit, HistoryPage, StorageFaultDecision};
use paros_observe::events::{config_hash, reconfigure_kind, reconfigure_reply_kind};
use paros_storage::StorageError;
use paros_storage::boot::{BootKind, BootRefusal, RunError};
use paros_tunables::DriverTunables;

use paros_wire::Addressed;

pub use paros_storage::matchmaker::{
    MatchmakerStorage, MemMatchmakerStorage, Registrations, matchmaker_storage_contract_suite,
};

/// The audit ports of one hosted set: one per journal of its tenant, made by
/// the caller's factory on first use (a journal the set learns from a
/// request gets its port then).
struct Ports<'f, A, F> {
    tenant: TenantId,
    factory: &'f F,
    by_journal: BTreeMap<JournalId, A>,
    /// The tenant's own port (the unset journal, as the set's store is
    /// stamped): a set-wide fact's only reader while the set knows no
    /// journal yet.
    unjournaled: Option<A>,
}

impl<'f, A, F: Fn(JournalIdentifier) -> A> Ports<'f, A, F> {
    /// The ports of `tenant`'s set, one per journal in `journals` at once.
    fn new(tenant: TenantId, journals: &[JournalId], factory: &'f F) -> Self {
        let mut ports = Self {
            tenant,
            factory,
            by_journal: BTreeMap::new(),
            unjournaled: None,
        };
        for journal in journals {
            ports.port(*journal);
        }
        assert!(
            journals
                .iter()
                .all(|journal| ports.by_journal.contains_key(journal)),
            "a hosted set has a port per journal it names"
        );
        assert!(
            ports.by_journal.len() <= journals.len(),
            "a hosted set makes no port it was not asked for"
        );
        ports
    }

    /// `journal`'s port, made on first use.
    fn port(&mut self, journal: JournalId) -> &A {
        let tenant = self.tenant;
        let factory = self.factory;
        self.by_journal
            .entry(journal)
            .or_insert_with(|| factory(JournalIdentifier::new(tenant, journal)))
    }
}

impl<A, F: Fn(JournalIdentifier) -> A> Ports<'_, A, F> {
    /// The ports a set-wide fact reaches: every journal's, or the tenant's
    /// own while the set knows no journal yet.
    fn set_wide(&mut self) -> impl Iterator<Item = &A> {
        if self.by_journal.is_empty() && self.unjournaled.is_none() {
            self.unjournaled = Some((self.factory)(JournalIdentifier::new(
                self.tenant,
                JournalId::UNSET,
            )));
        }
        let tenant = if self.by_journal.is_empty() {
            self.unjournaled.as_ref()
        } else {
            None
        };
        self.by_journal.values().chain(tenant)
    }
}

impl<A, F> Ports<'_, A, F> {
    /// Every port this set has, in journal order.
    fn all(&self) -> impl Iterator<Item = &A> {
        self.by_journal.values()
    }
}

/// Map a [`StorageError`] into the driver's deliberate crash decision, typed on
/// every audit port of the set at the instant it is made (the matchmaker twin
/// of the node driver's storage-fault crash).
fn storage_fault_crash<A: Audit, F>(
    ports: &Ports<'_, A, F>,
    id: MatchmakerId,
    e: StorageError,
) -> RunError {
    for audit in ports.all() {
        audit.matchmaker_storage_fault(id, &e, StorageFaultDecision::Crash);
    }
    tracing::warn!(matchmaker = id.0, tenant = ports.tenant.0, error = %e, decision = "crash", "matchmaker_storage_fault");
    RunError::Storage(e)
}

/// #183: judge the operator's claim against the registry's format marker,
/// before the core reads a byte — the matchmaker twin of the node driver's
/// check (#147). An empty-but-openable registry is indistinguishable from a
/// first boot to [`Matchmaker::new`], and a matchmaker that rejoined on one
/// would answer a matchmaking quorum as if it had never seen a
/// registration it once acknowledged: a candidate whose quorum met it and
/// one peer that also missed the record would skip the Phase 1 that record
/// demands. So the refusal happens here, on the claim. A first boot formats
/// the registry durably first — the marker lands no later than the first
/// registration, the ordering the refusal relies on. And, #207, a registry
/// formatted under another [`MatchmakerConfig`] — another identity, another
/// bootstrap set — is refused as [`BootRefusal::ConfigMismatch`].
///
/// Load-bearing, measured: with the `Amnesia` arm removed, a 3,000-seed
/// `sim-paros-hunt main` went red on 4 seeds — "matchmaker: a restart
/// recovers every durable registration" and "a recovered gc watermark never
/// regresses" (a wiped matchmaker rebooted on an empty registry; witness
/// 5299901180798451264 on that build, cited, not pinned) — and the same
/// 3,000 seeds are green with it.
///
/// # Errors
///
/// [`RunError::Refused`] when the claim and the marker disagree (nothing was
/// written); [`RunError::Storage`] when formatting the registry failed.
#[tracing::instrument(level = "debug", skip_all, fields(matchmaker = config.id.0))]
async fn check_format_marker<S: MatchmakerStorage, A: Audit, F>(
    storage: &mut S,
    boot: BootKind,
    config: &MatchmakerConfig,
    ports: &Ports<'_, A, F>,
) -> Result<(), RunError> {
    let id = config.id;
    let refusal = match (boot, storage.formatted_config()) {
        (BootKind::ExistingMember, Some(formatted)) if formatted == *config => return Ok(()),
        (BootKind::ExistingMember, Some(formatted)) => {
            tracing::error!(
                matchmaker = id.0,
                formatted = ?formatted,
                operator = ?config,
                "matchmaker_boot_config_mismatch"
            );
            BootRefusal::ConfigMismatch
        }
        (BootKind::FirstBoot, None) => {
            storage
                .format(config)
                .await
                .map_err(|e| storage_fault_crash(ports, id, e))?;
            storage
                .sync()
                .await
                .map_err(|e| storage_fault_crash(ports, id, e))?;
            tracing::info!(matchmaker = id.0, "matchmaker_store_formatted");
            return Ok(());
        }
        (BootKind::ExistingMember, None) => BootRefusal::Amnesia,
        (BootKind::FirstBoot, Some(_)) => BootRefusal::AlreadyFormatted,
    };
    for audit in ports.all() {
        audit.matchmaker_boot_refused(id, refusal);
    }
    tracing::warn!(
        matchmaker = id.0,
        refusal = refusal.label(),
        "matchmaker_boot_refused"
    );
    Err(RunError::Refused(refusal))
}

/// One drained batch: the replies the caller may now send.
struct Drained {
    replies: Vec<MatchReply>,
    reconfigure_replies: Vec<ReconfigureReply>,
}

/// Run the [`MatchmakerReady`](paros_core::MatchmakerReady) handshake once:
/// persist the batch's writes, fsync them, report them, and hand back the
/// replies the caller may now send. The two hints sit exactly where a
/// real crash would matter: before the fsync (the batch is lost whole, no
/// reply was sent) and after it (the batch is durable, the reply never
/// leaves).
///
/// The order — writes, fsync, *then* replies — is the persist-before-reply
/// rule (invariant 1 of #119): a `Registered` reply that left before its
/// registration reached the disk would, across a crash at the seam between
/// the two, name a configuration the restarted matchmaker no longer holds —
/// a later leader's history would then silently omit it. The audit judges the
/// rule at the reply (`match_replied` must find the registration already
/// folded durable) and again at every restart (`matchmaker_recovered` must
/// read back every durable registration), and the two hints below are the
/// moments the simulation strikes to make both crash windows likely. The same
/// ordering covers the generation writes of #125: a `StopAck` leaves only
/// once the freeze is durable, a bootstrap ack only once the pending record
/// is, a decree promise or vote only once the decree record is.
#[tracing::instrument(level = "trace", skip_all, fields(matchmaker = seat.matchmaker.id().0, tenant = seat.ports.tenant.0))]
async fn drain<S, A, F>(seat: &mut Seat<'_, S, A, F>) -> Result<Drained, RunError>
where
    S: MatchmakerStorage,
    A: Audit,
    F: Fn(JournalIdentifier) -> A,
{
    let Seat {
        matchmaker,
        storage,
        ports,
    } = seat;
    let id = matchmaker.id();
    let ready = matchmaker.ready();
    let writes = ready.writes().to_vec();
    let replies = ready.replies().to_vec();
    let reconfigure_replies = ready.reconfigure_replies().to_vec();
    ready.advance();

    // What this batch commits is in flight until its sync returns: every
    // journal it writes has its port first, an install's included. A
    // journal this incarnation holds no registration of has no port at
    // boot; unported, it never heard an install was in flight, and a kill
    // inside its commit read back a registration its audit never saw. Seed
    // 12338602098732519771 (recipe 1353357:2487056085033603764) on fba843a.
    if !writes.is_empty() {
        for op in &writes {
            match op {
                MatchmakerWriteOp::Register { journal, .. } => {
                    ports.port(*journal);
                }
                MatchmakerWriteOp::InstallRegistry {
                    scalars,
                    registrations,
                } => {
                    for journal in registrations.keys().chain(scalars.journals.keys()) {
                        ports.port(*journal);
                    }
                }
                _ => {}
            }
        }
        assert!(
            writes.iter().all(|op| match op {
                MatchmakerWriteOp::InstallRegistry {
                    scalars,
                    registrations,
                } => registrations
                    .keys()
                    .chain(scalars.journals.keys())
                    .all(|journal| ports.by_journal.contains_key(journal)),
                _ => true,
            }),
            "an install's journals each have a port before it is in flight"
        );
        for audit in ports.set_wide() {
            audit.matchmaker_in_flight(id, Some(&writes));
        }
    }
    // 1. Persist every write, in order.
    for op in &writes {
        let staged = match op {
            MatchmakerWriteOp::Register {
                journal,
                ballot,
                registration,
            } => storage.register(*journal, *ballot, registration).await,
            MatchmakerWriteOp::SetGcWatermark { journal, watermark } => {
                storage.set_gc_watermark(*journal, *watermark).await
            }
            MatchmakerWriteOp::SetScalars(scalars) => storage.set_scalars(scalars).await,
            MatchmakerWriteOp::InstallRegistry {
                scalars,
                registrations,
            } => storage.install_registry(scalars, registrations).await,
        };
        staged.map_err(|e| storage_fault_crash(ports, id, e))?;
    }
    if !writes.is_empty() {
        // Hint: staged but not flushed. The batch dies whole, and no reply
        // was handed out yet. A matchmaker drains a batch only on a round
        // change, a handful per run, so the rate is an order above the node
        // hints' and still crashes only a few registrations per seed.
        let hinted = moonpool_buggify::hint!("registration staged, not synced", 0.15);
        if hinted.strike() == Strike::Killed {
            moonpool_assertions::reachable!(
                "matchmaker: the driver crashes before syncing a registration"
            );
        }
        hinted.await;
        storage
            .sync()
            .await
            .map_err(|e| storage_fault_crash(ports, id, e))?;
        for audit in ports.set_wide() {
            audit.matchmaker_in_flight(id, None);
        }
        surface_registry_writes(&writes, id, ports);
    }
    // 2. Hint: durable, but the reply has not left. Only meaningful when
    //    there is a reply to lose. This is the persist-before-reply moment:
    //    with the order swapped (reply, then fsync) a crash here lets a
    //    `Registered` reply escape for a ballot the restarted matchmaker no
    //    longer holds, the registry's un-promise.
    if !replies.is_empty() || !reconfigure_replies.is_empty() {
        let hinted = moonpool_buggify::hint!("registration durable, reply not sent", 0.15);
        if hinted.strike() == Strike::Killed {
            moonpool_assertions::reachable!(
                "matchmaker: the driver crashes after syncing and before replying"
            );
        }
        hinted.await;
    }
    Ok(Drained {
        replies,
        reconfigure_replies,
    })
}

/// Report a flushed batch's durable registry state — one audit callback and
/// one tracing event per op. Split out of [`drain`] exactly as the node
/// driver splits `persist_writes` / `surface_persisted`: the staging half and
/// the reporting half each stay readable, both walk `writes` in order, and
/// the reports come strictly after the fsync so they never claim a write the
/// staged-not-synced hint then discards.
#[tracing::instrument(level = "trace", skip_all, fields(matchmaker = id.0))]
fn surface_registry_writes<A: Audit, F: Fn(JournalIdentifier) -> A>(
    writes: &[MatchmakerWriteOp],
    id: MatchmakerId,
    ports: &mut Ports<'_, A, F>,
) {
    for op in writes {
        match op {
            MatchmakerWriteOp::Register {
                journal,
                ballot,
                registration,
            } => {
                ports
                    .port(*journal)
                    .match_registered(id, *ballot, registration);
                tracing::info!(
                    matchmaker = id.0,
                    journal = journal.0,
                    round = ballot.round,
                    bnode = ballot.node.0,
                    members = registration.config.members().len() as u64,
                    reconfiguration = registration.kind.is_reconfiguration(),
                    config = config_hash(&registration.config),
                    "match_registered"
                );
            }
            MatchmakerWriteOp::SetGcWatermark { journal, watermark } => {
                ports.port(*journal).gc_watermark_raised(id, *watermark);
                tracing::info!(
                    matchmaker = id.0,
                    journal = journal.0,
                    round = watermark.round,
                    bnode = watermark.node.0,
                    "gc_watermark_raised"
                );
            }
            MatchmakerWriteOp::SetScalars(scalars) => {
                for audit in ports.set_wide() {
                    audit.matchmaker_scalars_persisted(id, scalars);
                }
                tracing::info!(
                    matchmaker = id.0,
                    generation = scalars.generation.0,
                    phase = ?scalars.phase,
                    successor = scalars.successor.as_ref().map_or(0, |s| s.generation.0),
                    pending = scalars.pending.len() as u64,
                    "matchmaker_scalars_persisted"
                );
            }
            MatchmakerWriteOp::InstallRegistry {
                scalars,
                registrations,
            } => {
                // The set the *op* installs, not whatever the live
                // handle happens to hold: the two agree today, and a
                // report that reads the handle would quietly start
                // describing a later generation the moment they do not.
                // Every journal the set reports to hears its own piece: a
                // journal the reconstruction carries nothing for installs an
                // empty registry at its own floor.
                let set = MatchmakerSet::new(scalars.generation, scalars.members.clone());
                let empty = BTreeMap::new();
                for journal in registrations.keys().chain(scalars.journals.keys()) {
                    ports.port(*journal);
                }
                if ports.by_journal.is_empty() {
                    let unset = JournalId::UNSET;
                    for audit in ports.set_wide() {
                        audit.matchmaker_activated(
                            id,
                            &set,
                            scalars.gc_watermark(unset),
                            scalars.effective(unset),
                            &empty,
                        );
                    }
                }
                for (journal, audit) in &ports.by_journal {
                    let registry = registrations.get(journal).unwrap_or(&empty);
                    audit.matchmaker_activated(
                        id,
                        &set,
                        scalars.gc_watermark(*journal),
                        scalars.effective(*journal),
                        registry,
                    );
                }
                tracing::info!(
                    matchmaker = id.0,
                    tenant = ports.tenant.0,
                    generation = set.generation.0,
                    members = set.members().len() as u64,
                    journals = registrations.len() as u64,
                    registrations = registrations.values().map(BTreeMap::len).sum::<usize>() as u64,
                    "matchmaker_activated"
                );
            }
        }
    }
}

/// Report one reply at the instant it leaves, from a matchmaker whose pages
/// carry at most `page_limit` registrations (#338).
fn report_reply<A: Audit>(audit: &A, reply: &MatchReply, page_limit: usize) {
    let id = reply.matchmaker;
    let journal = reply.journal.0;
    match &reply.outcome {
        MatchOutcome::Registered {
            from_ballot,
            history,
            next_from_ballot,
            gc_watermark,
            effective,
        } => {
            audit.match_replied(
                id,
                reply.to,
                reply.ballot,
                reply.generation.0,
                &HistoryPage {
                    from_ballot: *from_ballot,
                    history,
                    next_from_ballot: *next_from_ballot,
                    gc_watermark: *gc_watermark,
                    effective: effective.as_ref(),
                    page_limit,
                },
            );
            tracing::info!(
                matchmaker = id.0,
                journal,
                to = reply.to.0,
                round = reply.ballot.round,
                bnode = reply.ballot.node.0,
                generation = reply.generation.0,
                from_round = from_ballot.round,
                history = history.len() as u64,
                next_round = next_from_ballot.map_or(0, |b| b.round),
                watermark_round = gc_watermark.round,
                "match_replied"
            );
        }
        MatchOutcome::Probed { effective } => {
            audit.match_probed(
                id,
                reply.to,
                reply.ballot,
                reply.generation.0,
                effective.as_ref(),
            );
            tracing::info!(
                matchmaker = id.0,
                journal,
                to = reply.to.0,
                round = reply.ballot.round,
                bnode = reply.ballot.node.0,
                generation = reply.generation.0,
                effective_round = effective.as_ref().map_or(0, |(b, _)| b.round),
                "match_probed"
            );
        }
        MatchOutcome::Refused(refusal) => {
            audit.match_refused(id, reply.to, reply.ballot, refusal.clone());
            tracing::info!(
                matchmaker = id.0,
                journal,
                to = reply.to.0,
                round = reply.ballot.round,
                bnode = reply.ballot.node.0,
                generation = reply.generation.0,
                reason = ?refusal,
                "match_refused"
            );
        }
    }
}

/// Where a matchmaker host finds a set it does not host yet (#190): a
/// static deployment hosts only what it booted ([`NoOpener`]); a machine
/// provisions a tenant's set at the first request that names it.
pub trait SetOpener {
    /// The store a set opens on.
    type Storage: MatchmakerStorage;

    /// The set of `tenant`, provisioned, with the claim about its store; or
    /// `None` when this host does not serve `tenant` (its request is
    /// dropped, and the requester re-asks).
    fn open(
        &mut self,
        tenant: TenantId,
    ) -> impl std::future::Future<Output = Option<HostedSet<Self::Storage>>>;
}

/// The opener of a static deployment: it hosts what it booted, nothing more.
struct NoOpener<S>(std::marker::PhantomData<fn() -> S>);

impl<S: MatchmakerStorage> SetOpener for NoOpener<S> {
    type Storage = S;

    async fn open(&mut self, _tenant: TenantId) -> Option<HostedSet<Self::Storage>> {
        None
    }
}

/// The edge's run, or nothing on a shared edge another loop polls.
async fn run_edge<P: ParosProviders>(
    edge: &mut Option<&mut RpcEdge<P>>,
) -> moonpool_core::SimulationError {
    match edge {
        Some(edge) => edge.run().await,
        None => std::future::pending().await,
    }
}

/// A matchmaker host's fixed wiring: who it is, its page size, its audit
/// factory, and what a storage fault does.
pub struct Host<'f, F> {
    pub config: &'f MatchmakerConfig,
    pub page: usize,
    pub factory: &'f F,
    /// A storage fault ends the host (a matchmaker process, fail-stop), or
    /// only its set, which re-opens at its next request (a machine, where a
    /// storage fault quarantines its store, not the process).
    pub fail_stop: bool,
    /// The cell this host serves (#539 (matchmaker cell fence)): a request
    /// that names another is refused. `0` on a deployment without a cell
    /// plan.
    pub cell_id: u64,
}

impl<'f, F> Host<'f, F> {
    /// The seat of the tenant `addressed` names: hosted, or opened now
    /// through `opener`. `None` when no set serves that tenant here.
    async fn seat<'h, S, A, O>(
        &self,
        hosted: &'h mut BTreeMap<TenantId, Seat<'f, S, A, F>>,
        opener: &mut O,
        addressed: Addressed,
    ) -> Result<Option<&'h mut Seat<'f, S, A, F>>, RunError>
    where
        S: MatchmakerStorage,
        A: Audit,
        F: Fn(JournalIdentifier) -> A,
        O: SetOpener<Storage = S>,
    {
        // Paired with `admitted`: a set opens only for its own cell's
        // request (#539 (matchmaker cell fence)).
        assert!(
            addressed.cell_id == self.cell_id,
            "a set is seated only for its own cell's request"
        );
        let tenant = addressed.tenant;
        let vacant = match hosted.entry(tenant) {
            std::collections::btree_map::Entry::Occupied(hosted) => {
                return Ok(Some(hosted.into_mut()));
            }
            std::collections::btree_map::Entry::Vacant(vacant) => vacant,
        };
        let Some(set) = opener.open(tenant).await else {
            tracing::warn!(
                matchmaker = self.config.id.0,
                tenant = tenant.0,
                "matchmaker_unknown_tenant"
            );
            return Ok(None);
        };
        assert!(
            set.tenant == tenant,
            "an opener opens the set it was asked for"
        );
        match boot_set(set, self.config, self.page, self.factory).await {
            Ok(booted) => {
                let booted_seat = vacant.insert(booted);
                assert!(
                    booted_seat.matchmaker.id() == self.config.id,
                    "an booted_seat set speaks for this matchmaker"
                );
                Ok(Some(booted_seat))
            }
            Err(error) => {
                self.closed(tenant, error)?;
                Ok(None)
            }
        }
    }

    /// The tenant of a request this host serves, or `None` when it is
    /// another cell's or another matchmaker's: refused unanswered, and the
    /// requester re-asks elsewhere.
    fn admitted(&self, addressed: Addressed) -> Option<TenantId> {
        let served = self.ours(addressed.cell_id) && self.addressed(addressed.to);
        assert!(
            !served || addressed.cell_id == self.cell_id,
            "a served request names this host's cell"
        );
        served.then_some(addressed.tenant)
    }

    /// Whether a request of cell `cell_id` is this host's cell's (#539
    /// (matchmaker cell fence)), as a peer batch is judged (#216): a
    /// matchmaker at an address another cell left never answers it, nor
    /// provisions a set for it.
    fn ours(&self, cell_id: u64) -> bool {
        let ours = cell_id == self.cell_id;
        if !ours {
            moonpool_assertions::reachable!("matchmaker: a request from another cell is refused");
            tracing::warn!(
                matchmaker = self.config.id.0,
                from = cell_id,
                cell = self.cell_id,
                "matchmaker_foreign_cell"
            );
        }
        ours
    }

    /// Whether a request addressed to `to` is this matchmaker's (#190): a
    /// request for another one is dropped unanswered, so a machine that took
    /// over an address never answers, or provisions a set, as a matchmaker
    /// it is not.
    fn addressed(&self, to: MatchmakerId) -> bool {
        let mine = to == self.config.id;
        if !mine {
            moonpool_assertions::reachable!(
                "matchmaker: a request addressed to another matchmaker is dropped"
            );
            tracing::warn!(
                matchmaker = self.config.id.0,
                to = to.0,
                "matchmaker_misaddressed"
            );
        }
        assert!(
            mine == (to.0 == self.config.id.0),
            "an address names one matchmaker"
        );
        mine
    }

    /// A storage fault in `tenant`'s set: the host's end when it is
    /// fail-stop, else the set's (it re-opens at its next request).
    fn fail<S, A>(
        &self,
        hosted: &mut BTreeMap<TenantId, Seat<'f, S, A, F>>,
        tenant: TenantId,
        error: RunError,
    ) -> Result<(), RunError> {
        if !self.fail_stop {
            hosted.remove(&tenant);
            assert!(!hosted.contains_key(&tenant), "a failed set is closed");
        }
        self.closed(tenant, error)
    }

    /// `tenant`'s set closed on `error`: the host's end when it is
    /// fail-stop.
    fn closed(&self, tenant: TenantId, error: RunError) -> Result<(), RunError> {
        if self.fail_stop {
            return Err(error);
        }
        moonpool_assertions::reachable!("matchmaker: a host closes one set on a storage fault");
        tracing::warn!(matchmaker = self.config.id.0, tenant = tenant.0, %error, "matchmaker_set_closed");
        Ok(())
    }

    /// Serve the matchmaker contract on `inbox` until `shutdown`: route
    /// each request to its tenant's set, open a set `opener` provisions on
    /// its first request, and answer each request once its write is
    /// durable. `edge` is the RPC runtime to poll, `None` on a shared edge.
    // One select over the contract's three inboxes.
    /// # Panics
    ///
    /// # Errors
    ///
    /// When the call or the decoding fails: the error says why.
    ///
    /// If an assertion on its own invariants fails: a programmer error, never
    /// an operating condition.
    #[allow(clippy::too_many_lines)]
    pub async fn serve<P, S, A, O>(
        &self,
        mut edge: Option<&mut RpcEdge<P>>,
        mut inbox: MatchmakerInbox,
        mut hosted: BTreeMap<TenantId, Seat<'f, S, A, F>>,
        opener: &mut O,
        shutdown: CancellationToken,
    ) -> Result<(), RunError>
    where
        P: ParosProviders,
        S: MatchmakerStorage,
        A: Audit,
        F: Fn(JournalIdentifier) -> A,
        O: SetOpener<Storage = S>,
    {
        let id = self.config.id;
        assert!(
            hosted.values().all(|seat| seat.matchmaker.id() == id),
            "every hosted set speaks for this matchmaker"
        );
        loop {
            moonpool_core::select! {
                error = run_edge(&mut edge) => return Err(error.into()),
                Some(((addressed, request), reply)) = inbox.requests.recv() => {
                    // One request, one batch, one reply: the core answers every
                    // request it is stepped, and the drain hands the reply out
                    // only once the batch is durable.
                    let Some(tenant) = self.admitted(addressed) else {
                        continue;
                    };
                    let Some(seat) = self.seat(&mut hosted, opener, addressed).await? else {
                        continue;
                    };
                    let journal = request.journal;
                    seat.port(journal);
                    seat.matchmaker.step(request);
                    let mut drained = match drain(seat).await {
                        Ok(drained) => drained,
                        Err(error) => {
                            self.fail(&mut hosted, tenant, error)?;
                            continue;
                        }
                    };
                    let answer = drained.replies.pop();
                    assert!(
                        answer.is_some() && drained.replies.is_empty() && drained.reconfigure_replies.is_empty(),
                        "one matchmaking request yields exactly one reply"
                    );
                    if let Some(answer) = answer {
                        assert!(answer.journal == journal, "a reply echoes its request's journal");
                        let page_limit = seat.matchmaker.limits().registry_page;
                        report_reply(seat.ports.port(journal), &answer, page_limit);
                        // A lost reply is a legal outcome: the registration stands
                        // and the requester's retry is the same request again,
                        // answered from the retained history.
                        match_answer(id, Reply::Match, reply, answer);
                    }
                }
                Some(((addressed, request), reply)) = inbox.collects.recv() => {
                    // The GC *primitive*, not the GC protocol: raise one
                    // journal's floor (a no-op at or below the current one,
                    // refused for a generation this matchmaker is not active
                    // for), persist it, and only then acknowledge with the floor
                    // in force. The leader owns the paper's §3.5 preconditions
                    // (`paros_core` `node/gc.rs`).
                    let Some(tenant) = self.admitted(addressed) else {
                        continue;
                    };
                    let Some(seat) = self.seat(&mut hosted, opener, addressed).await? else {
                        continue;
                    };
                    let GcRequest { from, journal, generation, watermark } = request;
                    seat.port(journal);
                    let outcome = seat.matchmaker.advance_gc_watermark(journal, generation, watermark);
                    tracing::info!(
                        matchmaker = id.0,
                        from = from.0,
                        tenant = tenant.0,
                        journal = journal.0,
                        generation = generation.0,
                        round = watermark.round,
                        outcome = ?outcome,
                        "garbage_collect_requested"
                    );
                    let drained = match drain(seat).await {
                        Ok(drained) => drained,
                        Err(error) => {
                            self.fail(&mut hosted, tenant, error)?;
                            continue;
                        }
                    };
                    assert!(
                        drained.replies.is_empty() && drained.reconfigure_replies.is_empty(),
                        "a garbage-collect request yields no match reply"
                    );
                    let ack = GcAck {
                        matchmaker: id,
                        journal,
                        generation,
                        applied: outcome != GcOutcome::Refused,
                        watermark: seat.matchmaker.gc_watermark(journal),
                        newer: (outcome == GcOutcome::Refused)
                            .then(|| seat.matchmaker.newer_set(generation))
                            .flatten(),
                    };
                    assert!(
                        ack.applied || ack.newer.as_ref().is_none_or(|s| s.generation > generation),
                        "a stopped matchmaker names a later generation"
                    );
                    seat.port(journal).matchmaker_gc_replied(id, &ack);
                    match_answer(id, Reply::GcAck, reply, ack);
                }
                Some(((addressed, request), reply)) = inbox.reconfigures.recv() => {
                    // One step of a matchmaker-set handover (#125): the core
                    // answers it, and the reply leaves only once its write (a
                    // freeze, a pending bootstrap, a promise, a vote, an
                    // activation) is durable.
                    let Some(tenant) = self.admitted(addressed) else {
                        continue;
                    };
                    let Some(seat) = self.seat(&mut hosted, opener, addressed).await? else {
                        continue;
                    };
                    tracing::info!(
                        matchmaker = id.0,
                        from = request.from().0,
                        tenant = tenant.0,
                        kind = reconfigure_kind(&request),
                        "reconfigure_requested"
                    );
                    // A bootstrap names the journals the successor inherits:
                    // each gets its audit port before the activation reports.
                    if let paros_core::ReconfigureRequest::Bootstrap { bootstrap, .. } = &request {
                        for journal in bootstrap.registries.keys() {
                            seat.port(*journal);
                        }
                    }
                    seat.matchmaker.step_reconfigure(request.clone());
                    let mut drained = match drain(seat).await {
                        Ok(drained) => drained,
                        Err(error) => {
                            self.fail(&mut hosted, tenant, error)?;
                            continue;
                        }
                    };
                    let answer = drained.reconfigure_replies.pop();
                    assert!(
                        answer.is_some() && drained.reconfigure_replies.is_empty() && drained.replies.is_empty(),
                        "one reconfigure request yields exactly one reply"
                    );
                    if let Some(answer) = answer {
                        for audit in seat.ports.set_wide() {
                            audit.matchmaker_reconfigure_replied(id, &request, &answer);
                        }
                        tracing::info!(
                            matchmaker = id.0,
                            tenant = tenant.0,
                            kind = reconfigure_kind(&request),
                            reply = reconfigure_reply_kind(&answer),
                            generation = seat.matchmaker.set().generation.0,
                            phase = ?seat.matchmaker.phase(),
                            "reconfigure_replied"
                        );
                        match_answer(id, Reply::MatchmakerReconfigure, reply, answer);
                    }
                }
                () = shutdown.cancelled() => return Ok(()),
            }
        }
    }
}

/// One matchmaker set a process hosts (#190): its tenant, the journals of
/// that tenant whose audit ports exist from boot, its own store and the
/// operator's claim about that store.
#[derive(Debug)]
pub struct HostedSet<S> {
    /// The tenant the set serves.
    pub tenant: TenantId,
    /// The journals of the tenant known at boot: each gets its audit port
    /// before the first request (a journal a request names later gets one
    /// then).
    pub journals: Vec<JournalId>,
    /// The set's durable registry.
    pub storage: S,
    /// The operator's claim about `storage` ([`BootKind`], #183).
    pub boot: BootKind,
}

/// One hosted set at run time: its core, its store and its audit ports.
pub struct Seat<'f, S, A, F> {
    matchmaker: Matchmaker,
    storage: S,
    ports: Ports<'f, A, F>,
}

impl<S, A, F> Seat<'_, S, A, F>
where
    S: MatchmakerStorage,
    A: Audit,
    F: Fn(JournalIdentifier) -> A,
{
    /// `journal`'s audit port. A journal this set meets after its boot (a
    /// machine's set learns its journals from their requests, #190) is
    /// introduced to its port as a boot would: the set, its phase and that
    /// journal's registry as they stand.
    fn port(&mut self, journal: JournalId) -> &A {
        let fresh = !self.ports.by_journal.contains_key(&journal);
        assert!(
            !fresh || self.matchmaker.registry(journal).is_empty(),
            "a journal met after boot holds no registration yet"
        );
        let id = self.matchmaker.id();
        let audit = self.ports.port(journal);
        if fresh {
            audit.matchmaker_recovered(
                id,
                self.matchmaker.set(),
                self.matchmaker.phase(),
                self.matchmaker.hard_state(),
                self.matchmaker.registry(journal),
                self.matchmaker.gc_watermark(journal),
            );
        }
        audit
    }
}

/// Boot one hosted set: verify its store, judge the operator's claim
/// against its format marker, build its core from the store and report the
/// recovered registry of every journal it knows.
#[tracing::instrument(level = "debug", skip_all, fields(matchmaker = config.id.0, tenant = set.tenant.0))]
async fn boot_set<'f, S, A, F>(
    set: HostedSet<S>,
    config: &MatchmakerConfig,
    page: usize,
    factory: &'f F,
) -> Result<Seat<'f, S, A, F>, RunError>
where
    S: MatchmakerStorage,
    A: Audit,
    F: Fn(JournalIdentifier) -> A,
{
    let HostedSet {
        tenant,
        journals,
        mut storage,
        boot,
    } = set;
    let id = config.id;
    let mut ports = Ports::new(tenant, &journals, factory);
    // Verify the store before the core reads it (the node driver's rule).
    storage
        .boot_scan()
        .await
        .map_err(|e| storage_fault_crash(&ports, id, e))?;
    check_format_marker(&mut storage, boot, config, &ports).await?;
    // The sans-IO core, bootstrapped from durable storage through the
    // read-only port (scalars once, then record by record); re-report the
    // recovered registry so the oracles see this incarnation's belief.
    let limits = Limits {
        registry_page: page,
        ..Limits::DEFAULT
    };
    let matchmaker = Matchmaker::new(config, &storage, limits);
    assert!(
        matchmaker.limits().registry_page == page,
        "a booted matchmaker runs the driver's registry page"
    );
    for journal in matchmaker.journals() {
        ports.port(journal);
    }
    let phase = matchmaker.phase();
    let scalars = storage.initial_state();
    for (journal, audit) in &ports.by_journal {
        audit.matchmaker_recovered(
            id,
            matchmaker.set(),
            phase,
            &scalars,
            matchmaker.registry(*journal),
            matchmaker.gc_watermark(*journal),
        );
    }
    if ports.by_journal.is_empty() {
        let unset = JournalId::UNSET;
        for audit in ports.set_wide() {
            audit.matchmaker_recovered(
                id,
                matchmaker.set(),
                phase,
                &scalars,
                matchmaker.registry(unset),
                matchmaker.gc_watermark(unset),
            );
        }
    }
    tracing::info!(
        matchmaker = id.0,
        tenant = tenant.0,
        generation = matchmaker.set().generation.0,
        phase = ?phase,
        journals = ports.by_journal.len() as u64,
        "matchmaker_booted"
    );
    assert!(
        matchmaker
            .journals()
            .all(|journal| ports.by_journal.contains_key(&journal)),
        "every journal a booted set knows has its audit port"
    );
    Ok(Seat {
        matchmaker,
        storage,
        ports,
    })
}

/// Drive a paros matchmaker process to completion over the given providers:
/// one matchmaker set per hosted tenant (#190).
///
/// Generic over `P: ParosProviders` (production *or* simulation) and
/// `S: MatchmakerStorage` (the injected durable registry of each set). Every
/// set runs its own [`paros_core::Matchmaker`], built from `config` (the
/// process's identity and the deployment's bootstrap set) over its own
/// store; the loop serves the matchmaker RPC contract on `local_addr`,
/// routes each request to the set of the tenant it names, and answers it
/// only once its write is fsync-durable. A request for a tenant this process
/// hosts no set for is dropped unanswered (the requester re-asks; nothing
/// was registered). `tunables` supplies the RPC liveness and inbox shape
/// (the matchmaker has no tick and no peers); `audit` makes the audit port
/// of one journal, the same provider-generic seam the node driver takes,
/// one port per journal so each registry is judged alone. Each reply kind
/// is its own inline reply-drop location ([`Reply::Match`],
/// [`Reply::GcAck`], [`Reply::MatchmakerReconfigure`]).
///
/// Each set's `boot` is the operator's claim about its store
/// ([`BootKind`], #183), the node driver's rule applied to the registry: a
/// first boot formats the store before the core reads it, an existing
/// matchmaker whose store carries no format marker has lost its registry
/// and is refused ([`BootRefusal::Amnesia`]) — it is *replaced* through a
/// matchmaker-set reconfiguration (#125), never rejoined — and a first boot
/// on a formatted store is refused too ([`BootRefusal::AlreadyFormatted`]).
/// One refused set refuses the process.
///
/// # Errors
///
/// The exit is typed exactly like [`run_node`](crate::node::run_node)'s:
/// [`RunError::Storage`] for a
/// fail-stop storage fault, [`RunError::Refused`] when a boot claim and
/// a registry's format marker disagree, [`RunError::Infra`] for a genuine
/// provider/infrastructure failure or two sets of one tenant.
///
/// # Panics
///
/// If the core breaks its one-request-one-reply contract (a programmer error,
/// never an operating condition).
#[tracing::instrument(level = "debug", skip_all, fields(matchmaker = config.id.0, local_addr = %local_addr, sets = sets.len()))]
// The parameters are the matchmaker's complete wiring; a bundle would only
// rename them. The loop is one select over the contract's three inboxes.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn run_matchmaker<P, S, A, F>(
    providers: P,
    sets: Vec<HostedSet<S>>,
    local_addr: String,
    config: MatchmakerConfig,
    tunables: DriverTunables,
    transport: Transport,
    shutdown: CancellationToken,
    audit: F,
) -> Result<(), RunError>
where
    P: ParosProviders,
    S: MatchmakerStorage,
    A: Audit,
    F: Fn(JournalIdentifier) -> A,
{
    let id = config.id;
    // The registry page size (#338): the tunable, capped at the core's
    // ceiling (an operator's override is external input, never a panic).
    let page = tunables.registry_page.clamp(1, paros_core::REGISTRY_PAGE);
    let mut hosted: BTreeMap<TenantId, Seat<'_, S, A, F>> = BTreeMap::new();
    for set in sets {
        let tenant = set.tenant;
        if hosted.contains_key(&tenant) {
            return Err(RunError::Infra(
                moonpool_core::SimulationError::InvalidState(
                    "a matchmaker hosts one set per tenant".into(),
                ),
            ));
        }
        let seat = boot_set(set, &config, page, &audit).await?;
        hosted.insert(tenant, seat);
    }
    assert!(
        hosted.values().all(|seat| seat.matchmaker.id() == id),
        "every hosted set speaks for this matchmaker"
    );

    let mut edge =
        RpcEdge::listen(&providers, &local_addr, "matchmaker", &tunables, &transport).await?;
    let inbox = MatchmakerInbox::serve(edge.handle(), None)?;
    let host = Host {
        config: &config,
        page,
        factory: &audit,
        fail_stop: true,
        // A standalone matchmaker serves a deployment without a cell plan.
        cell_id: 0,
    };
    host.serve(
        Some(&mut edge),
        inbox,
        hosted,
        &mut NoOpener(std::marker::PhantomData),
        shutdown,
    )
    .await
}
