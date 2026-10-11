//! The provider-generic **matchmaker driver** — the I/O layer that owns a
//! sans-IO [`paros_core::Matchmaker`], the twin of [`run_node`](crate::run_node)
//! for the registry role.
//!
//! Written once over moonpool's `P: Providers`, so the *same* loop runs in
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

mod storage;

use std::collections::BTreeMap;

use moonpool_buggify::hint::Strike;
use moonpool_core::Providers;
use paros_core::{
    GcAck, GcOutcome, GcRequest, JournalId, JournalIdentifier, MatchOutcome, MatchReply,
    Matchmaker, MatchmakerConfig, MatchmakerId, MatchmakerSet, MatchmakerWriteOp, ReconfigureReply,
    TenantId,
};
use tokio_util::sync::CancellationToken;

use crate::audit::{Audit, HistoryPage, StorageFaultDecision};
use crate::driver::edge::{MatchmakerInbox, RpcEdge};
use crate::driver::events::{config_hash, reconfigure_kind, reconfigure_reply_kind};
use crate::driver::reply::Reply;
use crate::driver::reply::match_answer;
use crate::driver::{BootKind, BootRefusal, DriverTunables, RunError};
use crate::storage::StorageError;

pub use storage::{
    MatchmakerStorage, MemMatchmakerStorage, Registrations, matchmaker_storage_contract_suite,
};

/// The audit ports of one hosted set: one per journal of its tenant, made by
/// the caller's factory on first use (a journal the set learns from a
/// request gets its port then).
struct Ports<'f, A, F> {
    tenant: TenantId,
    factory: &'f F,
    by_journal: BTreeMap<JournalId, A>,
}

impl<'f, A, F: Fn(JournalIdentifier) -> A> Ports<'f, A, F> {
    /// The ports of `tenant`'s set, one per journal in `journals` at once.
    fn new(tenant: TenantId, journals: &[JournalId], factory: &'f F) -> Self {
        let mut ports = Self {
            tenant,
            factory,
            by_journal: BTreeMap::new(),
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
                for audit in ports.all() {
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
struct Seat<'f, S, A, F> {
    matchmaker: Matchmaker,
    storage: S,
    ports: Ports<'f, A, F>,
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
    let mut matchmaker = Matchmaker::new(config, &storage);
    matchmaker.set_registry_page(page);
    assert!(
        matchmaker.registry_page() == page,
        "a booted matchmaker runs the driver's registry page"
    );
    for journal in matchmaker.journals() {
        ports.port(journal);
    }
    let phase = matchmaker.phase();
    for (journal, audit) in &ports.by_journal {
        audit.matchmaker_recovered(
            id,
            matchmaker.set(),
            phase,
            matchmaker.registry(*journal),
            matchmaker.gc_watermark(*journal),
        );
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
/// Generic over `P: Providers` (production *or* simulation) and
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
/// The exit is typed exactly like [`run_node`](crate::run_node)'s:
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
    shutdown: CancellationToken,
    audit: F,
) -> Result<(), RunError>
where
    P: Providers,
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

    let mut edge = RpcEdge::listen(&providers, &local_addr, "matchmaker", &tunables).await?;
    let mut inbox = MatchmakerInbox::serve(&edge)?;

    loop {
        moonpool_core::select! {
            error = edge.run() => return Err(error.into()),
            Some(((tenant, request), reply)) = inbox.requests.recv() => {
                // One request, one batch, one reply: the core answers every
                // request it is stepped, and the drain hands the reply out
                // only once the batch is durable.
                let Some(seat) = hosted.get_mut(&tenant) else {
                    tracing::warn!(matchmaker = id.0, tenant = tenant.0, "matchmaker_unknown_tenant");
                    continue;
                };
                let journal = request.journal;
                seat.ports.port(journal);
                seat.matchmaker.step(request);
                let mut drained = drain(seat).await?;
                let answer = drained.replies.pop();
                assert!(
                    answer.is_some() && drained.replies.is_empty() && drained.reconfigure_replies.is_empty(),
                    "one matchmaking request yields exactly one reply"
                );
                if let Some(answer) = answer {
                    assert!(answer.journal == journal, "a reply echoes its request's journal");
                    let page_limit = seat.matchmaker.registry_page();
                    report_reply(seat.ports.port(journal), &answer, page_limit);
                    // A lost reply is a legal outcome: the registration stands
                    // and the requester's retry is the same request again,
                    // answered from the retained history.
                    match_answer(id, Reply::Match, reply, answer);
                }
            }
            Some(((tenant, request), reply)) = inbox.collects.recv() => {
                // The GC *primitive*, not the GC protocol: raise one
                // journal's floor (a no-op at or below the current one,
                // refused for a generation this matchmaker is not active
                // for), persist it, and only then acknowledge with the floor
                // in force. The leader owns the paper's §3.5 preconditions
                // (`paros_core` `node/gc.rs`).
                let Some(seat) = hosted.get_mut(&tenant) else {
                    tracing::warn!(matchmaker = id.0, tenant = tenant.0, "matchmaker_unknown_tenant");
                    continue;
                };
                let GcRequest { from, journal, generation, watermark } = request;
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
                let drained = drain(seat).await?;
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
                };
                seat.ports.port(journal).matchmaker_gc_replied(id, &ack);
                match_answer(id, Reply::GcAck, reply, ack);
            }
            Some(((tenant, request), reply)) = inbox.reconfigures.recv() => {
                // One step of a matchmaker-set handover (#125): the core
                // answers it, and the reply leaves only once its write (a
                // freeze, a pending bootstrap, a promise, a vote, an
                // activation) is durable.
                let Some(seat) = hosted.get_mut(&tenant) else {
                    tracing::warn!(matchmaker = id.0, tenant = tenant.0, "matchmaker_unknown_tenant");
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
                        seat.ports.port(*journal);
                    }
                }
                seat.matchmaker.step_reconfigure(request.clone());
                let mut drained = drain(seat).await?;
                let answer = drained.reconfigure_replies.pop();
                assert!(
                    answer.is_some() && drained.reconfigure_replies.is_empty() && drained.replies.is_empty(),
                    "one reconfigure request yields exactly one reply"
                );
                if let Some(answer) = answer {
                    for audit in seat.ports.all() {
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
