//! The provider-generic **matchmaker driver** — the I/O layer that owns a
//! sans-IO [`paros_core::Matchmaker`], the twin of [`run_node`](crate::run_node)
//! for the registry role.
//!
//! Written once over moonpool's `P: Providers`, so the *same* loop runs in
//! production and deterministic simulation; the harness adapts a moonpool
//! `Process` to it exactly as it adapts the node. The loop serves the
//! matchmaker gRPC contract, feeds each request into the core, and drains every
//! [`MatchmakerReady`](paros_core::MatchmakerReady) in **persist → fsync →
//! reply** order: a `Registered` reply leaves only once its registration is
//! durable, the registry's version of the acceptor's persist-before-`Promise`
//! rule — and the same order carries a freeze (`StopAck`), a pending
//! bootstrap, a decree promise or vote, and an activation (#125). The registry
//! is read back through the core's [`RegistryStorage`](paros_core::RegistryStorage)
//! port and written through [`MatchmakerStorage`] — the node's `Storage` /
//! `NodeStorage` split, mirrored so the log's CTRL recovery applies to the
//! registry (see [`storage`]).
//!
//! A cluster deployed without matchmakers never runs this loop; the node
//! driver does not know it exists.

mod storage;

use moonpool_core::{NetworkProvider, Providers, SimulationError, TcpListenerTrait};
use moonpool_hyper::{H2Server, H2ServerConfig};
use paros_core::{
    AcceptorConfig, Ballot, GcAck, GcOutcome, GcRequest, MatchOutcome, MatchReply, Matchmaker,
    MatchmakerConfig, MatchmakerId, MatchmakerWriteOp, ReconfigureReply, ReconfigureRequest,
    Registration,
};
use tokio_util::sync::CancellationToken;

use crate::audit::{Audit, StorageFaultDecision};
use crate::driver::{DriverTunables, RunError, accept_and_serve, grpc_keep_alive};
use crate::grpc::{MatchmakerInbox, ParosMatchmakerServer, matchmaker_channel};
use crate::hooks::{DriverHooks, Reply, Seam};
use crate::storage::StorageError;

pub use storage::{MatchmakerStorage, MemMatchmakerStorage, matchmaker_storage_contract_suite};

/// A stable digest of an acceptor configuration (FNV-1a over the sorted
/// membership and the quorum system), emitted on the audit callbacks so a
/// checker can compare configurations by equality without carrying them. The
/// same function on both ends: the driver hashes what it persists and what it
/// replies, an observer hashes what it sees on the wire.
#[must_use]
pub fn config_hash(config: &AcceptorConfig) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let mut fold = |bytes: &[u8]| {
        for &b in bytes {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
    };
    fold(&(config.members().len() as u64).to_le_bytes());
    for member in config.members() {
        fold(&member.0.to_le_bytes());
    }
    fold(&[match config.quorum_system() {
        paros_core::QuorumSystem::Majority => 0_u8,
    }]);
    h
}

/// Map a [`StorageError`] into the driver's deliberate crash decision, typed on
/// the audit at the instant it is made (the matchmaker twin of the node
/// driver's storage-fault crash).
fn storage_fault_crash<A: Audit>(audit: &A, id: MatchmakerId, e: StorageError) -> RunError {
    audit.matchmaker_storage_fault(id, &e, StorageFaultDecision::Crash);
    tracing::warn!(matchmaker = id.0, error = %e, decision = "crash", "matchmaker_storage_fault");
    RunError::Storage(e)
}

/// One drained batch: the replies the caller may now send.
struct Drained {
    replies: Vec<MatchReply>,
    reconfigure_replies: Vec<ReconfigureReply>,
}

/// Run the [`MatchmakerReady`](paros_core::MatchmakerReady) handshake once:
/// persist the batch's writes, fsync them, report them, and hand back the
/// replies the caller may now send. The two crash seams sit exactly where a
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
/// read back every durable registration), and the two crash seams below are
/// the BUGGIFY locations that make both crash windows likely. The same
/// ordering covers the generation writes of #125: a `StopAck` leaves only
/// once the freeze is durable, a bootstrap ack only once the pending record
/// is, a decree promise or vote only once the decree record is.
#[allow(clippy::too_many_lines)]
#[tracing::instrument(level = "trace", skip_all, fields(matchmaker = matchmaker.id().0))]
fn drain<S, H, A>(
    matchmaker: &mut Matchmaker,
    storage: &mut S,
    hooks: &H,
    audit: &A,
) -> Result<Drained, RunError>
where
    S: MatchmakerStorage,
    H: DriverHooks,
    A: Audit,
{
    let id = matchmaker.id();
    let ready = matchmaker.ready();
    let writes = ready.writes().to_vec();
    let replies = ready.replies().to_vec();
    let reconfigure_replies = ready.reconfigure_replies().to_vec();
    ready.advance();

    // 1. Persist every write, in order.
    for op in &writes {
        match op {
            MatchmakerWriteOp::Register {
                ballot,
                registration,
            } => storage
                .register(*ballot, registration)
                .map_err(|e| storage_fault_crash(audit, id, e))?,
            MatchmakerWriteOp::SetGcWatermark(watermark) => storage
                .set_gc_watermark(*watermark)
                .map_err(|e| storage_fault_crash(audit, id, e))?,
            MatchmakerWriteOp::SetScalars(scalars) => storage
                .set_scalars(scalars)
                .map_err(|e| storage_fault_crash(audit, id, e))?,
            MatchmakerWriteOp::InstallRegistry {
                scalars,
                registrations,
            } => storage
                .install_registry(scalars, registrations)
                .map_err(|e| storage_fault_crash(audit, id, e))?,
        }
    }
    if !writes.is_empty() {
        // Crash seam: staged but not flushed — the batch dies whole, and no
        // reply has been handed out yet.
        if hooks.crash_at(Seam::MatchBeforeSync) {
            audit.matchmaker_crashed(id, Seam::MatchBeforeSync);
            tracing::info!(
                matchmaker = id.0,
                seam = "match_before_sync",
                "matchmaker_crashed"
            );
            return Err(RunError::SeamCrash(Seam::MatchBeforeSync));
        }
        storage
            .sync()
            .map_err(|e| storage_fault_crash(audit, id, e))?;
        // Durable now — report the truthful persisted state.
        for op in &writes {
            match op {
                MatchmakerWriteOp::Register {
                    ballot,
                    registration,
                } => {
                    audit.match_registered(id, *ballot, registration);
                    tracing::info!(
                        matchmaker = id.0,
                        round = ballot.round,
                        bnode = ballot.node.0,
                        members = registration.config.members().len() as u64,
                        reconfiguration = registration.kind.is_reconfiguration(),
                        config = config_hash(&registration.config),
                        "match_registered"
                    );
                }
                MatchmakerWriteOp::SetGcWatermark(watermark) => {
                    audit.gc_watermark_raised(id, *watermark);
                    tracing::info!(
                        matchmaker = id.0,
                        round = watermark.round,
                        bnode = watermark.node.0,
                        "gc_watermark_raised"
                    );
                }
                MatchmakerWriteOp::SetScalars(scalars) => {
                    audit.matchmaker_scalars_persisted(id, scalars);
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
                    let set = matchmaker.set();
                    let registry: Vec<(Ballot, Registration)> =
                        registrations.iter().map(|(b, r)| (*b, r.clone())).collect();
                    audit.matchmaker_activated(
                        id,
                        &set,
                        scalars.gc_watermark,
                        scalars.effective.as_ref(),
                        &registry,
                    );
                    tracing::info!(
                        matchmaker = id.0,
                        generation = set.generation.0,
                        members = set.members.len() as u64,
                        watermark_round = scalars.gc_watermark.round,
                        registrations = registry.len() as u64,
                        "matchmaker_activated"
                    );
                }
            }
        }
    }
    // 2. Crash seam: durable, but the reply never leaves. Only meaningful when
    //    there is a reply to lose.
    if (!replies.is_empty() || !reconfigure_replies.is_empty())
        && hooks.crash_at(Seam::MatchAfterSyncBeforeReply)
    {
        audit.matchmaker_crashed(id, Seam::MatchAfterSyncBeforeReply);
        tracing::info!(
            matchmaker = id.0,
            seam = "match_after_sync_before_reply",
            "matchmaker_crashed"
        );
        return Err(RunError::SeamCrash(Seam::MatchAfterSyncBeforeReply));
    }
    Ok(Drained {
        replies,
        reconfigure_replies,
    })
}

/// Report one reply at the instant it leaves.
fn report_reply<A: Audit>(audit: &A, reply: &MatchReply) {
    let id = reply.matchmaker;
    match &reply.outcome {
        MatchOutcome::Registered {
            history,
            gc_watermark,
            ..
        } => {
            let history: Vec<(Ballot, Registration)> = history
                .iter()
                .map(|(ballot, registration)| (*ballot, registration.clone()))
                .collect();
            audit.match_replied(
                id,
                reply.to,
                reply.ballot,
                reply.generation.0,
                &history,
                *gc_watermark,
            );
            tracing::info!(
                matchmaker = id.0,
                to = reply.to.0,
                round = reply.ballot.round,
                bnode = reply.ballot.node.0,
                generation = reply.generation.0,
                history = history.len() as u64,
                watermark_round = gc_watermark.round,
                "match_replied"
            );
        }
        MatchOutcome::Refused(refusal) => {
            audit.match_refused(id, reply.to, reply.ballot, refusal.clone());
            tracing::info!(
                matchmaker = id.0,
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

/// Drive a paros matchmaker to completion over the given providers.
///
/// Generic over `P: Providers` (production *or* simulation) and
/// `S: MatchmakerStorage` (the injected durable registry). The loop owns a
/// [`paros_core::Matchmaker`] built from `config` (its identity and the
/// deployment's bootstrap set), serves the matchmaker gRPC contract on
/// `local_addr`, and answers each request only once its write is
/// fsync-durable. `tunables` supplies the h2 keep-alive and inbox shape (the
/// matchmaker has no tick and no peers); `hooks` and `audit` are the same
/// provider-generic seams the node driver takes, with the matchmaker's own
/// crash locations ([`Seam::MatchBeforeSync`],
/// [`Seam::MatchAfterSyncBeforeReply`]) and reply-drop locations
/// ([`Reply::Match`], [`Reply::GcAck`], [`Reply::MatchmakerReconfigure`]).
///
/// # Errors
///
/// The exit is typed exactly like [`run_node`](crate::run_node)'s:
/// [`RunError::SeamCrash`] for a hook-injected crash at a durability seam (the
/// caller re-runs against the surviving storage), [`RunError::Storage`] for a
/// fail-stop storage fault, [`RunError::Infra`] for a genuine
/// provider/infrastructure failure.
///
/// # Panics
///
/// If the core breaks its one-request-one-reply contract (a programmer error,
/// never an operating condition).
#[tracing::instrument(level = "debug", skip_all, fields(matchmaker = config.id.0, local_addr = %local_addr))]
// The parameters are the matchmaker's complete wiring; a bundle would only
// rename them. The loop is one select over the contract's three inboxes.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub async fn run_matchmaker<P, S, H, A>(
    providers: P,
    mut storage: S,
    local_addr: String,
    config: MatchmakerConfig,
    tunables: DriverTunables,
    shutdown: CancellationToken,
    hooks: &H,
    audit: &A,
) -> Result<(), RunError>
where
    P: Providers,
    S: MatchmakerStorage,
    H: DriverHooks,
    A: Audit + Clone + Send + Sync + 'static,
{
    let id = config.id;
    // Verify the store before the core reads it (the node driver's rule).
    storage
        .boot_scan()
        .map_err(|e| storage_fault_crash(audit, id, e))?;

    let incarnation_shutdown = CancellationToken::new();
    let _incarnation_guard = incarnation_shutdown.clone().drop_guard();

    let listener = providers
        .network()
        .bind(&local_addr)
        .await
        .map_err(|e| SimulationError::InvalidState(format!("matchmaker gRPC listener: {e}")))?;

    // The sans-IO core, bootstrapped from durable storage through the
    // read-only port (scalars once, then record by record); re-report the
    // recovered registry so the oracles see this incarnation's belief.
    let mut matchmaker = Matchmaker::new(&config, &storage);
    let recovered: Vec<(Ballot, Registration)> = matchmaker
        .registry()
        .iter()
        .map(|(ballot, registration)| (*ballot, registration.clone()))
        .collect();
    let watermark = matchmaker.hard_state().gc_watermark;
    let set = matchmaker.set();
    let phase = matchmaker.phase();
    audit.matchmaker_recovered(id, &set, phase, &recovered, watermark);
    tracing::info!(
        matchmaker = id.0,
        generation = set.generation.0,
        phase = ?phase,
        registrations = recovered.len() as u64,
        watermark_round = watermark.round,
        "matchmaker_booted"
    );

    let (service, mut inbox): (_, MatchmakerInbox) =
        matchmaker_channel(tunables.client_inbox_capacity);
    let grpc_service = tonic::service::Routes::new(ParosMatchmakerServer::new(service)).prepare();
    let grpc_server = H2Server::new(&providers).with_config(H2ServerConfig {
        keep_alive: Some(grpc_keep_alive(&tunables)),
        vectored_writes: true,
    });

    loop {
        moonpool_core::select! {
            accepted = listener.accept() => {
                let (stream, addr) = accepted
                    .map_err(|e| SimulationError::InvalidState(format!("matchmaker gRPC accept: {e}")))?;
                let connection = grpc_server.serve_connection_with_shutdown(
                    stream,
                    grpc_service.clone(),
                    incarnation_shutdown.clone().cancelled_owned(),
                );
                accept_and_serve(&providers, "paros-matchmaker-grpc-server", "matchmaker", addr, connection);
            }
            Some((request, reply)) = inbox.requests.recv() => {
                // One request, one batch, one reply: the core answers every
                // request it is stepped, and the drain hands the reply out
                // only once the batch is durable.
                matchmaker.step(request);
                let mut drained = drain(&mut matchmaker, &mut storage, hooks, audit)?;
                let answer = drained.replies.pop();
                assert!(
                    answer.is_some() && drained.replies.is_empty() && drained.reconfigure_replies.is_empty(),
                    "one matchmaking request yields exactly one reply"
                );
                if let Some(answer) = answer {
                    report_reply(audit, &answer);
                    // A lost reply is a legal outcome: the registration stands
                    // and the requester's retry is the same request again,
                    // answered from the retained history.
                    if hooks.drop_client_reply(Reply::Match) {
                        audit.match_reply_dropped(id, Reply::Match);
                        tracing::info!(matchmaker = id.0, reply = "match", "match_reply_dropped");
                    } else {
                        let _ = reply.send(answer);
                    }
                }
            }
            Some((request, reply)) = inbox.collects.recv() => {
                // The GC *primitive*, not the GC protocol: raise the floor (a
                // no-op at or below the current one, refused for a generation
                // this matchmaker is not active for), persist it, and only
                // then acknowledge with the floor in force. The leader owns
                // the paper's §3.5 preconditions (`paros_core` `node/gc.rs`).
                let GcRequest { from, generation, watermark } = request;
                let outcome = matchmaker.advance_gc_watermark(generation, watermark);
                tracing::info!(
                    matchmaker = id.0,
                    from = from.0,
                    generation = generation.0,
                    round = watermark.round,
                    outcome = ?outcome,
                    "garbage_collect_requested"
                );
                let drained = drain(&mut matchmaker, &mut storage, hooks, audit)?;
                assert!(
                    drained.replies.is_empty() && drained.reconfigure_replies.is_empty(),
                    "a garbage-collect request yields no match reply"
                );
                let ack = GcAck {
                    matchmaker: id,
                    generation,
                    applied: outcome != GcOutcome::Refused,
                    watermark: matchmaker.hard_state().gc_watermark,
                };
                audit.matchmaker_gc_replied(id, &ack);
                if hooks.drop_client_reply(Reply::GcAck) {
                    audit.match_reply_dropped(id, Reply::GcAck);
                    tracing::info!(matchmaker = id.0, reply = "gc_ack", "match_reply_dropped");
                } else {
                    let _ = reply.send(ack);
                }
            }
            Some((request, reply)) = inbox.reconfigures.recv() => {
                // One step of a matchmaker-set handover (#125): the core
                // answers it, and the reply leaves only once its write (a
                // freeze, a pending bootstrap, a promise, a vote, an
                // activation) is durable.
                tracing::info!(
                    matchmaker = id.0,
                    from = request.from().0,
                    kind = reconfigure_kind(&request),
                    "reconfigure_requested"
                );
                matchmaker.step_reconfigure(request.clone());
                let mut drained = drain(&mut matchmaker, &mut storage, hooks, audit)?;
                let answer = drained.reconfigure_replies.pop();
                assert!(
                    answer.is_some() && drained.reconfigure_replies.is_empty() && drained.replies.is_empty(),
                    "one reconfigure request yields exactly one reply"
                );
                if let Some(answer) = answer {
                    audit.matchmaker_reconfigure_replied(id, &request, &answer);
                    tracing::info!(
                        matchmaker = id.0,
                        kind = reconfigure_kind(&request),
                        reply = reconfigure_reply_kind(&answer),
                        generation = matchmaker.set().generation.0,
                        phase = ?matchmaker.phase(),
                        "reconfigure_replied"
                    );
                    if hooks.drop_client_reply(Reply::MatchmakerReconfigure) {
                        audit.match_reply_dropped(id, Reply::MatchmakerReconfigure);
                        tracing::info!(matchmaker = id.0, reply = "reconfigure", "match_reply_dropped");
                    } else {
                        let _ = reply.send(answer);
                    }
                }
            }
            () = shutdown.cancelled() => return Ok(()),
        }
    }
}

/// The trace label of one reconfiguration request.
#[must_use]
pub fn reconfigure_kind(request: &ReconfigureRequest) -> &'static str {
    match request {
        ReconfigureRequest::Stop { .. } => "stop",
        ReconfigureRequest::Bootstrap { .. } => "bootstrap",
        ReconfigureRequest::DecreePrepare { .. } => "decree_prepare",
        ReconfigureRequest::DecreeAccept { .. } => "decree_accept",
        ReconfigureRequest::Chosen { .. } => "chosen",
    }
}

/// The trace label of one reconfiguration reply.
#[must_use]
pub fn reconfigure_reply_kind(reply: &ReconfigureReply) -> &'static str {
    match reply {
        ReconfigureReply::Stopped { .. } => "stopped",
        ReconfigureReply::Bootstrapped { .. } => "bootstrapped",
        ReconfigureReply::Promised { .. } => "promised",
        ReconfigureReply::Accepted { .. } => "accepted",
        ReconfigureReply::Nacked { .. } => "nacked",
        ReconfigureReply::Learned { .. } => "learned",
        ReconfigureReply::Refused { .. } => "refused",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paros_core::{NodeId, QuorumSystem};

    #[test]
    fn config_hash_distinguishes_membership_and_is_order_independent() {
        let a = AcceptorConfig::new(
            vec![NodeId(0), NodeId(1), NodeId(2)],
            QuorumSystem::Majority,
        );
        let b = AcceptorConfig::new(
            vec![NodeId(2), NodeId(1), NodeId(0)],
            QuorumSystem::Majority,
        );
        let c = AcceptorConfig::new(vec![NodeId(0), NodeId(1)], QuorumSystem::Majority);
        assert_eq!(config_hash(&a), config_hash(&b));
        assert_ne!(config_hash(&a), config_hash(&c));
    }
}
